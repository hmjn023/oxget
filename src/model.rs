use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::input;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
pub enum Status {
    Queued,
    Downloading,
    Verifying,
    Paused,
    Done,
    Failed,
    #[value(name = "verification_failed", alias = "verification-failed")]
    VerificationFailed,
}

impl Status {
    pub fn label(self) -> &'static str {
        match self {
            Self::Queued => "Queued",
            Self::Downloading => "Downloading",
            Self::Verifying => "Verifying",
            Self::Paused => "Paused",
            Self::Done => "Done",
            Self::Failed => "Failed",
            Self::VerificationFailed => "Verification failed",
        }
    }
}

impl Status {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::VerificationFailed)
    }
    pub fn failed(self) -> bool {
        matches!(self, Self::Failed | Self::VerificationFailed)
    }
    pub fn transferring(self) -> bool {
        matches!(self, Self::Downloading | Self::Verifying)
    }
    pub fn json_name(self) -> String {
        if self == Self::VerificationFailed {
            "verification_failed".into()
        } else {
            self.label().to_ascii_lowercase()
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Verification {
    pub expected_sha256: Option<String>,
    pub actual_sha256: Option<String>,
    pub result: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransferInfo {
    pub resume_supported: Option<bool>,
    pub reason: Option<String>,
}

impl TransferInfo {
    pub fn description(&self) -> Option<&str> {
        self.reason.as_deref().map(|reason| match reason {
            "no_strong_etag" => "No strong ETag: single request, restart from the beginning",
            "range_not_supported" => {
                "Range unsupported: single request, restart from the beginning"
            }
            "range_ignored_during_transfer" => "Server ignored Range: restarted as a full download",
            "empty_resource" => "Empty resource",
            other => other,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: u64,
    pub url: String,
    pub file_name: String,
    pub status: Status,
    pub downloaded: u64,
    pub total: Option<u64>,
    pub connections: usize,
    pub error: Option<String>,
    #[serde(default)]
    pub verification: Verification,
    #[serde(default)]
    pub transfer: TransferInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Queue {
    version: u32,
    next_id: u64,
    #[serde(default)]
    pub held: bool,
    pub jobs: Vec<Job>,
}

impl Default for Queue {
    fn default() -> Self {
        Self {
            version: 1,
            next_id: 1,
            held: false,
            jobs: Vec::new(),
        }
    }
}

impl Queue {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let mut queue: Self = serde_json::from_slice(&fs::read(path)?)
            .with_context(|| format!("Invalid queue file: {} (left untouched)", path.display()))?;
        if queue.version != 1 {
            bail!("Unsupported queue version: {}", queue.version);
        }
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        for job in &mut queue.jobs {
            input::validate_url(&job.url)?;
            if Path::new(&job.file_name).components().count() != 1
                || !matches!(
                    Path::new(&job.file_name).components().next(),
                    Some(std::path::Component::Normal(_))
                )
                || job.file_name == ".oxget"
                || !ids.insert(job.id)
                || !names.insert(job.file_name.clone())
            {
                bail!("Invalid job in queue file");
            }
            if let Some(hash) = &job.verification.expected_sha256 {
                crate::checksum::normalize(hash)?;
            }
            if job.status.transferring() {
                job.status = Status::Queued;
            }
        }
        if queue.jobs.iter().any(|job| job.id >= queue.next_id) {
            bail!("Invalid next job ID");
        }
        Ok(queue)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        atomic_json(path, self)
    }

    pub fn add(&mut self, urls: Vec<String>, output: &Path) -> usize {
        let mut added = 0;
        for url in urls {
            if self.jobs.iter().any(|j| j.url == url) {
                continue;
            }
            let base = input::file_name(&url);
            let mut name = base.clone();
            let mut n = 1;
            while name == ".oxget"
                || output.join(&name).symlink_metadata().is_ok()
                || self.jobs.iter().any(|j| j.file_name == name)
            {
                name = input::numbered_name(&base, n);
                n += 1;
            }
            self.jobs.push(Job {
                id: self.next_id,
                url,
                file_name: name,
                status: Status::Queued,
                downloaded: 0,
                total: None,
                connections: 0,
                error: None,
                verification: Verification::default(),
                transfer: TransferInfo::default(),
            });
            self.next_id += 1;
            added += 1;
        }
        added
    }

    pub fn move_job(&mut self, index: usize, offset: isize) -> usize {
        let target = index.saturating_add_signed(offset);
        if index < self.jobs.len() && target < self.jobs.len() {
            self.jobs.swap(index, target);
            target
        } else {
            index
        }
    }
}

pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .context("State file needs a parent directory")?;
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temp, value)?;
    temp.write_all(b"\n")?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub struct Store {
    pub root: PathBuf,
    pub queue_path: PathBuf,
    _lock: File,
}

impl Store {
    pub fn open(output: &Path) -> Result<Self> {
        Self::try_open(output)?.context("Another oxget is using this output directory")
    }

    /// A busy lock is distinct from a permissions or filesystem failure.
    pub fn try_open(output: &Path) -> Result<Option<Self>> {
        let root = output.join(".oxget");
        fs::create_dir_all(root.join("parts"))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("lock"))?;
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        Ok(Some(Self {
            queue_path: root.join("queue.json"),
            root,
            _lock: lock,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_persists_order_pause_and_avoids_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.zip"), "existing").unwrap();
        let mut q = Queue::default();
        assert_eq!(
            q.add(
                vec![
                    "https://one.example/a.zip".into(),
                    "https://two.example/a.zip".into(),
                    "https://one.example/a.zip".into()
                ],
                dir.path()
            ),
            2
        );
        assert_eq!(q.jobs[0].file_name, "a.1.zip");
        assert_eq!(q.jobs[1].file_name, "a.2.zip");
        q.jobs[0].status = Status::Paused;
        q.jobs[1].status = Status::Downloading;
        q.move_job(1, -1);
        let path = dir.path().join("queue.json");
        q.save(&path).unwrap();
        let restored = Queue::load(&path).unwrap();
        assert_eq!(restored.jobs[0].status, Status::Queued);
        assert_eq!(restored.jobs[1].status, Status::Paused);
        assert_eq!(restored.jobs[0].file_name, "a.2.zip");
    }

    #[test]
    fn store_excludes_concurrent_writers() {
        let dir = tempfile::tempdir().unwrap();
        let first = Store::open(dir.path()).unwrap();
        assert!(Store::open(dir.path()).is_err());
        drop(first);
        assert!(Store::open(dir.path()).is_ok());
    }
}
