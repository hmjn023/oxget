//! Stable commands shared by the terminal, offline CLI, and IPC worker.
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    input,
    model::{Queue, Status},
};

pub type Result<T> = std::result::Result<T, Failure>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    pub code: String,
    pub message: String,
}

impl Failure {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn io(error: impl std::fmt::Display) -> Self {
        Self::new("io_error", error.to_string())
    }

    pub fn exit_code(&self) -> i32 {
        match self.code.as_str() {
            "invalid_arguments" => 2,
            "timeout" => 3,
            "download_failed" => 4,
            "verification_failed" => 5,
            _ => 1,
        }
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Failure {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reply {
    pub schema_version: u32,
    pub ok: bool,
    pub data: Value,
    pub error: Option<Failure>,
}

impl Reply {
    pub fn success(data: Value) -> Self {
        Self {
            schema_version: 1,
            ok: true,
            data,
            error: None,
        }
    }
    pub fn failure(error: Failure) -> Self {
        Self {
            schema_version: 1,
            ok: false,
            data: Value::Null,
            error: Some(error),
        }
    }
    pub fn from_result(result: Result<Value>) -> Self {
        match result {
            Ok(value) => Self::success(value),
            Err(error) => Self::failure(error),
        }
    }
    pub fn exit_code(&self) -> i32 {
        self.error.as_ref().map_or(0, Failure::exit_code)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Action {
    Add {
        urls: Vec<String>,
        paused: bool,
        #[serde(default)]
        sha256: BTreeMap<String, String>,
    },
    List {
        state: Option<Status>,
    },
    Status {
        id: Option<u64>,
    },
    Pause {
        ids: Vec<u64>,
        all: bool,
    },
    Resume {
        ids: Vec<u64>,
        all: bool,
    },
    Move {
        id: u64,
        placement: Placement,
    },
    Retry {
        ids: Vec<u64>,
        failed: bool,
    },
    Remove {
        ids: Vec<u64>,
    },
    Stop,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "position", content = "id", rename_all = "snake_case")]
pub enum Placement {
    Before(u64),
    After(u64),
    First,
    Last,
}

#[derive(Default)]
pub struct Change {
    pub ids: Vec<u64>,
    pub added_ids: Vec<u64>,
    pub existing_ids: Vec<u64>,
    pub cancel: Vec<u64>,
    pub remove: Vec<u64>,
    pub discard: Vec<u64>,
}

impl Action {
    pub fn mutates(&self) -> bool {
        !matches!(self, Self::List { .. } | Self::Status { .. } | Self::Ready)
    }
}

fn check_ids(queue: &Queue, ids: &[u64]) -> Result<()> {
    if ids.is_empty() {
        return Err(Failure::new(
            "invalid_arguments",
            "Specify one or more job IDs",
        ));
    }
    for id in ids {
        if !queue.jobs.iter().any(|job| job.id == *id) {
            return Err(Failure::new(
                "not_found",
                format!("Job {id} does not exist"),
            ));
        }
    }
    Ok(())
}

fn unique(ids: &[u64]) -> Vec<u64> {
    let mut seen = HashSet::new();
    ids.iter().copied().filter(|id| seen.insert(*id)).collect()
}

/// Validate the entire operation before modifying any job. Disk commit is owned by App.
pub fn mutate(queue: &mut Queue, output: &Path, action: &Action) -> Result<Change> {
    let mut change = Change::default();
    match action {
        Action::List { .. } | Action::Status { id: None } | Action::Stop | Action::Ready => {}
        Action::Status { id: Some(id) } => check_ids(queue, &[*id])?,
        Action::Add {
            urls,
            paused,
            sha256,
        } => {
            if urls.is_empty() {
                return Err(Failure::new(
                    "invalid_arguments",
                    "Provide a URL, a list file, or -i - for stdin",
                ));
            }
            let urls: Vec<String> = urls
                .iter()
                .map(|url| {
                    input::validate_url(url)
                        .map_err(|error| Failure::new("invalid_arguments", error.to_string()))
                })
                .collect::<Result<_>>()?;
            for (url, hash) in sha256 {
                crate::checksum::normalize(hash)
                    .map_err(|e| Failure::new("invalid_arguments", e.to_string()))?;
                if let Some(job) = queue.jobs.iter().find(|job| job.url == *url) {
                    if let Some(previous) = &job.verification.expected_sha256 {
                        if !previous.eq_ignore_ascii_case(hash) {
                            return Err(Failure::new(
                                "hash_conflict",
                                format!("Job {} already has a different SHA-256", job.id),
                            ));
                        }
                    } else if job.status == Status::Done || job.status.transferring() {
                        return Err(Failure::new(
                            "invalid_state",
                            format!(
                                "Cannot attach a new checksum to active or completed job {}",
                                job.id
                            ),
                        ));
                    }
                }
            }
            for url in urls {
                let expected = sha256.get(&url).map(|hash| hash.to_ascii_lowercase());
                let id = if let Some(job) = queue.jobs.iter_mut().find(|job| job.url == url) {
                    if job.verification.expected_sha256.is_none() {
                        job.verification.expected_sha256 = expected;
                    }
                    change.existing_ids.push(job.id);
                    job.id
                } else {
                    queue.add(vec![url], output);
                    let job = queue.jobs.last_mut().expect("a validated URL was added");
                    if *paused {
                        job.status = Status::Paused;
                    }
                    job.verification.expected_sha256 = expected;
                    change.added_ids.push(job.id);
                    job.id
                };
                if !change.ids.contains(&id) {
                    change.ids.push(id);
                }
            }
        }
        Action::Pause { ids, all } | Action::Resume { ids, all } => {
            let pause = matches!(action, Action::Pause { .. });
            if *all {
                if !ids.is_empty() {
                    return Err(Failure::new(
                        "invalid_arguments",
                        "Use IDs or --all, not both",
                    ));
                }
                queue.held = pause;
                change.ids = queue.jobs.iter().map(|job| job.id).collect();
                if pause {
                    for job in &mut queue.jobs {
                        if job.status.transferring() {
                            job.status = Status::Queued;
                            change.cancel.push(job.id);
                        }
                    }
                }
            } else {
                check_ids(queue, ids)?;
                change.ids = unique(ids);
                for job in &mut queue.jobs {
                    if !change.ids.contains(&job.id) {
                        continue;
                    }
                    if pause && (job.status == Status::Queued || job.status.transferring()) {
                        job.status = Status::Paused;
                        change.cancel.push(job.id);
                    } else if !pause && job.status == Status::Paused {
                        job.status = Status::Queued;
                    }
                }
            }
        }
        Action::Retry { ids, failed } => {
            if *failed {
                if !ids.is_empty() {
                    return Err(Failure::new(
                        "invalid_arguments",
                        "Use IDs or --failed, not both",
                    ));
                }
                change.ids = queue
                    .jobs
                    .iter()
                    .filter(|job| job.status.failed())
                    .map(|job| job.id)
                    .collect();
            } else {
                check_ids(queue, ids)?;
                change.ids = unique(ids);
            }
            for job in &mut queue.jobs {
                if change.ids.contains(&job.id) && job.status.failed() {
                    if job.status == Status::VerificationFailed {
                        change.discard.push(job.id);
                    }
                    job.verification.actual_sha256 = None;
                    job.verification.result = None;
                    job.status = Status::Queued;
                    job.error = None;
                }
            }
        }
        Action::Move { id, placement } => {
            check_ids(queue, &[*id])?;
            if let Placement::Before(other) | Placement::After(other) = placement {
                check_ids(queue, &[*other])?;
                if id == other {
                    change.ids.push(*id);
                    return Ok(change);
                }
            }
            let from = queue
                .jobs
                .iter()
                .position(|job| job.id == *id)
                .expect("validated ID");
            let job = queue.jobs.remove(from);
            let to = match placement {
                Placement::First => 0,
                Placement::Last => queue.jobs.len(),
                Placement::Before(other) => queue
                    .jobs
                    .iter()
                    .position(|job| job.id == *other)
                    .expect("validated ID"),
                Placement::After(other) => {
                    queue
                        .jobs
                        .iter()
                        .position(|job| job.id == *other)
                        .expect("validated ID")
                        + 1
                }
            };
            queue.jobs.insert(to, job);
            change.ids.push(*id);
        }
        Action::Remove { ids } => {
            if ids.is_empty() {
                return Err(Failure::new(
                    "invalid_arguments",
                    "Specify one or more job IDs",
                ));
            }
            change.ids = unique(ids);
            change.cancel = change.ids.clone();
            change.remove = change.ids.clone();
            queue.jobs.retain(|job| !change.ids.contains(&job.id));
        }
    }
    Ok(change)
}

pub fn effect_json(change: &Change) -> Value {
    json!({ "ids": change.ids, "added_ids": unique(&change.added_ids), "existing_ids": unique(&change.existing_ids) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_validate_batches_and_use_stable_ids() {
        let dir = tempfile::tempdir().unwrap();
        let mut queue = Queue::default();
        let add = Action::Add {
            sha256: Default::default(),
            urls: vec![
                "https://example.org/a".into(),
                "https://example.org/b".into(),
            ],
            paused: false,
        };
        let first = mutate(&mut queue, dir.path(), &add).unwrap();
        assert_eq!(first.added_ids, [1, 2]);
        assert_eq!(
            mutate(&mut queue, dir.path(), &add).unwrap().existing_ids,
            [1, 2]
        );
        assert!(
            mutate(
                &mut queue,
                dir.path(),
                &Action::Pause {
                    ids: vec![1, 999],
                    all: false
                }
            )
            .is_err()
        );
        assert_eq!(queue.jobs[0].status, Status::Queued);
        mutate(
            &mut queue,
            dir.path(),
            &Action::Move {
                id: 2,
                placement: Placement::Before(1),
            },
        )
        .unwrap();
        assert_eq!(
            queue.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
            [2, 1]
        );
        for _ in 0..2 {
            mutate(
                &mut queue,
                dir.path(),
                &Action::Pause {
                    ids: vec![1],
                    all: false,
                },
            )
            .unwrap();
        }
        assert_eq!(queue.jobs[1].status, Status::Paused);
        mutate(
            &mut queue,
            dir.path(),
            &Action::Resume {
                ids: vec![],
                all: true,
            },
        )
        .unwrap();
        assert_eq!(queue.jobs[1].status, Status::Paused);
        for _ in 0..2 {
            mutate(&mut queue, dir.path(), &Action::Remove { ids: vec![1] }).unwrap();
        }
        assert_eq!(queue.jobs.len(), 1);
    }
}
