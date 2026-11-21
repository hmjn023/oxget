use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

/// Open for each event so rotation never leaves a writer attached to an old inode.
pub struct WorkerLog {
    path: PathBuf,
    max_bytes: u64,
    backups: usize,
}

impl WorkerLog {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            max_bytes: 1024 * 1024,
            backups: 3,
        }
    }
    pub fn write(&self, event: &str) -> std::io::Result<()> {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let line = format!("{time} {}\n", event.replace(['\r', '\n'], " "));
        if fs::metadata(&self.path)
            .is_ok_and(|m| m.len().saturating_add(line.len() as u64) > self.max_bytes)
        {
            for index in (1..=self.backups).rev() {
                let source = if index == 1 {
                    self.path.clone()
                } else {
                    self.path.with_extension(format!("log.{}", index - 1))
                };
                let target = self.path.with_extension(format!("log.{index}"));
                if source.exists() {
                    if target.exists() {
                        fs::remove_file(&target)?;
                    }
                    fs::rename(source, target)?;
                }
            }
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&self.path)?;
        // Bound individual error messages as well as the total number of backups.
        let mut end = line.len().min(self.max_bytes as usize);
        while !line.is_char_boundary(end) {
            end -= 1;
        }
        file.write_all(&line.as_bytes()[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotation_bounds_all_files_and_preserves_recent_events() {
        let dir = tempfile::tempdir().unwrap();
        let log = WorkerLog {
            path: dir.path().join("worker.log"),
            max_bytes: 128,
            backups: 3,
        };
        for i in 0..100 {
            log.write(&format!("event={i} {}", "x".repeat(50))).unwrap();
        }
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 4);
        assert!(fs::read_to_string(&log.path).unwrap().contains("event=99"));
        for entry in fs::read_dir(dir.path()).unwrap() {
            assert!(entry.unwrap().metadata().unwrap().len() <= 128);
        }
    }
}
