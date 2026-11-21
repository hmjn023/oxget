use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use crossterm::{
    event::{
        DisableBracketedPaste, EnableBracketedPaste, Event, EventStream, KeyCode, KeyEvent,
        KeyEventKind, KeyModifiers,
    },
    execute,
};
use futures_util::StreamExt;
use ratatui::widgets::TableState;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    control::{self, Action, Change, Failure, Placement, Reply},
    download::{self, Options, Outcome, Progress},
    input, ipc,
    model::{Queue, Status, Store},
    ui,
};

pub struct Active {
    cancel: CancellationToken,
    progress: Arc<Progress>,
    pub speed: f64,
    last_bytes: u64,
    last_sample: Instant,
}

pub struct App {
    pub queue: Queue,
    pub store: Store,
    pub output: PathBuf,
    pub parallel: usize,
    pub options: Options,
    pub table: TableState,
    pub input: Option<String>,
    pub message: String,
    pub active: HashMap<u64, Active>,
    client: reqwest::Client,
    tasks: JoinSet<(u64, Result<Outcome>)>,
    server: Option<ipc::Server>,
    running: bool,
    stopping: bool,
    pub exit_when_done: bool,
    pub worker_log: Option<crate::logging::WorkerLog>,
    pub require_ready: bool,
    ready: bool,
}

impl App {
    pub fn new(
        queue: Queue,
        store: Store,
        output: PathBuf,
        parallel: usize,
        options: Options,
    ) -> Result<Self> {
        let selected = (!queue.jobs.is_empty()).then_some(0);
        Ok(Self {
            queue,
            store,
            output,
            parallel,
            options,
            table: TableState::default().with_selected(selected),
            input: None,
            message: "Ready — press a to add a URL or list file".into(),
            active: HashMap::new(),
            client: download::client()?,
            tasks: JoinSet::new(),
            server: None,
            running: false,
            stopping: false,
            exit_when_done: false,
            worker_log: None,
            require_ready: false,
            ready: false,
        })
    }

    pub fn save(&self) -> Result<()> {
        self.queue.save(&self.store.queue_path)
    }

    pub fn overall(&self) -> serde_json::Value {
        let jobs = &self.queue.jobs;
        let downloaded: u64 = jobs
            .iter()
            .fold(0u64, |sum, j| sum.saturating_add(j.downloaded));
        let total = jobs
            .iter()
            .try_fold(0u64, |sum, j| sum.checked_add(j.total?));
        let completed = jobs.iter().filter(|j| j.status == Status::Done).count();
        let speed: f64 = jobs
            .iter()
            .filter(|j| j.status == Status::Downloading)
            .filter_map(|j| self.active.get(&j.id))
            .map(|a| a.speed)
            .sum();
        let percent = total.map(|total| {
            if total == 0 {
                if completed == jobs.len() { 100.0 } else { 0.0 }
            } else {
                (downloaded as f64 / total as f64 * 100.0).min(100.0)
            }
        });
        let eta = if completed == jobs.len() {
            Some(0)
        } else if !self.queue.held
            && speed > 0.0
            && jobs.iter().all(|j| {
                matches!(
                    j.status,
                    Status::Done | Status::Queued | Status::Downloading
                )
            })
        {
            total
                .filter(|total| *total > downloaded)
                .map(|total| ((total - downloaded) as f64 / speed).ceil() as u64)
        } else {
            None
        };
        serde_json::json!({"files_total": jobs.len(), "files_completed": completed,
            "files_failed": jobs.iter().filter(|j| j.status.failed()).count(),
            "files_verifying": jobs.iter().filter(|j| j.status == Status::Verifying).count(),
            "downloaded_bytes": downloaded, "total_bytes": total, "percent": percent,
            "speed_bytes_per_second": speed, "eta_seconds": eta})
    }

    fn record(&self, event: &str) -> Result<()> {
        if let Some(log) = &self.worker_log {
            log.write(event)?;
        }
        Ok(())
    }

    pub fn snapshot(&self, action: &Action, change: &Change) -> serde_json::Value {
        let running = self.running && !self.stopping;
        let jobs: Vec<serde_json::Value> = self.queue.jobs.iter().enumerate().filter(|(_, job)| {
            match action {
                Action::List { state } => state.is_none_or(|state| state == job.status),
                Action::Status { id } => id.is_none_or(|id| id == job.id),
                Action::Stop | Action::Ready => true,
                _ => change.ids.contains(&job.id),
            }
        }).map(|(index, job)| {
            let speed = if job.status == Status::Downloading {
                self.active.get(&job.id).map_or(0.0, |active| active.speed)
            } else { 0.0 };
            let eta = job.total.filter(|_| speed > 0.0).map(|total| (total.saturating_sub(job.downloaded) as f64 / speed).ceil() as u64);
            serde_json::json!({
                "id": job.id, "position": index + 1, "url": job.url, "file_name": job.file_name,
                "path": self.output.join(&job.file_name), "state": job.status.json_name(),
                "downloaded_bytes": job.downloaded, "total_bytes": job.total,
                "speed_bytes_per_second": speed, "eta_seconds": eta,
                "connections": job.connections, "error": job.error,
                "verification": job.verification, "resume_supported": job.transfer.resume_supported,
                "transfer_reason": job.transfer.reason, "transfer_note": job.transfer.description(),
            })
        }).collect();
        let mut result = serde_json::json!({
            "running": running, "pid": if running { Some(std::process::id()) } else { None },
            "directory": self.output, "queue_paused": self.queue.held, "jobs": jobs,
            "overall": self.overall(), "exit_when_done": self.exit_when_done,
        });
        if action.mutates() {
            result.as_object_mut().expect("object").extend(
                control::effect_json(change)
                    .as_object()
                    .expect("object")
                    .clone(),
            );
        }
        result
    }

    /// The event loop owns both the queue and transfers. CLI and keys use this same path.
    pub async fn command(&mut self, action: &Action) -> control::Result<serde_json::Value> {
        self.sample();
        if matches!(action, Action::Ready) {
            self.ready = true;
        }
        if matches!(action, Action::Stop) {
            self.stopping = true;
            self.shutdown().await.map_err(Failure::io)?;
            return Ok(self.snapshot(action, &Change::default()));
        }
        let selected = self
            .table
            .selected()
            .and_then(|i| self.queue.jobs.get(i))
            .map(|job| job.id);
        let mut next = self.queue.clone();
        let change = control::mutate(&mut next, &self.output, action)?;
        if action.mutates() {
            next.save(&self.store.queue_path).map_err(Failure::io)?;
            self.queue = next;
            for id in &change.cancel {
                if let Some(active) = self.active.get(id) {
                    active.cancel.cancel();
                }
            }
            // A pause/remove acknowledgment means the partial files have been flushed.
            while change.cancel.iter().any(|id| self.active.contains_key(id)) {
                if let Some(result) = self.tasks.join_next().await {
                    let (id, result) = result.map_err(Failure::io)?;
                    self.finish(id, result).map_err(Failure::io)?;
                } else {
                    break;
                }
            }
            for id in change.remove.iter().chain(change.discard.iter()) {
                match tokio::fs::remove_dir_all(self.store.root.join("parts").join(id.to_string()))
                    .await
                {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(Failure::io(error)),
                }
            }
            let index = selected
                .and_then(|id| self.queue.jobs.iter().position(|job| job.id == id))
                .or_else(|| {
                    (!self.queue.jobs.is_empty()).then(|| {
                        self.table
                            .selected()
                            .unwrap_or(0)
                            .min(self.queue.jobs.len() - 1)
                    })
                });
            self.table.select(index);
            self.save().map_err(Failure::io)?;
            self.record(&format!(
                "command={} ids={:?}",
                serde_json::to_value(action).expect("action")["command"],
                change.ids
            ))
            .map_err(Failure::io)?;
        }
        Ok(self.snapshot(action, &change))
    }

    async fn accept_call(&mut self, call: ipc::Call) {
        let reply = Reply::from_result(self.command(&call.action).await);
        let _ = call.response.send(reply);
    }

    pub fn schedule(&mut self) -> Result<()> {
        if self.queue.held {
            return Ok(());
        }
        let mut changed = false;
        for job in &mut self.queue.jobs {
            if self.active.len() >= self.parallel {
                break;
            }
            if job.status != Status::Queued || self.active.contains_key(&job.id) {
                continue;
            }
            job.status = Status::Downloading;
            job.error = None;
            job.verification.actual_sha256 = None;
            job.verification.result = None;
            let (id, client, job, output, options) = (
                job.id,
                self.client.clone(),
                job.clone(),
                self.output.clone(),
                self.options,
            );
            let progress = Arc::new(Progress::default());
            *progress.verification.lock().expect("verification lock") = job.verification.clone();
            let cancel = CancellationToken::new();
            self.active.insert(
                id,
                Active {
                    cancel: cancel.clone(),
                    progress: progress.clone(),
                    speed: 0.0,
                    last_bytes: 0,
                    last_sample: Instant::now(),
                },
            );
            self.tasks.spawn(async move {
                (
                    id,
                    download::download(client, job, output, options, progress, cancel).await,
                )
            });
            changed = true;
        }
        if changed {
            self.save()?;
            self.record("transfers_started")?;
        }
        Ok(())
    }

    pub fn sample(&mut self) {
        for job in &mut self.queue.jobs {
            let Some(active) = self.active.get_mut(&job.id) else {
                continue;
            };
            let (bytes, total, connections) = active.progress.snapshot();
            let elapsed = active.last_sample.elapsed().as_secs_f64();
            if elapsed >= 0.5 {
                let speed = bytes.saturating_sub(active.last_bytes) as f64 / elapsed;
                active.speed = if active.speed == 0.0 {
                    speed
                } else {
                    active.speed * 0.35 + speed * 0.65
                };
                if active.cancel.is_cancelled() {
                    active.speed = 0.0;
                }
                active.last_bytes = bytes;
                active.last_sample = Instant::now();
            }
            job.downloaded = bytes;
            job.total = total;
            job.connections = connections;
            job.transfer = active
                .progress
                .transfer
                .lock()
                .expect("transfer lock")
                .clone();
            job.verification = active
                .progress
                .verification
                .lock()
                .expect("verification lock")
                .clone();
            if job.status.transferring() {
                job.status = if active
                    .progress
                    .verifying
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    Status::Verifying
                } else {
                    Status::Downloading
                };
            }
        }
    }

    fn finish(&mut self, id: u64, result: Result<Outcome>) -> Result<()> {
        self.sample();
        self.active.remove(&id);
        if let Some(job) = self.queue.jobs.iter_mut().find(|job| job.id == id) {
            match result {
                Ok(Outcome::Done) => {
                    job.status = Status::Done;
                    job.error = None;
                    self.message = format!("Completed: {}", job.file_name);
                }
                Ok(Outcome::Paused) => {
                    // Keep a queued state if the user already requested resume or quit.
                    if job.status.transferring() {
                        job.status = Status::Paused;
                    }
                }
                Ok(Outcome::VerificationFailed) => {
                    job.status = Status::VerificationFailed;
                    job.error = Some("SHA-256 mismatch".into());
                    self.message = format!("Verification failed: {}", job.file_name);
                }
                Err(error) => {
                    job.status = Status::Failed;
                    job.error = Some(format!("{error:#}"));
                    self.message = format!("Failed: {} — {error:#}", job.file_name);
                }
            }
        }
        if let Some(job) = self.queue.jobs.iter().find(|j| j.id == id) {
            self.record(&format!(
                "job={id} state={} error={:?}",
                job.status.json_name(),
                job.error
            ))?;
        }
        self.save()
    }

    pub fn select(&mut self, delta: isize) {
        if self.queue.jobs.is_empty() {
            self.table.select(None);
            return;
        }
        let current = self.table.selected().unwrap_or(0);
        self.table.select(Some(
            current
                .saturating_add_signed(delta)
                .min(self.queue.jobs.len() - 1),
        ));
    }

    pub async fn toggle_selected(&mut self) -> Result<()> {
        let Some(job) = self.table.selected().and_then(|i| self.queue.jobs.get(i)) else {
            return Ok(());
        };
        let ids = vec![job.id];
        let action = match job.status {
            Status::Downloading | Status::Verifying | Status::Queued => {
                Action::Pause { ids, all: false }
            }
            Status::Paused => Action::Resume { ids, all: false },
            Status::Failed | Status::VerificationFailed => Action::Retry { ids, failed: false },
            Status::Done => return Ok(()),
        };
        self.command(&action).await?;
        Ok(())
    }

    pub async fn toggle_all(&mut self) -> Result<()> {
        let action = if self.queue.held {
            Action::Resume {
                ids: vec![],
                all: true,
            }
        } else {
            Action::Pause {
                ids: vec![],
                all: true,
            }
        };
        self.command(&action).await?;
        Ok(())
    }

    async fn key(&mut self, key: KeyEvent) -> Result<bool> {
        if key.kind == KeyEventKind::Release {
            return Ok(false);
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return Ok(true);
        }
        if let Some(value) = &mut self.input {
            match key.code {
                KeyCode::Esc => {
                    self.input = None;
                }
                KeyCode::Backspace => {
                    value.pop();
                }
                KeyCode::Enter => {
                    let value = self.input.take().unwrap_or_default();
                    match input::expand(&[value.trim().to_owned()]) {
                        Ok(urls) => {
                            let result = self
                                .command(&Action::Add {
                                    sha256: Default::default(),
                                    urls,
                                    paused: false,
                                })
                                .await?;
                            self.message = format!(
                                "Added {} download(s)",
                                result["added_ids"].as_array().map_or(0, Vec::len)
                            );
                        }
                        Err(error) => {
                            self.message = format!("{error:#}");
                        }
                    }
                }
                KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => value.push(c),
                _ => {}
            }
            return Ok(false);
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(true),
            KeyCode::Down | KeyCode::Char('j') => self.select(1),
            KeyCode::Up | KeyCode::Char('k') => self.select(-1),
            KeyCode::Char(' ') => self.toggle_selected().await?,
            KeyCode::Char('p' | 'P') => self.toggle_all().await?,
            KeyCode::Char('a') => {
                self.input = Some(String::new());
            }
            KeyCode::Char('K') | KeyCode::Char('J') => {
                if let Some(index) = self.table.selected() {
                    let target = if key.code == KeyCode::Char('K') {
                        index.checked_sub(1)
                    } else {
                        index.checked_add(1)
                    };
                    if let Some(other) = target.and_then(|i| self.queue.jobs.get(i)) {
                        let placement = if key.code == KeyCode::Char('K') {
                            Placement::Before(other.id)
                        } else {
                            Placement::After(other.id)
                        };
                        self.command(&Action::Move {
                            id: self.queue.jobs[index].id,
                            placement,
                        })
                        .await?;
                    }
                }
            }
            KeyCode::Char('r') => {
                if let Some(job) = self.table.selected().and_then(|i| self.queue.jobs.get(i))
                    && job.status.failed()
                {
                    self.command(&Action::Retry {
                        ids: vec![job.id],
                        failed: false,
                    })
                    .await?;
                }
            }
            _ => {}
        }
        Ok(false)
    }

    async fn shutdown(&mut self) -> Result<()> {
        for job in &mut self.queue.jobs {
            if let Some(active) = self.active.get(&job.id) {
                active.cancel.cancel();
                if job.status.transferring() {
                    job.status = Status::Queued;
                }
            }
        }
        let mut failure = None;
        while let Some(result) = self.tasks.join_next().await {
            let handled = match result {
                Ok((id, result)) => self.finish(id, result),
                Err(error) => Err(error.into()),
            };
            if let Err(error) = handled {
                failure = Some(error);
            }
        }
        self.save()?;
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(())
    }

    pub async fn run(&mut self, interactive: bool) -> Result<()> {
        self.run_mode(interactive, false).await
    }

    pub async fn run_worker(&mut self) -> Result<()> {
        self.run_mode(false, true).await
    }

    async fn run_mode(&mut self, interactive: bool, keep_alive: bool) -> Result<()> {
        self.server = Some(ipc::Server::bind(&self.store.root).await?);
        self.running = true;
        self.record("worker_started")?;
        let result = if interactive {
            self.run_tui().await
        } else {
            self.run_headless(keep_alive).await
        };
        self.stopping = true;
        let shutdown = self.shutdown().await;
        self.running = false;
        let log_result = self.record("worker_stopped");
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
        result?;
        log_result?;
        shutdown
    }

    async fn run_tui(&mut self) -> Result<()> {
        let mut terminal = ratatui::try_init()?;
        let _guard = TerminalGuard;
        execute!(std::io::stdout(), EnableBracketedPaste)?;
        let mut events = EventStream::new();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        let signal = shutdown_signal();
        tokio::pin!(signal);
        loop {
            if self.stopping {
                break;
            }
            self.schedule()?;
            self.sample();
            terminal.draw(|frame| ui::draw(frame, self))?;
            tokio::select! {
                Some(call) = next_call(&mut self.server) => self.accept_call(call).await,
                _ = &mut signal => break,
                _ = tick.tick() => {},
                result = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(result) = result { let (id, result) = result?; self.finish(id, result)?; }
                }
                event = events.next() => match event {
                    Some(Ok(Event::Key(key))) => { if self.key(key).await? { break; } },
                    Some(Ok(Event::Paste(text))) => {
                        if let Some(value) = &mut self.input { value.push_str(text.trim()); }
                    }
                    Some(Err(error)) => return Err(error.into()),
                    None => break,
                    _ => {},
                }
            }
        }
        Ok(())
    }

    async fn run_headless(&mut self, keep_alive: bool) -> Result<()> {
        if !keep_alive && self.queue.jobs.is_empty() {
            bail!("No URLs queued. Pass a URL or a URL list file.");
        }
        if !keep_alive && self.queue.held {
            bail!("Queue is globally paused. Open oxget in a terminal and press p to resume.");
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let signal = shutdown_signal();
        tokio::pin!(signal);
        loop {
            if self.stopping {
                break;
            }
            self.schedule()?;
            if (!keep_alive && self.tasks.is_empty())
                || (self.exit_when_done
                    && (!self.require_ready || self.ready)
                    && self.tasks.is_empty()
                    && self.queue.jobs.iter().all(|job| job.status.terminal()))
            {
                break;
            }
            tokio::select! {
                Some(call) = next_call(&mut self.server) => self.accept_call(call).await,
                _ = &mut signal => { eprintln!("Saving partial downloads…"); break; },
                _ = tick.tick() => {
                    self.sample();
                    for job in self.queue.jobs.iter().filter(|job| self.worker_log.is_none() && job.status == Status::Downloading) {
                        let speed = self.active.get(&job.id).map_or(0.0, |active| active.speed);
                        eprintln!("{}  {} / {}  {}/s  {} conn", job.file_name, ui::bytes(job.downloaded),
                            job.total.map(ui::bytes).unwrap_or_else(|| "?".into()), ui::bytes(speed as u64), job.connections);
                    }
                }
                result = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(result) = result {
                        let (id, result) = result?;
                        self.finish(id, result)?;
                        if self.worker_log.is_none() { eprintln!("{}", self.message); }
                    }
                }
            }
        }
        Ok(())
    }
}

async fn next_call(server: &mut Option<ipc::Server>) -> Option<ipc::Call> {
    if let Some(server) = server {
        server.receiver.recv().await
    } else {
        std::future::pending().await
    }
}

struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(std::io::stdout(), DisableBracketedPaste);
        ratatui::restore();
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overall_tracks_bytes_and_hides_eta_for_unknown_paused_or_verifying_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let mut q = Queue::default();
        q.add(
            vec![
                "https://example.org/a".into(),
                "https://example.org/b".into(),
            ],
            dir.path(),
        );
        let mut app = App::new(
            q,
            store,
            dir.path().into(),
            1,
            Options {
                connections: 1,
                retries: 0,
            },
        )
        .unwrap();
        app.queue.jobs[0].total = Some(30);
        app.queue.jobs[0].downloaded = 30;
        app.queue.jobs[0].status = Status::Done;
        assert!(app.overall()["percent"].is_null());
        assert!(app.overall()["eta_seconds"].is_null());
        app.queue.jobs[1].total = Some(10);
        app.queue.jobs[1].downloaded = 5;
        app.queue.jobs[1].status = Status::Downloading;
        app.active.insert(
            2,
            Active {
                cancel: CancellationToken::new(),
                progress: Arc::new(Progress::default()),
                speed: 1.0,
                last_bytes: 0,
                last_sample: Instant::now(),
            },
        );
        assert_eq!(app.overall()["percent"], 87.5);
        assert_eq!(app.overall()["eta_seconds"], 5);
        app.queue.jobs[1].status = Status::Paused;
        assert!(app.overall()["eta_seconds"].is_null());
        app.queue.jobs[1].status = Status::Verifying;
        assert!(app.overall()["eta_seconds"].is_null());
        app.queue.jobs[1].status = Status::Done;
        app.queue.jobs[1].downloaded = 10;
        assert_eq!(app.overall()["percent"], 100.0);
        assert_eq!(app.overall()["eta_seconds"], 0);
    }

    #[tokio::test]
    async fn pause_resume_reorder_and_global_pause_preserve_individual_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let mut queue = Queue::default();
        queue.add(
            vec![
                "https://example.org/a".into(),
                "https://example.org/b".into(),
            ],
            dir.path(),
        );
        let mut app = App::new(
            queue,
            store,
            dir.path().into(),
            1,
            Options {
                connections: 4,
                retries: 0,
            },
        )
        .unwrap();
        app.toggle_selected().await.unwrap();
        assert_eq!(app.queue.jobs[0].status, Status::Paused);
        app.toggle_all().await.unwrap();
        app.toggle_all().await.unwrap();
        assert_eq!(app.queue.jobs[0].status, Status::Paused);
        app.toggle_selected().await.unwrap();
        assert_eq!(app.queue.jobs[0].status, Status::Queued);
        app.key(KeyEvent::new(KeyCode::Char('J'), KeyModifiers::SHIFT))
            .await
            .unwrap();
        assert_eq!(app.queue.jobs[0].file_name, "b");
        assert_eq!(app.table.selected(), Some(1));
    }
}
