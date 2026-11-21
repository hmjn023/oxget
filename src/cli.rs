use std::{
    io::{IsTerminal, Read},
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    time::Duration,
};

use clap::{Args, Parser, Subcommand, error::ErrorKind};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::{
    app::App,
    control::{Action, Change, Failure, Placement, Reply},
    download::Options,
    input, ipc,
    model::{Queue, Status, Store},
};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Interactive, resumable HTTP download queue",
    subcommand_precedence_over_arg = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    /// HTTP(S) URLs or list files (legacy download-and-run mode)
    #[arg(value_name = "URL_OR_FILE")]
    inputs: Vec<String>,
    /// Read URLs from a UTF-8 list; use - for stdin (repeatable)
    #[arg(short = 'i', long = "input-file", global = true, value_name = "FILE")]
    input_files: Vec<String>,
    /// Output directory and persistent queue
    #[arg(
        short = 'd',
        long = "directory",
        alias = "directory-prefix",
        global = true,
        default_value = "."
    )]
    directory: PathBuf,
    /// Maximum parallel HTTP requests per file
    #[arg(short = 'c', long, global = true, default_value_t = 4, value_parser = clap::value_parser!(u8).range(1..=32))]
    connections: u8,
    /// Concurrent files (takes effect when starting a worker)
    #[arg(short = 'j', long, global = true, default_value_t = 1, value_parser = clap::value_parser!(u8).range(1..=16))]
    jobs: u8,
    /// Retries per transfer
    #[arg(short = 'r', long, global = true, default_value_t = 3, value_parser = clap::value_parser!(u8).range(0..=10))]
    retries: u8,
    /// Return one versioned JSON result on stdout; logs go to stderr
    #[arg(long, global = true)]
    json: bool,
    /// Disable the terminal UI in legacy mode
    #[arg(long, global = true, conflicts_with = "paused")]
    no_tui: bool,
    /// Add new jobs paused; in legacy mode, pause the whole queue
    #[arg(long, global = true)]
    paused: bool,
    /// Expected SHA-256 for exactly one URL
    #[arg(long, global = true)]
    sha256: Option<String>,
    /// GNU SHA256SUMS file matching each requested URL
    #[arg(long, global = true)]
    sha256sums: Option<PathBuf>,
    /// Stop the worker after all jobs finish or fail (including hash verification)
    #[arg(long, global = true)]
    exit_when_done: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Enqueue URLs/list files without starting a worker
    Add {
        #[arg(value_name = "URL_OR_FILE")]
        inputs: Vec<String>,
    },
    /// List jobs, optionally filtered by state
    List {
        #[arg(long, value_enum)]
        state: Option<Status>,
    },
    /// Show live queue or individual job progress
    Status { id: Option<u64> },
    /// Pause jobs, or hold the entire queue with --all
    Pause(Targets),
    /// Resume jobs, or release the global queue hold with --all
    Resume(Targets),
    /// Reorder a job using stable IDs
    Move {
        id: u64,
        #[command(flatten)]
        destination: Destination,
    },
    /// Requeue failed jobs
    Retry(RetryTargets),
    /// Remove jobs and partial data; completed files are kept
    Remove {
        #[arg(required = true, num_args = 1..)]
        ids: Vec<u64>,
    },
    /// Start a persistent worker; --background returns when it is ready
    Start {
        #[arg(long)]
        background: bool,
    },
    /// Flush partial files, save the queue, and stop the worker/TUI
    Stop,
    /// Wait for jobs to finish; exit 3 on timeout, 4 on download failure
    Wait {
        #[command(flatten)]
        targets: Targets,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(0..=86400))]
        timeout: u64,
    },
    #[command(name = "__worker", hide = true)]
    Worker,
}

#[derive(Debug, Args)]
struct Targets {
    #[arg(required_unless_present = "all", conflicts_with = "all", num_args = 1..)]
    ids: Vec<u64>,
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Args)]
struct RetryTargets {
    #[arg(required_unless_present = "failed", conflicts_with = "failed", num_args = 1..)]
    ids: Vec<u64>,
    #[arg(long)]
    failed: bool,
}

#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct Destination {
    #[arg(long)]
    before: Option<u64>,
    #[arg(long)]
    after: Option<u64>,
    #[arg(long)]
    first: bool,
    #[arg(long)]
    last: bool,
}

impl Destination {
    fn placement(&self) -> Placement {
        if let Some(id) = self.before {
            Placement::Before(id)
        } else if let Some(id) = self.after {
            Placement::After(id)
        } else if self.first {
            Placement::First
        } else {
            Placement::Last
        }
    }
}

impl Cli {
    fn options(&self) -> Options {
        Options {
            connections: self.connections as usize,
            retries: self.retries as usize,
        }
    }
}

pub async fn main_entry() -> i32 {
    let raw: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let json = raw.iter().any(|arg| arg == "--json");
    let args = match Cli::try_parse_from(&raw) {
        Ok(args) => args,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            let _ = error.print();
            return 0;
        }
        Err(error) => {
            let reply = Reply::failure(Failure::new("invalid_arguments", error.to_string()));
            print_reply(&reply, json);
            return reply.exit_code();
        }
    };
    let reply = match run(&args).await {
        Ok(reply) => reply,
        Err(error) => Reply::failure(error),
    };
    if !matches!(args.command, Some(Command::Worker)) {
        print_reply(&reply, args.json);
    } else if let Some(error) = &reply.error {
        let path = args.directory.join(".oxget/worker.log");
        let _ = crate::logging::WorkerLog::new(path)
            .write(&format!("worker_error code={} {error}", error.code));
    }
    reply.exit_code()
}

fn print_reply(reply: &Reply, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string(reply).expect("JSON result is serializable")
        );
        return;
    }
    if let Some(error) = &reply.error {
        eprintln!("oxget [{}]: {}", error.code, error.message);
    }
    if reply.data.is_null() {
        return;
    }
    let running = reply.data["running"].as_bool().unwrap_or(false);
    let held = reply.data["queue_paused"].as_bool().unwrap_or(false);
    println!(
        "Worker: {}{}",
        if running { "running" } else { "stopped" },
        if held { " · queue paused" } else { "" }
    );
    if let Some(overall) = reply.data.get("overall") {
        let percent = overall["percent"]
            .as_f64()
            .map_or_else(|| "unknown".into(), |n| format!("{n:.1}%"));
        let eta = overall["eta_seconds"]
            .as_u64()
            .map_or_else(|| "unknown".into(), |n| format!("{n}s"));
        println!(
            "Overall: {}/{} files · {percent} · ETA {eta}",
            overall["files_completed"], overall["files_total"]
        );
    }
    if let Some(ids) = reply.data["ids"].as_array() {
        println!(
            "IDs: {}",
            ids.iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if let Some(jobs) = reply.data["jobs"].as_array() {
        println!("ID\tSTATE\tPOSITION\tDOWNLOADED\tFILE");
        for job in jobs {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                job["id"],
                job["state"].as_str().unwrap_or("?"),
                job["position"],
                crate::ui::bytes(job["downloaded_bytes"].as_u64().unwrap_or(0)),
                job["file_name"].as_str().unwrap_or("?")
            );
            if let Some(note) = job["transfer_note"].as_str() {
                println!("  {note}");
            }
            if let Some(hash) = job["verification"]["expected_sha256"].as_str() {
                println!(
                    "  SHA-256 expected={hash} actual={} result={}",
                    job["verification"]["actual_sha256"]
                        .as_str()
                        .unwrap_or("unknown"),
                    job["verification"]["result"]
                );
            }
        }
    }
}

async fn run(args: &Cli) -> crate::control::Result<Reply> {
    if args.command.is_some() && !args.inputs.is_empty() {
        return Err(Failure::new(
            "invalid_arguments",
            "Place URLs after add, or omit the subcommand for legacy mode",
        ));
    }
    if !args.input_files.is_empty() && !matches!(args.command, None | Some(Command::Add { .. })) {
        return Err(Failure::new(
            "invalid_arguments",
            "--input-file is supported by add and legacy mode",
        ));
    }
    if args.paused && !matches!(args.command, None | Some(Command::Add { .. })) {
        return Err(Failure::new(
            "invalid_arguments",
            "--paused is supported by add and legacy mode",
        ));
    }
    if (args.sha256.is_some() || args.sha256sums.is_some())
        && !matches!(args.command, None | Some(Command::Add { .. }))
    {
        return Err(Failure::new(
            "invalid_arguments",
            "Checksums are supported by add and legacy mode",
        ));
    }
    if args.exit_when_done && !matches!(args.command, Some(Command::Start { .. } | Command::Worker))
    {
        return Err(Failure::new(
            "invalid_arguments",
            "--exit-when-done requires start",
        ));
    }
    let urls = match &args.command {
        Some(Command::Add { inputs }) => Some(sources(inputs, &args.input_files)?),
        None => Some(sources(&args.inputs, &args.input_files)?),
        _ => None,
    };
    let hashes = crate::checksum::for_urls(
        urls.as_deref().unwrap_or_default(),
        args.sha256.as_deref(),
        args.sha256sums.as_deref(),
    )
    .map_err(|error| Failure::new("invalid_arguments", error.to_string()))?;
    std::fs::create_dir_all(&args.directory).map_err(Failure::io)?;
    let output = std::fs::canonicalize(&args.directory).map_err(Failure::io)?;
    let action = match &args.command {
        Some(Command::Add { .. }) => Action::Add {
            urls: urls.unwrap_or_default(),
            paused: args.paused,
            sha256: hashes,
        },
        Some(Command::List { state }) => Action::List { state: *state },
        Some(Command::Status { id }) => Action::Status { id: *id },
        Some(Command::Pause(targets)) => Action::Pause {
            ids: targets.ids.clone(),
            all: targets.all,
        },
        Some(Command::Resume(targets)) => Action::Resume {
            ids: targets.ids.clone(),
            all: targets.all,
        },
        Some(Command::Move { id, destination }) => Action::Move {
            id: *id,
            placement: destination.placement(),
        },
        Some(Command::Retry(targets)) => Action::Retry {
            ids: targets.ids.clone(),
            failed: targets.failed,
        },
        Some(Command::Remove { ids }) => Action::Remove { ids: ids.clone() },
        Some(Command::Stop) => {
            let reply = execute(&output, &Action::Stop, args).await?;
            if reply.ok {
                wait_for_unlock(&output).await?;
            }
            return Ok(reply);
        }
        Some(Command::Start { background: true }) => return background(&output, args).await,
        Some(Command::Start { background: false }) => {
            if args.json {
                return Err(Failure::new(
                    "invalid_arguments",
                    "Use start --background --json to get a readiness response",
                ));
            }
            return worker(&output, args).await;
        }
        Some(Command::Worker) => return worker(&output, args).await,
        Some(Command::Wait { targets, timeout }) => {
            return wait(&output, &targets.ids, targets.all, *timeout, args).await;
        }
        None => return legacy(&output, urls.unwrap_or_default(), args).await,
    };
    execute(&output, &action, args).await
}

fn sources(inputs: &[String], files: &[String]) -> crate::control::Result<Vec<String>> {
    let mut sources = inputs.to_vec();
    sources.extend_from_slice(files);
    let mut urls = Vec::new();
    let mut stdin = None;
    for source in sources {
        if source == "-" {
            if stdin.is_none() {
                let mut text = String::new();
                std::io::stdin()
                    .read_to_string(&mut text)
                    .map_err(Failure::io)?;
                stdin =
                    Some(input::parse_list(&text, "stdin").map_err(|error| {
                        Failure::new("invalid_arguments", format!("{error:#}"))
                    })?);
            }
            urls.extend(stdin.as_ref().expect("read above").iter().cloned());
        } else {
            urls.extend(
                input::expand(&[source])
                    .map_err(|error| Failure::new("invalid_arguments", format!("{error:#}")))?,
            );
        }
    }
    Ok(urls)
}

fn open_app(output: &Path, store: Store, args: &Cli) -> crate::control::Result<App> {
    let queue = Queue::load(&store.queue_path).map_err(Failure::io)?;
    App::new(
        queue,
        store,
        output.into(),
        args.jobs as usize,
        args.options(),
    )
    .map_err(Failure::io)
}

async fn execute(output: &Path, action: &Action, args: &Cli) -> crate::control::Result<Reply> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match Store::try_open(output).map_err(Failure::io)? {
            Some(store) => {
                let mut app = open_app(output, store, args)?;
                return Ok(Reply::from_result(app.command(action).await));
            }
            None => {
                let root = output.join(".oxget");
                if root.join("worker.json").exists() {
                    match ipc::request(&root, action).await {
                        Ok(reply) => return Ok(reply),
                        Err(_) if !action.mutates() && Instant::now() < deadline => {
                            // Read-only requests may race automatic worker shutdown.
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            continue;
                        }
                        Err(error) => {
                            return Err(Failure::new("worker_unavailable", format!("{error:#}")));
                        }
                    }
                }
                if Instant::now() >= deadline {
                    return Err(Failure::new(
                        "busy",
                        "Queue is locked but its worker endpoint is not ready",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

async fn wait_for_unlock(output: &Path) -> crate::control::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Store::try_open(output).map_err(Failure::io)?.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Failure::new(
                "timeout",
                "Queue was saved, but the worker has not released its lock",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn legacy(output: &Path, urls: Vec<String>, args: &Cli) -> crate::control::Result<Reply> {
    let hashes =
        crate::checksum::for_urls(&urls, args.sha256.as_deref(), args.sha256sums.as_deref())
            .map_err(|error| Failure::new("invalid_arguments", error.to_string()))?;
    let Some(store) = Store::try_open(output).map_err(Failure::io)? else {
        let action = if urls.is_empty() {
            Action::Status { id: None }
        } else {
            Action::Add {
                urls,
                paused: args.paused,
                sha256: hashes,
            }
        };
        return execute(output, &action, args).await;
    };
    let interactive = !args.no_tui
        && !args.json
        && std::io::stdin().is_terminal()
        && std::io::stdout().is_terminal();
    if args.paused && !interactive {
        return Err(Failure::new(
            "invalid_arguments",
            "--paused requires a terminal in legacy mode; use add --paused for noninteractive enqueueing",
        ));
    }
    let mut app = open_app(output, store, args)?;
    if !urls.is_empty() {
        app.command(&Action::Add {
            urls,
            paused: false,
            sha256: hashes,
        })
        .await?;
    }
    if args.paused {
        app.queue.held = true;
    }
    app.save().map_err(Failure::io)?;
    app.run(interactive).await.map_err(Failure::io)?;
    let data = app.snapshot(&Action::Status { id: None }, &Change::default());
    let failed = app
        .queue
        .jobs
        .iter()
        .filter(|job| job.status.failed())
        .count();
    if failed > 0 {
        let mut reply = Reply::failure(Failure::new(
            if app
                .queue
                .jobs
                .iter()
                .any(|job| job.status == Status::VerificationFailed)
            {
                "verification_failed"
            } else {
                "download_failed"
            },
            format!("{failed} download(s) failed; use retry --failed"),
        ));
        reply.data = data;
        return Ok(reply);
    }
    Ok(Reply::success(data))
}

async fn worker(output: &Path, args: &Cli) -> crate::control::Result<Reply> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let store = loop {
        if let Some(store) = Store::try_open(output).map_err(Failure::io)? {
            break store;
        }
        // A short-lived offline CLI can hold the lock too. Its status response
        // is not evidence that another worker has started.
        let status = execute(output, &Action::Status { id: None }, args).await?;
        if !status.ok || status.data["running"] == true {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(Failure::new(
                "busy",
                "Could not acquire the queue lock while starting the worker",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut app = open_app(output, store, args)?;
    app.exit_when_done = args.exit_when_done;
    app.require_ready = args.exit_when_done && matches!(args.command, Some(Command::Worker));
    if matches!(args.command, Some(Command::Worker)) {
        app.worker_log = Some(crate::logging::WorkerLog::new(
            output.join(".oxget/worker.log"),
        ));
    }
    app.run_worker().await.map_err(Failure::io)?;
    if args.exit_when_done && app.queue.jobs.iter().any(|job| job.status.failed()) {
        let code = if app
            .queue
            .jobs
            .iter()
            .any(|job| job.status == Status::VerificationFailed)
        {
            "verification_failed"
        } else {
            "download_failed"
        };
        let mut reply = Reply::failure(Failure::new(code, "Worker finished with failed jobs"));
        reply.data = app.snapshot(&Action::Status { id: None }, &Change::default());
        return Ok(reply);
    }
    Ok(Reply::success(app.snapshot(
        &Action::Status { id: None },
        &Change::default(),
    )))
}

async fn background(output: &Path, args: &Cli) -> crate::control::Result<Reply> {
    let status = execute(output, &Action::Status { id: None }, args).await?;
    if status.ok && status.data["running"] == true {
        return execute(output, &Action::Ready, args).await;
    }
    if !status.ok {
        return Ok(status);
    }
    let log_path = output.join(".oxget").join("worker.log");
    let mut command = ProcessCommand::new(std::env::current_exe().map_err(Failure::io)?);
    command
        .arg("__worker")
        .arg("-d")
        .arg(output)
        .arg("-c")
        .arg(args.connections.to_string())
        .arg("-j")
        .arg(args.jobs.to_string())
        .arg("-r")
        .arg(args.retries.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if args.exit_when_done {
        command.arg("--exit-when-done");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x00000008 | 0x00000200);
    }
    let mut child = command.spawn().map_err(Failure::io)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Ok(mut status)) =
            tokio::time::timeout_at(deadline, execute(output, &Action::Ready, args)).await
            && status.ok
            && status.data["running"] == true
        {
            status.data["log_path"] = json!(log_path);
            return Ok(status);
        }
        if let Some(status) = child.try_wait().map_err(Failure::io)? {
            return Err(Failure::new(
                "worker_unavailable",
                format!("Worker exited with {status}; see {}", log_path.display()),
            ));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(Failure::new(
                "timeout",
                format!("Worker startup timed out; see {}", log_path.display()),
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait(
    output: &Path,
    ids: &[u64],
    all: bool,
    timeout: u64,
    args: &Cli,
) -> crate::control::Result<Reply> {
    let deadline = Instant::now() + Duration::from_secs(timeout);
    let mut targets = ids.to_vec();
    let mut first = true;
    let mut last_data = Value::Null;
    loop {
        // Queries may wait behind a flushing transfer; they still honor wait's deadline.
        // timeout=0 checks state once, with a bounded one-second IPC query.
        let query_deadline = if timeout == 0 {
            Instant::now() + Duration::from_secs(1)
        } else {
            deadline
        };
        let mut reply = match tokio::time::timeout_at(
            query_deadline,
            execute(output, &Action::Status { id: None }, args),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                let mut reply =
                    Reply::failure(Failure::new("timeout", "Timed out waiting for downloads"));
                reply.data = last_data;
                return Ok(reply);
            }
        };
        if !reply.ok {
            return Ok(reply);
        }
        let jobs = reply.data["jobs"].as_array().expect("snapshot has jobs");
        if first && all {
            targets = jobs.iter().filter_map(|job| job["id"].as_u64()).collect();
        }
        first = false;
        for id in &targets {
            if !jobs.iter().any(|job| job["id"] == *id) {
                return Err(Failure::new(
                    "not_found",
                    format!("Job {id} does not exist"),
                ));
            }
        }
        let jobs: Vec<Value> = jobs
            .iter()
            .filter(|job| job["id"].as_u64().is_some_and(|id| targets.contains(&id)))
            .cloned()
            .collect();
        let verification_failed = jobs.iter().any(|job| job["state"] == "verification_failed");
        let failed = jobs.iter().any(|job| job["state"] == "failed");
        let done = jobs.iter().all(|job| job["state"] == "done");
        reply.data["jobs"] = json!(jobs);
        last_data = reply.data.clone();
        if done {
            return Ok(reply);
        }
        let failure = if verification_failed {
            Some(Failure::new(
                "verification_failed",
                "A requested download failed SHA-256 verification",
            ))
        } else if failed {
            Some(Failure::new(
                "download_failed",
                "A requested download failed",
            ))
        } else if Instant::now() >= deadline {
            Some(Failure::new("timeout", "Timed out waiting for downloads"))
        } else {
            None
        };
        if let Some(error) = failure {
            reply.ok = false;
            reply.error = Some(error);
            return Ok(reply);
        }
        tokio::time::sleep(
            Duration::from_millis(200).min(deadline.saturating_duration_since(Instant::now())),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_and_subcommands_parse_global_flags_in_either_position() {
        for args in [
            vec![
                "oxget",
                "-d",
                "downloads",
                "add",
                "https://example.org/a",
                "--json",
            ],
            vec![
                "oxget",
                "add",
                "https://example.org/a",
                "-d",
                "downloads",
                "--json",
            ],
            vec![
                "oxget",
                "https://example.org/a",
                "-d",
                "downloads",
                "--json",
            ],
            vec!["oxget", "move", "12", "--before", "7", "--json"],
            vec!["oxget", "wait", "--all", "--timeout", "0", "--json"],
        ] {
            Cli::try_parse_from(args).unwrap();
        }
        for args in [
            vec!["oxget", "pause"],
            vec!["oxget", "pause", "1", "--all"],
            vec!["oxget", "move", "1"],
            vec!["oxget", "move", "1", "--first", "--last"],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
    }
}
