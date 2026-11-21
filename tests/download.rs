use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use oxget::{
    download::{self, Options, Outcome, Progress},
    model::{Job, Status},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Ranges,
    IgnoreRange,
    NoEtag,
    UnknownLength,
    WrongRange,
    ProbeOnly,
    Truncated,
}

#[derive(Clone, Debug)]
struct Request {
    range: Option<(usize, usize)>,
    if_range: Option<String>,
}

struct Server {
    url: String,
    version: Arc<AtomicUsize>,
    records: Arc<Mutex<Vec<Request>>>,
    size: usize,
    task: JoinHandle<()>,
    cancel: CancellationToken,
}

impl Server {
    async fn start(mode: Mode, size: usize, slow: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/file.bin", listener.local_addr().unwrap());
        let version = Arc::new(AtomicUsize::new(1));
        let records = Arc::new(Mutex::new(Vec::new()));
        let cancel = CancellationToken::new();
        let (v, r, stop) = (version.clone(), records.clone(), cancel.clone());
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break; };
                        let (v, r) = (v.clone(), r.clone());
                        tasks.spawn(async move { serve(socket, mode, size, slow, v, r).await; });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {},
                }
            }
        });
        Self {
            url,
            version,
            records,
            size,
            task,
            cancel,
        }
    }

    fn bytes(&self) -> Vec<u8> {
        data(self.size, self.version.load(Ordering::Relaxed))
    }

    fn requests(&self) -> Vec<Request> {
        self.records.lock().unwrap().clone()
    }

    fn clear_requests(&self) {
        self.records.lock().unwrap().clear();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

fn data(size: usize, version: usize) -> Vec<u8> {
    (0..size)
        .map(|i| ((i * 17 + version * 37) % 251) as u8)
        .collect()
}

async fn serve(
    mut socket: TcpStream,
    mode: Mode,
    size: usize,
    slow: bool,
    version: Arc<AtomicUsize>,
    records: Arc<Mutex<Vec<Request>>>,
) {
    let mut input = Vec::new();
    let mut buffer = [0u8; 1024];
    while !input.windows(4).any(|w| w == b"\r\n\r\n") {
        match socket.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(n) => input.extend_from_slice(&buffer[..n]),
        }
        if input.len() > 64 * 1024 {
            return;
        }
    }
    let text = String::from_utf8(input).unwrap();
    assert!(text.starts_with("GET "));
    let headers = text.to_ascii_lowercase();
    let header = |name: &str| {
        headers
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}: ")).map(str::trim))
    };
    let range = header("range")
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .and_then(|(start, end)| Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?)));
    let request = Request {
        range,
        if_range: header("if-range").map(str::to_owned),
    };
    records.lock().unwrap().push(request.clone());
    let version = version.load(Ordering::Relaxed);
    let tag = format!("\"v{version}\"");
    let etag = if mode == Mode::NoEtag {
        String::new()
    } else {
        format!("ETag: {tag}\r\n")
    };
    let use_range = range.is_some()
        && !matches!(mode, Mode::IgnoreRange | Mode::UnknownLength)
        && (mode != Mode::ProbeOnly || range == Some((0, 0)))
        && request.if_range.as_ref().is_none_or(|value| *value == tag);
    let payload = data(size, version);
    let (status, extra, body) = if use_range {
        let (start, end) = range.unwrap();
        if start >= size || end >= size {
            (
                "416 Range Not Satisfiable",
                format!("Content-Range: bytes */{size}\r\n"),
                &payload[..0],
            )
        } else {
            let reported_start = if mode == Mode::WrongRange && (start, end) != (0, 0) {
                start + 1
            } else {
                start
            };
            (
                "206 Partial Content",
                format!("Content-Range: bytes {reported_start}-{end}/{size}\r\n"),
                &payload[start..=end],
            )
        }
    } else {
        ("200 OK", String::new(), payload.as_slice())
    };
    let length = if mode == Mode::UnknownLength {
        String::new()
    } else {
        format!("Content-Length: {}\r\n", body.len())
    };
    let response = format!("HTTP/1.1 {status}\r\nConnection: close\r\n{length}{etag}{extra}\r\n");
    if socket.write_all(response.as_bytes()).await.is_err() {
        return;
    }
    let body = if mode == Mode::Truncated && body.len() > 1 {
        &body[..body.len() / 2]
    } else {
        body
    };
    for chunk in body.chunks(4096) {
        if socket.write_all(chunk).await.is_err() {
            return;
        }
        if slow {
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
    }
    let _ = socket.shutdown().await;
}

fn job(url: &str) -> Job {
    Job {
        verification: Default::default(),
        transfer: Default::default(),
        id: 1,
        url: url.into(),
        file_name: "result.bin".into(),
        status: Status::Queued,
        downloaded: 0,
        total: None,
        connections: 0,
        error: None,
    }
}

async fn run(server: &Server, path: &Path, connections: usize) -> anyhow::Result<Outcome> {
    download::download(
        download::client()?,
        job(&server.url),
        path.into(),
        Options {
            connections,
            retries: 0,
        },
        Arc::new(Progress::default()),
        CancellationToken::new(),
    )
    .await
}

async fn pause(server: &Server, path: &Path) -> Arc<Progress> {
    let progress = Arc::new(Progress::default());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(download::download(
        download::client().unwrap(),
        job(&server.url),
        path.into(),
        Options {
            connections: 4,
            retries: 0,
        },
        progress.clone(),
        cancel.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if progress.snapshot().0 >= 16 * 1024 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("download should start");
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap(), Outcome::Paused);
    assert!(!path.join("result.bin").exists());
    assert!(progress.snapshot().0 < server.size as u64);
    progress
}

#[tokio::test]
async fn parallel_ranges_produce_exact_bytes() {
    let server = Server::start(Mode::Ranges, 100_003, false).await;
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(run(&server, dir.path(), 4).await.unwrap(), Outcome::Done);
    assert_eq!(
        tokio::fs::read(dir.path().join("result.bin"))
            .await
            .unwrap(),
        server.bytes()
    );
    let requests = server.requests();
    assert_eq!(requests.len(), 5);
    assert!(
        requests
            .iter()
            .skip(1)
            .all(|r| r.range.is_some() && r.if_range.as_deref() == Some("\"v1\""))
    );
}

#[tokio::test]
async fn pause_and_resume_uses_partial_offsets_even_with_a_new_connection_limit() {
    let server = Server::start(Mode::Ranges, 1024 * 1024, true).await;
    let dir = tempfile::tempdir().unwrap();
    pause(&server, dir.path()).await;
    server.clear_requests();
    let progress = Arc::new(Progress::default());
    let result = download::download(
        download::client().unwrap(),
        job(&server.url),
        dir.path().into(),
        Options {
            connections: 2,
            retries: 0,
        },
        progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(result, Outcome::Done);
    assert_eq!(
        tokio::fs::read(dir.path().join("result.bin"))
            .await
            .unwrap(),
        server.bytes()
    );
    assert!(
        server
            .requests()
            .iter()
            .skip(1)
            .any(|r| r.range.is_some_and(|(start, _)| start % (256 * 1024) != 0))
    );
    assert_eq!(progress.snapshot().2, 2);
}

#[tokio::test]
async fn changed_etag_discards_old_partial_bytes() {
    let server = Server::start(Mode::Ranges, 512 * 1024, true).await;
    let dir = tempfile::tempdir().unwrap();
    pause(&server, dir.path()).await;
    server.version.store(2, Ordering::Relaxed);
    server.clear_requests();
    assert_eq!(run(&server, dir.path(), 4).await.unwrap(), Outcome::Done);
    assert_eq!(
        tokio::fs::read(dir.path().join("result.bin"))
            .await
            .unwrap(),
        server.bytes()
    );
    assert!(
        server
            .requests()
            .iter()
            .skip(1)
            .all(|r| r.if_range.as_deref() == Some("\"v2\""))
    );
}

#[tokio::test]
async fn ignored_ranges_and_missing_etags_fall_back_to_a_full_download() {
    for mode in [Mode::IgnoreRange, Mode::NoEtag, Mode::UnknownLength] {
        let server = Server::start(mode, 23_456, false).await;
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(run(&server, dir.path(), 4).await.unwrap(), Outcome::Done);
        assert_eq!(
            tokio::fs::read(dir.path().join("result.bin"))
                .await
                .unwrap(),
            server.bytes()
        );
        assert_eq!(server.requests().len(), 2);
        assert!(server.requests()[1].range.is_none());
    }
}

#[tokio::test]
async fn a_server_ignoring_non_probe_ranges_falls_back_without_corruption() {
    let server = Server::start(Mode::ProbeOnly, 65_536, false).await;
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(run(&server, dir.path(), 4).await.unwrap(), Outcome::Done);
    assert_eq!(
        tokio::fs::read(dir.path().join("result.bin"))
            .await
            .unwrap(),
        server.bytes()
    );
}

#[tokio::test]
async fn malformed_ranges_and_truncated_bodies_are_never_published() {
    for mode in [Mode::WrongRange, Mode::Truncated] {
        let server = Server::start(mode, 65_536, false).await;
        let dir = tempfile::tempdir().unwrap();
        assert!(run(&server, dir.path(), 4).await.is_err());
        assert!(!dir.path().join("result.bin").exists());
    }
}

#[tokio::test]
async fn an_empty_resource_creates_an_empty_file() {
    let server = Server::start(Mode::Ranges, 0, false).await;
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(run(&server, dir.path(), 4).await.unwrap(), Outcome::Done);
    assert!(
        tokio::fs::read(dir.path().join("result.bin"))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn existing_files_are_preserved() {
    let server = Server::start(Mode::Ranges, 100, false).await;
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(dir.path().join("result.bin"), b"keep me")
        .await
        .unwrap();
    assert!(run(&server, dir.path(), 4).await.is_err());
    assert_eq!(
        tokio::fs::read(dir.path().join("result.bin"))
            .await
            .unwrap(),
        b"keep me"
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn headless_cli_accepts_a_list_file_and_restores_the_completed_queue() {
    let server = Server::start(Mode::Ranges, 50_003, false).await;
    let dir = tempfile::tempdir().unwrap();
    let list = dir.path().join("urls.txt");
    let output = dir.path().join("downloads");
    std::fs::write(&list, format!("# URLs\n{}\n", server.url)).unwrap();
    let binary = env!("CARGO_BIN_EXE_oxget");
    let (o, l) = (output.clone(), list.clone());
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new(binary)
            .arg(l)
            .arg("-d")
            .arg(o)
            .args(["--no-tui", "-c", "3", "-r", "0"])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert_eq!(
        std::fs::read(output.join("file.bin")).unwrap(),
        server.bytes()
    );
    let count = server.requests().len();
    let second = tokio::task::spawn_blocking(move || {
        std::process::Command::new(binary)
            .arg("-d")
            .arg(output)
            .arg("--no-tui")
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(second.status.success());
    assert_eq!(server.requests().len(), count);
}

async fn command(path: &Path, args: &[&str]) -> (i32, serde_json::Value) {
    let (path, args) = (
        path.to_path_buf(),
        args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>(),
    );
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_oxget"))
            .arg("-d")
            .arg(path)
            .arg("--json")
            .args(args)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    let reply: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|error| {
        panic!(
            "{error}: {text}; stderr={}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code().unwrap(), reply)
}

struct WorkerGuard(std::path::PathBuf);

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new(env!("CARGO_BIN_EXE_oxget"))
            .arg("-d")
            .arg(&self.0)
            .args(["stop", "--json"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

async fn wait_for_partial(path: &Path, id: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (code, status) = command(path, &["status", id]).await;
            assert_eq!(code, 0, "{status}");
            if status["data"]["jobs"][0]["downloaded_bytes"]
                .as_u64()
                .unwrap()
                > 0
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn background_commands_pause_move_stop_restart_and_resume_exact_bytes() {
    let server = Server::start(Mode::Ranges, 4 * 1024 * 1024, true).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let _guard = WorkerGuard(path.into());
    let second = server.url.replace("file.bin", "second.bin");
    let (code, _) = command(path, &["add", &server.url, &second, "--paused"]).await;
    assert_eq!(code, 0);
    let (code, started) = command(path, &["start", "--background", "-r", "0"]).await;
    assert_eq!(code, 0, "{started}");
    assert_eq!(started["data"]["running"], true);
    let (code, again) = command(path, &["start", "--background"]).await;
    assert_eq!(code, 0);
    assert_eq!(again["data"]["pid"], started["data"]["pid"]);
    assert_eq!(command(path, &["move", "2", "--first"]).await.0, 0);
    assert_eq!(command(path, &["list"]).await.1["data"]["jobs"][0]["id"], 2);
    assert_eq!(command(path, &["resume", "2"]).await.0, 0);
    wait_for_partial(path, "2").await;
    let (code, paused) = command(path, &["pause", "2"]).await;
    assert_eq!(code, 0);
    let bytes = paused["data"]["jobs"][0]["downloaded_bytes"]
        .as_u64()
        .unwrap();
    assert!(bytes > 0 && bytes < server.size as u64);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        command(path, &["status", "2"]).await.1["data"]["jobs"][0]["downloaded_bytes"],
        bytes
    );
    assert_eq!(command(path, &["resume", "2"]).await.0, 0);
    let (code, done) = command(path, &["wait", "2", "--timeout", "10"]).await;
    assert_eq!(code, 0, "{done}");
    assert_eq!(
        std::fs::read(path.join("second.bin")).unwrap(),
        server.bytes()
    );

    assert_eq!(command(path, &["pause", "--all"]).await.0, 0);
    assert_eq!(command(path, &["resume", "1"]).await.0, 0);
    assert_eq!(command(path, &["wait", "1", "--timeout", "0"]).await.0, 3);
    assert_eq!(command(path, &["resume", "--all"]).await.0, 0);
    wait_for_partial(path, "1").await;
    let (code, stopped) = command(path, &["stop"]).await;
    assert_eq!(code, 0, "{stopped}");
    assert_eq!(stopped["data"]["running"], false);
    assert!(!path.join(".oxget/worker.json").exists());
    assert_eq!(
        command(path, &["status", "1"]).await.1["data"]["jobs"][0]["state"],
        "queued"
    );
    let (code, restart) = command(path, &["start", "--background", "-r", "0"]).await;
    assert_eq!(code, 0, "{restart}");
    let (code, done) = command(path, &["wait", "--all", "--timeout", "10"]).await;
    assert_eq!(code, 0, "{done}");
    assert_eq!(
        std::fs::read(path.join("file.bin")).unwrap(),
        server.bytes()
    );
    assert_eq!(command(path, &["remove", "2"]).await.0, 0);
    assert_eq!(
        std::fs::read(path.join("second.bin")).unwrap(),
        server.bytes()
    );
}

#[tokio::test]
async fn live_additions_are_serialized_and_active_removal_stops_file_writes() {
    let server = Server::start(Mode::Ranges, 4 * 1024 * 1024, true).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let _guard = WorkerGuard(path.into());
    assert_eq!(
        command(path, &["start", "--background", "-r", "0"]).await.0,
        0
    );
    let mut calls = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let (path, url) = (path.to_path_buf(), server.url.clone());
        calls.spawn(async move { command(&path, &["add", &url, "--paused"]).await });
    }
    while let Some(result) = calls.join_next().await {
        let (code, reply) = result.unwrap();
        assert_eq!(code, 0, "{reply}");
        assert_eq!(reply["data"]["ids"], serde_json::json!([1]));
    }
    assert_eq!(
        command(path, &["list"]).await.1["data"]["jobs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(command(path, &["resume", "1"]).await.0, 0);
    wait_for_partial(path, "1").await;
    assert_eq!(command(path, &["remove", "1"]).await.0, 0);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(!path.join("file.bin").exists());
    assert!(!path.join(".oxget/parts/1").exists());
    assert_eq!(
        command(path, &["list"]).await.1["data"]["jobs"],
        serde_json::json!([])
    );
}

#[tokio::test]
async fn ipc_rejects_an_invalid_token_without_stopping_the_worker() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let _guard = WorkerGuard(path.into());
    assert_eq!(command(path, &["start", "--background"]).await.0, 0);
    let endpoint: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join(".oxget/worker.json")).unwrap()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path.join(".oxget/worker.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let mut socket = TcpStream::connect(("127.0.0.1", endpoint["port"].as_u64().unwrap() as u16))
        .await
        .unwrap();
    let payload = serde_json::to_vec(
        &serde_json::json!({"token": "wrong-token", "action": {"command": "stop"}}),
    )
    .unwrap();
    socket.write_u32(payload.len() as u32).await.unwrap();
    socket.write_all(&payload).await.unwrap();
    let len = socket.read_u32().await.unwrap() as usize;
    let mut bytes = vec![0; len];
    socket.read_exact(&mut bytes).await.unwrap();
    let reply: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(reply["error"]["code"], "unauthorized");
    assert_eq!(command(path, &["status"]).await.1["data"]["running"], true);
}

#[tokio::test]
async fn wait_deadline_also_bounds_a_worker_that_does_not_answer_ipc() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    assert_eq!(
        command(path, &["add", "https://example.org/a", "--paused"])
            .await
            .0,
        0
    );
    let store = oxget::model::Store::open(path).unwrap();
    let server = oxget::ipc::Server::bind(&store.root).await.unwrap();
    let start = std::time::Instant::now();
    let (code, reply) = command(path, &["wait", "1", "--timeout", "1"]).await;
    assert_eq!(code, 3, "{reply}");
    assert!(start.elapsed() < Duration::from_secs(3));
    drop(server);
}

struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn crashed_worker_recovers_partial_files_and_replaces_its_stale_endpoint() {
    let server = Server::start(Mode::Ranges, 4 * 1024 * 1024, true).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let _guard = WorkerGuard(path.into());
    assert_eq!(command(path, &["add", &server.url]).await.0, 0);
    let mut child = ChildGuard(
        std::process::Command::new(env!("CARGO_BIN_EXE_oxget"))
            .arg("__worker")
            .arg("-d")
            .arg(path)
            .args(["-r", "0"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for_partial(path, "1").await;
    let endpoint = path.join(".oxget/worker.json");
    let original: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&endpoint).unwrap()).unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    assert!(endpoint.exists());
    let (code, status) = command(path, &["status", "1"]).await;
    assert_eq!(code, 0, "{status}");
    assert_eq!(status["data"]["running"], false);
    assert_eq!(status["data"]["jobs"][0]["state"], "queued");
    server.clear_requests();
    let (code, started) = command(path, &["start", "--background", "-r", "0"]).await;
    assert_eq!(code, 0, "{started}");
    let replacement: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&endpoint).unwrap()).unwrap();
    assert_ne!(original["token"], replacement["token"]);
    let (code, result) = command(path, &["wait", "1", "--timeout", "10"]).await;
    assert_eq!(code, 0, "{result}");
    assert_eq!(
        std::fs::read(path.join("file.bin")).unwrap(),
        server.bytes()
    );
    assert!(
        server
            .requests()
            .iter()
            .skip(1)
            .any(|r| r.range.is_some_and(|(start, _)| start % (1024 * 1024) != 0))
    );
}

fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

async fn wait_until_stopped(path: &Path) {
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let (code, status) = command(path, &["status"]).await;
            assert_eq!(code, 0, "{status}");
            if status["data"]["running"] == false {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker should exit after terminal jobs");
}

#[tokio::test]
async fn checksum_is_verified_before_publication_for_ranges_full_and_empty_files() {
    for (mode, size) in [
        (Mode::Ranges, 65_537),
        (Mode::NoEtag, 35_123),
        (Mode::Ranges, 0),
    ] {
        let server = Server::start(mode, size, false).await;
        let dir = tempfile::tempdir().unwrap();
        let mut job = job(&server.url);
        let hash = digest(&server.bytes());
        job.verification.expected_sha256 = Some(hash.clone());
        let progress = Arc::new(Progress::default());
        let outcome = download::download(
            download::client().unwrap(),
            job,
            dir.path().into(),
            Options {
                connections: 4,
                retries: 0,
            },
            progress.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, Outcome::Done);
        assert_eq!(
            std::fs::read(dir.path().join("result.bin")).unwrap(),
            server.bytes()
        );
        let verification = progress.verification.lock().unwrap();
        assert_eq!(verification.actual_sha256.as_deref(), Some(hash.as_str()));
        assert_eq!(verification.result, Some(true));
    }
}

#[tokio::test]
async fn mismatch_is_distinct_persisted_and_retry_discards_bad_parts() {
    let server = Server::start(Mode::Ranges, 65_537, false).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let _guard = WorkerGuard(path.into());
    let bad_hash = "00".repeat(32);
    assert_eq!(
        command(path, &["add", &server.url, "--sha256", &bad_hash])
            .await
            .0,
        0
    );
    let (code, start) = command(
        path,
        &["start", "--background", "--exit-when-done", "-r", "0"],
    )
    .await;
    assert_eq!(code, 0, "{start}");
    let (code, result) = command(path, &["wait", "1", "--timeout", "10"]).await;
    assert_eq!(code, 5, "{result}");
    assert_eq!(result["error"]["code"], "verification_failed");
    wait_until_stopped(path).await;
    assert!(!path.join("file.bin").exists());
    let (_, status) = command(path, &["status", "1"]).await;
    let job = &status["data"]["jobs"][0];
    assert_eq!(job["state"], "verification_failed");
    assert_eq!(job["verification"]["expected_sha256"], bad_hash);
    assert_eq!(
        job["verification"]["actual_sha256"],
        digest(&server.bytes())
    );
    assert_eq!(job["verification"]["result"], false);
    assert_eq!(
        command(path, &["list", "--state", "verification_failed"])
            .await
            .1["data"]["jobs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(path.join(".oxget/parts/1/transfer.json").exists());
    assert_eq!(command(path, &["retry", "--failed"]).await.0, 0);
    assert!(!path.join(".oxget/parts/1").exists());
    let (_, status) = command(path, &["status", "1"]).await;
    assert_eq!(status["data"]["jobs"][0]["state"], "queued");
    assert!(status["data"]["jobs"][0]["verification"]["actual_sha256"].is_null());
}

#[tokio::test]
async fn sums_batch_auto_exit_and_final_overall_are_available_after_worker_exit() {
    let server = Server::start(Mode::Ranges, 50_003, false).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let _guard = WorkerGuard(path.into());
    let hash = digest(&server.bytes());
    let sums = path.join("SHA256SUMS");
    std::fs::write(&sums, format!("{hash}  file.bin\n{hash} *second.bin\n")).unwrap();
    let second = server.url.replace("file.bin", "second.bin");
    let (code, added) = command(
        path,
        &[
            "add",
            &server.url,
            &second,
            "--sha256sums",
            sums.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(code, 0, "{added}");
    assert!(added["data"]["overall"]["total_bytes"].is_null());
    assert!(added["data"]["overall"]["percent"].is_null());
    assert!(added["data"]["overall"]["eta_seconds"].is_null());
    assert_eq!(
        command(
            path,
            &[
                "start",
                "--background",
                "--exit-when-done",
                "-j",
                "2",
                "-r",
                "0"
            ]
        )
        .await
        .0,
        0
    );
    let (code, done) = command(path, &["wait", "--all", "--timeout", "10"]).await;
    assert_eq!(code, 0, "{done}");
    wait_until_stopped(path).await;
    let (_, status) = command(path, &["status"]).await;
    assert_eq!(status["data"]["overall"]["files_completed"], 2);
    assert_eq!(status["data"]["overall"]["total_bytes"], 100_006);
    assert_eq!(status["data"]["overall"]["percent"], 100.0);
    assert_eq!(status["data"]["overall"]["eta_seconds"], 0);
    for job in status["data"]["jobs"].as_array().unwrap() {
        assert_eq!(job["verification"]["result"], true);
    }
    assert_eq!(
        std::fs::read(path.join("file.bin")).unwrap(),
        server.bytes()
    );
    assert_eq!(
        std::fs::read(path.join("second.bin")).unwrap(),
        server.bytes()
    );
    // Already-completed and empty queues must still acknowledge background startup.
    assert_eq!(
        command(path, &["start", "--background", "--exit-when-done"])
            .await
            .0,
        0
    );
    wait_until_stopped(path).await;
    assert_eq!(command(path, &["remove", "1", "2"]).await.0, 0);
    assert_eq!(
        command(path, &["start", "--background", "--exit-when-done"])
            .await
            .0,
        0
    );
    wait_until_stopped(path).await;
}

#[tokio::test]
async fn paused_jobs_keep_once_worker_alive_and_idle_logging_is_quiet() {
    let server = Server::start(Mode::NoEtag, 4 * 1024 * 1024, true).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let _guard = WorkerGuard(path.into());
    assert_eq!(command(path, &["add", &server.url, "--paused"]).await.0, 0);
    assert_eq!(
        command(
            path,
            &["start", "--background", "--exit-when-done", "-r", "0"]
        )
        .await
        .0,
        0
    );
    let log = path.join(".oxget/worker.log");
    let before = std::fs::read(&log).unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(std::fs::read(&log).unwrap(), before);
    assert_eq!(command(path, &["status"]).await.1["data"]["running"], true);
    assert_eq!(command(path, &["resume", "1"]).await.0, 0);
    wait_for_partial(path, "1").await;
    let active_before = std::fs::read(&log).unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        std::fs::read(&log).unwrap(),
        active_before,
        "active byte progress must not enter worker.log"
    );
    assert_eq!(command(path, &["wait", "1", "--timeout", "10"]).await.0, 0);
    wait_until_stopped(path).await;
    let (_, status) = command(path, &["status", "1"]).await;
    assert_eq!(status["data"]["jobs"][0]["resume_supported"], false);
    assert_eq!(
        status["data"]["jobs"][0]["transfer_reason"],
        "no_strong_etag"
    );
    let log = std::fs::read_to_string(log).unwrap();
    assert!(log.contains("state=done"));
    assert!(!log.contains("conn"));
    assert!(log.lines().count() < 15);
}

#[tokio::test]
async fn hash_verification_detects_corrupted_resumed_parts_even_with_unchanged_etag() {
    let server = Server::start(Mode::Ranges, 1024 * 1024, true).await;
    let dir = tempfile::tempdir().unwrap();
    pause(&server, dir.path()).await;
    let work = dir.path().join(".oxget/parts/1");
    let part = std::fs::read_dir(&work)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.extension().is_some_and(|e| e == "part") && std::fs::metadata(p).unwrap().len() > 0
        })
        .unwrap();
    use std::io::Write;
    std::fs::OpenOptions::new()
        .write(true)
        .open(part)
        .unwrap()
        .write_all(b"corrupted")
        .unwrap();
    let mut job = job(&server.url);
    job.verification.expected_sha256 = Some(digest(&server.bytes()));
    let result = download::download(
        download::client().unwrap(),
        job,
        dir.path().into(),
        Options {
            connections: 4,
            retries: 0,
        },
        Arc::new(Progress::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(result, Outcome::VerificationFailed);
    assert!(!dir.path().join("result.bin").exists());
}

#[tokio::test]
async fn old_worker_protocol_cannot_silently_ignore_checksum_requests() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let _guard = WorkerGuard(path.into());
    assert_eq!(command(path, &["start", "--background"]).await.0, 0);
    let endpoint_path = path.join(".oxget/worker.json");
    let mut endpoint: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&endpoint_path).unwrap()).unwrap();
    assert_eq!(endpoint["version"], 2);
    endpoint["version"] = serde_json::json!(1);
    std::fs::write(&endpoint_path, serde_json::to_vec(&endpoint).unwrap()).unwrap();
    let hash = "00".repeat(32);
    let (code, reply) = command(
        path,
        &[
            "add",
            "https://example.org/model",
            "--sha256",
            &hash,
            "--paused",
        ],
    )
    .await;
    assert_eq!(code, 1, "{reply}");
    assert_eq!(reply["error"]["code"], "worker_unavailable");
    assert_eq!(
        command(path, &["list"]).await.1["data"]["jobs"],
        serde_json::json!([])
    );
    assert_eq!(command(path, &["stop"]).await.0, 0);
}
