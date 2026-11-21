//! Authenticated loopback IPC. Only the process holding Store's lock publishes an endpoint.
use std::{
    net::Ipv4Addr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    control::{Action, Failure, Reply},
    model::atomic_json,
};

const MAX_FRAME: usize = 16 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Serialize, Deserialize)]
struct Endpoint {
    version: u32,
    port: u16,
    token: String,
    pid: u32,
}

#[derive(Serialize, Deserialize)]
struct Request {
    token: String,
    action: Action,
}

pub struct Call {
    pub action: Action,
    pub response: oneshot::Sender<Reply>,
}

pub struct Server {
    pub receiver: mpsc::Receiver<Call>,
    path: PathBuf,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl Server {
    pub async fn bind(root: &Path) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let mut random = [0u8; 32];
        getrandom::fill(&mut random)
            .map_err(|error| anyhow::anyhow!("Generating IPC token: {error}"))?;
        let token: String = random.iter().map(|value| format!("{value:02x}")).collect();
        let endpoint = Endpoint {
            version: 2,
            port: listener.local_addr()?.port(),
            token: token.clone(),
            pid: std::process::id(),
        };
        let path = root.join("worker.json");
        // NamedTempFile creates this endpoint with owner-only permissions on Unix.
        atomic_json(&path, &endpoint)?;
        let (sender, receiver) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let task = tokio::spawn(async move {
            let mut requests = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    accepted = listener.accept(), if requests.len() < 64 => {
                        let Ok((socket, _)) = accepted else { break; };
                        let (sender, token) = (sender.clone(), token.clone());
                        requests.spawn(async move {
                            let _ = tokio::time::timeout(REQUEST_TIMEOUT, serve(socket, sender, token)).await;
                        });
                    }
                    _ = requests.join_next(), if !requests.is_empty() => {},
                }
            }
            // Do not drop the response to `stop` before it reaches the client.
            drop(sender);
            let _ = tokio::time::timeout(Duration::from_secs(2), async {
                while requests.join_next().await.is_some() {}
            })
            .await;
        });
        Ok(Self {
            receiver,
            path,
            cancel,
            task: Some(task),
        })
    }

    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn serve(mut socket: TcpStream, sender: mpsc::Sender<Call>, token: String) -> Result<()> {
    let request: Request =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut socket)).await??;
    if request.token != token {
        return write_frame(
            &mut socket,
            &Reply::failure(Failure::new("unauthorized", "Invalid IPC token")),
        )
        .await;
    }
    let (response, receiver) = oneshot::channel();
    if sender
        .send(Call {
            action: request.action,
            response,
        })
        .await
        .is_err()
    {
        return write_frame(
            &mut socket,
            &Reply::failure(Failure::new(
                "worker_unavailable",
                "Worker is shutting down",
            )),
        )
        .await;
    }
    let reply = receiver.await.unwrap_or_else(|_| {
        Reply::failure(Failure::new(
            "worker_unavailable",
            "Worker stopped before completing the request",
        ))
    });
    write_frame(&mut socket, &reply).await
}

pub async fn request(root: &Path, action: &Action) -> Result<Reply> {
    request_with_timeout(root, action, REQUEST_TIMEOUT).await
}

pub async fn request_with_timeout(
    root: &Path,
    action: &Action,
    timeout: Duration,
) -> Result<Reply> {
    tokio::time::timeout(timeout, async {
        let endpoint: Endpoint =
            serde_json::from_slice(&tokio::fs::read(root.join("worker.json")).await?)?;
        // Older workers silently ignore unknown Add fields. Never let them
        // accept a checksum job without actually verifying it. Read/stop remain
        // available so a running old worker can be upgraded gracefully.
        let legacy_read_or_stop = endpoint.version == 1 && matches!(action, Action::Status { .. } | Action::List { .. } | Action::Stop);
        ensure!(endpoint.version == 2 || legacy_read_or_stop,
            "Worker IPC version is outdated; run oxget stop for this directory, then restart the worker before adding or changing jobs");
        let mut socket = TcpStream::connect((Ipv4Addr::LOCALHOST, endpoint.port)).await?;
        write_frame(
            &mut socket,
            &Request {
                token: endpoint.token,
                action: action.clone(),
            },
        )
        .await?;
        read_frame(&mut socket).await
    })
    .await
    .context("IPC request timed out; a mutation may already have been applied")?
}

async fn write_frame(socket: &mut TcpStream, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAX_FRAME, "IPC message exceeds 16 MiB");
    socket.write_u32(bytes.len() as u32).await?;
    socket.write_all(&bytes).await?;
    socket.flush().await?;
    Ok(())
}

async fn read_frame<T: DeserializeOwned>(socket: &mut TcpStream) -> Result<T> {
    let len = socket.read_u32().await? as usize;
    ensure!(len <= MAX_FRAME, "IPC message exceeds 16 MiB");
    let mut bytes = vec![0u8; len];
    socket.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
