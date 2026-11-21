use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{
    Client, Response, StatusCode,
    header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_RANGE, ETAG, IF_RANGE, RANGE},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::model::{Job, TransferInfo, Verification, atomic_json};

const UNKNOWN: u64 = u64::MAX;

pub struct Progress {
    pub downloaded: AtomicU64,
    total: AtomicU64,
    pub connections: AtomicUsize,
    pub verifying: AtomicBool,
    pub verification: Mutex<Verification>,
    pub transfer: Mutex<TransferInfo>,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            downloaded: AtomicU64::new(0),
            total: AtomicU64::new(UNKNOWN),
            connections: AtomicUsize::new(0),
            verifying: AtomicBool::new(false),
            verification: Mutex::new(Verification::default()),
            transfer: Mutex::new(TransferInfo::default()),
        }
    }
}

impl Progress {
    pub fn snapshot(&self) -> (u64, Option<u64>, usize) {
        let total = self.total.load(Ordering::Relaxed);
        (
            self.downloaded.load(Ordering::Relaxed),
            (total != UNKNOWN).then_some(total),
            self.connections.load(Ordering::Relaxed),
        )
    }
}

#[derive(Clone, Copy)]
pub struct Options {
    pub connections: usize,
    pub retries: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Done,
    Paused,
    VerificationFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Segment {
    start: u64,
    end: u64,
}

impl Segment {
    fn len(&self) -> u64 {
        self.end - self.start + 1
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    url: String,
    total: Option<u64>,
    etag: Option<String>,
    ranged: bool,
    segments: Vec<Segment>,
}

struct Remote {
    total: Option<u64>,
    etag: Option<String>,
    ranged: bool,
    reason: Option<String>,
}

#[derive(Debug)]
struct RangeIgnored;

impl std::fmt::Display for RangeIgnored {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Server ignored a range request")
    }
}

impl std::error::Error for RangeIgnored {}

#[derive(Debug)]
struct HashMismatch;
impl std::fmt::Display for HashMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SHA-256 mismatch")
    }
}
impl std::error::Error for HashMismatch {}

pub fn client() -> Result<Client> {
    Ok(Client::builder()
        .user_agent(concat!("oxget/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(15))
        .read_timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?)
}

/// A cancellation-safe wrapper: children finish flushing their partial files before returning.
pub async fn download(
    client: Client,
    job: Job,
    output: PathBuf,
    options: Options,
    progress: Arc<Progress>,
    cancel: CancellationToken,
) -> Result<Outcome> {
    *progress.verification.lock().expect("verification lock") = job.verification.clone();
    ensure!(
        (1..=32).contains(&options.connections),
        "Connections must be between 1 and 32"
    );
    for attempt in 0..=options.retries {
        if cancel.is_cancelled() {
            return Ok(Outcome::Paused);
        }
        let result = attempt_download(&client, &job, &output, options, &progress, &cancel).await;
        match result {
            Ok(()) => return Ok(Outcome::Done),
            Err(error) if error.downcast_ref::<HashMismatch>().is_some() => {
                return Ok(Outcome::VerificationFailed);
            }
            Err(_) if cancel.is_cancelled() => return Ok(Outcome::Paused),
            Err(error) if attempt == options.retries => return Err(error),
            Err(_) => {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(Outcome::Paused),
                    _ = tokio::time::sleep(Duration::from_secs(1 << attempt.min(4))) => {},
                }
            }
        }
    }
    unreachable!()
}

async fn send(request: reqwest::RequestBuilder, cancel: &CancellationToken) -> Result<Response> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => bail!("Paused"),
        result = request.send() => Ok(result?),
    }
}

fn identity(response: &Response) -> Result<()> {
    let encoding = response
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok());
    ensure!(
        encoding.is_none_or(|v| v.eq_ignore_ascii_case("identity")),
        "Server ignored Accept-Encoding: identity"
    );
    Ok(())
}

fn etag(response: &Response) -> Option<String> {
    response
        .headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .filter(|v| v.starts_with('"') && v.ends_with('"'))
        .map(str::to_owned)
}

fn content_range(response: &Response) -> Result<(u64, u64, u64)> {
    let value = response
        .headers()
        .get(CONTENT_RANGE)
        .context("Missing Content-Range")?
        .to_str()?;
    let (range, total) = value
        .strip_prefix("bytes ")
        .context("Invalid Content-Range unit")?
        .split_once('/')
        .context("Invalid Content-Range")?;
    let (start, end) = range
        .split_once('-')
        .context("Invalid Content-Range bounds")?;
    let (start, end, total): (u64, u64, u64) = (start.parse()?, end.parse()?, total.parse()?);
    ensure!(start <= end && end < total, "Invalid Content-Range bounds");
    Ok((start, end, total))
}

async fn probe(client: &Client, url: &str, cancel: &CancellationToken) -> Result<Remote> {
    // GET works on servers that reject HEAD. Merely advertising Accept-Ranges is insufficient.
    let response = send(
        client
            .get(url)
            .header(ACCEPT_ENCODING, "identity")
            .header(RANGE, "bytes=0-0"),
        cancel,
    )
    .await?;
    identity(&response)?;
    let tag = etag(&response);
    match response.status() {
        StatusCode::PARTIAL_CONTENT => {
            let (start, end, total) = content_range(&response)?;
            ensure!(
                start == 0 && end == 0,
                "Server returned the wrong probe range"
            );
            // Strong ETags protect both parallel requests and resumed partial files.
            Ok(Remote {
                total: Some(total),
                ranged: tag.is_some(),
                reason: if tag.is_some() {
                    None
                } else {
                    Some("no_strong_etag".into())
                },
                etag: tag,
            })
        }
        StatusCode::RANGE_NOT_SATISFIABLE
            if response
                .headers()
                .get(CONTENT_RANGE)
                .is_some_and(|v| v == "bytes */0") =>
        {
            Ok(Remote {
                total: Some(0),
                etag: tag,
                ranged: false,
                reason: Some("empty_resource".into()),
            })
        }
        StatusCode::OK => Ok(Remote {
            total: response.content_length(),
            etag: tag,
            ranged: false,
            reason: Some("range_not_supported".into()),
        }),
        _ => {
            response.error_for_status_ref()?;
            bail!("Unexpected probe status: {}", response.status());
        }
    }
}

fn split(total: u64, count: usize) -> Vec<Segment> {
    if total == 0 {
        return Vec::new();
    }
    let count = (count as u64).min(total);
    let chunk = total / count;
    let extra = total % count;
    let mut start = 0;
    (0..count)
        .map(|i| {
            let len = chunk + u64::from(i < extra);
            let segment = Segment {
                start,
                end: start + len - 1,
            };
            start += len;
            segment
        })
        .collect()
}

fn valid_segments(manifest: &Manifest) -> bool {
    let Some(total) = manifest.total else {
        return !manifest.ranged && manifest.segments.is_empty();
    };
    let mut next = 0;
    for segment in &manifest.segments {
        if segment.start != next || segment.end < segment.start || segment.end >= total {
            return false;
        }
        next = segment.end + 1;
    }
    next == total && manifest.segments.len() <= 32
}

async fn attempt_download(
    client: &Client,
    job: &Job,
    output: &Path,
    options: Options,
    progress: &Arc<Progress>,
    cancel: &CancellationToken,
) -> Result<()> {
    let final_path = output.join(&job.file_name);
    ensure!(
        final_path.symlink_metadata().is_err(),
        "Output already exists; refusing to overwrite {}",
        final_path.display()
    );
    let remote = probe(client, &job.url, cancel)
        .await
        .context("Checking remote file")?;
    *progress.transfer.lock().expect("transfer lock") = TransferInfo {
        resume_supported: Some(remote.ranged),
        reason: remote.reason.clone(),
    };
    progress.verifying.store(false, Ordering::Relaxed);
    let work = output.join(".oxget").join("parts").join(job.id.to_string());
    fs::create_dir_all(&work).await?;
    let manifest_path = work.join("transfer.json");
    let old: Option<Manifest> = fs::read(&manifest_path)
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    let reusable = old.as_ref().is_some_and(|old| {
        remote.ranged
            && old.ranged
            && old.url == job.url
            && old.total == remote.total
            && old.etag == remote.etag
            && valid_segments(old)
    });
    let mut manifest = if reusable {
        old.expect("checked above")
    } else {
        // Remove only this job's private partial directory when the representation changes.
        fs::remove_dir_all(&work).await?;
        fs::create_dir_all(&work).await?;
        let segments = remote
            .total
            .map(|size| {
                split(
                    size,
                    if remote.ranged {
                        options.connections
                    } else {
                        1
                    },
                )
            })
            .unwrap_or_default();
        let manifest = Manifest {
            url: job.url.clone(),
            total: remote.total,
            etag: remote.etag,
            ranged: remote.ranged,
            segments,
        };
        atomic_json(&manifest_path, &manifest)?;
        manifest
    };
    progress
        .total
        .store(manifest.total.unwrap_or(UNKNOWN), Ordering::Relaxed);
    progress.downloaded.store(0, Ordering::Relaxed);
    progress.connections.store(
        if manifest.ranged {
            manifest.segments.len().min(options.connections)
        } else {
            1
        },
        Ordering::Relaxed,
    );

    if manifest.ranged {
        let group = cancel.child_token();
        let slots = Arc::new(Semaphore::new(options.connections));
        let mut tasks = JoinSet::new();
        for (index, segment) in manifest.segments.iter().enumerate() {
            let path = work.join(format!("{index}.part"));
            let mut offset = fs::metadata(&path).await.map(|m| m.len()).unwrap_or(0);
            if offset > segment.len() {
                fs::remove_file(&path).await?;
                offset = 0;
            }
            progress.downloaded.fetch_add(offset, Ordering::Relaxed);
            if offset == segment.len() {
                continue;
            }
            let request = client
                .get(&job.url)
                .header(ACCEPT_ENCODING, "identity")
                .header(
                    RANGE,
                    format!("bytes={}-{}", segment.start + offset, segment.end),
                )
                .header(
                    IF_RANGE,
                    manifest
                        .etag
                        .as_ref()
                        .expect("ranged requires a strong ETag"),
                );
            let (segment, progress, cancel) = (segment.clone(), progress.clone(), group.clone());
            let total = manifest.total.expect("range responses have a size");
            let tag = manifest.etag.clone();
            let slots = slots.clone();
            tasks.spawn(async move {
                let _slot = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => bail!("Paused"),
                    permit = slots.acquire_owned() => permit?,
                };
                let response = send(request, &cancel).await?;
                if response.status() == StatusCode::OK {
                    return Err(RangeIgnored.into());
                }
                if response.status() != StatusCode::PARTIAL_CONTENT {
                    // Never append a full response to a partial file.
                    response.error_for_status_ref()?;
                    bail!("Unexpected range response: {}", response.status());
                }
                identity(&response)?;
                ensure!(
                    content_range(&response)? == (segment.start + offset, segment.end, total),
                    "Server returned a mismatched Content-Range"
                );
                if let Some(received) = etag(&response) {
                    ensure!(Some(received) == tag, "Remote ETag changed");
                }
                write_response(
                    response,
                    &path,
                    true,
                    Some(segment.len() - offset),
                    &progress,
                    &cancel,
                )
                .await
            });
        }
        let mut failure = None;
        while let Some(result) = tasks.join_next().await {
            let result = result
                .map_err(anyhow::Error::from)
                .and_then(|result| result);
            if let Err(error) = result {
                if failure.is_none() {
                    failure = Some(error);
                }
                group.cancel();
            }
        }
        if let Some(error) = failure {
            if error.downcast_ref::<RangeIgnored>().is_none() {
                return Err(error);
            }
            ensure!(!cancel.is_cancelled(), "Paused");
            // Some servers honor the probe but ignore subsequent ranges. All
            // children have flushed and stopped, so start a clean full transfer.
            fs::remove_dir_all(&work).await?;
            fs::create_dir_all(&work).await?;
            manifest.ranged = false;
            manifest.segments.clear();
            atomic_json(&manifest_path, &manifest)?;
            progress.downloaded.store(0, Ordering::Relaxed);
            progress.connections.store(1, Ordering::Relaxed);
            *progress.transfer.lock().expect("transfer lock") = TransferInfo {
                resume_supported: Some(false),
                reason: Some("range_ignored_during_transfer".into()),
            };
            full_download(client, job, &work, progress, cancel).await?;
        }
    } else if manifest.total != Some(0) {
        full_download(client, job, &work, progress, cancel).await?;
    }
    ensure!(!cancel.is_cancelled(), "Paused");
    publish(&work, &final_path, &manifest, progress, cancel).await?;
    let _ = fs::remove_dir_all(&work).await;
    Ok(())
}

async fn full_download(
    client: &Client,
    job: &Job,
    work: &Path,
    progress: &Progress,
    cancel: &CancellationToken,
) -> Result<()> {
    let response = send(
        client.get(&job.url).header(ACCEPT_ENCODING, "identity"),
        cancel,
    )
    .await?;
    ensure!(
        response.status() == StatusCode::OK,
        "Unexpected download status: {}",
        response.status()
    );
    identity(&response)?;
    // This full response is authoritative, even if it differs from the probe.
    let total = response.content_length();
    progress
        .total
        .store(total.unwrap_or(UNKNOWN), Ordering::Relaxed);
    write_response(
        response,
        &work.join("0.part"),
        false,
        total,
        progress,
        cancel,
    )
    .await
}

async fn write_response(
    mut response: Response,
    path: &Path,
    append: bool,
    expected: Option<u64>,
    progress: &Progress,
    cancel: &CancellationToken,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(path)
        .await?;
    let result: Result<()> = async {
        let mut written = 0u64;
        loop {
            let chunk = tokio::select! {
                biased;
                _ = cancel.cancelled() => bail!("Paused"),
                chunk = response.chunk() => chunk?,
            };
            let Some(chunk) = chunk else {
                break;
            };
            written = written
                .checked_add(chunk.len() as u64)
                .context("File too large")?;
            ensure!(
                expected.is_none_or(|len| written <= len),
                "Response exceeds the requested size"
            );
            file.write_all(&chunk).await?;
            progress
                .downloaded
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
        }
        ensure!(
            expected.is_none_or(|len| written == len),
            "Incomplete response body"
        );
        Ok(())
    }
    .await;
    // Do not drop in-flight Tokio file writes when pausing.
    file.flush().await?;
    file.sync_data().await?;
    result
}

async fn publish(
    work: &Path,
    final_path: &Path,
    manifest: &Manifest,
    progress: &Progress,
    cancel: &CancellationToken,
) -> Result<()> {
    let temp =
        tempfile::NamedTempFile::new_in(final_path.parent().context("Missing output directory")?)?;
    let mut output = File::from_std(temp.reopen()?);
    let count = if manifest.ranged {
        manifest.segments.len()
    } else {
        usize::from(progress.downloaded.load(Ordering::Relaxed) > 0)
    };
    let mut copied = 0;
    let expected = progress
        .verification
        .lock()
        .expect("verification lock")
        .expected_sha256
        .clone();
    let mut hasher = expected.as_ref().map(|_| Sha256::new());
    progress
        .verifying
        .store(expected.is_some(), Ordering::Relaxed);
    let mut buffer = vec![0u8; 128 * 1024];
    for index in 0..count {
        let mut input = File::open(work.join(format!("{index}.part"))).await?;
        loop {
            ensure!(!cancel.is_cancelled(), "Paused");
            let len = input.read(&mut buffer).await?;
            if len == 0 {
                break;
            }
            output.write_all(&buffer[..len]).await?;
            if let Some(hash) = &mut hasher {
                hash.update(&buffer[..len]);
            }
            copied += len as u64;
        }
    }
    ensure!(
        copied == progress.downloaded.load(Ordering::Relaxed),
        "Partial file size changed during assembly"
    );
    if manifest.ranged {
        ensure!(
            Some(copied) == manifest.total,
            "Assembled file has the wrong size"
        );
    }
    output.flush().await?;
    output.sync_all().await?;
    drop(output);
    progress.total.store(copied, Ordering::Relaxed);
    if let Some(hash) = hasher {
        let actual = format!("{:x}", hash.finalize());
        let matched = expected
            .as_deref()
            .is_some_and(|expected| expected.eq_ignore_ascii_case(&actual));
        *progress.verification.lock().expect("verification lock") = Verification {
            expected_sha256: expected,
            actual_sha256: Some(actual),
            result: Some(matched),
        };
        if !matched {
            return Err(HashMismatch.into());
        }
    }
    ensure!(!cancel.is_cancelled(), "Paused");
    temp.persist_noclobber(final_path)
        .map_err(|e| e.error)
        .context("Publishing completed download without overwriting existing files")?;
    progress.total.store(copied, Ordering::Relaxed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_cover_the_file_without_overlap_or_overflow() {
        for size in [0, 1, 3, 10, 1_000_001, u64::MAX] {
            for count in [1, 4, 32] {
                let segments = split(size, count);
                let manifest = Manifest {
                    url: String::new(),
                    total: Some(size),
                    etag: None,
                    ranged: true,
                    segments,
                };
                assert!(valid_segments(&manifest));
            }
        }
    }
}
