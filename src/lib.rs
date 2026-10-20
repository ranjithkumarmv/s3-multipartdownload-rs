//! Fast parallel downloads of large Amazon S3 (or S3-compatible) objects.
//!
//! The object is split into byte ranges that are fetched concurrently with the
//! AWS SDK for Rust. Each range is streamed straight to its offset in a
//! pre-sized temp file, which is renamed into place once every range has
//! arrived. Memory use stays flat whatever the object size.
//!
//! ```no_run
//! # async fn example() -> Result<(), s3_multipartdownload::Error> {
//! let config = aws_config::load_from_env().await;
//! let client = aws_sdk_s3::Client::new(&config);
//!
//! let path = s3_multipartdownload::download(&client, "my-bucket", "datasets/big.parquet")
//!     .to_dir("./downloads")
//!     .parallel(16)
//!     .on_progress(|p| eprintln!("{}/{} bytes", p.downloaded_bytes, p.total_bytes))
//!     .send()
//!     .await?;
//! println!("saved to {}", path.display());
//! # Ok(())
//! # }
//! ```
//!
//! # Guarantees
//!
//! - Every range request sends `If-Match` with the ETag from the initial
//!   `HeadObject`, so an object overwritten mid-download fails with HTTP 412
//!   instead of producing a file that mixes two versions.
//! - A range that fails, including one that fails mid-stream, is retried with
//!   exponential backoff. Client errors (4xx other than 408 and 429) are not.
//! - The output appears only when complete: data goes to `<file>.download`,
//!   which is renamed on success and removed on failure.
//!
//! # Cancellation
//!
//! Drop the future returned by [`Download::send`], for example with
//! `tokio::time::timeout` or `tokio::select!`. In-flight ranges are aborted and
//! the temp file is removed.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::{DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStreamError;
use aws_sdk_s3::types::RequestPayer;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::task::JoinSet;

/// Default bytes per range request: 8 MiB.
pub const DEFAULT_PART_SIZE: u64 = 8 * 1024 * 1024;
/// Default number of concurrent range requests.
pub const DEFAULT_PARALLEL: usize = 8;
/// Default extra attempts per range after a retryable failure.
pub const DEFAULT_RETRIES: u32 = 3;

/// Bytes written so far, passed to the [`Download::on_progress`] callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
}

/// Everything that can go wrong during a download.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid option: {0}")]
    InvalidOption(&'static str),

    #[error("cannot derive a file name from key {0:?}; use to_file() to name the output")]
    NoFileName(String),

    #[error("HeadObject failed: {}", DisplayErrorContext(.0.as_ref()))]
    Head(#[source] Box<SdkError<HeadObjectError>>),

    /// The bucket lives in another region than the client's. Build a client
    /// for `region` and try again.
    #[error("the bucket is in region {region}, not the client's region")]
    WrongRegion { region: String },

    #[error("HeadObject did not return a valid content length")]
    MissingContentLength,

    #[error("GetObject bytes={}-{} failed: {}", .range.0, .range.1, DisplayErrorContext(.source.as_ref()))]
    Get {
        range: (u64, u64),
        #[source]
        source: Box<SdkError<GetObjectError>>,
    },

    #[error("reading bytes={}-{} failed: {source}", .range.0, .range.1)]
    Body {
        range: (u64, u64),
        #[source]
        source: ByteStreamError,
    },

    #[error("bytes={}-{} returned {got} bytes, expected {expected}", .range.0, .range.1)]
    ShortPart {
        range: (u64, u64),
        got: u64,
        expected: u64,
    },

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    /// HTTP status code of the failed S3 request, if there was a response.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Head(e) => e.raw_response().map(|r| r.status().as_u16()),
            Error::Get { source, .. } => source.raw_response().map(|r| r.status().as_u16()),
            _ => None,
        }
    }

    fn is_retryable(&self) -> bool {
        match self {
            // 412 means the object changed mid-download; retrying would mix versions.
            Error::Get { .. } => {
                !matches!(self.status(), Some(s) if (400..500).contains(&s) && s != 408 && s != 429)
            }
            Error::Body { .. } | Error::ShortPart { .. } => true,
            _ => false,
        }
    }
}

/// Start building a download of `s3://bucket/key`. Finish with [`Download::send`].
pub fn download(client: &Client, bucket: impl Into<String>, key: impl Into<String>) -> Download {
    Download {
        client: client.clone(),
        bucket: bucket.into(),
        key: key.into(),
        dest: Dest::Dir(PathBuf::from(".")),
        parallel: DEFAULT_PARALLEL,
        part_size: DEFAULT_PART_SIZE,
        retries: DEFAULT_RETRIES,
        version_id: None,
        request_payer: false,
        on_progress: None,
    }
}

type ProgressFn = Arc<dyn Fn(Progress) + Send + Sync>;

enum Dest {
    Dir(PathBuf),
    File(PathBuf),
}

/// A configured download. Created by [`download`].
#[must_use = "a download does nothing until you call .send().await"]
pub struct Download {
    client: Client,
    bucket: String,
    key: String,
    dest: Dest,
    parallel: usize,
    part_size: u64,
    retries: u32,
    version_id: Option<String>,
    request_payer: bool,
    on_progress: Option<ProgressFn>,
}

impl Download {
    /// Save inside `dir` (created if missing) under the key's last path segment.
    /// This is the default, with `dir = "."`.
    pub fn to_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dest = Dest::Dir(dir.into());
        self
    }

    /// Save to exactly this path. Its parent directory is created if missing.
    pub fn to_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.dest = Dest::File(path.into());
        self
    }

    /// Maximum concurrent range requests. Default [`DEFAULT_PARALLEL`].
    pub fn parallel(mut self, n: usize) -> Self {
        self.parallel = n;
        self
    }

    /// Bytes per range request. Default [`DEFAULT_PART_SIZE`].
    pub fn part_size(mut self, bytes: u64) -> Self {
        self.part_size = bytes;
        self
    }

    /// Extra attempts per range on network errors, 5xx, 408 and 429. Default [`DEFAULT_RETRIES`].
    pub fn retries(mut self, n: u32) -> Self {
        self.retries = n;
        self
    }

    /// Download a specific object version.
    pub fn version_id(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = Some(version_id.into());
        self
    }

    /// Acknowledge that the bucket owner charges the requester.
    pub fn request_payer(mut self, yes: bool) -> Self {
        self.request_payer = yes;
        self
    }

    /// Called from download tasks as bytes are written. Keep it cheap.
    pub fn on_progress(mut self, f: impl Fn(Progress) + Send + Sync + 'static) -> Self {
        self.on_progress = Some(Arc::new(f));
        self
    }

    /// Run the download. Resolves to the path of the finished file.
    pub async fn send(self) -> Result<PathBuf, Error> {
        if self.parallel == 0 {
            return Err(Error::InvalidOption("parallel must be at least 1"));
        }
        if self.part_size == 0 {
            return Err(Error::InvalidOption("part_size must be at least 1"));
        }

        let head = self.head().await?;
        let total = head
            .content_length()
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(Error::MissingContentLength)?;

        let output = match &self.dest {
            Dest::Dir(dir) => dir.join(file_name_from_key(&self.key)?),
            Dest::File(path) => path.clone(),
        };
        if let Some(parent) = output.parent().filter(|p| !p.as_os_str().is_empty()) {
            tokio::fs::create_dir_all(parent).await?;
        }
        let temp = temp_path(&output);

        // Pre-size the temp file so every range can write at its own offset.
        let file = tokio::fs::File::create(&temp).await?;
        let guard = TempFile(Some(temp.clone()));
        file.set_len(total).await?;
        drop(file);

        let ctx = Arc::new(PartContext {
            client: self.client,
            bucket: self.bucket,
            key: self.key,
            version_id: self.version_id,
            request_payer: self.request_payer,
            etag: head.e_tag().map(str::to_owned),
            path: temp.clone(),
            retries: self.retries,
            total,
            downloaded: AtomicU64::new(0),
            on_progress: self.on_progress,
        });

        // Declared after `guard`, so on cancellation the tasks are aborted
        // before the temp file is removed.
        let mut tasks = JoinSet::new();
        let mut pending = ranges(total, self.part_size).into_iter();
        loop {
            while tasks.len() < self.parallel {
                let Some(range) = pending.next() else { break };
                let ctx = Arc::clone(&ctx);
                tasks.spawn(async move { ctx.download_part(range).await });
            }
            match tasks.join_next().await {
                None => break,
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(err))) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(err);
                }
                Some(Err(join_err)) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    if join_err.is_panic() {
                        std::panic::resume_unwind(join_err.into_panic());
                    }
                    return Err(Error::Io(std::io::Error::other(join_err)));
                }
            }
        }

        tokio::fs::rename(&temp, &output).await?;
        guard.keep();
        Ok(output)
    }

    async fn head(&self) -> Result<aws_sdk_s3::operation::head_object::HeadObjectOutput, Error> {
        let mut req = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&self.key);
        if let Some(v) = &self.version_id {
            req = req.version_id(v);
        }
        if self.request_payer {
            req = req.request_payer(RequestPayer::Requester);
        }
        req.send().await.map_err(|err| {
            // S3 answers a request sent to the wrong region with the bucket's
            // real region in a header. Surface it so callers can retry there.
            let region = err
                .raw_response()
                .and_then(|r| r.headers().get("x-amz-bucket-region"))
                .map(str::to_owned);
            let client_region = self.client.config().region().map(|r| r.as_ref().to_owned());
            match region {
                Some(region) if Some(&region) != client_region.as_ref() => {
                    Error::WrongRegion { region }
                }
                _ => Error::Head(Box::new(err)),
            }
        })
    }
}

struct PartContext {
    client: Client,
    bucket: String,
    key: String,
    version_id: Option<String>,
    request_payer: bool,
    etag: Option<String>,
    path: PathBuf,
    retries: u32,
    total: u64,
    downloaded: AtomicU64,
    on_progress: Option<ProgressFn>,
}

impl PartContext {
    async fn download_part(&self, range: (u64, u64)) -> Result<(), Error> {
        let mut attempt = 0;
        loop {
            let mut written = 0;
            let result = self.try_part(range, &mut written).await;
            // Roll back progress from a failed attempt; it gets rewritten on retry.
            if result.is_err() && written > 0 {
                let now = self.downloaded.fetch_sub(written, Ordering::Relaxed) - written;
                self.report(now);
            }
            match result {
                Ok(()) => return Ok(()),
                Err(err) if attempt < self.retries && err.is_retryable() => {
                    tokio::time::sleep(Duration::from_millis((100u64 << attempt.min(5)).min(2000)))
                        .await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn try_part(&self, range: (u64, u64), written: &mut u64) -> Result<(), Error> {
        let (start, end) = range;
        let mut req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(format!("bytes={start}-{end}"));
        // If-Match pins every range to the version we sized the file from.
        if let Some(etag) = &self.etag {
            req = req.if_match(etag);
        }
        if let Some(v) = &self.version_id {
            req = req.version_id(v);
        }
        if self.request_payer {
            req = req.request_payer(RequestPayer::Requester);
        }
        let mut body = req
            .send()
            .await
            .map_err(|source| Error::Get {
                range,
                source: Box::new(source),
            })?
            .body;

        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .await?;
        file.seek(std::io::SeekFrom::Start(start)).await?;
        while let Some(chunk) = body
            .try_next()
            .await
            .map_err(|source| Error::Body { range, source })?
        {
            file.write_all(&chunk).await?;
            let n = chunk.len() as u64;
            *written += n;
            let now = self.downloaded.fetch_add(n, Ordering::Relaxed) + n;
            self.report(now);
        }
        file.flush().await?;

        let expected = end - start + 1;
        if *written != expected {
            return Err(Error::ShortPart {
                range,
                got: *written,
                expected,
            });
        }
        Ok(())
    }

    fn report(&self, downloaded_bytes: u64) {
        if let Some(f) = &self.on_progress {
            f(Progress {
                downloaded_bytes,
                total_bytes: self.total,
            });
        }
    }
}

/// Inclusive `(start, end)` byte ranges covering `0..total`.
pub fn ranges(total: u64, part_size: u64) -> Vec<(u64, u64)> {
    assert!(part_size > 0, "part_size must be at least 1");
    (0..total)
        .step_by(usize::try_from(part_size).unwrap_or(usize::MAX))
        .map(|start| (start, start.saturating_add(part_size).min(total) - 1))
        .collect()
}

fn file_name_from_key(key: &str) -> Result<&str, Error> {
    match key.rsplit('/').next() {
        Some(name) if !name.is_empty() && name != "." && name != ".." => Ok(name),
        _ => Err(Error::NoFileName(key.to_owned())),
    }
}

fn temp_path(output: &Path) -> PathBuf {
    let mut name = output.as_os_str().to_owned();
    name.push(".download");
    PathBuf::from(name)
}

/// Removes the temp file when dropped, unless the download completed.
struct TempFile(Option<PathBuf>);

impl TempFile {
    fn keep(mut self) {
        self.0 = None;
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_cover_every_byte_once() {
        assert_eq!(ranges(0, 10), vec![]);
        assert_eq!(ranges(1, 10), vec![(0, 0)]);
        assert_eq!(ranges(10, 10), vec![(0, 9)]);
        assert_eq!(ranges(25, 10), vec![(0, 9), (10, 19), (20, 24)]);
        assert_eq!(ranges(5, u64::MAX), vec![(0, 4)]);
    }

    #[test]
    fn file_name_comes_from_the_last_key_segment() {
        assert_eq!(file_name_from_key("a/b/file.bin").unwrap(), "file.bin");
        assert_eq!(file_name_from_key("file.bin").unwrap(), "file.bin");
        for bad in ["", "folder/", "a/..", "."] {
            assert!(
                matches!(file_name_from_key(bad), Err(Error::NoFileName(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn temp_path_appends_suffix() {
        assert_eq!(
            temp_path(Path::new("out/x.bin")),
            PathBuf::from("out/x.bin.download")
        );
    }
}
