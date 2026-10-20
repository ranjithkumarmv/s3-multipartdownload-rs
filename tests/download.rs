//! End-to-end tests against a tiny in-process S3-compatible HTTP server.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use s3_multipartdownload::{Error, download};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Default)]
struct MockState {
    objects: HashMap<String, (Vec<u8>, String)>,
    /// GET ranges whose first response is cut off halfway through the body.
    cut_once: HashSet<String>,
    /// Answer HEAD with a 301 pointing here unless the request was signed for it.
    bucket_region: Option<String>,
    /// Delay before each GET response, to make concurrency observable.
    get_delay: Duration,
    gets: usize,
}

struct MockS3 {
    endpoint: String,
    state: Arc<Mutex<MockState>>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

impl MockS3 {
    async fn start(state: MockState) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let mock = MockS3 {
            endpoint,
            state: Arc::new(Mutex::new(state)),
            in_flight: Arc::default(),
            max_in_flight: Arc::default(),
        };
        let (state, in_flight, max) = (
            mock.state.clone(),
            mock.in_flight.clone(),
            mock.max_in_flight.clone(),
        );
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let (state, in_flight, max) = (state.clone(), in_flight.clone(), max.clone());
                tokio::spawn(async move { serve(socket, state, in_flight, max).await });
            }
        });
        mock
    }

    fn client(&self) -> Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .endpoint_url(&self.endpoint)
            .force_path_style(true)
            .build();
        Client::from_conf(config)
    }
}

async fn serve(
    mut socket: TcpStream,
    state: Arc<Mutex<MockState>>,
    in_flight: Arc<AtomicUsize>,
    max: Arc<AtomicUsize>,
) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    let request = String::from_utf8_lossy(&buf).to_string();
    let mut lines = request.lines();
    let mut start_line = lines.next().unwrap().split(' ');
    let method = start_line.next().unwrap().to_owned();
    let path = start_line
        .next()
        .unwrap()
        .split('?')
        .next()
        .unwrap()
        .to_owned();
    let headers: HashMap<String, String> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let key = path
        .trim_start_matches('/')
        .split_once('/')
        .map(|(_, k)| k.to_owned())
        .unwrap_or_default();

    // Work out the response while holding the lock, then write it without.
    let (status, extra_headers, body, cut, delay) = {
        let mut s = state.lock().unwrap();
        let signed_region = headers
            .get("authorization")
            .and_then(|a| a.split("Credential=").nth(1))
            .and_then(|c| c.split('/').nth(2))
            .map(str::to_owned);
        if let Some(region) = s
            .bucket_region
            .clone()
            .filter(|r| signed_region.as_ref() != Some(r))
        {
            (
                301,
                vec![("x-amz-bucket-region".to_owned(), region)],
                Vec::new(),
                false,
                Duration::ZERO,
            )
        } else if let Some((data, etag)) = s.objects.get(&key).cloned() {
            if headers.get("if-match").is_some_and(|m| *m != etag) {
                (412, vec![], Vec::new(), false, Duration::ZERO)
            } else if method == "HEAD" {
                (
                    200,
                    vec![
                        ("etag".into(), etag),
                        ("x-full-length".into(), data.len().to_string()),
                    ],
                    Vec::new(),
                    false,
                    Duration::ZERO,
                )
            } else {
                let range = headers.get("range").cloned().unwrap_or_default();
                let (a, b) = range.trim_start_matches("bytes=").split_once('-').unwrap();
                let (a, b): (usize, usize) = (a.parse().unwrap(), b.parse().unwrap());
                let body = data[a..=b.min(data.len() - 1)].to_vec();
                s.gets += 1;
                let cut = s.cut_once.remove(&range);
                let content_range = format!("bytes {a}-{}/{}", a + body.len() - 1, data.len());
                (
                    206,
                    vec![
                        ("etag".into(), etag),
                        ("content-range".into(), content_range),
                    ],
                    body,
                    cut,
                    s.get_delay,
                )
            }
        } else {
            (404, vec![], Vec::new(), false, Duration::ZERO)
        }
    };

    let is_get = method == "GET" && status == 206;
    if is_get {
        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        max.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(delay).await;
    }
    // HEAD advertises the object's length; everything else the body's.
    let length = extra_headers
        .iter()
        .find(|(k, _)| k == "x-full-length")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| body.len().to_string());
    let mut head =
        format!("HTTP/1.1 {status} X\r\ncontent-length: {length}\r\nconnection: close\r\n");
    for (k, v) in extra_headers.iter().filter(|(k, _)| k != "x-full-length") {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    let _ = socket.write_all(head.as_bytes()).await;
    let sent = if cut {
        &body[..body.len() / 2]
    } else {
        &body[..]
    };
    let _ = socket.write_all(sent).await;
    let _ = socket.shutdown().await;
    if is_get {
        in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

fn random_bytes(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("s3mpd-rs-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn files_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn object(key: &str, data: Vec<u8>) -> HashMap<String, (Vec<u8>, String)> {
    HashMap::from([(key.to_owned(), (data, "\"etag-1\"".to_owned()))])
}

#[tokio::test]
async fn downloads_byte_for_byte_in_parallel() {
    let data = random_bytes(1_000_003, 1);
    let mock = MockS3::start(MockState {
        objects: object("a/b/file.bin", data.clone()),
        get_delay: Duration::from_millis(20),
        ..Default::default()
    })
    .await;
    let dir = temp_dir("parallel").join("nested");
    let last = Arc::new(AtomicUsize::new(0));
    let last2 = last.clone();

    let path = download(&mock.client(), "bucket", "a/b/file.bin")
        .to_dir(&dir)
        .part_size(100_000)
        .parallel(4)
        .on_progress(move |p| last2.store(p.downloaded_bytes as usize, Ordering::SeqCst))
        .send()
        .await
        .unwrap();

    assert_eq!(path, dir.join("file.bin"));
    assert_eq!(std::fs::read(&path).unwrap(), data);
    assert_eq!(files_in(&dir), ["file.bin"]);
    assert_eq!(last.load(Ordering::SeqCst), data.len());
    assert_eq!(mock.state.lock().unwrap().gets, 11);
    let max = mock.max_in_flight.load(Ordering::SeqCst);
    assert!(max > 1 && max <= 4, "max in flight was {max}");
}

#[tokio::test]
async fn handles_empty_and_tiny_objects() {
    let mut objects = object("empty", Vec::new());
    objects.extend(object("tiny", b"hi".to_vec()));
    let mock = MockS3::start(MockState {
        objects,
        ..Default::default()
    })
    .await;
    let dir = temp_dir("tiny");

    let p = download(&mock.client(), "bucket", "empty")
        .to_dir(&dir)
        .send()
        .await
        .unwrap();
    assert_eq!(std::fs::read(p).unwrap(), b"");
    let p = download(&mock.client(), "bucket", "tiny")
        .to_file(dir.join("renamed.txt"))
        .parallel(16)
        .send()
        .await
        .unwrap();
    assert_eq!(std::fs::read(p).unwrap(), b"hi");
    assert_eq!(files_in(&dir), ["empty", "renamed.txt"]);
}

#[tokio::test]
async fn retries_a_range_cut_off_mid_stream() {
    let data = random_bytes(300_000, 2);
    let mock = MockS3::start(MockState {
        objects: object("f", data.clone()),
        cut_once: HashSet::from(["bytes=100000-199999".to_owned()]),
        ..Default::default()
    })
    .await;
    let dir = temp_dir("retry");
    let last = Arc::new(AtomicUsize::new(0));
    let last2 = last.clone();

    let path = download(&mock.client(), "bucket", "f")
        .to_dir(&dir)
        .part_size(100_000)
        .on_progress(move |p| last2.store(p.downloaded_bytes as usize, Ordering::SeqCst))
        .send()
        .await
        .unwrap();

    assert_eq!(std::fs::read(path).unwrap(), data);
    assert_eq!(
        last.load(Ordering::SeqCst),
        data.len(),
        "progress from the cut attempt was rolled back"
    );
    assert_eq!(mock.state.lock().unwrap().gets, 4);
}

#[tokio::test]
async fn missing_object_reports_404_and_leaves_nothing() {
    let mock = MockS3::start(MockState::default()).await;
    let dir = temp_dir("missing");
    let err = download(&mock.client(), "bucket", "nope")
        .to_dir(&dir)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(404), "{err}");
    assert!(files_in(&dir).is_empty());
}

#[tokio::test]
async fn object_changed_mid_download_fails_without_retry() {
    let data = random_bytes(400_000, 3);
    let mock = MockS3::start(MockState {
        objects: object("f", data),
        get_delay: Duration::from_millis(50),
        ..Default::default()
    })
    .await;
    let dir = temp_dir("changed");
    let state = mock.state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        state.lock().unwrap().objects.get_mut("f").unwrap().1 = "\"etag-2\"".into();
    });

    let err = download(&mock.client(), "bucket", "f")
        .to_dir(&dir)
        .part_size(100_000)
        .parallel(1)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(412), "{err}");
    assert!(files_in(&dir).is_empty());
}

#[tokio::test]
async fn wrong_region_is_reported_with_the_real_region() {
    let mock = MockS3::start(MockState {
        objects: object("f", b"x".to_vec()),
        bucket_region: Some("eu-west-1".into()),
        ..Default::default()
    })
    .await;
    let err = download(&mock.client(), "bucket", "f")
        .to_dir(temp_dir("region"))
        .send()
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::WrongRegion { region } if region == "eu-west-1"),
        "{err}"
    );
}

#[tokio::test]
async fn dropping_the_future_cancels_and_removes_the_temp_file() {
    let mock = MockS3::start(MockState {
        objects: object("slow.bin", random_bytes(500_000, 4)),
        get_delay: Duration::from_secs(5),
        ..Default::default()
    })
    .await;
    let dir = temp_dir("cancel");
    let fut = download(&mock.client(), "bucket", "slow.bin")
        .to_dir(&dir)
        .part_size(100_000)
        .send();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), fut)
            .await
            .is_err()
    );
    assert!(
        files_in(&dir).is_empty(),
        "left behind: {:?}",
        files_in(&dir)
    );
}

#[tokio::test]
async fn rejects_invalid_options() {
    let mock = MockS3::start(MockState::default()).await;
    let err = download(&mock.client(), "b", "k")
        .parallel(0)
        .send()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidOption(_)));
    let err = download(&mock.client(), "b", "k")
        .part_size(0)
        .send()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidOption(_)));
}

// ---- CLI ----

async fn run_cli(args: &[&str], cwd: &Path) -> (i32, String, String) {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_s3-multipartdownload"))
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_CONFIG_FILE", "/nonexistent")
        .env("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .output()
        .await
        .unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

#[tokio::test]
async fn cli_downloads_end_to_end() {
    let data = random_bytes(700_001, 5);
    let mock = MockS3::start(MockState {
        objects: object("data/big.bin", data.clone()),
        ..Default::default()
    })
    .await;
    let dir = temp_dir("cli");
    let ep = mock.endpoint.as_str();

    let (code, stdout, stderr) = run_cli(
        &[
            "s3://bucket/data/big.bin",
            "out/",
            "--endpoint",
            ep,
            "--path-style",
            "-p",
            "4",
            "-s",
            "128k",
        ],
        &dir,
    )
    .await;
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(stdout.trim(), "out/big.bin");
    assert_eq!(stderr, "", "not a terminal, so no progress");
    assert_eq!(std::fs::read(dir.join("out/big.bin")).unwrap(), data);

    let (code, _, stderr) = run_cli(
        &[
            "s3://bucket/data/big.bin",
            "copy.bin",
            "--endpoint",
            ep,
            "--path-style",
            "--no-sign-request",
        ],
        &dir,
    )
    .await;
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(std::fs::read(dir.join("copy.bin")).unwrap(), data);
}

#[tokio::test]
async fn cli_errors_and_exit_codes() {
    let mock = MockS3::start(MockState::default()).await;
    let dir = temp_dir("cli-errors");

    let (code, _, stderr) = run_cli(
        &[
            "s3://bucket/missing.bin",
            "--endpoint",
            &mock.endpoint,
            "--path-style",
        ],
        &dir,
    )
    .await;
    assert_eq!(code, 1);
    assert!(
        stderr.contains("s3://bucket/missing.bin not found"),
        "{stderr}"
    );
    assert!(files_in(&dir).is_empty());

    let (code, _, stderr) = run_cli(&["bucket/key"], &dir).await;
    assert_eq!(code, 2);
    assert!(stderr.contains("s3://bucket/key"), "{stderr}");

    let (code, _, _) = run_cli(&["s3://b/k", "--bogus"], &dir).await;
    assert_eq!(code, 2);

    let (code, _, _) = run_cli(&["s3://b/k", "-p", "0"], &dir).await;
    assert_eq!(code, 2);

    let (code, stdout, _) = run_cli(&["--version"], &dir).await;
    assert_eq!(code, 0);
    assert_eq!(
        stdout.trim(),
        format!("s3-multipartdownload {}", env!("CARGO_PKG_VERSION"))
    );
}
