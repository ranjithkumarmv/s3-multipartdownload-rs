use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use clap::{ArgAction, Parser};
use s3_multipartdownload::{DEFAULT_PARALLEL, DEFAULT_RETRIES, Error, Progress};

const EXAMPLES: &str = "\
Destination:
  omitted                  save as ./<basename of key>
  existing dir, or ends /  save inside that directory
  anything else            save to exactly that file path

Examples:
  s3-multipartdownload s3://my-bucket/data/big.parquet
  s3-multipartdownload s3://my-bucket/video.mp4 ./downloads/ -p 16
  s3-multipartdownload s3://assets/build.tar.gz --endpoint http://localhost:9000 --path-style

Prints the saved file path to stdout. Progress goes to stderr.
Exit codes: 0 ok, 1 download failed, 2 bad arguments, 130 cancelled.";

/// Download one S3 object using parallel byte-range requests.
#[derive(Parser, Debug)]
#[command(name = "s3-multipartdownload", version, disable_version_flag = true, after_help = EXAMPLES)]
struct Cli {
    /// Source object, e.g. s3://my-bucket/path/to/file
    source: String,

    /// Output file or directory (see below)
    destination: Option<PathBuf>,

    /// Concurrent range requests
    #[arg(short, long, default_value_t = DEFAULT_PARALLEL, value_parser = clap::builder::RangedI64ValueParser::<usize>::new().range(1..=1024))]
    parallel: usize,

    /// Bytes per range: 8MiB, 16M or 1048576
    #[arg(short = 's', long, default_value = "8MiB", value_parser = parse_size)]
    part_size: u64,

    /// Extra attempts per part on network/5xx errors
    #[arg(short, long, default_value_t = DEFAULT_RETRIES)]
    retries: u32,

    /// AWS region (default: AWS_REGION or your profile)
    #[arg(long)]
    region: Option<String>,

    /// Named profile from ~/.aws/config
    #[arg(long)]
    profile: Option<String>,

    /// S3-compatible endpoint (MinIO, R2, Wasabi, ...)
    #[arg(long)]
    endpoint: Option<String>,

    /// Use path-style URLs (needed by MinIO and some others)
    #[arg(long)]
    path_style: bool,

    /// Anonymous access, for public buckets
    #[arg(long)]
    no_sign_request: bool,

    /// Download a specific object version
    #[arg(long)]
    version_id: Option<String>,

    /// Acknowledge requester-pays charges
    #[arg(long)]
    request_payer: bool,

    /// No progress output
    #[arg(short, long)]
    quiet: bool,

    /// Show the version
    #[arg(short = 'v', long, action = ArgAction::Version)]
    version: (),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let (bucket, key) = match parse_s3_url(&cli.source) {
        Ok(parts) => parts,
        Err(msg) => {
            eprintln!("error: {msg}\n\nRun with --help for usage.");
            return ExitCode::from(2);
        }
    };

    let progress = (!cli.quiet && std::io::stderr().is_terminal()).then(ProgressLine::new);
    let run = async {
        let client = build_client(&cli, cli.region.clone()).await;
        match start(&cli, &client, &bucket, &key, progress.as_ref()).await {
            // Follow S3 to the bucket's real region once, like the AWS CLI does.
            Err(Error::WrongRegion { region }) if cli.endpoint.is_none() => {
                let client = build_client(&cli, Some(region)).await;
                start(&cli, &client, &bucket, &key, progress.as_ref()).await
            }
            result => result,
        }
    };

    let result = tokio::select! {
        result = run => result,
        _ = tokio::signal::ctrl_c() => {
            // Dropping the download aborted its parts and removed the temp file.
            if let Some(p) = &progress { p.finish() }
            eprintln!("Cancelled.");
            return ExitCode::from(130);
        }
    };
    if let Some(p) = &progress {
        p.finish();
    }

    match result {
        Ok(path) => {
            println!("{}", path.display());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("error: {}", describe_error(&err, &cli, &bucket, &key));
            ExitCode::from(1)
        }
    }
}

async fn start(
    cli: &Cli,
    client: &Client,
    bucket: &str,
    key: &str,
    progress: Option<&ProgressLine>,
) -> Result<PathBuf, Error> {
    let mut download = s3_multipartdownload::download(client, bucket, key)
        .parallel(cli.parallel)
        .part_size(cli.part_size)
        .retries(cli.retries)
        .request_payer(cli.request_payer);
    download = match &cli.destination {
        None => download,
        Some(dest) if is_dir_like(dest) => download.to_dir(dest),
        Some(dest) => download.to_file(dest),
    };
    if let Some(v) = &cli.version_id {
        download = download.version_id(v);
    }
    if let Some(p) = progress {
        let p = p.clone();
        download = download.on_progress(move |progress| p.update(progress));
    }
    download.send().await
}

async fn build_client(cli: &Cli, region: Option<String>) -> Client {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(profile) = &cli.profile {
        loader = loader.profile_name(profile);
    }
    if let Some(region) = region {
        loader = loader.region(Region::new(region));
    }
    if cli.no_sign_request {
        loader = loader.no_credentials();
    }
    let shared = loader.load().await;
    let mut config = aws_sdk_s3::config::Builder::from(&shared);
    if shared.region().is_none() {
        // Any region works as a starting point: S3 tells us the real one.
        config = config.region(Region::new("us-east-1"));
    }
    if let Some(endpoint) = &cli.endpoint {
        config = config.endpoint_url(endpoint);
    }
    if cli.path_style {
        config = config.force_path_style(true);
    }
    Client::from_conf(config.build())
}

fn parse_s3_url(url: &str) -> Result<(String, String), String> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| format!("source must look like s3://bucket/key, got {url:?}"))?;
    match rest.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
            if key.ends_with('/') {
                Err(format!(
                    "{url:?} looks like a folder; this tool downloads a single object"
                ))
            } else {
                Ok((bucket.to_owned(), key.to_owned()))
            }
        }
        _ => Err(format!(
            "source must look like s3://bucket/key, got {url:?}"
        )),
    }
}

// Accepts plain bytes or a K/M/G suffix (binary units): 8MiB, 8M, 8mb, 16k.
fn parse_size(text: &str) -> Result<u64, String> {
    let err = || format!("expected a size like 8MiB or 1048576, got {text:?}");
    let lower = text.trim().to_ascii_lowercase();
    let unitless = lower
        .strip_suffix("ib")
        .or_else(|| lower.strip_suffix('b'))
        .unwrap_or(&lower);
    let (number, multiplier) = match unitless.chars().last() {
        Some('k') => (&unitless[..unitless.len() - 1], 1024u64),
        Some('m') => (&unitless[..unitless.len() - 1], 1024u64.pow(2)),
        Some('g') => (&unitless[..unitless.len() - 1], 1024u64.pow(3)),
        _ => (unitless, 1),
    };
    let number = number.trim();
    if number.is_empty() || !number.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return Err(err());
    }
    let value: f64 = number.parse().map_err(|_| err())?;
    let bytes = (value * multiplier as f64).floor() as u64;
    if bytes == 0 { Err(err()) } else { Ok(bytes) }
}

fn is_dir_like(dest: &std::path::Path) -> bool {
    let s = dest.as_os_str().to_string_lossy();
    s.ends_with('/') || s.ends_with(std::path::MAIN_SEPARATOR) || dest.is_dir()
}

// Turn SDK errors into something a person at a terminal can act on.
fn describe_error(err: &Error, cli: &Cli, bucket: &str, key: &str) -> String {
    let target = format!("s3://{bucket}/{key}");
    let detail = err.to_string();
    match err.status() {
        Some(404) => format!("{target} not found (check the bucket, key and region)"),
        Some(403) if cli.no_sign_request => format!("access denied to {target}"),
        Some(403) => format!(
            "access denied to {target}. Check your credentials or --profile, or use --no-sign-request for a public bucket"
        ),
        Some(412) => format!("{target} changed while downloading; run the command again"),
        None if detail.contains("credentials") => {
            "no AWS credentials found. Run `aws configure`, set AWS_PROFILE or pass --profile, \
             or use --no-sign-request for a public bucket"
                .to_owned()
        }
        _ => detail,
    }
}

/// A single self-overwriting progress line on stderr, redrawn at most every 100 ms.
#[derive(Clone)]
struct ProgressLine(std::sync::Arc<Mutex<ProgressState>>);

struct ProgressState {
    started: Instant,
    last_draw: Option<Instant>,
    last: Progress,
}

impl ProgressLine {
    fn new() -> Self {
        Self(std::sync::Arc::new(Mutex::new(ProgressState {
            started: Instant::now(),
            last_draw: None,
            last: Progress {
                downloaded_bytes: 0,
                total_bytes: 0,
            },
        })))
    }

    fn update(&self, progress: Progress) {
        let mut state = self.0.lock().unwrap();
        state.last = progress;
        let now = Instant::now();
        if state
            .last_draw
            .is_some_and(|t| now - t < Duration::from_millis(100))
        {
            return;
        }
        state.last_draw = Some(now);
        draw(&state);
    }

    fn finish(&self) {
        let state = self.0.lock().unwrap();
        if state.last.total_bytes > 0 {
            draw(&state);
        }
        if state.last_draw.is_some() {
            eprintln!();
        }
    }
}

fn draw(state: &ProgressState) {
    let Progress {
        downloaded_bytes,
        total_bytes,
    } = state.last;
    let pct = if total_bytes == 0 {
        100.0
    } else {
        downloaded_bytes as f64 / total_bytes as f64 * 100.0
    };
    let rate = downloaded_bytes as f64 / state.started.elapsed().as_secs_f64().max(0.001);
    let mut stderr = std::io::stderr().lock();
    let _ = write!(
        stderr,
        "\r\x1b[K{pct:>5.1}%  {} / {}  {}/s",
        format_bytes(downloaded_bytes as f64),
        format_bytes(total_bytes as f64),
        format_bytes(rate)
    );
    let _ = stderr.flush();
}

fn format_bytes(mut n: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut i = 0;
    while n >= 1024.0 && i < UNITS.len() - 1 {
        n /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n:.0} B")
    } else {
        format!("{n:.1} {}", UNITS[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_accepts_bytes_and_binary_suffixes() {
        assert_eq!(parse_size("1048576"), Ok(1048576));
        assert_eq!(parse_size("8MiB"), Ok(8 * 1024 * 1024));
        assert_eq!(parse_size("8M"), Ok(8 * 1024 * 1024));
        assert_eq!(parse_size("16mb"), Ok(16 * 1024 * 1024));
        assert_eq!(parse_size("512k"), Ok(512 * 1024));
        assert_eq!(parse_size("1.5G"), Ok(1024 * 1024 * 1024 * 3 / 2));
        for bad in ["", "0", "abc", "8 TB", "-1", "M"] {
            assert!(parse_size(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn parse_s3_url_splits_bucket_and_key() {
        assert_eq!(
            parse_s3_url("s3://b/a/b/c.bin"),
            Ok(("b".into(), "a/b/c.bin".into()))
        );
        for bad in ["b/k", "s3://b", "s3://b/", "s3:///k", "s3://b/folder/"] {
            assert!(parse_s3_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
