<div align="center">

# ⚡ s3-multipartdownload (Rust)

**Fast, parallel S3 downloads as a single binary or a Rust library.**
Splits large Amazon S3 objects into byte ranges, fetches them concurrently with the AWS SDK for Rust, and writes each one straight to its place on disk. Memory use stays flat whatever the file size.

[![crates.io](https://img.shields.io/crates/v/s3-multipartdownload?color=2a78d6)](https://crates.io/crates/s3-multipartdownload)
[![docs.rs](https://img.shields.io/docsrs/s3-multipartdownload)](https://docs.rs/s3-multipartdownload)
[![CI](https://github.com/ranjithkumarmv/s3-multipartdownload-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/ranjithkumarmv/s3-multipartdownload-rs/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/s3-multipartdownload)](#license)

Rust port of the [`s3-multipartdownload`](https://github.com/ranjithkumarmv/s3-multipartdownload) npm package. It has the same behaviour and CLI flags, with no Node.js or AWS CLI needed.

**Up to 8.9× faster than a single stream** ([simulated benchmark](#benchmarks)) · ~25–40 MB of memory · one static binary

</div>

## Install the command

**macOS / Linux, prebuilt binary:**

```bash
curl -fsSL https://raw.githubusercontent.com/ranjithkumarmv/s3-multipartdownload-rs/main/install.sh | sh
```

It installs to `~/.local/bin` (change it with `INSTALL_DIR=...`) and checks the SHA-256 of the download. Linux binaries are static (musl), so they run on any distribution, including Amazon Linux 2.

**Windows:** download `s3-multipartdownload-x86_64-pc-windows-msvc.zip` from the [latest release](https://github.com/ranjithkumarmv/s3-multipartdownload-rs/releases/latest).

**With Cargo:**

```bash
cargo binstall s3-multipartdownload   # prebuilt binary in seconds (needs cargo-binstall)
cargo install s3-multipartdownload    # or compile from source (a few minutes)
```

Cargo has no `npx`-style "run without installing". [`cargo binstall`](https://github.com/cargo-bins/cargo-binstall) is the closest: it fetches the same prebuilt release binary instead of compiling.

## Use the command

```bash
s3-multipartdownload s3://my-bucket/datasets/big-file.parquet
```

It uses the same credentials as the AWS CLI (environment variables, `~/.aws/config`, SSO, instance roles). It prints the saved path on stdout and shows progress on stderr. Ctrl-C cancels the download and removes the partial file. If the bucket is in a different region than the one configured, the command follows S3 to the right region.

```bash
# Into a directory, with 16 parallel ranges
s3-multipartdownload s3://my-bucket/video.mp4 ./downloads/ -p 16

# To a specific file name, with a named profile
s3-multipartdownload s3://my-bucket/logs.tar.gz ./backup.tar.gz --profile prod

# Public bucket, no credentials
s3-multipartdownload s3://noaa-goes16/index.html --no-sign-request

# MinIO or another S3-compatible service
s3-multipartdownload s3://assets/build.tar.gz --endpoint http://localhost:9000 --path-style
```

| Flag | Default | Description |
| --- | --- | --- |
| `[destination]` | `./<key basename>` | An existing directory or a path ending in `/` saves inside it. Anything else is the output file path |
| `-p, --parallel <n>` | `8` | Concurrent range requests |
| `-s, --part-size <size>` | `8MiB` | Bytes per range, such as `16MiB`, `16M` or `16777216` |
| `-r, --retries <n>` | `3` | Extra attempts per part on network or 5xx errors |
| `--region <region>` | from env/profile | AWS region |
| `--profile <name>` | | Named profile from `~/.aws/config` |
| `--endpoint <url>` | | S3-compatible endpoint (MinIO, R2, Wasabi…) |
| `--path-style` | | Path-style URLs, needed by MinIO and some others |
| `--no-sign-request` | | Anonymous access for public buckets |
| `--version-id <id>` | | Download a specific object version |
| `--request-payer` | | Acknowledge requester-pays charges |
| `-q, --quiet` | | No progress output |

Exit codes: `0` success, `1` download failed, `2` bad arguments, `130` cancelled.

## Use the library

```toml
[dependencies]
s3-multipartdownload = { version = "0.1", default-features = false } # drop the CLI's dependencies
aws-config = "1"
aws-sdk-s3 = "1"
tokio = { version = "1", features = ["full"] }
```

```rust
#[tokio::main]
async fn main() -> Result<(), s3_multipartdownload::Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    let path = s3_multipartdownload::download(&client, "my-bucket", "datasets/big.parquet")
        .to_dir("./downloads")       // or .to_file("./big.parquet")
        .parallel(16)                // default 8
        .part_size(16 * 1024 * 1024) // default 8 MiB
        .on_progress(|p| eprintln!("{}/{}", p.downloaded_bytes, p.total_bytes))
        .send()
        .await?;

    println!("saved to {}", path.display());
    Ok(())
}
```

To cancel, drop the future, for example with `tokio::time::timeout` or `tokio::select!`. In-flight ranges are aborted and the partial file is removed.

If the bucket is in another region than the client, `send()` returns `Error::WrongRegion { region }`. Build a client for that region and call it again.

## How it works

1. **`HeadObject`** reads the size and ETag.
2. A **pre-sized temp file** (`<file>.download`) lets every range write at its own offset, so there are no part files and no merge step.
3. Ranges are fetched by up to `parallel` tasks. Each request sends `If-Match: <ETag>`, so all the bytes come from the same object version.
4. A failed range is **retried with backoff**, including one that fails mid-stream, and its progress is rolled back. On a fatal error, ranges still running are cancelled and the temp file is deleted.
5. On success the temp file is **renamed** to the final name.

## Benchmarks

**500 MB object**, median of 5 runs, every output checked with SHA-256 ([full results](./bench/results/RESULTS.md)):

| Strategy | Time | Throughput | Speed-up | CPU time | Peak memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Single stream (baseline) | 14.43 s | 34.7 MB/s | 1.00× | 1.40 s | 20 MB |
| Rust ×4 | 4.35 s | 114.9 MB/s | 3.32× | 1.07 s | 28 MB |
| Rust ×8 | 2.47 s | 202.4 MB/s | 5.84× | 1.09 s | 28 MB |
| **Rust ×16** | **1.63 s** | **306.7 MB/s** | **8.85×** | 1.12 s | **38 MB** |
| Node.js npm CLI ×16 | 1.65 s | 303.0 MB/s | 8.75× | 1.17 s | 112 MB |

**100 MB object:** 3.06 s → 0.49 s (**6.24×**) with Rust ×16, in 24 MB of memory. The Node CLI took 0.43 s with 114 MB.

**What this shows:**
- Parallel ranges give the speed-up. Rust and Node reach about the same throughput, because the network is the limit.
- The Rust binary uses **3–5× less memory** than the Node CLI and slightly less CPU. It also needs no runtime, which helps on small containers, Lambda and CI runners.

> [!NOTE]
> **These numbers come from a simulation, not live S3.** The benchmark runs a local S3-compatible server that caps each connection at 40 MB/s, total bandwidth at 400 MB/s, and adds 20 ms of latency per request. It ran on an Apple M1 (8 cores), with s3-multipartdownload 0.1.0 and npm CLI 2.1.0 on Node 22. Each trial is a fresh process timed by `/usr/bin/time`, so startup counts. Real-world gains depend on your network, region and instance type.

Reproduce it (needs Node.js for the mock server; `--node-cli` is optional):

```bash
cargo build --release
node bench/benchmark.js --runs 5 --node-cli path/to/s3-multipartdownload/cli.js
```

## Choosing `--parallel` and `--part-size`

- **Laptops / small instances:** `-p 4` to `-p 8`
- **EC2 in the same region as the bucket:** `-p 16` to `-p 32`; raise it until throughput stops improving
- Keep **`--part-size` at 8–16 MiB**. Smaller parts add request overhead and cost (each part is a billed `GET`); larger parts retry more slowly.

## Development

```bash
cargo test      # unit tests + end-to-end tests against an in-process mock S3
cargo clippy --all-targets
```

Pushing a `v*` tag builds binaries for Linux (x86_64, arm64, static musl), macOS (Apple Silicon, Intel) and Windows, and attaches them to a GitHub release.

## License

[MIT](./LICENSE)
