# Benchmark results

- **Mode:** simulated S3 (local server, 40 MB/s per connection, 400 MB/s total, 20 ms latency)
- **Machine:** darwin arm64 (Apple M1, 8 cores)
- **Binaries:** s3-multipartdownload 0.1.0 (Rust), npm CLI 2.1.0 on Node v22.16.0
- **Part size:** 8 MiB · **Runs:** median of 5 · **Date:** 2026-10-20

## 100 MB object

| Strategy | Time | Throughput | Speed-up | CPU time | Peak RSS | SHA-256 |
| --- | ---: | ---: | ---: | ---: | ---: | :---: |
| Single stream (baseline) | 3.06 s | 32.7 MB/s | 1.00× | 0.33 s | 19 MB | ✅ |
| Rust ×4 | 1.07 s | 93.5 MB/s | 2.86× | 0.28 s | 21 MB | ✅ |
| Rust ×8 | 0.69 s | 144.9 MB/s | 4.43× | 0.25 s | 23 MB | ✅ |
| Rust ×16 | 0.49 s | 204.1 MB/s | 6.24× | 0.25 s | 24 MB | ✅ |
| Node.js (npm) ×16 | 0.43 s | 232.6 MB/s | 7.12× | 0.32 s | 114 MB | ✅ |

## 500 MB object

| Strategy | Time | Throughput | Speed-up | CPU time | Peak RSS | SHA-256 |
| --- | ---: | ---: | ---: | ---: | ---: | :---: |
| Single stream (baseline) | 14.43 s | 34.7 MB/s | 1.00× | 1.40 s | 20 MB | ✅ |
| Rust ×4 | 4.35 s | 114.9 MB/s | 3.32× | 1.07 s | 28 MB | ✅ |
| Rust ×8 | 2.47 s | 202.4 MB/s | 5.84× | 1.09 s | 28 MB | ✅ |
| Rust ×16 | 1.63 s | 306.7 MB/s | 8.85× | 1.12 s | 38 MB | ✅ |
| Node.js (npm) ×16 | 1.65 s | 303.0 MB/s | 8.75× | 1.17 s | 112 MB | ✅ |
