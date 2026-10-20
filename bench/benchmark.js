#!/usr/bin/env node
// Benchmark the Rust CLI (and optionally the npm CLI) against a throttled,
// S3-compatible server running locally, so results are repeatable.
//
//   cargo build --release
//   node bench/benchmark.js [--sizes 100,500] [--runs 3] [--parallel 4,8,16]
//                           [--per-conn 40] [--aggregate 400] [--latency 20]
//                           [--node-cli path/to/s3-multipartdownload/cli.js]
//
// Each trial runs in a fresh process under /usr/bin/time to capture wall time,
// CPU time and peak memory. Every output file is checked with SHA-256.
const fs = require('fs')
const os = require('os')
const path = require('path')
const crypto = require('crypto')
const { execFile } = require('child_process')
const { promisify } = require('util')
const { startMockS3 } = require('./mock-s3-server')
const execFileAsync = promisify(execFile)

const args = parseArgs(process.argv.slice(2))
const runs = Number(args.runs || 3)
const parallels = String(args.parallel || '4,8,16').split(',').map(Number)
const rustBin = path.resolve(__dirname, '..', 'target', 'release', 's3-multipartdownload')
const nodeCli = args['node-cli'] && path.resolve(args['node-cli'])
const outDir = fs.mkdtempSync(path.join(os.tmpdir(), 's3mpd-rs-bench-'))
const resultsDir = path.join(__dirname, 'results')

if (!fs.existsSync(rustBin)) {
    console.error('Build first: cargo build --release')
    process.exit(1)
}

const strategies = [
    // One range covering the whole object over one connection: a plain GetObject.
    { label: 'Single stream (baseline)', cmd: [rustBin], extra: ['-p', '1', '-s', '100G'] },
    ...parallels.map(p => ({ label: `Rust ×${p}`, cmd: [rustBin], extra: ['-p', String(p)] })),
    ...(nodeCli ? [{ label: `Node.js (npm) ×${parallels.at(-1)}`, cmd: [process.execPath, nodeCli], extra: ['-p', String(parallels.at(-1))] }] : [])
]

async function main() {
    const net = {
        perConnMBps: Number(args['per-conn'] || 40),
        aggregateMBps: Number(args.aggregate || 400),
        latencyMs: Number(args.latency || 20)
    }
    const sizes = String(args.sizes || '100,500').split(',').map(Number)
    const objects = new Map(sizes.map(mb => [`object-${mb}MB.bin`, crypto.randomBytes(mb * 1024 * 1024)]))
    const { server, endpoint } = await startMockS3({ objects, ...net })

    const report = {
        date: new Date().toISOString().slice(0, 10),
        platform: `${os.platform()} ${os.arch()} (${os.cpus()[0].model}, ${os.cpus().length} cores)`,
        rustVersion: (await execFileAsync(rustBin, ['--version'])).stdout.trim(),
        nodeCliVersion: nodeCli ? (await execFileAsync(process.execPath, [nodeCli, '--version'])).stdout.trim() : null,
        network: net,
        runs,
        cases: []
    }
    try {
        for (const [key, buf] of objects) {
            const expected = crypto.createHash('sha256').update(buf).digest('hex')
            const c = { key, sizeMB: buf.length / 1024 / 1024, results: [] }
            for (const s of strategies) {
                const trials = []
                for (let i = 0; i < runs; i++) trials.push(await trial(s, endpoint, key, expected))
                trials.sort((a, b) => a.seconds - b.seconds)
                const median = trials[Math.floor(trials.length / 2)]
                c.results.push({ label: s.label, ...median })
                console.error(`${key}  ${s.label.padEnd(26)} ${median.seconds.toFixed(2)} s  ` +
                    `cpu ${median.cpuSeconds.toFixed(2)} s  rss ${median.maxRssMB.toFixed(0)} MB`)
            }
            const base = c.results[0].seconds
            for (const r of c.results) {
                r.throughputMBps = c.sizeMB / r.seconds
                r.speedup = base / r.seconds
            }
            report.cases.push(c)
        }
    } finally {
        server.close()
        fs.rmSync(outDir, { recursive: true, force: true })
    }

    fs.mkdirSync(resultsDir, { recursive: true })
    fs.writeFileSync(path.join(resultsDir, 'results.json'), JSON.stringify(report, null, 2) + '\n')
    fs.writeFileSync(path.join(resultsDir, 'RESULTS.md'), toMarkdown(report))
    console.log(toMarkdown(report))
}

async function trial(strategy, endpoint, key, expectedSha) {
    const out = path.join(outDir, 'out.bin')
    const [exe, ...pre] = strategy.cmd
    const timeArgs = os.platform() === 'darwin' ? ['-l'] : ['-v']
    const env = { ...process.env, AWS_ACCESS_KEY_ID: 'bench', AWS_SECRET_ACCESS_KEY: 'bench', AWS_REGION: 'us-east-1' }
    // Async so the in-process mock server keeps serving while the trial runs.
    const { stderr } = await execFileAsync('/usr/bin/time', [...timeArgs, exe, ...pre,
        `s3://bench/${key}`, out, '--endpoint', endpoint, '--path-style', '-q', ...strategy.extra], { env })

    const sha = crypto.createHash('sha256').update(fs.readFileSync(out)).digest('hex')
    fs.rmSync(out)
    if (sha !== expectedSha) throw new Error(`${strategy.label}: checksum mismatch`)
    return { ...parseTime(stderr), sha256Ok: true }
}

// Parse BSD (`time -l`, bytes) or GNU (`time -v`, KiB) output.
function parseTime(text) {
    const num = re => Number((re.exec(text) || [])[1])
    if (/maximum resident set size/.test(text) && /\breal\b/.test(text)) {
        return {
            seconds: num(/([\d.]+)\s+real/),
            cpuSeconds: num(/([\d.]+)\s+user/) + num(/([\d.]+)\s+sys/),
            maxRssMB: num(/(\d+)\s+maximum resident set size/) / 1024 / 1024
        }
    }
    const [, m, s] = /Elapsed \(wall clock\) time.*?: (?:(\d+):)?([\d.:]+)/.exec(text) || []
    return {
        seconds: Number(m || 0) * 60 + Number(s),
        cpuSeconds: num(/User time \(seconds\): ([\d.]+)/) + num(/System time \(seconds\): ([\d.]+)/),
        maxRssMB: num(/Maximum resident set size \(kbytes\): (\d+)/) / 1024
    }
}

function toMarkdown(r) {
    const lines = [
        '# Benchmark results',
        '',
        `- **Mode:** simulated S3 (local server, ${r.network.perConnMBps} MB/s per connection, ` +
            `${r.network.aggregateMBps} MB/s total, ${r.network.latencyMs} ms latency)`,
        `- **Machine:** ${r.platform}`,
        `- **Binaries:** ${r.rustVersion}${r.nodeCliVersion ? ` (Rust), npm CLI ${r.nodeCliVersion} on Node ${process.version}` : ''}`,
        `- **Part size:** 8 MiB · **Runs:** median of ${r.runs} · **Date:** ${r.date}`,
        ''
    ]
    for (const c of r.cases) {
        lines.push(`## ${c.sizeMB} MB object`, '',
            '| Strategy | Time | Throughput | Speed-up | CPU time | Peak RSS | SHA-256 |',
            '| --- | ---: | ---: | ---: | ---: | ---: | :---: |')
        for (const x of c.results) {
            lines.push(`| ${x.label} | ${x.seconds.toFixed(2)} s | ${x.throughputMBps.toFixed(1)} MB/s | ` +
                `${x.speedup.toFixed(2)}× | ${x.cpuSeconds.toFixed(2)} s | ${x.maxRssMB.toFixed(0)} MB | ✅ |`)
        }
        lines.push('')
    }
    return lines.join('\n')
}

function parseArgs(argv) {
    const out = {}
    for (let i = 0; i < argv.length; i++) {
        if (argv[i].startsWith('--')) out[argv[i].slice(2)] = argv[i + 1]?.startsWith('--') ? true : argv[++i]
    }
    return out
}

main().catch(err => {
    console.error(err)
    process.exit(1)
})
