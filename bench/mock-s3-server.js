// A tiny S3-compatible HTTP server (HEAD/GET + Range + If-Match) that serves an
// in-memory object with simulated network limits:
//   - per-connection bandwidth cap (S3 throughput per TCP stream is limited)
//   - aggregate bandwidth cap (your NIC / instance network ceiling)
//   - time-to-first-byte latency per request
const http = require('http')
const crypto = require('crypto')

const CHUNK = 64 * 1024

function startMockS3({ objects, perConnMBps = 40, aggregateMBps = 400, latencyMs = 20 }) {
    const perConnRate = perConnMBps * 1024 * 1024 / 1000      // bytes per ms
    const aggregateRate = aggregateMBps * 1024 * 1024 / 1000
    let aggregateNextFree = 0
    const etags = new Map()
    for (const [key, buf] of objects) {
        etags.set(key, '"' + crypto.createHash('md5').update(buf).digest('hex') + '"')
    }

    const server = http.createServer(async (req, res) => {
        const key = decodeURIComponent(req.url.split('?')[0].replace(/^\/[^/]+\//, ''))
        const data = objects.get(key)
        if (!data) {
            res.writeHead(404, { 'Content-Type': 'application/xml' })
            return res.end('<Error><Code>NoSuchKey</Code></Error>')
        }
        const etag = etags.get(key)
        if (req.headers['if-match'] && req.headers['if-match'] !== etag) {
            res.writeHead(412, { 'Content-Type': 'application/xml' })
            return res.end('<Error><Code>PreconditionFailed</Code></Error>')
        }

        let start = 0
        let end = data.length - 1
        const m = /^bytes=(\d+)-(\d*)$/.exec(req.headers.range || '')
        if (m) {
            start = Number(m[1])
            end = m[2] ? Math.min(Number(m[2]), data.length - 1) : end
        }
        const headers = {
            'Content-Length': data.length ? end - start + 1 : 0,
            'ETag': etag,
            'Accept-Ranges': 'bytes',
            'Content-Type': 'application/octet-stream',
            'Last-Modified': new Date(0).toUTCString()
        }
        if (m) headers['Content-Range'] = `bytes ${start}-${end}/${data.length}`

        await sleep(latencyMs)
        res.writeHead(m ? 206 : 200, headers)
        if (req.method === 'HEAD' || !data.length) return res.end()

        let connNextFree = Date.now()
        for (let off = start; off <= end; off += CHUNK) {
            const chunk = data.subarray(off, Math.min(off + CHUNK, end + 1))
            const now = Date.now()
            const slot = Math.max(now, aggregateNextFree)
            aggregateNextFree = slot + chunk.length / aggregateRate
            connNextFree = Math.max(connNextFree, now) + chunk.length / perConnRate
            const wait = Math.max(aggregateNextFree, connNextFree) - now
            if (wait >= 1) await sleep(wait)
            if (!res.write(chunk)) await new Promise(r => res.once('drain', r))
            if (res.destroyed) return
        }
        res.end()
    })
    server.keepAliveTimeout = 60_000

    return new Promise(resolve => server.listen(0, '127.0.0.1', () => {
        resolve({ server, endpoint: `http://127.0.0.1:${server.address().port}` })
    }))
}

const sleep = ms => new Promise(r => setTimeout(r, ms))

module.exports = { startMockS3 }
