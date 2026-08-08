#!/usr/bin/env node
// Read-only API + static dashboard for klend-indexer.
//
// SECURITY MODEL, and the reason this file looks the way it does.
//
// The first version of this proxy accepted a POST body and forwarded it to
// ClickHouse verbatim as SQL, authenticated as `default`, with no auth and a
// firewall rule open to 0.0.0.0/0. That is an unauthenticated public admin SQL
// endpoint: any host on the internet could have run DROP DATABASE klend. The
// data behind it is not reconstructible, because Yellowstone serves only the
// chain tip and cannot re-serve old slots, so a drop is permanent loss rather
// than a restore. klend.slot_gaps already carries one row proving what that
// costs: 73,874 slots lost to an 8h outage on 2026-08-05.
//
// Three properties follow from that, and none of them are optional:
//
//   1. The client cannot supply SQL. Every statement is fixed in QUERIES below
//      and selected by name. There is no code path from request data into a
//      query string, so there is nothing to inject into.
//   2. The upstream credential is a SELECT-only user, never `default`. Boot
//      fails closed if that is violated. Defence in depth: if a query in this
//      file were ever wrong, the database still refuses to mutate.
//   3. Every query is cost-capped. ClickHouse Cloud is billed per byte scanned
//      and this dashboard polls on a timer, so an uncapped query is a billing
//      incident waiting to happen, whether or not anyone is malicious.

const http = require('http');
const fs = require('fs');
const path = require('path');
const { loadConfig, createClient, QUERIES } = require('./chclient.js');

const LISTEN_PORT = parseInt(process.env.LISTEN_PORT || '8080', 10);
const CACHE_TTL_MS = parseInt(process.env.CACHE_TTL_MS || '10000', 10);

// The credential guards, the cost-capped request shape, and the statement list
// all live in chclient.js / queries.js, shared with web/export.js. Two consumers
// issuing the same queries must not each carry a copy: a drifted copy is a
// silently wrong answer, not a compile error.
let client;
try {
  client = createClient(loadConfig());
} catch (err) {
  console.error(`FATAL: ${err.message}`);
  process.exit(1);
}

// ── Response cache ───────────────────────────────────────────────────────────
// The dashboard polls every 15s and every panel runs FINAL over hundreds of
// thousands of rows. Without this, N open browser tabs multiply directly into
// the ClickHouse bill. With it, upstream load is bounded by TTL regardless of
// how many clients are watching.
const cache = new Map();

function runQuery(name) {
  const hit = cache.get(name);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return Promise.resolve(hit.payload);
  return client.query(name).then((payload) => {
    cache.set(name, { at: Date.now(), payload });
    return payload;
  });
}

// ── Static dashboard ─────────────────────────────────────────────────────────
let htmlContent = '';
try {
  htmlContent = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf-8');
  console.error(`loaded dashboard: ${(htmlContent.length / 1024).toFixed(1)} KB`);
} catch (e) {
  console.error(`WARNING: could not load index.html: ${e.message}`);
}

function send(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
    'X-Content-Type-Options': 'nosniff',
  });
  res.end(body);
}

const server = http.createServer((req, res) => {
  // The dashboard is served from this same origin, so no CORS header is set.
  // The previous `Access-Control-Allow-Origin: *` let any page on the internet
  // read this data from a visitor's browser; nothing here needs that.
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    if (!htmlContent) { res.writeHead(503, { 'Content-Type': 'text/plain' }); res.end('Dashboard not available'); return; }
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'Referrer-Policy': 'no-referrer',
    });
    res.end(htmlContent);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/healthz') {
    send(res, 200, { ok: true, endpoints: Object.keys(QUERIES) });
    return;
  }

  // Named, fixed queries only. `name` indexes a literal object and is never
  // concatenated into SQL, so an unknown name is a 404 and nothing more.
  if (req.method === 'GET' && url.pathname.startsWith('/api/')) {
    const name = url.pathname.slice('/api/'.length);
    if (!Object.prototype.hasOwnProperty.call(QUERIES, name)) {
      send(res, 404, { error: 'unknown endpoint' });
      return;
    }
    runQuery(name).then(
      (payload) => send(res, 200, payload),
      (err) => send(res, 502, { error: err.message })
    );
    return;
  }

  // The old arbitrary-SQL entry point. Answered explicitly rather than with a
  // generic 404 so that anything still pointed at it fails in an obvious way.
  if (req.method === 'POST') {
    send(res, 405, { error: 'this service does not accept SQL; use GET /api/<name>' });
    return;
  }

  send(res, 404, { error: 'not found' });
});

server.listen(LISTEN_PORT, () => {
  console.error(`klend-proxy listening on :${LISTEN_PORT}`);
  // Report only what is read back from the client's own config. An earlier
  // version restated the readonly setting as a literal here and kept printing
  // readonly=1 after the setting moved to 2, so the log asserted a security
  // property the process was not applying. Settings now live in queries.js and
  // are not duplicated in any log line.
  console.error(`upstream ${client.cfg.host}:${client.cfg.port} as user '${client.cfg.user}'`);
  console.error(`endpoints: ${Object.keys(QUERIES).map((k) => '/api/' + k).join(', ')}`);
});
