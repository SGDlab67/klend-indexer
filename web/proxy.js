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
const https = require('https');
const fs = require('fs');
const path = require('path');

const CH_HOST = process.env.CH_HOST;
const CH_PORT = parseInt(process.env.CH_PORT || '8443', 10);
const CH_USER = process.env.CH_USER || 'klend_ro';
const CH_PASS = process.env.CH_PASSWORD || '';
const LISTEN_PORT = parseInt(process.env.LISTEN_PORT || '8080', 10);
const CACHE_TTL_MS = parseInt(process.env.CACHE_TTL_MS || '10000', 10);

// Fail closed, loudly, at boot rather than at the first request. A proxy that
// starts and then 500s is harder to notice than one that never starts.
if (!CH_HOST) {
  console.error('FATAL: CH_HOST not set. The hostname is deliberately not baked into this image.');
  process.exit(1);
}
if (!CH_PASS) {
  console.error('FATAL: CH_PASSWORD not set');
  process.exit(1);
}
// The guard that would have prevented the original incident. Overridable only
// by an explicit, obviously-named env var, so running as an admin user is
// always a deliberate act that shows up in the container config.
if (CH_USER === 'default' && process.env.ALLOW_ADMIN_DB_USER !== 'yes-i-mean-it') {
  console.error(
    'FATAL: refusing to start as ClickHouse user "default". This process is reachable ' +
    'from the network and must hold a SELECT-only credential. Create one with ' +
    'deploy/create-readonly-user.sh, then set CH_USER to it.'
  );
  process.exit(1);
}

const auth = Buffer.from(`${CH_USER}:${CH_PASS}`).toString('base64');

// Server-side cost ceilings, applied by ClickHouse itself, and the belt to the
// read-only user's braces: even a mistake in QUERIES cannot write.
//
// readonly=2, not 1, and the difference matters. readonly=1 forbids writes AND
// forbids changing settings, which would make ClickHouse reject every other
// parameter in this very object ("Cannot modify 'max_execution_time' setting in
// readonly mode"). readonly=2 forbids INSERT/ALTER/CREATE/DROP while still
// allowing a caller to TIGHTEN limits, which is exactly the combination wanted
// here. The user's own grants (SELECT on klend.* and nothing else) remain the
// primary control; this is the second one.
const CH_SETTINGS = new URLSearchParams({
  readonly: '2',
  max_execution_time: '15',
  max_result_rows: '1000',
  max_result_bytes: '10000000',
  max_rows_to_read: '500000000',
  result_overflow_mode: 'break',
  timeout_overflow_mode: 'break',
});

// ── The complete set of statements this service can execute ──────────────────
// Adding a capability means adding a named entry here, which is the point: the
// reachable query surface is reviewable in one place, in a diff.
const QUERIES = {
  overview: `
    SELECT
      (SELECT count(DISTINCT pubkey) FROM klend.reserve_snapshots FINAL) AS reserves,
      (SELECT count(DISTINCT pubkey) FROM klend.obligation_snapshots FINAL
       WHERE health_factor_bps != 18446744073709551615) AS active,
      (SELECT quantile(0.5)(health_factor_bps)/1000000 FROM klend.obligation_snapshots FINAL
       WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0) AS med_health,
      (SELECT count() FROM klend.obligation_snapshots FINAL
       WHERE health_factor_bps < 1050000 AND health_factor_bps > 0) AS at_risk,
      (SELECT quantile(0.25)(health_factor_bps)/1000000 FROM klend.obligation_snapshots FINAL
       WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0
         AND health_factor_bps < 10000000) AS p25
    FORMAT TSVRaw`,

  risk: `
    SELECT base58Encode(pubkey) AS pk, argMax(owner_b58, slot) AS ow,
           argMax(health_factor_bps, slot) AS hf,
           argMax(num_deposits, slot) AS dep, argMax(num_borrows, slot) AS bor
    FROM klend.obligation_snapshots FINAL
    WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0
    GROUP BY pubkey
    HAVING hf < 2000000
    ORDER BY hf ASC LIMIT 15
    FORMAT TSVRaw`,

  reserves: `
    SELECT base58Encode(pubkey) AS pk, argMax(available_amount, slot) AS avail,
           argMax(borrowed_amount_sf, slot) AS borr, argMax(mint_decimals, slot) AS dec,
           argMax(base58Encode(liquidity_mint), slot) AS liq_mint
    FROM klend.reserve_snapshots FINAL
    GROUP BY pubkey
    ORDER BY avail DESC LIMIT 15
    FORMAT TSVRaw`,

  system: `
    SELECT
      (SELECT max(slot) FROM klend.account_updates) AS latest_slot,
      (SELECT last_slot FROM klend.ingest_checkpoint FINAL WHERE stream='klend') AS checkpoint,
      (SELECT count() FROM klend.slot_gaps FINAL WHERE filled=0) AS unfilled_gaps,
      (SELECT count() FROM klend.account_updates) AS total_rows,
      (SELECT count() FROM klend.obligation_snapshots FINAL) AS snap_rows,
      (SELECT max(slot) FROM klend.reserve_snapshots FINAL) AS reserve_max
    FORMAT TSVRaw`,
};

// ── Response cache ───────────────────────────────────────────────────────────
// The dashboard polls every 15s and every panel runs FINAL over hundreds of
// thousands of rows. Without this, N open browser tabs multiply directly into
// the ClickHouse bill. With it, upstream load is bounded by TTL regardless of
// how many clients are watching.
const cache = new Map();

function runQuery(name) {
  const hit = cache.get(name);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return Promise.resolve(hit.payload);

  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: CH_HOST,
        port: CH_PORT,
        path: `/?${CH_SETTINGS.toString()}`,
        method: 'POST',
        rejectUnauthorized: true,
        timeout: 20000,
        headers: {
          Authorization: `Basic ${auth}`,
          'Content-Type': 'text/plain; charset=utf-8',
          Accept: 'text/tab-separated-values',
        },
      },
      (cres) => {
        let data = '';
        cres.on('data', (c) => { data += c; });
        cres.on('end', () => {
          if (cres.statusCode !== 200) {
            // Upstream text can carry schema details and settings, so it is
            // logged here and never returned to the client.
            console.error(`upstream ${cres.statusCode} for '${name}': ${data.slice(0, 300)}`);
            reject(new Error('upstream query failed'));
            return;
          }
          const rows = data.trim().split('\n').filter(Boolean).map((l) => l.split('\t'));
          const payload = { rows, count: rows.length };
          cache.set(name, { at: Date.now(), payload });
          resolve(payload);
        });
      }
    );
    req.on('timeout', () => { req.destroy(new Error('upstream timeout')); });
    req.on('error', (err) => {
      console.error(`upstream error for '${name}': ${err.message}`);
      reject(new Error('upstream unreachable'));
    });
    req.write(QUERIES[name]);
    req.end();
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
  // Read the value back from CH_SETTINGS rather than restating it. A hardcoded
  // copy here already drifted once: it still said readonly=1 after the setting
  // moved to 2, so the log asserted a security property the process was not
  // applying.
  console.error(`upstream ${CH_HOST}:${CH_PORT} as user '${CH_USER}' (readonly=${CH_SETTINGS.get('readonly')})`);
  console.error(`endpoints: ${Object.keys(QUERIES).map((k) => '/api/' + k).join(', ')}`);
});
