// ClickHouse access shared by the live proxy and the static exporter.
//
// Both consumers need the same three things and must not each reimplement them:
// the credential guards, the cost-capped request shape, and the TSV-to-rows
// parse. The guards especially: web/export.js runs unattended on a timer, so a
// misconfiguration there is even less likely to be noticed than in a server that
// someone is watching.

const https = require('https');
const { QUERIES, CH_SETTINGS } = require('./queries.js');

function loadConfig() {
  const cfg = {
    host: process.env.CH_HOST,
    port: parseInt(process.env.CH_PORT || '8443', 10),
    user: process.env.CH_USER || 'klend_ro',
    pass: process.env.CH_PASSWORD || '',
  };

  // Fail closed, loudly, at startup rather than at the first query. A process
  // that starts and then errors on every request is harder to notice than one
  // that never starts.
  if (!cfg.host) {
    throw new Error('CH_HOST not set. The hostname is deliberately not baked into this image.');
  }
  if (!cfg.pass) {
    throw new Error('CH_PASSWORD not set');
  }
  // The guard that would have prevented the 2026-08-07 incident, in which a
  // network-reachable process held the admin credential. Overridable only by an
  // explicit, obviously-named variable, so running as an admin user is always a
  // deliberate act visible in the container config.
  if (cfg.user === 'default' && process.env.ALLOW_ADMIN_DB_USER !== 'yes-i-mean-it') {
    throw new Error(
      'refusing to run as ClickHouse user "default"; this process must hold a ' +
      'SELECT-only credential. Create one with deploy/create-readonly-user.sh, ' +
      'then set CH_USER to it.'
    );
  }
  return cfg;
}

function createClient(cfg) {
  const auth = Buffer.from(`${cfg.user}:${cfg.pass}`).toString('base64');
  const settings = new URLSearchParams(CH_SETTINGS).toString();

  // `name` indexes a literal object and is never concatenated into SQL, so an
  // unknown name is an error here and never reaches the database.
  function query(name) {
    if (!Object.prototype.hasOwnProperty.call(QUERIES, name)) {
      return Promise.reject(new Error(`unknown query '${name}'`));
    }
    return new Promise((resolve, reject) => {
      const req = https.request(
        {
          hostname: cfg.host,
          port: cfg.port,
          path: `/?${settings}`,
          method: 'POST',
          rejectUnauthorized: true,
          timeout: 20000,
          headers: {
            Authorization: `Basic ${auth}`,
            'Content-Type': 'text/plain; charset=utf-8',
            Accept: 'text/tab-separated-values',
          },
        },
        (res) => {
          let data = '';
          res.on('data', (c) => { data += c; });
          res.on('end', () => {
            if (res.statusCode !== 200) {
              // Upstream text can carry schema and settings detail, so it is
              // logged here and never propagated to a caller that might render it.
              console.error(`upstream ${res.statusCode} for '${name}': ${data.slice(0, 300)}`);
              reject(new Error('upstream query failed'));
              return;
            }
            const rows = data.trim().split('\n').filter(Boolean).map((l) => l.split('\t'));
            resolve({ rows, count: rows.length });
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

  return { query, cfg };
}

module.exports = { loadConfig, createClient, QUERIES, CH_SETTINGS };
