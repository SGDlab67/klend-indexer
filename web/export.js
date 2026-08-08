#!/usr/bin/env node
// Generate the static dashboard payload.
//
// WHY STATIC
// The live version of this dashboard was an HTTP service on the indexer VM,
// reachable from the internet. Even after it was reduced to fixed read-only
// queries under a SELECT-only credential, the residual risk was co-location:
// code execution in that process lands on the single host in ClickHouse's IP
// allowlist, with a metadata server that hands out the VM service account, which
// reads Secret Manager, which holds the ADMIN ClickHouse password and the
// Alchemy token. Scoping the proxy's own credential does nothing about the
// box's.
//
// A generator removes the exposure rather than guarding it. Nothing listens.
// The output is data at rest in object storage, so the reachable surface is a
// bucket serving two files, and the dashboard stays publicly linkable.
//
// Writes stats.json into an output directory. Uploading is deploy/'s job, not
// this script's: this process holds a database credential and should not also
// hold storage write access.
//
// The page itself is NOT produced here. index.html changes when the markup
// changes, which is a git event, not a 60-second event, so it is published once
// per deploy by deploy/install-dashboard-export.sh. An earlier version copied it
// out of the image on every run, which coupled a text edit to an image rebuild
// and then served a stale page because the worker never pulled the new image.
//
// Usage: OUT_DIR=/tmp/dash node export.js

const fs = require('fs');
const path = require('path');
const { loadConfig, createClient, QUERIES } = require('./chclient.js');

const OUT_DIR = process.env.OUT_DIR || '/tmp/klend-dashboard';

async function main() {
  const client = createClient(loadConfig());
  const names = Object.keys(QUERIES);

  // Run sequentially, not with Promise.all. This is a background timer job on a
  // shared 966 MB box whose important tenant is the indexer, and the queries all
  // use FINAL. Four concurrent merges to save two seconds on a job that runs
  // every 60 seconds is a bad trade against the process that must not stall.
  const data = {};
  for (const name of names) {
    const started = Date.now();
    data[name] = (await client.query(name)).rows;
    console.error(`  ${name}: ${data[name].length} rows in ${Date.now() - started}ms`);
  }

  // generated_at is the freshness signal the page renders as "updated Ns ago".
  // It is this process's clock, deliberately: it answers "when was this page
  // built", which is the question a viewer is actually asking. Chain freshness
  // is a separate number and comes from the `system` query.
  const payload = { generated_at: new Date().toISOString(), ...data };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const statsPath = path.join(OUT_DIR, 'stats.json');
  fs.writeFileSync(statsPath, JSON.stringify(payload));

  const bytes = fs.statSync(statsPath).size;
  console.error(`wrote ${OUT_DIR}/stats.json (${bytes} B)`);
}

main().catch((err) => {
  // Exit nonzero so the systemd unit records a failure. A generator that fails
  // silently leaves the last good stats.json in place, and a stale dashboard
  // that looks live is the same class of problem as a container that reports
  // "Up" while the stream is dead.
  console.error(`FATAL: ${err.message}`);
  process.exit(1);
});
