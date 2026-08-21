# klend-indexer

A Rust service that streams Kamino Lend (klend) account updates from a Solana
validator over Yellowstone gRPC and stores them as queryable history.

Status: **FROZEN** as of 2026-08-20. The stream is stopped, the full dataset is
exported to Parquet on GCS, and the ClickHouse service is idled. See
`deploy/FREEZE.md` for what freeze does and how to resume.

Dashboard: https://storage.googleapis.com/klend-indexer-dashboard/index.html
(numbers are frozen; the "updated Ns ago" liveness dot is stale by design)

## What it is

- Subscribes to a Yellowstone gRPC account stream, owner-filtered to the klend
  program, so only Kamino Lend accounts arrive.
- Derives Anchor discriminators at startup and uses them to tag each account
  update with its type (Reserve, Obligation, UserMetadata, and so on). Unknown
  discriminators are recorded as `untagged:<len>b` rather than dropped.
- Writes raw updates to ClickHouse `klend.account_updates` and decoded
  obligations to `klend.obligation_snapshots`.
- Exports the full history to Parquet on GCS and serves a static dashboard from
  the same bucket.

## Architecture

- **Ordering and idempotency:** tables are `ReplacingMergeTree` keyed on
  `pubkey + slot + write_version`. Replays from a reconnect collapse instead of
  duplicating. In the frozen dataset the raw and `FINAL` counts differ by 3 rows,
  all reconnect replays.
- **Resume:** a persisted checkpoint in `klend.ingest_checkpoint` gives the
  resume point. Restart subscribes with `from_slot` inclusive, so the boundary
  slot is re-read rather than skipped.
- **Gap accounting:** if the checkpoint is stale beyond the replay window, the
  service does not silently skip. It writes a row to `klend.slot_gaps` with the
  missed range and increments a metric.
- **Reconnects:** capped exponential backoff on stream failure, plus a watchdog
  that restarts a wedged container (`STALE_THRESHOLD=900s`).
- **Export:** Parquet written Hive-partitioned by `slot_bucket`, two table roots,
  staged on the VM and uploaded to `gs://klend-indexer-dashboard/klend-parquet/`.

## Measured numbers

Frozen dataset (compact summary refreshed 2026-08-16; full SQL and results in
`demo/numbers.md`):

| Metric | Value |
|---|---|
| Rows in `account_updates` | 1,043,542 (1,043,545 `FINAL`) |
| Obligations ever seen | 140,603 |
| Obligations currently debt-bearing | 57,083 |
| Accumulation span | ~11.1 days (2026-08-05 to 2026-08-16) |
| Ingest lag, live reading | 4 s |
| Ingest lag distribution | p50 4 s, p99 34 s, p99.9 58 s, max 90 s over 52,100 ingest-seconds |
| Throughput, post-fix steady state | ~7.45 KB/s |
| RPC spend | ~$1 to $2 per month (0.02% of the Alchemy plan, peak 390 of 10,000 CU/s) |
| Slot density | 8.29% (189,535 of 2,286,409 slots carry a klend write) |

Slot density matters for interpreting the stream: about 92% of slots contain no
klend write at all, so a quiet stream is normal rather than broken.

At freeze the exported dataset is ~1.1M `account_updates` rows and ~180K
`obligation_snapshots`, roughly 100 MiB.

## Failure modes handled

- **Stream disconnect.** Capped exponential backoff, resume from checkpoint.
- **Half-open connection (container alive, no data).** External watchdog on a
  freshness threshold restarts the container.
- **Duplicate delivery after reconnect.** Collapsed by the ReplacingMergeTree key.
- **Checkpoint too old to replay.** Recorded as a `slot_gaps` row plus a metric.
  The gap is visible in the data, not hidden.
- **Silent data corruption.** A per-kind data-distribution audit caught an
  `accounts_data_slice` misconfiguration that truncated Reserves to 4,664 B and
  emptied Obligation payloads. It ran for 158.91 h and lost 64,650 rows across
  4,385 accounts (3,175 confirmed obligations). Every liveness monitor
  (checkpoint, freshness, gap counter, watchdog) stayed green throughout; only
  the distribution audit saw that one kind's `max(slot)` had frozen while the
  others advanced. This is the strongest argument in the project for monitoring
  the shape of the data, not just its arrival.

## Failure modes not handled

- **The 73,873-slot backfill hole from 2026-08-05 is not filled.** An 8h40m wedge
  (slots 437,313,969 to 437,387,843) left one `slot_gaps` row with `filled = 0`.
  Yellowstone serves the tip and cannot re-serve those slots, and no alternate
  source was wired up. This is a permanent limitation of the dataset, not an open
  task. Any analysis crossing that window must account for it.
- **The 64,650 rows lost to the data-slice incident are gone**, for the same
  reason: the truncated payloads cannot be re-fetched from the stream.
- **One 4,664-byte account type was never identified.** Some updates remain
  tagged `untagged:*` because their discriminator was never matched to a known
  klend account layout.
- **No backfill path in general.** The service is tip-following only.
- **Frozen means frozen.** While the ClickHouse service is idled, live queries
  fail until `deploy/resume.sh` starts it. Parquet on GCS is the durable copy.

## How to run

- First deploy, VM setup, and configuration: `deploy/DEPLOY.md`.
- Stopping the stream and exporting to Parquet: `deploy/FREEZE.md`, run
  `./deploy/freeze.sh` from the repo root. Idempotent, safe to re-run.
- Bringing it back: `./deploy/resume.sh`. It restarts the ClickHouse service,
  re-runs the boot startup script to relaunch the indexer and watchdog, and
  re-enables the dashboard timer. The indexer resumes from its checkpoint, so
  nothing after the freeze point is lost.
- Refreshing the reported numbers: `demo/refresh-numbers.sh summary`.
