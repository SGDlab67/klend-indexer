# demo/ — demo-day fallback package (2026-08-17)

Everything here exists so that if venue wifi or the cloud path dies on stage, the
demo still lands from local artifacts. Generated 2026-08-14 against the live
deployment (post-slice-fix); headline numbers refreshed 2026-08-16.

## What's here

| Path | What |
|---|---|
| recording/*.webm | Screen recording of the live dashboard (42s, 1440x900, green liveness dot) |
| screenshots/*.png | full.png + per-view: overview, risk, liquidity, system, about |
| queries.sql | The three headline queries, tested against the klend_ro read-only user |
| results/*.tsv | Current result snapshots for those queries |
| numbers.md | Every demo number, each with its source query + result cited |
| script.md | 7-minute talk track, verbatim opening, war story, Q&A prep (numbers filled) |
| slides.html | Self-contained presentation deck (dark, offline, keyboard nav, fullscreen) |
| rehearsal.md | Rehearsal runbook: beats, click-by-click demo, fallback drills, pre-flight |

## The recording

`demo/recording/*.webm` — a full dashboard walkthrough (overview -> risk ->
liquidity/reserves -> system -> about) with the liveness dot green. Play it with
any browser or `open demo/recording/*.webm`. It is the fallback if the live
stream shows nothing on stage (expected: only ~8.3% of slots carry any klend
write, so a quiet live stream is normal, not broken).

Note: recording/, screenshots/, and results/*.tsv are frozen 2026-08-14 captures,
so they show the then-current numbers (931,073 rows, 11,169 snapshots). The
headline numbers below are the live 2026-08-16 readings. The transition line for
the recording covers this: "and here is the same run, recorded."

## The three headline queries (demo/queries.sql, run against klend_ro)

1. **Health-factor distribution (A)** — count, min/p10/p25/median/p75/p90/p99/max
   health, plus at-risk count (health < 1.05). Result snapshot: ~76k valid
   snapshots, median health ~1.51, 5 at-risk.
2. **One obligation's history by pubkey (B)**: the richest obligation
   `BYojGuT56e2TUb8PQwRyT1wL5X5Ekv4kZH1HUQgBu6Zg`. Now 16,538 snapshots over ~10.1
   days (11,169 over ~8.7 days when the Aug-14 results/ snapshot was captured).
   klend_ro caps results at 1000 rows, so the full history is paged with a
   (slot, write_version) cursor; results/b_obligation_history.tsv holds the Aug-14
   capture of 11,169.
3. **Row counts / ingest stats (C)** — total rows, distinct pubkeys, latest slot,
   checkpoint, ingest lag (seconds), unfilled gaps, snapshot count.

## Headline numbers (full sourcing in numbers.md)

- Rows ingested: 1,043,542 (FINAL 1,043,545)
- Obligations: 140,603 ever-seen, 57,083 active
- Accumulation: ~11.1 days, one logged 8h40m gap (2026-08-05)
- Ingest lag: 4s now (distribution p50 4s / p99 34s / p99.9 58s / max 90s)
- Throughput: ~7.45 KB/s post-fix; RPC ~$1-2/mo (0.02% of plan)
- Slot density: 8.3%
- Data-slice incident: 64,650 rows lost, 4,385 accounts (3,175 obligations), 158.91h

## Parquet export — DONE

The last-resort offline dataset (account_updates + obligation_snapshots as
Parquet) has been exported and lives in two places:

- **Local:** `demo/parquet/` (gitignored) — account_updates 932,930 rows in 3
  partitions, obligation_snapshots 163,533 rows in 2 partitions, ~91 MB total.
- **GCS:** `gs://klend-indexer-dashboard/klend-parquet/` (durable; survives the
  2026-08-19 ClickHouse credit expiry).

Verified: `coldquery gaps` derives the one logged gap (437,313,969 → 437,387,843,
73,873 slots) from the data, matching `klend.slot_gaps` exactly.

Query it offline with:

  KLEND_PARQUET_DIR=demo/parquet ./target/debug/coldquery sql "SELECT ..."
  KLEND_PARQUET_DIR=demo/parquet ./target/debug/coldquery activity
  KLEND_PARQUET_DIR=demo/parquet ./target/debug/coldquery gaps

Note: the export ran against the LIVE stream (~65s window), so the exporter's
"DEFECT: source reported N but M exported" line is a false positive from the
writer racing the export (~29 new rows landed mid-export), not a data-integrity
bug. See NOTES.md "Day 9 (cont. II)". The e2-micro cannot run this export (OOMs);
it needs a real-RAM host and, unless run on the VM with swap, a temporary
allowlist entry like this run used.
