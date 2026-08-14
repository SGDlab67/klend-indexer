# demo/ — demo-day fallback package (2026-08-17)

Everything here exists so that if venue wifi or the cloud path dies on stage, the
demo still lands from local artifacts. Generated 2026-08-14 against the live
deployment (post-slice-fix).

## What's here

| Path | What |
|---|---|
| recording/*.webm | Screen recording of the live dashboard (42s, 1440x900, green liveness dot) |
| screenshots/*.png | full.png + per-view: overview, risk, liquidity, system, about |
| queries.sql | The three headline queries, tested against the klend_ro read-only user |
| results/*.tsv | Current result snapshots for those queries |
| numbers.md | Every demo number, each with its source query + result cited |
| script.md | 7-minute talk track, verbatim opening, war story, Q&A prep |

## The recording

`demo/recording/*.webm` — a full dashboard walkthrough (overview -> risk ->
liquidity/reserves -> system -> about) with the liveness dot green. Play it with
any browser or `open demo/recording/*.webm`. It is the fallback if the live
stream shows nothing on stage (expected: only ~8.4% of slots carry any klend
write, so a quiet live stream is normal, not broken).

## The three headline queries (demo/queries.sql, run against klend_ro)

1. **Health-factor distribution (A)** — count, min/p10/p25/median/p75/p90/p99/max
   health, plus at-risk count (health < 1.05). Result snapshot: ~76k valid
   snapshots, median health ~1.52, 5 at-risk.
2. **One obligation's history by pubkey (B)** — the richest obligation
   `BYojGuT56e2TUb8PQwRyT1wL5X5Ekv4kZH1HUQgBu6Zg`, 11,169 snapshots over ~8.7 days.
   Note: klend_ro caps results at 1000 rows, so the full history is paged with a
   (slot, write_version) cursor; results/b_obligation_history.tsv holds all 11,169.
3. **Row counts / ingest stats (C)** — total rows, distinct pubkeys, latest slot,
   checkpoint, ingest lag (seconds), unfilled gaps, snapshot count.

## Headline numbers (full sourcing in numbers.md)

- Rows ingested: 931,073 (FINAL 931,076)
- Obligations: 140,196 ever-seen, 56,990 active
- Accumulation: ~9.7 days, one logged 8h40m gap (2026-08-05)
- Ingest lag: 3s now (distribution p50 4s / p99 34s / p99.9 58s / max 90s)
- Throughput: ~7.45 KB/s post-fix; RPC ~$1-2/mo (0.02% of plan)
- Slot density: 8.40%
- Data-slice incident: 64,650 rows lost, 4,385 accounts (3,175 obligations), 158.91h

## Parquet export — NOT YET DONE

The last-resort offline dataset (account_updates + obligation_snapshots as
Parquet) has not been produced. The export OOMs on the e2-micro (966MB RAM); a
2GB swapfile is now in place on the VM but the export is still impractically slow
there. Three paths forward: (1) export on this Mac (M4, 32GB) after a temporary
ClickHouse IP-allowlist entry; (2) resize the VM temporarily; (3) let it grind on
the VM overnight (swap already configured). See NOTES.md "Day 9 (cont.)",
docs/coldpath.md §7, and deploy/run-parquet-export.sh (a ready runbook). The
deadline is the ClickHouse credit expiry 2026-08-19, not demo day itself.
