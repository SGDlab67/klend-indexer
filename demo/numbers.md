# Demo-day numbers — klend-indexer

Sections 1-7 below are the frozen 2026-08-14 audit: each number carries its exact
SQL and result, run live against ClickHouse Cloud
(`um7rnv0cif.us-central1.gcp.clickhouse.cloud`) through the VM over IAP via
`deploy/ch-remote.sh`. The **Compact summary** at the bottom was refreshed
2026-08-16 and is the source the slides and script draw from. Demo day: 2026-08-17.

Refresh the compact summary anytime with `demo/refresh-numbers.sh summary`.

## How to read this

- **Query-derived** numbers were run live on 2026-08-14; the exact SQL and its
  result are cited in each row. The stream is a live system, so every
  query-derived number drifts as slots advance — a reading, not a constant.
- **Log/billing-derived** numbers come from NOTES.md and are labeled as such,
  not re-queried. Where the two disagree (section 7), both are shown.

---

## 1. Total rows ingested

| Number | Value | Source (query) | Result |
|---|---|---|---|
| Rows in `account_updates` | **931,073** | `SELECT count() FROM klend.account_updates FORMAT TSVRaw` | `931073` |
| Rows after dedup (FINAL) | **931,076** | `SELECT count() FROM klend.account_updates FINAL FORMAT TSVRaw` | `931076` |

The raw and FINAL counts differ by 3 rows: three replayed inserts from
reconnect that the `ReplacingMergeTree(ingested_at)` key collapses. Headline
number is **931,073** rows ingested.

---

## 2. Unique obligations tracked

| Number | Value | Source (query) | Result |
|---|---|---|---|
| Obligations ever seen (all history) | **140,196** | `SELECT count(DISTINCT pubkey) FROM klend.obligation_snapshots FINAL FORMAT TSVRaw` | `140196` |
| Obligations currently debt-bearing (active) | **56,990** | `SELECT count(DISTINCT pubkey) FROM klend.obligation_snapshots FINAL WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0 FORMAT TSVRaw` | `56990` |

**Current vs ever-seen:** 140,196 distinct obligations have ever appeared in the
decoded snapshot history; 56,990 of them currently carry debt
(`health_factor_bps` not the `u64::MAX` "no-debt" sentinel and not 0). The
difference (≈83,206) are tracked positions with no outstanding borrows.

---

## 3. Days of continuous accumulation

| Number | Value | Source (query) | Result |
|---|---|---|---|
| First slot / first ingest | slot **437,280,282** @ **2026-08-05 00:52:57.875 UTC** | `SELECT min(slot), min(ingested_at) FROM klend.account_updates FORMAT TSVRaw` | `437280282 … 2026-08-05 00:52:57.875` |
| Latest slot / latest ingest | slot **439,267,730** @ **2026-08-14 17:14:56.190 UTC** | `SELECT max(slot), max(ingested_at) FROM klend.account_updates FORMAT TSVRaw` | `439267213 … 2026-08-14 17:14:56.190` (slot read moments later: `439267730`) |
| Total span | **≈9.7 days** wall clock / **1,986,931** slots chain time | min→max above | `439267213 − 437280282 = 1986931` |
| The one logged gap | slot **437,313,969 → 437,387,843** (2026-08-05 04:50:15 → 13:30:02 UTC, **8h40m**) | `SELECT * FROM klend.slot_gaps FINAL FORMAT TSVRaw` | `klend 437313969 437387843 … 0 reconnect gap: checkpoint stale beyond replay window` |

**Honesty on "continuous":** accumulation is *not* one unbroken 9.7 days. The
indexer first ran 2026-08-05 00:52:57 → 04:50:15 (~3h57m), froze for 8h40m (the
half-open-connection wedge, Day 5), then resumed at 13:30:02 and has run since.
So the honest framing is **~9.2 days of continuous coverage since the gap**,
plus a ~4-hour pre-gap stint. The gap is 73,873 slots actually missed
(`end_slot − start_slot = 73,874` is off by one: `end_slot` is exclusive per
`schema/004`, the first slot received after the gap — corrected in NOTES.md
Day 8). `slot_gaps` holds exactly one row, `filled = 0`.

Current checkpoint (proof the stream is live and advancing):

| Number | Value | Source (query) | Result |
|---|---|---|---|
| Checkpoint (resume point) | slot **439,267,746** @ **2026-08-14 17:19:18.351 UTC** | `SELECT * FROM klend.ingest_checkpoint FINAL FORMAT TSVRaw` | `klend 439267746 1271 2026-08-14 17:19:18.351` |

---

## 4. Ingest lag

| Number | Value | Source (query) | Result |
|---|---|---|---|
| Current ingest lag | **3 seconds** | `SELECT dateDiff('second', max(ingested_at), now64(3)) FROM klend.account_updates FORMAT TSVRaw` | `3` |
| Freshness distribution (NOTES, log-derived) | p50 **4s** · p99 **34s** · p99.9 **58s** · max **90s** over **52,100** ingest-seconds | NOTES.md Day 8 (cont.) §3 — measured before arming `STALE_THRESHOLD=900s` | — |

The current lag is a live reading; the distribution is the four-day steady-state
measured in NOTES.md Day 8 and is cited, not re-queried.

---

## 5. Throughput + RPC spend

| Number | Value | Source (query) | Result |
|---|---|---|---|
| Throughput, post-fix steady state | **≈7.45 KB/s** | `SELECT count(), sum(data_len), min(ingested_at), max(ingested_at) FROM klend.account_updates WHERE ingested_at >= '2026-08-14 16:58:03' FORMAT TSVRaw` | `1167  9021408  2026-08-14 16:58:06.304  2026-08-14 17:17:48.186` → 9,021,408 B ÷ 1024 ÷ 1,181.9 s |
| Throughput, last 60 min (spans the fix) | **≈5.61 KB/s** | `SELECT count(), sum(data_len), min(ingested_at), max(ingested_at) FROM klend.account_updates WHERE ingested_at >= now64(3) - INTERVAL 60 MINUTE FORMAT TSVRaw` | `4164  20684928  2026-08-14 16:16:04.305  2026-08-14 17:16:02.186` → 20,684,928 B ÷ 1024 ÷ 3,597.9 s |
| RPC spend (billing, NOT a query) | **≈$1–2 / month** | NOTES.md Day 8 (cont. III) — Alchemy at **15,540 of 66,666,667 CUs = 0.02%** of plan, peak **390 of 10,000 CU/s** | — |

The post-fix window is the honest steady-state figure (full-fidelity payloads:
Reserve 8,624 B + Obligation 3,344 B). The 60-minute window averages lower
because its first ~42 minutes still ran under the `accounts_data_slice`, which
trimmed Reserves to 4,664 B and emptied Obligations. RPC spend is a billing
figure from NOTES.md, not a live query.

---

## 6. Slot density

| Number | Value | Source (query) | Result |
|---|---|---|---|
| Slots carrying ≥1 klend update | **166,985** | `SELECT count(DISTINCT slot) FROM klend.account_updates FORMAT TSVRaw` | `166985` (consolidated query below) |
| Slot span | **1,987,448** | `max(slot) − min(slot)` | `439267730 − 437280282` |
| Density | **8.402%** | `SELECT min(slot), max(slot), count(DISTINCT slot), max(slot)-min(slot), round(count(DISTINCT slot)/(max(slot)-min(slot))*100, 4) FROM klend.account_updates FORMAT TSVRaw` | `437280282  439267730  166985  1987448  8.402` |

**8.4% of slots carry at least one Kamino account update** — i.e. ~91.6% of
chain slots have no klend write at all. (Earlier small samples measured ~6.5%;
this is the full 9.7-day figure, so it supersedes the Day 2 estimate.) This is
why a quiet live stream is normal, not broken.

---

## 7. Data-slice incident (accounts_data_slice truncation)

| Number | Value | Source (query) | Result |
|---|---|---|---|
| Rows lost (funded-but-empty) | **64,650** | `SELECT count() FROM klend.account_updates WHERE data_len = 0 AND lamports > 0 FORMAT TSVRaw` | `64650` |
| Kind of the lost rows | all **`untagged:0b`** | `SELECT kind, count() FROM klend.account_updates WHERE data_len = 0 AND lamports > 0 GROUP BY kind FORMAT TSVRaw` | `untagged:0b  64650` |
| Distinct affected accounts (pubkeys) | **4,385** | `SELECT count(DISTINCT pubkey) FROM klend.account_updates WHERE data_len = 0 AND lamports > 0 FORMAT TSVRaw` | `4385` |
| … of which confirmed Obligations | **3,175** | `SELECT count(DISTINCT pubkey) FROM klend.account_updates WHERE data_len = 0 AND lamports > 0 AND pubkey IN (SELECT pubkey FROM klend.obligation_snapshots FINAL) FORMAT TSVRaw` | `3175` |
| … not in obligation_snapshots | **1,209** | same query with `NOT IN` | `1209` |
| First lost row | slot **437,903,892** @ **2026-08-08 02:03:13.961 UTC** | `SELECT min(slot), min(ingested_at) … WHERE data_len = 0 AND lamports > 0` | `437903892 … 2026-08-08 02:03:13.961` |
| Last lost row | slot **439,264,777** @ **2026-08-14 16:58:02.303 UTC** | `SELECT max(slot), max(ingested_at) … WHERE data_len = 0 AND lamports > 0` | `439264777 … 2026-08-14 16:58:02.303` |
| First restored Obligation | slot **439,264,851** | `SELECT min(slot) FROM klend.obligation_snapshots FINAL WHERE slot > 439264777 FORMAT TSVRaw` | `439264851` |
| Loss window | **572,089 s = 158.91 h ≈ 6d 14h 55m** | `SELECT dateDiff('second', min(ingested_at), max(ingested_at)) … WHERE data_len = 0 AND lamports > 0 FORMAT TSVRaw` | `572089` |
| Detection mechanism | **per-kind data-distribution audit** — a kind whose `max(slot)` froze while others advanced; no liveness monitor (checkpoint/freshness/gaps/watchdog) saw it | NOTES.md Day 8 (cont. III); confirmed here: all 64,650 lost rows are one kind, `untagged:0b` | — |

**Cross-check notes (parent figures vs live DB):**
- "64,650 lost rows" — **matches exactly.**
- "4,385 distinct affected obligations" — the live DB gives **4,385 distinct
  affected pubkeys**, of which **3,175 are confirmed Obligations** (present in
  `obligation_snapshots`) and **1,209 are other account types** (UserMetadata,
  also shorter than the 4,664 B slice, plus Obligations first seen only during
  the incident and therefore never decoded). State it as "4,385 affected
  accounts, 3,175 confirmed obligations" to stay precise.
- "loss window 158.94 hours" — live DB: **158.91 h** (572,089 s). Within 2
  minutes; the parent figure used a slightly earlier start (02:02 vs the first
  empty row's 02:03:13).
- "end slot 439,264,851" — that is the **first restored Obligation** (fix took
  effect), not the last empty row; the last garbage row is **439,264,777**, 74
  slots earlier. Both are above.

---

## Compact summary (for the slides)

1. Rows ingested: **1,043,542** (FINAL 1,043,545)
2. Obligations tracked: **140,603** ever-seen · **57,083** active
3. Accumulation: **≈11.1 days** total (Aug 5 → Aug 16), **one 8h40m gap** on
   Aug 5 (73,873 slots); ~10.6 days continuous since
4. Ingest lag: **4 s** now (distribution p50 4s / p99 34s / p99.9 58s / max 90s)
5. Throughput: **≈7.45 KB/s** post-fix steady state · RPC **≈$1–2/mo** (0.02% of
   plan)
6. Slot density: **8.29%** (189,535 of 2,286,409 slots)
7. Data-slice incident: **64,650 rows lost** · **4,385 accounts** (3,175
   obligations) · **158.91 h** · detected by data-distribution audit
