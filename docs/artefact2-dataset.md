# Artefact 2: dataset specification

**Date:** 2026-08-09
**Status:** specification only. Nothing here is published yet.
**Source plan:** `lanes-abc-execution-plan.md` §Artefact 2
**Deadline pressure:** ClickHouse Cloud credits expire **2026-08-19**. Whatever
is not exported to Parquet by then is not recoverable from this system.

This document states what the dataset *is*, what it honestly covers, and which
of the plan's five candidate datasets are actually reachable from it. It is
deliberately written before publication, because the plan's definition of done
requires a domain expert to engage with it, and a domain expert will check the
coverage claims first.

---

## 1. What exists right now

Measured against production, 2026-08-10 01:07 UTC.

| Metric | Value |
|---|---|
| Raw account updates | 517,404 |
| Distinct accounts | 141,624 |
| Slot range | 437,280,282 – 438,304,801 (span 1,024,519) |
| Wall-clock range | 2026-08-05 00:52 – 2026-08-10 01:07 |
| Decoded Obligation snapshots | 163,184 across 140,185 accounts |
| Recorded slot gaps | 1 (437,313,969 – 437,387,843, 73,873 slots) |

By account type:

| Kind | Rows | Share | Accounts | Slot range |
|---|---|---|---|---|
| Reserve | 334,192 | 64.59% | 558 | 437,280,282 – 438,304,801 |
| Obligation | 165,066 | 31.90% | 140,200 | 437,280,282 – **437,903,877** |
| `untagged:0b` | 17,289 | 3.34% | 1,708 | **437,903,892** – 438,304,801 |
| LendingMarket | 482 | 0.09% | 212 | 437,291,009 – 438,292,148 |
| UserMetadata | 365 | 0.07% | 346 | 437,282,214 – 437,903,116 |
| ReferrerTokenState | 8 | 0.00% | 2 | 437,636,745 – 437,638,519 |
| `unknown:0000…` | 2 | 0.00% | 1 | 437,560,435 – 437,606,511 |

## 2. Two holes, stated up front

A dataset's value is set by what a skeptic can verify about its gaps, so both
are stated before any claim about what the data can answer.

### Hole 1 — the outage: 73,873 slots

2026-08-05, slots 437,313,969 – 437,387,843, roughly 8h40m. The indexer wedged;
`--restart always` could not see it because the process never exited. Recorded
in `slot_gaps`, independently re-derivable from the data itself via
`coldquery gaps`. Unfillable: Yellowstone serves only the tip.

This one is honest by construction. It is in the table, it is in the writeup,
and the derivation query agrees with the recorded bounds.

### Hole 2 — the payload truncation: Obligation and UserMetadata, ~44 hours

From slot **437,903,892** (2026-08-08 02:02 UTC) onward, a request-level
`accounts_data_slice { offset: 0, length: 4664 }` caused Yellowstone to return
**empty** payloads for every account shorter than 4,664 bytes. Obligation is
3,344 bytes; UserMetadata is 1,032. Both went to zero-length rows classified
`untagged:0b`.

Note the arithmetic: Obligation stops at 437,903,877 and `untagged:0b` begins at
437,903,892. There is no overlap. It is a clean cutover, not a degradation.

Of the 1,708 accounts appearing as `untagged:0b`, 1,396 were previously seen as
Obligations. All 17,289 rows carry `lamports > 0` with zero bytes of data, which
is the shape that made the fault detectable at all (see `src/payload.rs`).

**This hole was invisible to every guard the project had**, because it was not a
stall: rows arrived at full rate, freshness stayed at seconds, no slot was
missed. It was found by auditing the data, not by monitoring.

Unfillable for the same reason as Hole 1. **Current** Obligation state is
recoverable via a `getProgramAccounts` snapshot; the intra-slot history of those
44 hours is not.

### The resulting honest coverage statement

> Reserve state is continuous across the full span except the recorded 73,873-slot
> outage. Obligation state is continuous from 437,280,282 to 437,903,877, then
> absent until the fix is deployed. Do not use this dataset for any analysis that
> requires Obligation state after slot 437,903,877.

## 3. Which candidate datasets survive contact with this

The plan lists five. Assessed against what is actually captured, not against
what a lending indexer could capture in principle.

| Candidate | Reachable? | Why |
|---|---|---|
| Liquidation forensics | **No** | Requires transaction and instruction data: liquidator identity, tip paid, realised bonus. This indexer subscribes to **accounts only**. There is no transaction subscription, so liquidator identity is not in the data at any slot. This is a structural gap, not a coverage gap. |
| Health-factor distribution over time | **Yes, partially** | The only candidate reachable today. 163,184 snapshots with `health_factor_bps` populated across 140,185 accounts, slots 437,394,119 – 437,903,892. Ends at the truncation. |
| Time-to-liquidation survival curves | **Not yet** | Needs continuous Obligation history through a liquidation event, plus the event itself. Blocked on both Hole 2 and the missing transaction stream. |
| Oracle-deviation sensitivity | **Partially** | Reserve carries oracle configuration and is the best-covered type (334,192 rows, 558 accounts, full span). Needs price history joined from outside this dataset. |
| Liquidator concentration & latency ranking | **No** | Same structural gap as liquidation forensics. Liquidator identity lives in transactions. |

**The uncomfortable finding: four of five candidates need transaction data this
indexer does not collect.** The plan's framing ("Concrete candidates from a
lending index") assumed an account index implied them. It does not. Either the
subscription grows a transaction stream, or Artefact 2 is scoped to the
state-distribution questions that account data genuinely answers.

That is a scoping decision, not a defect, and it is better discovered now than
in a governance-forum reply.

## 4. What Artefact 2 should actually be

Given the above, the defensible dataset is the second row of the table, scoped
tightly and published with its holes documented:

> **Kamino Lend obligation health distribution, sub-minute resolution.**
> Every observed change to every Kamino Lend obligation's health factor, at the
> slot it changed, for 140,185 obligations. Freshness measured at p50 4s /
> p99 34s from chain to queryable, against Dune's 1–60 min and Flipside's ~15 min.

This satisfies the plan's "why it's possible at all" test on freshness and on
program-specific semantics, and it does not depend on data the indexer never
collected.

The value columns added on 2026-08-09 (`deposited_value_sf`,
`borrowed_value_sf`, `borrow_factor_adjusted_debt_sf`, `allowed_borrow_value_sf`,
`unhealthy_borrow_value_sf`, `lowest_deposit_liquidation_ltv`) make the
distribution legible in dollar terms rather than only as a ratio. **They are
currently zero in all 163,184 rows** because the migration is applied but the
binary that writes them has not been deployed. They populate going forward, not
retroactively.

## 5. Publication shape

- **Format:** Parquet, ZSTD, Hive-partitioned `slot_bucket={:012}` at 1,000,000
  slots per partition. Written by `bin/parquet_export`.
- **Ordering:** by `slot, write_version, pubkey`. This is the whole point of the
  cold path: ClickHouse orders by `(pubkey, slot, write_version)` to serve
  "this account's history", which serves "all accounts around slot N" badly, and
  every dashboard query pays `FINAL` at read time. The export resolves the
  ReplacingMergeTree dedup once.
- **Companion files, non-optional:**
  - `gaps.parquet` — both holes, machine-readable, with the derivation query
    that reproduces them.
  - `README.md` — §2 of this document, verbatim. The coverage statement leads;
    it does not appear in a footnote.
  - `schema.md` — column semantics, and specifically that `*_sf` columns are raw
    U68F60 fixed-point (`FRACTION_ONE_SCALED = 1 << 60`), undivided by design so
    a wrong scale stays a one-line fix at the query edge rather than baked into
    history.

## 6. Blocking sequence

1. **Deploy the fix.** Until then Obligation coverage does not resume and Hole 2
   keeps growing. Currently blocked on a permission denial, not on code.
2. **Snapshot current Obligation state** via `getProgramAccounts` (`bin/snapshot`)
   to re-establish the 140,200 accounts at a known slot, with value columns.
3. **Export to Parquet/GCS before 2026-08-19.** Hard deadline; credits expire.
4. **Decide the transaction-stream question.** Four of five candidates depend on
   it. This is the scoping call that determines whether Artefact 2 is the
   health-distribution dataset or something larger.
5. Only then Artefact 3, per the plan's own caveat: build the engine after the
   dataset reveals which queries actually recur.

---

## Related
- `docs/coldpath.md` — export and query mechanics
- `src/payload.rs` — the guard that makes Hole 2 non-repeatable
- `src/resume.rs` — the guard that makes Hole 1 detectable
- `lanes-abc-execution-plan.md` §Artefact 2
