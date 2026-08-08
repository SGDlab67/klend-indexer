# Backfill (Phase 2): state of play

Date: 2026-08-06
Author: survey before writing any backfill code

## Status

Decided and done: **Option A** (current-state snapshot) shipped as
`src/bin/snapshot.rs`, run via `deploy/run-snapshot.sh`. Distinct accounts went
from 1,166 to 140,625. **Option B** (archival block timeline) is explicitly
declined: it spends an uncapped key to produce metadata about a hole rather than a
filling of it. **Option C** remains unavailable.

The three plumbing defects in section 4 are fixed; `deploy/ch-remote.sh` is the
new VM-tunneled SQL path. Section 2a's gap is still unrecorded in `slot_gaps`.
See the 2026-08-06 entry in NOTES.md for the full account.

## 1. Ground truth, measured

Queried ClickHouse Cloud (`Klend-Indexer`, service `78f807ed`) directly.

| Metric | Value |
|---|---|
| rows in `account_updates` | 49,626 |
| distinct accounts | 1,153 |
| slot range | 437,280,282 .. 437,478,741 |
| checkpoint | slot 437,478,808 |
| ingest lag | 11s (healthy) |
| rows in `slot_gaps` | **0** |

## 2. The premise needs correcting

Two assumptions in the Phase 2 brief do not hold.

### 2a. `slot_gaps` is empty. Gaps are not accumulating; one gap exists and was never recorded.

Derived the real holes by walking `account_updates` ordered by slot and looking
for jumps. Exactly one:

| start (exclusive) | end (inclusive) | slots missed | wall clock (UTC) | duration |
|---|---|---|---|---|
| 437,313,969 | 437,387,843 | 73,874 | 2026-08-05 04:50:15 -> 13:30:02 | 8h 40m |

This is the 2026-08-05 wedge incident, not a stream reconnect. The indexer hung
on a ClickHouse write while the container stayed `Up`, so neither
`record_gap` call site fired: both live on the reconnect path, and there was no
reconnect, just a frozen process. `slot_gaps` therefore records the class of gap
the indexer notices, not the class that has actually cost data so far.

Consequence for Phase 2: a backfill job that reads unfilled spans from
`slot_gaps` would today read zero rows and do nothing. The gap has to be
reconciled into the table first, and gap detection has to stop depending on
catching a reconnect in the act.

### 2b. `getProgramAccounts` cannot reconstruct historical account state.

The brief describes "an archive RPC ... with `getProgramAccounts` + block range
queries" writing "the missed account updates back into `account_updates`".
`getProgramAccounts` has no at-slot variant on Helius, Triton, or Alchemy: it
returns present state only. There is no supported way to ask any of them for the
bytes of a Kamino Reserve as of slot 437,320,000.

What archival RPC can actually supply for a past span:

- **Which** accounts were written, and at **which** slot, by walking `getBlock`
  over the span and keeping transactions that invoke the klend program. The
  writable account keys per transaction give a write timeline.
- **Not** the post-write account bytes. Transaction meta carries pre/post
  balances and token balances, nothing else. Reconstructing a Reserve's bytes
  would mean replaying klend's instruction logic off-chain, which is a
  reimplementation of the program, not a backfill.

So `account_updates.data` for the gap span is unrecoverable. That is a property
of Solana RPC, not of this indexer.

## 3. What is actually recoverable

Ranked by value per unit of effort and spend.

### Option A: current-state snapshot (`getProgramAccounts`, one call)

Fetch every account owned by the klend program right now and write it at the
current slot. Does not fill history, but it does fix a different and arguably
worse hole: the stream only ever sees accounts that *update*, so any klend
account that has been idle since 2026-08-05 00:52 is absent from the dataset
entirely. 1,153 accounts observed so far is a floor, not the universe.

Cost: one RPC call on the existing Alchemy key. Effort: small.

### Option B: write timeline for the gap span (`getBlock` x 73,874)

Recovers slot + pubkey + "was written" for the 8h40m hole, with `data` left
empty and a provenance marker. Answers "when did this obligation change"
across the gap; cannot answer "what did it hold".

Cost: 73,874 archival block fetches. On Alchemy this bills against the same
usage-based key CLAUDE.md flags as a payment instrument with no ceiling, so it
needs a bandwidth estimate and a spend cap before it runs. Effort: real.

### Option C: true historical bytes

Not available from Helius, Triton, or Alchemy. Would require a provider serving
per-slot account snapshots. Treat as out of scope.

## 4. Unrelated defects found while surveying

Both make the babysit runbook non-functional and should be fixed regardless of
which backfill option is chosen.

1. **The VM is in the wrong project for every documented command.** `klend-indexer`
   lives in `agentbiz-sungodlab`; the active gcloud project is
   `gen-lang-client-0502946726`. `deploy/health-check.sh` and both escalation
   steps in the runbook omit `--project`, so they resolve against the default
   project and fail with "resource not found".
2. **The Mac can no longer reach ClickHouse Cloud.** The service IP access list
   is now a single entry, the VM's external IP. `health-check.sh` queries
   `:8443` from the Mac, so it times out with curl (28) every run. That is the
   source of the `rows= accounts= last_slot= lag=s` output in the last loop
   iteration, and it means the health check has been blind, not healthy.
3. **`health-check.sh` reports a connection failure as STALE, not unreachable.**
   The `|| { ... exit 2; }` guard is attached to `read`, but the failure happens
   inside the `$(...)` it consumes, so `read` succeeds on an empty string, `LAG`
   ends up empty, and the `${LAG:-99999}` default routes it down the stale path.
