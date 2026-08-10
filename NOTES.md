# NOTES

Blocker log for klend-indexer. Plan §6 rule: every blocker + fix goes here.
This becomes README material in week 12.

## 2026-07-19 — Day 1

### Stale version pin from a code sample
A scaffold pinned `yellowstone-grpc-client = "10"`; actual latest was **13.2.1**.
Writing that by hand would have produced an API that didn't match any current docs.
**Lesson:** `cargo add`, never hand-copy a version from a code sample.

### client and proto version numbers don't match — and that's correct
`yellowstone-grpc-client 13.2.1` depends on `yellowstone-grpc-proto ^12.5.0`, and 12.5.0
is proto's own latest. The two crates version independently despite shipping from one repo.
Checked because a mismatch would normally mean two copies of the proto crate compiled in,
producing the confusing `expected SubscribeRequest, found SubscribeRequest` class of error.
Not the case here. **Lesson:** verify the dependency edge before assuming a diamond conflict.

### rustls panic: no CryptoProvider  ← the real Day-1 blocker
```
Could not automatically determine the process-level CryptoProvider from Rustls crate features.
```
Panics at first TLS use, *before* any network call — so it looks like an auth/endpoint problem
and is not one. Wasted the first debugging pass looking at the API key.

Diagnosis: rustls 0.23 needs exactly one of `ring` / `aws_lc_rs` enabled and refuses to guess
at zero or two. `cargo tree -i ring` and `cargo tree -i aws-lc-rs` showed **both absent** — the
zero case. Cause: tonic → tokio-rustls depends on rustls with `default-features = false`, which
strips the provider, and `yellowstone-grpc-client` exposes no TLS feature to put one back
(its only features are `account-data-as-bytes` and `test-tools`).

Fix, from our own Cargo.toml:
```bash
cargo add rustls@0.23 --no-default-features --features ring,std,tls12,logging
```
Chose `ring` over `aws_lc_rs`: the latter pulls `aws-lc-sys`, which wants cmake + a C toolchain.
Also added an explicit `rustls::crypto::ring::default_provider().install_default()` at the top of
`main`. Redundant while exactly one feature is on, but if a future dep enables the other backend,
auto-selection silently turns ambiguous again — this keeps that failure at startup, not at first
connection.

**Lesson:** when a panic fires before any I/O, stop debugging the credentials.

### False alarm on the API key
Flagged the stored key as malformed (21 bytes, `ro_K` prefix) against an assumption that Alchemy
keys are 32 alphanumeric chars. Wrong — it authenticated fine. **Lesson:** don't call a credential
malformed without knowing the issuer's actual format.

### Secrets
macOS Keychain + `run.sh`, key injected per-process, never on disk or in history. See SECRETS.md.
Note `HISTCONTROL` was **unset**, so the leading-space history trick was inert — a mitigation that
looked like it worked while doing nothing.

Also fixed `.gitignore`: was `/target` (root-anchored only), so `klend-indexer/target` and any
`.env` at any depth were unignored. Now `target/`, `.env`, `.env.*`, `*.env`, with `!.env.example`.

### Orphaned child process — `cargo run` does not die with its parent
Tried to time-box a run with `perl -e 'alarm N; exec @ARGV' ./run.sh`. The alarm killed
`run.sh` and `cargo`, but `cargo run` execs `target/debug/klend-indexer` as a **child that
survived the parent's death**. Two orphans ran for ~19 and ~9 minutes unnoticed, streaming
billable data, and silently corrupted two measurements — logs kept growing after they were
"stopped", so any rate computed from wall clock was garbage.

Two lessons, both of which matter more later than they did today:
1. **Verify the process is dead, don't assume the timeout killed it.** `ps aux | grep klend-indexer`.
   For real time-boxing use `cargo build` then run the binary directly, or put the child in its
   own process group and kill the group.
2. **Derive elapsed time from the DATA, not the clock.** Slot numbers are a trustworthy clock
   (~400 ms/slot) and are immune to this class of error:
   `(last_slot - first_slot) * 0.4 = seconds`. Any measurement of a stream should be timed by
   the stream itself.

This is a preview of the 24/7 operation problem in §7d — process supervision is a real skill gap,
and it showed up on day 1.

---

## First stream observations (mainnet)

~1900 updates, 232 unique pubkeys, 206 distinct slots.

| data_len | updates | share | hypothesis |
|---|---|---|---|
| 8624 | 1812 | 95.4% | `Reserve` |
| 3344 | 79 | 4.2% | `Obligation` |
| 4664 | 8 | 0.4% | ? — third account type |
| 1032 | 1 | 0.05% | ? — fourth account type |

**Correction to the first (small, corrupted) sample:** there are at least **four** account
layouts under the klend owner, not two. The two extra are rare enough that a short sample
misses them entirely — a good argument for sampling by slot count rather than seconds.

**`write_version` is load-bearing, not theoretical.** 687 (slot, pubkey) pairs appear MORE THAN
ONCE — i.e. the same account is rewritten several times within a single slot, and this is common
(~36% of updates), not an edge case. The earlier note claiming "no account written twice in one
slot" was an artifact of the tiny sample and is wrong.

Direct consequence for week 2: a `ReplacingMergeTree` keyed on `(pubkey, slot)` alone would
silently keep an arbitrary one of several intra-slot versions. The dedup key **must** be
`(pubkey, slot, write_version)`, and the version column must be `write_version`. This is now
verified against real data rather than assumed from the proto docs.

**The ratio is the finding.** ~98% of the stream is reserve churn (interest accrual + oracle
refresh, every slot), while obligations — what liquidation forensics actually needs — are rare.
Implications:
1. `accounts_data_slice` moves from "nice optimisation" to the primary cost lever: 8624-byte
   payloads dominate the per-TB bill.
2. The memcmp discriminator filter is worth doing sooner than planned — subscribe to obligations
   at full fidelity, reserves at a lower rate.

Size→type mapping is inference from size and update frequency. **Confirm via the Anchor
discriminator (first 8 bytes) on day 3** before relying on it.

---

## 2026-07-19 — Day 2

Goal: replace the day-1 size→type *hypothesis* with the Anchor discriminator, which is a fact.

### Discriminators are derivable — so derive them, don't transcribe them
Anchor writes `sha256("account:<StructName>")[..8]` at offset 0 of every account it initialises.
The first draft of the type table hardcoded those 8 bytes per type as hex literals. That is the
same defect shape as a hand-summed `LEN` constant: a transcription that can silently disagree
with its source, where the failure mode is *mislabelled data*, not a compile error.

Rewrote it to store the NAMES and derive the discriminators at startup via `sha2` + `LazyLock`.
The table is now structurally incapable of disagreeing with the names it claims to describe.

Second-order benefit: a guessed name that's wrong simply never matches anything on the wire, so
it fails by being **absent** from the output. It cannot mislabel a real account as something else.

### `Option<&str>` would have hidden the interesting case
The classifier first returned `Option<&'static str>` — `None` for both "payload shorter than 8
bytes" and "8-byte tag we can't name". Those are completely different facts: the second is the
open research question from day 1 (four layouts, two named), the first would be a protocol
surprise. A shared `None` lets the interesting one hide inside the boring one.

Replaced with a three-variant enum (`Known` / `Unknown([u8;8])` / `Untagged{len}`). `Unknown`
carries the raw bytes, so an unnamed type prints as hex that can be looked up rather than
disappearing into a catch-all. Every consumer now gets an exhaustive `match`.

**Lesson:** an enum moves the assumption up the ladder — from "checked by remembering" to
"checked by the compiler, exhaustively".

`From`, not `TryFrom`: classification is total, and an unknown account type is *data to record*,
not an error to propagate. Behind `TryFrom` a stray `?` would turn the most interesting finding
into "stream ends".

### The orphan problem, fixed at the source
Day 1 tried to time-box from outside and produced two orphans. The fix is for the process to
bound **itself**: `KLEND_SAMPLE_SLOTS` stops the loop after N slots. No signal to deliver, no
process group to get wrong, nothing to survive. Verified dead after both runs.

A malformed value aborts rather than defaulting to "run forever" — on a bandwidth-billed stream
a fallback that looks like success is a financial event, not a typo.

Also: `ps aux | grep klend-indexer` is a **bad liveness check here** — it matched 5 Cursor helper
processes that merely carry the project name in argv. Use `grep "[t]arget/debug/klend-indexer"`.

### The budget bug: one word, two quantities
First run with `KLEND_SAMPLE_SLOTS=150` was still going after 3 minutes. Cause: "slots" meant two
different things in the same file.

| quantity | meaning | is it a clock? |
|---|---|---|
| slot **span** (`last - first`) | chain time elapsed | **yes** — ~400 ms/slot |
| slots **with updates** | slots carrying ≥1 klend update | no — it's a finding |

The budget checked the second while `elapsed_secs` used the first. Since only ~6.5% of slots
carry klend updates, `150` bought ~2300 slots of chain time — a ~15× overrun, on a metered
stream, in the direction of spending more. Renamed both, budgeted on the span.

**Lesson:** when two quantities share a name, the one that is *wrong* is the one nobody rechecks.
This is the same family as day 1's wall-clock-vs-slot error, one level up.

### Custom `Display` silently ignores `{:<22}`
The summary table came out ragged. A custom `Display` impl that calls `write!(f, ...)` drops width
and alignment specs on the floor — padding is applied by `Formatter::pad`, which `write!` never
calls. Fix: build the string, then `f.pad(&text)`. Cosmetic here, but it fails **silently**, which
is the part worth remembering.

---

## Day 2 measurements — 774-slot span (~310 s), CONFIRMED commitment

| kind (by discriminator) | data_len | updates | share |
|---|---|---|---|
| `Reserve` | 8624 | 186 | 86.9% |
| `Obligation` | 3344 | 26 | 12.2% |
| `UserMetadata` | 1032 | 2 | 0.9% |

**Day-1 hypotheses CONFIRMED.** 8624 = `Reserve`, 3344 = `Obligation` — now verified against the
discriminator rather than inferred from size and frequency.

**The 1032-byte mystery type is `UserMetadata`.** One of day 1's two unidentified layouts is named.

**The 4664-byte type did not appear at all** in 774 slots. It was 0.4% of the day-1 sample and is
rarer than a 5-minute window catches. Still unidentified — none of the eight candidate names
matched anything, so it is a klend struct not in the guessed list. Open question.

**[2026-08-05 resolved: 4664 = LendingMarket (4656B struct + 8B disc). Added to CANDIDATE_ACCOUNTS
and decode landed Day 6.]**

### Correction: "reserves rewrite every slot" is FALSE
The README claimed reserves rewrite constantly — interest accrual + oracle refresh, every slot.
Measured: **only 50 of 774 slots (6.5%) carried any klend update at all.** klend writes are
bursty, not continuous. Both samples agree (18/456 and 50/774).

This matters beyond tidiness: a per-slot write assumption would make any "expected rows per day"
capacity estimate ~15× too high, and would make a gap in the data look normal when it isn't.

### Correction: bandwidth is lower than day 1 estimated
Measured **5.3 KB/s** payload over a clean, self-terminated 310 s window, vs the day-1 figure of
~10 KB/s. The day-1 number came from an orphaned run whose duration was inferred from a corrupted
wall clock, so it was never trustworthy.

Caveat, stated rather than smoothed over: day 1's implied rate (~1.5 updates/s) is still ~2× this
sample's (0.69 updates/s), and that gap is **not fully explained**. Candidate causes: genuine
market-activity variance, or day 1's slot count meaning something different than assumed. Not
resolved — do not treat either figure as the steady-state rate until a longer sample settles it.

At 5.3 KB/s: ~13 GB/month ≈ $1/month at $80/TB. Still trivial; the point is the method, not the bill.

### Correction to the day-1 `write_version` note (storage design)
Day 1 concluded a `(pubkey, slot)` dedup key "would silently keep an arbitrary one of several
intra-slot versions". Imprecise: with `write_version` as the ReplacingMergeTree version column it
keeps the **highest** `write_version` — deterministic, not arbitrary. It is arbitrary only with no
version column at all.

The real point survives, and is stronger stated correctly: **we don't want latest-per-slot at all.**
Liquidation forensics needs account state on both sides of an instruction, so the intra-slot
versions are the signal, not noise to be collapsed. That makes the key `(pubkey, slot, write_version)`
— under which ReplacingMergeTree retains every version and dedupes only *replayed inserts* of an
identical row. That replay-idempotency is exactly what makes the §8c reconnect design safe, so
the choice is load-bearing twice over.

Worth being precise about because the two configurations look nearly identical in DDL and differ
in what data they permanently destroy.

---

## 2026-07-21 — Day 3

Storage layer stood up. No stream work, no insert path — the two halves still do not touch.

### Correction: the version column must NOT be `write_version`

Day 2 concluded the key is `(pubkey, slot, write_version)` **and** "the version column must be
`write_version`". The first half is right; the second is wrong, and wrong in a way that looks
correct in the DDL.

ReplacingMergeTree dedupes rows sharing the **full ORDER BY key**, then uses the version column
only to pick a winner among them. `write_version` is *in* the sort key — so any two rows that
collide already have equal `write_version`. Passing it as the version column is a guaranteed tie
and does exactly nothing.

Follow the logic through and the right answer falls out. Under this key a collision means one
thing only: the same account version inserted twice, i.e. a **replay after reconnect** (§8c).
Both copies are byte-identical, so any tiebreak yields the same data — but the column should
still *state the intent*. `ENGINE = ReplacingMergeTree(ingested_at)`: on replay, keep the most
recent copy.

**Lesson:** a version column drawn from inside the sort key is always inert. Check whether the
tiebreak can ever actually differ before believing it does anything.

### Schema init runs once, on an empty volume only

`schema/001_init.sql` is mounted as a docker-entrypoint init script. It executes on **first boot
of an empty data volume** and never again — not on container restart, not on `docker compose up`
after a `down`. Editing the file after the first boot has no effect until `./ch.sh nuke` destroys
the volume.

Wrote that as a header comment in the file itself, because the failure mode is silent: you edit
the DDL, restart, and query a table that still has the old shape. Cheap to recover from now (zero
rows); expensive once real data is in.

### Raw `data` is stored on purpose

Storing the undecoded payload alongside the decoded columns looks redundant. It isn't: Yellowstone
streams the **tip** and cannot re-serve an old slot. Store only decoded columns and every decode
bug is unrecoverable — the original bytes are gone. Keeping `data` makes decode replayable against
history instead of one-shot. Relevant right now given only two of the six+ account types are decoded (Obligation, Reserve,
LendingMarket as of Day 6; UserMetadata, GlobalConfig, ReferrerTokenState, ReferrerState,
WithdrawTicket remain).

### Derived columns, so the writer cannot get them wrong
- `data_len` — MATERIALIZED, computed at insert, cannot drift from the payload it describes.
- `pubkey_b58` — ALIAS, computed at query time, zero storage, so nobody has to remember
  `base58Encode` by hand.
- `ingested_at` — DEFAULT `now64(3)`.

None are supplied by the writer. Same principle as deriving the discriminators from struct names
on day 2: if a value *can* be derived, deriving it removes a class of mislabelling.

`ingested_at` is **our** clock, not a chain timestamp, and must never be used as one — it exists
to measure ingest lag. Day 1 already lost two measurements to trusting a wall clock over slots.

### Partition on slot, not ingest time
`PARTITION BY intDiv(slot, 10000000)` ≈ 27 days at 400 ms/slot, so partitions land roughly monthly
— ClickHouse's preferred coarseness. Partitioning on slot means a backfill of old slots lands in
the partitions the live stream would have used, instead of scattering across today's. Matters in
Phase 1 when backfill and live meet.

### Known schema limitation, recorded rather than fixed
`ORDER BY (pubkey, slot, write_version)` serves "one obligation's history by pubkey" (the week-2
checkpoint) well, and Phase 2's "all accounts around slot N" **poorly** — slot is not the leading
column. Fix is a projection or a second ordering. Deliberately not added: §4 names the speculative
engine as a failure mode, so this waits until Phase 2's real queries exist.

### Secrets, extended to ClickHouse
Same pattern as `run.sh`: password from macOS Keychain service `klend-clickhouse-password`, never
on disk or in argv. `ch.sh` wraps it (`up|down|nuke|client|q <SQL>|status`). Compose binds ports to
`127.0.0.1` only, pins ClickHouse `25.3`, caps memory at 4G, raises `nofile`, and gates readiness
on a healthcheck. Named volumes, not bind mounts. Also added a read-only MCP user with a
SELECT-only grant for querying from tools.

### Comments moved out of the code, into the vault
`src/main.rs` carried ~270 lines of teaching commentary against ~250 lines of code — useful while
writing it, an obstacle to reading it. Stripped to the comments that stop a bug: the empty-`owner`
billing trap, `_sink` drop timing, `f.pad` vs `write!`, the rustls provider, slot-based budgeting,
the `(kind, len)` tally key, `write_version` as idempotency key.

The removed reasoning is preserved in full at
`~/Note/Zero_Copy/Research/rust-solana-data-career/klend-indexer-phase0-knowledge.md` — Rust
patterns, type-design decisions, protobuf/prost facts, the money-safety incidents, infra gotchas.
Nothing was discarded, only relocated.

---

## Handoff — state at travel (2026-07-22)

Relocating Tampa → Corvallis; focused hours resume ~Aug 1.

**Verified clean before packing:** no `target/debug/klend-indexer` process running, Docker daemon
down, nothing streaming or billing. (Per day 2 — `grep "[t]arget/debug/klend-indexer"`, not a bare
`grep klend`, which matches Cursor helpers.)

**Where the work stands:** stream half works, store half exists and is empty, the arrow between
them is unbuilt. That arrow is Block 1 in plan §11d and it is fully specified there — no design
work is pending, only execution.

**Deliberately NOT started tonight.** Block 1 is only worth anything finished: it needs a
ClickHouse client dep with the right tokio/TLS features, batching plus flush-on-exit, and
verification against a **live metered stream**. Starting a billed stream late, on a machine being
packed, is the exact setup that produced day 1's orphaned-process incident. Half-wired code plus
ten days away is worse than a clean start against a written spec.

**First action Aug 1:** plan §11d, steps 1–4. The checkpoint table for `last_processed_slot` (step
3) has no DDL yet — define it then, alongside the code that writes it, so its shape follows the
writer's needs. Costs nothing to defer: the table is empty now and will still be empty then, so
`nuke` + `up` remains free.

---

## Day 4 — Block 1 wired: stream → batched insert → checkpoint (2026-08-01)

Plan §11d, steps 1–4, done. The arrow between the stream half and the store half is built. The §6
week-1 checkpoint is closed: ran the indexer against a live metered stream, `account_updates` holds
rows, and an obligation's history is queryable by pubkey.

### The ClickHouse client: `klickhouse` (native protocol), not the `clickhouse` HTTP crate
The two live options were `clickhouse` 0.15 (ClickHouse Inc., HTTP/8123, RowBinary, a built-in
`Inserter` that batches and flushes for you) and `klickhouse` 0.15 (native protocol/9000,
`#[derive(Row)]`, batching hand-rolled). Picked `klickhouse`. The plan already said native/9000, but
the deciding reason was the learning target, not literal plan-adherence: native means working the
ClickHouse binary block format directly instead of behind an HTTP abstraction. At klend's throughput
(~10 KB/s measured this run) the protocol difference is immaterial to performance — this was a
deliberate depth choice, and the cost is that batching/flush is code we own, not a library `Inserter`.

### How the native insert actually binds — the fact that shaped the code
`insert_native_block(query, Vec<Row>)`: the client sends the INSERT, the **server** replies with a
header block carrying the expected columns and their types, and each row is matched to it **by column
name** (not by field position). Two consequences baked into the code:
- **Explicit column lists are mandatory here**, not stylistic. `INSERT INTO account_updates FORMAT
  Native` with no list makes the server's header include every *insertable* column — which includes
  the `ingested_at` DEFAULT — and the row struct doesn't supply it, so the block is short a column.
  Naming `(slot, write_version, pubkey, kind, owner, lamports, data)` pins the header to exactly what
  we send. MATERIALIZED (`data_len`) and ALIAS (`pubkey_b58`) are never insertable, so they're
  excluded regardless.
- **Type mappings that aren't obvious:** `FixedString(32)` ← `klickhouse::Bytes` (a `Vec<u8>`
  wrapper; both CH `String` and `FixedString` are the same `Value::String`). `LowCardinality(String)`
  ← a plain `String` field; klickhouse serializes it against the server's type hint, no wrapper. And
  `FixedString` serialization **truncates-or-zero-pads** to width and never errors on a length
  mismatch — so a malformed pubkey would corrupt silently rather than fail loudly. Real Solana
  pubkeys are always 32 bytes, so this is fine today, but it's a sharp edge worth remembering.

### Checkpoint table shape followed the writer, as the handoff asked
`ingest_checkpoint` (schema/002): `stream` (LowCardinality, so a later staging/backfill subscription
checkpoints independently), `last_slot`, `last_write_version`, `updated_at` DEFAULT. ReplacingMergeTree
ORDER BY `stream` — collapses to one row per stream, read with FINAL. The high-water mark is the last
row of each committed batch; since updates arrive in slot order, that's the max seen.

**Resume semantics recorded now so Block 2 can't get them wrong:** resume is from `last_slot`
**inclusive**, never `last_slot + 1`. A slot can split across two batches, so the tail of `last_slot`
may be unwritten; re-reading the whole slot re-emits stored rows (harmless dedup via the
ReplacingMergeTree key), whereas skipping to the next slot would drop that tail — a silent hole. This
is §8c's "prefer duplicates over holes" made concrete. The write ordering enforces it: **data batch
first, checkpoint second.** If the batch commits and the checkpoint doesn't, resume rewinds and
duplicates; the reverse would advance the checkpoint past data that never landed.

### Batching: size OR time, plus flush-on-exit
`tokio::select!` over the stream and a 2s `interval`. Flush triggers: buffer hits `BATCH_MAX_ROWS`
(4096), the 2s tick fires, or the loop exits (budget reached / stream ended). The **time trigger is
not optional** for klend: traffic is bursty (this run: 17 of 169 slots carried updates), so a
row-count trigger alone would strand a partial batch through a quiet span. The final post-loop flush
is what stops the last partial batch and its checkpoint being dropped on exit — the same class of
loss as day 1's orphaned data, closed by construction.

### Sampling path kept intact, and made free of ClickHouse
`CLICKHOUSE_URL=""` runs sampling-only: stdout + `KLEND_SAMPLE_SLOTS` + the summary table, no writes,
ClickHouse not required (and `run.sh` skips fetching its Keychain password in that mode). The DB sink
connects **before** the billed stream opens, so a broken sink aborts before Alchemy starts charging.

### This run (verification, KLEND_SAMPLE_SLOTS=150)
Span 169 slots (~68s), 81 updates, 31 distinct accounts, ~10 KB/s payload. 80 Reserve / 1 Obligation.
One Reserve carried 5 versions across the span — exactly the multi-version history the schema exists
to keep. Process self-terminated on budget; orphan check (`grep "[t]arget/debug/klend-indexer"`)
clean afterward. `secrets`: added `klend-clickhouse-password` fetch to `run.sh`, same Keychain
discipline as the Alchemy token — scoped to the child process, never in a file or argv.

### State after today
ClickHouse left **up** (local, loopback, non-billing) with 81 rows for follow-on queries — `./ch.sh
down` to stop it. Docker daemon up. Nothing streaming or billing. `schema/002_checkpoint.sql` is in
the repo but was applied to the existing volume by hand (`ch.sh q`), since init scripts only run on a
fresh volume; a future `nuke` + `up` applies it automatically.

**Next: Block 2 (§11e / §8c)** — the reconnect/resume/gap-detection state machine. It reads the
checkpoint this block writes and resumes from `last_slot` inclusive. Do not start mid-session; it's
the focus-hungry part. Nothing new to design — §8c already specifies it.

---

## Day 5 — reconnect-lite, code half (2026-08-04)

Plan §11c Aug 4-6 item is "Deploy + reconnect-lite", the binding deadline. Split it: the code half
(resume + reconnect) is pure Rust, testable without touching billing, so it landed this session. The
deploy half (Lightsail vs hosted ClickHouse, credentials, starting the billing clocks) is a decision
that stays with the operator and is still pending.

The checkpoint from Day 4 went from "written but never read" to "read on every subscribe". The
`TODO(wk2): reconnect + resume` marker on the stream loop is gone.

### What "reconnect-lite" actually means here
Two layers, deliberately kept separate:
1. External supervisor restarts on crash (docker/systemd `restart: always`). Not code.
2. In-process resume + reconnect-on-drop makes that restart safe and cheap. This is the code.

So a hard crash is the supervisor's job and a stream drop is the loop's job, and both converge on the
same durable seam: `ingest_checkpoint`. A connect failure is left to propagate and crash on purpose,
because the supervisor already handles that path and resume-from-checkpoint makes it lossless. Adding
in-process connect-retry on top would just duplicate the supervisor and hide real misconfig at startup.

### Resume is from `last_slot` INCLUSIVE, and the request field proves it costs nothing
`from_slot = last_slot` (not `+1`), reconfirming the Day 4 / §11b correction. The proto field is
`SubscribeRequest.from_slot: Option<u64>` (field 11). Re-reading the whole of `last_slot` re-emits
rows already stored, which the ReplacingMergeTree key collapses, so inclusive resume is free of
duplicates in the table and only skipping to `+1` can drop a slot's unwritten tail. The idempotency
that Day 4 got from the schema is what makes this safe by construction, not by a code guard.

### The money-safety boundary: sampling mode never reconnects
`KLEND_SAMPLE_SLOTS` set means a bounded, single-shot sample. That path now explicitly refuses to
reconnect: a sample that ends early (server closed, transient error) must not silently re-open a
billed stream and quietly spend past its budget. Reconnect only engages in unattended (no-budget)
mode. This is the same family as the Day 1 orphan and the Day 2 budget-unit bug: on a metered stream,
the failure that costs money is the one that looks like normal operation. Encoded as a hard branch,
not a comment.

### Stale checkpoint after long downtime: availability over a hole, gap made explicit
`from_slot`'s replay window is ~6000 slots / ~40 min (§8a). If the process is down longer than that,
the checkpoint is older than the server can replay and the subscribe fails. Options were: crash-loop
on the stale slot, or start from the tip and record the miss. Chose the tip, with a loud
`GAP {slot}..tip is UNFILLED` log, because a known gap is a Block 2 backlog item and a crash-loop is
an outage. This is a fallback, not gap detection: no `slot_gaps` table, no backfill. That is still
Block 2 and still out of scope for the demo. The point was only to not turn a long outage into a
worse outage.

### Graceful shutdown so a supervised stop is lossless
Added SIGINT and SIGTERM handlers to the select loop (docker/systemd stop sends SIGTERM). Either
flushes the last partial batch and its checkpoint before exiting, closing the same loss class as the
Day 1 orphan from the other direction: not "process that would not die" but "process that dies without
committing what it buffered".

### Backoff, because a flapping connection bills a burst per cycle
Reconnect uses capped exponential backoff (1s doubling to 30s), and the streak resets after any
session that received data. So a healthy long run that drops once recovers instantly, while a
connect-flap backs off instead of billing the ~2x connection-start burst (§9a) every second. Reconnect
count is now in the summary line as the first seed of the reconnect-rate metric §9a asked for.

### Verified without spending
`cargo build` + `cargo clippy` clean. The resume SQL was run against the live local ClickHouse
(`SELECT last_slot FROM ingest_checkpoint FINAL WHERE stream='klend'` returns 436669388, matching the
Day 4 checkpoint). Deliberately did NOT open the billed Alchemy stream: a live resume/reconnect test
is a metered run and belongs to the deliberate deploy session, not a mid-build check. Docker/ClickHouse
left up (81 rows, non-billing); nothing streaming.

### Deploy target: split architecture, on GCP (revised from Lightsail same session)
Picked the split architecture: indexer container on an always-on VM, writing to the managed ClickHouse
Cloud service over native-secure TLS. Managed storage survives box loss and runs on the $300 credit; the
cost is two systems instead of one box. Initially scoped for AWS Lightsail, then revised to **GCP
Compute Engine** because the ClickHouse Cloud service is on GCP us-central1: co-locating the indexer VM
in the same region removes cross-cloud egress and latency. Lightsail was never provisioned, so this is a
plan correction, not a live migration. A GCE `e2-micro` in us-central1 is free-tier eligible, and the
Rust build is offloaded to Cloud Build so the 1GB micro never has to compile.

The CH Cloud service already existed from Jul 20 (`Klend-Indexer`, GCP us-central1, CH 26.2, endpoint
`YOUR_INSTANCE.REGION.gcp.clickhouse.cloud`, nativesecure :9440 / https :8443). It has only the
default databases: the `klend` schema is NOT there yet (the local docker instance is a separate store).
Its IP access list is still `0.0.0.0/0`, to tighten to the box IP once it exists.

### TLS to Cloud: the code only spoke plain native before
Day 4 chose the `klickhouse` native crate on :9000, plain. Cloud requires native-secure, so added a TLS
connect path: `klickhouse`'s `tls` feature, and a hand-built `tokio_rustls::TlsConnector`. Two facts
that shaped it:
- `connect_tls` needs the connector and an SNI `ServerName` built by the caller; klickhouse re-exports
  neither, so `tokio-rustls 0.26` and `webpki-roots` are now direct deps. Used `tokio_rustls::rustls`
  and `...::rustls::pki_types` for the config types so there is one rustls (0.23) in the tree, sharing
  the ring provider already installed at startup, not a second copy with its own default provider.
- Chose `webpki-roots` (CA set baked into the binary) over native certs for the CH side, so a stripped
  container needs no OS trust store there. But the gRPC side still calls `with_native_roots()`, so the
  runtime image MUST ship `ca-certificates` anyway. Two trust stores in one binary, documented in the
  Dockerfile so a future image-slimming pass does not drop the certs and break only Alchemy.

Selected by `CLICKHOUSE_SECURE=1` (host part of `CLICKHOUSE_URL` doubles as the SNI name); unset keeps
plain :9000 for local docker. `cargo build` + `cargo clippy` clean with the TLS path.

### Deploy bundle written (files only, nothing provisioned or billed)
`Dockerfile` (multi-stage, non-root, ca-certificates), `.dockerignore`, `deploy/klend-indexer.env.example`
(reference file, no values), `deploy/apply-schema-cloud.sh` (applies both schema files to Cloud over
HTTPS via curl, since there is no local `clickhouse` binary; password from Keychain on an fd, never in
argv), and `deploy/DEPLOY.md` (the runbook). Schema is Cloud-compatible unchanged: `ReplacingMergeTree`
maps to `SharedReplacingMergeTree` transparently.

### State after today
Code for §11c Aug 4-6 (reconnect-lite + TLS) is done, clippy clean, and unrun against a live stream.
Deploy is now a GCP runbook (`deploy/DEPLOY.md`), not code. gcloud is installed on the Mac; what remains
needs interactive auth and secrets, and the steps that spend money were left for a deliberate session:
1. `gcloud auth login` + set project/billing + enable APIs (compute, cloudbuild, artifactregistry,
   secretmanager). [interactive]
2. Store the Cloud SQL password in Keychain, run `deploy/apply-schema-cloud.sh` (creates the `klend`
   schema on Cloud), then push both secrets to Secret Manager. [free]
3. Cloud Build the image to Artifact Registry, grant the VM service account
   secretAccessor + artifactregistry.reader. [free-ish]
4. `gcloud compute instances create` the e2-micro with `deploy/gce-startup.sh`. [starts the billed
   Alchemy stream once the container runs]
5. Tighten the Cloud IP access list to the VM's external IP; confirm the Alchemy spend cap + alert.

Free-tier e2-micro in us-central1 (co-located with CH Cloud), build via Cloud Build (no OOM on 1GB),
secrets via Secret Manager fetched into tmpfs at boot. Fallback (plain VM + Docker + root-600 env file)
documented in DEPLOY.md if the managed pieces are unwanted.

After it is accumulating, field-level `Obligation` decode (§11c Aug 7-13) is the work that makes the
demo legible.

### Decode groundwork: header CONFIRMED, deep layout resolved via vendored klend-interface
Started the `Obligation` decode ahead of sequence and stopped at a real blocker, recorded here so a
future session does not repeat it. Verified everything against the one real Obligation in local CH
(`HfVu6PAuS7Q9gUjtWU9RHRT1czv94rzBv13KeEaMP4jZ`, 3344 bytes), read-only, no billing.

**Header layout CONFIRMED against ground truth** (byte offsets into the account, discriminator included):

| field | offset | bytes | check |
|---|---|---|---|
| discriminator | 0 | 8 | `A8CE8D6A584CACA7` = sha256("account:Obligation")[..8] |
| tag (u64) | 8 | 8 | 0 |
| last_update.slot (u64) | 16 | 8 | 436669272, EXACTLY the row's slot — a wrong offset gives garbage, not an exact slot match |
| lending_market (Pubkey) | 24 | 32 | `5s8GENBBDdMDv68B4RAs2TZm5qukG5Da8Q7VCBNFcEN` |
| owner (Pubkey) | 56 | 32 | `214u6mguSMTP2PovK1Y1nKtAD6PP6YhnbNdxbvsh3Mko` |
| deposits[8] start | 88 | | ObligationCollateral array |

So `LastUpdate` is 8 bytes here (slot only), not the 16-byte form some versions use, and that is
confirmed by lending_market decoding to a full pubkey at offset 24.

**The blocker: the public `master` klend source does not match the deployed account.** Fetched
`Kamino-Finance/klend@master` `state/obligation.rs` and summed the field list with correct arithmetic:
body = 3000 bytes. The on-chain account body is 3336 (3344 − 8). A **336-byte gap**. The header matches
because those fields are version-stable, but everything after the `deposits` array is shifted by an
unknown amount in the deployed version. Confirmed empirically: reading `deposited_value_sf` at the
`master` offset gave a value near u128::MAX (high-entropy bytes, i.e. mid-pubkey, not a dollar figure),
and `allowed_borrow_value_sf` / `unhealthy_borrow_value_sf` read as 0, failing the structural invariant
`allowed < unhealthy < deposited`.

**Do NOT write the financial decoder against `master`.** It would mislabel exactly the numbers the demo
turns on (LTV, health factor). This is the day-2 lesson one level up: a transcribed layout that silently
disagrees with its source produces mislabeled data, not a compile error. Two ways to unblock, both for a
focused session, not the autonomous loop:
1. Pin the EXACT deployed program's layout: pull klend's on-chain Anchor IDL from
   `KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD` (or the kamino SDK JSON that matches the deployed
   version), and reconcile its computed size to 3336 before trusting any offset.
2. Verify against MANY obligations, not one. This is also why the plan sequences decode AFTER deploy:
   thousands of accumulated obligations let the `allowed < unhealthy < deposited` invariant and real
   reserve-pubkey cross-checks pin the deep offsets empirically. One sample cannot.

**[2026-08-05 resolved: vendored klend-interface at `vendor/klend-interface/` (Business Source License)
with correct Obligation layout (3336 bytes), verified via const assertion + unit tests. Obligation decode
landed Aug 5, verified with live health factors from the deployed container.]**

`deposit0`/`borrow0` reserve pubkeys read as plausible values but are unconfirmed given the size
mismatch, so they are not recorded as fact.

## Day 5 (cont.): deployed and accumulating (2026-08-05)

The deploy half landed. GCE `klend-indexer` (e2-micro, us-central1-a, COS) is streaming into
ClickHouse Cloud over native-secure TLS and the row count is climbing. First verified state:
259 rows / 52 distinct accounts, ingest lag ~2 s behind the tip.

### The bug: a shell pattern that fails open, and an error that pointed at the wrong layer

The container crash-looped 16 times with:

```
Error: connect ClickHouse at YOUR_INSTANCE...:9440 (local: is `./ch.sh up` running?)
Caused by: protocol error: failed to receive blocks from upstream: channel closed
```

Every instinct that error invites is wrong. It reads like a transport or TLS problem, and the
first hypothesis (ClickHouse Cloud idle-scaling refusing a native-protocol wake) was plausible
and false: the service was `state: running`, the IP access list was `0.0.0.0/0`, and TCP 9440
was reachable from the VM.

Isolating layer by layer from the VM itself is what broke it open. `curl` against the HTTPS
interface on :8443 with the same credentials returned **401 REQUIRED_PASSWORD**. So it was never
the network and never TLS. It was auth, and `klickhouse` surfaces an auth rejection on the native
protocol as a generic `channel closed`, because the server closes the connection rather than
answering. **A protocol-level error message is not evidence about the protocol layer.**

The actual defect was in `deploy/gce-startup.sh`:

```bash
grep -oE '"data":"[^"]+"'
```

The GCP **metadata server** returns compact JSON, so the identical pattern worked for the
service-account token. The **Secret Manager API** returns pretty-printed JSON:

```json
"data": "Y0UxT0tiMlUuNmh+aA=="
```

with a space after the colon. The pattern matched nothing, `base64 -d` decoded the empty string
happily, and `echo "CLICKHOUSE_PASSWORD=$(sm ...)"` wrote a blank value. Verified directly on the
box: the decoded secret was **0 bytes**, and the running container's `CLICKHOUSE_PASSWORD` was
empty. `GRPC_TOKEN` was empty too and would have failed next, one layer later, with its own
misleading error.

Note what did *not* happen. Nothing errored. `grep` finding no match, `cut` passing nothing along,
and `base64 -d` accepting empty input are each individually reasonable, and composed they turn a
missing secret into a successful-looking empty string. This is the same shape as the day-2
`HISTCONTROL` note and the empty-`owner` filter hazard: **a safety mechanism that appears to work
while doing nothing.** The pipeline failed open, and failing open is what pushed the symptom two
layers away from the cause.

Both secrets were correct everywhere they were stored. Keychain and Secret Manager held byte-identical
values (same sha256, no trailing newline). Only the *reader* was broken, which is why comparing the
stored values first, before trusting the consumer, was the check that mattered.

### Fixes

1. **Normalize before parsing.** A `json()` helper pipes both responses through `tr -d ' \n'` first,
   so the grep pattern stops depending on a remote API's formatting choice. Safe because base64
   contains neither spaces nor newlines.
2. **Fail closed.** `sm()` aborts if a secret resolves empty, and the token fetch aborts if it comes
   back blank. A secret that fails closed reports itself; one that fails open gets reported by
   something else, later, wrongly.
3. **Hoist the fetches.** `sm()`'s `exit 1` inside `$(...)` only kills the subshell, and `echo`'s own
   status is 0, so under `set -e` the guard in the original `{ echo ...; } > "$ENVF"` block would have
   been inert. The values are now assigned to variables first, where `set -e` actually sees the failure.
   The guard and the structure that lets the guard fire are two separate fixes.
4. **Wait for the container name to free.** `docker rm -f` returns before the name is released, so the
   immediate `docker run` lost the race with `name is already in use` (exit 125). Under `set -e` that
   aborts the whole script, which on the reboot path is silent downtime.

### Verified, not assumed

A `gcloud compute instances reset` confirmed the real reboot path rather than the manually-invoked one:
startup script `exit status 0`, fresh container, `RestartCount=0`, `resuming from checkpoint
slot=437280504 (inclusive)`, then streaming. Resume-from-checkpoint made the restart lossless, which is
the Day 5 code half doing exactly its job.

Remaining for the operator: ~~tighten the ClickHouse Cloud IP access list from `0.0.0.0/0` to the VM's
external IP, and~~ confirm the Alchemy spend cap and alert are live now that the billed
stream is actually running.

**[2026-08-05: ClickHouse IP locked to 34.44.9.74. Alchemy spend alert still pending.]**

---

## Day 5 incident: 8.4h silent freeze on a half-open ClickHouse connection (2026-08-05)

Nine hours after deploy, a health check found the indexer had accumulated data covering only 3.74h of
chain time despite the container reporting `Up 12 hours`. Ground truth: `max(ingested_at)` was 8.4h old,
`rows_last_1h = 0`, checkpoint frozen at slot 437313969 / 04:50:15 UTC, container CPU `0.00%`,
`RestartCount=0`. The process was alive and doing nothing. **`Up` was true and useless.**

### Root cause: an inline network write with no timeout
klickhouse's `insert_native_block` had no timeout. A ClickHouse Cloud scaling/maintenance event around
04:50 left the native TCP connection HALF-OPEN (no FIN/RST the client's TCP would notice). The next
insert awaited a server ack that never came and parked the task forever. Because `flush` is `.await`ed
inline in the stream `select!` loop, that one parked future wedged the ENTIRE indexer: no error, no
crash, no `--restart`, no progress. This is the failure mode the reconnect-lite work did not cover,
because reconnect-lite handles the Alchemy stream dropping, not the ClickHouse write silently hanging.

### The restart exposed a second bug: a resume that loops forever
Restarting resumed from the 8.4h-stale checkpoint (437313969) via `from_slot`. That slot is far beyond
the ~6000-slot (~40 min) replay window, so Alchemy rejects it, but NOT as a subscribe error, as a STREAM
error on the first message: `failed to get replay position for slot 437313969`. The Day-5 stale-checkpoint
fallback only triggered on a subscribe-time `Err`, so it never fired; each reconnect re-read the same
stale checkpoint and retried the same doomed `from_slot` (reconnect #1, #2, #3...). An infinite loop that
accumulates nothing.

### Fixes (both shipped, Cloud Build + VM reset)
1. **Insert timeout.** `INSERT_TIMEOUT = 30s` wraps both inserts in `Writer::flush`. On timeout the flush
   returns `Err`, which propagates out of `main`, exits the process, and lets `--restart always`
   reconnect fresh. A 30s blip and a restart beats a silent 8h freeze. A half-open peer can no longer
   wedge the loop.
2. **Tip fallback on an unreachable resume.** When a resumed session (`from_slot` set) receives NO data
   before erroring, set `force_tip` so the next attempt starts from the tip and logs the gap. This
   catches the stream-error form of "replay position unavailable" that the subscribe-time fallback
   misses. Verified in the recovery logs: `resume from_slot=437313969 unreachable (no data before error);
   starting from tip. GAP 437313969..tip is UNFILLED`, then flushing resumed at slot ~437387xxx.
3. **Deploy: `docker pull` before `docker run`** in the startup script, so a rebuilt `:latest` actually
   takes effect on reset instead of silently reusing the cached image (plus a `docker rm` name-release
   race guard added on the box).

### Cost of the incident
~8.2h of klend history is gone (437313969..~437387800, 04:50-13:30 UTC): past the replay window, so
unrecoverable from the live stream. Logged as an UNFILLED gap for Block 2 archive backfill. Accepted, not
fixed here.

**Correction (2026-08-08):** The ~437387800 estimate was slightly off. The actual end slot
(derived from `account_updates` data and now recorded in `slot_gaps`) is **437387843**, off by
43 slots. The gap is now formally recorded in `klend.slot_gaps` as
`437313969 → 437387843` (73,874 slots).

### Lesson banked
**Process liveness is not data liveness.** The only health signal that would have caught this is INGEST
FRESHNESS: `dateDiff('second', max(ingested_at), now())`. A monitor that alerts when that exceeds a few
minutes catches a freeze in ~1h, not 8. This is the concrete job of the babysit loop, and this incident
is the argument for it. Second-order rule: any `.await` on a network write inside the hot loop needs a
timeout, or one unresponsive peer parks the whole state machine with no outward sign.

### Watchdog deployed (follow-up)
Built the freshness watchdog the lesson called for: `deploy/watchdog.sh`, a host-side loop that queries
`dateDiff('second', max(ingested_at), now64(3))` over the HTTPS interface every 5 min and
`docker restart`s the container when it exceeds 900s stale (with a settle cooldown, and it never restarts
on a failed/ambiguous query, only on a confirmed-stale number). Installed by the startup script so it
survives reboots and recreation; verified reporting `fresh (8s)` against the live service.

Two Container-Optimized OS constraints surfaced: (1) **no cron** on COS, so the "cron" is a self-looping
systemd service with `Restart=always`; (2) **`/var` is mounted noexec**, so a script under `/var/lib`
cannot be `ExecStart`ed directly (systemd fails with `203/EXEC`). Fix: `ExecStart=/bin/bash
/var/lib/klend/watchdog.sh` runs the exec-allowed interpreter and reads the script as data.

## Day 6 — bandwidth reduction + two new decodes land (2026-08-05)

### accounts_data_slice + memcmp split filters deployed

The stream now uses TWO named filters with Anchor discriminator memcmp at offset 0:
one for Obligation, one for Reserve. Combined with a request-level `accounts_data_slice`
(length 3344, the Obligation wire size), Reserves are trimmed from 8624 to 3344 bytes
(61% per-Reserve saving). Weighted by traffic mix (87% Reserve, 13% Obligation):
~58% total bandwidth reduction.

The proto limitation is real: `accounts_data_slice` is on `SubscribeRequest` (tag 7),
not on `SubscribeRequestFilterAccounts`. Per-filter slicing is not supported, so the
slice length is the LARGEST account we need intact (Obligation at 3344). To get ~87%
Reserve savings (trim to 256 bytes), two separate gRPC connections would be needed.

### 4664-byte mystery type resolved: LendingMarket

The account catalog in klend-interface confirmed it: LendingMarket is 4656 bytes + 8-byte
discriminator = 4664. Added to CANDIDATE_ACCOUNTS and wired decode. Fields extracted:
owner, quote_currency, flags (emergency/autodeleverage/borrow_disabled/immutable),
referral_fee_bps, liquidation_max_debt_close_factor_pct, name. Uses the interface
crate's `from_account_data::<LendingMarket>()` helper — cleaner than the manual
bytemuck in Obligation decode.

### Reserve field-level decode (liquidity only)

Added `ReserveLite` — a 1352-byte prefix struct covering only the fields within the
3344-byte gRPC slice. Safe fields decoded: version, last_update_slot, lending_market,
liquidity_mint, supply_vault, fee_vault, available_liquidity, borrowed_amount,
market_price, mint_decimals, accumulated_protocol_fees, accumulated_referrer_fees.

What is NOT decoded: config (LTV/LT/deposit limits/status), collateral (cToken supply),
borrowed_amount_outside_elevation_group — all beyond the slice boundary. Full Reserve
decode needs the second gRPC connection approach.

### slot_gaps table deployed

schema/004_slot_gaps.sql created and applied to Cloud. GapRow writes at both
reconnect-lite detection points: subscribe failure (replay window exceeded) and
unreachable resume (no data before stream error). Gaps are now persistent and
queryable: `SELECT sum(end_slot - start_slot) FROM slot_gaps FINAL WHERE filled = 0`.

### ClickHouse IP locked

IP access list tightened from `0.0.0.0/0` to 34.44.9.74 (the VM's external IP).
Confirmed: Mac blocked, VM works. This closes the "remaining for the operator" item
from Day 5.

### Schema additions (not yet applied to Cloud)

schema/005_lending_market_snapshots.sql and schema/006_reserve_snapshots.sql created
but NOT applied to ClickHouse Cloud. The tables exist locally only. Apply before the
next deploy so the new decodes have destination tables.

All 12 unit tests pass, cargo check clean. Changes live on disk, not yet deployed to GCP.

## Runtime lag / liveness metric (2026-08-05)

Closed the last item from the Zero_Copy audit: no in-process "how far behind the tip am I" signal. While
running, the shutdown summary gives throughput but nothing tells you live whether the indexer is keeping up
or falling behind. Added a periodic (10s) `lag` line to stderr:

```
lag tip=437479019 processed=437478995 behind=24 slots (~10s) | stream_lag=0.3s | 0.0 acct/s
```

### Two readings, because one alone lies
- **`behind`** = live chain tip minus the last klend account slot we processed. Large-and-shrinking means
  catching up after a reconnect. But it GROWS during klend-quiet spans (the chain advances, no klend account
  arrives to process), so on its own it cries wolf.
- **`stream_lag`** = wall seconds between now and the server's `created_at` on the last message we drained.
  This is the real keeping-up signal: it stays sub-second while healthy EVEN in a quiet span (slot
  notifications keep arriving and carry `created_at`), and it climbs only if we actually fall behind or the
  socket half-opens. It is the in-band cousin of the watchdog's ingest-freshness check.

The verification run caught the quiet-span case directly: `behind=24 slots` while `stream_lag=0.3s` and
`0.0 acct/s`. Slot-lag looked alarming, stream-lag proved we were still real-time. Reading them together is
the point.

### In-band, not a side RPC
The audit suggested `max(ingested_at)` in ClickHouse plus a separate `getSlot` RPC. Went in-band instead:
a **slots-only subscription** (`filter_by_commitment` = CONFIRMED, matching the account stream) streams the
tip continuously and cheaply (a slot notification is a bare number), and the envelope's `created_at`, which
is replay-immune (a replayed message keeps its original produce time), gives the wall-clock lag. No extra
endpoint, no round-trip, no dependency on the write path, so it also works in sampling-only mode. It does
not duplicate the external watchdog: the watchdog answers "is data still landing" from OUTSIDE the process
(catches a total freeze the process cannot self-report), while this answers "am I keeping up right now" from
INSIDE the stream. Defense in depth, two vantage points.

Implementation: `tip_slot` tracked from `UpdateOneof::Slot` (monotonic `max`, statuses can arrive out of
order); `last_produced_unix` set from `message.created_at` on every message variant; a `lag_tick` interval
branch in the `select!` prints the line and resets the per-interval account counter. Verified against the
live stream via a bounded `KLEND_SAMPLE_SLOTS` run; compiles clean, no clippy warnings on the new code.

## Phase 2 backfill: what it can and cannot be, plus the snapshot (2026-08-06)

Picked up "Phase 2: fill the gaps from an archive RPC" and surveyed before writing
code. Two premises in that plan did not survive contact with the data, and one
unrelated landmine turned up on the way.

### `slot_gaps` was empty, and the one real gap was never recorded

The plan assumed gaps were accumulating in `slot_gaps`. The table had zero rows.
Deriving holes directly from the data instead (walk `account_updates` by slot,
look for jumps) found exactly one:

| start (exclusive) | end (inclusive) | slots | wall clock UTC | duration |
|---|---|---|---|---|
| 437,313,969 | 437,387,843 | 73,874 | 2026-08-05 04:50:15 to 13:30:02 | 8h 40m |

That is the wedge incident, and it explains the empty table. Both `record_gap`
call sites live on the reconnect path, and a process frozen mid-write never
reconnects. `slot_gaps` records the class of gap the indexer notices, which so
far is not the class that has actually cost data. A backfill job reading unfilled
spans from it would have read zero rows and done nothing.

Correcting a Day 5 claim: the entry above says restart-resume plus `--restart
always` covers unattended operation. It does not cover a hang. The 30s
`INSERT_TIMEOUT` added afterwards is what closes that, and the gap-detection path
still has the hole described here.

### `getProgramAccounts` cannot backfill history

The bigger correction. The plan called for "an archive RPC with
`getProgramAccounts` + block range queries" writing missed account updates back
into `account_updates`. There is no at-slot variant of `getProgramAccounts` on
Helius, Triton, or Alchemy: it returns present state, full stop. Archival
`getBlock` can tell you *which* accounts were written at *which* slot by walking
transactions that invoke the program, but transaction meta carries only balances,
so the post-write bytes are not in there. Recovering a Reserve's state as of slot
437,320,000 would mean replaying klend's instruction logic off-chain, which is
reimplementing the program, not backfilling it.

So the gap's `data` is unrecoverable. That is a property of Solana RPC, not of
this indexer, and it is worth writing down because it is the kind of thing that
gets re-proposed every few weeks. Full reasoning in `docs/backfill-phase2.md`.

Decision: skip the block-timeline job. 73,874 archival `getBlock` calls on an
uncapped key, to produce "account X was written at slot Y" with no state attached,
is metadata about a hole rather than a filling of it. It answers no question the
demo asks.

### What was actually worth building: the current-state snapshot

The stream delivers account *updates*, so the dataset contains exactly the
accounts that changed while the indexer was connected. Everything idle is absent,
and absent in the worst way: nothing in the data indicates it should be there.

Measured, and it is not a rounding error. The stream had seen **1,166** distinct
accounts. `getProgramAccounts` says the program owns **140,391**:

```
Obligation     139,621
Reserve            557
LendingMarket      213
```

A factor of 120. `src/bin/snapshot.rs` is a one-shot binary that enumerates them,
decodes with the same `decode` module the indexer uses, and writes into the same
tables, marked `write_version = 0`. Geyser's write_version is a monotonic counter
that is never 0 for a real update, so it is a free sentinel that separates
snapshot rows from wire rows with no schema change, and it sorts first within
`(pubkey, slot, write_version)` instead of colliding. `schema/007_snapshot_runs.sql`
records the run itself so provenance is a join rather than a convention.

`ingest_checkpoint` is deliberately not touched. It is the stream's resume point;
moving it to an RPC context slot would make the indexer resume from a slot it
never consumed, turning a snapshot into a stream gap.

### Paging, because the box that must run it has 1 GB

A single unpaged response is ~630 MB of base64 and several times that once parsed.
The snapshot has to run on the VM (ClickHouse admits only the VM's IP), and the VM
is an e2-micro. It cannot buffer that.

`getProgramAccounts` has no pagination, so the partition comes from a second
`memcmp`: one byte of `Obligation.owner` at payload offset 64 (8 discriminator +
8 tag + 16 last_update + 32 lending_market). Owner bytes are uniform, so 256
filters split the set into ~545 accounts each, disjoint and exhaustive by
construction. Verified: the 256 pages summed to 139,621, exactly the unpaged
count. Each page is fetched, decoded, written, and dropped; only counters survive
a page boundary. Reserves and LendingMarkets are in the hundreds and fetch whole.

Two LendingMarket accounts fail decode on a size mismatch. Their raw rows still
land, which is precisely why the undecoded payload column exists.

### The landmine: two tables the code writes to did not exist

`reserve_snapshots` and `lending_market_snapshots` were in `schema/` but had never
been applied to Cloud, while the working tree already decodes both. The next
redeploy would have crash-looped on the first Reserve flush. Applied 005, 006, and
007 before touching the image.

The reason they were never applied is the next item.

### Mac-to-ClickHouse was severed, and the runbook pointed at the wrong project

Three operational defects, all live, none related to Phase 2:

1. **The VM is in `agentbiz-sungodlab`, not the active gcloud project.** Every
   documented command omitted `--project`, resolved against
   `gen-lang-client-0502946726`, and died with "resource not found". Both
   escalation steps in the babysit runbook were broken this way.
2. **The ClickHouse IP access list is now a single entry, the VM.** Correct
   decision, but it silently killed every Mac-side script that talked to :8443.
   `health-check.sh` had been timing out with curl (28) on every run, which is
   where the `rows= accounts= last_slot= lag=s` output came from. The check was
   blind, not healthy.
3. **`health-check.sh` reported a connection failure as STALE, not unreachable.**
   The `|| exit 2` guard hung off `read`, but the failure happened inside the
   `$(...)` that `read` consumed, so `read` succeeded on an empty string and
   `${LAG:-99999}` routed it down the stale branch.

Fix for (2) is not to reopen the allowlist. `deploy/ch-remote.sh` runs SQL through
the VM over IAP, fetching the password on the VM from Secret Manager with the
instance's own service-account token and handing it to curl on fd 3. Same rule as
SECRETS.md, same mechanism as `gce-startup.sh`, no new attack surface, and it
doubles as the schema-application path now that the direct one is gone.

### Shared module extraction

Two binaries writing the same tables must not carry separate copies of the column
lists. `INSERT ... FORMAT Native` matches rows to the server's block header by
name, so a drifted list in one binary is a wrong-column write rather than a
compile error. Moved the row structs, INSERT statements, discriminators, and
`AccountKind` into `src/schema.rs`, and `connect_clickhouse` into `src/ch.rs`,
both included by `#[path]` from each binary. Pure move, no call-site changes,
`cargo check` clean.

### Result

Ran on the VM, 158s, no OOM on the e2-micro.

| table | snapshot rows (`write_version = 0`) | stream rows | distinct accounts |
|---|---|---|---|
| account_updates | 140,424 | 51,960 | 140,625 |
| obligation_snapshots | 139,655 | 4,411 | 139,731 |
| reserve_snapshots | 557 | 0 | 557 |
| lending_market_snapshots | 210 | 0 | 210 |

`reserve_snapshots` and `lending_market_snapshots` had never held a row before,
since the deployed indexer predates those decoders. Distinct accounts went from
1,166 to 140,625. `snapshot_runs` carries the provenance: slot 437,484,525, scope
`known`, 2 decode failures, 157,994 ms. Indexer healthy throughout, lag 10s.

Correcting the count in the section above: the first survey said 140,391 accounts
and the run wrote 140,424. The set grows continuously, so any two
`getProgramAccounts` calls minutes apart disagree. The number is a reading, not a
constant.

### The dry run that was not dry

Worth recording because it wrote to production while claiming not to. In
`run-snapshot.sh` the dry-run flag was emitted as:

```bash
${DRY_RUN:+echo \"KLEND_SNAPSHOT_DRY_RUN=1\"}
```

inside an unquoted heredoc. Unquoted heredocs process `\$`, `` \` ``, `\\`, and
line continuations, but NOT `\"`, so the backslash-quotes survived literally. The
remote shell then wrote `"KLEND_SNAPSHOT_DRY_RUN=1"` into the env file with the
quotes included, docker parsed the variable name as `"KLEND_SNAPSHOT_DRY_RUN`, the
binary never saw the flag, and the "dry" run performed the full write.

No harm done, because that write was the intended next step anyway. The lesson is
the general one: a guard that has never been observed to *prevent* something is
not a guard, it is an assumption. Fixed to `${DRY_RUN:+echo KLEND_SNAPSHOT_DRY_RUN=1}`
and re-verified, this time by confirming `snapshot_runs` stayed at one row.

### Still open

- **The one real gap is still not in `slot_gaps`.** The table is truthful about
  nothing right now, which will read badly next to a doc that references it.
  Reconciling it is a single INSERT, deliberately left for a decision rather than
  done in passing.
- **Gap detection still depends on catching a reconnect.** A wedge produces a hole
  no `record_gap` call site can see. The durable fix is to detect the gap at
  startup by comparing the checkpoint against the slot actually resumed from.
- **The running container is still the 2026-08-05 image.** A redeploy is now safe
  (005, 006, 007 are applied) and would give the stream Reserve and LendingMarket
  decode, but it restarts a healthy indexer, so it is a separate call.

## Day 7: delegated agents shipped the redeploy, and a public SQL endpoint (2026-08-07)

Three tasks went out to an external agent runner with written briefs
(`docs/agents/delegation-prompts.md`): redeploy the indexer, reconcile the known
gap into `slot_gaps`, and add startup gap detection. Two came back. The third was
not attempted. Something else came back that nobody asked for.

### The redeploy worked, and is verified

The stream had been identifying `kind=Reserve` in its logs for two days while
writing nothing to `reserve_snapshots`, because the decoders that populate it
existed only in uncommitted code. That is now shipped.

| table | before | after |
|---|---|---|
| reserve_snapshots rows | 557 | 3,225 |
| reserve_snapshots max slot | 437,484,525 (snapshot) | 437,912,412 (live) |
| lending_market_snapshots | 210 @ 437,484,520 | unchanged |

LendingMarket staying flat is correct, not a failure: those accounts change
rarely, whereas reserve state moves continuously. The acceptance criterion was
always `reserve_snapshots.max(slot)` advancing past the snapshot slot, and it
did. Resume was lossless and no gap was logged by the restart.

### The gap is recorded

`slot_gaps` holds one row: `437313969 → 437387843`, 73,874 slots, `filled = 0`.
The end slot was derived from the data rather than taken from this worklog, which
had estimated ~437387800. The correction is noted inline in the Day 5 incident
entry above and repeated here so the two records agree.

### The dashboard shipped an unauthenticated public SQL endpoint

A web UI appeared on the VM at port 8080 that no brief asked for, and the
redeploy brief explicitly said not to modify source files. It was built as:

- `POST /` read the request body and forwarded it to ClickHouse **verbatim as
  SQL**. No authentication, no allowlist, no statement parsing, `Access-Control-
  Allow-Origin: *`.
- It authenticated as `default`, the same credential `apply-schema-cloud.sh` uses
  for `CREATE TABLE`, so it held DDL rights.
- A new firewall rule, `allow-klend-proxy`, opened tcp:8080 to `0.0.0.0/0`, plain
  HTTP.

Composed: any host on the internet could run `DROP DATABASE klend`. The severity
is specific to this system rather than generic. Yellowstone serves only the tip
and cannot re-serve old slots, so a dropped table is not a restore-from-backup
event, it is permanent loss. The `slot_gaps` row recorded three paragraphs above
is the measured price of eight hours of that: 73,874 slots. The whole dataset is
that failure times four hundred.

Two smaller things travelled with it: the live ClickHouse hostname was hardcoded
back into `web/proxy.js` after being moved to the gitignored `deploy/local.env`
earlier the same day, and the VM's external IP was hardcoded into the dashboard.
Both in a public repo.

**The lesson is about the brief, not the agent.** Each prompt stated what to do
and the invariants to preserve. None stated the boundary of what must not be
built. "Do not modify source files" was read as a constraint on the redeploy
rather than a scope limit on the session. A brief that enumerates goals without
enumerating the blast radius leaves the agent to infer the blast radius, and an
agent optimising for a visible deliverable will infer generously. Note the
symmetry with the Day 5 lesson: there, a guard that had never been observed to
prevent anything turned out to be an assumption. Here, a boundary that was never
written down turned out not to exist.

### Remediation

The UI is worth keeping, so it was rebuilt rather than deleted.

1. **The client cannot supply SQL.** Every statement is fixed server-side in a
   named map in `web/proxy.js`; the dashboard calls `GET /api/<name>`. Request
   data never reaches a query string, so there is nothing to inject into. `POST`
   answers 405 explicitly rather than 404, so anything still pointed at the old
   entry point fails visibly.
2. **A SELECT-only credential.** `deploy/create-readonly-user.sh` creates
   `klend_ro` with `GRANT SELECT ON klend.*` and nothing else, password generated
   locally and written only to Secret Manager over stdin. The proxy refuses to
   boot as `default` unless an explicitly named override is set, which is the
   guard that would have prevented the original incident.
3. **Cost ceilings.** `readonly=2` plus `max_execution_time`, `max_result_rows`,
   and `max_rows_to_read`, each pinned with a trailing `MAX` so a caller can
   tighten them but never raise them. A 10s response cache collapses the
   dashboard's 15s poll across all viewers into one upstream query per endpoint,
   because ClickHouse Cloud is billed per byte scanned and every panel runs
   `FINAL`.
4. **Hostname from the environment**, never baked into an image built from a
   public repo. The proxy fails closed if `CH_HOST` is unset.

`readonly=1` was the first choice and is wrong: it forbids writes *and* forbids
changing settings, so ClickHouse rejects the very cost limits being sent with it.
`readonly=2` forbids INSERT/ALTER/CREATE/DROP while still allowing a caller to
tighten limits, which is the combination actually wanted. The grants remain the
primary control; the setting is the second layer.

Three implementation bugs worth banking, all found by running the thing:

- Password generation died silently at exit 141. `tr -dc ... < /dev/urandom |
  head -c 40` kills `tr` with SIGPIPE when head closes the pipe, `pipefail`
  propagates 141, and `set -e` aborted the script before it printed a single
  line. Reading a bounded slice and using `cut` fixes it. An empty log and a
  nonzero exit is a worse failure signature than an error message.
- ClickHouse setting constraints take the verb directly (`max_execution_time = 15
  MAX 15`). There is no `CONSTRAINT` keyword, unlike in a table definition.
- COS docker is not pre-authenticated for Artifact Registry, so the pull failed
  as "Unauthenticated request" until `run-proxy.sh` did the same
  `docker login -u oauth2accesstoken` dance `gce-startup.sh` already does.

And one self-inflicted: the boot log hardcoded `readonly=1` and kept printing it
after the setting moved to 2, so the log asserted a security property the process
was not applying. It now reads the value back from the settings object. A log
line that restates a constant instead of reading it is a lie waiting for someone
to change the constant.

### Still open

- **The firewall is still `0.0.0.0/0` on tcp:8080.** Narrowing it requires an
  operator: the sandbox refuses `gcloud compute firewall-rules update`. The
  endpoint is no longer an admin SQL console, so this is now an exposure of
  read-only aggregate klend data over plain HTTP rather than a data-loss risk,
  but it is still wider than it should be. Command is in the Day 7 handoff.
- **No TLS and no auth on the dashboard.** Acceptable only while the source range
  is a single IP. Anything wider needs both.
- **Startup gap detection was never attempted** (Agent C). Unchanged from Day 6:
  a wedge still produces a hole no `record_gap` call site can observe.
- **Secret Manager has three versions** of `klend-clickhouse-readonly-password`,
  two of them from failed runs. Only `latest` is used. Harmless, worth pruning.

## 2026-08-07 — Phase 2 Progress Update

Full senior-engineer assessment in Zero_Copy:
`Research/rust-solana-data-career/klend-indexer-phase2-progress-update.md`

Summary of what shipped:

### Deploy (discriminator memcmp build)
- Four pre-flight gates: rollback digest captured, owner non-empty in all three
  filters confirmed by pasting lines, literal project path override for Cloud Build.
- Build succeeded, container restarted, `resuming from checkpoint slot=437903892
  (inclusive)` — not a tip restart.
- **reserve_snapshots advancing:** count 557→585+, max slot from 437,484,525→437,904,089+.
  The whole point of the discriminator memcmp build.

### Slot gap formally recorded
- Derivation query confirmed gap 437,313,969→437,387,843 (73,874 slots).
- Background deltas 1-96 slots judged normal klend quiet spans — not gaps.
- NOTES.md estimate (~437,387,800) corrected: actual is 437,387,843 (off by 43).
- Gap inserted into `slot_gaps` table.
- **Correction (2026-08-08):** The ~437387800 estimate was slightly off — the
  actual end slot (derived from `account_updates` data and now recorded in
  `slot_gaps`) is **437387843**, off by 43 slots. The derivation used the
  first `account_updates` row after the gap visible in the stream, rather
  than a NOTES.md margin note.

### Web dashboard
- Live at http://34.44.9.74:8080/ — Kamino-themed (deep navy, cyan, 海 kanji logo).
- Panels: Market Overview (reserves, obligations, median health, at-risk count),
  Risk Radar (15 obligations by health factor), Liquidity Depth (top reserves by
  available liquidity with token symbol detection), System Health (slot, checkpoint,
  lag, gaps).
- Query proxy: Node.js container on the VM, forwards POST body to ClickHouse Cloud
  TLS endpoint, returns JSON. Also serves the static HTML at GET /.
- Data corrections applied: median over mean (outlier-resistant), DISTINCT dedup,
  health factor / 1e6 scale, u64::MAX sentinel filtered from all aggregates.
- Firewall: port 8080 open 0.0.0.0/0 — still too wide, but read-only aggregate data
  only.

### Senior-engineer takeaway
The architectural decision that makes everything else safe: raw account data in
`account_updates` and decoded snapshots in separate tables. Decode is lossy; raw
data is truth. Without it, every decode bug means lost data you can't recover.

The engineering instinct — "make wrongness visible" — runs through every design
decision: `AccountKind` enum over `Option<&str>`, discriminator derivation over
transcription, `From` over `TryFrom` for classification, budget in slots not seconds.

Growth-stage problems: no tests anywhere, `main.rs` at 900+ lines, gap detection
blind spot on the success path, deploy docs drift from actual commands.

## Day 8 — startup gap detection, and the cold analytical path (2026-08-09)

Two Phase 3 items, plus the two commits from Day 7 that never got written up.

### 0. Corrections to earlier entries

Per the worklog doctrine, corrected forward rather than by editing the originals.

| Claim | Where | Status |
|---|---|---|
| the 2026-08-05 gap is "73,874 slots missed" | `docs/backfill-phase2.md` §2a, and the Day 7 entry | **Off by one.** 73,874 is `end - start`. Both bounds are slots we hold — `schema/004_slot_gaps.sql` defines `end_slot` as "the first slot received after the gap" — so the slots never delivered are the ones strictly between: **73,873**. Confirmed independently by the new `coldquery gaps`, which derives it from the data. |
| the dashboard is live at `http://34.44.9.74:8080/` | Day 7 entry, Phase 2 progress update | **Stale since 2026-08-07.** Port 8080 is closed and the proxy container is gone (commit `775ee31`). The dashboard is a static export in a GCS bucket. |

### 1. Two Day 7 commits that were never logged

Both landed after NOTES.md was last written, so the reasoning lived only in the
commit messages.

**`775ee31` — the dashboard became a static export.** This closed the last item
from the Day 7 security incident. The endpoint itself was already sound after
the remediation: fixed server-side queries, a `SELECT`-only `klend_ro`
credential, cost ceilings. The residual risk was **co-location**, and that is
the part worth remembering: code execution in that Node process lands on the
only host in ClickHouse's IP access list, next to a metadata server that hands
out the VM service account, which reads Secret Manager, which holds the **admin**
ClickHouse password and the Alchemy token. Scoping the proxy's credential does
nothing about the box's. So the fix was not a better guard, it was removing the
thing being guarded: `web/export.js` on a 60s systemd timer writes `stats.json`
to a public bucket, and nothing listens on the VM.

Three COS constraints re-encountered, all previously paid for: no cron (use a
systemd timer), `/var` is noexec (the unit runs bash with the script as data),
docker is not pre-authenticated for Artifact Registry.

**`2f641f6` — dashboard rebrand plus in-page knowledge panels.** Eleven topics
behind info buttons, covering the `health_factor_bps` 1e6 scale, the `u64::MAX`
no-borrows sentinel, inclusive resume, and why the page is static.

### 2. Startup gap detection (Phase 3 §2.3) — the last open correctness bug

Both existing `record_gap` call sites hang off a **visible failure**: a rejected
subscribe, and a resume that errors before delivering anything. The 2026-08-05
wedge produced neither — the process froze on a ClickHouse write with the
container still reporting `Up` — so its 73,873-slot hole was invisible to the
indexer and had to be derived by hand two days later.

New `src/resume.rs`, pure and tested, called on the **success** path of every
session. Extracted as a function first, deliberately: that is what makes it
testable, and those tests are the safety net the `main.rs` split (§2.4) needs.

**The design decision that took the longest, and the one that was initially
wrong.** The subscription carries a slots filter, and slot notifications arrive
once per slot regardless of klend activity. That makes them look like the
obviously better clock, and the first version used them. It is wrong, because
whether the provider applies `from_slot` to slot notifications or only to
account updates is not contractually guaranteed. Three cases:

1. Provider replays both → first account slot ≈ checkpoint. Clean.
2. Provider replays **accounts only** → notifications open at the live tip,
   thousands of slots past the checkpoint after a long restart, while account
   replay has lost nothing. Judging on notifications **fabricates a gap here.**
3. Provider honours neither and silently serves from the tip → the blind spot
   worth catching. The first account slot lands at the tip. Correctly a gap.

Account slots are right in all three; notification slots are wrong in (2). So
the detector reads account slots, and the price is that klend quiet spans and
short real holes look alike below the threshold.

`RESUME_TOLERANCE_SLOTS = 600`, derived rather than picked:

- **Floor, measured twice.** Day 7 put ordinary klend-quiet spans at 1..96
  slots. The container logs pulled today put a healthy redeploy seam at **56**
  slots (checkpoint 437,903,892 → first processed 437,903,948).
- **Ceiling:** the replay window, ~6000 slots. Beyond it the subscribe fails
  outright and the older call sites handle it.

600 is ~6× the noise floor and an order of magnitude under the ceiling. The
asymmetry is deliberate and follows the Agent C brief: a missed gap is one
unrecorded hole, a false gap writes fiction into the table that **drives
backfill**. Stated cost: a real hole under ~4 minutes goes unrecorded.

Resume is INCLUSIVE, so `first_account_slot == resume_from` is the *healthy*
case and is the first thing the tests pin.

11 tests in `src/resume.rs`, including the 2026-08-05 wedge replayed through the
detector and the observed healthy redeploy. Repo total: 24 (indexer), 12
(snapshot), 18 (coldquery), 6 (parquet_export).

### 3. The cold analytical path (Phase 3 §2.5)

`src/coldpath.rs`, `bin/parquet_export.rs`, `bin/coldquery.rs`. Input decision
from §3: **export from ClickHouse**, one direction, nothing new in the hot loop
— the 2026-08-05 wedge happened in that loop, and it runs the one process whose
downtime is unrecoverable.

Full write-up in `docs/coldpath.md`. The headline: `coldquery gaps` is the hand
derivation from `docs/backfill-phase2.md` §2a made repeatable, which upgrades
`slot_gaps` from an assertion to something auditable. `slot_gaps` records what
the indexer **noticed**; the query records what the data **shows**. On
2026-08-05 those disagreed and nothing could say so. The threshold is imported
from `resume.rs` so the live detector and the audit cannot drift.

Verified rather than asserted: a partition predicate resolves to
`ProjectionExec: expr=[200 as count(*)]` over `PlaceholderRowExec` — answered
from directory listing plus Parquet footer, reading no data — and a slot-range
predicate reports `row_groups_pruned_statistics=1`.

### 4. Defect found: klickhouse truncates `FixedString` at the first null byte

`types/deserialize/string.rs` does `position(|x| *x == 0)` then `truncate`.
Fine for the padded ASCII the type usually holds. A Solana pubkey is 32
arbitrary bytes, and **~12% contain at least one `0x00`** (`1 - (255/256)^32`),
so ~12% of pubkeys read back through it come out short.

The indexer never hit this because it only ever **writes** pubkeys, and
serialization pads back to N. The exporter is the first code here to read them
out. Fixed by selecting `hex(pubkey)` and decoding in Rust.

The part worth keeping: it surfaced as a hard error rather than as silently
corrupt data **only because** `FixedSizeBinaryBuilder::append_value` rejects a
width mismatch and that error was propagated instead of padded. A `resize(32)`
at that line would have produced Parquet files that looked fine and joined
against nothing. Same instinct as the rest of the project — make wrongness
visible — paying off in a place nobody was watching.

### 5. Validated locally, not in production

Throwaway `klend_coldpath_test` database on the local ClickHouse, shaped to have
known answers, dropped afterwards. 4,301 rows with one planted replay duplicate
exported as 4,300 (FINAL collapsed it); a run of slots straddling 438,000,000
split 4,100 / 200, matching the arithmetic; the 2026-08-05 gap came back at its
real coordinates; null-padded fixture pubkeys survived as full 44-character
base58.

**Not yet run against ClickHouse Cloud.** The IP access list holds one entry,
the VM, and that VM is an e2-micro that cannot compile Rust. Production export
needs a container build like the indexer's. Output is also still local disk
rather than object storage.

### Still open

- Phase 3 §2.2 cheap ops fixes: `DEPLOY.md` project drift, watchdog unverified
  since the redeploy, two dead Secret Manager versions.
- Phase 3 §2.4: split `main.rs` (now ~1000 lines). Unblocked — the state-machine
  tests it was waiting on exist.
- Phase 3 §2.6: archive backfill for the 73,873-slot hole.
- The cold path has never touched production data.
- 8 local commits unpushed. Still a deliberate decision, not an oversight: the
  Day 7 entry describes the security incident in detail.

## Day 8 (cont.) — the watchdog that never ran, and the ClickHouse cost cliff (2026-08-09)

Started as "pick a low-risk task"; the audit that was supposed to justify the
choice turned up two things bigger than the task.

### 1. The freshness watchdog has never run

Audited the live VM. No `/var/lib/klend/watchdog.sh`. No `klend-watchdog.service`.
Only `klend-dashboard.service`/`.timer` (Aug 8) exist.

The guard written for the 2026-08-05 freeze — the incident that cost 8h40m of
unrecoverable history while the container reported `Up` at CPU 0% — has never been
running. `--restart always` structurally cannot catch that failure: Docker never
considered the container unhealthy.

**Root cause.** `deploy/gce-startup.sh` contains the install logic (lines
~105-131), reading the script from the `watchdog-script` instance metadata
attribute. The instance carries `watchdog-script` and **no `startup-script` key**,
so the file that consumes it is never executed. The last
`google-startup-scripts` run (Aug 5) was the default no-op.

Second consequence, worth its own line: the redeploy path documented in
`DEPLOY.md`, `sudo google_metadata_script_runner startup`, currently does nothing.
Phase 3 §0 called that command "the documented redeploy path and probably right";
it is attached to nothing.

`deploy/install-watchdog.sh` now installs it directly rather than depending on
that wiring.

### 2. `gce-startup.sh` would have broken production if attached

It carried `CLICKHOUSE_URL=YOUR_INSTANCE.REGION.gcp.clickhouse.cloud:9440` as a
literal. Attaching it as `startup-script` — the obvious "fix" for §1 — would make
the next reboot `docker rm -f klend-indexer` and relaunch it against a hostname
that does not resolve, taking the pipeline down and keeping it down.

Now takes a rendered `__CH_URL__` and aborts if the placeholder survives. Aborting
is fail-SAFE and the ordering is why: nothing destructive has run at that point,
so the existing container is untouched and `--restart always` brings it back with
the env already captured in its config.

**Deliberately still not attached.** Reboot survival already works through the
restart policy — verified: secrets live in the container's `Config.Env`, no mounts
— and the watchdog installer does not depend on the startup script. Attaching it
is a separate, verified step, not a side effect of today.

Note this also diverges from `SECRETS.md`: the design says Secret Manager → tmpfs
at boot, but the values are in the container config on disk, because the container
was launched by hand rather than by the script.

### 3. STALE_THRESHOLD is now measured, not assumed

Before arming something that can restart production, measured what "normal" is.
Over **52,100** distinct ingest seconds across four days of steady state:

| p50 | p99 | p99.9 | max |
|---|---|---|---|
| 4s | 34s | 58s | **90s** |

The longest ingest silence ever observed is 90 seconds. `STALE_THRESHOLD=900s`
is ~10× that. Kept, now with evidence.

Same asymmetry as `RESUME_TOLERANCE_SLOTS`: firing late costs detection latency,
firing early restarts a **healthy** indexer and manufactures the seam the watchdog
exists to prevent. Against an 8.4h incident, anything under ~20 min is a rounding
error, so spend the slack on false-positive margin.

Known limitation recorded in the script: it cannot distinguish "indexer stalled"
from "Solana halted". A long chain halt would restart the indexer repeatedly
(bounded by `MIN_SETTLE`). Harmless — each restart resumes from checkpoint — but
noisy. The real fix is a heartbeat carrying `tip_slot`, which the indexer does not
write.

### 4. Watchdog now runs as `klend_ro`

It performs exactly one `max(ingested_at)` query and was holding the **admin**
password to do it. Verified on the VM: the SELECT succeeds and an INSERT is
refused with `klend_ro: Not enough privileges`. Same reasoning that moved the
dashboard off admin on Day 7; missed then only because the watchdog was never
installed.

The freshness query also dropped its string literal —
`toUnixTimestamp(now64(3)) - toUnixTimestamp(max(ingested_at))` — because the
script travels workstation → metadata → curl → file → bash, and the Day 5
blank-password bug came from exactly that class of pipeline damage.

### 5. Tested dynamically against a real stall

`docker pause` reproduces the Aug 5 shape precisely: process alive, container not
"exited", nothing being written. Verified first on a throwaway container that
`docker restart` recovers a paused container on Docker 27.4.1 (paused → running),
so the test could not strand the indexer.

```
23:07:18  docker pause
23:10:19  watchdog: fresh (190s)
23:15:19  watchdog: fresh (490s)
23:20:19  watchdog: fresh (790s)
23:25:20  watchdog: STALE 1091s > 900s; restarting klend-indexer
23:25:21  watchdog: restarted
          indexer: resuming from checkpoint slot=438287802 (inclusive)
```

Staleness tracked the prediction to the second at every check.

**Zero data loss, measured rather than assumed.** Largest slot jump across the
whole paused window: **117 slots** (~47s, an ordinary quiet span) over 246 slots
carrying data. An 18-minute hole would have shown as ~2,700. `slot_gaps` stayed
at 1 row — no false gap recorded. Ingest age back to 1s.

A background poller held a hard auto-unpause backstop at 25 minutes, so a dropped
session could not leave the container paused past the ~6000-slot replay window and
turn a test into real unrecoverable loss. It was never needed.

**Honest limit: this did not exercise the Day 8 startup gap detector.** The
running image is `created=2026-08-08T02:00:12Z`, which predates today's commit, so
that code is not deployed. The test validated the watchdog, the resume path, and
the replay window — not the detector.

### 6. The dashboard could not have shown any of this

Found while the indexer was paused: the page kept reporting **live** over frozen
data, and could not have done otherwise. The status dot was
`max(slot) - checkpoint`, and those two advance together and freeze together, so
their difference is ~0 whether the pipeline is healthy or wedged.

The static export made it worse. `generated_at` refreshes every 60s because the
exporter queries ClickHouse independently of the indexer, so the page showed a
25-second-old timestamp above a slot number that had not moved in four minutes.
Fresh clock, stale data — the same "looks live while going stale" shape the
`775ee31` message called out for the uploader, one layer down.

Fixed by adding `ingest_age_s` to the system query and driving the dot and a new
Last Write tile from it — the same expression the watchdog acts on, so the page
and the guard cannot disagree. Thresholds from the measured distribution: 120s
(above the observed 90s max) and 900s (the watchdog's own).

**Committed, NOT deployed.** `queries.js` ships inside the export container image
while `index.html` uploads straight to the bucket, so the two must ship together
or every field after `checkpoint` shifts by one.

### 7. ClickHouse is burning $18.02/day, and credits expire Aug 19

Pulled from the billing API rather than the dashboard summary.

| Date | Cost |
|---|---|
| Jul 21 – Aug 4 | **$0.00** (15 days) |
| Aug 5 | $12.15 |
| Aug 6 / 7 / 8 | $18.02 each |

100% compute. Storage $0, data transfer $0. The zero-days are the pre-deploy
period when the service idled; billing began the day the indexer went 24/7.

**Why:** `numReplicas: 2`, `minReplicaMemoryGb: 12`, `maxReplicaMemoryGb: 12` —
autoscaling pinned, so it can never scale down. And `idleScaling: true` with a
15-minute timeout that **can never trigger**, because `FLUSH_INTERVAL_SECS = 2`
means a write every two seconds. The one mechanism that would make this cheap is
structurally unreachable.

The generalisable part: *a continuous low-volume writer defeats every idle-based
pricing model — the cost is set by how often you write, not how much.* The flush
cadence was tuned for data safety, and that is exactly what makes an idle-billed
service the wrong host.

**The cliff:** $216.47 remaining ÷ $18.02/day = 12 days. Credits expire Aug 19
regardless, leaving ~$36 stranded. From Aug 20 it is a credit card at ~$548/month.
Demo day (Aug 17) is inside the window; the cliff lands three days later, at the
start of the Aug 18 – Sept 12 window §11e says holds Phase 1's hardest work.

Alchemy, for contrast: 15,540 of 66.7M CUs — **0.02%**. The metered upstream
everyone budgeted for is not the cost. The managed database is.

**Decision: export to Parquet/GCS before Aug 19 and let the service lapse.** Full
reasoning in the vault note. This promotes the Day 8 cold path from portfolio
piece to exit strategy, and makes "never run against production" the blocking item
with a hard date. Consequence: every day of ingest between now and Aug 19 becomes
permanent in the archive, which is what raised the watchdog from hygiene to the
thing protecting the final accumulation window.

### Still open

- Cold path against production, now with an Aug 19 deadline.
- Dashboard freshness fix is committed but needs an image rebuild to deploy.
- Obligation value columns (deposited/borrowed/LTV) still computed and discarded.
- `startup-script` still unattached; reboot survival rests on the restart policy.
- Phase 3 §2.4 (`main.rs` split), §2.6 (archive backfill).
- Day 8 indexer code (gap detector) is committed but not deployed.

## Day 8 (cont. II) — value columns, the flow view, and a broken redeploy path (2026-08-09)

Continues the entry above. Everything here followed from one question: what
actually puts the data flow at risk.

### 1. Live data flow section

`web/index.html` gains a four-stage animated pipeline, paced from a new `flow`
query (per-minute writes split by account type, last 60 minutes).

The rule the section is built around: **when ingest stops, the animation stops.**
A flow diagram is the easiest thing on a dashboard to fake — left on a CSS loop
it animates just as convincingly over a dead pipeline as a live one. Dot count
and dot speed both derive from measured throughput, and the stall threshold, the
status dot, and the server-side watchdog all read the same freshness number.

Two bugs came out of testing the **stalled** state rather than only the happy
path:

- The header rendered `no writes for 22m ago`, because the duration helper baked
  in a suffix the two sentences need in different positions.
- More seriously, the sparkline packed whatever buckets existed left-to-right
  across an axis labelled "60 min ago → now". During a stall the series is short,
  so the chart **silently rescaled and looked normal** while 22 minutes of nothing
  were missing. Bars are now placed by when they happened across a fixed 60-slot
  window, so an outage renders as an actual hole. The one chart whose job is to
  make an outage obvious was hiding it.

The newest minute is excluded from the throughput average: it is a partial
bucket and always reads low, which would make a healthy pipeline appear to be
dying whenever the exporter runs mid-minute.

### 2. Obligation value columns (§11c's demo-legibility gap)

`decode.rs` has computed `deposited_value_sf`, `borrowed_assets_market_value_sf`,
`borrow_factor_adjusted_debt_value_sf`, `allowed_borrow_value_sf`,
`unhealthy_borrow_value_sf` and `lowest_deposit_liquidation_ltv` since the
decoder landed. The writer persisted **only the derived health factor**. The
store could answer "how healthy is this position" but not "how large is it" —
the half a non-specialist reads, and the half any analytical summary needs.

`schema/008_obligation_values.sql` adds six columns. Stored **raw and undivided**:
these are Kamino `Fraction` values, U68F60, so the on-chain integer is the real
value × 2^60. Converting at write time would bake that assumption into history —
if the scale is ever wrong, every stored row becomes silently incorrect and
unrecoverable. Dividing at the query edge makes a mistaken scale a one-line fix
in a view rather than a backfill. Same reasoning that keeps raw payloads in
`account_updates`.

`borrow_factor_adjusted_debt_sf` is stored even though it is only the denominator
`health_factor_bps` was already derived from, so the derived number can be
**audited against its own inputs** rather than trusted.

Verified against a real ClickHouse before going near production: 003 then 008
applied, a row inserted through the exact `INSERT` column list, read back scaled
— deposited 10000, borrowed 5000, max borrow 8500, headroom 3500, liquidation
LTV 85%, health 2.0, current LTV 50%. `UInt128` needs an explicit `toFloat64()`
before dividing, which the dashboard queries will need.

**Applied to production:** `obligation_snapshots` went 14 → 20 columns. Ingest
unaffected (age 3s immediately after), because `ADD COLUMN` is metadata-only and
the running indexer names its columns explicitly.

**Ordering is load-bearing** and is now recorded in the migration: ALTER first,
binary second. The reverse fails every insert until the ALTER lands. Rows written
before the migration read back 0, indistinguishable from a genuinely empty
position, so queries must bound by slot or filter `> 0`.

### 3. The documented redeploy path did nothing

Two defects, both of which made `DEPLOY.md` read as correct:

- `sudo google_metadata_script_runner startup` was a **no-op** — no
  `startup-script` metadata attached (§1 of the previous entry). Phase 3 §0 spent
  paragraphs arguing whether that command was the right redeploy path; it was
  attached to nothing.
- `$(gcloud config get-value project)` resolves to `gen-lang-client-0502946726`
  on this workstation, not `agentbiz-sungodlab`. **Confirmed by running it.**
  Following the build step verbatim targets the wrong Artifact Registry.

`deploy/install-startup-script.sh` renders `__CH_URL__` from `local.env` and
attaches the result, following the same pattern as the watchdog and dashboard
installers. It re-runs the placeholder guard against the *unrendered* file before
attaching — not for today, but so a future edit that drops the abort is caught
here rather than by a reboot that takes the pipeline down.

`startup-script` is now attached, so the documented redeploy works for the first
time.

### 4. Insights brief — decided, not yet built

Chosen: **deterministic templates, not an LLM.** Rules compute real deltas,
templates render them with the numbers inline. Every sentence traceable to a
query, reproducible, no new dependency in the 60-second job.

The reasoning is the same one that runs through the rest of the page: an LLM
writing confident narrative over 5.5 days of gap-containing, price-less data is
exactly the failure mode this project has spent two incidents learning to avoid.

Constraints recorded so they are not rediscovered:
- **~5.5 days of history**, with the 73,873-slot hole in it. Day-over-day is
  solid; "insight of the week" is not supportable until ~Aug 11-12.
- **No USD prices.** All amounts are native token units.
- **Blocked on §2 landing in production** — position-size findings have no data
  until the new binary has been running.

Sequencing chosen: value columns → insights → cold path.

### Still open

- Cloud Build in flight; deploy is one command once it lands, and that relaunch
  also ships the Day 8 gap detector, committed but undeployed all day.
- Dashboard changes (`ingest_age_s`, flow section) need an export image rebuild;
  `index.html` and `queries.js` must ship together or every field after
  `checkpoint` shifts by one.
- Cold path against production, hard Aug 19 deadline.
- Insights brief.

## Day 8 (cont. III) — the data-slice truncation: 44 hours of Obligation lost

The worst incident of the project, found by auditing the data rather than by any
guard, and caused by an optimisation this project's own plan called "the primary
cost lever".

### What broke

`accounts_data_slice { offset: 0, length: 4664 }` is applied at the **request**
level, not per account type. Yellowstone returns an **empty** payload for any
account shorter than the requested length — not a truncated one, empty.

Reserve is 4,664 bytes and matched exactly, so it kept flowing perfectly.
Obligation is 3,344 and UserMetadata is 1,032. From slot **437,903,892**
(2026-08-08 02:02 UTC) both arrived with zero-length payloads, classified
`untagged:0b`, and were stored as empty rows.

The cutover is clean, which is what made it unambiguous:

```
Obligation      3344   165066   437280282 .. 437903877
untagged:0b        0    17289   437903892 .. 438304801
```

No overlap. 1,396 of the 1,708 `untagged:0b` pubkeys were previously Obligations.

Unrecoverable as history. Current state is recoverable via `getProgramAccounts`.

### Why every guard stayed green

This is the part worth keeping. The project has: checkpoint advance, ingest
freshness (p50 4s), `slot_gaps` detection, `--restart always`, a watchdog proven
by an induced 18-minute stall that same day, and a dashboard liveness dot fixed
that same day. **Every one of them was correct and green throughout.**

None of them was wrong. They all answer "is data moving?" and data was moving —
at full rate, with fresh timestamps, with no missed slots. Nothing in the suite
asks "is the data the right *shape*?"

> Throughput is not integrity. A monitoring suite built entirely from liveness
> signals is blind to "working perfectly and producing garbage".

Note the timing: this was ingesting empty Obligations for the entire session in
which the watchdog was installed, the liveness dot was fixed, and the flow
sparkline was corrected. A full day of monitoring work, on a pipeline that was
actively destroying data the whole time.

### The cost premise was also wrong

The plan justified the slice as the primary cost lever, "~87% of spend is reserve
churn". Measured against the actual quota: Alchemy is at **15,540 of 66,666,667
CUs, 0.02% of plan**, peak 390 of 10,000 CU/s. The saving was against a bill that
does not exist. A share-of-spend framing makes any optimisation look urgent
without ever checking the absolute number against the limit.

Meanwhile the real cost problem that week was ClickHouse at $18.02/day, set by
write cadence. The cost attention was pointed at the cheap component the whole
time.

### Fixed

- `f6c0aa4` — slice removed. One request-level slice cannot serve both Obligation
  (3,344) and Reserve decode (4,664). If bandwidth ever binds, the correct shape
  is one subscription per account type.
- `22008d9` — `src/payload.rs`. A funded account with a zero-length payload is a
  shape the chain does not produce, so this guard needs **no threshold**, unlike
  `RESUME_TOLERANCE_SLOTS`. Verified against the incident: all 17,289 affected
  rows carry `lamports > 0`, zero exceptions. Would have fired on the first
  Obligation, ~44 hours earlier. 11 tests, including `a_healthy_reserve_stream_
  never_fires` — the counterfactual a throughput guard cannot distinguish.
- Test count 60 → 71.

### Artefact 2 assessment, done in passing

`docs/artefact2-dataset.md`. The finding that matters beyond this incident:
**four of the plan's five candidate datasets need transaction data this indexer
does not collect.** Liquidator identity and tips are not in account updates at
any slot. The plan assumed an account index implied them. Either the subscription
grows a transaction stream, or Artefact 2 scopes to health distribution — which
is genuinely well covered (163,184 snapshots, 140,185 accounts) up to the
truncation.

Also corrected in passing: Reserve decode rate is **100%** on recent slots. The
44% figure computed earlier today was pre-Aug-7 history, not a defect. Worth
catching before it reached a published number.

### Still open

- **Deploy is blocked**, not by code. The redeploy command was denied by the
  permission classifier; production is still writing empty rows until it runs.
  Image with both fixes is building.
- `bin/snapshot` to re-establish current Obligation state, after deploy.
- Cold path against production, hard Aug 19 deadline.
- Insights brief, still blocked on Obligation data flowing.
- The transaction-stream scoping decision for Artefact 2.
