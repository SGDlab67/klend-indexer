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
history instead of one-shot. Relevant right now given the 4664-byte type is still unidentified.

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

### Decode groundwork: header CONFIRMED, deep layout BLOCKED on the deployed struct version
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

Remaining for the operator: tighten the ClickHouse Cloud IP access list from `0.0.0.0/0` to the VM's
external IP, and confirm the Alchemy spend cap and alert are live now that the billed
stream is actually running.

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
