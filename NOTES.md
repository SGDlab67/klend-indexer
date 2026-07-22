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
