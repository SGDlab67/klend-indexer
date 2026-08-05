# klend-indexer

A real-time indexer for [Kamino Lend](https://kamino.com) (`klend`) account state on Solana,
streaming from a validator via Yellowstone gRPC.

> **Status: day 2 of a ~6-month build.** Right now it subscribes to klend account updates,
> identifies each account's type by its Anchor discriminator, and prints them to stdout. No
> field-level decoding, no storage. This README grows with the project rather than being written
> at the end — including its corrections.

## Why

Public analytics warehouses lag Solana by minutes (Dune 1–60 min, Flipside ~15 min) and flatten
program-specific state into generic schemas. Anything needing sub-minute freshness, pre/post
state diffs around a single instruction, or klend-specific semantics is out of their reach.
This indexer exists to produce datasets they structurally cannot.

The eventual target: **liquidation forensics** — every klend liquidation with account state at
slot−1 and slot+1, liquidator identity, tip paid, and realised bonus. Kamino cut liquidation
penalties ~90% (1% → 0.1%) in 2026; what that did to liquidator margins and competition is a
live, unanswered question.

## Pipeline position

```
Validator → Geyser → Yellowstone gRPC → [THIS] → decode → ClickHouse → Parquet/analysis
                                          ▲
                                     working today
```

## Working today

- Yellowstone gRPC subscription, `owner`-filtered to the klend program
- Prints `slot`, `pubkey` (bs58), `kind`, `data_len`, `write_version` per account update
- **Account type identification** by Anchor discriminator — discriminators are derived at
  startup from struct names (`sha256("account:<Name>")[..8]`), never transcribed
- Self-terminating sample runs via `KLEND_SAMPLE_SLOTS`, plus a summary of type mix and bandwidth
- API key held in the macOS Keychain, injected per-process, never on disk (see [SECRETS.md](SECRETS.md))

## Not built yet

Field-level decoding · ClickHouse persistence · reconnect + slot-gap recovery · backfill
reconciliation · fork/commitment handling · observability · `accounts_data_slice` bandwidth
trimming · memcmp discriminator filtering

## Requirements

- Rust (built on 1.95, edition 2024)
- A Yellowstone gRPC endpoint. Currently Alchemy pay-as-you-go — gRPC is gated to PAYG+ tiers
  across providers, and billing is by **bandwidth**, not request count.

## Setup

Store your API key in the Keychain once:

```bash
security add-generic-password -a "$USER" -s alchemy-grpc-token -w
```

`-w` with no value prompts interactively, so the key never reaches shell history or another
process's `ps` output.

## Running

```bash
./run.sh
```

`run.sh` reads the key from the Keychain and scopes it to a single child process. It contains a
*reference* to the secret, never the value, which is why it is safe to commit.

Expected output:

```
subscribed to klend KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD; waiting for updates…
slot=434017049 pubkey=HJmTFJxbH2q1eANEQiMogq1p5AgF6K3htE34xyMRkyoh kind=Obligation data_len=3344 write_version=1051
slot=434017049 pubkey=d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q kind=Reserve data_len=8624 write_version=1051
```

An account type we haven't named prints as `kind=unknown:<hex discriminator>` rather than being
folded into a catch-all — an unidentified type should be actionable, not invisible.

Logs go to stderr, data to stdout — so `./run.sh > sample.jsonl` captures clean records while
status stays visible.

## Measured findings

From 214 updates across a 774-slot span (~310 s) on mainnet, at `CONFIRMED`. Types are
**verified by Anchor discriminator**, not inferred from size:

| kind | `data_len` | updates | share |
|---|---|---|---|
| `Reserve` | 8624 B | 186 | 86.9% |
| `Obligation` | 3344 B | 26 | 12.2% |
| `UserMetadata` | 1032 B | 2 | 0.9% |
| *unidentified* | 4664 B | 0 | — (0.4% in a longer day-1 sample) |

The 4664-byte type is real but rarer than a 5-minute window catches, and matches none of the
candidate struct names — still open.

**The stream is dominated by reserve churn**, and since gRPC bills by *bandwidth* — not request
count — ~87% of the spend currently buys the accounts *least* needed for liquidation analysis.
Trimming that with `accounts_data_slice` and a memcmp discriminator filter is the main
optimisation ahead.

**klend writes are bursty, not per-slot.** Only 50 of 774 slots (6.5%) carried any klend update.
An earlier version of this README claimed reserves rewrite every slot; that is false. The
distinction matters — a per-slot assumption inflates any rows-per-day estimate ~15× and makes a
genuine gap in the data look normal.

**`write_version` is load-bearing.** ~36% of day-1 updates were repeat writes to the same account
*within the same slot*. A dedup key of `(pubkey, slot)` collapses those to one row — keeping the
highest `write_version` if it is the version column, or an arbitrary one if there is no version
column at all. Neither is acceptable here: liquidation forensics needs the pre/post state around
an instruction, so the intra-slot versions *are* the signal. The key must be
`(pubkey, slot, write_version)`, which retains every version and dedupes only replayed inserts.

**Bandwidth:** 5.3 KB/s payload measured over a clean self-terminated window ≈ 13 GB/month ≈
$1/month at $80/TB. Billed bytes track payload bytes closely, so the indexer meters its own spend
by summing `data_len`. This supersedes an earlier ~10 KB/s figure that was derived from a run
with a corrupted clock; the two still disagree by ~2× and the cause is not yet established, so
neither should be treated as the steady-state rate ([NOTES.md](NOTES.md)).

## Sampling

```bash
KLEND_SAMPLE_SLOTS=750 ./run.sh > sample.txt    # ~5 min of chain time, then exits
```

The budget is counted in **chain slots, not seconds** — slots are produced by the chain at
~400 ms and are immune to the wall-clock errors that corrupted the first measurements. The
process bounds itself rather than relying on an external timeout, which is what stops
`cargo run`'s child from outliving the thing that was supposed to kill it.

## Notes

[NOTES.md](NOTES.md) is a running blocker log — every problem hit and how it was resolved. It is
deliberately verbose; it is the raw material for later writeups.

## License

MIT, except `vendor/klend-interface/`, which is redistributed under its own
Business Source License 1.1 (copyright StroudGlobal S.A.; Change Date 2029-04-15,
Change License GPL-3.0-or-later). See `vendor/klend-interface/LICENSE`.
