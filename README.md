# klend-indexer

A real-time indexer for [Kamino Lend](https://kamino.com) (`klend`) account state on Solana,
streaming from a validator via Yellowstone gRPC.

> **Status: day 1 of a ~6-month build.** Right now it subscribes to klend account updates and
> prints them to stdout. No decoding, no storage. Everything below the "Working today" line is
> not built yet. This README grows with the project rather than being written at the end.

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
- Prints `slot`, `pubkey` (bs58), `data_len`, `write_version` per account update
- API key held in the macOS Keychain, injected per-process, never on disk (see [SECRETS.md](SECRETS.md))

## Not built yet

Account decoding · ClickHouse persistence · reconnect + slot-gap recovery · backfill
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
slot=433848453 pubkey=8rM1AY8M4YP4xNVmxhKnEUnj5CRWrcbcHpcgMoDfgqVi data_len=8624 write_version=1809
```

Logs go to stderr, data to stdout — so `./run.sh > sample.jsonl` captures clean records while
status stays visible.

## Measured findings

From ~1900 updates across 206 slots on mainnet:

| `data_len` | share | hypothesis |
|---|---|---|
| 8624 B | 95.4% | `Reserve` |
| 3344 B | 4.2% | `Obligation` |
| 4664 B | 0.4% | unidentified |
| 1032 B | 0.05% | unidentified |

Type mapping is inferred from size and update frequency — **unconfirmed** until verified against
the Anchor discriminator.

**~95% of the stream is reserve churn.** Reserves are few but rewrite constantly (interest
accrual, oracle refresh); obligations are many but only change on user action. Since gRPC bills
by bandwidth, ~95% of the cost is currently spent on the accounts *least* needed for liquidation
analysis. Fixing that via `accounts_data_slice` and discriminator filtering is the main
optimisation ahead.

**`write_version` is load-bearing.** 687 of ~1900 updates (~36%) are repeat writes to the same
account *within the same slot*. Any dedup keyed on `(pubkey, slot)` alone would silently retain
an arbitrary version — the key must be `(pubkey, slot, write_version)`.

**Bandwidth:** ~10 KB/s steady state ≈ 26 GB/month ≈ $2/month at $80/TB. Billed bytes track
payload bytes closely, so the indexer can meter its own spend by summing `data_len`. A fresh
subscription costs roughly 2× steady state for its first minute, meaning reconnects are not free.

## Notes

[NOTES.md](NOTES.md) is a running blocker log — every problem hit and how it was resolved. It is
deliberately verbose; it is the raw material for later writeups.

## License

MIT
