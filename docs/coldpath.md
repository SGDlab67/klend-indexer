# The cold analytical path

Date: 2026-08-09
Status: built and validated locally; not yet run against ClickHouse Cloud

Two binaries and one shared module:

| Piece | File | Role |
|---|---|---|
| Layout | `src/coldpath.rs` | Arrow schema, partition naming, shared by both binaries |
| Export | `src/bin/parquet_export.rs` | ClickHouse → partitioned Parquet |
| Query | `src/bin/coldquery.rs` | DataFusion over the Parquet |

## 1. Why a second store exists

Not "ClickHouse is slow". Two specific limits, both already written down before
today.

**`FINAL` does not scale.** Every dashboard query collapses a
ReplacingMergeTree at read time. §3b of the Phase 2 note measured this as
instant at 355K rows and flagged that it will not survive 351M. The export
resolves dedup once, so no cold query ever pays it.

**The hot primary key is the wrong shape for slot-range work.**
`account_updates` is `ORDER BY (pubkey, slot, write_version)`.
`schema/001_init.sql` already carries the admission: it "serves Phase 2's 'all
accounts around slot N' poorly, since slot is not the leading column". The cold
copy is partitioned and sorted by slot, which is precisely the ordering the hot
store chose not to have. This is not a workaround for a mistake; it is a second
ordering of the same data, which is what a second store is for.

ClickHouse stays authoritative. The Parquet is derived and reproducible, so a
bad export is deleted rather than repaired.

## 2. Layout

```
<dir>/slot_bucket=000437000000/account_updates.parquet
<dir>/slot_bucket=000438000000/account_updates.parquet
```

- **1M slots per partition** (~4.6 days), one tenth of the ClickHouse partition
  width. ClickHouse's partitions are sized for merge behaviour on a
  continuously written table; these are sized for how little a query has to open.
- **Hive-style `key=value`**, not a bare `slot_<n>` directory. This is
  load-bearing rather than cosmetic: a bare directory is not listed recursively,
  and registering the table fails with "cannot infer schema from an empty
  location". `key=value` also promotes the bucket to a queryable column.
- **Zero-padded to 12 digits**, so lexical order matches numeric order. Object
  stores list keys lexically, and an unpadded `9000000` sorts before `10000000`.
- **Rows sorted by slot within each file**, which is what makes the row-group
  min/max statistics tight enough to prune on.

## 3. Two levels of skipping, both verified

Measured on the local fixture, not asserted:

```
EXPLAIN SELECT count(*) FROM account_updates WHERE slot_bucket = '000438000000'

physical_plan
  ProjectionExec: expr=[200 as count(*)]
    PlaceholderRowExec
```

The partition predicate is answered from the directory listing and the count
from Parquet footer metadata. No data is read at all.

```
EXPLAIN ANALYZE SELECT count(*), sum(lamports) FROM account_updates
WHERE slot BETWEEN 437387843 AND 437390000

row_groups_pruned_statistics=1
row_groups_pruned_bloom_filter=1
```

Within the surviving files, row groups are skipped on `slot` min/max.

## 4. Queries

```sh
cargo run --bin coldquery -- gaps          # derive slot gaps from the data
cargo run --bin coldquery -- activity      # volume per partition
cargo run --bin coldquery -- hot-accounts  # most-rewritten accounts
cargo run --bin coldquery -- sql "SELECT ..."
```

`gaps` is the one that matters. It is the hand derivation from
`docs/backfill-phase2.md` §2a — walking `account_updates` in slot order looking
for jumps — turned into a repeatable query. That changes what `slot_gaps` is
worth: the table records what the indexer **noticed**, this records what the
data **shows**, and the two disagreeing is a defect rather than an opinion. On
2026-08-05 they disagreed, and nothing in the system could say so.

The threshold is `resume::RESUME_TOLERANCE_SLOTS`, imported rather than
repeated, so the live detector and the retrospective audit cannot drift apart.

A `base58` scalar UDF renders pubkeys at the query edge. Storage keeps the 32
raw bytes; rendering is a presentation concern and is applied to the rows that
survive aggregation, not to every row scanned.

## 5. Defect found while building this

**klickhouse truncates `FixedString(N)` at the first null byte on read.**
`types/deserialize/string.rs` does `position(|x| *x == 0)` then `truncate`.
Sensible for the padded ASCII the type usually holds; wrong for a Solana
pubkey, which is 32 arbitrary bytes. About 12% of keys contain at least one
`0x00` (`1 - (255/256)^32`), and every one of them comes back short.

The indexer never hit this because it only ever **writes** pubkeys, and
serialization pads back to N. The exporter is the first code in the project to
read them out, so it is the first that could be corrupted by it.

Fix: select `hex(pubkey)` and decode in Rust. Lossless by construction, computed
server-side, and 32 extra bytes per row against a 4664-byte payload is not worth
measuring.

It surfaced as an error rather than as bad data only because
`FixedSizeBinaryBuilder::append_value` rejects a width mismatch and that error
was propagated instead of padded. A `resize(32)` there would have produced
Parquet files that looked fine and joined against nothing.

## 6. Validation

Local ClickHouse, throwaway `klend_coldpath_test` database, dropped afterwards.
The fixture was shaped to have known answers:

- 4,301 rows including one deliberate replay duplicate → **4,300 exported**,
  confirming `FINAL` collapses it.
- Slots spanning the 438,000,000 boundary → **4,100 / 200 split**, matching the
  arithmetic exactly.
- The 2026-08-05 gap at its real coordinates → `gaps` returns
  **437,313,969 → 437,387,843, 73,873 slots**.
- Fixture pubkeys were null-padded, i.e. exactly the truncation case, and came
  back as full 44-character base58.

## 7. Production runbook (the Aug 19 deadline)

ClickHouse Cloud credits expire **2026-08-19**. Anything not exported by then is
gone. The export must run **on the VM**: it is the only host on the service's IP
access list, and it cannot compile Rust, so the binaries ship in the image
(`25e226c`) rather than being built there.

Run it from the same image the indexer uses, overriding the entrypoint. The
indexer keeps running throughout; the export is a reader.

```bash
# On the VM. /run/klend-indexer.env is written by gce-startup.sh into tmpfs and
# holds CLICKHOUSE_URL/USER/PASSWORD/SECURE. It does NOT survive a reboot: if it
# is missing, re-run the startup script before the export rather than recreating
# it by hand, or the secret ends up on disk.
sudo mkdir -p /var/klend/parquet && sudo chown 10001 /var/klend/parquet

docker run --rm \
  --entrypoint /usr/local/bin/klend-parquet-export \
  --env-file /run/klend-indexer.env \
  -e KLEND_PARQUET_DIR=/out \
  -v /var/klend/parquet:/out \
  us-central1-docker.pkg.dev/agentbiz-sungodlab/klend/klend-indexer:latest
```

`/var` on Container-Optimized OS is mounted `noexec`, which blocks execution and
not writes, so it is a valid target for output. Disk is the constraint to check
first: an e2-micro's boot disk is small and the export writes before it uploads.

Then push to object storage and verify the round trip rather than assuming it:

```bash
gsutil -m rsync -r /var/klend/parquet gs://<bucket>/klend/account_updates/
gsutil du -sh gs://<bucket>/klend/account_updates/
```

Verify the export against the source before trusting it. Row counts must agree
per partition, and the `FINAL` collapse means the Parquet count will be **lower**
than the raw table count. That difference is the point of the export, so check it
is the expected difference rather than checking it is zero:

```bash
docker run --rm --entrypoint /usr/local/bin/klend-coldquery \
  -e KLEND_PARQUET_DIR=/out -v /var/klend/parquet:/out \
  us-central1-docker.pkg.dev/agentbiz-sungodlab/klend/klend-indexer:latest gaps
```

The `gaps` output must agree with `klend.slot_gaps`, which as of 2026-08-09 holds
exactly one entry (437,313,969 – 437,387,843). Disagreement is a defect in one of
the two, not a matter of opinion.

**Export the snapshots too.** `klend.obligation_snapshots` (163,184 rows) is a
separate table and is not covered by the `account_updates` export path. It is the
table Artefact 2 is actually built on. Currently unhandled.

## 8. Not done yet

- **Never yet run against ClickHouse Cloud.** The binaries now ship in the image,
  so the blocker is a command that has not been run, not missing capability.
- **`obligation_snapshots` has no export path.** See above; this is the gap that
  matters most for Artefact 2.
- **Output is local disk, not object storage.** `object_store` with a GCS
  backend is the intended target and the layout is already compatible.
- **Compression is unmeasured on real payloads.** ZSTD level 3 barely dented
  the fixture because `randomString` is incompressible. Real account data is
  mostly zeroed tails and should compress hard.
- **Incremental export is coarse.** Closed partitions are skipped, the newest is
  rewritten whole. Fine at this size; wrong eventually.
