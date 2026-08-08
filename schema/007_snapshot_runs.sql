-- Provenance for RPC snapshot runs (Phase 2, "Option A").
--
-- The stream only ever sees accounts that CHANGE. An account that has been idle
-- since the indexer started is not missing data, it is invisible data: no row
-- exists and nothing in the dataset says one should. `getProgramAccounts` closes
-- that hole by enumerating every klend account that currently exists, decoding
-- it, and writing it at the RPC's context slot.
--
-- Snapshot rows go into the SAME tables as stream rows, marked by
-- `write_version = 0`. Geyser's write_version is a monotonic per-node counter
-- that is never 0 in practice, so the value is free as a sentinel, and it sorts
-- first within (pubkey, slot, write_version) rather than colliding with a real
-- write. This table is where the run itself is recorded, so provenance is a
-- join away instead of a convention someone has to remember:
--
--   SELECT * FROM account_updates FINAL WHERE write_version = 0   -- snapshot rows
--   SELECT * FROM account_updates FINAL WHERE write_version > 0   -- stream rows
--
-- What a snapshot canNOT do is fill a historical gap. `getProgramAccounts` has no
-- at-slot variant on any provider we use, so the bytes it returns are current
-- bytes, dated to the current slot. See docs/backfill-phase2.md.
CREATE TABLE IF NOT EXISTS klend.snapshot_runs
(
    -- RPC context slot the snapshot was taken at. This is the `slot` written on
    -- every row the run produced, so it is the join key back to the data.
    slot            UInt64,

    -- Which subscription-equivalent filter set ran: 'known' = the three decoded
    -- discriminators (Obligation, Reserve, LendingMarket), 'all' = every account
    -- owned by the program, including kinds we cannot yet decode.
    scope           LowCardinality(String),

    -- Accounts returned by RPC, and rows actually written. They differ when an
    -- account fails to decode: the raw row still lands, the decoded one does not.
    accounts        UInt64,
    rows_written    UInt64,
    decode_failures UInt64,

    -- How long the run took, RPC call included. A snapshot that starts taking
    -- minutes is the early warning that the account set has outgrown a single
    -- getProgramAccounts call.
    --
    -- There is no `started_at` column on purpose: the provenance row is written
    -- last, so `ingested_at` is the run's completion time and
    -- `ingested_at - duration_ms` is its start. Storing both would be storing the
    -- same fact twice, with two chances to disagree.
    duration_ms     UInt64,

    ingested_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (slot, scope);
