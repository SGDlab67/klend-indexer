-- klend-indexer schema.
--
-- Runs ONCE, on first boot of an empty data volume. It does NOT re-run on
-- container restart. Editing this file after the first `docker compose up` has
-- no effect until the volume is destroyed (`./ch.sh nuke`).

CREATE DATABASE IF NOT EXISTS klend;

-- ---------------------------------------------------------------------------
-- Raw account updates, exactly as they arrive off the wire.
-- ---------------------------------------------------------------------------
-- Storing the undecoded payload is deliberate. Decoding is the part most likely
-- to be wrong early (field offsets, struct versions, a type we have not yet
-- named — the 4664-byte layout is still unidentified). If we stored only decoded
-- columns, every decode bug would be unrecoverable: Yellowstone streams the tip
-- and cannot re-serve an old slot, so the original bytes would be gone for good.
-- Keeping `data` makes decode replayable against history instead of a one-shot.
CREATE TABLE IF NOT EXISTS klend.account_updates
(
    slot            UInt64,
    write_version   UInt64,

    -- Raw 32 bytes, not base58 text: 32 bytes vs ~44, and fixed width. The
    -- readable form is an ALIAS below — computed at query time, costing no
    -- storage, so nobody has to remember to call base58Encode by hand.
    pubkey          FixedString(32),
    pubkey_b58      String ALIAS base58Encode(pubkey),

    -- Anchor discriminator name where we can resolve it. LowCardinality because
    -- there are single-digit distinct values across billions of rows.
    kind            LowCardinality(String),

    owner           FixedString(32),
    lamports        UInt64,
    data            String,

    -- MATERIALIZED, so it is computed once at insert and cannot drift from the
    -- payload it describes. Derived, never supplied by the writer.
    data_len        UInt32 MATERIALIZED length(data),

    -- Ingest wall-clock. NOT a chain timestamp and must never be used as one:
    -- it is our clock, and day 1 already lost two measurements to trusting a
    -- wall clock over slot numbers. It exists to measure ingest lag.
    ingested_at     DateTime64(3) DEFAULT now64(3)
)
-- ⚠️  The version column here is `ingested_at`, NOT `write_version`, and that is
-- deliberate despite the plan naming write_version.
--
-- ReplacingMergeTree deduplicates rows sharing the full ORDER BY key, and uses
-- the version column only to pick a winner among them. Since `write_version` is
-- IN the sort key, any two rows that collide already have equal write_version —
-- so passing it as the version column is a guaranteed tie and does nothing.
--
-- A collision therefore means exactly one thing: the same account version was
-- inserted twice, i.e. a REPLAY after a reconnect (§8c). Both copies are
-- byte-identical, so any tiebreak yields the same data; `ingested_at` states the
-- intent plainly — on replay, keep the most recent copy.
--
-- This is what makes the §8c reconnect design safe. That design deliberately
-- prefers duplicates over holes ("if the write succeeds and the checkpoint
-- doesn't, you get duplicates — the reverse creates silent holes"). This table
-- is what makes duplicates harmless.
ENGINE = ReplacingMergeTree(ingested_at)

-- Every intra-slot version is retained, on purpose. ~36% of updates are repeat
-- writes to the same account within one slot; collapsing to (pubkey, slot) would
-- keep only the last and destroy the pre/post-instruction state that liquidation
-- forensics is entirely built on. The redundant-looking write_version in the key
-- is the whole product.
ORDER BY (pubkey, slot, write_version)

-- ~6M slots ≈ 27 days at 400 ms/slot, so partitions land roughly monthly —
-- ClickHouse's preferred coarseness. Partitioning on slot rather than on
-- ingest time keeps a backfill of old slots landing in the same partitions as
-- the live stream would have, instead of scattering across today's.
PARTITION BY intDiv(slot, 10000000);

-- NOTE — known limitation, not an oversight:
-- This key serves "an obligation's history by pubkey" (the week-2 checkpoint).
-- It serves Phase 2's "all accounts around slot N" poorly, since slot is not the
-- leading column. The fix is a projection or a second ordering, and it should be
-- added when Phase 2's real queries exist — not designed speculatively now.
