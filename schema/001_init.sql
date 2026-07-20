-- klend-indexer schema.
--
-- Runs ONCE, on first boot of an empty data volume. It does NOT re-run on
-- container restart. Editing this file after the first `docker compose up` has
-- no effect until the volume is destroyed — which is why the week-2 tables are
-- defined here now rather than added later, even though nothing writes to them
-- yet. Deferring them would cost a volume rebuild, and §8c is explicit that
-- retrofitting the resume path is worse than building it in.

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

-- ---------------------------------------------------------------------------
-- Resume checkpoint (§8c). Not yet written to.
-- ---------------------------------------------------------------------------
-- On startup: gap = tip - last_processed_slot. Under ~5500 slots, resume via
-- from_slot; over, stream from the tip immediately and record the gap below.
CREATE TABLE IF NOT EXISTS klend.ingest_checkpoint
(
    stream              LowCardinality(String),
    last_processed_slot UInt64,
    updated_at          DateTime64(3) DEFAULT now64(3)
)
-- Latest checkpoint per stream wins. Here the version column does real work:
-- concurrent or retried writers can collide on the same key with genuinely
-- different values, unlike account_updates.
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY stream;

-- ---------------------------------------------------------------------------
-- Known gaps (§8c). Not yet written to.
-- ---------------------------------------------------------------------------
-- "Never silently skip. A gap you know about is a backlog item; a gap you don't
-- is a corrupt dataset." Every gap gets a row, and `filled_at` is nullable so an
-- unfilled gap is a different SHAPE from a filled one, not a sentinel value.
CREATE TABLE IF NOT EXISTS klend.slot_gaps
(
    from_slot   UInt64,
    to_slot     UInt64,
    reason      LowCardinality(String),
    detected_at DateTime64(3) DEFAULT now64(3),
    filled_at   Nullable(DateTime64(3))
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (from_slot, to_slot);
