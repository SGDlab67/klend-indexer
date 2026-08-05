-- Decoded Obligation snapshots — one row per Obligation account update.
-- Populated by the field-level decode path in src/decode.rs.
--
-- Stored separately from account_updates so that decode is replayable: the raw
-- `data` column in account_updates is the source of truth, and this table is a
-- materialised view of it — computed at ingest time but reconstructable from raw
-- if decode logic changes.
--
-- Key demo query: "show me obligations near liquidation"
--   SELECT pubkey_b58, health_factor_bps / 1000000 AS health_factor, num_deposits, num_borrows
--   FROM obligation_snapshots FINAL
--   WHERE health_factor_bps < 1050000
--   ORDER BY health_factor_bps ASC
CREATE TABLE IF NOT EXISTS klend.obligation_snapshots
(
    slot            UInt64,
    write_version   UInt64,
    pubkey          FixedString(32),
    pubkey_b58      String ALIAS base58Encode(pubkey),

    -- Obligation owner (the wallet that controls this position).
    owner           FixedString(32),
    owner_b58       String ALIAS base58Encode(owner),

    -- Which lending market this obligation is in.
    lending_market  FixedString(32),

    -- Collateral and borrow position counts (0..8 deposits, 0..5 borrows).
    num_deposits    UInt8,
    num_borrows     UInt8,

    -- Health factor × 10^6. 1_000_000 = exactly 1.0 (position at liquidation
    -- threshold). u64::MAX = no debt (infinite health). Query it as:
    --   health_factor_bps / 1000000.0  for a human-readable ratio.
    health_factor_bps UInt64,

    -- Bit 0: has_debt, bit 1: elevation_group, bit 2: autodeleverage active.
    flags           UInt8,

    -- Elevation group (e-mode category); 0 = none.
    elevation_group UInt8,

    -- Referrer pubkey for the obligation.
    referrer        FixedString(32),

    ingested_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (pubkey, slot, write_version)
PARTITION BY intDiv(slot, 10000000);
