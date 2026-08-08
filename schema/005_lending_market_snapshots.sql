-- Decoded LendingMarket snapshots — one row per LendingMarket account update.
-- Populated by the field-level decode path in src/decode.rs.
--
-- Stored separately from account_updates so that decode is replayable: the raw
-- `data` column in account_updates is the source of truth, and this table is a
-- materialised view of it — computed at ingest time but reconstructable from raw
-- if decode logic changes.
--
-- Key dashboard query: "show me all lending markets and their state"
--   SELECT pubkey_b58,
--          if(flags & 1, 'YES', 'NO') AS emergency_mode,
--          if(flags & 2, 'YES', 'NO') AS autodeleverage,
--          if(flags & 4, 'YES', 'NO') AS borrow_disabled,
--          if(flags & 8, 'YES', 'NO') AS immutable,
--          referral_fee_bps / 100.0 AS referral_fee_pct,
--          replaceRegexpAll(UTF8ToString(name), '\0', '') AS name_str
--   FROM lending_market_snapshots FINAL
--   ORDER BY slot DESC
CREATE TABLE IF NOT EXISTS klend.lending_market_snapshots
(
    slot            UInt64,
    write_version   UInt64,
    pubkey          FixedString(32),
    pubkey_b58      String ALIAS base58Encode(pubkey),

    -- Lending market owner authority.
    owner           FixedString(32),
    owner_b58       String ALIAS base58Encode(owner),

    -- Quote currency mint (the base token for this market, e.g. USDC mint).
    quote_currency  FixedString(32),

    -- Flags: bit 0 = emergency_mode, bit 1 = autodeleverage, bit 2 = borrow_disabled, bit 3 = immutable.
    flags           UInt8,

    -- Referral fee in basis points (e.g. 50 = 0.5%).
    referral_fee_bps UInt16,

    -- Max percentage of debt that can be closed in a single liquidation.
    liquidation_max_debt_close_factor_pct UInt8,

    -- UTF-8 market name, null-padded to 32 bytes.
    name            FixedString(32),

    ingested_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (pubkey, slot, write_version)
PARTITION BY intDiv(slot, 10000000);
