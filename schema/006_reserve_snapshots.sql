-- Decoded Reserve snapshots — one row per Reserve account update.
-- Populated by the field-level decode path in src/decode.rs.
--
-- Only covers the liquidity portion of the Reserve (first 1352 struct bytes
-- after the 8-byte discriminator). Full Reserve is 8616 bytes but the gRPC
-- stream trims it to 3344 bytes (Obligation wire size).
--
-- Key demo query: "show me reserves with low available liquidity"
--   SELECT pubkey_b58, available_amount, borrowed_amount_sf, mint_decimals
--   FROM reserve_snapshots FINAL
--   WHERE available_amount < 1000
--   ORDER BY available_amount ASC
CREATE TABLE IF NOT EXISTS klend.reserve_snapshots
(
    slot            UInt64,
    write_version   UInt64,
    pubkey          FixedString(32),
    pubkey_b58      String ALIAS base58Encode(pubkey),

    -- Lending market this reserve belongs to.
    lending_market  FixedString(32),

    -- Mint of the underlying liquidity token (e.g. USDC, SOL).
    liquidity_mint  FixedString(32),

    -- Vault where deposited liquidity is held.
    supply_vault    FixedString(32),

    -- Vault where protocol and referrer fees are collected.
    fee_vault       FixedString(32),

    -- Available (unborrowed) liquidity in native token units.
    available_amount UInt64,

    -- Total borrowed amount (scaled fraction).
    borrowed_amount_sf UInt128,

    -- Market price in quote currency (scaled fraction).
    market_price_sf UInt128,

    -- Decimals of the liquidity mint (e.g. USDC = 6, SOL = 9).
    mint_decimals   UInt64,

    -- Accumulated protocol fees (scaled fraction).
    acc_protocol_fees_sf UInt128,

    -- Accumulated referrer fees (scaled fraction).
    acc_referrer_fees_sf UInt128,

    ingested_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (pubkey, slot, write_version)
PARTITION BY intDiv(slot, 10000000);
