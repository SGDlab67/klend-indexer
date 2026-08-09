-- Add the position VALUE columns to obligation_snapshots.
--
-- These were computed from day one and thrown away. `decode.rs` produces
-- deposited_value_sf, borrowed_assets_market_value_sf,
-- borrow_factor_adjusted_debt_value_sf, allowed_borrow_value_sf,
-- unhealthy_borrow_value_sf and lowest_deposit_liquidation_ltv on every
-- Obligation, then the writer persisted only the derived health factor. The
-- store could answer "how healthy is this position" but not "how large is it",
-- which is the half a non-specialist actually reads, and the half any analytical
-- summary needs.
--
-- ── Why ALTER rather than editing 003 ──────────────────────────────────────
-- 003 already ran against a live table holding real history. Editing it would
-- change nothing on the deployed service (schema/ only executes on a fresh
-- volume) while silently disagreeing with what is actually deployed. A
-- numbered migration is the only form that is true in both places.
--
-- `IF NOT EXISTS` on every statement so this is idempotent: safe to re-run, and
-- safe on a fresh volume where 003 has just created the table.
--
-- ── Why UInt128 raw, not a scaled Decimal ─────────────────────────────────
-- These are Kamino `Fraction` values: U68F60 fixed point, 68 integer bits and
-- 60 fractional bits, so the on-chain integer is the real value times 2^60
-- (`FRACTION_ONE_SCALED` in vendor/klend-interface/src/fraction.rs).
--
-- Stored exactly as they arrive, undivided. Converting at write time would bake
-- an assumption about the scale into history: if 2^60 is ever wrong, or Kamino
-- changes the representation, every stored row becomes silently incorrect and
-- unrecoverable. Dividing at the query edge keeps the stored number equal to
-- the on-chain number and makes a mistaken scale a one-line fix in a view
-- rather than a backfill. Same reasoning that keeps the raw payload in
-- account_updates: decode is lossy, the source bytes are truth.
--
-- To read one as a human number:
--     deposited_value_sf / pow(2, 60)
--
-- ALTER ... ADD COLUMN is metadata-only in ClickHouse. Existing parts are not
-- rewritten; old rows read back as the column default (0) until they are
-- rewritten by a merge. So rows written BEFORE this migration report 0 for
-- every value column, and 0 is indistinguishable from a genuinely empty
-- position. Any query over these columns must therefore bound itself to
-- slots after the migration, or filter `deposited_value_sf > 0`.
ALTER TABLE klend.obligation_snapshots
    ADD COLUMN IF NOT EXISTS deposited_value_sf UInt128
    COMMENT 'Total deposit market value, U68F60 fixed point (divide by 2^60)';

ALTER TABLE klend.obligation_snapshots
    ADD COLUMN IF NOT EXISTS borrowed_value_sf UInt128
    COMMENT 'Total borrow market value, U68F60 fixed point (divide by 2^60)';

ALTER TABLE klend.obligation_snapshots
    ADD COLUMN IF NOT EXISTS borrow_factor_adjusted_debt_sf UInt128
    COMMENT 'Risk-adjusted debt including borrow factors, U68F60. Denominator of health_factor_bps';

ALTER TABLE klend.obligation_snapshots
    ADD COLUMN IF NOT EXISTS allowed_borrow_value_sf UInt128
    COMMENT 'Max borrow value at weighted LTV, U68F60. Headroom = this minus borrowed_value_sf';

ALTER TABLE klend.obligation_snapshots
    ADD COLUMN IF NOT EXISTS unhealthy_borrow_value_sf UInt128
    COMMENT 'Borrow value at the liquidation threshold, U68F60. Crossing this is liquidatable';

-- Plain basis points, NOT a Fraction: 8500 = 85%. The `_sf` suffix is absent on
-- purpose, because attaching it would invite a 2^60 divide that would silently
-- produce zero.
ALTER TABLE klend.obligation_snapshots
    ADD COLUMN IF NOT EXISTS lowest_deposit_liquidation_ltv UInt64
    COMMENT 'Worst liquidation LTV across deposit reserves, basis points (8500 = 85%)';
