//! Field-level decode for klend account types.
//!
//! Uses klend-interface's zero-copy, #[repr(C)] Pod types directly via bytemuck.
//! The types are byte-for-byte identical to what arrives on the wire (after the
//! 8-byte Anchor discriminator), so no deserialisation step — just cast the slice
//! and read the fields.

use bytemuck::{Pod, Zeroable};
use klend_interface::state::{
    from_account_data, AccountDataError, LastUpdate, LendingMarket, Obligation, ReserveLiquidity,
};

/// Decoded Obligation fields relevant to liquidation forensics and the demo.
///
/// Field names match the on-chain struct; `_sf` suffix means "scaled fraction"
/// (a fixed-point value in the lending market's exchange-rate scale).
#[derive(Debug, Clone)]
#[allow(dead_code)] // Some fields reserved for Phase 2 liquidation forensics
pub struct DecodedObligation {
    /// Owner of this obligation (same as the account's pubkey owner field).
    pub owner: [u8; 32],
    /// Lending market this obligation belongs to.
    pub lending_market: [u8; 32],

    // ── Collateral (deposits) ──
    /// Number of non-empty deposit slots.
    pub num_deposits: u8,
    /// Sum of market values across all deposits (scaled fraction).
    pub deposited_value_sf: u128,
    /// Worst liquidation LTV across deposit reserves (bps, e.g. 8500 = 85%).
    pub lowest_deposit_liquidation_ltv: u64,

    // ── Debt (borrows) ──
    /// Number of non-empty borrow slots.
    pub num_borrows: u8,
    /// Market value of all borrows (scaled fraction).
    pub borrowed_assets_market_value_sf: u128,
    /// Risk-adjusted debt (scaled fraction, includes borrow factors).
    pub borrow_factor_adjusted_debt_value_sf: u128,

    // ── Risk boundaries ──
    /// Max borrow value at weighted LTV (scaled fraction).
    pub allowed_borrow_value_sf: u128,
    /// Borrow value at liquidation threshold (scaled fraction).
    pub unhealthy_borrow_value_sf: u128,

    // ── Derived (computed at decode time) ──
    /// Health factor: deposited_value / borrow_factor_adjusted_debt * 10^6.
    /// Stored as u64 fixed-point (6 decimals). u64::MAX if no debt.
    pub health_factor_bps: u64,
    /// Flags: bit 0 = has_debt, bit 1 = elevation_group non-zero, bit 2 = autodeleverage active.
    pub flags: u8,

    // ── Structural ──
    /// Elevation group (0 = none).
    pub elevation_group: u8,
    /// Referrer pubkey.
    pub referrer: [u8; 32],
}

/// Try to decode an Obligation from raw account data.
///
/// Returns `None` if the data is too short (not an Obligation, or discriminator
/// already mismatched — caller should pre-filter by discriminator). Returns
/// `Some(Err(...))` if the data is the right size but bytemuck rejects the cast
/// (shouldn't happen with verified size+alignment; treat as a bug).
pub fn decode_obligation(data: &[u8]) -> Option<anyhow::Result<DecodedObligation>> {
    // Skip the 8-byte Anchor discriminator. klend-interface Obligation is 3336 bytes
    // and the wire payload is 3344 (3336 + 8).
    const DISC_SIZE: usize = 8;
    const OBLIG_SIZE: usize = 3336;
    if data.len() < DISC_SIZE + OBLIG_SIZE {
        return None;
    }

    let payload = &data[DISC_SIZE..DISC_SIZE + OBLIG_SIZE];
    let obl: &Obligation = match bytemuck::try_from_bytes(payload) {
        Ok(o) => o,
        Err(_) => {
            return Some(Err(anyhow::anyhow!(
                "bytemuck cast failed for Obligation (alignment or padding mismatch)"
            )));
        }
    };

    let num_deposits = obl.num_deposits() as u8;
    let num_borrows = obl.num_borrows() as u8;
    let has_debt = obl.has_debt;

    let deposited_value: u128 = obl.deposited_value_sf.into();
    let borrowed_value: u128 = obl.borrowed_assets_market_value_sf.into();
    let debt_adjusted: u128 = obl.borrow_factor_adjusted_debt_value_sf.into();

    // Health factor: deposited_value / debt_adjusted, scaled to bps (×10^6).
    // u64::MAX when no debt (infinite health).
    let health_factor_bps = if debt_adjusted == 0 || has_debt == 0 {
        u64::MAX
    } else {
        // Use 128-bit math to avoid overflow: multiply first, then divide.
        (deposited_value.saturating_mul(1_000_000) / debt_adjusted) as u64
    };

    // Bits: 0=has_debt, 1=elevation_group, 2=autodeleverage active
    let mut flags: u8 = 0;
    if has_debt != 0 {
        flags |= 0b001;
    }
    if obl.elevation_group != 0 {
        flags |= 0b010;
    }
    if obl.autodeleverage_margin_call_started_timestamp != 0 {
        flags |= 0b100;
    }

    Some(Ok(DecodedObligation {
        owner: obl.owner.to_bytes(),
        lending_market: obl.lending_market.to_bytes(),
        num_deposits,
        deposited_value_sf: deposited_value,
        lowest_deposit_liquidation_ltv: obl.lowest_reserve_deposit_liquidation_ltv,
        num_borrows,
        borrowed_assets_market_value_sf: borrowed_value,
        borrow_factor_adjusted_debt_value_sf: debt_adjusted,
        allowed_borrow_value_sf: obl.allowed_borrow_value_sf.into(),
        unhealthy_borrow_value_sf: obl.unhealthy_borrow_value_sf.into(),
        health_factor_bps,
        flags,
        elevation_group: obl.elevation_group,
        referrer: obl.referrer.to_bytes(),
    }))
}

/// Decoded LendingMarket fields — key configuration worth surfacing in the dashboard.
#[derive(Debug, Clone)]
pub struct DecodedLendingMarket {
    /// Lending market owner authority.
    pub owner: [u8; 32],
    /// Quote currency mint (the base token for this market, e.g. USDC mint).
    pub quote_currency: [u8; 32],
    /// Flags: bit 0 = emergency_mode, bit 1 = autodeleverage, bit 2 = borrow_disabled, bit 3 = immutable.
    pub flags: u8,
    /// Referral fee in basis points (e.g. 50 = 0.5%).
    pub referral_fee_bps: u16,
    /// Max percentage of debt that can be closed in a single liquidation.
    pub liquidation_max_debt_close_factor_pct: u8,
    /// UTF-8 market name, null-padded to 32 bytes.
    pub name: [u8; 32],
}

/// Try to decode a LendingMarket from raw account data.
///
/// Uses klend_interface's `from_account_data` helper, which handles both
/// discriminator verification and bytemuck casting in one call.
///
/// Returns `None` if the data is too short or the discriminator mismatches
/// (both are expected — caller pre-filters by discriminator, but data may
/// be truncated or not yet big enough). Returns `Some(Err(...))` for
/// alignment errors (shouldn't happen; treat as a bug).
pub fn decode_lending_market(data: &[u8]) -> Option<anyhow::Result<DecodedLendingMarket>> {
    let market: &LendingMarket = match from_account_data(data) {
        Ok(m) => m,
        Err(AccountDataError::DataTooShort { .. }) => return None,
        Err(AccountDataError::InvalidDiscriminator { .. }) => return None,
        Err(AccountDataError::AlignmentError) => {
            return Some(Err(anyhow::anyhow!(
                "alignment error casting LendingMarket (unexpected padding)"
            )));
        }
    };

    // Encode flags into a single byte: bit 0 = emergency, bit 1 = autodeleverage,
    // bit 2 = borrow_disabled, bit 3 = immutable.
    let mut flags: u8 = 0;
    if market.is_emergency_mode() {
        flags |= 0b0001;
    }
    if market.is_autodeleverage_enabled() {
        flags |= 0b0010;
    }
    if market.is_borrow_disabled() {
        flags |= 0b0100;
    }
    if market.is_immutable() {
        flags |= 0b1000;
    }

    Some(Ok(DecodedLendingMarket {
        owner: market.lending_market_owner.to_bytes(),
        quote_currency: market.quote_currency,
        flags,
        referral_fee_bps: market.referral_fee_bps(),
        liquidation_max_debt_close_factor_pct: market.liquidation_max_debt_close_factor_pct(),
        name: market.name,
    }))
}

// ── Reserve decode ──────────────────────────────────────────────────────────

/// Prefix of Reserve that fits within the 3344-byte gRPC data slice.
///
/// Full Reserve is 8616 bytes but the gRPC stream trims it to 3344 bytes
/// (Obligation wire size). This struct covers only the safe prefix: version
/// through liquidity (1352 bytes of struct data, 1360 with discriminator).
/// Field order and #[repr(C)] match the real Reserve so bytemuck casts work.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct ReserveLite {
    pub version: u64,
    pub last_update: LastUpdate,
    /// Raw bytes of the lending market pubkey. Same layout as Pubkey (32 bytes).
    pub lending_market: [u8; 32],
    pub farm_collateral: [u8; 32],
    pub farm_debt: [u8; 32],
    pub liquidity: ReserveLiquidity,
}

const _: () = assert!(core::mem::size_of::<ReserveLite>() == 1352);

/// Decoded Reserve fields from the liquidity portion of the account.
///
/// Only covers fields accessible within the truncated gRPC data (3344 bytes on
/// wire, 3336 struct bytes after discriminator). Full Reserve is 8616 bytes but
/// the liquidity fields sit at offset 120..1352, well within the safe window.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DecodedReserve {
    pub version: u64,
    /// Slot of the last update (from last_update.slot).
    pub last_update_slot: u64,
    pub lending_market: [u8; 32],
    /// Mint of the underlying liquidity token.
    pub liquidity_mint: [u8; 32],
    /// Vault where deposited liquidity is held.
    pub supply_vault: [u8; 32],
    /// Vault where fees are collected.
    pub fee_vault: [u8; 32],
    /// Available (unborrowed) liquidity in native token units.
    pub available_liquidity: u64,
    /// Total borrowed amount as a raw u128 scaled fraction.
    pub borrowed_amount: u128,
    /// Market price of the reserve token as a raw u128 scaled fraction.
    pub market_price: u128,
    /// Decimals of the liquidity mint.
    pub mint_decimals: u64,
    /// Accumulated protocol fees as a raw u128 scaled fraction.
    pub accumulated_protocol_fees: u128,
    /// Accumulated referrer fees as a raw u128 scaled fraction.
    pub accumulated_referrer_fees: u128,
}

/// Try to decode Reserve liquidity fields from truncated account data.
///
/// The Reserve's gRPC data is sliced to 3344 bytes at the request level
/// (shared with Obligation). This function only accesses fields within the
/// safe window (bytes 8..1360 of the wire payload). Returns `None` if the
/// data is too short; `Some(Err(...))` if bytemuck rejects the cast.
pub fn decode_reserve(data: &[u8]) -> Option<anyhow::Result<DecodedReserve>> {
    const DISC_SIZE: usize = 8;
    const RESERVE_LITE_SIZE: usize = core::mem::size_of::<ReserveLite>();

    // Need at least discriminator + ReserveLite struct.
    if data.len() < DISC_SIZE + RESERVE_LITE_SIZE {
        return None;
    }

    let payload = &data[DISC_SIZE..DISC_SIZE + RESERVE_LITE_SIZE];
    let reserve: &ReserveLite = match bytemuck::try_from_bytes(payload) {
        Ok(r) => r,
        Err(_) => {
            return Some(Err(anyhow::anyhow!(
                "bytemuck cast failed for Reserve prefix"
            )));
        }
    };

    // ReserveLiquidity has Pubkey fields (mint_pubkey, supply_vault, fee_vault).
    // Pubkey is #[repr(transparent)] over [u8; 32] and Pod, so bytemuck::cast_ref
    // safely reinterprets as [u8; 32].
    let liquidity_mint: [u8; 32] = *bytemuck::cast_ref(&reserve.liquidity.mint_pubkey);
    let supply_vault: [u8; 32] = *bytemuck::cast_ref(&reserve.liquidity.supply_vault);
    let fee_vault: [u8; 32] = *bytemuck::cast_ref(&reserve.liquidity.fee_vault);

    Some(Ok(DecodedReserve {
        version: reserve.version,
        last_update_slot: reserve.last_update.slot,
        lending_market: reserve.lending_market,
        liquidity_mint,
        supply_vault,
        fee_vault,
        available_liquidity: reserve.liquidity.total_available_amount,
        borrowed_amount: u128::from(reserve.liquidity.borrowed_amount_sf),
        market_price: u128::from(reserve.liquidity.market_price_sf),
        mint_decimals: reserve.liquidity.mint_decimals,
        accumulated_protocol_fees: u128::from(reserve.liquidity.accumulated_protocol_fees_sf),
        accumulated_referrer_fees: u128::from(reserve.liquidity.accumulated_referrer_fees_sf),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_rejects_short_data() {
        assert!(decode_obligation(&[0u8; 7]).is_none());
    }

    #[test]
    fn decode_rejects_wrong_size() {
        // Too short: only discriminator
        assert!(decode_obligation(&[0u8; 10]).is_none());
    }

    #[test]
    fn decode_obligation_zeroed_is_valid() {
        // Zeroed Obligation is a valid struct — all fields are zeroable types.
        let mut data = vec![0u8; 3344];
        // Write the Obligation discriminator at bytes 0..8
        data[..8].copy_from_slice(&[0xa8, 0xce, 0x8d, 0x6a, 0x58, 0x4c, 0xac, 0xa7]);
        let result = decode_obligation(&data);
        assert!(result.is_some());
        let decoded = result.unwrap().unwrap();
        assert_eq!(decoded.num_deposits, 0);
        assert_eq!(decoded.num_borrows, 0);
        assert_eq!(decoded.flags & 0b001, 0); // no debt flag
        assert_eq!(decoded.health_factor_bps, u64::MAX); // no debt = infinite health
    }

    // ── LendingMarket decode tests ──

    #[test]
    fn lm_decode_rejects_short_data() {
        assert!(decode_lending_market(&[0u8; 7]).is_none());
    }

    #[test]
    fn lm_decode_rejects_wrong_discriminator() {
        // Full 4664 bytes, but wrong discriminator.
        let mut data = vec![0u8; 4664];
        // Write a wrong discriminator (all zeros)
        data[..8].copy_from_slice(&[0u8; 8]);
        assert!(decode_lending_market(&data).is_none());
    }

    #[test]
    fn lm_decode_zeroed_is_valid() {
        // Zeroed LendingMarket is a valid struct — all fields are zeroable.
        let mut data = vec![0u8; 4664];
        // Write the LendingMarket discriminator at bytes 0..8
        data[..8].copy_from_slice(&[0xf6, 0x72, 0x32, 0x62, 0x48, 0x9d, 0x1c, 0x78]);
        let result = decode_lending_market(&data);
        assert!(result.is_some());
        let decoded = result.unwrap().unwrap();
        assert_eq!(decoded.owner, [0u8; 32]);
        assert_eq!(decoded.quote_currency, [0u8; 32]);
        assert_eq!(decoded.flags, 0); // no flags set
        assert_eq!(decoded.referral_fee_bps, 0);
        assert_eq!(decoded.liquidation_max_debt_close_factor_pct, 0);
    }

    #[test]
    fn lm_decode_flags_encoded_correctly() {
        let mut data = vec![0u8; 4664];
        data[..8].copy_from_slice(&[0xf6, 0x72, 0x32, 0x62, 0x48, 0x9d, 0x1c, 0x78]);
        // Offsets after the 8-byte discriminator:
        //   0..8: version (8)
        //   8..16: bump_seed (8)
        //   16..48: lending_market_owner (32)
        //   48..80: lending_market_owner_cached (32)
        //   80..112: quote_currency (32)
        //   112..114: referral_fee_bps (2)
        //   114: emergency_mode (1)
        //   115: autodeleverage_enabled (1)
        // (Zero-indexed in data. data[0..8] is the discriminator, then the struct starts at data[8].)
        data[8 + 114] = 1; // emergency_mode
        data[8 + 115] = 1; // autodeleverage_enabled
        let result = decode_lending_market(&data);
        let decoded = result.unwrap().unwrap();
        assert!(
            decoded.flags & 0b0001 != 0,
            "emergency_mode flag should be set, got flags={:08b}",
            decoded.flags
        );
        assert!(
            decoded.flags & 0b0010 != 0,
            "autodeleverage flag should be set, got flags={:08b}",
            decoded.flags
        );
    }

    // ── Reserve decode tests ────────────────────────────────────────────────

    #[test]
    fn decode_reserve_rejects_too_short() {
        // Discriminator (8) + ReserveLite (1352) = 1360 bytes minimum.
        assert!(decode_reserve(&[0u8; 10]).is_none());
        assert!(decode_reserve(&[0u8; 1359]).is_none());
    }

    #[test]
    fn decode_reserve_accepts_minimum_size() {
        let mut data = vec![0u8; 1360];
        data[..8].copy_from_slice(&[0x2b, 0xf2, 0xcc, 0xca, 0x1a, 0xf7, 0x3b, 0x7f]);
        let result = decode_reserve(&data);
        assert!(result.is_some());
    }

    #[test]
    fn decode_reserve_zeroed_fields() {
        let mut data = vec![0u8; 4664];
        data[..8].copy_from_slice(&[0x2b, 0xf2, 0xcc, 0xca, 0x1a, 0xf7, 0x3b, 0x7f]);
        let result = decode_reserve(&data);
        assert!(result.is_some());
        let decoded = result.unwrap().unwrap();
        assert_eq!(decoded.version, 0);
        assert_eq!(decoded.last_update_slot, 0);
        assert_eq!(decoded.lending_market, [0u8; 32]);
        assert_eq!(decoded.liquidity_mint, [0u8; 32]);
        assert_eq!(decoded.supply_vault, [0u8; 32]);
        assert_eq!(decoded.fee_vault, [0u8; 32]);
        assert_eq!(decoded.available_liquidity, 0);
        assert_eq!(decoded.borrowed_amount, 0);
        assert_eq!(decoded.market_price, 0);
        assert_eq!(decoded.mint_decimals, 0);
        assert_eq!(decoded.accumulated_protocol_fees, 0);
        assert_eq!(decoded.accumulated_referrer_fees, 0);
    }

    #[test]
    fn decode_reserve_reads_known_fields() {
        let mut data = vec![0u8; 4664];
        data[..8].copy_from_slice(&[0x2b, 0xf2, 0xcc, 0xca, 0x1a, 0xf7, 0x3b, 0x7f]);

        // version at struct offset 0 (wire offset 8)
        data[8..16].copy_from_slice(&42u64.to_le_bytes());

        // last_update.slot at struct offset 8 (wire offset 16)
        data[16..24].copy_from_slice(&777u64.to_le_bytes());

        // liquidity.total_available_amount is at struct offset:
        //   120 (liquidity start) + 32(mint) + 32(supply) + 32(fee) = 216
        //   wire offset = 8 (disc) + 216 = 224
        data[224..232].copy_from_slice(&1_000_000u64.to_le_bytes());

        // liquidity.mint_decimals is at struct offset:
        //   216 + 8(avail) + 16(borrow_sf) + 16(price_sf) + 8(ts) = 264
        //   wire offset = 8 + 264 = 272
        data[272..280].copy_from_slice(&9u64.to_le_bytes());

        let result = decode_reserve(&data);
        assert!(result.is_some());
        let decoded = result.unwrap().unwrap();
        assert_eq!(decoded.version, 42);
        assert_eq!(decoded.last_update_slot, 777);
        assert_eq!(decoded.available_liquidity, 1_000_000);
        assert_eq!(decoded.mint_decimals, 9);
    }

    #[test]
    fn decode_reserve_uses_only_disc_plus_prefix() {
        // Verify that data beyond ReserveLite does not affect the decoded result.
        let mut data = vec![0u8; 4664];
        data[..8].copy_from_slice(&[0x2b, 0xf2, 0xcc, 0xca, 0x1a, 0xf7, 0x3b, 0x7f]);

        // version at struct offset 0
        data[8..16].copy_from_slice(&99u64.to_le_bytes());

        // Write garbage beyond ReserveLite (past struct offset 1352, wire offset 1360).
        data[1360..1368].fill(0xff);

        let result = decode_reserve(&data);
        assert!(result.is_some());
        let decoded = result.unwrap().unwrap();
        assert_eq!(decoded.version, 99);
    }
}
