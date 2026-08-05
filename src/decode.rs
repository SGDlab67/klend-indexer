//! Field-level decode for klend account types.
//!
//! Uses klend-interface's zero-copy, #[repr(C)] Pod types directly via bytemuck.
//! The types are byte-for-byte identical to what arrives on the wire (after the
//! 8-byte Anchor discriminator), so no deserialisation step — just cast the slice
//! and read the fields.

use klend_interface::state::Obligation;

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
}
