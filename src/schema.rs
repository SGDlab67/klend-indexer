//! Table shapes and account classification, shared by every binary that writes
//! to the klend schema.
//!
//! Extracted from `main.rs` when the snapshot writer arrived. Two writers
//! inserting into the same tables must not carry separate copies of the column
//! lists: the INSERT header is positional-by-name against the server's block,
//! so a drifted list in one binary is a wrong-column write, not a compile error.
//! Kept as a `#[path]` module rather than a lib so neither binary grows a
//! dependency on the other's build graph.

use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

use klickhouse::{Bytes, Row};
use sha2::{Digest, Sha256};

/// Candidate klend account struct names. Discriminators are derived from these,
/// never transcribed. Names are a guess from klend's public structs, not a
/// verified IDL — a wrong guess simply never matches, it cannot mislabel.
pub const CANDIDATE_ACCOUNTS: &[&str] = &[
    "Reserve",
    "Obligation",
    "LendingMarket",
    "ReferrerTokenState",
    "UserMetadata",
    "ReferrerState",
    "ShortUrl",
    "GlobalConfig",
];

/// `discriminator -> struct name`, derived once from [`CANDIDATE_ACCOUNTS`].
pub static DISCRIMINATORS: LazyLock<HashMap<[u8; 8], &'static str>> = LazyLock::new(|| {
    CANDIDATE_ACCOUNTS
        .iter()
        .map(|name| {
            let hash = Sha256::digest(format!("account:{name}").as_bytes());
            let disc: [u8; 8] = hash[..8].try_into().expect("sha256 digest is 32 bytes");
            (disc, *name)
        })
        .collect()
});

/// What an account's leading bytes tell us about its type.
///
/// Three variants rather than `Option<&str>`: "too short to be tagged" and
/// "tagged but unnamed" are different facts and must not share a `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AccountKind {
    /// Discriminator matched a name in [`CANDIDATE_ACCOUNTS`].
    Known(&'static str),
    /// Well-formed 8-byte tag, not one we can name. Carries the raw bytes.
    Unknown([u8; 8]),
    /// Fewer than 8 bytes of payload — cannot carry a discriminator.
    Untagged { len: usize },
}

// `From`, not `TryFrom`: classification is total. An unknown account type is
// data to record, not an error to propagate.
impl From<&[u8]> for AccountKind {
    fn from(data: &[u8]) -> Self {
        let Some(head) = data.get(..8) else {
            return AccountKind::Untagged { len: data.len() };
        };
        let disc: [u8; 8] = head.try_into().expect("slice of exactly 8 bytes");

        match DISCRIMINATORS.get(&disc) {
            Some(name) => AccountKind::Known(name),
            None => AccountKind::Unknown(disc),
        }
    }
}

impl fmt::Display for AccountKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Must route through `f.pad`, not `write!` — otherwise width/alignment
        // specs like `{kind:<22}` are silently ignored.
        let text = match self {
            AccountKind::Known(name) => (*name).to_owned(),
            AccountKind::Unknown(disc) => format!("unknown:{}", hex8(disc)),
            AccountKind::Untagged { len } => format!("untagged:{len}b"),
        };
        f.pad(&text)
    }
}

/// Lowercase hex for a discriminator.
pub fn hex8(bytes: &[u8; 8]) -> String {
    use fmt::Write as _;
    bytes.iter().fold(String::with_capacity(16), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Kamino Lend production mainnet program ID.
/// Staging equivalent: SLendK7ySfcEzyaFqy93gDnD3RtrpXJcnRwb6zFHJSh
pub const KLEND_PROGRAM: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";

/// Anchor discriminator for Obligation accounts — SHA256("account:Obligation")[..8].
/// Verified against [`DISCRIMINATORS`] and decode tests.
pub const OBLIGATION_DISC: [u8; 8] = [0xa8, 0xce, 0x8d, 0x6a, 0x58, 0x4c, 0xac, 0xa7];

/// Anchor discriminator for Reserve accounts — SHA256("account:Reserve")[..8].
/// Verified against [`DISCRIMINATORS`].
pub const RESERVE_DISC: [u8; 8] = [0x2b, 0xf2, 0xcc, 0xca, 0x1a, 0xf7, 0x3b, 0x7f];

/// Anchor discriminator for LendingMarket accounts — SHA256("account:LendingMarket")[..8].
/// Verified against [`DISCRIMINATORS`] and klend-interface tests.
pub const LENDING_MARKET_DISC: [u8; 8] = [0xf6, 0x72, 0x32, 0x62, 0x48, 0x9d, 0x1c, 0x78];

/// Checkpoint stream label. One row per stream in `ingest_checkpoint`; a second
/// subscription (staging, backfill) would use a different label.
pub const STREAM: &str = "klend";

/// Explicit column lists, deliberately: without them the server's INSERT header
/// includes every insertable column — for `account_updates` that means the
/// `ingested_at` DEFAULT column, which the row below does not supply, so the
/// block would be short a column. Naming columns pins the header to exactly what
/// we send. MATERIALIZED (`data_len`) and ALIAS (`pubkey_b58`) are never
/// insertable and are excluded regardless.
pub const INSERT_UPDATES_SQL: &str = "INSERT INTO account_updates \
    (slot, write_version, pubkey, kind, owner, lamports, data) FORMAT Native";
pub const INSERT_CHECKPOINT_SQL: &str = "INSERT INTO ingest_checkpoint \
    (stream, last_slot, last_write_version) FORMAT Native";

/// One raw account update, shaped for `klend.account_updates`. Field names are
/// the column names: klickhouse matches a row to the server's block header by
/// name, so field order here is free — kept in schema order for readers.
#[derive(Row, Debug)]
pub struct AccountUpdateRow {
    pub slot: u64,
    pub write_version: u64,
    /// FixedString(32) — raw 32 bytes, not base58. `Bytes` maps to CH String/
    /// FixedString; the readable form is a query-time ALIAS in the schema.
    pub pubkey: Bytes,
    /// LowCardinality(String) — a plain `String` serializes into it.
    pub kind: String,
    pub owner: Bytes,
    pub lamports: u64,
    /// Undecoded payload → CH String. Decoding is deliberately not done here.
    pub data: Bytes,
}

/// A checkpoint advance. `updated_at` is a schema DEFAULT, so it is not sent.
#[derive(Row, Debug)]
pub struct CheckpointRow {
    pub stream: String,
    pub last_slot: u64,
    pub last_write_version: u64,
}

/// A detected stream gap: a span where the indexer KNOWS it missed data.
/// Written by the reconnect-lite path when a resume point is unreachable.
/// `detected_at` and `ingested_at` are schema DEFAULTs and are not sent.
#[derive(Row, Debug)]
pub struct GapRow {
    pub stream: String,
    pub start_slot: u64,
    pub end_slot: u64,
    pub reason: String,
}

/// Decoded Obligation snapshot — one row per Obligation account update.
#[derive(Row, Debug)]
pub struct ObligationSnapshotRow {
    pub slot: u64,
    pub write_version: u64,
    pub pubkey: Bytes,
    /// Obligation owner.
    pub owner: Bytes,
    /// Lending market this obligation belongs to.
    pub lending_market: Bytes,
    pub num_deposits: u8,
    pub num_borrows: u8,
    /// Health factor in bps (x10^6). u64::MAX = no debt / infinite health.
    /// A value of 1_050_000 means health = 1.05.
    pub health_factor_bps: u64,
    /// Flags: bit 0 = has_debt, bit 1 = elevation_group active, bit 2 = autodeleverage active.
    pub flags: u8,
    pub elevation_group: u8,
    pub referrer: Bytes,
}

/// Decoded Reserve snapshot — one row per Reserve account update.
/// Only liquidity fields are populated (first 1352 struct bytes after
/// the discriminator); full Reserve decode is not possible with the
/// 3344-byte gRPC data slice.
#[derive(Row, Debug)]
pub struct ReserveSnapshotRow {
    pub slot: u64,
    pub write_version: u64,
    pub pubkey: Bytes,
    /// Lending market this reserve belongs to.
    pub lending_market: Bytes,
    /// Mint of the underlying liquidity token.
    pub liquidity_mint: Bytes,
    /// Vault where deposited liquidity is held.
    pub supply_vault: Bytes,
    /// Vault where fees are collected.
    pub fee_vault: Bytes,
    /// Available (unborrowed) liquidity in native token units.
    pub available_amount: u64,
    /// Total borrowed amount (scaled fraction).
    pub borrowed_amount_sf: u128,
    /// Market price in quote currency (scaled fraction).
    pub market_price_sf: u128,
    /// Decimals of the liquidity mint.
    pub mint_decimals: u64,
    /// Accumulated protocol fees (scaled fraction).
    pub acc_protocol_fees_sf: u128,
    /// Accumulated referrer fees (scaled fraction).
    pub acc_referrer_fees_sf: u128,
}

pub const INSERT_SNAPSHOT_SQL: &str = "INSERT INTO obligation_snapshots \
    (slot, write_version, pubkey, owner, lending_market, \
     num_deposits, num_borrows, health_factor_bps, flags, elevation_group, referrer) \
     FORMAT Native";

pub const INSERT_RESERVE_SNAPSHOT_SQL: &str = "INSERT INTO reserve_snapshots \
    (slot, write_version, pubkey, lending_market, liquidity_mint, supply_vault, \
     fee_vault, available_amount, borrowed_amount_sf, market_price_sf, \
     mint_decimals, acc_protocol_fees_sf, acc_referrer_fees_sf) \
     FORMAT Native";

/// Decoded LendingMarket snapshot — one row per LendingMarket account update.
#[derive(Row, Debug)]
pub struct LendingMarketSnapshotRow {
    pub slot: u64,
    pub write_version: u64,
    pub pubkey: Bytes,
    /// Lending market owner authority.
    pub owner: Bytes,
    /// Quote currency mint (the base token for this market).
    pub quote_currency: Bytes,
    /// Flags: bit 0 = emergency_mode, bit 1 = autodeleverage, bit 2 = borrow_disabled, bit 3 = immutable.
    pub flags: u8,
    /// Referral fee in basis points (e.g. 50 = 0.5%).
    pub referral_fee_bps: u16,
    /// Max percentage of debt that can be closed in a single liquidation.
    pub liquidation_max_debt_close_factor_pct: u8,
    /// UTF-8 market name (padded with null bytes to 32).
    pub name: Bytes,
}

pub const INSERT_LM_SNAPSHOT_SQL: &str = "INSERT INTO lending_market_snapshots \
    (slot, write_version, pubkey, owner, quote_currency, \
     flags, referral_fee_bps, liquidation_max_debt_close_factor_pct, name) \
     FORMAT Native";

pub const INSERT_GAP_SQL: &str = "INSERT INTO slot_gaps \
    (stream, start_slot, end_slot, reason) FORMAT Native";
