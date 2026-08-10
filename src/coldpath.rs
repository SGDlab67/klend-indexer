//! The cold analytical path: Arrow schema and partition layout for
//! `account_updates` on object storage.
//!
//! Shared by the exporter (`bin/parquet_export.rs`) and the query tool
//! (`bin/coldquery.rs`) for the same reason `schema.rs` is shared by the stream
//! and the snapshot binaries: two consumers that disagree about a layout do not
//! fail to compile, they return wrong answers.
//!
//! # Why a second store exists at all
//!
//! Not because ClickHouse is slow. Two specific, measured limits:
//!
//! 1. **`FINAL` does not scale.** Every dashboard query collapses a
//!    ReplacingMergeTree at read time. That is instant at 500K rows and will not
//!    survive 351M. Dedup here is resolved once, at export, so the read path
//!    never pays for it.
//! 2. **The primary key is the wrong shape for slot-range analytics.**
//!    `account_updates` is `ORDER BY (pubkey, slot, write_version)`, which
//!    `schema/001_init.sql` already flags as serving "all accounts around slot
//!    N" poorly. The cold copy is partitioned and sorted by slot, which is the
//!    ordering the hot store deliberately does not have.
//!
//! Hot path stays authoritative. This is a derived copy, and every export is
//! reproducible from ClickHouse, so a bad export is thrown away rather than
//! recovered.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Slots per Parquet partition.
///
/// One tenth of the ClickHouse partition width (`intDiv(slot, 10000000)`,
/// ~27 days). Deliberately finer: ClickHouse partitions are tuned for merge
/// behaviour on a table that is written continuously, while these are tuned for
/// how much a query has to open in order to skip the rest. At ~500K rows per
/// 1M slots today, a partition is a few MB — large enough that per-file overhead
/// is irrelevant, small enough that a single-day question does not open a month.
///
/// Changing this value re-partitions on the next full export. That is safe
/// precisely because the hot store is authoritative and this copy is derived.
pub const SLOTS_PER_PARTITION: u64 = 1_000_000;

/// The Hive partition column name. A plain directory name would do for the
/// exporter, but `key=value` is what makes the engine treat the bucket as a
/// queryable COLUMN rather than an opaque path segment: a predicate on it prunes
/// whole directories without opening a single footer.
pub const PARTITION_COL: &str = "slot_bucket";

/// Directory name for the partition containing `slot`.
///
/// Hive-style `slot_bucket=<value>`, which is the layout DataFusion and every
/// other Parquet reader already know how to list and prune. A bare
/// `slot_<value>` directory does not list recursively at all — DataFusion looks
/// for files in the root and finds none.
///
/// Zero-padded so lexical order matches numeric order. Object stores list keys
/// lexically, and an unpadded `9000000` sorting before `10000000` turns "scan
/// the partitions in order" into a silent reordering. The padding is also why
/// the column reads as a string rather than an integer, which is the trade:
/// correct ordering for free, at the cost of casting when comparing to a slot.
pub fn partition_dir(slot: u64) -> String {
    format!(
        "{PARTITION_COL}={:012}",
        (slot / SLOTS_PER_PARTITION) * SLOTS_PER_PARTITION
    )
}

/// The Arrow schema for exported `account_updates`.
///
/// Field-by-field divergences from the ClickHouse table, each deliberate:
///
/// - `pubkey` and `owner` are `FixedSizeBinary(32)`, matching
///   `FixedString(32)`. Not base58 text: 32 bytes against ~44, fixed width, and
///   Parquet dictionary-encodes the repeated values. Rendering to base58 is a
///   presentation concern and stays at the query edge.
/// - `data` is `Binary`, not `Utf8`. ClickHouse's `String` is a byte string and
///   holds raw account payloads that are not valid UTF-8. Arrow's `Utf8` asserts
///   validity, so this must be `Binary` or the export panics on real data.
/// - `data_len` is materialised in ClickHouse and carried explicitly here, so a
///   query can filter on payload size without decompressing `data`. That is the
///   whole point of a column store, and it is free at export.
/// - `ingested_at` is `Timestamp(Millisecond)` from `DateTime64(3)`. It is our
///   wall clock, never a chain timestamp — see the warning in
///   `schema/001_init.sql`. Slot is the only honest ordering.
///
/// Nothing here is nullable. Every column is non-null in the source table, and
/// declaring that lets Parquet skip definition levels entirely.
pub fn account_updates_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("write_version", DataType::UInt64, false),
        Field::new("pubkey", DataType::FixedSizeBinary(32), false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("owner", DataType::FixedSizeBinary(32), false),
        Field::new("lamports", DataType::UInt64, false),
        Field::new("data", DataType::Binary, false),
        Field::new("data_len", DataType::UInt32, false),
        Field::new(
            "ingested_at",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

/// Precision for the U68F60 fixed-point value columns.
///
/// Arrow has no unsigned 128-bit integer, so the `*_sf` columns land in
/// `Decimal128(38, 0)`: scale zero, because these are raw integers and dividing
/// by `2^60` here would bake the scale assumption into the files exactly as
/// writing them divided into ClickHouse would have. Store what you were given;
/// derive at the query edge.
///
/// The ceiling is real but not binding. `Decimal128(38, 0)` holds up to
/// `10^38 - 1` (~9.99e37) while `u128` reaches ~3.4e38, so the top of the source
/// range is unrepresentable. At `2^60` scaling, `10^38 - 1` is a position of
/// ~8.7e19 USD, which is not a number this protocol can produce.
///
/// ⚠️ The bound is the **precision**, not `i128::MAX`. Those differ by 1.7×, and
/// the difference is not academic: a first cut guarded with `i128::try_from` and
/// a fixture row of `i128::MAX` round-tripped to a 38-digit number with the last
/// digit silently gone. Arrow stores the raw `i128` and formats to `precision`,
/// so an over-precision value is not rejected anywhere in the write path — it is
/// simply read back wrong. See `MAX_SF_VALUE`.
pub const SF_DECIMAL_PRECISION: u8 = 38;

/// Largest value representable in `Decimal128(SF_DECIMAL_PRECISION, 0)`.
///
/// `10^38 - 1`. Anything above this must be rejected at export rather than
/// written, because nothing downstream will notice.
pub const MAX_SF_VALUE: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// The Arrow schema for exported `obligation_snapshots`.
///
/// This is the table Artefact 2 is actually built on — `account_updates` is the
/// raw substrate, but the decoded snapshots are what answers a question about
/// obligation health. It exports separately because it is a separate table with
/// its own ReplacingMergeTree collapse, not a projection of the other.
///
/// Divergences from the ClickHouse table:
///
/// - The `*_b58` columns are **dropped**. They are ALIAS/derived renderings of
///   `pubkey` and `owner`, and carrying both a binary and a text spelling of the
///   same key doubles the column for no query that the base58 UDF in
///   `bin/coldquery.rs` cannot serve.
/// - The five `UInt128` value columns become `Decimal128(38, 0)`. See above.
/// - `lowest_deposit_liquidation_ltv` stays `UInt64`: it is basis-point-ish, not
///   fixed point, and does not share the `_sf` scaling.
pub fn obligation_snapshots_schema() -> Arc<Schema> {
    let sf = |name: &str| Field::new(name, DataType::Decimal128(SF_DECIMAL_PRECISION, 0), false);
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("write_version", DataType::UInt64, false),
        Field::new("pubkey", DataType::FixedSizeBinary(32), false),
        Field::new("owner", DataType::FixedSizeBinary(32), false),
        Field::new("lending_market", DataType::FixedSizeBinary(32), false),
        Field::new("num_deposits", DataType::UInt8, false),
        Field::new("num_borrows", DataType::UInt8, false),
        Field::new("health_factor_bps", DataType::UInt64, false),
        Field::new("flags", DataType::UInt8, false),
        Field::new("elevation_group", DataType::UInt8, false),
        Field::new("referrer", DataType::FixedSizeBinary(32), false),
        Field::new(
            "ingested_at",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        sf("deposited_value_sf"),
        sf("borrowed_value_sf"),
        sf("borrow_factor_adjusted_debt_sf"),
        sf("allowed_borrow_value_sf"),
        sf("unhealthy_borrow_value_sf"),
        Field::new("lowest_deposit_liquidation_ltv", DataType::UInt64, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_schema_drops_the_base58_duplicates() {
        let s = obligation_snapshots_schema();
        assert!(s.field_with_name("pubkey").is_ok());
        assert!(s.field_with_name("pubkey_b58").is_err());
        assert!(s.field_with_name("owner_b58").is_err());
    }

    #[test]
    fn value_columns_keep_their_raw_fixed_point_scale() {
        // Scale 0. A non-zero scale here would mean the 2^60 divide happened at
        // export, which is the mistake this project already decided not to make
        // at the write path.
        let s = obligation_snapshots_schema();
        for name in [
            "deposited_value_sf",
            "borrowed_value_sf",
            "borrow_factor_adjusted_debt_sf",
            "allowed_borrow_value_sf",
            "unhealthy_borrow_value_sf",
        ] {
            match s.field_with_name(name).expect("column present").data_type() {
                DataType::Decimal128(p, 0) => assert_eq!(*p, SF_DECIMAL_PRECISION),
                other => panic!("{name} must be Decimal128(_, 0), got {other:?}"),
            }
        }
    }

    #[test]
    fn every_snapshot_column_is_non_null() {
        // Non-nullable lets Parquet skip definition levels. Every source column
        // is non-null, so this must stay true as columns are added.
        for f in obligation_snapshots_schema().fields() {
            assert!(!f.is_nullable(), "{} became nullable", f.name());
        }
    }

    #[test]
    fn partition_dirs_sort_lexically_in_slot_order() {
        // The reason for the zero padding. Without it "slot_9000000" sorts
        // before "slot_10000000" and an object-store listing walks the
        // partitions out of order while looking perfectly sorted.
        let mut dirs: Vec<String> = [9_500_000u64, 437_000_000, 1_500_000, 500_000]
            .iter()
            .map(|s| partition_dir(*s))
            .collect();
        let ordered = dirs.clone();
        dirs.sort();
        let mut by_slot = ordered;
        by_slot.sort_by_key(|d| {
            d.split_once('=')
                .expect("hive layout")
                .1
                .trim_start_matches('0')
                .parse::<u64>()
                .unwrap_or(0)
        });
        assert_eq!(dirs, by_slot);
    }

    #[test]
    fn partitions_are_hive_style() {
        // Not cosmetic: a bare `slot_<n>` directory is not listed recursively,
        // so the table registers with zero files and the failure reads as
        // "cannot infer schema from an empty location".
        let dir = partition_dir(437_313_969);
        let (col, value) = dir.split_once('=').expect("key=value layout");
        assert_eq!(col, PARTITION_COL);
        assert_eq!(value, "000437000000");
    }

    #[test]
    fn partition_boundaries_are_half_open() {
        // A slot exactly on a boundary belongs to the partition it opens, not
        // the one it closes. Off by one here spreads a partition's rows across
        // two files and makes range pruning quietly incomplete.
        assert_eq!(partition_dir(0), "slot_bucket=000000000000");
        assert_eq!(
            partition_dir(SLOTS_PER_PARTITION - 1),
            "slot_bucket=000000000000"
        );
        assert_eq!(
            partition_dir(SLOTS_PER_PARTITION),
            "slot_bucket=000001000000"
        );
    }

    #[test]
    fn the_recorded_gap_bounds_land_where_expected() {
        // The 2026-08-05 gap, 437,313,969 → 437,387,843, sits inside one
        // partition. A gap query therefore opens one file, which is the
        // property the layout exists for.
        assert_eq!(partition_dir(437_313_969), partition_dir(437_387_843));
        assert_eq!(partition_dir(437_313_969), "slot_bucket=000437000000");
    }

    #[test]
    fn payload_column_is_binary_not_utf8() {
        // Real klend payloads are not valid UTF-8. Arrow's Utf8 asserts
        // validity, so this being wrong is a panic on the first export, not a
        // subtle mis-read.
        let schema = account_updates_schema();
        let data = schema.field_with_name("data").unwrap();
        assert_eq!(data.data_type(), &DataType::Binary);
        assert!(!data.is_nullable());
    }

    #[test]
    fn pubkeys_keep_their_fixed_width() {
        let schema = account_updates_schema();
        for name in ["pubkey", "owner"] {
            assert_eq!(
                schema.field_with_name(name).unwrap().data_type(),
                &DataType::FixedSizeBinary(32),
                "{name} must stay 32 raw bytes, not base58 text"
            );
        }
    }
}
