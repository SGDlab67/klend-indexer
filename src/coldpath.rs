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

#[cfg(test)]
mod tests {
    use super::*;

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
