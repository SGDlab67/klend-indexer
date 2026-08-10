//! Export `account_updates` from ClickHouse to partitioned Parquet.
//!
//! The write half of the cold analytical path. ClickHouse stays the source of
//! truth and the only thing the indexer writes to; this produces a derived copy
//! laid out for slot-range analytics, which is the shape the hot store's primary
//! key deliberately does not have. See `src/coldpath.rs` for why the second
//! store exists and how partitions are named.
//!
//! Direction is one-way on purpose. The indexer's hot loop is untouched: the
//! 2026-08-05 wedge happened in that loop, and adding a second sink to it would
//! put a new stall in the one process whose downtime is unrecoverable. An
//! exporter that dies is restarted and re-run, and costs nothing but a re-read.
//!
//! Run:
//!   CLICKHOUSE_URL=... CLICKHOUSE_PASSWORD=... cargo run --bin parquet_export
//!
//! Env:
//!   KLEND_PARQUET_DIR    output directory (default ./parquet)
//!   KLEND_PARQUET_FULL   set to rewrite partitions that already exist
//!   CLICKHOUSE_*         as in the indexer

use anyhow::{Context, Result};
use futures::StreamExt;
use klickhouse::{Bytes, Client, ClientOptions, Row};
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, Decimal128Builder, FixedSizeBinaryBuilder, StringBuilder,
    TimestampMillisecondBuilder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;

#[path = "../ch.rs"]
mod ch;
#[path = "../coldpath.rs"]
mod coldpath;

use ch::connect_clickhouse;
use coldpath::{
    account_updates_schema, obligation_snapshots_schema, partition_dir, MAX_SF_VALUE,
    SF_DECIMAL_PRECISION, SLOTS_PER_PARTITION,
};

/// Rows per Arrow batch, and therefore per Parquet row group.
///
/// Bounded by payload, not by row count. The VM that can run this has 1 GB of
/// RAM and is the only host on ClickHouse's IP access list, so the ceiling is
/// real rather than theoretical.
///
/// ⚠️ Revised 2026-08-09. This previously read "trimmed to 4664 bytes on the
/// wire (`accounts_data_slice`), so 8192 rows is ~38 MB". That slice was removed
/// (`f6c0aa4`) because it silently destroyed 44 hours of Obligation data, so
/// payloads are now full size: Reserve is 8624 bytes, putting a batch at ~70 MB.
/// Still comfortable on a 1 GB box, but the headroom halved and the comment
/// would have kept claiming otherwise.
const BATCH_ROWS: usize = 8192;

/// One exported row. Mirrors `AccountUpdateRow` plus the two columns the stream
/// writer never sends because ClickHouse derives them: `data_len` (MATERIALIZED)
/// and `ingested_at` (DEFAULT).
#[derive(Row, Debug)]
struct ExportRow {
    slot: u64,
    write_version: u64,
    /// ⚠️ Hex, not raw bytes, and this is NOT cosmetic.
    ///
    /// klickhouse deserializes `FixedString(N)` by truncating at the first null
    /// byte (`types/deserialize/string.rs`: `position(|x| *x == 0)`, then
    /// `truncate`). That is reasonable for the padded ASCII the type usually
    /// holds and wrong for a Solana pubkey, which is 32 arbitrary bytes: about
    /// 12% of keys contain at least one `0x00` (`1 - (255/256)^32`), and every
    /// one of those would come back short.
    ///
    /// The indexer never hit this because it only ever WRITES pubkeys, and
    /// serialization pads back to N. This exporter is the first code in the
    /// project to read them out, so it is the first that could be corrupted by
    /// it. `hex()` is computed server-side and is lossless by construction; 32
    /// extra bytes per row against a 4664-byte payload is not worth measuring.
    pubkey_hex: String,
    kind: String,
    owner_hex: String,
    lamports: u64,
    /// Plain `String`, so it is length-prefixed on the wire and NOT subject to
    /// the truncation above. Raw payloads keep their interior nulls.
    data: Bytes,
    data_len: u32,
    /// Epoch millis, converted in SQL rather than mapped through a ClickHouse
    /// `DateTime64(3)` binding. One less type conversion to get subtly wrong,
    /// and it lands directly in Arrow's `TimestampMillisecond`.
    ingested_at_ms: i64,
}

/// Decode a 32-byte pubkey from ClickHouse's `hex()` output.
///
/// Hand-rolled rather than pulling a hex crate for eleven lines. Strict on
/// length and on digits: this is the only thing standing between a malformed
/// key and a Parquet file that looks fine and joins against nothing.
fn decode_pubkey_hex(hex: &str, what: &str, slot: u64) -> Result<[u8; 32]> {
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        anyhow::bail!(
            "{what} at slot {slot} is {} hex chars, expected 64",
            bytes.len()
        );
    }
    let mut out = [0u8; 32];
    for (i, pair) in bytes.chunks_exact(2).enumerate() {
        let hi = (pair[0] as char)
            .to_digit(16)
            .with_context(|| format!("{what} at slot {slot} has a non-hex digit"))?;
        let lo = (pair[1] as char)
            .to_digit(16)
            .with_context(|| format!("{what} at slot {slot} has a non-hex digit"))?;
        out[i] = (hi * 16 + lo) as u8;
    }
    Ok(out)
}

/// Accumulates rows into Arrow columns and drains them as a `RecordBatch`.
///
/// Column-at-a-time from the start. Building row structs and transposing later
/// would double the peak memory of the payload column, which is ~99% of the
/// bytes here.
struct BatchBuilder {
    slot: UInt64Builder,
    write_version: UInt64Builder,
    pubkey: FixedSizeBinaryBuilder,
    kind: StringBuilder,
    owner: FixedSizeBinaryBuilder,
    lamports: UInt64Builder,
    data: BinaryBuilder,
    data_len: UInt32Builder,
    ingested_at: TimestampMillisecondBuilder,
    rows: usize,
}

impl BatchBuilder {
    fn new() -> Self {
        Self {
            slot: UInt64Builder::new(),
            write_version: UInt64Builder::new(),
            pubkey: FixedSizeBinaryBuilder::new(32),
            kind: StringBuilder::new(),
            owner: FixedSizeBinaryBuilder::new(32),
            lamports: UInt64Builder::new(),
            data: BinaryBuilder::new(),
            data_len: UInt32Builder::new(),
            ingested_at: TimestampMillisecondBuilder::new(),
            rows: 0,
        }
    }

    fn push(&mut self, row: ExportRow) -> Result<()> {
        // `append_value` on a FixedSizeBinaryBuilder errors rather than panics on
        // a width mismatch, and that error is worth surfacing: a pubkey that is
        // not 32 bytes means the source table or the row mapping is wrong, and
        // silently padding it would corrupt every downstream join.
        // Decoded from hex, so width is guaranteed by the decoder rather than
        // hoped for. `append_value` still errors rather than panics on a
        // mismatch, and that error stays surfaced: a pubkey that is not 32 bytes
        // means the source or the mapping is wrong, and silently padding it
        // would corrupt every downstream join.
        let pubkey = decode_pubkey_hex(&row.pubkey_hex, "pubkey", row.slot)?;
        let owner = decode_pubkey_hex(&row.owner_hex, "owner", row.slot)?;
        self.pubkey
            .append_value(pubkey)
            .context("append pubkey")?;
        self.owner.append_value(owner).context("append owner")?;

        self.slot.append_value(row.slot);
        self.write_version.append_value(row.write_version);
        self.kind.append_value(&row.kind);
        self.lamports.append_value(row.lamports);
        self.data.append_value(&row.data.0);
        self.data_len.append_value(row.data_len);
        self.ingested_at.append_value(row.ingested_at_ms);
        self.rows += 1;
        Ok(())
    }

    /// Drain into a `RecordBatch`, leaving the builders empty and reusable.
    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.slot.finish()),
            Arc::new(self.write_version.finish()),
            Arc::new(self.pubkey.finish()),
            Arc::new(self.kind.finish()),
            Arc::new(self.owner.finish()),
            Arc::new(self.lamports.finish()),
            Arc::new(self.data.finish()),
            Arc::new(self.data_len.finish()),
            Arc::new(self.ingested_at.finish()),
        ];
        self.rows = 0;
        RecordBatch::try_new(account_updates_schema(), columns)
            .context("assemble RecordBatch (schema/column mismatch)")
    }
}

/// One exported `obligation_snapshots` row.
///
/// Pubkeys come through `hex()` for the same truncation reason as `ExportRow`.
/// The five U68F60 value columns come through `toString()` for a related reason:
/// they are `UInt128` in ClickHouse, and rather than trust this client's 128-bit
/// binding — in a library already caught truncating `FixedString` — the value is
/// carried as decimal text and parsed here. Text is unambiguous, and at 163K
/// rows the cost is irrelevant.
#[derive(Row, Debug)]
struct SnapshotRow {
    slot: u64,
    write_version: u64,
    pubkey_hex: String,
    owner_hex: String,
    lending_market_hex: String,
    num_deposits: u8,
    num_borrows: u8,
    health_factor_bps: u64,
    flags: u8,
    elevation_group: u8,
    referrer_hex: String,
    ingested_at_ms: i64,
    deposited_value_sf: String,
    borrowed_value_sf: String,
    borrow_factor_adjusted_debt_sf: String,
    allowed_borrow_value_sf: String,
    unhealthy_borrow_value_sf: String,
    lowest_deposit_liquidation_ltv: u64,
}

/// Parse a `UInt128` rendered as decimal text into the `i128` Arrow's
/// `Decimal128` stores.
///
/// Errors rather than saturating on overflow.
///
/// ⚠️ The bound is `MAX_SF_VALUE` (`10^38 - 1`), NOT `i128::MAX`. They differ by
/// 1.7×, and an earlier cut of this function guarded with `i128::try_from`,
/// which is wrong: Arrow stores the raw `i128` and formats it to the declared
/// precision, so a value between `10^38` and `i128::MAX` is accepted by every
/// layer and read back with its leading digits only. A fixture row of
/// `i128::MAX` round-tripped to 38 digits with the last one silently gone.
///
/// Caught by exporting a deliberately extreme fixture row and reading it back,
/// not by review. Nothing in the write path complains.
fn parse_sf(text: &str, what: &str, slot: u64) -> Result<i128> {
    let v: u128 = text
        .parse()
        .with_context(|| format!("{what} at slot {slot} is not a u128: {text:?}"))?;
    let v = i128::try_from(v).unwrap_or(i128::MAX);
    if v > MAX_SF_VALUE {
        anyhow::bail!(
            "{what} at slot {slot} is {text}, which exceeds Decimal128({},0)'s \
             maximum of {MAX_SF_VALUE}. Writing it would silently drop digits. \
             A value this large is a decode defect, not a real position.",
            SF_DECIMAL_PRECISION
        );
    }
    Ok(v)
}

struct SnapshotBatchBuilder {
    slot: UInt64Builder,
    write_version: UInt64Builder,
    pubkey: FixedSizeBinaryBuilder,
    owner: FixedSizeBinaryBuilder,
    lending_market: FixedSizeBinaryBuilder,
    num_deposits: UInt8Builder,
    num_borrows: UInt8Builder,
    health_factor_bps: UInt64Builder,
    flags: UInt8Builder,
    elevation_group: UInt8Builder,
    referrer: FixedSizeBinaryBuilder,
    ingested_at: TimestampMillisecondBuilder,
    deposited_value_sf: Decimal128Builder,
    borrowed_value_sf: Decimal128Builder,
    borrow_factor_adjusted_debt_sf: Decimal128Builder,
    allowed_borrow_value_sf: Decimal128Builder,
    unhealthy_borrow_value_sf: Decimal128Builder,
    lowest_deposit_liquidation_ltv: UInt64Builder,
    rows: usize,
}

impl SnapshotBatchBuilder {
    fn new() -> Self {
        // Precision must be restated on the builder as well as the schema, or
        // `RecordBatch::try_new` rejects the batch for a type mismatch that
        // reads as a schema bug rather than a builder default.
        let sf = || Decimal128Builder::new().with_precision_and_scale(SF_DECIMAL_PRECISION, 0);
        Self {
            slot: UInt64Builder::new(),
            write_version: UInt64Builder::new(),
            pubkey: FixedSizeBinaryBuilder::new(32),
            owner: FixedSizeBinaryBuilder::new(32),
            lending_market: FixedSizeBinaryBuilder::new(32),
            num_deposits: UInt8Builder::new(),
            num_borrows: UInt8Builder::new(),
            health_factor_bps: UInt64Builder::new(),
            flags: UInt8Builder::new(),
            elevation_group: UInt8Builder::new(),
            referrer: FixedSizeBinaryBuilder::new(32),
            ingested_at: TimestampMillisecondBuilder::new(),
            deposited_value_sf: sf().expect("precision 38 scale 0 is valid"),
            borrowed_value_sf: sf().expect("precision 38 scale 0 is valid"),
            borrow_factor_adjusted_debt_sf: sf().expect("precision 38 scale 0 is valid"),
            allowed_borrow_value_sf: sf().expect("precision 38 scale 0 is valid"),
            unhealthy_borrow_value_sf: sf().expect("precision 38 scale 0 is valid"),
            lowest_deposit_liquidation_ltv: UInt64Builder::new(),
            rows: 0,
        }
    }

    fn push(&mut self, row: SnapshotRow) -> Result<()> {
        let slot = row.slot;
        for (builder, hex, what) in [
            (&mut self.pubkey, &row.pubkey_hex, "pubkey"),
            (&mut self.owner, &row.owner_hex, "owner"),
            (&mut self.lending_market, &row.lending_market_hex, "lending_market"),
            (&mut self.referrer, &row.referrer_hex, "referrer"),
        ] {
            let key = decode_pubkey_hex(hex, what, slot)?;
            builder.append_value(key).with_context(|| format!("append {what}"))?;
        }

        self.deposited_value_sf
            .append_value(parse_sf(&row.deposited_value_sf, "deposited_value_sf", slot)?);
        self.borrowed_value_sf
            .append_value(parse_sf(&row.borrowed_value_sf, "borrowed_value_sf", slot)?);
        self.borrow_factor_adjusted_debt_sf.append_value(parse_sf(
            &row.borrow_factor_adjusted_debt_sf,
            "borrow_factor_adjusted_debt_sf",
            slot,
        )?);
        self.allowed_borrow_value_sf.append_value(parse_sf(
            &row.allowed_borrow_value_sf,
            "allowed_borrow_value_sf",
            slot,
        )?);
        self.unhealthy_borrow_value_sf.append_value(parse_sf(
            &row.unhealthy_borrow_value_sf,
            "unhealthy_borrow_value_sf",
            slot,
        )?);

        self.slot.append_value(slot);
        self.write_version.append_value(row.write_version);
        self.num_deposits.append_value(row.num_deposits);
        self.num_borrows.append_value(row.num_borrows);
        self.health_factor_bps.append_value(row.health_factor_bps);
        self.flags.append_value(row.flags);
        self.elevation_group.append_value(row.elevation_group);
        self.ingested_at.append_value(row.ingested_at_ms);
        self.lowest_deposit_liquidation_ltv
            .append_value(row.lowest_deposit_liquidation_ltv);
        self.rows += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.slot.finish()),
            Arc::new(self.write_version.finish()),
            Arc::new(self.pubkey.finish()),
            Arc::new(self.owner.finish()),
            Arc::new(self.lending_market.finish()),
            Arc::new(self.num_deposits.finish()),
            Arc::new(self.num_borrows.finish()),
            Arc::new(self.health_factor_bps.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(self.elevation_group.finish()),
            Arc::new(self.referrer.finish()),
            Arc::new(self.ingested_at.finish()),
            Arc::new(self.deposited_value_sf.finish()),
            Arc::new(self.borrowed_value_sf.finish()),
            Arc::new(self.borrow_factor_adjusted_debt_sf.finish()),
            Arc::new(self.allowed_borrow_value_sf.finish()),
            Arc::new(self.unhealthy_borrow_value_sf.finish()),
            Arc::new(self.lowest_deposit_liquidation_ltv.finish()),
        ];
        self.rows = 0;
        RecordBatch::try_new(obligation_snapshots_schema(), columns)
            .context("assemble snapshot RecordBatch (schema/column mismatch)")
    }
}

/// Export one slot partition of `obligation_snapshots`.
///
/// Deliberately a near-twin of `export_partition` rather than a generic over
/// both. The two differ in schema, row type, and builder, which is everything a
/// generic would have to abstract; the shared part is the ~20 lines of
/// tmp-file-and-rename discipline. Unifying them would trade a readable
/// duplication for an unreadable trait, and the duplicated part is the part that
/// is already correct and stable.
async fn export_snapshot_partition(
    client: &Client,
    lo: u64,
    hi: u64,
    table_dir: &std::path::Path,
) -> Result<u64> {
    let sql = format!(
        "SELECT slot, write_version, hex(pubkey) AS pubkey_hex, hex(owner) AS owner_hex, \
                hex(lending_market) AS lending_market_hex, num_deposits, num_borrows, \
                health_factor_bps, flags, elevation_group, hex(referrer) AS referrer_hex, \
                toUnixTimestamp64Milli(ingested_at) AS ingested_at_ms, \
                toString(deposited_value_sf) AS deposited_value_sf, \
                toString(borrowed_value_sf) AS borrowed_value_sf, \
                toString(borrow_factor_adjusted_debt_sf) AS borrow_factor_adjusted_debt_sf, \
                toString(allowed_borrow_value_sf) AS allowed_borrow_value_sf, \
                toString(unhealthy_borrow_value_sf) AS unhealthy_borrow_value_sf, \
                lowest_deposit_liquidation_ltv \
         FROM obligation_snapshots FINAL \
         WHERE slot >= {lo} AND slot < {hi} \
         ORDER BY slot, write_version, pubkey"
    );

    let mut stream = client
        .query::<SnapshotRow, _>(sql)
        .await
        .with_context(|| format!("stream obligation_snapshots for slots {lo}..{hi}"))?;

    let mut builder = SnapshotBatchBuilder::new();
    let mut writer: Option<ArrowWriter<std::fs::File>> = None;
    let mut written: u64 = 0;

    let final_path = table_dir
        .join(partition_dir(lo))
        .join("obligation_snapshots.parquet");
    let tmp_path = final_path.with_extension("parquet.tmp");

    while let Some(row) = stream.next().await {
        let row = row.context("deserialize obligation_snapshots row")?;

        if writer.is_none() {
            if let Some(parent) = tmp_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            let file = std::fs::File::create(&tmp_path)
                .with_context(|| format!("create {}", tmp_path.display()))?;
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .build();
            writer = Some(
                ArrowWriter::try_new(file, obligation_snapshots_schema(), Some(props))
                    .context("open snapshot parquet writer")?,
            );
        }

        builder.push(row)?;
        if builder.rows >= BATCH_ROWS {
            let batch = builder.finish()?;
            written += batch.num_rows() as u64;
            writer
                .as_mut()
                .expect("writer exists once a row has arrived")
                .write(&batch)
                .context("write snapshot row group")?;
        }
    }

    if let Some(mut w) = writer {
        if builder.rows > 0 {
            let batch = builder.finish()?;
            written += batch.num_rows() as u64;
            w.write(&batch).context("write final snapshot row group")?;
        }
        w.close().context("close snapshot parquet writer (footer)")?;
        std::fs::rename(&tmp_path, &final_path)
            .with_context(|| format!("publish {}", final_path.display()))?;
    }

    Ok(written)
}

/// Export one slot partition to one Parquet file. Returns rows written.
async fn export_partition(
    client: &Client,
    lo: u64,
    hi: u64,
    table_dir: &std::path::Path,
) -> Result<u64> {
    // `FINAL` collapses the ReplacingMergeTree here, once, so that no query on
    // the cold side ever has to. That is the whole trade: pay the dedup at
    // export time in a batch job, not on every dashboard read forever.
    //
    // ORDER BY slot is what makes the copy worth having. The hot table is
    // ORDER BY (pubkey, slot, write_version), which `schema/001_init.sql`
    // already flags as poor for "all accounts around slot N". Sorting on the way
    // out means Parquet's row-group statistics carry tight min/max on slot, and
    // a slot-range query prunes whole row groups instead of scanning.
    let sql = format!(
        "SELECT slot, write_version, hex(pubkey) AS pubkey_hex, kind, \
                hex(owner) AS owner_hex, lamports, data, data_len, \
                toUnixTimestamp64Milli(ingested_at) AS ingested_at_ms \
         FROM account_updates FINAL \
         WHERE slot >= {lo} AND slot < {hi} \
         ORDER BY slot, write_version, pubkey"
    );

    let mut stream = client
        .query::<ExportRow, _>(sql)
        .await
        .with_context(|| format!("stream account_updates for slots {lo}..{hi}"))?;

    let mut builder = BatchBuilder::new();
    let mut writer: Option<ArrowWriter<std::fs::File>> = None;
    let mut written: u64 = 0;

    // Written to `.tmp` and renamed only on success. A reader that lists the
    // directory must never see a half-written partition, and rename is atomic
    // within a filesystem. The same discipline the dashboard export learned:
    // a partial artifact that looks complete is worse than a missing one.
    let final_path = table_dir.join(partition_dir(lo)).join("account_updates.parquet");
    let tmp_path = final_path.with_extension("parquet.tmp");

    while let Some(row) = stream.next().await {
        let row = row.context("deserialize account_updates row")?;

        // Deferred until the first row: a slot range with no data should leave
        // no file at all, not an empty one that a later run would skip as
        // "already exported".
        if writer.is_none() {
            if let Some(parent) = tmp_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            let file = std::fs::File::create(&tmp_path)
                .with_context(|| format!("create {}", tmp_path.display()))?;
            // ZSTD over SNAPPY: these files are written once by a batch job and
            // read repeatedly over a network, so decode speed matters less than
            // bytes on the wire. Level 3 is the knee of the curve; the payload
            // column is mostly zeroed account tails and compresses hard.
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .build();
            writer = Some(
                ArrowWriter::try_new(file, account_updates_schema(), Some(props))
                    .context("open parquet writer")?,
            );
        }

        builder.push(row)?;
        if builder.rows >= BATCH_ROWS {
            let batch = builder.finish()?;
            written += batch.num_rows() as u64;
            writer
                .as_mut()
                .expect("writer exists once a row has arrived")
                .write(&batch)
                .context("write parquet row group")?;
        }
    }

    if let Some(mut w) = writer {
        if builder.rows > 0 {
            let batch = builder.finish()?;
            written += batch.num_rows() as u64;
            w.write(&batch).context("write final parquet row group")?;
        }
        // `close` writes the footer. Without it the file has no metadata and is
        // unreadable — an error here must not be swallowed.
        w.close().context("close parquet writer (footer)")?;
        std::fs::rename(&tmp_path, &final_path)
            .with_context(|| format!("publish {}", final_path.display()))?;
    }

    Ok(written)
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let out_dir = std::path::PathBuf::from(
        std::env::var("KLEND_PARQUET_DIR").unwrap_or_else(|_| "./parquet".to_owned()),
    );
    let full = std::env::var("KLEND_PARQUET_FULL").is_ok();

    let url = std::env::var("CLICKHOUSE_URL").context("CLICKHOUSE_URL is required")?;
    let options = ClientOptions {
        username: std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "klend".to_owned()),
        password: std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
        default_database: std::env::var("CLICKHOUSE_DATABASE")
            .unwrap_or_else(|_| "klend".to_owned()),
        ..Default::default()
    };
    let secure = matches!(
        std::env::var("CLICKHOUSE_SECURE").as_deref(),
        Ok("1") | Ok("true")
    );
    let client = connect_clickhouse(&url, secure, options)
        .await
        .context("connect ClickHouse source")?;

    #[derive(Row, Debug)]
    struct Bounds {
        lo: u64,
        hi: u64,
        rows: u64,
    }

    // Both tables, not just the raw one. `obligation_snapshots` is what Artefact
    // 2 is actually built on — the decoded health history — and leaving it out
    // of the export meant the credit expiry on 2026-08-19 would have taken the
    // more valuable of the two tables with it.
    for (table, file) in [
        ("account_updates", "account_updates.parquet"),
        ("obligation_snapshots", "obligation_snapshots.parquet"),
    ] {
        // Each table gets its own root, `<out>/<table>/slot_bucket=…/<file>`.
        //
        // Not cosmetic. `bin/coldquery.rs` registers a whole directory tree as
        // ONE logical table, so two tables sharing partition directories would
        // put two different schemas under one registration — a read failure at
        // best, and at worst the kind of silently-wrong result this project
        // already paid for once. Separate roots make that unrepresentable
        // rather than merely avoided.
        let table_dir = out_dir.join(table);
        let bounds: Bounds = client
            .query_one(format!(
                "SELECT min(slot) AS lo, max(slot) AS hi, count() AS rows FROM {table}"
            ))
            .await
            .with_context(|| format!("read {table} slot bounds"))?;

        if bounds.rows == 0 {
            eprintln!("{table} is empty; nothing to export");
            continue;
        }

        eprintln!(
            "exporting {table} slots {}..{} ({} rows) to {}",
            bounds.lo,
            bounds.hi,
            bounds.rows,
            out_dir.display()
        );

        let first_part = (bounds.lo / SLOTS_PER_PARTITION) * SLOTS_PER_PARTITION;
        let last_part = (bounds.hi / SLOTS_PER_PARTITION) * SLOTS_PER_PARTITION;

        let mut total: u64 = 0;
        let mut part = first_part;
        while part <= last_part {
            let path = table_dir.join(partition_dir(part)).join(file);

            // Skip completed partitions. Only the partition holding `max(slot)`
            // can still be growing, because the stream only moves forward — so
            // it is the only one a re-run must redo. Anything earlier is closed
            // history and re-exporting it would burn the read for a
            // byte-identical result.
            if !full && part != last_part && path.exists() {
                eprintln!("  {} exists, skipping (closed partition)", partition_dir(part));
                part += SLOTS_PER_PARTITION;
                continue;
            }

            let hi = part + SLOTS_PER_PARTITION;
            let n = match table {
                "account_updates" => export_partition(&client, part, hi, &table_dir).await?,
                _ => export_snapshot_partition(&client, part, hi, &table_dir).await?,
            };
            if n > 0 {
                eprintln!("  {} — {n} rows", partition_dir(part));
            }
            total += n;
            part += SLOTS_PER_PARTITION;
        }

        eprintln!("exported {total} {table} rows");

        // The export is a derived copy, so a row-count mismatch against the
        // source is the one thing worth checking before anyone trusts it.
        // Reported, not asserted: `FINAL` legitimately collapses replay
        // duplicates, so fewer rows out than in is expected. MORE would be a
        // real defect, so that case says so in different words.
        if total > bounds.rows {
            eprintln!(
                "DEFECT: {table} source reported {} rows but {total} were exported. \
                 FINAL can only ever collapse rows, never create them.",
                bounds.rows
            );
        } else if total < bounds.rows {
            eprintln!(
                "note: {table} source reported {} rows, exported {total} (difference {} \
                 — expected when FINAL collapses reconnect duplicates)",
                bounds.rows,
                bounds.rows - total
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_values_parse() {
        // 1.0 in U68F60.
        assert_eq!(parse_sf("1152921504606846976", "v", 1).unwrap(), 1u128 as i128 * (1 << 60));
        assert_eq!(parse_sf("0", "v", 1).unwrap(), 0);
    }

    #[test]
    fn the_largest_representable_value_is_accepted() {
        assert_eq!(parse_sf(&MAX_SF_VALUE.to_string(), "v", 1).unwrap(), MAX_SF_VALUE);
    }

    /// The bug this test exists for: `i128::MAX` is 1.7× larger than
    /// `Decimal128(38,0)` can represent, so an `i128::try_from` guard lets it
    /// through and Arrow reads it back with the last digit gone. Found by
    /// exporting a fixture row and looking at it, not by review.
    #[test]
    fn i128_max_is_rejected_because_precision_is_the_real_bound() {
        let err = parse_sf(&i128::MAX.to_string(), "deposited_value_sf", 437_394_119)
            .expect_err("i128::MAX exceeds Decimal128(38,0) and must not be written");
        let msg = err.to_string();
        assert!(msg.contains("exceeds"), "{msg}");
        assert!(msg.contains("437394119"), "{msg}");
    }

    #[test]
    fn one_past_the_maximum_is_rejected() {
        // The exact boundary, so an off-by-one in the comparison cannot hide.
        let over = (MAX_SF_VALUE as u128) + 1;
        assert!(parse_sf(&over.to_string(), "v", 1).is_err());
    }

    #[test]
    fn u128_values_above_i128_max_are_rejected_not_wrapped() {
        // u128::MAX would wrap to -1 under a naive `as i128` cast, which would
        // then compare as BELOW the maximum and be written as a negative value.
        assert!(parse_sf(&u128::MAX.to_string(), "v", 1).is_err());
    }

    #[test]
    fn non_numeric_text_is_an_error_not_a_zero() {
        assert!(parse_sf("", "v", 1).is_err());
        assert!(parse_sf("-1", "v", 1).is_err());
        assert!(parse_sf("1.5", "v", 1).is_err());
    }
}
