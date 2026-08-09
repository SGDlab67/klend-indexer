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
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder, TimestampMillisecondBuilder,
    UInt32Builder, UInt64Builder,
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
use coldpath::{account_updates_schema, partition_dir, SLOTS_PER_PARTITION};

/// Rows per Arrow batch, and therefore per Parquet row group.
///
/// Bounded by payload, not by row count: klend accounts are trimmed to 4664
/// bytes on the wire (`accounts_data_slice`), so 8192 rows is ~38 MB of `data`
/// held at once. The VM that can run this has 1 GB of RAM and is the only host
/// on ClickHouse's IP access list, so the ceiling is real rather than
/// theoretical. Large enough that row-group metadata is noise, small enough that
/// the exporter's memory does not track the table's size.
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

/// Export one slot partition to one Parquet file. Returns rows written.
async fn export_partition(client: &Client, lo: u64, hi: u64, out_dir: &std::path::Path) -> Result<u64> {
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
    let final_path = out_dir.join(partition_dir(lo)).join("account_updates.parquet");
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
    let bounds: Bounds = client
        .query_one("SELECT min(slot) AS lo, max(slot) AS hi, count() AS rows FROM account_updates")
        .await
        .context("read account_updates slot bounds")?;

    if bounds.rows == 0 {
        eprintln!("account_updates is empty; nothing to export");
        return Ok(());
    }

    eprintln!(
        "exporting slots {}..{} ({} rows) to {}",
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
        let path = out_dir.join(partition_dir(part)).join("account_updates.parquet");

        // Skip completed partitions. Only the partition holding `max(slot)` can
        // still be growing, because the stream only moves forward — so it is the
        // only one a re-run must redo. Anything earlier is closed history and
        // re-exporting it would burn the read for a byte-identical result.
        if !full && part != last_part && path.exists() {
            eprintln!("  {} exists, skipping (closed partition)", partition_dir(part));
            part += SLOTS_PER_PARTITION;
            continue;
        }

        let n = export_partition(&client, part, part + SLOTS_PER_PARTITION, &out_dir).await?;
        if n > 0 {
            eprintln!("  {} — {n} rows", partition_dir(part));
        }
        total += n;
        part += SLOTS_PER_PARTITION;
    }

    eprintln!("exported {total} rows");

    // The export is a derived copy, so a row-count mismatch against the source
    // is the one thing worth checking before anyone trusts it. Reported, not
    // asserted: `FINAL` legitimately collapses replay duplicates, so fewer rows
    // out than in is expected, and only MORE would indicate a real defect.
    if total != bounds.rows {
        eprintln!(
            "note: source reported {} rows, exported {total} (difference {} \
             — expected when FINAL collapses reconnect duplicates)",
            bounds.rows,
            bounds.rows as i64 - total as i64
        );
    }

    Ok(())
}
