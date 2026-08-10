//! Query the cold Parquet path with DataFusion.
//!
//! The read half of the analytical path written by `bin/parquet_export.rs`.
//! Nothing here touches ClickHouse: the point is that these questions are
//! answerable without the hot store, over files that can sit on object storage,
//! with no `FINAL` and no server to keep alive.
//!
//! Run:
//!   cargo run --bin coldquery -- gaps
//!   cargo run --bin coldquery -- activity
//!   cargo run --bin coldquery -- hot-accounts
//!   cargo run --bin coldquery -- sql "SELECT ..."
//!
//! Env:
//!   KLEND_PARQUET_DIR  input directory (default ./parquet)

use anyhow::{bail, Context, Result};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::*;

// Only `RESUME_TOLERANCE_SLOTS` is used here; the classifier itself belongs to
// the indexer. Shared whole rather than trimmed, for the same reason
// `bin/snapshot.rs` takes all of `schema.rs`: a module that two binaries import
// must not be shaped by whichever one imports less of it.
#[path = "../resume.rs"]
#[allow(dead_code)]
mod resume;
#[path = "../coldpath.rs"]
#[allow(dead_code)]
mod coldpath;

/// The gap threshold is `resume::RESUME_TOLERANCE_SLOTS`, imported rather than
/// repeated. The live detector and this retrospective derivation must agree on
/// what counts as a hole, or the table and the query that audits it will
/// disagree about the same history — and the audit is only worth running if a
/// disagreement means a defect rather than two different definitions.
use resume::RESUME_TOLERANCE_SLOTS;

/// Register `base58(pubkey)` so raw 32-byte keys can be read by a human at the
/// query edge.
///
/// Storage keeps the 32 raw bytes (`schema/001_init.sql`: 32 against ~44 for
/// text, fixed width, and it dictionary-encodes). Rendering belongs here, at the
/// boundary where a person looks at the output, not in the stored bytes.
fn base58_udf() -> ScalarUDF {
    create_udf(
        "base58",
        vec![DataType::FixedSizeBinary(32)],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            // Scalars are widened to a one-row array rather than special-cased.
            // The alternative is two encoding paths that can disagree, which is
            // the same argument as sharing the query set between the dashboard
            // exporter and the local proxy.
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let input = arrays[0]
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("signature pins the argument to FixedSizeBinary(32)");

            let mut out = StringBuilder::new();
            for i in 0..input.len() {
                if input.is_null(i) {
                    out.append_null();
                } else {
                    out.append_value(bs58::encode(input.value(i)).into_string());
                }
            }
            Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
        }),
    )
}

/// Slot gaps derived from the data itself, rather than read from `slot_gaps`.
///
/// This is the query that had to be run by hand in `docs/backfill-phase2.md`,
/// walking `account_updates` in slot order looking for jumps. As a stored
/// procedure over the cold copy it becomes repeatable, which turns the gap
/// table from an assertion into something auditable: `slot_gaps` records what
/// the indexer NOTICED, and this records what the data SHOWS. The two agreeing
/// is the check worth having, and the 2026-08-05 incident is exactly the case
/// where they did not.
///
/// The window function is why this belongs on the cold path. `lag` over every
/// distinct slot in history is a full ordered scan; the Parquet copy is sorted
/// by slot and its row groups carry min/max, so the engine reads it in order and
/// prunes. The hot table is `ORDER BY (pubkey, slot, write_version)`, where the
/// same scan has to sort the world first.
const GAPS_SQL: &str = "
    WITH observed AS (
        SELECT DISTINCT slot FROM account_updates
    ),
    stepped AS (
        SELECT slot, lag(slot) OVER (ORDER BY slot) AS prev
        FROM observed
    )
    SELECT
        prev              AS start_slot,
        slot              AS end_slot,
        slot - prev - 1   AS slots_missed
    FROM stepped
    WHERE prev IS NOT NULL AND slot - prev - 1 > {threshold}
    ORDER BY slots_missed DESC
";

/// Ingest volume per partition: what is actually in the cold store.
///
/// Grouped on `slot_bucket`, the Hive partition column, so the grouping key
/// comes from the directory listing rather than from any file. A full-history
/// scan whose answer never opens `data` — which is ~99% of the bytes on disk —
/// because a column store reads only the columns named.
///
/// `data_len` is summed instead of measuring `data`, which is the reason
/// `coldpath.rs` carries that column at all: the size of every payload without
/// decompressing a single one.
const ACTIVITY_SQL: &str = "
    SELECT
        slot_bucket,
        count(*)                  AS updates,
        count(DISTINCT pubkey)    AS accounts,
        sum(data_len)             AS payload_bytes,
        min(slot)                 AS first_slot,
        max(slot)                 AS last_slot,
        max(slot) - min(slot)     AS slot_span
    FROM account_updates
    GROUP BY slot_bucket
    ORDER BY slot_bucket
";

/// The accounts rewritten most often. `base58` is applied once, to the 20 rows
/// that survive the aggregation, rather than to every row scanned.
const HOT_ACCOUNTS_SQL: &str = "
    SELECT
        base58(pubkey)                       AS pubkey,
        -- `max`, not `any_value` (absent in DataFusion 54). `kind` is derived
        -- from the account's Anchor discriminator, so it is constant per pubkey
        -- and any aggregate over the group returns the same label.
        max(kind)                            AS kind,
        count(*)                             AS updates,
        count(DISTINCT slot)                 AS slots_touched,
        max(slot)                            AS last_seen
    FROM account_updates
    GROUP BY pubkey
    ORDER BY updates DESC
    LIMIT 20
";

#[tokio::main]
async fn main() -> Result<()> {
    let dir = std::env::var("KLEND_PARQUET_DIR").unwrap_or_else(|_| "./parquet".to_owned());
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first() else {
        bail!("usage: coldquery <gaps|activity|hot-accounts|sql \"SELECT ...\">");
    };

    let ctx = SessionContext::new();
    ctx.register_udf(base58_udf());

    // One logical table per directory of per-partition files, laid out as
    // `<dir>/<table>/slot_bucket=<padded>/<table>.parquet`.
    //
    // The `<table>` level is load-bearing: registration takes a whole tree, so
    // two tables sharing partition directories would be registered as one table
    // with two schemas. Separate roots make that unrepresentable.
    //
    // Declaring the partition column is what makes the directory name a column
    // instead of a path: a predicate on `slot_bucket` is answered from the
    // listing, without opening a file. Within the files that survive that,
    // `parquet_pruning` uses each row group's min/max on `slot` — which are
    // tight only because the exporter wrote the rows in slot order.
    // Two levels of skipping, neither of which the hot store's
    // `ORDER BY (pubkey, slot, write_version)` can offer for a slot range.
    //
    // `obligation_snapshots` is registered best-effort: it is the newer export
    // and an older Parquet tree will not have it. Failing the whole tool because
    // a second table is absent would break `gaps`, which is the query that
    // audits the first one.
    for (table, required) in [("account_updates", true), ("obligation_snapshots", false)] {
        let path = format!("{}/{table}", dir.trim_end_matches('/'));
        let opts = ParquetReadOptions::default()
            .parquet_pruning(true)
            .table_partition_cols(vec![(
                coldpath::PARTITION_COL.to_string(),
                DataType::Utf8,
            )]);
        match ctx.register_parquet(table, &path, opts).await {
            Ok(()) => {}
            Err(e) if !required => {
                eprintln!("note: {table} not registered ({e}); queries using it will fail");
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("register parquet at {path} (run `cargo run --bin parquet_export` first)")
                })
            }
        }
    }

    let sql = match command.as_str() {
        "gaps" => GAPS_SQL.replace("{threshold}", &RESUME_TOLERANCE_SLOTS.to_string()),
        "activity" => ACTIVITY_SQL.to_owned(),
        "hot-accounts" => HOT_ACCOUNTS_SQL.to_owned(),
        "sql" => args
            .get(1)
            .context("sql takes a statement: coldquery sql \"SELECT ...\"")?
            .clone(),
        other => bail!("unknown command {other:?}; try gaps, activity, hot-accounts, or sql"),
    };

    if command == "gaps" {
        eprintln!(
            "deriving gaps from the data (threshold {RESUME_TOLERANCE_SLOTS} slots, \
             shared with the live detector)\n"
        );
    }

    ctx.sql(&sql)
        .await
        .context("plan query")?
        .show()
        .await
        .context("execute query")?;

    Ok(())
}
