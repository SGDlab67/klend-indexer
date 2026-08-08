//! klend snapshot writer — Phase 2, Option A.
//!
//! Pipeline: Solana JSON-RPC `getProgramAccounts` → [HERE] → decode → ClickHouse
//!
//! One-shot, not a daemon. Enumerates every Kamino Lend account that currently
//! exists, decodes it with the same `decode` module the stream indexer uses, and
//! writes it into the same tables.
//!
//! Why this exists: the Yellowstone stream delivers account *updates*, so the
//! dataset contains exactly the accounts that happened to change while the
//! indexer was connected. Everything idle is absent, and absent in the worst way
//! — nothing in the data indicates it should be there. A snapshot turns
//! "accounts we saw" into "accounts that exist".
//!
//! What this is NOT: a backfill. `getProgramAccounts` returns current state at
//! the current slot; no provider offers an at-slot variant, so the 2026-08-05
//! gap stays unfilled and stays recorded in `slot_gaps`. See
//! docs/backfill-phase2.md for why the archival-RPC route cannot recover bytes.

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use klickhouse::{Bytes, Client, ClientOptions, Row};

#[path = "../ch.rs"]
mod ch;
#[path = "../decode.rs"]
mod decode;
// The shared module carries the whole klend schema, including the checkpoint and
// gap rows that only the stream indexer writes. Unused here by design, not by
// oversight, so the dead-code warnings are silenced at the include rather than
// by trimming a module the other binary depends on.
#[allow(dead_code)]
#[path = "../schema.rs"]
mod schema;

use ch::connect_clickhouse;
use schema::*;

/// Provenance row for `klend.snapshot_runs`. `ingested_at` is a schema DEFAULT.
#[derive(Row, Debug)]
struct SnapshotRunRow {
    slot: u64,
    scope: String,
    accounts: u64,
    rows_written: u64,
    decode_failures: u64,
    duration_ms: u64,
}

const INSERT_RUN_SQL: &str = "INSERT INTO snapshot_runs \
    (slot, scope, accounts, rows_written, decode_failures, duration_ms) FORMAT Native";

/// Marks every row this binary writes. Geyser's write_version is a monotonic
/// per-node counter and is never 0 for a real update, so 0 is a free sentinel
/// that separates "snapshotted from RPC" from "seen on the wire" without a
/// schema change. It also sorts first within the ReplacingMergeTree key rather
/// than colliding with a genuine write at the same slot.
const SNAPSHOT_WRITE_VERSION: u64 = 0;

/// `getProgramAccounts` returns the whole account set in one response, so the
/// read is a single large body rather than many small ones. Generous, because
/// the failure mode we care about is a truncated snapshot, not a slow one.
const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Rows per insert. Matches the indexer's batch size so both writers put
/// similarly-shaped parts into ClickHouse.
const INSERT_BATCH: usize = 4096;

/// One account as returned by `getProgramAccounts`.
struct RpcAccount {
    pubkey: Vec<u8>,
    owner: Vec<u8>,
    lamports: u64,
    data: Vec<u8>,
}

/// Base64-encoded 8-byte Anchor discriminator, for a `memcmp` filter at offset 0.
fn disc_b64(disc: &[u8; 8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(disc)
}

/// Byte offset of `Obligation.owner` within the account payload, discriminator
/// included: 8 (discriminator) + 8 (tag) + 16 (last_update) + 32 (lending_market).
/// Used only as a paging key — see [`OBLIGATION_PAGES`].
const OBLIGATION_OWNER_OFFSET: usize = 64;

/// `getProgramAccounts` has no pagination, and the Obligation set is ~140k
/// accounts at 3.3 KB each: one response is ~630 MB of base64, and the parsed
/// JSON several times that. The VM is an e2-micro with 1 GB of RAM, and it is the
/// only machine on the ClickHouse IP access list, so the snapshot has to run
/// there. It cannot buffer the whole set.
///
/// So page it: add a second memcmp matching one byte of the owner pubkey. Owner
/// bytes are uniformly distributed, so 256 filters partition the set into ~550
/// accounts each, every page is a few MB, and the pages are disjoint and
/// exhaustive by construction. Smaller account types skip this and fetch whole.
const OBLIGATION_PAGES: u16 = 256;

/// Fetch klend accounts matching an optional discriminator filter and an optional
/// single-byte page filter. Returns the RPC context slot alongside the accounts:
/// that slot is the only honest timestamp for the bytes, so it is never
/// substituted with a local clock.
async fn get_program_accounts(
    http: &reqwest::Client,
    rpc_url: &str,
    disc: Option<&[u8; 8]>,
    page: Option<(usize, u8)>,
) -> Result<(u64, Vec<RpcAccount>)> {
    let mut config = serde_json::json!({
        "encoding": "base64",
        // `confirmed`, matching the stream's commitment. `finalized` would be a
        // different consistency point than the rest of the dataset.
        "commitment": "confirmed",
        // Without this the result is a bare array and the slot is unrecoverable,
        // which would leave every snapshot row undatable.
        "withContext": true,
    });
    let mut filters = Vec::new();
    if let Some(d) = disc {
        filters.push(serde_json::json!({
            "memcmp": { "offset": 0, "bytes": disc_b64(d), "encoding": "base64" }
        }));
    }
    if let Some((offset, byte)) = page {
        filters.push(serde_json::json!({
            "memcmp": {
                "offset": offset,
                "bytes": base64::engine::general_purpose::STANDARD.encode([byte]),
                "encoding": "base64",
            }
        }));
    }
    if !filters.is_empty() {
        config["filters"] = serde_json::Value::Array(filters);
    }

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [KLEND_PROGRAM, config],
    });

    let resp = http
        .post(rpc_url)
        .timeout(RPC_TIMEOUT)
        .json(&body)
        .send()
        .await
        .context("getProgramAccounts request failed")?;

    let status = resp.status();
    let text = resp.text().await.context("read RPC response body")?;
    if !status.is_success() {
        // Truncated: the URL carries the API key, and a 4xx body can echo it back.
        bail!("RPC returned HTTP {status}: {}", &text[..text.len().min(200)]);
    }

    let v: serde_json::Value = serde_json::from_str(&text).context("parse RPC response as JSON")?;
    if let Some(e) = v.get("error") {
        bail!("RPC error: {e}");
    }

    let slot = v["result"]["context"]["slot"]
        .as_u64()
        .context("response has no result.context.slot (withContext ignored?)")?;
    let values = v["result"]["value"]
        .as_array()
        .context("response has no result.value array")?;

    let mut out = Vec::with_capacity(values.len());
    for item in values {
        let pubkey = bs58::decode(item["pubkey"].as_str().context("account has no pubkey")?)
            .into_vec()
            .context("pubkey is not valid base58")?;
        let account = &item["account"];
        let owner = bs58::decode(account["owner"].as_str().context("account has no owner")?)
            .into_vec()
            .context("owner is not valid base58")?;
        let lamports = account["lamports"].as_u64().unwrap_or(0);
        // `data` is `[<payload>, <encoding>]` under base64 encoding.
        let data_b64 = account["data"][0]
            .as_str()
            .context("account data is not [base64, encoding]")?;
        let data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .context("account data is not valid base64")?;

        out.push(RpcAccount {
            pubkey,
            owner,
            lamports,
            data,
        });
    }

    Ok((slot, out))
}

/// Insert a vector in `INSERT_BATCH`-sized chunks. `insert_native_block` sends
/// one block per call, and a single 100k-row block is a needlessly large unit of
/// work to retry when something goes wrong mid-write.
async fn insert_chunked<T: Row + Send + Sync + 'static>(
    client: &Client,
    sql: &str,
    rows: Vec<T>,
    label: &str,
) -> Result<usize> {
    let total = rows.len();
    if total == 0 {
        return Ok(0);
    }
    let mut chunks: Vec<Vec<T>> = Vec::new();
    let mut rows = rows;
    while rows.len() > INSERT_BATCH {
        chunks.push(rows.split_off(rows.len() - INSERT_BATCH));
    }
    chunks.push(rows);

    for chunk in chunks {
        let n = chunk.len();
        client
            .insert_native_block(sql, chunk)
            .await
            .with_context(|| format!("insert {n} rows into {label}"))?;
    }
    Ok(total)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Same process-default provider the indexer installs. klickhouse's TLS path
    // needs one chosen explicitly; without it, connect_tls panics at runtime.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let started = std::time::Instant::now();

    // Default to the same Alchemy credentials the stream already uses, assembled
    // into the JSON-RPC form. Reusing GRPC_TOKEN means the deploy needs no second
    // secret, no second Secret Manager entry, and no second thing to rotate.
    let rpc_url = match std::env::var("RPC_URL") {
        Ok(u) => u,
        Err(_) => {
            let base = std::env::var("GRPC_URL")
                .context("set RPC_URL, or GRPC_URL + GRPC_TOKEN to reuse the stream credentials")?;
            let token = std::env::var("GRPC_TOKEN")
                .context("set RPC_URL, or GRPC_URL + GRPC_TOKEN to reuse the stream credentials")?;
            format!("{}/v2/{}", base.trim_end_matches('/'), token)
        }
    };

    // 'known' (default): the three discriminators the pipeline decodes, one
    // filtered call each. 'all': every account the program owns, including kinds
    // with no decoder yet. 'known' is the default because it is bounded and
    // matches what the stream subscribes to; 'all' is the wider sweep, and on an
    // uncapped key a wider sweep should be something you ask for.
    let scope = std::env::var("KLEND_SNAPSHOT_SCOPE").unwrap_or_else(|_| "known".to_owned());
    // Each entry is (discriminator, paging offset). Only Obligations need paging;
    // Reserves and LendingMarkets are in the hundreds and fetch whole. 'all' is
    // deliberately unpaged: there is no single offset that partitions every
    // account type, so the wider sweep is only safe on a machine with headroom.
    let filters: Vec<(Option<[u8; 8]>, Option<usize>)> = match scope.as_str() {
        "known" => vec![
            (Some(OBLIGATION_DISC), Some(OBLIGATION_OWNER_OFFSET)),
            (Some(RESERVE_DISC), None),
            (Some(LENDING_MARKET_DISC), None),
        ],
        "all" => vec![(None, None)],
        other => bail!("KLEND_SNAPSHOT_SCOPE must be 'known' or 'all', got {other:?}"),
    };

    let dry_run = std::env::var("KLEND_SNAPSHOT_DRY_RUN").is_ok();

    let http = reqwest::Client::builder()
        .build()
        .context("build HTTP client")?;

    // ── ClickHouse sink, opened BEFORE the first billed RPC call ──
    // Same ordering rule the indexer follows: prove the sink works before opening
    // a metered upstream, so a bad password costs nothing but a connection.
    let client = if dry_run {
        None
    } else {
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
        Some(
            connect_clickhouse(&url, secure, options)
                .await
                .context("connect ClickHouse sink")?,
        )
    };

    // ── Fetch, decode, and write one page at a time ──
    // Nothing accumulates across pages except counters. Holding even one account
    // type in full would defeat the paging.
    let mut totals = Totals::default();

    for (disc, page_offset) in &filters {
        let label = disc.map_or_else(|| "all".to_owned(), |d| hex8(&d));
        match page_offset {
            None => {
                let page = fetch_and_write(&http, &rpc_url, disc.as_ref(), None, client.as_ref())
                    .await
                    .with_context(|| format!("snapshot filter={label}"))?;
                eprintln!("filter={label} slot={} accounts={}", page.slot, page.accounts);
                totals.absorb(page);
            }
            Some(offset) => {
                for byte in 0..OBLIGATION_PAGES {
                    let byte = byte as u8;
                    let page = fetch_and_write(
                        &http,
                        &rpc_url,
                        disc.as_ref(),
                        Some((*offset, byte)),
                        client.as_ref(),
                    )
                    .await
                    .with_context(|| format!("snapshot filter={label} page={byte}"))?;
                    totals.absorb(page);
                    // One line per 32 pages: enough to watch progress on a long run
                    // without burying the decode failures that matter.
                    if byte % 32 == 31 {
                        eprintln!(
                            "filter={label} page={}/{OBLIGATION_PAGES} accounts_so_far={}",
                            u16::from(byte) + 1,
                            totals.accounts
                        );
                    }
                }
            }
        }
    }

    if totals.accounts == 0 {
        bail!("getProgramAccounts returned no accounts — refusing to record an empty snapshot");
    }

    eprintln!(
        "snapshot slot={} scope={scope} accounts={} kinds={:?} decode_failures={}",
        totals.max_slot, totals.accounts, totals.by_kind, totals.decode_failures
    );

    let Some(client) = client else {
        eprintln!("KLEND_SNAPSHOT_DRY_RUN set — nothing written");
        return Ok(());
    };

    // Provenance last, mirroring the indexer's data-then-checkpoint ordering: a
    // recorded run with no data behind it is the one inconsistency that would
    // actively mislead, so it is written only once the data is committed.
    //
    // ingest_checkpoint is deliberately NOT advanced. It is the stream's resume
    // point; moving it to an RPC context slot would make the indexer resume from
    // a slot it never consumed, converting a snapshot into a stream gap.
    client
        .insert_native_block(
            INSERT_RUN_SQL,
            vec![SnapshotRunRow {
                slot: totals.max_slot,
                scope: scope.clone(),
                accounts: totals.accounts,
                rows_written: totals.rows_written,
                decode_failures: totals.decode_failures,
                duration_ms: started.elapsed().as_millis() as u64,
            }],
        )
        .await
        .context("record snapshot_runs")?;

    eprintln!(
        "snapshot complete: slot={} rows={} in {:.1}s",
        totals.max_slot,
        totals.rows_written,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Running counts across pages. The only thing that survives a page boundary.
#[derive(Default)]
struct Totals {
    accounts: u64,
    rows_written: u64,
    decode_failures: u64,
    max_slot: u64,
    by_kind: std::collections::BTreeMap<String, u64>,
}

impl Totals {
    fn absorb(&mut self, page: PageResult) {
        self.accounts += page.accounts;
        self.rows_written += page.rows_written;
        self.decode_failures += page.decode_failures;
        // Pages are separate RPC round trips landing on different slots. Each row
        // keeps its own page's slot; the run is dated at the newest, so the run
        // never claims a slot the snapshot had not reached.
        self.max_slot = self.max_slot.max(page.slot);
        for (kind, n) in page.by_kind {
            *self.by_kind.entry(kind).or_default() += n;
        }
    }
}

/// What one page contributed.
struct PageResult {
    slot: u64,
    accounts: u64,
    rows_written: u64,
    decode_failures: u64,
    by_kind: std::collections::BTreeMap<String, u64>,
}

/// Fetch one page, decode it, and write it. `client` is `None` on a dry run, in
/// which case everything runs except the inserts — so a dry run still exercises
/// the RPC shape, the paging, and every decoder.
async fn fetch_and_write(
    http: &reqwest::Client,
    rpc_url: &str,
    disc: Option<&[u8; 8]>,
    page: Option<(usize, u8)>,
    client: Option<&Client>,
) -> Result<PageResult> {
    let (slot, accounts) = get_program_accounts(http, rpc_url, disc, page).await?;

    let mut raw_rows: Vec<AccountUpdateRow> = Vec::with_capacity(accounts.len());
    let mut obligations: Vec<ObligationSnapshotRow> = Vec::new();
    let mut reserves: Vec<ReserveSnapshotRow> = Vec::new();
    let mut markets: Vec<LendingMarketSnapshotRow> = Vec::new();
    let mut decode_failures = 0u64;
    let mut by_kind: std::collections::BTreeMap<String, u64> = Default::default();

    for acct in accounts {
        let kind = AccountKind::from(acct.data.as_slice());
        *by_kind.entry(kind.to_string()).or_default() += 1;

        match kind {
            AccountKind::Known("Obligation") => match decode::decode_obligation(&acct.data) {
                Some(Ok(d)) => obligations.push(ObligationSnapshotRow {
                    slot,
                    write_version: SNAPSHOT_WRITE_VERSION,
                    pubkey: Bytes(acct.pubkey.clone()),
                    owner: Bytes(d.owner.to_vec()),
                    lending_market: Bytes(d.lending_market.to_vec()),
                    num_deposits: d.num_deposits,
                    num_borrows: d.num_borrows,
                    health_factor_bps: d.health_factor_bps,
                    flags: d.flags,
                    elevation_group: d.elevation_group,
                    referrer: Bytes(d.referrer.to_vec()),
                }),
                other => {
                    decode_failures += 1;
                    eprintln!(
                        "decode Obligation failed for {}: {}",
                        bs58::encode(&acct.pubkey).into_string(),
                        describe_decode_miss(other.map(|r| r.err()))
                    );
                }
            },
            AccountKind::Known("Reserve") => match decode::decode_reserve(&acct.data) {
                Some(Ok(d)) => reserves.push(ReserveSnapshotRow {
                    slot,
                    write_version: SNAPSHOT_WRITE_VERSION,
                    pubkey: Bytes(acct.pubkey.clone()),
                    lending_market: Bytes(d.lending_market.to_vec()),
                    liquidity_mint: Bytes(d.liquidity_mint.to_vec()),
                    supply_vault: Bytes(d.supply_vault.to_vec()),
                    fee_vault: Bytes(d.fee_vault.to_vec()),
                    available_amount: d.available_liquidity,
                    borrowed_amount_sf: d.borrowed_amount,
                    market_price_sf: d.market_price,
                    mint_decimals: d.mint_decimals,
                    acc_protocol_fees_sf: d.accumulated_protocol_fees,
                    acc_referrer_fees_sf: d.accumulated_referrer_fees,
                }),
                other => {
                    decode_failures += 1;
                    eprintln!(
                        "decode Reserve failed for {}: {}",
                        bs58::encode(&acct.pubkey).into_string(),
                        describe_decode_miss(other.map(|r| r.err()))
                    );
                }
            },
            AccountKind::Known("LendingMarket") => match decode::decode_lending_market(&acct.data) {
                Some(Ok(d)) => markets.push(LendingMarketSnapshotRow {
                    slot,
                    write_version: SNAPSHOT_WRITE_VERSION,
                    pubkey: Bytes(acct.pubkey.clone()),
                    owner: Bytes(d.owner.to_vec()),
                    quote_currency: Bytes(d.quote_currency.to_vec()),
                    flags: d.flags,
                    referral_fee_bps: d.referral_fee_bps,
                    liquidation_max_debt_close_factor_pct: d.liquidation_max_debt_close_factor_pct,
                    name: Bytes(d.name.to_vec()),
                }),
                other => {
                    decode_failures += 1;
                    eprintln!(
                        "decode LendingMarket failed for {}: {}",
                        bs58::encode(&acct.pubkey).into_string(),
                        describe_decode_miss(other.map(|r| r.err()))
                    );
                }
            },
            // Every other kind still gets its raw row. An account type we cannot
            // name today is exactly what the undecoded payload column is for.
            _ => {}
        }

        raw_rows.push(AccountUpdateRow {
            slot,
            write_version: SNAPSHOT_WRITE_VERSION,
            pubkey: Bytes(acct.pubkey),
            kind: kind.to_string(),
            owner: Bytes(acct.owner),
            lamports: acct.lamports,
            data: Bytes(acct.data),
        });
    }

    let accounts = raw_rows.len() as u64;
    let mut rows_written = 0usize;
    if let Some(client) = client {
        rows_written += insert_chunked(client, INSERT_UPDATES_SQL, raw_rows, "account_updates").await?;
        rows_written +=
            insert_chunked(client, INSERT_SNAPSHOT_SQL, obligations, "obligation_snapshots").await?;
        rows_written +=
            insert_chunked(client, INSERT_RESERVE_SNAPSHOT_SQL, reserves, "reserve_snapshots")
                .await?;
        rows_written += insert_chunked(
            client,
            INSERT_LM_SNAPSHOT_SQL,
            markets,
            "lending_market_snapshots",
        )
        .await?;
    }

    Ok(PageResult {
        slot,
        accounts,
        rows_written: rows_written as u64,
        decode_failures,
        by_kind,
    })
}

/// Render the two non-success outcomes of a decoder distinctly. `None` means the
/// payload was not the shape the decoder handles at all; `Some(Err)` means it was
/// the right shape and decoding still failed. Collapsing them would hide which.
fn describe_decode_miss(err: Option<Option<anyhow::Error>>) -> String {
    match err {
        Some(Some(e)) => format!("{e:#}"),
        Some(None) => "decoder returned Ok on a retry path".to_owned(),
        None => "payload did not match the decoder's expected size".to_owned(),
    }
}
