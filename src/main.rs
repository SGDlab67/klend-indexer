//! klend-indexer — Phase 0.
//!
//! Streams raw Kamino Lend account updates from a Yellowstone gRPC endpoint.
//!
//! Pipeline: Validator → Geyser → Yellowstone gRPC → [HERE] → decode → ClickHouse

use std::collections::HashMap;

use anyhow::{Context, Result};
use futures::StreamExt;
use klickhouse::{Bytes, Client, ClientOptions, Row};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof,
    subscribe_request_filter_accounts_filter::Filter as FilterVariant,
    subscribe_request_filter_accounts_filter_memcmp::Data as MemcmpData,
    CommitmentLevel, SubscribeRequest, SubscribeRequestAccountsDataSlice,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterSlots,
};

mod ch;
mod decode;
mod resume;
mod schema;

// Glob-imported so the moved items keep their original unqualified names here and
// the extraction stays a pure move, with no call-site churn to review.
use ch::connect_clickhouse;
use resume::{classify_resume, startup_gap_reason, ResumeVerdict};
use schema::*;


/// Current wall-clock time as fractional Unix seconds. Used to compare against a
/// message's server-set `created_at` for the runtime lag metric.
fn now_unix() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}


/// Flush when the buffer reaches this many rows. Bounds memory and insert size;
/// tuned small since klend traffic is light (KB/s), so batches stay modest.
const BATCH_MAX_ROWS: usize = 4096;

/// Flush at least this often even below `BATCH_MAX_ROWS`. klend updates are
/// bursty — a row-count trigger alone could hold a partial batch unwritten
/// through a quiet span; the timer bounds how stale the tail can get.
const FLUSH_INTERVAL_SECS: u64 = 2;

/// How often to log the lag/liveness line. Frequent enough to watch catch-up
/// after a reconnect, sparse enough not to bury the per-account log lines.
const LAG_INTERVAL_SECS: u64 = 10;

/// Hard ceiling on a single ClickHouse insert. Inserts are tiny (KB), so this is
/// far above normal; its job is to turn a half-open connection that never acks into
/// a fast error-and-restart instead of an indefinite hang. See `Writer::flush`.
const INSERT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);


/// Buffered writer: accumulates rows, then commits a batch and advances the
/// checkpoint in one `flush`. Four buffers: raw account_updates rows,
/// decoded obligation_snapshots, reserve_snapshots, and lending_market_snapshots.
struct Writer {
    client: Client,
    buf: Vec<AccountUpdateRow>,
    snap_buf: Vec<ObligationSnapshotRow>,
    reserve_buf: Vec<ReserveSnapshotRow>,
    lm_buf: Vec<LendingMarketSnapshotRow>,
}

impl Writer {
    /// Push a decoded Obligation snapshot into the snapshot buffer.
    fn push_snapshot(&mut self, row: ObligationSnapshotRow) {
        self.snap_buf.push(row);
    }

    /// Push a decoded Reserve snapshot into the reserve buffer.
    fn push_reserve_snapshot(&mut self, row: ReserveSnapshotRow) {
        self.reserve_buf.push(row);
    }

    /// Push a decoded LendingMarket snapshot into the lending market buffer.
    fn push_lm_snapshot(&mut self, row: LendingMarketSnapshotRow) {
        self.lm_buf.push(row);
    }

    /// Record a detected stream gap. This is fire-and-forget: if the insert
    /// fails, the caller logs it and continues — gap recording must never
    /// block the reconnect path.
    async fn record_gap(
        &mut self,
        stream: &str,
        start_slot: u64,
        end_slot: u64,
        reason: &str,
    ) -> Result<()> {
        self.client
            .insert_native_block(
                INSERT_GAP_SQL,
                vec![GapRow {
                    stream: stream.to_owned(),
                    start_slot,
                    end_slot,
                    reason: reason.to_owned(),
                }],
            )
            .await
            .map_err(Into::into)
    }

    /// Commit the buffered rows, then advance the checkpoint. Order is
    /// load-bearing: data first, checkpoint second (§8c — prefer duplicates over
    /// holes; a checkpoint ahead of its data is a silent hole). No-op when empty.
    async fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() && self.snap_buf.is_empty() && self.reserve_buf.is_empty() && self.lm_buf.is_empty() {
            return Ok(());
        }
        // Stream order is slot order, so the last buffered row is the high-water
        // mark. Read it before `take` moves the buffer into the insert.
        let (last_slot, last_write_version) = {
            // Prefer the raw buffer's last row; snapshot, reserve, and lm rows
            // track same (slot, write_version) pairs so the raw buffer's tail is
            // the authoritative high-water mark.
            let slot = if !self.buf.is_empty() {
                self.buf.last().unwrap().slot
            } else if !self.snap_buf.is_empty() {
                self.snap_buf.last().unwrap().slot
            } else if !self.reserve_buf.is_empty() {
                self.reserve_buf.last().unwrap().slot
            } else {
                self.lm_buf.last().unwrap().slot
            };
            let wv = if !self.buf.is_empty() {
                self.buf.last().unwrap().write_version
            } else if !self.snap_buf.is_empty() {
                self.snap_buf.last().unwrap().write_version
            } else if !self.reserve_buf.is_empty() {
                self.reserve_buf.last().unwrap().write_version
            } else {
                self.lm_buf.last().unwrap().write_version
            };
            (slot, wv)
        };
        let rows = std::mem::take(&mut self.buf);
        let snap_rows = std::mem::take(&mut self.snap_buf);
        let reserve_rows = std::mem::take(&mut self.reserve_buf);
        let lm_rows = std::mem::take(&mut self.lm_buf);
        let n = rows.len();
        let n_snap = snap_rows.len();
        let n_reserve = reserve_rows.len();
        let n_lm = lm_rows.len();

        // Bound every insert with a timeout. A ClickHouse Cloud scaling/maintenance
        // event can leave the native connection HALF-OPEN: writes never error, they
        // just never get acked, so `insert_native_block` parks forever and (since
        // flush is awaited inline in the stream loop) wedges the whole indexer with no
        // crash and no restart. The 2026-08-05 incident was exactly this: frozen 8.4h
        // at CPU 0%. On timeout we return Err, which propagates out of `main`, exits
        // the process, and lets `--restart always` reconnect fresh. Prefer a 30s blip
        // and a restart over a silent multi-hour freeze.
        tokio::time::timeout(
            INSERT_TIMEOUT,
            self.client.insert_native_block(INSERT_UPDATES_SQL, rows),
        )
        .await
        .context("insert account_updates batch timed out (connection likely half-open)")?
        .context("insert account_updates batch")?;

        // Snapshot insert — same timeout guard as the raw insert.
        if !snap_rows.is_empty() {
            tokio::time::timeout(
                INSERT_TIMEOUT,
                self.client
                    .insert_native_block(INSERT_SNAPSHOT_SQL, snap_rows),
            )
            .await
            .context("insert obligation_snapshots batch timed out")?
            .context("insert obligation_snapshots batch")?;
        }

        // LendingMarket snapshot insert — same timeout guard.
        if !lm_rows.is_empty() {
            tokio::time::timeout(
                INSERT_TIMEOUT,
                self.client
                    .insert_native_block(INSERT_LM_SNAPSHOT_SQL, lm_rows),
            )
            .await
            .context("insert lending_market_snapshots batch timed out")?
            .context("insert lending_market_snapshots batch")?;
        }

        // Reserve snapshot insert — same timeout guard.
        if !reserve_rows.is_empty() {
            tokio::time::timeout(
                INSERT_TIMEOUT,
                self.client
                    .insert_native_block(INSERT_RESERVE_SNAPSHOT_SQL, reserve_rows),
            )
            .await
            .context("insert reserve_snapshots batch timed out")?
            .context("insert reserve_snapshots batch")?;
        }

        tokio::time::timeout(
            INSERT_TIMEOUT,
            self.client.insert_native_block(
                INSERT_CHECKPOINT_SQL,
                vec![CheckpointRow {
                    stream: STREAM.to_owned(),
                    last_slot,
                    last_write_version,
                }],
            ),
        )
        .await
        .context("advance ingest_checkpoint timed out (connection likely half-open)")?
        .context("advance ingest_checkpoint")?;

        let snap_info = if n_snap > 0 {
            format!(", {n_snap} obligation snapshots")
        } else {
            String::new()
        };
        let reserve_info = if n_reserve > 0 {
            format!(", {n_reserve} reserve snapshots")
        } else {
            String::new()
        };
        let lm_info = if n_lm > 0 {
            format!(", {n_lm} lending market snapshots")
        } else {
            String::new()
        };
        eprintln!(
            "flushed {n} rows{snap_info}{reserve_info}{lm_info}; checkpoint slot={last_slot} write_version={last_write_version}"
        );
        Ok(())
    }
}

/// Build the subscription request. `from_slot` resumes the stream from a past
/// slot on reconnect; `None` starts from the tip. Extracted so the reconnect
/// loop can rebuild the request each attempt with a fresh resume point.
///
/// Uses TWO filters with memcmp discriminator matching so Obligations get full
/// data (needed for decode) while Reserves are trimmed via `accounts_data_slice`
/// at the request level. The slice length is 3344 — the full Obligation wire
/// size (8-byte discriminator + 3336-byte struct), so Obligation decode is
/// unaffected. Reserves are cut from 8624→3344 bytes (61% per-Reserve saving).
fn build_request(from_slot: Option<u64>) -> SubscribeRequest {
    // Helper: build a memcmp filter that matches the given 8-byte discriminator
    // at account-data offset 0.
    fn memcmp_disc(disc: &[u8; 8]) -> SubscribeRequestFilterAccountsFilter {
        SubscribeRequestFilterAccountsFilter {
            filter: Some(FilterVariant::Memcmp(
                SubscribeRequestFilterAccountsFilterMemcmp {
                    offset: 0,
                    data: Some(MemcmpData::Bytes(disc.to_vec())),
                },
            )),
        }
    }

    let mut accounts = HashMap::new();

    // ── Obligation filter: full data, no slice ──
    accounts.insert(
        "klend-obligation".to_owned(),
        SubscribeRequestFilterAccounts {
            owner: vec![KLEND_PROGRAM.to_owned()],
            filters: vec![memcmp_disc(&OBLIGATION_DISC)],
            nonempty_txn_signature: Some(true), // skip vote transactions
            ..Default::default()
        },
    );

    // ── Reserve filter: memcmp on RESERVE_DISC ──
    accounts.insert(
        "klend-reserve".to_owned(),
        SubscribeRequestFilterAccounts {
            owner: vec![KLEND_PROGRAM.to_owned()],
            filters: vec![memcmp_disc(&RESERVE_DISC)],
            nonempty_txn_signature: Some(true),
            ..Default::default()
        },
    );

    // ── LendingMarket filter: memcmp on LENDING_MARKET_DISC ──
    accounts.insert(
        "klend-lending-market".to_owned(),
        SubscribeRequestFilterAccounts {
            owner: vec![KLEND_PROGRAM.to_owned()],
            filters: vec![memcmp_disc(&LENDING_MARKET_DISC)],
            nonempty_txn_signature: Some(true),
            ..Default::default()
        },
    );

    // Live chain tip, for the runtime lag metric. A slots-only subscription is
    // cheap (a slot notification is a bare number) and streams the tip continuously,
    // even through klend-quiet spans when no account updates arrive. filter_by_
    // commitment pins it to the account stream's CONFIRMED level, so `tip` and the
    // last processed slot are directly comparable. This is the in-band replacement
    // for a separate getSlot RPC: no extra endpoint, no round-trip, same answer.
    let mut slots = HashMap::new();
    slots.insert(
        "klend-tip".to_owned(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            ..Default::default()
        },
    );

    SubscribeRequest {
        accounts,
        slots,

        // ⚠️ accounts_data_slice is REQUEST-level (not per-filter), so all
        // matched accounts share the same trim.  4664 = full LendingMarket wire
        // size (8-byte discriminator + 4656-byte struct); LendingMarket decode
        // requires the full payload.  Obligation (3344B) and Reserve (8624B)
        // also share this trim; Reserves are cut from 8624→4664 (~46% savings).
        accounts_data_slice: vec![SubscribeRequestAccountsDataSlice {
            offset: 0,
            length: 4664,
        }],

        // CONFIRMED: supermajority voted, not finalized. PROCESSED rolls back on
        // forks; FINALIZED is ~13s behind. Fork handling lands in Phase 1.
        commitment: Some(CommitmentLevel::Confirmed as i32),

        // Resume seam. Alchemy replays ~6000 slots (~40 min) back (§8a); beyond
        // that the subscribe fails and the caller falls back to the tip. `None`
        // is a fresh start from the tip.
        from_slot,

        ..Default::default()
    }
}

/// Read the durable resume point: the last checkpointed slot for our stream, or
/// `None` on a first run (nothing committed yet). Resume is from this slot
/// INCLUSIVE (see `schema/002_checkpoint.sql` and §8c).
async fn read_resume_slot(client: &Client) -> Result<Option<u64>> {
    // Single-column projection; klickhouse matches the field to the column by name.
    #[derive(Row, Debug)]
    struct ResumeRow {
        last_slot: u64,
    }
    // FINAL collapses the ReplacingMergeTree to the newest row per stream. STREAM
    // is a compile-time constant, so no injection surface.
    let row = client
        .query_opt::<ResumeRow>(format!(
            "SELECT last_slot FROM ingest_checkpoint FINAL WHERE stream = '{STREAM}'"
        ))
        .await
        .context("read ingest_checkpoint for resume point")?;
    Ok(row.map(|r| r.last_slot))
}

#[tokio::main]
async fn main() -> Result<()> {
    // rustls 0.23 picks no crypto backend on its own and panics at first use if
    // it finds zero or two. `ring` is declared directly in Cargo.toml; this call
    // keeps the choice explicit so an added dependency fails at startup, not on
    // first connection.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // Alchemy: GRPC_URL is the bare host (no path); GRPC_TOKEN is sent as the
    // x-token header, not in the URL.
    let url = std::env::var("GRPC_URL")
        .context("GRPC_URL not set — export the Alchemy endpoint first")?;
    let token = std::env::var("GRPC_TOKEN")
        .context("GRPC_TOKEN not set — export your Alchemy API key first")?;

    // Connect the ClickHouse sink BEFORE opening the billed stream: a broken sink
    // must abort here, not after Alchemy has started charging for bytes. An unset
    // or empty CLICKHOUSE_URL means sampling-only — stdout and the slot budget
    // still work, nothing is written. (run.sh sets it by default.)
    let mut writer: Option<Writer> = match std::env::var("CLICKHOUSE_URL") {
        Ok(url) if !url.is_empty() => {
            let options = ClientOptions {
                username: std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "klend".to_owned()),
                password: std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
                default_database: std::env::var("CLICKHOUSE_DATABASE")
                    .unwrap_or_else(|_| "klend".to_owned()),
                ..Default::default()
            };
            // CLICKHOUSE_SECURE=1 selects native-secure TLS (:9440), which hosted
            // ClickHouse Cloud requires; unset stays plain native (:9000) for the
            // local docker sink. Any value other than "1"/"true" is treated as off.
            let secure = matches!(
                std::env::var("CLICKHOUSE_SECURE").as_deref(),
                Ok("1") | Ok("true")
            );
            let client = connect_clickhouse(url.as_str(), secure, options)
                .await
                .with_context(|| {
                    format!("connect ClickHouse at {url} (local: is `./ch.sh up` running?)")
                })?;
            eprintln!(
                "writing to ClickHouse at {url} ({})",
                if secure { "native-secure TLS" } else { "plain native" }
            );
            Some(Writer {
                client,
                buf: Vec::with_capacity(BATCH_MAX_ROWS),
                snap_buf: Vec::with_capacity(BATCH_MAX_ROWS),
                reserve_buf: Vec::with_capacity(BATCH_MAX_ROWS),
                lm_buf: Vec::with_capacity(BATCH_MAX_ROWS),
            })
        }
        _ => {
            eprintln!("CLICKHOUSE_URL unset — sampling only, nothing will be written");
            None
        }
    };

    // The process bounds ITSELF: external time-boxing left orphaned children
    // streaming billable data. Budget is in SLOTS so elapsed time derives from
    // the data, not the wall clock. Unset = run unattended with reconnect; set =
    // a bounded single-shot sample that NEVER reconnects (so a sample can't bill
    // extra by silently re-opening the stream).
    let slot_budget: Option<u64> = match std::env::var("KLEND_SAMPLE_SLOTS") {
        Ok(raw) => Some(
            raw.parse()
                // Must abort, never fall back to "run forever".
                .with_context(|| format!("KLEND_SAMPLE_SLOTS must be a slot count, got {raw:?}"))?,
        ),
        Err(_) => None,
    };

    // ── Accumulators, hoisted above the reconnect loop so a shutdown summary is
    // cumulative across reconnects. Keyed by (kind, data_len): kind alone hides a
    // type with two layouts, len alone merges two types that share a size.
    let mut tally: HashMap<(AccountKind, usize), u64> = HashMap::new();
    let mut total_bytes: u64 = 0;
    let mut first_slot: Option<u64> = None;
    let mut last_slot: u64 = 0;
    // Distinct from slot SPAN. klend updates are bursty (18 active slots across a
    // 456-slot span in one sample), so only the span is a clock.
    let mut slots_with_updates: u64 = 0;
    let mut reconnects: u64 = 0;

    // ── Runtime lag / liveness state ──
    // `tip_slot`: newest slot the chain has reached, from the slots subscription.
    // `last_produced_unix`: server produce-time (`created_at`) of the last message
    // we drained — the keeping-up signal, and it stays fresh even in klend-quiet
    // spans because slot notifications keep arriving. `acct_since_lag`: account
    // updates since the last lag line, for a throughput readout. These persist
    // across reconnects; tip is simply re-learned from the next slot message.
    let mut tip_slot: u64 = 0;
    let mut last_produced_unix: Option<f64> = None;
    let mut acct_since_lag: u64 = 0;

    // Flush the buffer at least every FLUSH_INTERVAL_SECS even when it has not
    // filled. The first tick fires immediately, so consume it before the loop.
    let mut flush_tick =
        tokio::time::interval(std::time::Duration::from_secs(FLUSH_INTERVAL_SECS));
    flush_tick.tick().await;

    // Lag line cadence. Same immediate-first-tick quirk, so consume it up front.
    let mut lag_tick = tokio::time::interval(std::time::Duration::from_secs(LAG_INTERVAL_SECS));
    lag_tick.tick().await;

    // Supervised shutdown: docker/systemd send SIGTERM, an interactive stop sends
    // SIGINT. Either must flush the last partial batch, not drop it (same loss
    // class as day 1's orphan). Handlers built once, awaited in the select below.
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("install SIGINT handler")?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;

    // After a subscribe with `from_slot` fails (replay window exceeded), the next
    // attempt starts from the tip instead of crash-looping on a stale checkpoint.
    let mut force_tip = false;
    // Consecutive sessions that produced no data, for capped exponential backoff.
    // Reset once a session actually receives updates, so a healthy long run that
    // drops recovers instantly while a connect-flap still backs off.
    let mut fail_streak: u32 = 0;
    let mut shutdown = false;

    'reconnect: loop {
        // Resume point (inclusive). First pass: the durable seam a prior process
        // left in the checkpoint. Later passes: batches this run just committed.
        // `force_tip` overrides after a blown replay window. Sampling-only runs
        // (no writer) always start from the tip.
        let resume_from = if force_tip {
            None
        } else {
            match writer.as_ref() {
                Some(w) => read_resume_slot(&w.client).await?,
                None => None,
            }
        };
        force_tip = false;
        if let Some(s) = resume_from {
            eprintln!("resuming from checkpoint slot={s} (inclusive)");
        }

        let mut client = GeyserGrpcClient::build_from_shared(url.clone())?
            .x_token(Some(token.clone()))?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect()
            .await?;

        // `_sink` must stay bound — `_` would drop it immediately and the server may
        // tear the subscription down, producing a stream that ends with no error.
        let subscribed = client
            .subscribe_with_request(Some(build_request(resume_from)))
            .await;
        let (_sink, mut stream) = match subscribed {
            Ok(s) => s,
            // A `from_slot` the server can no longer replay (~6000 slots / ~40 min,
            // §8a). Choose availability over a hole: restart from the tip next pass
            // and leave the gap EXPLICIT for Block 2's backfill rather than crashing
            // on a stale checkpoint. Unattended mode only; a bounded sample should
            // surface the error, not paper over it.
            Err(e) if resume_from.is_some() && slot_budget.is_none() => {
                let s = resume_from.unwrap();
                eprintln!(
                    "subscribe from_slot={s} failed ({e}); replay window likely exceeded, \
                     restarting from tip. GAP {s}..tip is UNFILLED (needs Block 2 backfill)."
                );
                if let Some(w) = writer.as_mut() {
                    // `last_slot` is the tip from the previous session (0 on first run).
                    if let Err(gap_err) = w
                        .record_gap(
                            STREAM,
                            s,
                            last_slot,
                            "subscribe failed: replay window exceeded",
                        )
                        .await
                    {
                        eprintln!("failed to record gap: {gap_err:#}");
                    }
                }
                force_tip = true;
                continue 'reconnect;
            }
            Err(e) => return Err(anyhow::Error::new(e).context("subscribe to klend stream")),
        };

        // Logs to stderr, data to stdout, so `cargo run > sample.jsonl` stays clean.
        eprintln!("subscribed to klend {KLEND_PROGRAM}; waiting for updates…");

        let mut got_data = false;
        let mut stream_err: Option<String> = None;

        // Startup gap detection (§2.3). The first ACCOUNT slot this session, and
        // whether the seam has been judged yet. Both reset per session because
        // each subscribe is its own resume seam.
        //
        // Accounts only, deliberately — NOT the slots filter, whose
        // notifications are denser and look like the better clock. See
        // `resume::classify_resume`: if the provider replays accounts but opens
        // slot notifications at the live tip, judging on notifications invents a
        // gap after every long restart.
        let mut session_first_slot: Option<u64> = None;
        let mut resume_judged = false;

        'stream: loop {
            tokio::select! {
                maybe = stream.next() => {
                    let Some(message) = maybe else { break 'stream }; // stream ended cleanly
                    let message = match message {
                        Ok(m) => m,
                        // A stream error is not fatal here: break out and let the
                        // reconnect loop resume from the checkpoint (unattended) or
                        // stop (sampling). Prefer duplicates over holes (§8c).
                        Err(e) => {
                            stream_err = Some(e.to_string());
                            break 'stream;
                        }
                    };

                    // Server produce-time of THIS message, any variant. Drives the
                    // keeping-up signal in the lag line: as long as we drain the
                    // stream promptly this tracks ~now, independent of klend activity,
                    // because slot notifications carry it too. A half-open connection
                    // or a slow consumer makes it go stale.
                    if let Some(ts) = message.created_at.as_ref() {
                        last_produced_unix = Some(ts.seconds as f64 + ts.nanos as f64 / 1e9);
                    }

                    match message.update_oneof {
                        Some(UpdateOneof::Account(update)) => {
                            let Some(info) = update.account else { continue 'stream };
                            got_data = true;
                            session_first_slot.get_or_insert(update.slot);
                            acct_since_lag += 1;

                            let kind = AccountKind::from(info.data.as_slice());
                            // Captured now so the payload Vec can be moved into the row below.
                            let data_len = info.data.len();

                            *tally.entry((kind, data_len)).or_insert(0) += 1;
                            total_bytes += data_len as u64;

                            // Updates arrive in slot order, so a change in `update.slot`
                            // marks a boundary — no set of seen slots to hold in memory.
                            if first_slot.is_none() {
                                first_slot = Some(update.slot);
                                slots_with_updates = 1;
                            } else if update.slot != last_slot {
                                slots_with_updates += 1;
                            }
                            last_slot = update.slot;

                            let slot_span = last_slot.saturating_sub(first_slot.unwrap_or(last_slot));

                            println!(
                                "slot={} pubkey={} kind={kind} data_len={data_len} write_version={}",
                                update.slot,
                                // pubkey is 32 raw bytes; bs58 gives the explorer form.
                                bs58::encode(&info.pubkey).into_string(),
                                // Idempotency key: disambiguates multiple writes to the same
                                // account in one slot. Part of the ReplacingMergeTree ORDER BY.
                                info.write_version,
                            );

                            // ── Decode Obligation accounts into snapshots ──
                            // Gate on the resolved discriminator so we don't attempt decode
                            // on non-Obligation data. The discriminator match was already done
                            // by AccountKind::Known("Obligation") above.
                            if matches!(kind, AccountKind::Known("Obligation")) {
                                if let Some(w) = writer.as_mut() {
                                    match decode::decode_obligation(&info.data) {
                                        Some(Ok(decoded)) => {
                                            w.push_snapshot(ObligationSnapshotRow {
                                                slot: update.slot,
                                                write_version: info.write_version,
                                                pubkey: Bytes(info.pubkey.clone()),
                                                owner: Bytes(decoded.owner.to_vec()),
                                                lending_market: Bytes(decoded.lending_market.to_vec()),
                                                num_deposits: decoded.num_deposits,
                                                num_borrows: decoded.num_borrows,
                                                health_factor_bps: decoded.health_factor_bps,
                                                flags: decoded.flags,
                                                elevation_group: decoded.elevation_group,
                                                referrer: Bytes(decoded.referrer.to_vec()),
                                                deposited_value_sf: decoded.deposited_value_sf,
                                                borrowed_value_sf: decoded
                                                    .borrowed_assets_market_value_sf,
                                                borrow_factor_adjusted_debt_sf: decoded
                                                    .borrow_factor_adjusted_debt_value_sf,
                                                allowed_borrow_value_sf: decoded
                                                    .allowed_borrow_value_sf,
                                                unhealthy_borrow_value_sf: decoded
                                                    .unhealthy_borrow_value_sf,
                                                lowest_deposit_liquidation_ltv: decoded
                                                    .lowest_deposit_liquidation_ltv,
                                            });
                                        }
                                        Some(Err(e)) => {
                                            eprintln!(
                                                "decode Obligation failed for {}: {e:#}",
                                                bs58::encode(&info.pubkey).into_string()
                                            );
                                        }
                                        None => {
                                            // Not an Obligation despite the discriminator match.
                                            // Shouldn't happen with verified size; log once.
                                            eprintln!(
                                                "decode_obligation returned None for {} (size {})",
                                                bs58::encode(&info.pubkey).into_string(),
                                                data_len,
                                            );
                                        }
                                    }
                                }
                            }

                            // ── Decode Reserve accounts into snapshots ──
                            if matches!(kind, AccountKind::Known("Reserve")) {
                                if let Some(w) = writer.as_mut() {
                                    match decode::decode_reserve(&info.data) {
                                        Some(Ok(decoded)) => {
                                            w.push_reserve_snapshot(ReserveSnapshotRow {
                                                slot: update.slot,
                                                write_version: info.write_version,
                                                pubkey: Bytes(info.pubkey.clone()),
                                                lending_market: Bytes(decoded.lending_market.to_vec()),
                                                liquidity_mint: Bytes(decoded.liquidity_mint.to_vec()),
                                                supply_vault: Bytes(decoded.supply_vault.to_vec()),
                                                fee_vault: Bytes(decoded.fee_vault.to_vec()),
                                                available_amount: decoded.available_liquidity,
                                                borrowed_amount_sf: decoded.borrowed_amount,
                                                market_price_sf: decoded.market_price,
                                                mint_decimals: decoded.mint_decimals,
                                                acc_protocol_fees_sf: decoded.accumulated_protocol_fees,
                                                acc_referrer_fees_sf: decoded.accumulated_referrer_fees,
                                            });
                                        }
                                        Some(Err(e)) => {
                                            eprintln!(
                                                "decode Reserve failed for {}: {e:#}",
                                                bs58::encode(&info.pubkey).into_string()
                                            );
                                        }
                                        None => {
                                            eprintln!(
                                                "decode_reserve returned None for {} (size {})",
                                                bs58::encode(&info.pubkey).into_string(),
                                                data_len,
                                            );
                                        }
                                    }
                                }
                            }

                            // ── Decode LendingMarket accounts into snapshots ──
                            if matches!(kind, AccountKind::Known("LendingMarket")) {
                                if let Some(w) = writer.as_mut() {
                                    match decode::decode_lending_market(&info.data) {
                                        Some(Ok(decoded)) => {
                                            w.push_lm_snapshot(LendingMarketSnapshotRow {
                                                slot: update.slot,
                                                write_version: info.write_version,
                                                pubkey: Bytes(info.pubkey.clone()),
                                                owner: Bytes(decoded.owner.to_vec()),
                                                quote_currency: Bytes(decoded.quote_currency.to_vec()),
                                                flags: decoded.flags,
                                                referral_fee_bps: decoded.referral_fee_bps,
                                                liquidation_max_debt_close_factor_pct: decoded.liquidation_max_debt_close_factor_pct,
                                                name: Bytes(decoded.name.to_vec()),
                                            });
                                        }
                                        Some(Err(e)) => {
                                            eprintln!(
                                                "decode LendingMarket failed for {}: {e:#}",
                                                bs58::encode(&info.pubkey).into_string()
                                            );
                                        }
                                        None => {
                                            eprintln!(
                                                "decode_lending_market returned None for {} (size {})",
                                                bs58::encode(&info.pubkey).into_string(),
                                                data_len,
                                            );
                                        }
                                    }
                                }
                            }

                            // Buffer for the batched write, moving the payload Vecs (no
                            // clone) — this is their last use, `data_len` already captured.
                            if let Some(w) = writer.as_mut() {
                                w.buf.push(AccountUpdateRow {
                                    slot: update.slot,
                                    write_version: info.write_version,
                                    pubkey: Bytes(info.pubkey),
                                    kind: kind.to_string(),
                                    owner: Bytes(info.owner),
                                    lamports: info.lamports,
                                    data: Bytes(info.data),
                                });
                                if w.buf.len() >= BATCH_MAX_ROWS || w.snap_buf.len() >= BATCH_MAX_ROWS || w.reserve_buf.len() >= BATCH_MAX_ROWS || w.lm_buf.len() >= BATCH_MAX_ROWS {
                                    w.flush().await?;
                                }
                            }

                            // `break`, not `return`, so the final flush and summary still run.
                            // A budget stop is intentional and never reconnects (checked below).
                            if slot_budget.is_some_and(|budget| slot_span >= budget) {
                                eprintln!("slot budget reached (span {slot_span}); stopping");
                                break 'stream;
                            }
                        }

                        // Live chain tip for the lag metric. Take the max: slot
                        // notifications for different statuses can arrive slightly
                        // out of order, and tip must never move backward.
                        Some(UpdateOneof::Slot(slot_update)) => {
                            tip_slot = tip_slot.max(slot_update.slot);
                        }

                        // Keepalive.
                        Some(UpdateOneof::Ping(_)) => {}

                        _ => {}
                    }

                    // ── Startup gap detection ──────────────────────────────
                    // Runs on the SUCCESS path, which is the point: the two
                    // older `record_gap` sites both hang off a visible failure
                    // (a rejected subscribe, a stream error before any data).
                    // The 2026-08-05 wedge produced neither — the process froze
                    // on a ClickHouse write with the container still `Up` — so
                    // its 73,874-slot hole was invisible to the indexer and had
                    // to be derived by hand from `account_updates` two days
                    // later. A subscribe that succeeds and then serves from far
                    // past the checkpoint is the same loss with no error
                    // attached, and this is the only place it is observable.
                    //
                    // Judged once per session, on the first account slot seen:
                    // later slots say nothing about the seam.
                    if !resume_judged && let Some(first) = session_first_slot {
                        resume_judged = true;
                        match classify_resume(resume_from, first) {
                            ResumeVerdict::NotApplicable => {}
                            ResumeVerdict::Clean { drift: 0 } => {}
                            ResumeVerdict::Clean { drift } => {
                                // Inside tolerance, so not recorded — but worth
                                // saying out loud. If these lines start showing
                                // drift near RESUME_TOLERANCE_SLOTS, the
                                // threshold is being leaned on rather than
                                // cleared, and its derivation needs revisiting.
                                eprintln!(
                                    "resume seam clean: first slot {first} is {drift} past \
                                     checkpoint (within tolerance)"
                                );
                            }
                            ResumeVerdict::Gap {
                                start_slot,
                                end_slot,
                                missed,
                            } => {
                                eprintln!(
                                    "STARTUP GAP: subscribe succeeded but the stream opened at \
                                     {end_slot}, {} slots past checkpoint {start_slot}. \
                                     {missed} slots were never delivered and cannot be \
                                     re-served. Recording for backfill."
                                    , end_slot - start_slot
                                );
                                if let Some(w) = writer.as_mut() {
                                    // Same fire-and-forget contract as the other
                                    // two sites: a failed gap insert must not
                                    // take down a stream that is otherwise
                                    // healthy and ingesting.
                                    if let Err(e) = w
                                        .record_gap(
                                            STREAM,
                                            start_slot,
                                            end_slot,
                                            &startup_gap_reason(end_slot - start_slot),
                                        )
                                        .await
                                    {
                                        eprintln!("failed to record startup gap: {e:#}");
                                    }
                                }
                            }
                        }
                    }
                }

                // Time-based flush. Guarded off in sampling-only mode, where there is
                // no writer and nothing to flush.
                _ = flush_tick.tick(), if writer.is_some() => {
                    if let Some(w) = writer.as_mut() {
                        w.flush().await?;
                    }
                }

                // Runtime lag line. Reads only in-memory counters and the slot
                // stream, so it runs in sampling mode too. Two independent readings:
                //   behind     — slots between chain tip and the last klend account we
                //                processed. Large-and-shrinking = catching up after a
                //                reconnect. Grows harmlessly during klend-quiet spans
                //                (tip advances, no accounts to process), so read it
                //                together with stream_lag, not alone.
                //   stream_lag — wall seconds between now and the server's produce
                //                time of the last message drained. This is the real
                //                keeping-up signal: ~0 while healthy even in quiet
                //                spans; climbs if we fall behind or the socket half-opens.
                _ = lag_tick.tick() => {
                    if tip_slot > 0 {
                        let behind = tip_slot.saturating_sub(last_slot);
                        let rate = acct_since_lag as f64 / LAG_INTERVAL_SECS as f64;
                        match last_produced_unix {
                            Some(t) => eprintln!(
                                "lag tip={tip_slot} processed={last_slot} behind={behind} slots \
                                 (~{:.0}s) | stream_lag={:.1}s | {rate:.1} acct/s",
                                behind as f64 * 0.4,
                                (now_unix() - t).max(0.0),
                            ),
                            None => eprintln!(
                                "lag tip={tip_slot} processed={last_slot} behind={behind} slots \
                                 (~{:.0}s) | stream_lag=? | {rate:.1} acct/s",
                                behind as f64 * 0.4,
                            ),
                        }
                        acct_since_lag = 0;
                    }
                }

                _ = sigint.recv() => {
                    eprintln!("SIGINT: flushing and shutting down");
                    shutdown = true;
                    break 'stream;
                }
                _ = sigterm.recv() => {
                    eprintln!("SIGTERM: flushing and shutting down");
                    shutdown = true;
                    break 'stream;
                }
            }
        }

        // Final flush covers every exit (budget stop, clean end, stream drop,
        // shutdown), so the last partial batch and its checkpoint are never dropped.
        if let Some(w) = writer.as_mut() {
            w.flush().await?;
        }

        // A signalled shutdown or a reached budget is an intended stop: leave the
        // reconnect loop and print the summary.
        if shutdown {
            break 'reconnect;
        }
        let slot_span = last_slot.saturating_sub(first_slot.unwrap_or(last_slot));
        if slot_budget.is_some_and(|budget| slot_span >= budget) {
            break 'reconnect;
        }
        // Sampling mode never reconnects: a bounded run that ends early (server
        // closed, transient error) must not silently re-open a billed stream.
        if slot_budget.is_some() {
            if let Some(e) = &stream_err {
                eprintln!("stream ended in sampling mode ({e}); not reconnecting");
            }
            break 'reconnect;
        }

        // A resumed session (from_slot set) that received NO data before erroring
        // almost certainly asked for a slot beyond the replay window. Alchemy reports
        // this NOT at subscribe time but as a stream error on the first message
        // ("failed to get replay position for slot N"), so the subscribe-time fallback
        // above never fires and retrying the same from_slot loops forever (the
        // 2026-08-05 restart hit exactly this). Start the next attempt from the tip and
        // leave the gap explicit for Block 2 backfill: availability over a hole (§8c).
        // No suppression needed against the startup detector above, and the
        // reason is not local: that detector only fires after an account update,
        // which is the same event that sets `got_data`. The two conditions are
        // mutually exclusive by construction. Worth knowing if the detector is
        // ever re-fed from slot notifications — both could then fire for one
        // seam, and since `slot_gaps` is ReplacingMergeTree ORDER BY (stream,
        // start_slot), the second row would silently replace the first and
        // substitute a staler `end_slot`.
        if !got_data && let Some(s) = resume_from {
            eprintln!(
                "resume from_slot={s} unreachable (no data before error); starting from tip. \
                 GAP {s}..tip is UNFILLED (needs Block 2 backfill)."
            );
            if let Some(w) = writer.as_mut() {
                // `last_slot` is the tip slot after the just-ended session.
                if let Err(gap_err) = w
                    .record_gap(
                        STREAM,
                        s,
                        last_slot,
                        "resume unreachable: no data before stream error",
                    )
                    .await
                {
                    eprintln!("failed to record gap: {gap_err:#}");
                }
            }
            force_tip = true;
        }

        // Unattended mode: reconnect with capped exponential backoff. A flapping
        // connection bills a ~2× burst each cycle (§9a), so back off; but a healthy
        // session that dropped should recover fast, so reset the streak on data.
        reconnects += 1;
        fail_streak = if got_data { 0 } else { fail_streak.saturating_add(1) };
        let backoff = std::time::Duration::from_secs((1u64 << fail_streak.min(5)).min(30));
        match &stream_err {
            Some(e) => eprintln!(
                "stream dropped ({e}); reconnect #{reconnects} in {}s",
                backoff.as_secs()
            ),
            None => eprintln!(
                "stream ended; reconnect #{reconnects} in {}s",
                backoff.as_secs()
            ),
        }
        tokio::time::sleep(backoff).await;
    }

    let total_updates: u64 = tally.values().sum();
    let slot_span = last_slot.saturating_sub(first_slot.unwrap_or(last_slot));
    let elapsed_secs = slot_span as f64 * 0.4;

    eprintln!("\n─── sample summary ───");
    eprintln!(
        "slots {}..{} — span {slot_span} (~{elapsed_secs:.0}s chain time)",
        first_slot.unwrap_or(0),
        last_slot,
    );
    let density = if slot_span == 0 {
        0.0
    } else {
        100.0 * slots_with_updates as f64 / slot_span as f64
    };
    eprintln!("{slots_with_updates} slots carried updates ({density:.1}% of span)");
    eprintln!("{total_updates} updates, {total_bytes} payload bytes, {reconnects} reconnects");
    if elapsed_secs > 0.0 {
        eprintln!(
            "~{:.1} KB/s payload (bandwidth is what Alchemy bills)",
            total_bytes as f64 / 1024.0 / elapsed_secs
        );
    }

    let mut rows: Vec<_> = tally.into_iter().collect();
    rows.sort_by_key(|&((kind, len), count)| (std::cmp::Reverse(count), kind, len));

    eprintln!("{:<22} {:>9} {:>8} {:>7}", "kind", "data_len", "updates", "share");
    for ((kind, len), count) in rows {
        let share = if total_updates == 0 {
            0.0
        } else {
            100.0 * count as f64 / total_updates as f64
        };
        eprintln!("{kind:<22} {len:>9} {count:>8} {share:>6.2}%");
    }

    Ok(())
}
