//! klend-indexer — Phase 0.
//!
//! Streams raw Kamino Lend account updates from a Yellowstone gRPC endpoint.
//!
//! Pipeline: Validator → Geyser → Yellowstone gRPC → [HERE] → decode → ClickHouse

use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

use sha2::{Digest, Sha256};

use anyhow::{Context, Result};
use futures::StreamExt;
use klickhouse::{Bytes, Client, ClientOptions, Row};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts,
};

mod decode;

/// Candidate klend account struct names. Discriminators are derived from these,
/// never transcribed. Names are a guess from klend's public structs, not a
/// verified IDL — a wrong guess simply never matches, it cannot mislabel.
const CANDIDATE_ACCOUNTS: &[&str] = &[
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
static DISCRIMINATORS: LazyLock<HashMap<[u8; 8], &'static str>> = LazyLock::new(|| {
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
enum AccountKind {
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
fn hex8(bytes: &[u8; 8]) -> String {
    use fmt::Write as _;
    bytes.iter().fold(String::with_capacity(16), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Kamino Lend production mainnet program ID.
/// Staging equivalent: SLendK7ySfcEzyaFqy93gDnD3RtrpXJcnRwb6zFHJSh
const KLEND_PROGRAM: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";

/// Checkpoint stream label. One row per stream in `ingest_checkpoint`; a second
/// subscription (staging, backfill) would use a different label.
const STREAM: &str = "klend";

/// Flush when the buffer reaches this many rows. Bounds memory and insert size;
/// tuned small since klend traffic is light (KB/s), so batches stay modest.
const BATCH_MAX_ROWS: usize = 4096;

/// Flush at least this often even below `BATCH_MAX_ROWS`. klend updates are
/// bursty — a row-count trigger alone could hold a partial batch unwritten
/// through a quiet span; the timer bounds how stale the tail can get.
const FLUSH_INTERVAL_SECS: u64 = 2;

/// Hard ceiling on a single ClickHouse insert. Inserts are tiny (KB), so this is
/// far above normal; its job is to turn a half-open connection that never acks into
/// a fast error-and-restart instead of an indefinite hang. See `Writer::flush`.
const INSERT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Explicit column lists, deliberately: without them the server's INSERT header
/// includes every insertable column — for `account_updates` that means the
/// `ingested_at` DEFAULT column, which the row below does not supply, so the
/// block would be short a column. Naming columns pins the header to exactly what
/// we send. MATERIALIZED (`data_len`) and ALIAS (`pubkey_b58`) are never
/// insertable and are excluded regardless.
const INSERT_UPDATES_SQL: &str = "INSERT INTO account_updates \
    (slot, write_version, pubkey, kind, owner, lamports, data) FORMAT Native";
const INSERT_CHECKPOINT_SQL: &str = "INSERT INTO ingest_checkpoint \
    (stream, last_slot, last_write_version) FORMAT Native";

/// One raw account update, shaped for `klend.account_updates`. Field names are
/// the column names: klickhouse matches a row to the server's block header by
/// name, so field order here is free — kept in schema order for readers.
#[derive(Row, Debug)]
struct AccountUpdateRow {
    slot: u64,
    write_version: u64,
    /// FixedString(32) — raw 32 bytes, not base58. `Bytes` maps to CH String/
    /// FixedString; the readable form is a query-time ALIAS in the schema.
    pubkey: Bytes,
    /// LowCardinality(String) — a plain `String` serializes into it.
    kind: String,
    owner: Bytes,
    lamports: u64,
    /// Undecoded payload → CH String. Decoding is deliberately not done here.
    data: Bytes,
}

/// A checkpoint advance. `updated_at` is a schema DEFAULT, so it is not sent.
#[derive(Row, Debug)]
struct CheckpointRow {
    stream: String,
    last_slot: u64,
    last_write_version: u64,
}

/// Decoded Obligation snapshot — one row per Obligation account update.
#[derive(Row, Debug)]
struct ObligationSnapshotRow {
    slot: u64,
    write_version: u64,
    pubkey: Bytes,
    /// Obligation owner.
    owner: Bytes,
    /// Lending market this obligation belongs to.
    lending_market: Bytes,
    num_deposits: u8,
    num_borrows: u8,
    /// Health factor in bps (×10^6). u64::MAX = no debt / infinite health.
    /// A value of 1_050_000 means health = 1.05.
    health_factor_bps: u64,
    /// Flags: bit 0 = has_debt, bit 1 = elevation_group active, bit 2 = autodeleverage active.
    flags: u8,
    elevation_group: u8,
    referrer: Bytes,
}

const INSERT_SNAPSHOT_SQL: &str = "INSERT INTO obligation_snapshots \
    (slot, write_version, pubkey, owner, lending_market, \
     num_deposits, num_borrows, health_factor_bps, flags, elevation_group, referrer) \
     FORMAT Native";

/// Buffered writer: accumulates rows, then commits a batch and advances the
/// checkpoint in one `flush`. Two buffers: raw account_updates rows, and
/// decoded obligation_snapshots rows.
struct Writer {
    client: Client,
    buf: Vec<AccountUpdateRow>,
    snap_buf: Vec<ObligationSnapshotRow>,
}

impl Writer {
    /// Push a decoded Obligation snapshot into the snapshot buffer.
    fn push_snapshot(&mut self, row: ObligationSnapshotRow) {
        self.snap_buf.push(row);
    }

    /// Commit the buffered rows, then advance the checkpoint. Order is
    /// load-bearing: data first, checkpoint second (§8c — prefer duplicates over
    /// holes; a checkpoint ahead of its data is a silent hole). No-op when empty.
    async fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() && self.snap_buf.is_empty() {
            return Ok(());
        }
        // Stream order is slot order, so the last buffered row is the high-water
        // mark. Read it before `take` moves the buffer into the insert.
        let (last_slot, last_write_version) = {
            // Prefer the raw buffer's last row; snapshot rows track same (slot, write_version)
            // pairs so the raw buffer's tail is the authoritative high-water mark.
            let slot = if !self.buf.is_empty() {
                self.buf.last().unwrap().slot
            } else {
                self.snap_buf.last().unwrap().slot
            };
            let wv = if !self.buf.is_empty() {
                self.buf.last().unwrap().write_version
            } else {
                self.snap_buf.last().unwrap().write_version
            };
            (slot, wv)
        };
        let rows = std::mem::take(&mut self.buf);
        let snap_rows = std::mem::take(&mut self.snap_buf);
        let n = rows.len();
        let n_snap = snap_rows.len();

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
        eprintln!(
            "flushed {n} rows{snap_info}; checkpoint slot={last_slot} write_version={last_write_version}"
        );
        Ok(())
    }
}

/// Connect the ClickHouse sink, plain or TLS. Hosted ClickHouse Cloud speaks
/// native-secure on :9440; the local docker sink speaks plain native on :9000.
/// `url` is `host:port`; when secure, the host part is also the TLS SNI name.
async fn connect_clickhouse(url: &str, secure: bool, options: ClientOptions) -> Result<Client> {
    if !secure {
        return Client::connect(url, options).await.map_err(Into::into);
    }

    // Trust anchors baked into the binary (webpki-roots), so a stripped container
    // image needs no OS cert store. The crypto backend is the ring provider
    // installed as process default at the top of `main`.
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));

    // SNI name is the host without the port. connect_tls resolves `url` for the
    // TCP connection and uses this name for the certificate check.
    let host = url.rsplit_once(':').map(|(h, _)| h).unwrap_or(url);
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from(host.to_owned())
        .with_context(|| format!("invalid TLS server name {host:?}"))?;

    Client::connect_tls(url, options, server_name, &connector)
        .await
        .map_err(Into::into)
}

/// Build the subscription request. `from_slot` resumes the stream from a past
/// slot on reconnect; `None` starts from the tip. Extracted so the reconnect
/// loop can rebuild the request each attempt with a fresh resume point.
fn build_request(from_slot: Option<u64>) -> SubscribeRequest {
    // Filters are a map; the key is a label the server echoes back on every
    // matching update.
    let mut accounts = HashMap::new();
    accounts.insert(
        "klend".to_owned(),
        SubscribeRequestFilterAccounts {
            owner: vec![KLEND_PROGRAM.to_owned()],

            // ⚠️  An EMPTY owner vec means "no filter" — every account update on
            // mainnet, silently, on a bandwidth-billed stream. Never empty this.
            //
            // Later: `filters` (memcmp on discriminator) and `accounts_data_slice`
            // (trim payloads) go here.
            ..Default::default()
        },
    );

    SubscribeRequest {
        accounts,

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

    // Flush the buffer at least every FLUSH_INTERVAL_SECS even when it has not
    // filled. The first tick fires immediately, so consume it before the loop.
    let mut flush_tick =
        tokio::time::interval(std::time::Duration::from_secs(FLUSH_INTERVAL_SECS));
    flush_tick.tick().await;

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
                force_tip = true;
                continue 'reconnect;
            }
            Err(e) => return Err(anyhow::Error::new(e).context("subscribe to klend stream")),
        };

        // Logs to stderr, data to stdout, so `cargo run > sample.jsonl` stays clean.
        eprintln!("subscribed to klend {KLEND_PROGRAM}; waiting for updates…");

        let mut got_data = false;
        let mut stream_err: Option<String> = None;

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

                    match message.update_oneof {
                        Some(UpdateOneof::Account(update)) => {
                            let Some(info) = update.account else { continue 'stream };
                            got_data = true;

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
                                if w.buf.len() >= BATCH_MAX_ROWS || w.snap_buf.len() >= BATCH_MAX_ROWS {
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

                        // Keepalive.
                        Some(UpdateOneof::Ping(_)) => {}

                        _ => {}
                    }
                }

                // Time-based flush. Guarded off in sampling-only mode, where there is
                // no writer and nothing to flush.
                _ = flush_tick.tick(), if writer.is_some() => {
                    if let Some(w) = writer.as_mut() {
                        w.flush().await?;
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
        if !got_data && let Some(s) = resume_from {
            eprintln!(
                "resume from_slot={s} unreachable (no data before error); starting from tip. \
                 GAP {s}..tip is UNFILLED (needs Block 2 backfill)."
            );
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
