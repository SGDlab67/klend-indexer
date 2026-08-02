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

/// Buffered writer: accumulates rows, then commits a batch and advances the
/// checkpoint in one `flush`.
struct Writer {
    client: Client,
    buf: Vec<AccountUpdateRow>,
}

impl Writer {
    /// Commit the buffered rows, then advance the checkpoint. Order is
    /// load-bearing: data first, checkpoint second (§8c — prefer duplicates over
    /// holes; a checkpoint ahead of its data is a silent hole). No-op when empty.
    async fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        // Stream order is slot order, so the last buffered row is the high-water
        // mark. Read it before `take` moves the buffer into the insert.
        let (last_slot, last_write_version) = {
            let last = self.buf.last().expect("buffer checked non-empty");
            (last.slot, last.write_version)
        };
        let rows = std::mem::take(&mut self.buf);
        let n = rows.len();

        self.client
            .insert_native_block(INSERT_UPDATES_SQL, rows)
            .await
            .context("insert account_updates batch")?;
        self.client
            .insert_native_block(
                INSERT_CHECKPOINT_SQL,
                vec![CheckpointRow {
                    stream: STREAM.to_owned(),
                    last_slot,
                    last_write_version,
                }],
            )
            .await
            .context("advance ingest_checkpoint")?;

        eprintln!("flushed {n} rows; checkpoint slot={last_slot} write_version={last_write_version}");
        Ok(())
    }
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
            let client = Client::connect(url.as_str(), options)
                .await
                .with_context(|| format!("connect ClickHouse at {url} (is `./ch.sh up` running?)"))?;
            eprintln!("writing to ClickHouse at {url}");
            Some(Writer {
                client,
                buf: Vec::with_capacity(BATCH_MAX_ROWS),
            })
        }
        _ => {
            eprintln!("CLICKHOUSE_URL unset — sampling only, nothing will be written");
            None
        }
    };

    let mut client = GeyserGrpcClient::build_from_shared(url)?
        .x_token(Some(token))?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

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

    let request = SubscribeRequest {
        accounts,

        // CONFIRMED: supermajority voted, not finalized. PROCESSED rolls back on
        // forks; FINALIZED is ~13s behind. Fork handling lands in Phase 1.
        commitment: Some(CommitmentLevel::Confirmed as i32),

        ..Default::default()
    };

    // `_sink` must stay bound — `_` would drop it immediately and the server may
    // tear the subscription down, producing a stream that ends with no error.
    let (_sink, mut stream) = client.subscribe_with_request(Some(request)).await?;

    // Logs to stderr, data to stdout, so `cargo run > sample.jsonl` stays clean.
    eprintln!("subscribed to klend {KLEND_PROGRAM}; waiting for updates…");

    // The process bounds ITSELF: external time-boxing left orphaned children
    // streaming billable data. Budget is in SLOTS so elapsed time derives from
    // the data, not the wall clock. Unset = run forever.
    let slot_budget: Option<u64> = match std::env::var("KLEND_SAMPLE_SLOTS") {
        Ok(raw) => Some(
            raw.parse()
                // Must abort, never fall back to "run forever".
                .with_context(|| format!("KLEND_SAMPLE_SLOTS must be a slot count, got {raw:?}"))?,
        ),
        Err(_) => None,
    };

    // Keyed by (kind, data_len): kind alone hides a type with two layouts, len
    // alone merges two types that share a size.
    let mut tally: HashMap<(AccountKind, usize), u64> = HashMap::new();
    let mut total_bytes: u64 = 0;
    let mut first_slot: Option<u64> = None;
    let mut last_slot: u64 = 0;

    // Distinct from slot SPAN. klend updates are bursty (18 active slots across a
    // 456-slot span in one sample), so only the span is a clock.
    let mut slots_with_updates: u64 = 0;

    // Flush the buffer at least every FLUSH_INTERVAL_SECS even when it has not
    // filled. The first tick fires immediately, so consume it before the loop.
    let mut flush_tick =
        tokio::time::interval(std::time::Duration::from_secs(FLUSH_INTERVAL_SECS));
    flush_tick.tick().await;

    'stream: loop {
        tokio::select! {
            maybe = stream.next() => {
                let Some(message) = maybe else { break 'stream }; // stream ended
                let message = message?; // TODO(wk2): reconnect + resume from checkpoint

                match message.update_oneof {
                    Some(UpdateOneof::Account(update)) => {
                        let Some(info) = update.account else { continue 'stream };

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
                            if w.buf.len() >= BATCH_MAX_ROWS {
                                w.flush().await?;
                            }
                        }

                        // `break`, not `return`, so the final flush and summary still run.
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
        }
    }

    // Final flush covers both the budget break and a naturally ended stream, so
    // the last partial batch and its checkpoint are never dropped on exit.
    if let Some(w) = writer.as_mut() {
        w.flush().await?;
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
    eprintln!("{total_updates} updates, {total_bytes} payload bytes");
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
