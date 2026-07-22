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

    while let Some(message) = stream.next().await {
        let message = message?; // TODO(wk2): reconnect + resume from last slot

        match message.update_oneof {
            Some(UpdateOneof::Account(update)) => {
                let Some(info) = update.account else { continue };

                let kind = AccountKind::from(info.data.as_slice());

                *tally.entry((kind, info.data.len())).or_insert(0) += 1;
                total_bytes += info.data.len() as u64;

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
                    "slot={} pubkey={} kind={kind} data_len={} write_version={}",
                    update.slot,
                    // pubkey is 32 raw bytes; bs58 gives the explorer form.
                    bs58::encode(&info.pubkey).into_string(),
                    info.data.len(),
                    // Idempotency key: disambiguates multiple writes to the same
                    // account in the same slot. Becomes part of the
                    // ReplacingMergeTree ORDER BY.
                    info.write_version,
                );

                // `break`, not `return`, so the summary still runs.
                if slot_budget.is_some_and(|budget| slot_span >= budget) {
                    eprintln!("slot budget reached (span {slot_span}); stopping");
                    break;
                }
            }

            // Keepalive.
            Some(UpdateOneof::Ping(_)) => {}

            _ => {}
        }
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
