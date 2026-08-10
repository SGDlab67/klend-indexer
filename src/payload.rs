//! Payload-shape guard: catch an account type that goes silently undecodable.
//!
//! Every guard this project had before today watches for a *stall* — the
//! checkpoint stops advancing, freshness decays, slots go missing. On
//! 2026-08-08 the pipeline lost 44 hours of Obligation data without stalling
//! once. A request-level `accounts_data_slice` longer than the Obligation
//! account made Yellowstone return an EMPTY payload for every Obligation, so
//! rows kept arriving at full rate, freshness stayed at seconds, no slot was
//! missed, and every existing guard stayed correctly green while a third of the
//! dataset was written as zero-length rows.
//!
//! The lesson generalises past that one bug: **throughput is not integrity.**
//! A guard that only watches whether data is *moving* cannot see data arriving
//! in the wrong shape.
//!
//! # Why this signal needs no threshold
//!
//! A Solana account that holds lamports has been allocated, and allocation sets
//! a data length. `lamports > 0 && data.is_empty()` is therefore a shape the
//! chain does not produce for a program-owned account. It is not "unusual" or
//! "high" — it is impossible, so there is no noise floor to sit above and no
//! threshold to tune wrong. Contrast `RESUME_TOLERANCE_SLOTS` in `resume.rs`,
//! which guards a genuinely noisy signal and must be tuned by error asymmetry.
//!
//! Verified against the incident: all 17,255 `untagged:0b` rows in production
//! carried `lamports > 0`. Zero exceptions. This guard would have fired on the
//! first Obligation after the bad deploy, roughly 44 hours earlier.
//!
//! A genuine account closure — the one case that legitimately produces a
//! zero-length payload — drains lamports to zero in the same instruction, so it
//! classifies as `Closed` and is reported, not alarmed.

use std::fmt;

/// What a single account payload's shape implies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadShape {
    /// Non-empty payload. Says nothing about whether it decodes — that is the
    /// discriminator's job, not this guard's.
    Ok,
    /// Empty payload, no lamports: an ordinary account closure.
    Closed,
    /// Empty payload while still funded. The chain does not produce this for a
    /// program-owned account; something between the validator and here is
    /// truncating payloads.
    ImpossibleEmpty,
}

/// Classify one account update by shape alone.
///
/// Deliberately takes the two primitives rather than a borrowed update struct,
/// so the whole incident can be reproduced in a unit test with two integers.
pub fn classify_payload(lamports: u64, data_len: usize) -> PayloadShape {
    match (lamports, data_len) {
        (_, len) if len > 0 => PayloadShape::Ok,
        (0, _) => PayloadShape::Closed,
        _ => PayloadShape::ImpossibleEmpty,
    }
}

/// How often to repeat the alarm once it is firing, counted in impossible
/// payloads. The first one is always logged; after that this rate-limits so a
/// systemic fault produces a steady heartbeat instead of a wall of text that
/// buries the rest of the log.
const REPEAT_EVERY: u64 = 500;

/// What the caller should do about this payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardAction {
    /// Nothing to say.
    Quiet,
    /// Log loudly: this is the first impossible payload of the session.
    AlarmFirst,
    /// Log a rate-limited continuation, carrying the running total.
    AlarmRepeat { total: u64 },
}

/// Session-scoped tally of impossible payloads.
///
/// Cumulative across reconnects, like the other accumulators in `main`: a fault
/// that survives a reconnect is more serious, not less, so resetting on
/// reconnect would hide exactly the case worth seeing.
#[derive(Debug, Default)]
pub struct PayloadGuard {
    impossible: u64,
    closed: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
}

impl PayloadGuard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one account update and report whether it warrants a log line.
    pub fn observe(&mut self, slot: u64, lamports: u64, data_len: usize) -> GuardAction {
        match classify_payload(lamports, data_len) {
            PayloadShape::Ok => GuardAction::Quiet,
            PayloadShape::Closed => {
                self.closed += 1;
                GuardAction::Quiet
            }
            PayloadShape::ImpossibleEmpty => {
                self.impossible += 1;
                self.first_slot.get_or_insert(slot);
                self.last_slot = Some(slot);

                if self.impossible == 1 {
                    GuardAction::AlarmFirst
                } else if self.impossible.is_multiple_of(REPEAT_EVERY) {
                    GuardAction::AlarmRepeat { total: self.impossible }
                } else {
                    GuardAction::Quiet
                }
            }
        }
    }

    pub fn impossible_count(&self) -> u64 {
        self.impossible
    }

    /// Surfaced through `Display` rather than read directly by `main`; kept as a
    /// named accessor because a rising closure count is a real protocol signal.
    #[allow(dead_code)]
    pub fn closed_count(&self) -> u64 {
        self.closed
    }

    /// Slot range over which impossible payloads were seen, if any.
    pub fn impossible_span(&self) -> Option<(u64, u64)> {
        Some((self.first_slot?, self.last_slot?))
    }

    pub fn is_firing(&self) -> bool {
        self.impossible > 0
    }
}

/// The text for the first alarm. Separated from the logging call so the wording
/// — which is the entire value of the guard at 3am — is itself testable.
pub fn alarm_text(slot: u64, pubkey_b58: &str) -> String {
    format!(
        "PAYLOAD SHAPE FAULT at slot={slot} pubkey={pubkey_b58}: account is funded \
         but arrived with a zero-length payload. The chain does not produce this. \
         Most likely cause: a subscription-level accounts_data_slice longer than \
         this account type, which makes Yellowstone return an empty payload rather \
         than a short one. Data for this account type is being SILENTLY DESTROYED \
         even though ingest looks healthy. See src/payload.rs."
    )
}

impl fmt::Display for PayloadGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.impossible_span() {
            Some((lo, hi)) => write!(
                f,
                "{} impossible empty payloads across slots {}..={} ({} ordinary closures)",
                self.impossible, lo, hi, self.closed
            ),
            None => write!(f, "no shape faults ({} ordinary closures)", self.closed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_normal_payload_is_quiet() {
        assert_eq!(classify_payload(6_264_000, 3344), PayloadShape::Ok);
    }

    #[test]
    fn a_closed_account_is_not_an_alarm() {
        // Drained and deallocated in the same instruction. Legitimate.
        assert_eq!(classify_payload(0, 0), PayloadShape::Closed);
    }

    #[test]
    fn funded_but_empty_is_impossible() {
        assert_eq!(classify_payload(6_264_000, 0), PayloadShape::ImpossibleEmpty);
    }

    /// The shape of the 2026-08-08 incident, in the exact terms it appeared:
    /// Obligation is 3344 bytes, the slice asked for 4664, the payload came back
    /// empty, the account was still funded.
    #[test]
    fn the_2026_08_08_data_slice_incident_is_caught_on_the_first_account() {
        let mut guard = PayloadGuard::new();
        let action = guard.observe(437_903_892, 6_264_000, 0);
        assert_eq!(action, GuardAction::AlarmFirst);
        assert!(guard.is_firing());
        assert_eq!(guard.impossible_count(), 1);
    }

    /// The counterfactual that matters: Reserve was 4664 bytes and matched the
    /// slice exactly, so it kept flowing perfectly. A guard keyed on throughput
    /// sees this stream as healthy — which is precisely why it missed 44 hours.
    #[test]
    fn a_healthy_reserve_stream_never_fires() {
        let mut guard = PayloadGuard::new();
        for i in 0..10_000 {
            assert_eq!(guard.observe(437_903_892 + i, 39_017_040, 4664), GuardAction::Quiet);
        }
        assert!(!guard.is_firing());
        assert_eq!(guard.impossible_count(), 0);
    }

    #[test]
    fn repeats_are_rate_limited_but_never_silent() {
        let mut guard = PayloadGuard::new();
        let mut alarms = 0;
        for i in 0..2_000 {
            if guard.observe(437_903_892 + i, 6_264_000, 0) != GuardAction::Quiet {
                alarms += 1;
            }
        }
        // First, then every 500th: 1, 500, 1000, 1500, 2000.
        assert_eq!(alarms, 5);
        assert_eq!(guard.impossible_count(), 2_000);
    }

    #[test]
    fn the_repeat_alarm_carries_the_running_total() {
        let mut guard = PayloadGuard::new();
        let mut last = GuardAction::Quiet;
        for i in 0..REPEAT_EVERY {
            last = guard.observe(1_000 + i, 1, 0);
        }
        assert_eq!(last, GuardAction::AlarmRepeat { total: REPEAT_EVERY });
    }

    #[test]
    fn closures_do_not_contaminate_the_fault_count() {
        let mut guard = PayloadGuard::new();
        guard.observe(100, 0, 0);
        guard.observe(101, 0, 0);
        guard.observe(102, 500, 3344);
        assert!(!guard.is_firing());
        assert_eq!(guard.closed_count(), 2);
        assert_eq!(guard.impossible_span(), None);
    }

    #[test]
    fn the_span_brackets_every_fault() {
        let mut guard = PayloadGuard::new();
        guard.observe(437_903_892, 1, 0);
        guard.observe(438_000_000, 1, 3344); // fine, not a fault
        guard.observe(438_300_972, 1, 0);
        assert_eq!(guard.impossible_span(), Some((437_903_892, 438_300_972)));
        assert_eq!(guard.impossible_count(), 2);
    }

    #[test]
    fn display_is_readable_in_both_states() {
        let mut guard = PayloadGuard::new();
        assert_eq!(format!("{guard}"), "no shape faults (0 ordinary closures)");
        guard.observe(437_903_892, 1, 0);
        assert_eq!(
            format!("{guard}"),
            "1 impossible empty payloads across slots 437903892..=437903892 (0 ordinary closures)"
        );
    }

    #[test]
    fn the_alarm_names_the_cause_not_just_the_symptom() {
        let text = alarm_text(437_903_892, "3xyz");
        // The operator must be able to act without reading the source.
        assert!(text.contains("accounts_data_slice"));
        assert!(text.contains("SILENTLY DESTROYED"));
        assert!(text.contains("437903892"));
    }
}
