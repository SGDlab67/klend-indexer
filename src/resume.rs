//! Resume-seam classification: what the first slot of a session proves about
//! whether history was lost across the restart.
//!
//! The two `record_gap` call sites in `main.rs` both live on failure paths — a
//! rejected subscribe, and a resume that errors before delivering anything. The
//! success path recorded nothing, so the failure mode that actually cost data
//! (the 2026-08-05 wedge: container `Up`, process frozen on a ClickHouse write,
//! no reconnect and therefore no error to hang detection off) produced a hole
//! that no call site could observe. This module is that missing observation.
//!
//! The classification is pure so it can be tested without a stream, a socket, or
//! a ClickHouse. `main.rs` supplies the two numbers and acts on the verdict.

/// Slots of drift between the requested resume point and the first ACCOUNT
/// update actually served that still count as a clean resume.
///
/// Derivation, not a round number picked for looking like one:
///
/// - Floor, measured twice. The 2026-08-07 derivation put ordinary klend-quiet
///   spans at 1..96 slots. The 2026-08-09 container logs put a healthy redeploy
///   seam at 56 slots (checkpoint 437,903,892 → first processed 437,903,948).
///   Both are the normal case and must classify clean.
/// - Ceiling, the replay window: ~6000 slots (~40 min, §8a). Beyond it the
///   subscribe fails outright and the older call sites handle it. A tolerance
///   anywhere near 6000 would swallow the gaps this exists to catch.
///
/// 600 sits ~6× above the measured noise floor and an order of magnitude below
/// the replay window. The asymmetry is deliberate: a missed gap is one
/// unrecorded hole, while a false gap writes fiction into the table that drives
/// backfill, and a backfill aimed at a hole that was never there is worse than
/// no backfill at all.
///
/// The cost of that asymmetry, stated plainly: a real hole shorter than 600
/// slots (~4 min) is indistinguishable here from klend simply being quiet, and
/// goes unrecorded. That is the trade accepted, not an oversight.
pub const RESUME_TOLERANCE_SLOTS: u64 = 600;

/// What the opening slot of a session says about the resume seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeVerdict {
    /// Nothing to compare against: a cold start, a forced tip restart after a
    /// blown replay window, or a sampling run with no writer. Absence of a
    /// checkpoint is not evidence of a gap.
    NotApplicable,

    /// The server resumed at, before, or near enough to the requested slot.
    /// `drift` is how far past `resume_from` the first slot landed (0 when the
    /// server served the requested slot or replayed further back than asked).
    Clean { drift: u64 },

    /// The server began serving materially past the requested slot. The span
    /// between the two was never delivered and cannot be re-served: the stream
    /// only moves forward, so this is history that is gone until backfilled.
    ///
    /// Bounds follow `schema/004_slot_gaps.sql`: `start_slot` is the last slot
    /// held, `end_slot` is the first slot received after the hole, and both are
    /// therefore *outside* it.
    Gap {
        start_slot: u64,
        end_slot: u64,
        missed: u64,
    },
}

/// Classify a session's resume seam from the checkpoint it asked for and the
/// first ACCOUNT slot it actually received.
///
/// The choice of evidence is the whole design, so it is worth stating why the
/// obvious alternative is wrong. The subscription also carries a slots filter,
/// and slot notifications arrive once per slot regardless of klend activity,
/// which makes them look like the better clock. They are not, because whether
/// the provider applies `from_slot` to slot notifications or only to account
/// updates is not contractually guaranteed. Take the three possibilities:
///
/// 1. Provider replays both. First account slot ≈ `resume_from`. Clean, and the
///    slot notifications agree.
/// 2. Provider replays accounts only. Slot notifications open at the live tip,
///    which after a long restart is thousands of slots past the checkpoint —
///    while account replay has in fact lost nothing. Judging on notifications
///    fabricates a gap here; judging on accounts correctly reports clean.
/// 3. Provider honours neither and silently serves from the tip. This is the
///    blind spot worth catching, and the first account slot lands at the tip,
///    far past `resume_from`. Correctly a gap.
///
/// Account slots are right in all three; notification slots are wrong in (2).
/// The price is that klend quiet spans and short real holes look alike, which is
/// what `RESUME_TOLERANCE_SLOTS` absorbs.
///
/// Resume is INCLUSIVE (`schema/002_checkpoint.sql`, §8c): the checkpointed slot
/// is requested again on purpose, because a duplicate is recoverable and a hole
/// is not. `first_account_slot == resume_from` is therefore the *healthy* case
/// and must never be recorded as a gap.
pub fn classify_resume(resume_from: Option<u64>, first_account_slot: u64) -> ResumeVerdict {
    let Some(resume_from) = resume_from else {
        return ResumeVerdict::NotApplicable;
    };

    // At or behind the requested slot: the server honoured the request, possibly
    // over-replaying. Over-replay is duplicates, which the schema's
    // ReplacingMergeTree collapses. Never a gap.
    if first_account_slot <= resume_from {
        return ResumeVerdict::Clean { drift: 0 };
    }

    let drift = first_account_slot - resume_from;
    if drift <= RESUME_TOLERANCE_SLOTS {
        return ResumeVerdict::Clean { drift };
    }

    ResumeVerdict::Gap {
        start_slot: resume_from,
        end_slot: first_account_slot,
        // Both bounds are slots we hold, so the never-delivered count excludes
        // them. `end - start` would overcount by one; the 73,874 figure in
        // docs/backfill-phase2.md is that overcount, and 73,873 slots were
        // actually missed.
        missed: drift - 1,
    }
}

/// The `reason` string stored alongside a startup-detected gap.
///
/// Gap rows are read by humans deciding what to backfill and why, months later.
/// The two existing reasons name the failure that produced them; this one has to
/// say that nothing visibly failed, which is the whole point of the detector.
pub fn startup_gap_reason(drift: u64) -> String {
    format!(
        "startup seam: server resumed {drift} slots past checkpoint without error \
         (no reconnect, no subscribe failure)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_checkpoint_is_not_a_gap() {
        // Cold start, forced tip, sampling run. An empty checkpoint means there
        // is no seam to be wrong about.
        assert_eq!(
            classify_resume(None, 437_313_969),
            ResumeVerdict::NotApplicable
        );
    }

    #[test]
    fn inclusive_resume_is_healthy() {
        // THE case the Agent C brief warned about. Resume is inclusive, so the
        // server serving exactly the checkpointed slot is the design working.
        // A detector that fires here would record a gap on every clean restart.
        assert_eq!(
            classify_resume(Some(437_313_969), 437_313_969),
            ResumeVerdict::Clean { drift: 0 }
        );
    }

    #[test]
    fn over_replay_is_healthy() {
        // Server replayed further back than asked: duplicates, which
        // ReplacingMergeTree collapses. Never a hole.
        assert_eq!(
            classify_resume(Some(437_313_969), 437_313_900),
            ResumeVerdict::Clean { drift: 0 }
        );
    }

    #[test]
    fn measured_quiet_spans_stay_clean() {
        // The 2026-08-07 derivation measured background klend quiet spans at
        // 1..96 slots and judged them normal. Every one of them must classify
        // clean, or the table fills with fiction on ordinary restarts.
        for delta in [1, 7, 42, 96] {
            assert_eq!(
                classify_resume(Some(437_313_969), 437_313_969 + delta),
                ResumeVerdict::Clean { drift: delta },
                "delta {delta} should be a quiet span, not a gap"
            );
        }
    }

    #[test]
    fn the_observed_healthy_redeploy_stays_clean() {
        // Straight from the 2026-08-09 container logs: the Aug 7 redeploy
        // resumed at 437,903,892 and the first slot it processed was
        // 437,903,948. This is what a correct restart looks like, and it is the
        // regression this detector most needs to not break.
        assert_eq!(
            classify_resume(Some(437_903_892), 437_903_948),
            ResumeVerdict::Clean { drift: 56 }
        );
    }

    #[test]
    fn a_long_restart_with_working_replay_stays_clean() {
        // Case (2) in the `classify_resume` docs: the provider replays account
        // updates but opens slot notifications at the live tip. A ten-minute
        // container restart puts the tip ~1500 slots past the checkpoint while
        // account replay loses nothing, so the first ACCOUNT slot is still at
        // the seam. Judging on notifications would fabricate a gap here.
        assert_eq!(
            classify_resume(Some(437_903_892), 437_903_892),
            ResumeVerdict::Clean { drift: 0 }
        );
    }

    #[test]
    fn tolerance_boundary_is_inclusive() {
        let base = 437_313_969;
        assert_eq!(
            classify_resume(Some(base), base + RESUME_TOLERANCE_SLOTS),
            ResumeVerdict::Clean {
                drift: RESUME_TOLERANCE_SLOTS
            }
        );
        assert!(matches!(
            classify_resume(Some(base), base + RESUME_TOLERANCE_SLOTS + 1),
            ResumeVerdict::Gap { .. }
        ));
    }

    #[test]
    fn one_slot_past_tolerance_counts_the_hole_correctly() {
        let base = 437_313_969;
        // Requested `base`, served `base + 601`. Slots base+1 ..= base+600 were
        // never delivered: 600 of them.
        assert_eq!(
            classify_resume(Some(base), base + RESUME_TOLERANCE_SLOTS + 1),
            ResumeVerdict::Gap {
                start_slot: base,
                end_slot: base + RESUME_TOLERANCE_SLOTS + 1,
                missed: RESUME_TOLERANCE_SLOTS,
            }
        );
    }

    #[test]
    fn the_2026_08_05_wedge_shape_is_caught() {
        // The real incident, replayed through the detector. Had this existed,
        // the gap would have been recorded by the process itself instead of
        // derived by hand from account_updates two days later.
        let verdict = classify_resume(Some(437_313_969), 437_387_843);
        assert_eq!(
            verdict,
            ResumeVerdict::Gap {
                start_slot: 437_313_969,
                end_slot: 437_387_843,
                // 73,874 is the span between the bounds; the slots actually
                // missed are the ones strictly inside it.
                missed: 73_873,
            }
        );
    }

    #[test]
    fn gap_bounds_are_slots_we_hold() {
        // The schema's contract: start_slot is the last slot held, end_slot the
        // first received after. Backfill reads these as exclusive bounds, so a
        // detector that reported either as missing would re-fetch data already
        // stored and, worse, imply the true edges are elsewhere.
        let ResumeVerdict::Gap {
            start_slot,
            end_slot,
            missed,
        } = classify_resume(Some(1_000), 2_000)
        else {
            panic!("expected a gap");
        };
        assert_eq!(start_slot, 1_000);
        assert_eq!(end_slot, 2_000);
        assert_eq!(missed, 999);
        assert_eq!(missed, end_slot - start_slot - 1);
    }

    #[test]
    fn checkpoint_at_zero_behaves() {
        // A checkpoint of 0 is a real value, not a sentinel: `Option` carries
        // "no checkpoint". Treating 0 as absent would silence the detector for
        // the one stream that genuinely starts at the beginning.
        assert!(matches!(
            classify_resume(Some(0), 10_000),
            ResumeVerdict::Gap {
                start_slot: 0,
                end_slot: 10_000,
                missed: 9_999,
            }
        ));
    }

    #[test]
    fn reason_names_the_absence_of_a_failure() {
        // The string is the only thing a human reads when deciding whether a
        // recorded gap is trustworthy, so it has to distinguish this detector
        // from the two that hang off visible errors.
        let reason = startup_gap_reason(73_874);
        assert!(reason.contains("73874"));
        assert!(reason.contains("without error"));
    }
}
