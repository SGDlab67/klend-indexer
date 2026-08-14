# Demo rehearsal runbook, 2026-08-17

Targets: 7:00 talk + 3:00 Q&A. Audience: instructors + peers, mixed depth.
Judged on Idea / Architecture / Technical implementation / Presentation.

The full talk track is in demo/script.md (numbers filled 2026-08-14). This file is
the operational half: what to open, the click-by-click demo, the fallback drills,
and the pre-flight checklist.

---

## The beats (target timing)

| Time | Beat | Cue |
|---|---|---|
| 0:00-1:00 | Idea | verbatim hook + one plain sentence (script.md 0:00-1:00) |
| 1:00-2:30 | Architecture | three decisions only (script.md 1:00-2:30) |
| 2:30-5:00 | Demo | query over history, stream as garnish (below) |
| 5:00-6:00 | War story | honest + fixed, 60 seconds (script.md 5:00-6:00) |
| 6:00-7:00 | Roadmap + close | end on the question (script.md 6:00-7:00) |

If you are over at 4:30, cut the obligation-history query (3) before anything else;
it is the most replaceable by the recording. If you are under, spend the slack on
the architecture slide, not the demo.

---

## What to open before you go on

1. Slides: `open demo/slides.html` in a browser. Press F for fullscreen,
   arrow keys to move, End for the appendix. No network needed; it works from disk.
2. Dashboard (live): https://storage.googleapis.com/klend-indexer-dashboard/index.html
   in a second tab. The liveness dot is driven by stats.json, republished every 60s
   by the VM, so the dot and the data cannot disagree.
3. Recording fallback: demo/recording/*.webm (42s dashboard walkthrough).
4. Parquet fallback: demo/parquet/ locally, plus gs://klend-indexer-dashboard/klend-parquet/.
   Query it offline with `KLEND_PARQUET_DIR=demo/parquet ./target/debug/coldquery activity`.

---

## The demo sequence (click by click, 2:30-5:00)

The stored data carries the demo. The live stream proves it is real. Pre-say the
burst fact once: "about 8.4% of slots carry any Kamino write, so a quiet stream is
normal, not broken." That inoculates you against 30 seconds of nothing.

1. Switch to the dashboard tab. Point at the liveness dot and the last-write tile.
   Say: "This dot is driven by the same freshness expression the watchdog acts on,
   so the page and the guard cannot disagree." The green dot is the proof the
   pipeline is running right now.

2. Health-factor distribution (headline query A). Say: "This is the current health
   of every tracked obligation. Median health factor is 1.52. Five positions sit
   below the liquidation threshold right now." Then point at the risk view, lowest
   health first: "these are the accounts closest to liquidation."

3. One obligation's history (headline query B). Say: "Here is a single obligation
   across 8.7 days: every deposit, borrow, and health change, in order. This is the
   thing a warehouse cannot give you: the full per-account timeline, not a
   point-in-time balance." (Obligation BYojGuT56e2TUb8PQwRyT1wL5X5Ekv4kZH1HUQgBu6Zg,
   11,169 snapshots.)

4. Row counts / ingest stats (headline query C). Say: "931,073 rows. 163,378
   decoded snapshots. Ingest lag 2 seconds. This is 9.7 days of accumulation."

Keep it mechanical. Rehearse these four until the narration runs itself, so the
live stream can be quiet without derailing you.

---

## Fallback drills (do these on the real laptop, day of)

- Kill the wifi once mid-rehearsal and switch to the recording without stopping.
  Practice the transition sentence: "and here is the same run, recorded."
- Hotspot is the backup network, not the backup plan. The recording is the backup
  plan. The Parquet export is the last resort if both fail.
- Verify the dashboard loads over the venue path before you depend on it: open it
  from a phone hotspot, not from home wifi. Venue wifi is the assumption that fails.

---

## Pre-flight checklist (15 minutes before)

- [ ] Refresh demo/numbers.md: numbers drift as slots advance. Re-run the queries
      in demo/queries.sql via `deploy/ch-remote.sh` and update numbers.md and the
      slides if any headline moved by more than a rounding step.
- [ ] Open slides, confirm fullscreen + arrow keys work, step through to the end.
- [ ] Open the dashboard, confirm the liveness dot is green and last-write is seconds.
- [ ] Confirm demo/recording/*.webm plays.
- [ ] Confirm `KLEND_PARQUET_DIR=demo/parquet ./target/debug/coldquery activity`
      returns rows with the network off.
- [ ] Phone on hotspot (backup network), laptop plugged in.

---

## Read-through protocol (aim for three)

- Run 1: read script.md verbatim, no demo, time each beat. Fix the places you stumble.
- Run 2: full run with slides + live dashboard, time the whole thing to 7:00.
- Run 3: full run with the wifi killed at 2:30, so the fallback is rehearsed, not hoped.

A technical talk lands around 130-150 words a minute. The verbatim opening
(script.md 0:00-1:00) is about 90 words, so it fills the first minute at a steady
pace. If a beat runs long, cut adjectives before you cut numbers; the numbers are
the credibility.

---

## One honesty note for the day

The numbers are readings, not constants (the stream keeps ingesting). The one
number that will NOT move is the loss window: 64,650 rows lost, 158.9 hours,
4,385 accounts (3,175 confirmed obligations). State that one as fact; state the
live ones with "as of this morning".
