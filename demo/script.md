# Demo script, talk track, and Q&A prep

Demo day 2026-08-17. 7 minutes live + 3 minutes Q&A.
Audience: instructors + peers, mixed technical depth.
Judged on Idea / Architecture / Technical implementation / Presentation.

Numbers in [brackets] are placeholders. Fill from demo/numbers.md when Task 3 lands.
Every number on stage must come from a real query or log, never an estimate.

---

## The 7-minute shape

| Time | Beat | What you do |
|---|---|---|
| 0:00-1:00 | Idea | The one-sentence hook + why it matters. Verbatim below. |
| 1:00-2:30 | Architecture | The diagram. Narrate exactly three decisions. |
| 2:30-5:00 | Demo | Query over accumulated history. Liveness dot as proof it is live now. |
| 5:00-6:00 | War story | The data-slice incident, honest + fixed. |
| 6:00-7:00 | Roadmap + close | Artefact 2, backfill, the falsifiable question. End on the question. |

---

## 0:00-1:00 — Idea (verbatim, rehearse this cold)

"Public data warehouses lag Solana by minutes, and they flatten program semantics
down to token transfers. In 2026 Kamino cut its liquidation penalty by ninety
percent, and there is no public dataset that can measure what that did to
liquidators. So I built one: a pipeline that streams every Kamino lending account
off the chain, decodes it at the field level, and stores the full history, so you
can ask questions about protocol behavior that no warehouse can answer today."

Then one sentence on what it is, plainly:

"It is a Rust indexer. It subscribes to a Yellowstone gRPC stream, decodes Kamino's
lending accounts into typed rows, and writes them to ClickHouse. [N] rows ingested,
[unique obligations] obligations tracked, running continuously since [date]."

---

## 1:00-2:30 — Architecture (three decisions, no more)

Point at docs/architecture.svg. Narrate ONLY these three. Each is a real,
defensible decision, and three is enough to signal depth without drowning a mixed
audience.

1. Raw bytes first, decode second. Every account payload is stored undecoded
   alongside the typed columns. Decode is replayable against history. A wrong
   offset is a bug you can fix and re-run, not a bug that destroyed data. This is
   the decision that made the war story survivable.

2. Key on (pubkey, slot, write_version). Measured: about 36% of writes are the
   same account written multiple times inside one slot. A (pubkey, slot) key would
   silently collapse those. write_version keeps every intra-slot version, which is
   exactly what liquidation forensics needs: state on both sides of an instruction.

3. Write the data batch, then the checkpoint. The checkpoint is the resume point,
   and it is written after the data, never before. If the process dies between the
   two, restart re-reads the slot and duplicates a few rows, which the store
   dedupes. The reverse ordering would advance the checkpoint past data that never
   landed, and that hole is silent and permanent. The rule in one line: prefer
   duplicates over holes.

---

## 2:30-5:00 — Demo (query over history; the stream is garnish)

The stored data carries the demo. The live stream proves it is real. Do not let a
quiet stretch on the live stream stall you: only about 6.5% of slots carry any
Kamino update, so 30+ seconds of nothing is normal, not broken. Pre-say that.

Sequence:

1. Open the dashboard. Point at the liveness dot and the "last write N seconds
   ago" tile. Say: "This dot is driven by the same freshness expression the
   watchdog acts on, so the page and the guard cannot disagree." The green dot is
   the proof the pipeline is running right now.

2. Headline query one: health-factor distribution.
   "This is the current health of every tracked obligation. Median health factor
   is [median]. [at_risk] positions sit below liquidation threshold right now."
   The risk view (lowest health factors first) is the legible one for a
   non-specialist: "these are the accounts closest to liquidation."

3. Headline query two: one obligation's history by pubkey.
   "Here is a single obligation across [N] days: every deposit, borrow, and health
   change, in order. This is the thing a warehouse cannot give you: the full
   per-account timeline, not a point-in-time balance."

4. Headline query three: row counts / ingest stats.
   "[total_rows] rows. [obligation_snapshots] decoded snapshots. Ingest lag [lag]
   seconds. This is [days] days of continuous accumulation."

Keep the demo mechanical. Rehearse the queries until they are muscle memory so the
narration runs itself.

---

## 5:00-6:00 — War story (60 seconds, honest + fixed)

"This is the bug I am most proud of finding. I added an accounts_data_slice to cut
bandwidth. It was sized for the Reserve account. But the slice is applied at the
request level, and Yellowstone returns an empty payload for any account shorter
than the slice. Obligations are 3,344 bytes, so from slot 437,903,892 every
Obligation arrived empty. For [hours] hours the pipeline wrote garbage while every
monitor stayed green: the checkpoint advanced, freshness stayed at seconds, no slot
was missed. Throughput was never the problem, so no liveness signal saw it. The
data was just gone.

I found it by auditing the data distribution, not by watching a monitor: one
account type's max slot froze while the others advanced. I removed the slice, and I
added a payload-shape guard: a funded account with a zero-length payload is a shape
the chain does not produce, so it needs no threshold, and it would have fired on
the first Obligation, [hours] earlier. [N] rows lost, unrecoverable. The lesson:
throughput is not integrity."

This honest-failure-plus-systematic-fix beat is the strongest seniority signal you
can send in 60 seconds. Do not soften it.

Close the section with the money numbers:
"RPC spend is about [$] a month. Measured throughput is [kb] KB/s. The cost
problem I actually had was ClickHouse, not the stream, and that is a different
lesson."

---

## 6:00-7:00 — Roadmap + close (end on the question)

"Next: Artefact 2, a liquidation-forensics dataset from this history. Then archive
backfill for the gap the slice created. And the question the whole thing exists
to answer: when Kamino cut liquidation penalties ninety percent, did liquidators
leave, and what did that do to protocol health? I have the data to answer that.
I do not know the answer yet."

End there. Do not say thanks.

---

## Q&A prep (five questions, answer in two sentences max)

1. Why not just use Dune?
   Dune indexes transaction traces, not decoded program account state at every
   slot, and it applies its own abstractions on top. I need the raw account bytes
   with a (pubkey, slot, write_version) history so decode is replayable and
   liquidation forensics can see both sides of an instruction. Dune cannot give me
   that.

2. What happens on a re-org?
   I subscribe at confirmed commitment and key rows on (pubkey, slot,
   write_version), so a re-served slot dedupes rather than corrupts. Re-orgs at
   confirmed are extremely rare on Solana; if one happens it looks like a normal
   replay and the store collapses it. It is on the roadmap to record explicitly.

3. What did this cost?
   The metered upstream is about [$] a month of RPC. The real cost surprise was
   ClickHouse Cloud at $18/day, set by write cadence, not volume; the data is
   being exported to Parquet before the credits lapse Aug 19.

4. Why Rust?
   The stream is a single low-latency decode loop over a high-rate gRPC stream.
   Rust gives zero-copy field access via bytemuck over the program's own interface
   crate, so decoding an account is a cast, not a parse. And the type system made
   the correctness bugs compile errors instead of silent mislabels.

5. What was the hardest bug?
   The data-slice incident. Not because it was hard to find, but because every
   guard was green while it destroyed data for [hours] hours. It forced the lesson
   that a monitoring suite built from liveness signals is blind to a pipeline that
   is working perfectly and producing garbage.

---

## Fallback drill (do this on Day 3, on the real laptop)

- Deliberately kill the wifi once mid-rehearsal and switch to the recording without
  stopping. Practice the transition sentence: "and here is the same run, recorded."
- Hotspot is the backup network, not the backup plan. The recording is the backup
  plan. The Parquet export is the last resort if both fail.
- Verify the dashboard works over the venue path before you depend on it: open it
  from a phone hotspot, not from home wifi.
