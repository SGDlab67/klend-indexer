# Delegation prompts: three klend-indexer tasks

Written 2026-08-07 for handoff to an external agent runner. Each prompt below is
self-contained: paste ONE of them, plus the shared preamble, as a single agent
task. They assume no prior conversation context.

Ordering constraint: Agent A and Agent C both change what runs in production, but
they do not conflict in the tree (A ships the already-committed image, C writes new
code for a later image). Agent B is independent of both. A and B can run now; C
produces a branch for review.

---

## Shared preamble (prepend to every one of the three)

```
You are working on `klend-indexer`, a production Rust service owned by a
mid-to-senior data engineer. Calibrate to that level: no fundamentals, no
tutorials, no restating what the code obviously does.

WHAT THE SYSTEM IS
A Solana account indexer for Kamino Lend (klend). It subscribes to a Yellowstone
gRPC stream (via Alchemy), decodes klend account writes, and batch-inserts them
into a managed ClickHouse Cloud service. It runs as a Docker container on a GCE
e2-micro in us-central1. It has been accumulating continuously and the data it
holds cannot be re-fetched: Yellowstone serves only the chain tip and cannot
re-serve old slots, so every hour the indexer is down is history that is gone
permanently. Treat uptime as the primary asset.

REPO
  /Users/stebit/Github/mastering-rust/klend-indexer   (git, branch `main`,
  remote https://github.com/SGDlab67/klend-indexer, PUBLIC, 3 commits ahead of
  origin and intentionally unpushed)

LAYOUT THAT MATTERS
  src/main.rs        stream binary: subscribe, decode, buffer, flush, checkpoint
  src/bin/snapshot.rs one-shot RPC snapshot writer (getProgramAccounts backfill)
  src/schema.rs      row structs + INSERT statements, SHARED by both binaries
  src/ch.rs          connect_clickhouse, shared by both binaries
  src/decode.rs      Anchor account decoders (has the repo's only unit tests)
  schema/*.sql       ClickHouse DDL, applied in numeric order
  deploy/            operational scripts, see below
  NOTES.md           the worklog. 62 KB. Read the entries relevant to your task.
  AGENTS.md          repo agent conventions

INFRASTRUCTURE FACTS YOU MUST NOT GUESS AT
- The GCP project is `agentbiz-sungodlab`. It is NOT the active gcloud project
  (that is `gen-lang-client-0502946726`). EVERY gcloud command must pass
  `--project agentbiz-sungodlab` explicitly. Omitting it is not a style issue:
  the command resolves against the wrong project and fails with "resource not
  found". This exact defect silently broke the runbook for days.
- VM: `klend-indexer`, zone `us-central1-a`, Container-Optimized OS, e2-micro
  (966 MB RAM). SSH requires `--tunnel-through-iap`.
- ClickHouse Cloud's IP access list admits exactly ONE address: the VM. You
  CANNOT reach ClickHouse directly from this machine. All SQL goes through
  `deploy/ch-remote.sh "<SQL>"`, which tunnels to the VM over IAP, fetches the
  password there from Secret Manager using the instance service-account token,
  and pipes it to curl on fd 3. Do not attempt to reopen the access list. Do not
  attempt to connect to port 8443 directly. Both are wrong answers.
- Database is `klend`. Tables: account_updates, ingest_checkpoint,
  obligation_snapshots, reserve_snapshots, lending_market_snapshots, slot_gaps,
  snapshot_runs.
- The live ClickHouse hostname is deliberately NOT in the repo (it is public).
  It lives in the gitignored `deploy/local.env`, which the scripts source. Never
  commit that hostname or the VM's external IP.

CREDENTIAL RULES (non-negotiable, see SECRETS.md)
Secrets live in the macOS Keychain locally and GCP Secret Manager on the VM.
Never write a credential to a file, never put one in argv, never let one reach
shell history or a log line. If a task seems to require a secret in a file, you
have the wrong approach.

HOUSE STYLE
- No em dashes anywhere: not in code comments, commit messages, docs, or output.
  Use a comma, colon, parentheses, or a period. Existing em dashes in files stay
  as they are; this rule governs text you write.
- Commit messages in this repo are substantial: a subject line, then prose
  explaining WHY, including what was considered and rejected. Match that. Read
  `git log -3` before writing one.
- Worklog doctrine: when a past claim turns out wrong, correct it in a NEW dated
  NOTES.md entry. Never silently rewrite an old entry.
- Comment density in this repo is high and explanatory, focused on rationale and
  on failure modes that were actually observed. Match the surrounding file.

REPORTING
End with: what you changed, what you verified and the literal command output
proving it, and what you did NOT do and why. If you could not complete something,
say so plainly. Do not report success on unverified work.
```

---

## Agent A: redeploy the indexer to the current committed image

**Why this is first:** production is running the 2026-08-05 image, three commits
behind. The stream identifies `kind=Reserve` in its logs but writes nothing to
`reserve_snapshots` or `lending_market_snapshots`, because the decoders that
populate them exist only in the newer code. Both tables are frozen at the
2026-08-06 snapshot slot while reserve state changes every slot.

```
TASK
Build and deploy the current committed HEAD of klend-indexer to the GCE VM, then
prove the new decoders are writing.

READ FIRST
- deploy/DEPLOY.md, section "Redeploy a new build" and section "Guardrails"
- deploy/gce-startup.sh (this is what actually relaunches the container)
- NOTES.md, the entry "Day 5 incident: 8.4h silent freeze" (2026-08-05). It
  explains why `docker pull` before `docker run` is load-bearing and why
  container liveness is not data liveness.

PRE-FLIGHT, ALL FOUR REQUIRED BEFORE YOU BUILD

1. Confirm the tree is clean and you are on the intended commit:
     git status --short          (must be empty)
     git log --oneline -3
   Expected HEAD subject: "Log Phase 2 backfill design and the snapshot run".

2. THE SUBSCRIPTION FILTER GUARDRAIL. DEPLOY.md states: never deploy a build
   that has touched the subscription filter without confirming `owner` is
   non-empty. This build DID touch it (discriminator-scoped memcmp filters were
   added in `build_request`). An empty `owner` field subscribes to every account
   on Solana, which is an unbounded Alchemy bill and a firehose the e2-micro
   cannot survive. Open `src/main.rs`, find `fn build_request`, and confirm ALL
   THREE filters set `owner: vec![KLEND_PROGRAM.to_owned()]`. Paste the three
   lines into your report. If any filter has an empty owner, STOP and report.

3. Confirm the schema the new code writes to actually exists in Cloud. The
   previous near-miss: reserve_snapshots and lending_market_snapshots were in
   schema/ but had never been applied, and a redeploy would have crash-looped on
   the first Reserve flush. Verify:
     deploy/ch-remote.sh "SHOW TABLES FROM klend FORMAT TSVRaw"
   All seven tables must be present. If any is missing, STOP and report; do not
   apply DDL as a side effect of a deploy.

4. Record the CURRENT state so you can prove the deploy changed something, and
   so you can roll back:
     deploy/health-check.sh
     deploy/ch-remote.sh "SELECT max(slot) FROM klend.reserve_snapshots FORMAT TSVRaw"
     deploy/ch-remote.sh "SELECT max(slot) FROM klend.lending_market_snapshots FORMAT TSVRaw"
   Also capture the currently-running image digest, which IS your rollback target:
     gcloud compute ssh klend-indexer --project agentbiz-sungodlab \
       --zone us-central1-a --tunnel-through-iap \
       --command='docker inspect klend-indexer --format "{{.Image}}"'

BUILD
The Rust build is offloaded to Cloud Build; the e2-micro does not have the RAM to
compile. Note the explicit project in the image path. Do NOT use
`$(gcloud config get-value project)` as DEPLOY.md shows, because the active
project is the wrong one:

  gcloud builds submit --project agentbiz-sungodlab \
    --tag us-central1-docker.pkg.dev/agentbiz-sungodlab/klend/klend-indexer:latest .

The Dockerfile is multi-stage and now produces TWO binaries: the indexer and
`snapshot` (copied to /usr/local/bin/klend-snapshot). Both must compile. A build
failure on the snapshot binary fails the whole image, so read the build log tail
before assuming success.

DEPLOY
The startup script is the supervisor. Re-running it pulls :latest and relaunches:

  gcloud compute ssh klend-indexer --project agentbiz-sungodlab \
    --zone us-central1-a --tunnel-through-iap \
    --command='sudo google_metadata_script_runner startup'

VERIFY, IN THIS ORDER. Do not stop at the first green signal.

1. The container came back and is on the NEW image (compare to the digest you
   captured in pre-flight, it must differ):
     docker ps --format '{{.Names}} | {{.Status}} | {{.Image}}'
     docker inspect klend-indexer --format 'restarts={{.RestartCount}} exit={{.State.ExitCode}}'

2. Resume was lossless. In `docker logs`, find the line
     resuming from checkpoint slot=<N> (inclusive)
   and confirm <N> is close to the last_slot you captured pre-flight. Resume is
   INCLUSIVE by design (a slot can split across batches), so re-reading the last
   slot is correct and the duplicate rows dedupe under the ReplacingMergeTree.
   What you are ruling out is a resume from the TIP, which would mean the
   checkpoint was rejected and a gap was just created. If you see
   "GAP ... is UNFILLED" in the logs, the deploy cost data. Report it loudly.

3. Rows are flowing again: `flushed N rows; checkpoint slot=...` appearing
   repeatedly in the logs, and `deploy/health-check.sh` returning HEALTHY with
   lag under ~60s.

4. THE ACTUAL POINT OF THIS DEPLOY. Wait at least 3 minutes after the container
   is healthy, then confirm the two previously-frozen tables are receiving
   STREAM rows, meaning max(slot) has advanced past the snapshot slot 437484525:
     deploy/ch-remote.sh "SELECT 'reserve' t, count(), max(slot) FROM klend.reserve_snapshots
       UNION ALL SELECT 'lm', count(), max(slot) FROM klend.lending_market_snapshots FORMAT TSVRaw"
   Reserves update frequently, so reserve_snapshots should move within minutes.
   LendingMarket accounts change rarely, so a flat max(slot) there is EXPECTED
   and is not a failure. Say which of the two you actually observed moving.
   If reserve_snapshots has not moved after 10 minutes, the deploy did not
   achieve its goal: investigate before declaring success.

5. Watch memory. The box has 966 MB and the previous steady state was ~381 MB
   used. Confirm with `free -m` that the new image has not changed that shape.

ROLLBACK
If the container crash-loops, or resume fails, or inserts error: redeploy the
previously-running image digest captured in pre-flight by pinning it in
`docker run` on the box, or `gcloud compute instances reset klend-indexer
--project agentbiz-sungodlab --zone us-central1-a`. Resume from checkpoint is
lossless, so a fast rollback costs seconds of data, not hours. Prefer a quick
rollback over a long debugging session with the stream down.

DO NOT
- Do not reopen the ClickHouse IP access list.
- Do not apply schema DDL as part of this task.
- Do not push commits to origin.
- Do not modify source files. This task ships what is already committed. If you
  find a bug, report it; do not fix it inside a deploy.
- Do not leave the indexer stopped under any circumstance. If you must abandon
  the task, the container must be running first.

DELIVERABLE
A NOTES.md entry dated 2026-08-07 recording: the image digest before and after,
the resume slot, whether any gap was logged, and the observed row movement in
reserve_snapshots. Commit it. Follow the worklog doctrine and the commit style in
`git log -3`.
```

---

## Agent B: reconcile the known stream gap into `slot_gaps`

**Why:** `slot_gaps` is empty while one real, known, permanent gap exists. The
table currently asserts something false, and `docs/backfill-phase2.md` already
references it as a source of truth.

```
TASK
Insert the one known unfilled stream gap into klend.slot_gaps, after verifying
its boundaries against the data rather than trusting the worklog.

BACKGROUND
On 2026-08-05 the indexer wedged for ~8.4h on a half-open ClickHouse connection
(a network write with no timeout parked the whole select! loop). The container
reported "Up" the entire time. On restart, the checkpoint was far outside
Alchemy's ~6000-slot (~40 min) replay window, so the stream restarted from the
tip. The intervening chain history is permanently unrecoverable from the live
stream: Yellowstone cannot re-serve old slots. Full detail is in NOTES.md, entry
"Day 5 incident: 8.4h silent freeze on a half-open ClickHouse connection".

The worklog records the gap as roughly 437313969..~437387800. "Roughly" is why
this task exists as a verification, not a paste.

READ FIRST
- schema/004_slot_gaps.sql (column semantics, especially that `end_slot` is
  EXCLUSIVE: it is the first slot received AFTER the gap)
- src/main.rs, `Writer::record_gap` and its two call sites, so your manual row
  matches the shape the code writes
- NOTES.md, the Day 5 incident entry

STEP 1: DERIVE THE BOUNDARIES FROM THE DATA
Run this against Cloud (all SQL goes through deploy/ch-remote.sh):

  deploy/ch-remote.sh "
  SELECT prev_slot, slot AS next_slot, slot - prev_slot AS delta
  FROM (
    SELECT slot, any(slot) OVER (ORDER BY slot ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_slot
    FROM (SELECT DISTINCT slot FROM klend.account_updates ORDER BY slot)
  )
  WHERE delta > 100
  ORDER BY delta DESC LIMIT 10 FORMAT TSVRaw"

Interpretation, which is the whole judgement of this task: klend accounts are not
written every slot, so small discontinuities are NORMAL absence of activity, not
missed data. The observed background is deltas of roughly 160 to 210 slots. The
real gap is three orders of magnitude larger. Expect exactly one row that stands
far apart from the rest. Do not record the background deltas as gaps: doing so
would poison the table with false positives and make it useless for the eventual
archive backfill.

Ignore the row whose prev_slot is 0. That is the window function's first-row
artifact at the start of the dataset, not a gap.

STEP 2: CONFIRM, THEN WRITE
Confirm slot_gaps is empty first (this must be idempotent, and the engine is a
ReplacingMergeTree ORDER BY (stream, start_slot), so a re-run collapses rather
than duplicates, but verify rather than rely on it):

  deploy/ch-remote.sh "SELECT count() FROM klend.slot_gaps FORMAT TSVRaw"

Then insert exactly one row, using the boundaries YOU derived in step 1, not the
approximation from NOTES.md:

  stream      = 'klend'
  start_slot  = <last slot before the gap, from step 1 prev_slot>
  end_slot    = <first slot after the gap, from step 1 next_slot>   -- exclusive
  filled      = 0
  reason      = a short factual string naming the cause. Something in the shape
                of 'half-open ClickHouse connection wedged writer 8.4h;
                checkpoint outside replay window on restart'. Keep it under ~120
                chars, no em dashes.

BEFORE YOU RUN THE INSERT: print the exact statement and the derived numbers, and
state whether they agree with NOTES.md. If your derived end_slot differs from the
worklog's ~437387800 estimate, that is expected and fine, but say so explicitly,
because a correction gets logged (see deliverable).

STEP 3: VERIFY
  deploy/ch-remote.sh "SELECT * FROM klend.slot_gaps FINAL FORMAT Vertical"
  deploy/ch-remote.sh "
    SELECT sum(end_slot - start_slot) AS total_missed_slots, count() AS gap_count
    FROM klend.slot_gaps FINAL WHERE filled = 0 FORMAT TSVRaw"

Sanity-check the magnitude: at roughly 2.5 slots/second, the missed slot count
should correspond to something near the 8.4h the incident describes. If it
implies a wildly different duration, your boundaries are wrong. Report the
arithmetic.

DO NOT
- Do not insert a row per background delta.
- Do not mark anything `filled = 1`. Nothing has been backfilled.
- Do not attempt to backfill the missing data. It is unrecoverable from the live
  stream by design; that is the entire premise.
- Do not touch ingest_checkpoint. It is the stream's live resume point and
  writing to it would make the running indexer resume from a slot it never
  consumed.
- Do not modify src/. This task writes one row and one NOTES entry.

DELIVERABLE
A NOTES.md entry dated 2026-08-07 recording the derived boundaries, the query
used to derive them, the reasoning that separates the real gap from background
inactivity, and (per worklog doctrine, as a NEW entry rather than an edit to the
old one) a correction of the approximate end_slot recorded on 2026-08-05 if it
differs. Commit it.
```

---

## Agent C: detect stream gaps at startup, not only on reconnect

**Why:** today a gap is only recorded when a call site happens to observe a
failure. A process that is killed, wedged, or restarted out from under the stream
produces a hole that no existing call site can see. This is the durable fix that
makes Agent B's manual row the last one ever written by hand.

```
TASK
Make the indexer detect and record a stream gap by comparing its durable
checkpoint against the slot it ACTUALLY resumed from, at startup, on every
session. Deliver it as a reviewable branch. Do not deploy it.

READ FIRST, AND UNDERSTAND BEFORE EDITING
- src/main.rs: `read_resume_slot`, `build_request`, `Writer::record_gap`, and the
  whole `'reconnect: loop`. Roughly lines 240 to 540 and 780 to 900.
- schema/002_checkpoint.sql and schema/004_slot_gaps.sql. Read the comments, they
  carry the invariants.
- NOTES.md, entry "Day 5 incident: 8.4h silent freeze". The bug you are fixing is
  the one that incident could not self-report.

THE CURRENT SHAPE, WHICH YOU MUST PRESERVE
The reconnect loop reads a resume point from ingest_checkpoint, subscribes with
`from_slot`, and records a gap in exactly two situations:
  1. subscribe() returns Err while resume_from is Some (replay window blown), and
  2. a resumed session receives NO data before erroring (`got_data == false`),
     which catches the stream-error form of "replay position unavailable".
Both are FAILURE paths. The uncovered case is the SUCCESS path: the subscription
is accepted and data flows, but the first slot actually delivered is far past the
slot we asked to resume from. Nothing today compares those two numbers, so that
hole is invisible.

Invariants you must not break:
- Resume is from last_slot INCLUSIVE, never last_slot + 1. A slot can be split
  across two batches, so the tail of last_slot may be unwritten. Re-reading it
  re-emits stored rows (harmless, they dedupe); skipping would drop the tail.
  Therefore first_received == resume_from is the NORMAL, healthy case and must
  never be recorded as a gap.
- Gap recording is fire-and-forget. It must never block, delay, or fail the
  ingest path. `record_gap` errors are logged and swallowed. Keep that.
- Data batch first, checkpoint second. Do not reorder any write.
- The hot loop must not gain an unbounded `.await` on a network write. The Day 5
  incident was exactly that: one parked future wedged the entire state machine
  for 8.4 hours with no outward sign. Any insert you add needs the same timeout
  discipline as the existing ones.

WHAT TO BUILD

1. Capture the first slot each session actually receives. There is already a
   `first_slot: Option<u64>` in the session scope; check whether it is suitable
   before adding a parallel variable.

2. On the first received update of a session, compare it against the slot that
   session intended to resume from, and record a gap when the difference exceeds
   a threshold. Handle BOTH cases:
     - resume_from == Some(s): compare first_received against s.
     - resume_from == None because force_tip was set, or because this is a fresh
       process whose checkpoint exists but was skipped: the honest comparison is
       against the checkpoint value read from ingest_checkpoint, not against
       nothing. A tip restart after a wedge is precisely the case that currently
       records nothing on the success path. Read the checkpoint for comparison
       purposes even when you are not resuming from it.
   Do NOT record a gap on a genuinely fresh deployment where no checkpoint row
   exists at all. That is an empty database, not a hole.

3. Choose the threshold from evidence and justify it in a comment. Inputs:
   klend accounts are not written every slot, so the observed background between
   consecutive klend-touched slots is roughly 160 to 210 slots; the real 2026-08-05
   gap was 73,874 slots; Alchemy's replay window is ~6000 slots (~40 min). A
   threshold has to sit well above background noise and below the replay window.
   Do not pick a round number without saying why it is safe on both sides.

4. Log it at the same volume as the existing gap paths (one clear stderr line),
   and use the existing `Writer::record_gap` rather than a new insert path.

TESTS
`src/main.rs` currently has no tests; `src/decode.rs` has the repo's only test
module, so follow its conventions. Extract the decision as a PURE function, for
example `fn gap_between(resume_from: Option<u64>, first_received: u64) ->
Option<(u64, u64)>`, so it is testable without a network, a stream, or a
database. Then cover at minimum:
  - first_received == resume_from            -> None (the inclusive-resume case)
  - first_received one slot ahead            -> None
  - first_received within background noise   -> None
  - first_received 73_874 slots ahead        -> Some, with the exact boundaries
  - resume_from == None, no checkpoint       -> None
  - first_received BEHIND resume_from        -> None, and say in a comment why
    this is possible and is not a gap
Run `cargo test` and paste the output. Run `cargo check --all-targets` and
confirm it is clean; note that both binaries share src/schema.rs and src/ch.rs by
`#[path]` include, so a change there affects the snapshot binary too.

DELIVERY
Work on a branch (`git checkout -b gap-detect-at-startup`). Commit with a message
in this repo's style: subject line, then prose on why the success path was blind,
what threshold you chose and why it is safe against both background noise and the
replay window, and what this does NOT cover. Do NOT push, do NOT merge to main,
do NOT build or deploy an image. Production is mid-redeploy on a separate track.

Report the diff and the reasoning for review. Explicitly state any case where the
detector could produce a FALSE gap, because a table of false gaps is worse than
an empty one: it would send a future archive backfill after data that was never
missing.
```

---

## Notes on what these prompts deliberately do not delegate

- **The decision to redeploy at all.** Agent A ships an already-reviewed commit
  and rolls back on trouble. It is not authorised to fix code it finds.
- **Whether the gap is worth recording.** Agent B verifies boundaries and writes
  one row. The judgement that the table should be truthful was already made.
- **Merging Agent C's work.** It produces a branch. Gap-detection logic that
  fires wrongly writes false history into a table meant to drive a backfill, so
  it gets human review before it runs anywhere.
