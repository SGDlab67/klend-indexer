-- Ingest checkpoint — the durable resume seam (plan §11d step 3, §8c).
--
-- Written WITH each batch, AFTER the data batch commits. Nothing reads it yet;
-- Block 2's reconnect/resume logic bolts onto it. The write ordering (data
-- first, checkpoint second) is what makes §8c's "prefer duplicates over holes"
-- hold: if the batch commits and this does not, resume rewinds to an OLDER slot
-- and reprocesses — and account_updates' ReplacingMergeTree makes the duplicate
-- rows harmless. The reverse order would advance the checkpoint past data that
-- never landed: a silent hole.
CREATE TABLE IF NOT EXISTS klend.ingest_checkpoint
(
    -- One logical checkpoint per stream, so a second subscription (staging, a
    -- backfill worker) can checkpoint independently without a schema change.
    stream              LowCardinality(String),

    -- High-water mark: the (slot, write_version) of the last row in the last
    -- committed batch. Updates arrive in slot order, so this is the max seen.
    --
    -- Resume is from last_slot INCLUSIVE, never last_slot + 1: a slot can be
    -- split across two batches, so the tail of last_slot may be unwritten.
    -- Re-reading the whole slot re-emits rows already stored (harmless dedup);
    -- skipping to the next slot would drop that unwritten tail (a hole).
    last_slot           UInt64,
    last_write_version  UInt64,

    -- Version column for the engine below, and a human "when did we last commit".
    -- Our clock, not a chain timestamp (same rule as account_updates.ingested_at).
    updated_at          DateTime64(3) DEFAULT now64(3)
)
-- Collapses to one row per stream, keeping the newest by updated_at. Read the
-- current checkpoint with FINAL (or argMax(last_slot, updated_at)); the row
-- count stays at "number of streams", not "number of batches committed".
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY stream;
