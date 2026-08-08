-- Detected gaps in the stream: spans where the indexer KNOWS it missed data.
-- Populated by the reconnect-lite path when a resume point is unreachable.
--
-- Query gap status:
--   SELECT sum(end_slot - start_slot) AS total_missed_slots,
--          count() AS gap_count,
--          max(detected_at) AS latest_gap
--   FROM slot_gaps FINAL
--   WHERE filled = 0
CREATE TABLE IF NOT EXISTS klend.slot_gaps
(
    stream          LowCardinality(String),
    start_slot      UInt64,
    end_slot        UInt64 COMMENT 'exclusive — the first slot received after the gap',
    detected_at     DateTime64(3) DEFAULT now64(3),
    filled          UInt8 DEFAULT 0 COMMENT '0 = unfilled, 1 = backfilled',
    reason          String,
    ingested_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (stream, start_slot)
PARTITION BY intDiv(start_slot, 10000000);
