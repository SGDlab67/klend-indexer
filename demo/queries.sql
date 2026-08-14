-- klend-indexer demo — three headline queries (tested against klend_ro, 2026-08-14)
--
-- How to run (from repo root), as the read-only user, through the VM tunnel:
--   CLICKHOUSE_USER=klend_ro \
--   CLICKHOUSE_SECRET_NAME=klend-clickhouse-readonly-password \
--   ./deploy/ch-remote.sh "SQL"
--
-- klend_ro constraints (deploy/create-readonly-user.sh):
--   readonly = 2, max_execution_time = 15 MAX 15,
--   max_result_rows = 1000 MAX 1000  (hard ceiling — see (B)),
--   max_rows_to_read = 500000000 MAX 500000000
--
-- Every table below is ReplacingMergeTree, so aggregate / distinct reads use FINAL.
-- pubkey is FixedString(32): render with base58Encode(pubkey), filter with
-- pubkey = unhex('<64-char hex>'). health_factor_bps is health × 10^6;
-- u64::MAX = 18446744073709551615 is the "no debt" sentinel, and 0 is also excluded.

-- ═══════════════════════════════════════════════════════════════════════════════
-- (A) Health-factor distribution over accumulated history (quantiles)
--     count + median + at-risk count (health < 1.05). 1.05 => 1050000.
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT
  count()                                             AS valid_snapshots,
  round(min(health_factor_bps) / 1000000, 4)          AS min_health,
  round(quantile(0.10)(health_factor_bps) / 1000000, 4) AS p10_health,
  round(quantile(0.25)(health_factor_bps) / 1000000, 4) AS p25_health,
  round(quantile(0.5)(health_factor_bps) / 1000000, 4)  AS median_health,
  round(quantile(0.75)(health_factor_bps) / 1000000, 4) AS p75_health,
  round(quantile(0.90)(health_factor_bps) / 1000000, 4) AS p90_health,
  round(quantile(0.99)(health_factor_bps) / 1000000, 4) AS p99_health,
  round(max(health_factor_bps) / 1000000, 4)          AS max_health,
  countIf(health_factor_bps < 1050000)                AS at_risk_below_1p05,
  round(countIf(health_factor_bps < 1050000) / count(), 4) AS at_risk_share
FROM klend.obligation_snapshots FINAL
WHERE health_factor_bps != 18446744073709551615
  AND health_factor_bps > 0
FORMAT TSVRaw

-- ── (A, histogram) Same data, bucketed — the legible view for a non-specialist. ──
SELECT
  multiIf(
    health_factor_bps / 1000000 < 1.0,  '1: <1.0 liquidatable',
    health_factor_bps / 1000000 < 1.05, '2: 1.0-1.05 at-risk',
    health_factor_bps / 1000000 < 1.25, '3: 1.05-1.25',
    health_factor_bps / 1000000 < 1.5,  '4: 1.25-1.5',
    health_factor_bps / 1000000 < 2.0,  '5: 1.5-2.0',
    health_factor_bps / 1000000 < 3.0,  '6: 2.0-3.0',
    health_factor_bps / 1000000 < 5.0,  '7: 3.0-5.0',
    health_factor_bps / 1000000 < 10.0, '8: 5.0-10.0',
    '9: >=10.0'
  ) AS bucket,
  count() AS n
FROM klend.obligation_snapshots FINAL
WHERE health_factor_bps != 18446744073709551615
  AND health_factor_bps > 0
GROUP BY bucket
ORDER BY bucket
FORMAT TSVRaw

-- ═══════════════════════════════════════════════════════════════════════════════
-- (B) One obligation's full slot-ordered history, by pubkey
--
--     Obligation BYojGuT56e2TUb8PQwRyT1wL5X5Ekv4kZH1HUQgBu6Zg
--     hex     9CBAB5F9B9F21F08DA5F515653BBE3732287A45B8F18526E120E8767073C127B
--
--     Chosen from the risk population (current health 1.28 < the 2.0 cutoff) as
--     the richest history on the table: 11k+ snapshots over ~8.7 days, 1 deposit,
--     1 borrow throughout, health oscillating 1.27 → 37.87 (plus 46 no-debt
--     sentinel moments).
--
--     To discover such a pubkey + its hex from the risk view:
--       SELECT base58Encode(pubkey) AS pk, hex(pubkey) AS pk_hex, count() AS n
--       FROM klend.obligation_snapshots FINAL
--       GROUP BY pubkey ORDER BY n DESC LIMIT 10 FORMAT TSVRaw
--
--     ⚠  klend_ro has max_result_rows = 1000 MAX 1000, so the FULL history cannot
--     be returned in one shot (it is 11k+ rows). The complete history in
--     demo/results/b_obligation_history.tsv was produced by paging with a
--     (slot, write_version) cursor — the single-shot form below is page 1; repeat
--     with `AND (slot, write_version) > (<last_slot>, <last_write_version>)` and
--     concatenate until a page returns < 1000 rows.
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT slot, write_version, health_factor_bps, num_deposits, num_borrows
FROM klend.obligation_snapshots FINAL
WHERE pubkey = unhex('9CBAB5F9B9F21F08DA5F515653BBE3732287A45B8F18526E120E8767073C127B')
ORDER BY slot ASC, write_version ASC
LIMIT 1000
FORMAT TSVRaw

-- ═══════════════════════════════════════════════════════════════════════════════
-- (C) Row counts / ingest stats
--
--     ingest_age_s is THE liveness signal: seconds since the last row landed.
--     checkpoint == latest_slot is expected whether the pipeline is healthy or
--     wedged (they advance together), so lag — not their difference — is the real
--     freshness check. Same expression the watchdog acts on.
-- ═══════════════════════════════════════════════════════════════════════════════
SELECT
  (SELECT count() FROM klend.account_updates)                                   AS total_rows,
  (SELECT count(DISTINCT pubkey) FROM klend.account_updates)                    AS distinct_pubkeys,
  (SELECT max(slot) FROM klend.account_updates)                                 AS latest_slot,
  (SELECT last_slot FROM klend.ingest_checkpoint FINAL WHERE stream = 'klend')  AS checkpoint,
  (SELECT toInt64(toUnixTimestamp(now64(3)) - toUnixTimestamp(max(ingested_at)))
   FROM klend.account_updates)                                                  AS ingest_age_s,
  (SELECT count() FROM klend.slot_gaps FINAL WHERE filled = 0)                  AS unfilled_gaps,
  (SELECT count() FROM klend.obligation_snapshots FINAL)                        AS snap_rows
FORMAT TSVRaw
