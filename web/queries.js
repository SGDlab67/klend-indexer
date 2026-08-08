// The complete set of statements the dashboard can cause to run.
//
// Shared by web/proxy.js (the local-dev live view) and web/export.js (the
// static generator that publishes to object storage). Same reasoning as
// src/schema.rs being shared by the two Rust binaries: two consumers issuing
// the same queries must not each carry a copy, because a drifted copy is a
// silently wrong answer rather than a compile error.
//
// Adding a capability means adding a named entry here, which is the point. The
// reachable query surface is reviewable in one place, in a diff. Nothing in
// either consumer builds SQL from request data.

const QUERIES = {
  overview: `
    SELECT
      (SELECT count(DISTINCT pubkey) FROM klend.reserve_snapshots FINAL) AS reserves,
      (SELECT count(DISTINCT pubkey) FROM klend.obligation_snapshots FINAL
       WHERE health_factor_bps != 18446744073709551615) AS active,
      (SELECT quantile(0.5)(health_factor_bps)/1000000 FROM klend.obligation_snapshots FINAL
       WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0) AS med_health,
      (SELECT count() FROM klend.obligation_snapshots FINAL
       WHERE health_factor_bps < 1050000 AND health_factor_bps > 0) AS at_risk,
      (SELECT quantile(0.25)(health_factor_bps)/1000000 FROM klend.obligation_snapshots FINAL
       WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0
         AND health_factor_bps < 10000000) AS p25
    FORMAT TSVRaw`,

  risk: `
    SELECT base58Encode(pubkey) AS pk, argMax(owner_b58, slot) AS ow,
           argMax(health_factor_bps, slot) AS hf,
           argMax(num_deposits, slot) AS dep, argMax(num_borrows, slot) AS bor
    FROM klend.obligation_snapshots FINAL
    WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0
    GROUP BY pubkey
    HAVING hf < 2000000
    ORDER BY hf ASC LIMIT 15
    FORMAT TSVRaw`,

  reserves: `
    SELECT base58Encode(pubkey) AS pk, argMax(available_amount, slot) AS avail,
           argMax(borrowed_amount_sf, slot) AS borr, argMax(mint_decimals, slot) AS dec,
           argMax(base58Encode(liquidity_mint), slot) AS liq_mint
    FROM klend.reserve_snapshots FINAL
    GROUP BY pubkey
    ORDER BY avail DESC LIMIT 15
    FORMAT TSVRaw`,

  system: `
    SELECT
      (SELECT max(slot) FROM klend.account_updates) AS latest_slot,
      (SELECT last_slot FROM klend.ingest_checkpoint FINAL WHERE stream='klend') AS checkpoint,
      (SELECT count() FROM klend.slot_gaps FINAL WHERE filled=0) AS unfilled_gaps,
      (SELECT count() FROM klend.account_updates) AS total_rows,
      (SELECT count() FROM klend.obligation_snapshots FINAL) AS snap_rows,
      (SELECT max(slot) FROM klend.reserve_snapshots FINAL) AS reserve_max
    FORMAT TSVRaw`,
};

// Cost ceilings applied by ClickHouse itself, and the belt to the read-only
// user's braces: even a mistake in QUERIES above cannot write.
//
// readonly=2, not 1, and the difference matters. readonly=1 forbids writes AND
// forbids changing settings, which would make ClickHouse reject every other
// parameter here ("Cannot modify 'max_execution_time' setting in readonly
// mode"). readonly=2 forbids INSERT/ALTER/CREATE/DROP while still allowing a
// caller to TIGHTEN limits, which is exactly the combination wanted. The user's
// own grants (SELECT on klend.* and nothing else) remain the primary control.
const CH_SETTINGS = {
  readonly: '2',
  max_execution_time: '15',
  max_result_rows: '1000',
  max_result_bytes: '10000000',
  max_rows_to_read: '500000000',
  result_overflow_mode: 'break',
  timeout_overflow_mode: 'break',
};

module.exports = { QUERIES, CH_SETTINGS };
