#!/usr/bin/env bash
# Refresh the demo-day numbers from the live ClickHouse instance.
#
# Numbers drift as slots advance. The deck, script, and numbers.md carry a
# "captured <date>" label for exactly this reason. Run this before a rehearsal
# or on demo morning, paste the "compact summary" block into numbers.md, and
# update any headline that moved more than a rounding step.
#
# Usage:
#   demo/refresh-numbers.sh          # print every headline query + compact summary
#   demo/refresh-numbers.sh summary  # print only the compact summary block
#
# Requires: gcloud (IAP tunnel to the VM) and the repo's deploy/ch-remote.sh.
# The password is fetched on the VM from Secret Manager; nothing secret touches
# the Mac. See deploy/ch-remote.sh for the full trust chain.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# ch-remote.sh prints an IAP "consider installing NumPy" warning to stderr on
# every call; drop stderr so only the query result (stdout) survives.
run() {
  ./deploy/ch-remote.sh "$1" 2>/dev/null
}

# --- headline queries ---------------------------------------------------------
ROWS=$(run "SELECT count() FROM klend.account_updates FORMAT TSVRaw")
ROWS_FINAL=$(run "SELECT count() FROM klend.account_updates FINAL FORMAT TSVRaw")
SNAP=$(run "SELECT count() FROM klend.obligation_snapshots FINAL FORMAT TSVRaw")
EVER=$(run "SELECT count(DISTINCT pubkey) FROM klend.obligation_snapshots FINAL FORMAT TSVRaw")
ACTIVE=$(run "SELECT count(DISTINCT pubkey) FROM klend.obligation_snapshots FINAL WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0 FORMAT TSVRaw")
HEALTH=$(run "SELECT count(), round(quantile(0.5)(health_factor_bps)/1000000,4), countIf(health_factor_bps < 1050000) FROM klend.obligation_snapshots FINAL WHERE health_factor_bps != 18446744073709551615 AND health_factor_bps > 0 FORMAT TSVRaw")
SPAN=$(run "SELECT min(slot), min(ingested_at), max(slot), max(ingested_at) FROM klend.account_updates FORMAT TSVRaw")
LAG=$(run "SELECT dateDiff('second', max(ingested_at), now64(3)) FROM klend.account_updates FORMAT TSVRaw")
DENSITY=$(run "SELECT count(DISTINCT slot), max(slot)-min(slot), round(count(DISTINCT slot)/(max(slot)-min(slot))*100,4) FROM klend.account_updates FORMAT TSVRaw")
GAPS=$(run "SELECT count() FROM klend.slot_gaps FINAL WHERE filled = 0 FORMAT TSVRaw")

# --- parse ---
VALID=$(echo "$HEALTH" | cut -f1)
MEDIAN=$(echo "$HEALTH" | cut -f2)
ATRISK=$(echo "$HEALTH" | cut -f3)
MIN_SLOT=$(echo "$SPAN" | cut -f1)
MIN_TS=$(echo "$SPAN" | cut -f2)
MAX_SLOT=$(echo "$SPAN" | cut -f3)
MAX_TS=$(echo "$SPAN" | cut -f4)
DEN_SLOTS=$(echo "$DENSITY" | cut -f1)
DEN_SPAN=$(echo "$DENSITY" | cut -f2)
DEN_PCT=$(echo "$DENSITY" | cut -f3)

MODE="${1:-full}"

if [ "$MODE" = "summary" ]; then
  echo "# captured $(date -u +%F) (UTC)"
  echo "1. rows ingested: $ROWS (FINAL $ROWS_FINAL)"
  echo "2. obligations: $EVER ever-seen · $ACTIVE active"
  echo "3. accumulation: first $MIN_SLOT @ $MIN_TS · latest $MAX_SLOT @ $MAX_TS"
  echo "4. ingest lag now: ${LAG}s"
  echo "5. slot density: $DEN_PCT% ($DEN_SLOTS of $DEN_SPAN slots)"
  echo "6. median health: $MEDIAN · at-risk (<1.05): $ATRISK"
  echo "7. snapshots: $SNAP · unfilled gaps: $GAPS"
  exit 0
fi

# --- full report ---
echo "klend-indexer live numbers  ·  $(date -u '+%F %T UTC')"
echo "────────────────────────────────────────────────────────────────"
echo ""
echo "Rows ingested (account_updates):        $ROWS  (FINAL $ROWS_FINAL)"
echo "Obligations ever-seen:                  $EVER"
echo "Obligations currently active:           $ACTIVE"
echo "Decoded snapshots (obligation_snapshots): $SNAP"
echo ""
echo "Health-factor distribution (valid = $VALID):"
echo "  median health: $MEDIAN"
echo "  at-risk below 1.05: $ATRISK"
echo ""
echo "Accumulation span:"
echo "  first: slot $MIN_SLOT @ $MIN_TS"
echo "  latest: slot $MAX_SLOT @ $MAX_TS"
echo "  ingest lag now: ${LAG}s"
echo ""
echo "Slot density:"
echo "  $DEN_PCT% of slots carry >=1 klend update"
echo "  ($DEN_SLOTS distinct slots of $DEN_SPAN span)"
echo ""
echo "Unfilled gaps: $GAPS"
echo ""
echo "────────────────────────────────────────────────────────────────"
echo "compact summary (paste into numbers.md):"
echo ""
echo "# captured $(date -u +%F) (UTC)"
echo "1. rows ingested: $ROWS (FINAL $ROWS_FINAL)"
echo "2. obligations: $EVER ever-seen · $ACTIVE active"
echo "3. accumulation: first $MIN_SLOT @ $MIN_TS · latest $MAX_SLOT @ $MAX_TS"
echo "4. ingest lag now: ${LAG}s"
echo "5. slot density: $DEN_PCT% ($DEN_SLOTS of $DEN_SPAN slots)"
echo "6. median health: $MEDIAN · at-risk (<1.05): $ATRISK"
echo "7. snapshots: $SNAP · unfilled gaps: $GAPS"
