#!/usr/bin/env bash
# Babysit check for the deployed indexer. Run from the Mac; read-only, no billing.
#
# The primary signal is ingest LAG, not container state: a container can be "Up"
# while the stream is silently dead, so freshness of the newest row is what
# actually proves the pipeline works end to end. The 2026-08-05 wedge was exactly
# that shape — `Up 5 hours`, RestartCount 0, and 8h40m of no writes. The container
# is only inspected when the lag says something is already wrong, which keeps the
# common case to a single query and no interactive SSH.
#
# The query goes through deploy/ch-remote.sh rather than straight to :8443,
# because the service's IP access list admits only the VM. See that script.
#
# Exit 0 healthy, 1 stale, 2 cannot reach ClickHouse.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The VM lives in agentbiz-sungodlab, NOT the active gcloud project. Unpinned,
# every gcloud call below resolves against the default and dies with "resource
# not found" — which is how the escalation path was quietly broken.
PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
# Batches flush on a size/time trigger, so a healthy pipeline still shows tens of
# seconds of lag. Alert well above that, low enough to catch a real stall fast.
STALE_AFTER="${STALE_AFTER:-300}"

export GCP_PROJECT="$PROJECT" GCE_ZONE="$ZONE" GCE_VM="$VM"

# Assign, then test. The previous version piped into `read` and hung the `|| exit 2`
# off THAT, but a failing query inside `$(...)` still leaves `read` succeeding on an
# empty string — so an unreachable database reported itself as STALE, sent the script
# down the SSH branch, and printed `lag=s`. A plain assignment carries the command
# substitution's own exit status, which is the status that matters.
OUT="$(deploy/ch-remote.sh "
  SELECT count(), uniqExact(pubkey), max(slot),
         dateDiff('second', max(ingested_at), now64(3))
  FROM klend.account_updates FORMAT TSVRaw")" || {
    echo "UNREACHABLE: ClickHouse query failed (via ${VM})" >&2
    exit 2
}

read -r ROWS ACCTS LAST_SLOT LAG <<<"$(printf '%s' "$OUT" | tr '\n' ' ')"

# A query that "succeeded" but returned nothing parseable is also unreachable, not
# stale. Without this, a truncated or reordered response would fall through to the
# stale branch and misreport the failure a second way.
case "${LAG:-}" in
    ''|*[!0-9-]*) echo "UNREACHABLE: unparseable response: ${OUT:0:120}" >&2; exit 2 ;;
esac

RATE="$(deploy/ch-remote.sh "
  SELECT round(count() / nullIf(dateDiff('second', min(ingested_at), max(ingested_at)), 0), 2)
  FROM klend.account_updates
  WHERE ingested_at > now64(3) - INTERVAL 10 MINUTE FORMAT TSVRaw" 2>/dev/null | tr -d '\n')"

echo "rows=${ROWS} accounts=${ACCTS} last_slot=${LAST_SLOT} lag=${LAG}s rate=${RATE:-0}/s"

if [ "$LAG" -le "$STALE_AFTER" ]; then
    echo "HEALTHY"
    exit 0
fi

echo "STALE: no new rows for ${LAG}s (threshold ${STALE_AFTER}s). Inspecting the VM ..." >&2
gcloud compute ssh "$VM" --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap --command='
    docker ps -a --format "{{.Names}} | {{.Status}}"
    docker inspect klend-indexer --format "restarts={{.RestartCount}} exit={{.State.ExitCode}}" 2>/dev/null
    echo "--- last 20 log lines ---"
    docker logs --tail 20 klend-indexer 2>&1' 2>&1 | grep -v "WARNING\|NumPy\|please see"

echo
echo "Recovery, in order of escalation:" >&2
echo "  1. gcloud compute ssh ${VM} --project ${PROJECT} --zone ${ZONE} --tunnel-through-iap \\" >&2
echo "       --command='sudo google_metadata_script_runner startup'   # re-runs the boot path" >&2
echo "  2. gcloud compute instances reset ${VM} --project ${PROJECT} --zone ${ZONE}   # full reboot; resume is lossless" >&2
exit 1
