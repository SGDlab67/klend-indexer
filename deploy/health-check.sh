#!/usr/bin/env bash
# Babysit check for the deployed indexer. Run from the Mac; read-only, no billing.
#
# The primary signal is ingest LAG, not container state: a container can be "Up"
# while the stream is silently dead, so freshness of the newest row is what
# actually proves the pipeline works end to end. The container is only inspected
# when the lag says something is already wrong, which keeps the common case to a
# single HTTPS query and no SSH.
#
# Exit 0 healthy, 1 stale, 2 cannot reach ClickHouse.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

HOST="${CLICKHOUSE_CLOUD_HOST:-YOUR_INSTANCE.REGION.gcp.clickhouse.cloud}"
USER_="${CLICKHOUSE_USER:-default}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
# Batches flush on a size/time trigger, so a healthy pipeline still shows tens of
# seconds of lag. Alert well above that, low enough to catch a real stall fast.
STALE_AFTER="${STALE_AFTER:-300}"

PW="$(security find-generic-password -a "$USER" -s klend-clickhouse-cloud-password -w 2>/dev/null)" || {
    echo "error: no keychain entry 'klend-clickhouse-cloud-password' for user '$USER'." >&2
    exit 2
}

# Credentials on fd 3 via curl --config, never in argv. Same rule as SECRETS.md.
q() { curl -sS --fail-with-body --max-time 30 --config /dev/fd/3 \
        "https://${HOST}:8443/" --data-binary "$1" 3<<<"user = \"${USER_}:${PW}\""; }

read -r ROWS ACCTS LAST_SLOT LAG <<<"$(q "
  SELECT count(), uniqExact(pubkey), max(slot),
         dateDiff('second', max(ingested_at), now64(3))
  FROM klend.account_updates FORMAT TSVRaw" | tr '\n' ' ')" || {
    echo "UNREACHABLE: ClickHouse query failed" >&2
    exit 2
}

RATE="$(q "
  SELECT round(count() / nullIf(dateDiff('second', min(ingested_at), max(ingested_at)), 0), 2)
  FROM klend.account_updates
  WHERE ingested_at > now64(3) - INTERVAL 10 MINUTE FORMAT TSVRaw")"

echo "rows=${ROWS} accounts=${ACCTS} last_slot=${LAST_SLOT} lag=${LAG}s rate=${RATE:-0}/s"

if [ "${LAG:-99999}" -le "$STALE_AFTER" ]; then
    echo "HEALTHY"
    exit 0
fi

echo "STALE: no new rows for ${LAG}s (threshold ${STALE_AFTER}s). Inspecting the VM ..." >&2
gcloud compute ssh "$VM" --zone "$ZONE" --tunnel-through-iap --command='
    docker ps -a --format "{{.Names}} | {{.Status}}"
    docker inspect klend-indexer --format "restarts={{.RestartCount}} exit={{.State.ExitCode}}" 2>/dev/null
    echo "--- last 20 log lines ---"
    docker logs --tail 20 klend-indexer 2>&1' 2>&1 | grep -v "WARNING\|NumPy\|please see"

echo
echo "Recovery, in order of escalation:" >&2
echo "  1. sudo google_metadata_script_runner startup   # re-runs the boot path on the VM" >&2
echo "  2. gcloud compute instances reset ${VM} --zone ${ZONE}   # full reboot; resume is lossless" >&2
exit 1
