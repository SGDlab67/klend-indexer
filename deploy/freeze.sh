#!/usr/bin/env bash
# Freeze klend-indexer after the demo.
#
# What "freeze" means: the live indexer has accumulated klend history that only
# exists in ClickHouse Cloud, and the Cloud credit is running down. This script
# (1) exports the full dataset to Parquet in GCS so the history survives the
# service going away, (2) stops the indexer + watchers, and (3) idles the
# ClickHouse service so compute billing stops. Storage in both GCS and the idled
# service is preserved; deploy/resume.sh brings everything back.
#
# Run from the Mac, AFTER the demo. Idempotent: re-running is safe.
#
# Usage: deploy/freeze.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
SERVICE_ID="${CLICKHOUSE_SERVICE_ID:-78f807ed-43c0-4aae-bf93-3a9a438160e6}"
BUCKET="${DASHBOARD_BUCKET:-klend-indexer-dashboard}"
IMAGE="us-central1-docker.pkg.dev/${PROJECT}/klend/klend-indexer:latest"

# The live ClickHouse hostname is operator config (deploy/local.env), not
# committed source. Same rule as ch-remote.sh / install-dashboard-export.sh.
# shellcheck source=/dev/null
[ -f deploy/local.env ] && . deploy/local.env
CH_HOST="${CLICKHOUSE_CLOUD_HOST:-}"
[ -n "$CH_HOST" ] || { echo "error: CLICKHOUSE_CLOUD_HOST unset. Put it in deploy/local.env." >&2; exit 2; }

svc_state() {  # -> running | idling | stopped | ... (no jq/python dependency)
  clickhousectl cloud service get "$SERVICE_ID" --json 2>/dev/null \
    | tr -d ' \n' | grep -o '"state":"[a-z]*"' | cut -d'"' -f4
}

remote() {  # run a root script on the VM over IAP; reads the script from stdin
  gcloud compute ssh "$VM" --project "$PROJECT" --zone "$ZONE" \
    --tunnel-through-iap --command 'sudo bash -s' -- -T
}

banner() { printf '\n==> %s\n' "$*"; }

# ─────────────────────────────────────────────────────────────────────────────
banner "[1/7] preflight"
STATE="$(svc_state)"
echo "    ClickHouse service: $STATE (id $SERVICE_ID)"
if [ "$STATE" != "running" ]; then
  echo "    NOTE: service is not 'running'. The export still works if storage is live,"
  echo "    but there is no compute to stop. Proceeding."
fi
gcloud storage ls "gs://${BUCKET}/" --project "$PROJECT" >/dev/null 2>&1 \
  || { echo "FATAL: bucket gs://${BUCKET} not reachable" >&2; exit 1; }
echo "    bucket gs://${BUCKET}/ OK"

# ─────────────────────────────────────────────────────────────────────────────
banner "[2/7] stop watchdog + indexer (freeze the stream)"
remote <<'REMOTE'
set -euo pipefail
systemctl stop klend-watchdog.service 2>/dev/null || true
systemctl disable klend-watchdog.service 2>/dev/null || true
docker stop klend-indexer 2>/dev/null || true
# --restart always only relaunches on crash/reboot, but drop it anyway so a
# stray reboot cannot silently unfreeze the box. resume.sh re-creates it.
docker update --restart=no klend-indexer 2>/dev/null || true
sleep 3
echo "watchdog stopped; indexer: $(docker inspect -f '{{.State.Status}}' klend-indexer 2>/dev/null || echo absent)"
REMOTE

# ─────────────────────────────────────────────────────────────────────────────
banner "[3/7] capture frozen numbers (ClickHouse is still up)"
FROZEN="$(./deploy/ch-remote.sh "SELECT max(slot), count(), max(ingested_at), (SELECT count(DISTINCT pubkey) FROM klend.obligation_snapshots FINAL), (SELECT count() FROM klend.obligation_snapshots FINAL) FROM klend.account_updates FORMAT TSVRaw" 2>/dev/null)"
LAST_SLOT="$(echo "$FROZEN" | cut -f1)"
ROWS="$(echo "$FROZEN" | cut -f2)"
LAST_INGEST="$(echo "$FROZEN" | cut -f3)"
OBLIGATIONS="$(echo "$FROZEN" | cut -f4)"
SNAPSHOTS="$(echo "$FROZEN" | cut -f5)"
FROZEN_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "    last slot $LAST_SLOT · $ROWS rows · $OBLIGATIONS obligations · $SNAPSHOTS snapshots"
echo "    last ingest $LAST_INGEST · frozen_at $FROZEN_AT"

# ─────────────────────────────────────────────────────────────────────────────
banner "[4/7] full Parquet export -> gs://${BUCKET}/klend-parquet/"
remote <<REMOTE
set -euo pipefail
META='http://metadata.google.internal/computeMetadata/v1'
hdr='Metadata-Flavor: Google'
json() { tr -d ' \n'; }

PROJ="\$(curl -s -H "\$hdr" "\$META/project/project-id")"
TOKEN="\$(curl -s -H "\$hdr" "\$META/instance/service-accounts/default/token" \
  | json | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
[ -n "\$TOKEN" ] || { echo "FATAL: no service-account token" >&2; exit 1; }

PW="\$(curl -s -H "Authorization: Bearer \$TOKEN" \
  "https://secretmanager.googleapis.com/v1/projects/\${PROJ}/secrets/klend-clickhouse-cloud-password/versions/latest:access" \
  | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
[ -n "\$PW" ] || { echo "FATAL: secret resolved empty" >&2; exit 1; }

ENVF=/run/klend-freeze-export.env
umask 077
{
  echo "CLICKHOUSE_URL=${CH_HOST}:9440"
  echo "CLICKHOUSE_SECURE=1"
  echo "CLICKHOUSE_USER=default"
  echo "CLICKHOUSE_DATABASE=klend"
  echo "CLICKHOUSE_PASSWORD=\${PW}"
  echo "KLEND_PARQUET_FULL=1"
} > "\$ENVF"

echo "==> running full parquet export (KLEND_PARQUET_FULL=1)"
docker run --rm \
  --entrypoint /usr/local/bin/klend-parquet-export \
  --env-file "\$ENVF" \
  -e KLEND_PARQUET_DIR=/out \
  -v /var/klend/parquet:/out \
  ${IMAGE}
rc=\$?
rm -f "\$ENVF"
[ "\$rc" -eq 0 ] || { echo "FATAL: export failed (exit \$rc)" >&2; exit "\$rc"; }

echo "==> uploading /var/klend/parquet -> gs://${BUCKET}/klend-parquet/"
find /var/klend/parquet -type f | while read -r f; do
  rel="\${f#/var/klend/parquet/}"
  curl -sS --fail-with-body --max-time 120 -X PUT \
    -H "Authorization: Bearer \$TOKEN" \
    -H "Content-Type: application/octet-stream" \
    --data-binary @"\$f" \
    "https://storage.googleapis.com/${BUCKET}/klend-parquet/\${rel}" >/dev/null
done
echo "==> upload complete"
REMOTE

# ─────────────────────────────────────────────────────────────────────────────
banner "[5/7] final dashboard refresh + freeze manifest"
remote <<'REMOTE'
set -euo pipefail
systemctl start klend-dashboard.service 2>/dev/null || true
sleep 5
echo "final stats.json published"
REMOTE

MANIFEST="$(mktemp)"
cat > "$MANIFEST" <<EOF
{
  "frozen_at": "$FROZEN_AT",
  "service_id": "$SERVICE_ID",
  "last_slot": "$LAST_SLOT",
  "account_updates_rows": "$ROWS",
  "obligations_distinct": "$OBLIGATIONS",
  "obligation_snapshots_rows": "$SNAPSHOTS",
  "last_ingested_at": "$LAST_INGEST",
  "parquet_prefix": "gs://${BUCKET}/klend-parquet/",
  "stats": "gs://${BUCKET}/stats.json"
}
EOF
gcloud storage cp "$MANIFEST" "gs://${BUCKET}/freeze-manifest.json" --project "$PROJECT"
rm -f "$MANIFEST"
echo "    wrote gs://${BUCKET}/freeze-manifest.json"

# ─────────────────────────────────────────────────────────────────────────────
banner "[6/7] stop dashboard timer"
remote <<'REMOTE'
set -euo pipefail
systemctl stop klend-dashboard.timer klend-dashboard.service 2>/dev/null || true
systemctl disable klend-dashboard.timer 2>/dev/null || true
echo "dashboard timer stopped; stats.json is frozen at its last refresh"
REMOTE

# ─────────────────────────────────────────────────────────────────────────────
banner "[7/7] idle the ClickHouse service (stop compute billing)"
clickhousectl cloud service stop "$SERVICE_ID"
echo "    waiting for state to leave 'running'..."
for _ in $(seq 1 20); do
  s="$(svc_state)"
  [ "$s" != "running" ] && { echo "    service state: $s"; break; }
  sleep 10
done

# ─────────────────────────────────────────────────────────────────────────────
banner "freeze complete"
echo "  Parquet:   gs://${BUCKET}/klend-parquet/  (account_updates + obligation_snapshots)"
echo "  Manifest:  gs://${BUCKET}/freeze-manifest.json"
echo "  Dashboard: gs://${BUCKET}/index.html + stats.json (frozen; the liveness dot"
echo "             going stale is the honest signal, not a bug)"
echo "  Service:   $STATE -> $(svc_state)  (data preserved; deploy/resume.sh to bring back)"
echo "  Last data: slot $LAST_SLOT @ $LAST_INGEST · $ROWS rows · $OBLIGATIONS obligations"
