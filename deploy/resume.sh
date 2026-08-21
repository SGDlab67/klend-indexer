#!/usr/bin/env bash
# Resume after a freeze: start the ClickHouse service, relaunch the indexer via
# the boot startup-script, and re-enable the dashboard timer + watchdog.
#
# Reverse of deploy/freeze.sh. Safe to run at any time; the pieces are idempotent.
#
# Usage: deploy/resume.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
SERVICE_ID="${CLICKHOUSE_SERVICE_ID:-78f807ed-43c0-4aae-bf93-3a9a438160e6}"

svc_state() {  # -> running | idling | stopped | ...
  clickhousectl cloud service get "$SERVICE_ID" --json 2>/dev/null \
    | tr -d ' \n' | grep -o '"state":"[a-z]*"' | cut -d'"' -f4
}

remote() {  # run a root script on the VM over IAP; reads the script from stdin
  gcloud compute ssh "$VM" --project "$PROJECT" --zone "$ZONE" \
    --tunnel-through-iap --command 'sudo bash -s' -- -T
}

banner() { printf '\n==> %s\n' "$*"; }

# ─────────────────────────────────────────────────────────────────────────────
banner "[1/3] start the ClickHouse service"
clickhousectl cloud service start "$SERVICE_ID"
echo "    waiting for 'running'... (idle -> running can take a minute or two)"
for _ in $(seq 1 40); do
  s="$(svc_state)"
  [ "$s" = "running" ] && { echo "    state: running"; break; }
  sleep 15
done

# ─────────────────────────────────────────────────────────────────────────────
banner "[2/3] relaunch indexer + watchdog (re-runs the boot startup-script)"
remote <<'REMOTE'
set -euo pipefail
google_metadata_script_runner startup
echo "indexer: $(docker ps --filter name=klend-indexer --format '{{.Names}} {{.Status}}')"
echo "watchdog: $(systemctl is-active klend-watchdog.service)"
REMOTE

# ─────────────────────────────────────────────────────────────────────────────
banner "[3/3] re-enable dashboard timer"
remote <<'REMOTE'
set -euo pipefail
systemctl enable --now klend-dashboard.timer
echo "dashboard timer: $(systemctl is-active klend-dashboard.timer)"
REMOTE

banner "resume complete — verify the stream is advancing:"
echo "  ./deploy/ch-remote.sh \"SELECT max(slot), dateDiff('second', max(ingested_at), now64(3)) FROM klend.account_updates FORMAT TSVRaw\""
