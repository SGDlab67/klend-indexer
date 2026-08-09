#!/usr/bin/env bash
# Install the ingest-freshness watchdog on the indexer VM.
#
# Why this file exists at all: `deploy/gce-startup.sh` already contains watchdog
# install logic (lines ~105-131), reading the script from the `watchdog-script`
# instance metadata attribute. That logic has never run. The instance carries a
# `watchdog-script` metadata key but NO `startup-script` key, so the file that
# would consume it is never executed. Audited 2026-08-09: no
# /var/lib/klend/watchdog.sh, no klend-watchdog.service, and the last
# google-startup-scripts run (Aug 5) was the default no-op.
#
# Consequence, stated plainly: the guard written for the 2026-08-05 freeze — the
# incident that cost 8h40m of unrecoverable history while the container reported
# "Up" — has never been running. `--restart always` cannot see that failure,
# because Docker never considered the container unhealthy.
#
# This installer does the job directly rather than depending on the startup
# script being wired up, and is idempotent so it can be re-run.
#
# Same three COS constraints as the dashboard installer, all previously paid for:
# no cron (so a self-looping systemd service), /var is noexec (so the unit runs
# bash with the script as data), and the metadata pipeline eats quotes.
#
# Usage: deploy/install-watchdog.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
RO_USER="${CH_READONLY_USER:-klend_ro}"
SECRET="${READONLY_SECRET_NAME:-klend-clickhouse-readonly-password}"

# shellcheck source=/dev/null
[ -f deploy/local.env ] && . deploy/local.env
CH_HOST="${CLICKHOUSE_CLOUD_HOST:-}"
[ -n "$CH_HOST" ] || { echo "error: CLICKHOUSE_CLOUD_HOST unset. Put it in deploy/local.env." >&2; exit 2; }

# Render the operator values into the script before it leaves the workstation.
# The repo copy keeps placeholders because it is public; the live hostname is
# operator config, the same rule ch-remote.sh and install-dashboard-export.sh
# already follow.
RENDERED="$(mktemp)"
trap 'rm -f "$RENDERED"' EXIT
sed -e "s|__CH_HOST__|${CH_HOST}|g" \
    -e "s|__RO_USER__|${RO_USER}|g" \
    -e "s|__SECRET__|${SECRET}|g" \
    deploy/watchdog.sh > "$RENDERED"

# Fail loudly if any placeholder survived. A watchdog that runs but silently
# cannot reach ClickHouse reports "freshness query failed; not acting" forever,
# which reads like a working guard and is not one — the same failure shape as a
# container reporting "Up" with a dead stream.
if grep -q '__[A-Z_]*__' "$RENDERED"; then
    echo "error: unsubstituted placeholder remains in rendered watchdog:" >&2
    grep -o '__[A-Z_]*__' "$RENDERED" | sort -u >&2
    exit 3
fi

echo "==> shipping watchdog to ${VM} metadata"
# Instance metadata, not scp: it survives a VM rebuild, and gce-startup.sh
# already expects to find the script under this exact key. Keeping the key means
# that path starts working for free once startup-script is attached.
gcloud compute instances add-metadata "$VM" \
    --project "$PROJECT" --zone "$ZONE" \
    --metadata-from-file "watchdog-script=${RENDERED}"

echo "==> installing systemd unit on ${VM}"
gcloud compute ssh "$VM" \
    --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
    --command 'sudo bash -s' <<'REMOTE'
set -euo pipefail

META='http://metadata.google.internal/computeMetadata/v1'
HDR='Metadata-Flavor: Google'

mkdir -p /var/lib/klend
curl -s -H "$HDR" "$META/instance/attributes/watchdog-script" > /var/lib/klend/watchdog.sh

# An empty or truncated fetch would install a unit that exits instantly and
# restarts forever, which looks like "enabled" in systemctl.
if [ ! -s /var/lib/klend/watchdog.sh ]; then
    echo "FATAL: watchdog-script metadata fetched empty" >&2
    exit 1
fi
chmod +x /var/lib/klend/watchdog.sh

# Restart=always, not a timer. The script owns its own loop (COS has no cron),
# so systemd's job is only to keep the loop alive across its own crashes.
# ExecStart runs bash with the script as an ARGUMENT because /var is mounted
# noexec on COS — the shebang alone would fail with EACCES.
cat > /etc/systemd/system/klend-watchdog.service <<'UNIT'
[Unit]
Description=klend-indexer ingest-freshness watchdog
After=docker.service
Requires=docker.service

[Service]
Type=simple
ExecStart=/bin/bash /var/lib/klend/watchdog.sh
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable klend-watchdog.service
systemctl restart klend-watchdog.service
sleep 3
systemctl is-active klend-watchdog.service
REMOTE

echo
echo "==> first log lines (expect 'started' then 'fresh (Ns)')"
gcloud compute ssh "$VM" \
    --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
    --command 'sudo journalctl -u klend-watchdog -n 15 --no-pager'

cat <<'NEXT'

Installed. To verify it actually guards rather than merely runs:

    # induce a real stall (the indexer cannot write while paused)
    gcloud compute ssh klend-indexer --project agentbiz-sungodlab \
        --zone us-central1-a --tunnel-through-iap \
        --command 'sudo docker pause klend-indexer'

    # wait past STALE_THRESHOLD + INTERVAL (~20 min worst case), then:
    #   journalctl -u klend-watchdog  -> "STALE ...s; restarting klend-indexer"
    #   docker logs klend-indexer     -> "resuming from checkpoint slot=N (inclusive)"
    #
    # No data is lost: 15-20 min is ~2250-3000 slots, inside the ~6000-slot
    # replay window, so the stream re-serves the pause. If the pause ever
    # outlasts that window, the gap becomes real and gets recorded.
NEXT
