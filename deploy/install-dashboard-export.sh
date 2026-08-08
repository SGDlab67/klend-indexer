#!/usr/bin/env bash
# Install the static dashboard generator on the VM as a systemd timer.
#
# WHY A GENERATOR AND NOT A SERVER
# The dashboard used to be an HTTP service on this VM, reachable from the
# internet. Even reduced to fixed read-only queries under a SELECT-only user,
# the residual risk was co-location: code execution in that process lands on the
# only host in ClickHouse's IP allowlist, next to a metadata server that hands
# out the VM service account, which reads Secret Manager, which holds the ADMIN
# ClickHouse password and the Alchemy token. Scoping the proxy's credential does
# nothing about the box's.
#
# This removes the exposure instead of guarding it. Nothing listens. The output
# is two files in a public bucket, so the reachable surface is object storage.
#
# COS CONSTRAINTS, both learned the hard way on 2026-08-05 (see NOTES.md):
#   1. There is no cron. The scheduler has to be a systemd timer.
#   2. /var is mounted noexec, so a script under /var/lib cannot be ExecStart'ed
#      directly (systemd fails with 203/EXEC). Run the interpreter and pass the
#      script as data.
#
# Usage: deploy/install-dashboard-export.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
BUCKET="${DASHBOARD_BUCKET:-klend-indexer-dashboard}"
RO_USER="${CH_READONLY_USER:-klend_ro}"
SECRET="${READONLY_SECRET_NAME:-klend-clickhouse-readonly-password}"
INTERVAL="${EXPORT_INTERVAL:-60}"

# shellcheck source=/dev/null
[ -f deploy/local.env ] && . deploy/local.env
CH_HOST="${CLICKHOUSE_CLOUD_HOST:-}"
[ -n "$CH_HOST" ] || { echo "error: CLICKHOUSE_CLOUD_HOST unset. Put it in deploy/local.env." >&2; exit 2; }

echo "==> publishing the dashboard page"
# From here, not from the VM. The page is a deploy artifact: it changes when the
# markup changes, which is a git event, not a 60-second event. Shipping it inside
# the image and re-copying it every run coupled a text edit to a container image
# rebuild, and the first version of that silently served a stale page because the
# worker never pulled.
gcloud storage cp web/index.html "gs://${BUCKET}/index.html" \
    --cache-control="public, max-age=60" --project "$PROJECT"

echo "==> installing dashboard export on ${VM} (every ${INTERVAL}s -> gs://${BUCKET})"

gcloud compute ssh "$VM" \
    --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
    --command 'sudo bash -s' -- -T <<REMOTE
set -euo pipefail

install -d -m 0755 /var/lib/klend

# The worker. Written as data under /var/lib (noexec is fine, systemd invokes
# bash and passes this path as an argument).
cat > /var/lib/klend/export-dashboard.sh <<'WORKER'
#!/bin/bash
set -euo pipefail

META='http://metadata.google.internal/computeMetadata/v1'
hdr='Metadata-Flavor: Google'

# Collapse whitespace before grepping: the metadata server returns compact JSON
# but Secret Manager returns pretty-printed JSON, and a '"data":"' pattern
# matches nothing there. That mismatch caused the 2026-08-05 empty-password
# failure. Safe because base64 has neither spaces nor newlines.
json() { tr -d ' \n'; }

PROJ="\$(curl -s -H "\$hdr" "\$META/project/project-id")"
TOKEN="\$(curl -s -H "\$hdr" "\$META/instance/service-accounts/default/token" \
    | json | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
[ -n "\$TOKEN" ] || { echo "FATAL: no service-account token" >&2; exit 1; }

PW="\$(curl -s -H "Authorization: Bearer \$TOKEN" \
    "https://secretmanager.googleapis.com/v1/projects/\${PROJ}/secrets/__SECRET__/versions/latest:access" \
    | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
# Fail closed. A blank credential surfaces later as a misleading error, which is
# exactly how the 2026-08-05 outage read as "channel closed" instead of "wrong
# password".
[ -n "\$PW" ] || { echo "FATAL: secret __SECRET__ resolved empty" >&2; exit 1; }

OUT=/run/klend-dashboard   # tmpfs: the payload never persists to disk
rm -rf "\$OUT"; mkdir -p "\$OUT"
# The image runs as the unprivileged 'node' user (uid 1000) and this bind mount
# is created root-owned, so the container cannot write its output without this.
# Chown rather than chmod 0777: the directory should be writable by the one uid
# that needs it, not by everything on the box.
chown 1000:1000 "\$OUT"

IMAGE="us-central1-docker.pkg.dev/\${PROJ}/klend/klend-proxy:latest"

# Credentials by --env-file on tmpfs, never on the command line: docker run args
# are visible in the process table to every user on the box.
ENVF=/run/klend-export.env
umask 077
{
    echo "CH_HOST=__CH_HOST__"
    echo "CH_PORT=8443"
    echo "CH_USER=__RO_USER__"
    echo "CH_PASSWORD=\$PW"
    echo "OUT_DIR=/out"
} > "\$ENVF"

docker run --rm --env-file "\$ENVF" -v "\$OUT":/out \
    --memory 128m \
    --entrypoint node "\$IMAGE" export.js
rm -f "\$ENVF"

# Upload with curl against the GCS XML API, not gcloud: Container-Optimized OS
# ships no gcloud, and pulling the cloud-sdk image every 60s to copy 4 KB would
# cost more than the payload. The same metadata token already fetched above
# authorizes this, so it adds no new dependency and no new credential.
#
# --fail-with-body matters. curl exits 0 on an HTTP 403 by default, which would
# make a permissions failure look like a successful publish and leave the last
# good stats.json in place: a dashboard that looks live while going stale, the
# same failure shape as a container reporting "Up" with a dead stream.
#
# Cache-Control is the whole point. Without it the bucket serves stats.json from
# cache and a page whose only claim is freshness quietly shows old numbers.
#
# Only stats.json is uploaded here. index.html is a deploy artifact that changes
# when the page changes, not every 60 seconds, so it is published separately by
# install-dashboard-export.sh and carries its own longer TTL.
put() {  # \$1 = local file, \$2 = object name, \$3 = content type, \$4 = cache-control
    curl -sS --fail-with-body --max-time 60 -X PUT \
        -H "Authorization: Bearer \$TOKEN" \
        -H "Content-Type: \$3" \
        -H "Cache-Control: \$4" \
        --data-binary @"\$1" \
        "https://storage.googleapis.com/__BUCKET__/\$2" >/dev/null
}

put "\$OUT/stats.json" stats.json 'application/json' 'no-cache, max-age=0'

rm -rf "\$OUT"
echo "published \$(date -Is)"
WORKER

# Substitute the deploy-time values. Done with sed on placeholders rather than
# by interpolating into the quoted heredoc above, because that heredoc is quoted
# precisely so the WORKER's own \$ variables survive intact.
sed -i \
    -e "s|__SECRET__|${SECRET}|g" \
    -e "s|__CH_HOST__|${CH_HOST}|g" \
    -e "s|__RO_USER__|${RO_USER}|g" \
    -e "s|__BUCKET__|${BUCKET}|g" \
    /var/lib/klend/export-dashboard.sh
chmod 0644 /var/lib/klend/export-dashboard.sh

# Pull the current image once, here, rather than in the worker. The worker runs
# every 60s and its image changes only on deploy, so a per-run pull would be
# 1440 manifest checks a day to learn nothing. Skipping the pull entirely is the
# other failure: docker run reuses the cached layer, a rebuilt :latest never
# takes effect, and the box keeps serving the old build while looking deployed.
# That is the 2026-08-05 bug, so this is deliberate rather than incidental.
export DOCKER_CONFIG=/run/klend-docker
mkdir -p "\$DOCKER_CONFIG"
TOKEN="\$(curl -s -H 'Metadata-Flavor: Google' \
    http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token \
    | tr -d ' \n' | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
PROJ="\$(curl -s -H 'Metadata-Flavor: Google' \
    http://metadata.google.internal/computeMetadata/v1/project/project-id)"
echo "\$TOKEN" | docker login -u oauth2accesstoken --password-stdin https://us-central1-docker.pkg.dev
docker pull "us-central1-docker.pkg.dev/\${PROJ}/klend/klend-proxy:latest"

cat > /etc/systemd/system/klend-dashboard.service <<'UNIT'
[Unit]
Description=Generate and publish the klend static dashboard
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
# /var is noexec on COS, so invoke bash and hand it the script as an argument.
# ExecStart=/var/lib/klend/export-dashboard.sh fails with 203/EXEC.
ExecStart=/bin/bash /var/lib/klend/export-dashboard.sh
UNIT

cat > /etc/systemd/system/klend-dashboard.timer <<UNIT
[Unit]
Description=Publish the klend dashboard every ${INTERVAL}s

[Timer]
OnBootSec=90
OnUnitActiveSec=${INTERVAL}
AccuracySec=5
Unit=klend-dashboard.service

[Install]
WantedBy=timers.target
UNIT

systemctl daemon-reload
systemctl enable --now klend-dashboard.timer

echo "--- running once now to verify ---"
systemctl start klend-dashboard.service || true
journalctl -u klend-dashboard.service -n 30 --no-pager | tail -25
REMOTE
