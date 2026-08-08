#!/usr/bin/env bash
# Build and (re)deploy the read-only dashboard proxy on the VM.
#
# The proxy is the only network-reachable component in this system, so its
# deployment carries constraints the indexer's does not:
#
#   - It authenticates as klend_ro (SELECT on klend.* only), never `default`.
#     web/proxy.js refuses to boot otherwise. See deploy/create-readonly-user.sh.
#   - The ClickHouse hostname is passed in, never baked into the image, because
#     the image is built from a public repo.
#   - The password is fetched on the VM from Secret Manager using the instance
#     service-account token and written to a tmpfs env file that docker reads.
#     It never touches the Mac, argv, or persistent disk. Same mechanism as
#     deploy/gce-startup.sh and deploy/run-snapshot.sh.
#
# Usage:
#   deploy/run-proxy.sh            # build the image, then redeploy
#   SKIP_BUILD=1 deploy/run-proxy.sh   # redeploy the current :latest
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
RO_USER="${CH_READONLY_USER:-klend_ro}"
SECRET="${READONLY_SECRET_NAME:-klend-clickhouse-readonly-password}"

# Operator-local config, gitignored. Holds CLICKHOUSE_CLOUD_HOST.
# shellcheck source=/dev/null
[ -f deploy/local.env ] && . deploy/local.env
CH_HOST="${CLICKHOUSE_CLOUD_HOST:-}"
[ -n "$CH_HOST" ] || {
    echo "error: CLICKHOUSE_CLOUD_HOST is unset. Put it in deploy/local.env." >&2
    exit 2
}

if [ -z "${SKIP_BUILD:-}" ]; then
    echo "==> building klend-proxy image"
    gcloud builds submit --project "$PROJECT" --config cloudbuild.proxy.yaml .
fi

echo "==> redeploying klend-proxy on ${VM}"
gcloud compute ssh "$VM" \
    --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
    --command 'sudo bash -s' -- -T <<REMOTE
set -euo pipefail

META='http://metadata.google.internal/computeMetadata/v1'
hdr='Metadata-Flavor: Google'

# Collapse whitespace before grepping: the metadata server returns compact JSON
# but Secret Manager returns pretty-printed JSON, and a '"data":"' pattern
# matches nothing there. That mismatch caused the empty-password failure on
# 2026-08-05. Safe because base64 contains neither spaces nor newlines.
json() { tr -d ' \n'; }

PROJ="\$(curl -s -H "\$hdr" "\$META/project/project-id")"
TOKEN="\$(curl -s -H "\$hdr" "\$META/instance/service-accounts/default/token" \
    | json | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
[ -n "\$TOKEN" ] || { echo "FATAL: could not obtain service-account token" >&2; exit 1; }

PW="\$(curl -s -H "Authorization: Bearer \$TOKEN" \
    "https://secretmanager.googleapis.com/v1/projects/\${PROJ}/secrets/${SECRET}/versions/latest:access" \
    | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
# Fail closed. A blank credential surfaces later as a misleading error; that is
# exactly how the 2026-08-05 outage read as "channel closed" rather than
# "wrong password".
[ -n "\$PW" ] || { echo "FATAL: secret '${SECRET}' resolved empty" >&2; exit 1; }

ENVF=/run/klend-proxy.env   # tmpfs on COS: never persisted to disk
umask 077
{
    echo "CH_HOST=${CH_HOST}"
    echo "CH_PORT=8443"
    echo "CH_USER=${RO_USER}"
    echo "CH_PASSWORD=\${PW}"
    echo "LISTEN_PORT=8080"
} > "\$ENVF"

IMAGE="us-central1-docker.pkg.dev/\${PROJ}/klend/klend-proxy:latest"

# COS docker is NOT pre-configured for Artifact Registry, so authenticate with
# the same service-account token (stdin, never argv). Without this the pull
# fails as "Unauthenticated request". COS mounts / read-only, so point docker's
# config at a writable tmpfs dir; login and pull both honour DOCKER_CONFIG.
export DOCKER_CONFIG=/run/klend-docker
mkdir -p "\$DOCKER_CONFIG"
echo "\$TOKEN" | docker login -u oauth2accesstoken --password-stdin https://us-central1-docker.pkg.dev

# Pull explicitly so a redeploy of the :latest tag actually takes effect. `docker
# run` alone reuses the cached image, so a rebuild would look deployed while
# still serving the old build.
docker pull "\$IMAGE"

# `docker rm -f` returns before the name is released, so an immediate `docker
# run` can lose the race and die with "name is already in use" (exit 125).
docker rm -f klend-proxy 2>/dev/null || true
for _ in \$(seq 1 30); do
    docker inspect klend-proxy >/dev/null 2>&1 || break
    sleep 1
done

# --restart always so a crash or reboot brings it back. The memory cap keeps the
# proxy from ever competing with the indexer for the box's 966 MB; the indexer
# is the irreplaceable process here and the dashboard is not.
docker run -d --name klend-proxy --restart always \
    --env-file "\$ENVF" \
    -p 8080:8080 \
    --memory 128m \
    "\$IMAGE"

rm -f "\$ENVF"
sleep 2
docker ps --format '{{.Names}} | {{.Status}}' | grep klend || true
echo "--- proxy logs ---"
docker logs --tail 15 klend-proxy 2>&1
REMOTE
