#!/usr/bin/env bash
# GCE boot startup-script for the klend-indexer container (Container-Optimized OS).
#
# Runs as root on EVERY boot, idempotently: fetches the two secrets from Secret
# Manager into a tmpfs env file, (re)launches the container with --restart always,
# then removes the env file. Restart-on-crash is Docker's job; restart-on-reboot is
# this script re-running. Resume-from-checkpoint makes every relaunch lossless.
#
# No secret lives in this script or in instance metadata. The script names secrets;
# the VM's service-account token (from the metadata server) authorizes the fetch, so
# the values exist only in Secret Manager and, for a few seconds at boot, in a
# root-only tmpfs file that never touches persistent disk. This is the GCP-native
# form of the SECRETS.md rule: references in the clear, values in the vault.
#
# Requires on the VM's service account: roles/secretmanager.secretAccessor and
# roles/artifactregistry.reader. COS ships curl, grep, cut, base64, and docker; no
# gcloud or python needed on the box.
set -euo pipefail

META='http://metadata.google.internal/computeMetadata/v1'
hdr='Metadata-Flavor: Google'

PROJECT="$(curl -s -H "$hdr" "$META/project/project-id")"

# Both JSON responses below are parsed by grep, so collapse whitespace FIRST.
# The metadata server returns compact JSON but the Secret Manager API returns
# PRETTY-PRINTED JSON (`"data": "<b64>"`, with a space after the colon). A
# `"data":"` pattern silently matches nothing there, and the 2026-08-05 failure
# was exactly that: both secrets resolved to the empty string, the indexer
# connected to ClickHouse with a blank password, and klickhouse reported the
# auth rejection as `protocol error: channel closed`. Stripping spaces and
# newlines is safe because base64 uses none of them.
json() { tr -d ' \n'; }

TOKEN="$(curl -s -H "$hdr" "$META/instance/service-accounts/default/token" \
    | json | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
[ -n "$TOKEN" ] || { echo "FATAL: could not obtain service-account token" >&2; exit 1; }

# sm <secret-name> -> plaintext value. The access response wraps the value as
# base64 under payload.data; extract with grep/cut/base64 so COS needs no jq/python.
# An empty result ABORTS rather than writing a blank env var: a secret that fails
# open is worse than one that fails closed, because the failure surfaces later,
# somewhere else, as a misleading error.
sm() {
    local val
    val="$(curl -s -H "Authorization: Bearer $TOKEN" \
        "https://secretmanager.googleapis.com/v1/projects/${PROJECT}/secrets/$1/versions/latest:access" \
        | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
    [ -n "$val" ] || { echo "FATAL: secret '$1' resolved empty" >&2; exit 1; }
    printf '%s' "$val"
}

# Fetch BEFORE writing the env file, into plain variables. sm()'s `exit 1` only
# leaves the subshell when it is called inside `$(...)` in an echo, and echo's own
# status is 0, so `set -e` would not see the failure and the env file would get a
# blank line written anyway. As separate assignments, `set -e` aborts the script.
GRPC_TOKEN_V="$(sm alchemy-grpc-token)"
CH_PASSWORD_V="$(sm klend-clickhouse-cloud-password)"

ENVF=/run/klend-indexer.env   # tmpfs on COS: never persisted to disk
umask 077
{
    echo "GRPC_URL=https://solana-mainnet.g.alchemy.com"
    echo "GRPC_TOKEN=${GRPC_TOKEN_V}"
    echo "CLICKHOUSE_URL=YOUR_INSTANCE.REGION.gcp.clickhouse.cloud:9440"
    echo "CLICKHOUSE_SECURE=1"
    echo "CLICKHOUSE_USER=default"
    echo "CLICKHOUSE_DATABASE=klend"
    echo "CLICKHOUSE_PASSWORD=${CH_PASSWORD_V}"
} > "$ENVF"

IMAGE="us-central1-docker.pkg.dev/${PROJECT}/klend/klend-indexer:latest"

# COS docker is NOT pre-configured for Artifact Registry, so authenticate it with
# the metadata service-account token (piped via stdin, never in argv). The token's
# ~1h TTL is ample for a boot pull. Requires roles/artifactregistry.reader on the SA.
# COS mounts / read-only, so point docker's config at a writable tmpfs dir first;
# docker login and pull both honour DOCKER_CONFIG.
export DOCKER_CONFIG=/run/klend-docker
mkdir -p "$DOCKER_CONFIG"
echo "$TOKEN" | docker login -u oauth2accesstoken --password-stdin https://us-central1-docker.pkg.dev

# Pull explicitly so a redeploy of the :latest tag actually takes effect. `docker
# run` alone reuses a cached image, so a rebuild-and-reset would look deployed while
# still running the old build.
docker pull "$IMAGE"

# `docker rm -f` returns before the name is actually released, so an immediate
# `docker run` can lose the race and die with "name is already in use" (exit 125).
# Under `set -e` that aborts the whole script and the indexer never comes up, which
# on the reboot path means silent downtime. Wait for the name to clear first.
docker rm -f klend-indexer 2>/dev/null || true
for _ in $(seq 1 30); do
    docker inspect klend-indexer >/dev/null 2>&1 || break
    sleep 1
done

docker run -d --name klend-indexer --restart always --env-file "$ENVF" "$IMAGE"

# Values are captured into the container config at run time, so the file is no
# longer needed; drop it to shrink the exposure window. A crash-restart reuses the
# stored env; a reboot re-runs this script.
rm -f "$ENVF"

# Install the ingest-freshness watchdog (defense-in-depth after the 2026-08-05
# freeze). Its script is carried in the `watchdog-script` instance metadata attribute
# so this file stays small; a systemd service (COS has no cron) runs it looping with
# Restart=always. Idempotent: re-runs on every boot, so a rebuilt watchdog ships by
# re-pushing the metadata attribute and resetting.
mkdir -p /var/lib/klend
curl -s -H "$hdr" "$META/instance/attributes/watchdog-script" > /var/lib/klend/watchdog.sh
chmod +x /var/lib/klend/watchdog.sh
cat > /etc/systemd/system/klend-watchdog.service <<'UNIT'
[Unit]
Description=klend-indexer ingest-freshness watchdog
After=docker.service
Requires=docker.service

[Service]
# COS mounts /var noexec, so invoke the script through bash (an exec-allowed
# interpreter reading the script as data) rather than exec'ing it directly.
ExecStart=/bin/bash /var/lib/klend/watchdog.sh
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable klend-watchdog.service
systemctl restart klend-watchdog.service
