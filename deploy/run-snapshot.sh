#!/usr/bin/env bash
# Run the one-shot RPC snapshot on the VM (Phase 2, Option A).
#
# It has to run there: ClickHouse Cloud's IP access list admits the VM and nothing
# else, and reopening it for a one-off job would trade a deliberate security
# decision for convenience. Same reasoning as deploy/ch-remote.sh.
#
# Leaves the running indexer container alone. This pulls the image and starts a
# SEPARATE, disposable container, so a snapshot never costs stream downtime.
#
# Usage:
#   deploy/run-snapshot.sh            # write the snapshot
#   DRY_RUN=1 deploy/run-snapshot.sh  # fetch and decode, write nothing
set -euo pipefail

PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
SCOPE="${KLEND_SNAPSHOT_SCOPE:-known}"
DRY_RUN="${DRY_RUN:-}"

# The live ClickHouse hostname is operator config, not source. This repo is
# public and every other script here carries the placeholder, so keep the real
# one in deploy/local.env, which .gitignore's *.env rule already covers.
# shellcheck source=/dev/null
[ -f "$(dirname "${BASH_SOURCE[0]}")/local.env" ] && . "$(dirname "${BASH_SOURCE[0]}")/local.env"
CH_HOST="${CLICKHOUSE_CLOUD_HOST:-YOUR_INSTANCE.REGION.gcp.clickhouse.cloud}"

gcloud compute ssh "$VM" \
    --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
    --command 'sudo bash -s' -- -T <<REMOTE
set -euo pipefail

META='http://metadata.google.internal/computeMetadata/v1'
hdr='Metadata-Flavor: Google'

# Collapse whitespace before grepping. The metadata server returns compact JSON,
# the Secret Manager API returns pretty-printed JSON, and a '"data":"' pattern
# matches nothing against the latter. That mismatch produced the empty-password
# failure on 2026-08-05. Safe because base64 has no spaces or newlines.
json() { tr -d ' \n'; }

PROJ="\$(curl -s -H "\$hdr" "\$META/project/project-id")"
TOKEN="\$(curl -s -H "\$hdr" "\$META/instance/service-accounts/default/token" \
    | json | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
[ -n "\$TOKEN" ] || { echo "FATAL: could not obtain service-account token" >&2; exit 1; }

# Fail closed on an empty secret. A blank credential surfaces later, somewhere
# else, as a misleading error; that is exactly how the 2026-08-05 outage read as
# "channel closed" rather than "wrong password".
sm() {
    local val
    val="\$(curl -s -H "Authorization: Bearer \$TOKEN" \
        "https://secretmanager.googleapis.com/v1/projects/\${PROJ}/secrets/\$1/versions/latest:access" \
        | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
    [ -n "\$val" ] || { echo "FATAL: secret '\$1' resolved empty" >&2; exit 1; }
    printf '%s' "\$val"
}

# Separate assignments, not inlined into the heredoc below: sm()'s 'exit 1' only
# leaves the subshell when called inside \$(...), and echo's own status is 0, so
# 'set -e' would not see the failure and a blank value would be written anyway.
GRPC_TOKEN_V="\$(sm alchemy-grpc-token)"
CH_PASSWORD_V="\$(sm klend-clickhouse-cloud-password)"

ENVF=/run/klend-snapshot.env   # tmpfs on COS: never persisted to disk
umask 077
{
    echo "GRPC_URL=https://solana-mainnet.g.alchemy.com"
    echo "GRPC_TOKEN=\${GRPC_TOKEN_V}"
    echo "CLICKHOUSE_URL=${CH_HOST}:9440"
    echo "CLICKHOUSE_SECURE=1"
    echo "CLICKHOUSE_USER=default"
    echo "CLICKHOUSE_DATABASE=klend"
    echo "CLICKHOUSE_PASSWORD=\${CH_PASSWORD_V}"
    echo "KLEND_SNAPSHOT_SCOPE=${SCOPE}"
    # Unquoted on purpose. This line is expanded by the LOCAL shell inside an
    # unquoted heredoc, where \" is not an escape and survives as a literal
    # backslash-quote. Writing echo \"KLEND_SNAPSHOT_DRY_RUN=1\" therefore emitted
    # "KLEND_SNAPSHOT_DRY_RUN=1" WITH the quotes into the env file, docker read the
    # variable name as \`"KLEND_SNAPSHOT_DRY_RUN\`, and the dry run wrote for real.
    ${DRY_RUN:+echo KLEND_SNAPSHOT_DRY_RUN=1}
} > "\$ENVF"

IMAGE="us-central1-docker.pkg.dev/\${PROJ}/klend/klend-indexer:latest"

# COS docker is not pre-configured for Artifact Registry, and / is read-only, so
# point docker's config at a writable tmpfs dir before logging in.
export DOCKER_CONFIG=/run/klend-snapshot-docker
mkdir -p "\$DOCKER_CONFIG"
echo "\$TOKEN" | docker login -u oauth2accesstoken --password-stdin https://us-central1-docker.pkg.dev >/dev/null
docker pull "\$IMAGE"

# --rm because this is one-shot; a stopped snapshot container is only clutter, and
# its provenance lives in klend.snapshot_runs rather than in docker's state.
docker run --rm --name klend-snapshot \
    --env-file "\$ENVF" \
    --entrypoint /usr/local/bin/klend-snapshot \
    "\$IMAGE"

rm -f "\$ENVF"
REMOTE
