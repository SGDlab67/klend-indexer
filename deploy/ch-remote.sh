#!/usr/bin/env bash
# Run SQL against ClickHouse Cloud from the Mac, tunneled through the VM.
#
# The service's IP access list is a single entry: the VM. That was deliberate —
# ClickHouse holds the whole dataset and is billed per byte, so it should be
# reachable from exactly one machine. The consequence is that every Mac-side
# script that talked to :8443 directly (apply-schema-cloud.sh, health-check.sh)
# stopped working, silently, as a curl (28) timeout. The fix is not to reopen the
# allowlist; it is to go through the box that is already on it.
#
# Nothing about the credential changes shape: the password is fetched on the VM
# from Secret Manager using the instance's own service-account token, and handed
# to curl on fd 3. It is never in argv, never on disk, never on the Mac. Same
# rule as SECRETS.md, same mechanism as deploy/gce-startup.sh.
#
# Usage:
#   deploy/ch-remote.sh "SELECT count() FROM klend.account_updates FORMAT TSVRaw"
#   deploy/ch-remote.sh < query.sql
set -euo pipefail

# The VM is NOT in the active gcloud project. Every documented command that
# omitted --project resolved against gen-lang-client-0502946726 and failed with
# "resource not found"; pin it here so callers inherit the right one.
PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"
# The live hostname is operator config, not source: this repo is public and every
# other script here carries the placeholder. Keep the real one in deploy/local.env,
# which .gitignore's *.env rule already covers.
# shellcheck source=/dev/null
[ -f "$(dirname "${BASH_SOURCE[0]}")/local.env" ] && . "$(dirname "${BASH_SOURCE[0]}")/local.env"
HOST="${CLICKHOUSE_CLOUD_HOST:-YOUR_INSTANCE.REGION.gcp.clickhouse.cloud}"
PORT="${CLICKHOUSE_CLOUD_HTTPS_PORT:-8443}"
USER_="${CLICKHOUSE_USER:-default}"
SECRET="${CLICKHOUSE_SECRET_NAME:-klend-clickhouse-cloud-password}"

SQL="${1:-$(cat)}"
[ -n "${SQL//[[:space:]]/}" ] || { echo "error: empty SQL" >&2; exit 2; }

# Ship the statement base64-encoded. SQL is not secret, but it is full of quotes,
# newlines, and parentheses, and it has to survive the Mac shell, gcloud, ssh, and
# the remote shell intact. Encoding removes every quoting hazard in that chain.
SQL_B64="$(printf '%s' "$SQL" | base64 | tr -d '\n')"

# `bash -s` reads the script below from stdin, so the remote side needs no file
# and leaves nothing behind. -T suppresses the TTY: with one allocated, ssh
# interleaves its own chatter into the query output.
gcloud compute ssh "$VM" \
    --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
    --command 'bash -s' -- -T <<REMOTE
set -euo pipefail

META='http://metadata.google.internal/computeMetadata/v1'
hdr='Metadata-Flavor: Google'

# Collapse whitespace before grepping: the metadata server returns compact JSON
# but the Secret Manager API returns pretty-printed JSON, and a '"data":"' pattern
# matches nothing there. That mismatch is what produced the empty-password failure
# on 2026-08-05. Safe because base64 contains neither spaces nor newlines.
json() { tr -d ' \n'; }

PROJ="\$(curl -s -H "\$hdr" "\$META/project/project-id")"
TOKEN="\$(curl -s -H "\$hdr" "\$META/instance/service-accounts/default/token" \
    | json | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
[ -n "\$TOKEN" ] || { echo "FATAL: could not obtain service-account token" >&2; exit 1; }

PW="\$(curl -s -H "Authorization: Bearer \$TOKEN" \
    "https://secretmanager.googleapis.com/v1/projects/\${PROJ}/secrets/${SECRET}/versions/latest:access" \
    | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
[ -n "\$PW" ] || { echo "FATAL: secret '${SECRET}' resolved empty" >&2; exit 1; }

SQL="\$(printf '%s' '${SQL_B64}' | base64 -d)"

curl -sS --fail-with-body --max-time 60 --config /dev/fd/3 \
    "https://${HOST}:${PORT}/" --data-binary "\$SQL" \
    3<<<"user = \"${USER_}:\${PW}\""
REMOTE
