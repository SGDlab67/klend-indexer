#!/usr/bin/env bash
# One-shot Parquet export of the klend dataset from ClickHouse Cloud.
# Runs ON the VM (only host on the CH IP allowlist). Fetches the CH password
# from Secret Manager into a tmpfs env file (never disk), runs the export
# container as a reader alongside the live indexer, then removes the env file.
set -euo pipefail

META='http://metadata.google.internal/computeMetadata/v1'
hdr='Metadata-Flavor: Google'
json() { tr -d ' \n'; }

TOKEN="$(curl -s -H "$hdr" "$META/instance/service-accounts/default/token" \
    | json | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
[ -n "$TOKEN" ] || { echo "FATAL: no service-account token" >&2; exit 1; }

PROJ="$(curl -s -H "$hdr" "$META/project/project-id")"

PW="$(curl -s -H "Authorization: Bearer ${TOKEN}" \
    "https://secretmanager.googleapis.com/v1/projects/${PROJ}/secrets/klend-clickhouse-cloud-password/versions/latest:access" \
    | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
[ -n "$PW" ] || { echo "FATAL: secret klend-clickhouse-cloud-password resolved empty" >&2; exit 1; }

ENVF=/run/klend-export.env
umask 077
{
    echo "CLICKHOUSE_URL=um7rnv0cif.us-central1.gcp.clickhouse.cloud:9440"
    echo "CLICKHOUSE_SECURE=1"
    echo "CLICKHOUSE_USER=default"
    echo "CLICKHOUSE_DATABASE=klend"
    echo "CLICKHOUSE_PASSWORD=${PW}"
} > "$ENVF"

echo "==> env file written to $ENVF (tmpfs); launching export"
docker run --rm \
    --entrypoint /usr/local/bin/klend-parquet-export \
    --env-file "$ENVF" \
    -e KLEND_PARQUET_DIR=/out \
    -v /var/klend/parquet:/out \
    us-central1-docker.pkg.dev/agentbiz-sungodlab/klend/klend-indexer:latest
rc=$?
rm -f "$ENVF"
echo "==> export exit code: $rc"
exit "$rc"
