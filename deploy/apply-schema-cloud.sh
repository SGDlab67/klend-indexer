#!/usr/bin/env bash
# Apply the schema to the hosted ClickHouse Cloud service over its HTTPS interface
# (:8443). Used because there is no local `clickhouse` binary (ClickHouse runs only
# in Docker locally), and curl needs nothing installed.
#
# The password is read from the macOS Keychain and passed to curl via a config file
# on a file descriptor, so it never appears in argv (visible to `ps`) or on disk.
# Run from the Mac once, before first deploy. See SECRETS.md.
#
# One-time setup:
#   security add-generic-password -a "$USER" -s klend-clickhouse-cloud-password -w
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

HOST="${CLICKHOUSE_CLOUD_HOST:-YOUR_INSTANCE.REGION.gcp.clickhouse.cloud}"
PORT="${CLICKHOUSE_CLOUD_HTTPS_PORT:-8443}"
USER_="${CLICKHOUSE_USER:-default}"
BASE="https://${HOST}:${PORT}/"

PW="$(security find-generic-password -a "$USER" -s klend-clickhouse-cloud-password -w 2>/dev/null)" || {
    echo "error: no keychain entry 'klend-clickhouse-cloud-password' for user '$USER'." >&2
    echo "store it first:  security add-generic-password -a \"\$USER\" -s klend-clickhouse-cloud-password -w" >&2
    exit 1
}

# Pass credentials on fd 3 via curl --config, so the secret is never in argv.
run() { # run <sql-or-@file-arg...>
    curl -sS --fail-with-body --config /dev/fd/3 "$BASE" "$@" \
        3<<<"user = \"${USER_}:${PW}\""
}

# The ClickHouse HTTP interface rejects multi-statement bodies, so send each
# statement separately. `--` comments are stripped FIRST (one of them contains a
# semicolon), leaving only statement-terminating ';' to split on. Blank chunks (e.g.
# the tail after the last ';') are skipped.
apply_file() { # apply_file <path>
    local f="$1"
    echo "applying $f ..."
    while IFS= read -r -d '' stmt; do
        if printf '%s' "$stmt" | grep -q '[^[:space:]]'; then
            run --data-binary "$stmt"
        fi
    done < <(sed 's/--.*$//' "$f" | awk 'BEGIN{RS=";"} {printf "%s%c", $0, 0}')
    echo "  ok"
}

apply_file schema/001_init.sql
apply_file schema/002_checkpoint.sql

echo "verifying ..."
echo -n "tables in klend: "
run --data-binary "SHOW TABLES FROM klend FORMAT TSVRaw"
echo -n "account_updates engine: "
run --data-binary "SELECT engine FROM system.tables WHERE database='klend' AND name='account_updates' FORMAT TSVRaw"

echo "done."
