#!/usr/bin/env bash
# Launch klend-indexer with secrets pulled from the macOS Keychain.
#
# This file contains REFERENCES to secrets, never the secrets themselves,
# so it is safe to commit. See SECRETS.md.
#
# One-time setup:
#   security add-generic-password -a "$USER" -s alchemy-grpc-token -w
#   security add-generic-password -a "$USER" -s klend-clickhouse-password -w
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# Bare host — Alchemy takes the key in the x-token header, never in the URL path.
export GRPC_URL="${GRPC_URL:-https://solana-mainnet.g.alchemy.com}"

# ClickHouse target the indexer writes to. Native protocol, loopback only.
# Set CLICKHOUSE_URL="" to run sampling-only (no writes, ClickHouse not needed) —
# the stdout path and KLEND_SAMPLE_SLOTS bound still work. Uses `-` not `:-` so an
# explicit empty value is honoured, not overridden by the default.
export CLICKHOUSE_URL="${CLICKHOUSE_URL-127.0.0.1:9000}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-klend}"
export CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-klend}"

fetch() { # fetch <keychain-service>
    security find-generic-password -a "$USER" -s "$1" -w 2>/dev/null || {
        echo "error: no keychain entry '$1' for user '$USER'." >&2
        echo "store it first:  security add-generic-password -a \"\$USER\" -s $1 -w" >&2
        exit 1
    }
}

TOKEN="$(fetch alchemy-grpc-token)"

# Only the write path needs the ClickHouse password. Sampling-only skips it, so a
# missing Keychain entry never blocks a pure bandwidth sample.
CHPW=""
[ -n "$CLICKHOUSE_URL" ] && CHPW="$(fetch klend-clickhouse-password)"

# `VAR=value command` scopes each secret to this one child process — it is NOT
# exported into the shell session, so it cannot leak into later commands.
GRPC_TOKEN="$TOKEN" CLICKHOUSE_PASSWORD="$CHPW" cargo run "$@"
