#!/usr/bin/env bash
# Launch the official ClickHouse MCP server against the local klend database.
#
# WHY THIS SCRIPT EXISTS AT ALL:
# An MCP server is normally configured by inlining `env` into a JSON config
# file. That would write the ClickHouse password to disk in plaintext — in
# ~/.claude.json, or worse in a committed .mcp.json. Same reasoning as run.sh:
# the file holds a REFERENCE to the secret, never the value, so it is safe to
# commit and safe to share. See SECRETS.md.
set -euo pipefail

# The READ-ONLY account, not the indexer's `klend` account. The MCP server's own
# read-only mode is a session setting it applies to itself — the client
# cooperating, not the database refusing. `klend_ro` holds SELECT and nothing
# else, so a write is rejected by the server regardless of how this process is
# configured. Created idempotently by `./ch.sh up`.
KEYCHAIN_SERVICE="klend-clickhouse-ro-password"

if ! CLICKHOUSE_PASSWORD="$(security find-generic-password -a "$USER" -s "$KEYCHAIN_SERVICE" -w 2>/dev/null)"; then
    # stderr, because stdout on an MCP stdio server is the JSON-RPC channel —
    # a stray echo there corrupts the protocol rather than printing a message.
    echo "error: no keychain entry '$KEYCHAIN_SERVICE' for user '$USER'." >&2
    echo "  security add-generic-password -a \"\$USER\" -s $KEYCHAIN_SERVICE -w" >&2
    exit 1
fi
export CLICKHOUSE_PASSWORD

export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-klend_ro}"
export CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-klend}"

# Plain HTTP is correct here and ONLY here: docker-compose.yml publishes 8123 on
# 127.0.0.1, so this connection never leaves the machine. Pointing this script at
# any non-loopback host — ClickHouse Cloud included — requires
# CLICKHOUSE_SECURE=true and port 8443. The default must be the safe-by-locality
# one, so a copy-paste to a remote host fails loudly instead of silently
# shipping a password in the clear.
export CLICKHOUSE_SECURE="${CLICKHOUSE_SECURE:-false}"
export CLICKHOUSE_VERIFY="${CLICKHOUSE_VERIFY:-true}"

# READ-ONLY. This is the package default; setting it explicitly makes the choice
# visible and means a future default change cannot silently grant write access.
#
# Deliberate: the indexer is the only thing that should ever write to these
# tables. An agent that can DROP TABLE during exploration is a data-loss path,
# and this data is not re-fetchable — Yellowstone serves only the tip, so a
# dropped historical slot is gone permanently.
export CLICKHOUSE_ALLOW_WRITE_ACCESS="${CLICKHOUSE_ALLOW_WRITE_ACCESS:-false}"

# `exec` replaces this shell with the server rather than forking it. Without it
# this process lingers as a parent, and the MCP client's shutdown signal lands
# here instead of on the server — the same orphaned-child shape that cost two
# corrupted measurements on day 1.
#
# Version PINNED: uvx would otherwise resolve the newest release on every launch,
# meaning the tool surface can change under a session with no local diff to show
# for it.
exec uvx mcp-clickhouse@0.4.1
