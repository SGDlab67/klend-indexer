#!/usr/bin/env bash
# Launch klend-indexer with the Alchemy key pulled from the macOS Keychain.
#
# This file contains a REFERENCE to the secret, never the secret itself,
# so it is safe to commit. See SECRETS.md.
#
# One-time setup:
#   security add-generic-password -a "$USER" -s alchemy-grpc-token -w
set -euo pipefail

# Bare host — Alchemy takes the key in the x-token header, never in the URL path.
export GRPC_URL="${GRPC_URL:-https://solana-mainnet.g.alchemy.com}"

if ! TOKEN="$(security find-generic-password -a "$USER" -s alchemy-grpc-token -w 2>/dev/null)"; then
    echo "error: no keychain entry 'alchemy-grpc-token' for user '$USER'." >&2
    echo "store it first:  security add-generic-password -a \"\$USER\" -s alchemy-grpc-token -w" >&2
    exit 1
fi

# `VAR=value command` scopes the secret to this one child process — it is NOT
# exported into the shell session, so it cannot leak into later commands.
GRPC_TOKEN="$TOKEN" cargo run "$@"
