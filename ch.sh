#!/usr/bin/env bash
# ClickHouse control script — password pulled from the macOS Keychain.
#
# Same discipline as run.sh: this file contains a REFERENCE to the secret, never
# the secret itself, so it is safe to commit. See SECRETS.md.
#
# One-time setup:
#   security add-generic-password -a "$USER" -s klend-clickhouse-password -w
#
# Usage:
#   ./ch.sh up          start ClickHouse, wait until it actually answers queries
#   ./ch.sh down        stop it (data survives — it lives in a named volume)
#   ./ch.sh nuke        stop AND destroy the volume; the schema re-runs on next up
#   ./ch.sh client      interactive SQL shell
#   ./ch.sh q "SQL"     run one query
#   ./ch.sh status      container state + row counts
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

KEYCHAIN_SERVICE="klend-clickhouse-password"
KEYCHAIN_SERVICE_RO="klend-clickhouse-ro-password"

if ! CLICKHOUSE_PASSWORD="$(security find-generic-password -a "$USER" -s "$KEYCHAIN_SERVICE" -w 2>/dev/null)"; then
    echo "error: no keychain entry '$KEYCHAIN_SERVICE' for user '$USER'." >&2
    echo "store one first (-w with no value prompts, so it never reaches shell history):" >&2
    echo "  security add-generic-password -a \"\$USER\" -s $KEYCHAIN_SERVICE -w" >&2
    exit 1
fi
# Exported because docker compose reads it for variable substitution in
# docker-compose.yml. Scoped to this script's process, not the calling shell.
export CLICKHOUSE_PASSWORD

# Runs a query INSIDE the container, so no clickhouse-client is needed on the
# host and the password never crosses a network boundary — not even loopback.
#
# `--password "$CLICKHOUSE_PASSWORD"` would put the secret in the container's
# argv, visible to `ps` and to `docker inspect`. Passing it as an env var and
# letting the client read CLICKHOUSE_PASSWORD keeps it out of both.
ch_exec() {
    docker compose exec -T \
        -e "CLICKHOUSE_PASSWORD=$CLICKHOUSE_PASSWORD" \
        clickhouse clickhouse-client --user klend --database klend "$@"
}

# Create/refresh the SELECT-only user the MCP server connects as.
#
# NOT in schema/, deliberately: files there run only on first boot of an empty
# volume, so a user added later would never exist on an already-running
# database. Doing it here — idempotent, every `up` — means account setup is not
# hostage to volume lifetime.
#
# The password is piped on STDIN rather than passed via --query, because
# `--query "CREATE USER ... IDENTIFIED BY 'secret'"` puts the secret in the
# container's argv where `ps` and `docker inspect` can read it.
ensure_readonly_user() {
    local ro_password
    if ! ro_password="$(security find-generic-password -a "$USER" -s "$KEYCHAIN_SERVICE_RO" -w 2>/dev/null)"; then
        echo "warning: no '$KEYCHAIN_SERVICE_RO' in Keychain; skipping read-only user" >&2
        return 0
    fi

    # Why a separate user at all: the `klend` account has full grants, including
    # DROP. The MCP server's read-only mode is a session setting it applies to
    # ITSELF — that is the client cooperating, not the database refusing. One
    # env var (CLICKHOUSE_ALLOW_WRITE_ACCESS=true), or any other client reusing
    # those credentials, and the protection is gone.
    #
    # Grants move the guarantee from "the client behaves" to "the server
    # refuses". That matters more than usual here: Yellowstone serves only the
    # tip, so a dropped historical slot cannot be re-fetched from anywhere.
    docker compose exec -T -e "CLICKHOUSE_PASSWORD=$CLICKHOUSE_PASSWORD" \
        clickhouse clickhouse-client --user klend --database klend --multiquery <<SQL
CREATE USER IF NOT EXISTS klend_ro IDENTIFIED BY '${ro_password}';
ALTER USER klend_ro IDENTIFIED BY '${ro_password}';
GRANT SELECT ON klend.* TO klend_ro;
-- system tables are what make schema introspection work (list_tables, DESCRIBE).
GRANT SELECT ON system.* TO klend_ro;
-- Belt and braces: even if a grant is later widened by accident, the profile
-- keeps the session read-only. Two independent mechanisms, not one.
ALTER USER klend_ro SETTINGS readonly = 1;
SQL
}

case "${1:-up}" in
    up)
        docker compose up -d
        echo "waiting for ClickHouse to accept queries…" >&2
        # Poll the healthcheck rather than sleeping a guessed number of seconds.
        # "Container started" is not "server ready"; the difference shows up as
        # an intermittent connection refusal, which is the worst kind of bug.
        for _ in $(seq 1 60); do
            if [ "$(docker inspect -f '{{.State.Health.Status}}' klend-clickhouse 2>/dev/null)" = "healthy" ]; then
                ensure_readonly_user
                echo "ClickHouse ready on 127.0.0.1:8123 (http) / 127.0.0.1:9000 (native)" >&2
                exit 0
            fi
            sleep 2
        done
        echo "error: ClickHouse did not become healthy. Logs:" >&2
        docker compose logs --tail=40 clickhouse >&2
        exit 1
        ;;
    down)
        docker compose down
        ;;
    nuke)
        # Destroys the data volume. The schema in schema/ only runs on a fresh
        # volume, so this is currently the only way to apply a schema edit.
        read -rp "destroy the ClickHouse data volume? [y/N] " reply
        [ "$reply" = "y" ] || { echo "aborted" >&2; exit 1; }
        docker compose down -v
        ;;
    client)
        docker compose exec \
            -e "CLICKHOUSE_PASSWORD=$CLICKHOUSE_PASSWORD" \
            clickhouse clickhouse-client --user klend --database klend
        ;;
    q)
        shift
        ch_exec --query "$*"
        ;;
    status)
        docker compose ps
        # FINAL forces dedup at query time. Without it a ReplacingMergeTree
        # count includes duplicates that background merges have not collapsed
        # yet — merges are eventual and may never run on their own. Fine for a
        # status check; too slow to put on a hot query path.
        ch_exec --query "
            SELECT 'account_updates' AS table, count() AS rows FROM account_updates FINAL
            UNION ALL
            SELECT 'slot_gaps', count() FROM slot_gaps FINAL
            UNION ALL
            SELECT 'ingest_checkpoint', count() FROM ingest_checkpoint FINAL
            FORMAT PrettyCompact"
        ;;
    *)
        echo "usage: $0 {up|down|nuke|client|q <SQL>|status}" >&2
        exit 1
        ;;
esac
