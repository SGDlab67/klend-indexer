#!/usr/bin/env bash
# Create the SELECT-only ClickHouse user that web/proxy.js authenticates as.
#
# WHY THIS EXISTS
# The dashboard proxy is reachable from the network. It previously ran as
# `default`, the admin user, which meant the credential behind a public HTTP
# port could DROP the database. The data is not reconstructible (Yellowstone
# serves only the chain tip), so that is permanent loss, not a restore. This
# script produces a credential whose worst case is reading klend.
#
# The grant is the primary control. Query-level settings and the proxy's fixed
# statement list are additional layers, not substitutes: a SELECT-only grant
# fails a write at the server regardless of what the client asks for.
#
# CREDENTIAL HANDLING (SECRETS.md)
# The password is generated here, held only in a shell variable, and written to
# exactly one place: GCP Secret Manager, over stdin. It never appears in argv,
# never touches disk, and is never echoed. The Mac does not keep a copy because
# the Mac does not need one; the proxy runs on the VM and reads the secret there
# the same way the indexer reads its own.
#
# Usage:  deploy/create-readonly-user.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
SECRET="${READONLY_SECRET_NAME:-klend-clickhouse-readonly-password}"
RO_USER="${CH_READONLY_USER:-klend_ro}"

command -v gcloud >/dev/null || { echo "error: gcloud not found" >&2; exit 2; }

# Generated locally so the plaintext exists in exactly one process for exactly
# as long as this script runs.
#
# Read a BOUNDED slice of /dev/urandom rather than piping the infinite stream
# into `head -c N`. In that shape head exits early, tr is killed by SIGPIPE, and
# under `set -o pipefail` the pipeline returns 141 and `set -e` aborts the
# script before it prints anything. `cut` consumes its whole input, so nothing
# gets a closed pipe.
rand_chars() {  # $1 = tr charset, $2 = how many
    LC_ALL=C head -c 4096 /dev/urandom | LC_ALL=C tr -dc "$1" | cut -c1-"$2"
}

# ClickHouse Cloud enforces a complexity policy (it rejects an all-alphanumeric
# password for want of a special character), so the charset has to include one.
# The excluded punctuation is deliberate, not aesthetic:
#   '  would terminate the single-quoted SQL string literal
#   $ ` \  would be expanded by the UNQUOTED heredoc that interpolates ${PW}
# What remains is safe through both layers.
SPECIALS='!#%^&*()_=+:,.?@-'

PW=''
for _ in 1 2 3 4 5; do
    CAND="$(rand_chars 'A-Za-z0-9' 36)$(rand_chars "$SPECIALS" 4)"
    # Verify every class is present rather than assuming it. With 36 base62
    # characters a missing class is vanishingly unlikely, but "vanishingly
    # unlikely" and "checked" are different things, and the failure mode is a
    # confusing server-side rejection three steps later.
    case "$CAND" in
        *[A-Z]*) case "$CAND" in
            *[a-z]*) case "$CAND" in
                *[0-9]*) PW="$CAND"; break ;;
            esac ;;
        esac ;;
    esac
done
[ "${#PW}" -eq 40 ] || { echo "error: password generation failed" >&2; exit 2; }

echo "==> storing the password in Secret Manager (project ${PROJECT}, secret ${SECRET})"
# --data-file=- reads stdin, so the value is not an argument and cannot appear
# in the process table or in shell history.
if gcloud secrets describe "$SECRET" --project "$PROJECT" >/dev/null 2>&1; then
    printf '%s' "$PW" | gcloud secrets versions add "$SECRET" \
        --project "$PROJECT" --data-file=- >/dev/null
    echo "    added a new version to the existing secret"
else
    printf '%s' "$PW" | gcloud secrets create "$SECRET" \
        --project "$PROJECT" --data-file=- --replication-policy=automatic >/dev/null
    echo "    created the secret"
fi

echo "==> creating ClickHouse user '${RO_USER}'"
# Piped into ch-remote.sh on STDIN, not passed as an argument, because this
# statement contains the password. ch-remote reads "${1:-$(cat)}".
#
# readonly = 2 (not 1): 1 forbids writes AND forbids changing settings, which
# would make the proxy's own per-query limits unusable. 2 forbids
# INSERT/ALTER/CREATE/DROP while still allowing limits to be tightened.
#
# The trailing MAX clauses are what stop a future caller from RAISING those
# limits. Without them the settings are defaults, not ceilings, and the cost
# protection on a metered service would be advisory. Note the syntax: the
# constraint verb (MIN / MAX / CONST / READONLY) attaches directly to the
# setting. There is no CONSTRAINT keyword here, unlike in a table definition.
deploy/ch-remote.sh <<SQL
CREATE USER IF NOT EXISTS ${RO_USER}
    IDENTIFIED WITH sha256_password BY '${PW}'
    SETTINGS
        readonly = 2,
        max_execution_time = 15 MAX 15,
        max_result_rows = 1000 MAX 1000,
        max_rows_to_read = 500000000 MAX 500000000
SQL

echo "==> granting SELECT on klend.* and nothing else"
deploy/ch-remote.sh "GRANT SELECT ON klend.* TO ${RO_USER}"

unset PW

echo "==> verifying"
deploy/ch-remote.sh "SELECT name FROM system.users WHERE name = '${RO_USER}' FORMAT TSVRaw"
deploy/ch-remote.sh "SELECT access_type, database FROM system.grants WHERE user_name = '${RO_USER}' FORMAT TSVRaw"

cat <<'NEXT'

Done. The grant list above must show SELECT on klend and nothing else. If it
shows anything wider, revoke before pointing the proxy at this user.

Next: redeploy the proxy with
    CH_USER=klend_ro
    CH_PASSWORD  <- from Secret Manager, on the VM, same mechanism as the indexer
    CH_HOST      <- from the environment, never baked into the image
NEXT
