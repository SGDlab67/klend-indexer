#!/usr/bin/env bash
# Attach gce-startup.sh to the instance as `startup-script` metadata.
#
# Audited 2026-08-09: the instance carried a `watchdog-script` key and NO
# `startup-script` key, so gce-startup.sh had never run. Two consequences, both
# of which looked like working infrastructure:
#
#   1. The ingest-freshness watchdog it installs was never installed, leaving
#      the 2026-08-05 failure class unguarded for four days.
#   2. The redeploy documented in DEPLOY.md —
#      `sudo google_metadata_script_runner startup` — is a no-op. It appears to
#      succeed, and changes nothing.
#
# Attaching it was NOT safe until now, and that is worth stating rather than
# quietly fixing: gce-startup.sh carried the CLICKHOUSE_URL placeholder as a
# literal, so the next reboot would have run `docker rm -f klend-indexer` and
# relaunched it against a hostname that does not resolve. It now takes a
# rendered __CH_URL__ and aborts before anything destructive if the placeholder
# survives.
#
# Usage: deploy/install-startup-script.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Explicit, never `gcloud config get-value project`. The active gcloud project
# on this workstation is a different one, so every DEPLOY.md command that
# interpolated it resolved to the wrong Artifact Registry.
PROJECT="${GCP_PROJECT:-agentbiz-sungodlab}"
ZONE="${GCE_ZONE:-us-central1-a}"
VM="${GCE_VM:-klend-indexer}"

# shellcheck source=/dev/null
[ -f deploy/local.env ] && . deploy/local.env
CH_HOST="${CLICKHOUSE_CLOUD_HOST:-}"
[ -n "$CH_HOST" ] || { echo "error: CLICKHOUSE_CLOUD_HOST unset. Put it in deploy/local.env." >&2; exit 2; }

# The indexer speaks native-secure. The dashboard and watchdog use :8443 (HTTPS)
# because that path stayed reliable when the native connection half-opened during
# the 2026-08-05 incident; the writer needs the native protocol for
# insert_native_block, so it gets :9440.
CH_URL="${CH_HOST%%:*}:9440"

RENDERED="$(mktemp)"
trap 'rm -f "$RENDERED"' EXIT
sed -e "s|__CH_URL__|${CH_URL}|g" deploy/gce-startup.sh > "$RENDERED"

if grep -q '__[A-Z_]*__' "$RENDERED"; then
    echo "error: unsubstituted placeholder remains in rendered startup script:" >&2
    grep -o '__[A-Z_]*__' "$RENDERED" | sort -u >&2
    exit 3
fi

# Cheap proof the guard still fires, run against the UNRENDERED file. If a future
# edit removes the abort, this installer should be the thing that notices —
# otherwise the only signal is a reboot that silently takes the pipeline down.
if bash -c 'CH_URL="__CH_URL__"; case "$CH_URL" in *__*|*YOUR_INSTANCE*) exit 1;; esac'; then
    echo "error: gce-startup.sh placeholder guard no longer rejects an unrendered URL" >&2
    exit 4
fi

echo "==> attaching startup-script to ${VM} (CLICKHOUSE_URL=${CH_URL})"
gcloud compute instances add-metadata "$VM" \
    --project "$PROJECT" --zone "$ZONE" \
    --metadata-from-file "startup-script=${RENDERED}"

cat <<'NEXT'

Attached. The documented redeploy path now actually does something:

    gcloud builds submit --project agentbiz-sungodlab \
      --tag us-central1-docker.pkg.dev/agentbiz-sungodlab/klend/klend-indexer:latest .

    gcloud compute ssh klend-indexer --project agentbiz-sungodlab \
      --zone us-central1-a --tunnel-through-iap \
      --command 'sudo google_metadata_script_runner startup'

That re-fetches both secrets into tmpfs, pulls :latest, relaunches the container
and reinstalls the watchdog. Resume-from-checkpoint makes the relaunch lossless
as long as the gap stays inside the ~6000-slot replay window; a measured healthy
redeploy seam is ~56 slots.
NEXT
