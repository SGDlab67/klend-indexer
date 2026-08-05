#!/usr/bin/env bash
# Ingest-freshness watchdog for klend-indexer. Runs on the COS host as a systemd
# service (Restart=always), looping every INTERVAL. COS has no cron; a self-looping
# service is the idiomatic equivalent and survives its own crashes.
#
# Why it exists: the 2026-08-05 incident froze the indexer for 8.4h while the
# container reported "Up" at CPU 0% (a half-open ClickHouse connection parked the
# inline insert). Process liveness is not data liveness. The only signal that catches
# that is INGEST FRESHNESS: now() - max(ingested_at) in ClickHouse. The insert-timeout
# fix already turns that specific freeze into a fast self-restart; this watchdog is
# defense-in-depth for any other stall that leaves the process alive but not writing.
#
# `set -e` is deliberately OFF: a transient ClickHouse or metadata hiccup must never
# kill the watchdog or, worse, be read as "stale" and trigger a needless restart.
set -uo pipefail

STALE_THRESHOLD=900   # seconds. Restart if ingest is older than this. The incident was
                      # 8.4h, so 15 min is fast; klend can be quiet for a few minutes,
                      # so it is above normal jitter and not trigger-happy.
INTERVAL=300          # seconds between checks (the "cron" period).
MIN_SETTLE=180        # after a restart, give the indexer this long to resume before
                      # judging freshness again, so we never restart-storm.
COOLDOWN=/run/klend-watchdog-last-restart

CH_HOST=YOUR_INSTANCE.REGION.gcp.clickhouse.cloud
CH_PORT=8443
META='http://metadata.google.internal/computeMetadata/v1'
HDR='Metadata-Flavor: Google'

# Secret Manager returns pretty-printed JSON; strip whitespace before grep (same
# lesson as the startup script's blank-password bug).
json() { tr -d ' \n'; }
log()  { echo "$(date -u +%FT%TZ) watchdog: $*"; }

check() {
    local project token chpw stale now last
    project="$(curl -s -H "$HDR" "$META/project/project-id")"
    token="$(curl -s -H "$HDR" "$META/instance/service-accounts/default/token" \
        | json | grep -oE '"access_token":"[^"]+"' | cut -d'"' -f4)"
    [ -n "$token" ] || { log "no SA token; skip"; return; }
    chpw="$(curl -s -H "Authorization: Bearer $token" \
        "https://secretmanager.googleapis.com/v1/projects/${project}/secrets/klend-clickhouse-cloud-password/versions/latest:access" \
        | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
    [ -n "$chpw" ] || { log "no CH password; skip"; return; }

    # Ingest freshness over the HTTPS interface (:8443), which is reliable even when
    # the native :9440 path is degraded (the connection that half-opened in the incident).
    stale="$(curl -sS -m 20 --user "default:${chpw}" "https://${CH_HOST}:${CH_PORT}/" \
        --data-binary "SELECT toInt64(dateDiff('second', max(ingested_at), now64(3))) FROM klend.account_updates" \
        2>/dev/null | tr -d '[:space:]')"

    # A failed or non-numeric result means "cannot tell", NOT "stale". Never restart on it.
    case "$stale" in
        ''|*[!0-9]*) log "freshness query failed (got '${stale}'); not acting"; return ;;
    esac

    if [ "$stale" -lt "$STALE_THRESHOLD" ]; then
        log "fresh (${stale}s)"
        return
    fi

    now="$(date +%s)"
    last="$(cat "$COOLDOWN" 2>/dev/null || echo 0)"
    if [ $((now - last)) -lt "$MIN_SETTLE" ]; then
        log "stale (${stale}s) but restarted $((now - last))s ago; letting it settle"
        return
    fi

    log "STALE ${stale}s > ${STALE_THRESHOLD}s; restarting klend-indexer"
    if docker restart klend-indexer >/dev/null 2>&1; then
        echo "$now" > "$COOLDOWN"
        log "restarted"
    else
        log "restart FAILED"
    fi
}

log "started (threshold=${STALE_THRESHOLD}s interval=${INTERVAL}s)"
while true; do
    check
    sleep "$INTERVAL"
done
