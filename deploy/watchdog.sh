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

STALE_THRESHOLD=900   # seconds. Restart if ingest is older than this.
                      #
                      # MEASURED 2026-08-09, not guessed. Over 52,100 distinct ingest
                      # seconds across 4 days of steady state, the gap between
                      # consecutive writes ran p50=4s, p99=34s, p99.9=58s, and the
                      # single longest silence was 90s. 900 is ~10x that maximum.
                      #
                      # The asymmetry is the point, and it runs the same direction as
                      # RESUME_TOLERANCE_SLOTS in src/resume.rs: firing late costs
                      # detection latency, firing early restarts a HEALTHY indexer and
                      # manufactures the seam this exists to prevent. Against the 8.4h
                      # incident, anything under ~20 min is a rounding error, so buy
                      # margin against false positives with the slack.
                      #
                      # Known limitation: this cannot distinguish "indexer stalled" from
                      # "Solana halted". A multi-hour chain halt would restart the
                      # indexer repeatedly (bounded by MIN_SETTLE). Harmless — each
                      # restart resumes from the checkpoint — but noisy. The real fix is
                      # a heartbeat carrying tip_slot, which the indexer does not write.
INTERVAL=300          # seconds between checks (the "cron" period).
MIN_SETTLE=180        # after a restart, give the indexer this long to resume before
                      # judging freshness again, so we never restart-storm.
COOLDOWN=/run/klend-watchdog-last-restart

# Substituted by deploy/install-watchdog.sh from deploy/local.env. The repo keeps
# the placeholder: this file is public and the live hostname is operator config,
# the same rule ch-remote.sh and install-dashboard-export.sh already follow.
CH_HOST=__CH_HOST__
CH_PORT=8443

# SELECT-only user, not the admin credential this script originally used.
#
# The watchdog runs exactly one query — a max(ingested_at) — and holding the
# admin password to do it put the credential that can DROP the dataset inside a
# long-lived loop on the box, for no capability it needs. Same reasoning that
# moved the dashboard off the admin user on Day 7; this was simply missed
# because the watchdog was never actually installed.
CH_USER=__RO_USER__
CH_SECRET=__SECRET__
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
        "https://secretmanager.googleapis.com/v1/projects/${project}/secrets/${CH_SECRET}/versions/latest:access" \
        | json | grep -oE '"data":"[^"]+"' | cut -d'"' -f4 | base64 -d)"
    [ -n "$chpw" ] || { log "no CH password; skip"; return; }

    # Ingest freshness over the HTTPS interface (:8443), which is reliable even when
    # the native :9440 path is degraded (the connection that half-opened in the incident).
    # Deliberately free of string literals: `dateDiff('second', ...)` needs quotes,
    # and this script travels workstation -> instance metadata -> curl -> file ->
    # bash before it runs. Every one of those layers can eat a quote, and the Day 5
    # blank-password bug came from exactly that class of pipeline damage. Subtracting
    # two Unix timestamps needs no quoting and returns the same integer.
    stale="$(curl -sS -m 20 --user "${CH_USER}:${chpw}" "https://${CH_HOST}:${CH_PORT}/" \
        --data-binary "SELECT toInt64(toUnixTimestamp(now64(3)) - toUnixTimestamp(max(ingested_at))) FROM klend.account_updates" \
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
