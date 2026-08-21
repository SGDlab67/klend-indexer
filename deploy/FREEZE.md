# Freeze / resume runbook — klend-indexer

When to use this: after the demo (2026-08-17), to stop the ClickHouse Cloud
credit burn while keeping every byte of accumulated history recoverable.

The indexer has been running ~12 days and holds ~1.1M `account_updates` rows and
~180K decoded `obligation_snapshots`. That history lives only in ClickHouse
Cloud; Yellowstone serves the tip and cannot re-serve old slots, so once the
service is gone the history is gone. The freeze therefore exports first, stops
second.

## What `deploy/freeze.sh` does (7 stages)

1. Preflight: confirms the service + bucket are reachable.
2. Stop the writers on the VM: watchdog (off + disabled) and the indexer
   container (docker stop). The stream is now frozen.
3. Capture frozen numbers from ClickHouse (still up): last slot, row counts,
   last ingest time.
4. Full Parquet export (`KLEND_PARQUET_FULL=1`) of `account_updates` +
   `obligation_snapshots` on the VM, then upload to
   `gs://klend-indexer-dashboard/klend-parquet/`.
5. One final dashboard refresh (fresh `stats.json`), then write
   `gs://klend-indexer-dashboard/freeze-manifest.json` recording the frozen
   state.
6. Stop + disable the dashboard timer.
7. Idle the ClickHouse service (`clickhousectl cloud service stop`). Compute
   billing stops; data is preserved.

Run it from the repo root:

    ./deploy/freeze.sh

It is idempotent; re-running is safe.

## What each component looks like after a freeze

- Dashboard (`https://storage.googleapis.com/klend-indexer-dashboard/index.html`):
  still loads. Numbers are frozen; the "updated Ns ago" liveness dot goes stale.
  That staleness is the freeze signal, not a bug.
- Parquet: full dataset at `klend-parquet/` (Hive-partitioned by
  `slot_bucket`, two table roots).
- ClickHouse: state `idling`/`stopped`, zero compute, storage preserved.
- VM: indexer container stopped, watchdog + dashboard timer stopped/disabled.

## How to resume (`deploy/resume.sh`)

    ./deploy/resume.sh

1. `clickhousectl cloud service start` and wait for `running`.
2. Re-run the boot startup-script (`google_metadata_script_runner startup`),
   which re-pulls the image and relaunches the indexer + watchdog. The indexer
   resumes from its checkpoint, so no history is lost.
3. Re-enable the dashboard timer.

## Order matters (why export comes first)

The export reads ClickHouse, so it must run while the service is up. The writers
must be stopped before the export so the export captures the exact frozen state
with no in-flight writes. The service is stopped last, after nothing needs it.

## Cost notes (observed 2026-08-16)

- The klend data itself is ~100 MiB. The credit burn is dominated by ClickHouse
  Cloud system telemetry logs (7.8 GiB, 78x the data), not the indexer. See the
  `clickhouse-cloud-management` skill for the exact tables and the two
  user-level settings that were already disabled (`opentelemetry_start_trace_probability`,
  `log_processors_profiles`) to slow that leak.
- The service is provisioned at 2 replicas x 12 GB (the `v1-default` profile),
  which is far more than a 100 MiB demo dataset needs. If you want to keep the
  service queryable instead of stopping it, scale it down to 1 replica (and a
  smaller memory tier) to cut active cost; `stop` is the stronger lever and the
  default here.
- Idle scaling (15 min) is already on, so the service idles on its own after the
  indexer stops; the explicit `stop` just makes it deterministic.

## Rollback / caveats

- To unfreeze only the ClickHouse service (keep the indexer off): run just the
  `clickhousectl cloud service start` line from `resume.sh`.
- The freeze writes Parquet to the VM's `/var/klend/parquet` as a staging dir,
  then uploads to GCS. GCS is the durable copy; the VM copy is scratch.
- If `freeze.sh` fails mid-export, nothing is half-committed: the exporter writes
  to `.tmp` and renames on success, and GCS PUTs overwrite atomically. Re-run it.
