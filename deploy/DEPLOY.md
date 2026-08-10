# Deploy: klend-indexer on GCP Compute Engine, hosted ClickHouse Cloud

Architecture chosen 2026-08-04 (plan §11c). Initially scoped for AWS Lightsail;
switched to GCP because the ClickHouse Cloud service already runs on GCP us-central1,
so co-locating the indexer there removes cross-cloud egress and latency. The indexer
runs as a container on a small always-on GCE VM in the SAME region and writes to the
managed ClickHouse Cloud service over native-secure TLS. `docker run --restart always`
plus a boot startup-script is the process supervisor; the in-process reconnect loop
resumes from the checkpoint after any drop.

```
GCE e2-micro  (us-central1, free-tier eligible, Container-Optimized OS)
  docker: klend-indexer  --restart always
      | native-secure TLS  :9440   (same-region, minimal egress)
      v
ClickHouse Cloud "Klend-Indexer"  (GCP us-central1, managed, $300 credit)
```

The point of this box is accumulation: Yellowstone serves only the tip and cannot
re-serve old slots, so every hour it runs is klend history that exists for the demo
and every hour it is down is history gone. Get it running, then improve it.

## Service facts (as of 2026-08-04)

- ClickHouse Cloud org: `Sun God lab` (`f0869896-ee15-48af-98e7-6ef3de8b987b`)
- Service: `Klend-Indexer` (`78f807ed-43c0-4aae-bf93-3a9a438160e6`), ClickHouse 26.2
- Native-secure endpoint: `YOUR_INSTANCE.REGION.gcp.clickhouse.cloud:9440`
- HTTPS endpoint: `YOUR_INSTANCE.REGION.gcp.clickhouse.cloud:8443`
- Idle scaling on (15 min); the service wakes on first connect.
- IP access list is currently `0.0.0.0/0` (open). Tighten to the VM's external IP
  once it exists (step 6).

## Cost shape

- Compute: `e2-micro` in us-central1 is free-tier eligible (1 non-preemptible micro
  per month across us-central1/us-west1/us-east1). The runtime container is tiny
  (a few KB/s of stream), so a micro is plenty; the heavy Rust build is offloaded to
  Cloud Build, so the micro never needs the RAM to compile.
- ClickHouse Cloud: on the $300 credit, idle scaling keeps quiet spans cheap.
- Alchemy: ~$1-2/mo measured. Egress is same-region GCP to GCP, so negligible.

## Prerequisites (once, interactive, from the Mac)

gcloud is installed (`brew install --cask google-cloud-sdk`, done 2026-08-04). Then:

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID          # create one first if needed; billing must be enabled
gcloud config set compute/region us-central1
gcloud services enable \
  compute.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com secretmanager.googleapis.com
```

Because `gcloud auth login` opens a browser, run it yourself in the session with
`! gcloud auth login` so its output lands here.

## Step 1: apply the schema to Cloud (from the Mac)

The Cloud service has only the default databases; `klend` does not exist there yet
(the local docker instance is separate). Store the Cloud SQL password in the
Keychain, then apply both schema files over HTTPS:

```bash
security add-generic-password -a "$USER" -s klend-clickhouse-cloud-password -w
./deploy/apply-schema-cloud.sh
```

Expected tail: `tables in klend: account_updates ingest_checkpoint` and an engine
containing `ReplacingMergeTree` (Cloud reports it as `SharedReplacingMergeTree`,
same semantics; DDL unchanged).

## Step 2: put the two secrets in Secret Manager

Piped straight from the Keychain, so the values never hit disk or argv:

```bash
security find-generic-password -a "$USER" -s alchemy-grpc-token -w \
  | gcloud secrets create alchemy-grpc-token --data-file=-
security find-generic-password -a "$USER" -s klend-clickhouse-cloud-password -w \
  | gcloud secrets create klend-clickhouse-cloud-password --data-file=-
```

(Use `gcloud secrets versions add <name> --data-file=-` instead of `create` to rotate.)

## Step 3: build and push the image with Cloud Build

```bash
gcloud artifacts repositories create klend \
  --repository-format=docker --location=us-central1
gcloud builds submit \
  --project agentbiz-sungodlab \
  --tag us-central1-docker.pkg.dev/agentbiz-sungodlab/klend/klend-indexer:latest .
```

Cloud Build compiles the multi-stage Dockerfile in Google's infra and pushes to
Artifact Registry. The runtime image is debian-slim with ca-certificates: required
because the gRPC side uses the OS trust store, while the ClickHouse side uses
in-binary webpki roots.

## Step 4: grant the VM service account access

The default compute service account needs to read the secrets and pull the image:

```bash
# Explicit, never `gcloud config get-value project`. Verified 2026-08-09: the
# active gcloud project on the workstation is gen-lang-client-0502946726, so
# every command that interpolated it silently targeted the wrong project.
PROJECT=agentbiz-sungodlab
PNUM=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
SA="${PNUM}-compute@developer.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${SA}" --role=roles/secretmanager.secretAccessor
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:${SA}" --role=roles/artifactregistry.reader
```

## Step 5: create the VM

Container-Optimized OS, e2-micro, us-central1, with the boot startup-script that
fetches the secrets and runs the container (see `deploy/gce-startup.sh`):

```bash
gcloud compute instances create klend-indexer \
  --zone=us-central1-a \
  --machine-type=e2-micro \
  --image-family=cos-stable --image-project=cos-cloud \
  --scopes=cloud-platform \
  --metadata-from-file=startup-script=deploy/gce-startup.sh
```

No inbound app ports are needed (the indexer only makes outbound gRPC + ClickHouse
connections), so leave the default firewall; SSH is enough for operating it.

## Step 6: tighten and verify

Get the VM's external IP and restrict the Cloud IP access list to it (ClickHouse
Cloud console, service Settings), replacing `0.0.0.0/0`. The database is a
payment-adjacent asset; do not leave it open to the internet past the first hour.

```bash
gcloud compute instances describe klend-indexer --zone=us-central1-a \
  --format='value(networkInterfaces[0].accessConfigs[0].natIP)'
```

Verify the container:

```bash
gcloud compute ssh klend-indexer --zone=us-central1-a \
  --command='docker logs --tail=50 klend-indexer'
```

Expect: `writing to ClickHouse at ...:9440 (native-secure TLS)`, then
`subscribed to klend ...`, then periodic `flushed N rows; checkpoint slot=...`.

Confirm rows are landing (ClickHouse Cloud console or the MCP `run_select_query`):

```
SELECT count(), max(slot) FROM klend.account_updates
SELECT * FROM klend.ingest_checkpoint FINAL
```

Confirm restart-resume: `docker restart klend-indexer` on the box, then check the log
prints `resuming from checkpoint slot=... (inclusive)` with no gap warning.

## Redeploy a new build

```bash
gcloud builds submit --project agentbiz-sungodlab \
  --tag us-central1-docker.pkg.dev/agentbiz-sungodlab/klend/klend-indexer:latest .
gcloud compute ssh klend-indexer --zone=us-central1-a \
  --command='sudo google_metadata_script_runner startup'   # re-runs the startup-script: pulls :latest, relaunches
```

## Guardrails (do not skip)

- Alchemy is usage-based with no ceiling. The Alchemy dashboard hard spend cap and
  spend alerts (SECRETS.md) must be set before this runs 24/7. Measured burn is
  ~$1-2/mo, but the cap turns a bad filter into a bounded loss, not an invoice.
- Watch the ClickHouse Cloud credit burn weekly (§7d).
- The empty-`owner` filter hazard still applies: never deploy a build that has
  touched the subscription filter without confirming `owner` is non-empty.

## What this deploy is NOT

Reconnect-lite only: resume from checkpoint plus reconnect-on-drop plus a
stale-checkpoint fallback to the tip. There is no gap-detection table and no archive
backfill. A downtime longer than the ~40 min replay window leaves an explicit,
logged, UNFILLED gap that full Block 2 (§8c) fills after the demo.

## The simple fallback (if you would rather not use Cloud Build / Secret Manager)

An Ubuntu e2-small VM with Docker installed, image built on the box, secrets in a
root-owned `600 /etc/klend-indexer.env` (the `deploy/klend-indexer.env.example`
template), and `docker run --restart always --env-file`. Same as the original
Lightsail runbook, just on GCE. Costs a few dollars a month instead of free and adds
no Secret Manager, at the price of secrets sitting on the box's disk.
