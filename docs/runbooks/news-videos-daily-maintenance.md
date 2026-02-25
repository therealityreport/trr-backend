# News + Videos Daily Maintenance Runbook

This runbook configures and operates the daily News/Videos maintenance job.

## Scope
The daily job runs three phases in order:
1. Bootstrap Bravo snapshots for Bravo-linked shows missing persisted Bravo snapshots.
2. Sync/mirror Bravo video thumbnails for shows with persisted Bravo snapshots.
3. Sync Google News for shows with configured `google_news_url` show links.

Primary script:
- `scripts/backfill/run_news_video_maintenance.py`

## Runtime Command
Cloud Run Job command:

```bash
PYTHONPATH=. python scripts/backfill/run_news_video_maintenance.py \
  --phase all \
  --continue-on-error \
  --json-summary -
```

Dry-run smoke command:

```bash
PYTHONPATH=. python scripts/backfill/run_news_video_maintenance.py \
  --phase all \
  --dry-run \
  --limit 20 \
  --json-summary -
```

## Required Runtime Environment
Set these in the Cloud Run Job runtime:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_DB_URL` (or `TRR_DB_URL` used by SQL helpers)
- S3/media mirror env vars used by `import_images` and media mirroring (same as backend API runtime)

## IAM
Recommended service accounts:
- Job runtime SA: backend service account with DB/secret access.
- Scheduler caller SA: dedicated scheduler invoker SA.

Required permissions:
- Scheduler caller SA can run Cloud Run Jobs:
  - `roles/run.invoker` on the job (or project-scoped if required by policy).
- Scheduler caller SA can mint OAuth token for job-run call.

## Create / Update Cloud Run Job
Use your deployed backend image.

```bash
PROJECT_ID="your-project"
REGION="us-central1"
JOB_NAME="trr-news-videos-daily"
IMAGE="us-docker.pkg.dev/${PROJECT_ID}/trr/backend:latest"
RUNTIME_SA="trr-backend-runtime@${PROJECT_ID}.iam.gserviceaccount.com"

# Create (first time)
gcloud run jobs create "${JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --service-account "${RUNTIME_SA}" \
  --command python \
  --args scripts/backfill/run_news_video_maintenance.py,--phase,all,--continue-on-error,--json-summary,-

# Update (subsequent deploys)
gcloud run jobs update "${JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --service-account "${RUNTIME_SA}" \
  --command python \
  --args scripts/backfill/run_news_video_maintenance.py,--phase,all,--continue-on-error,--json-summary,-
```

## Create Cloud Scheduler Trigger (Daily)
Schedule: daily at `03:00` in `America/New_York`.

```bash
PROJECT_ID="your-project"
REGION="us-central1"
JOB_NAME="trr-news-videos-daily"
SCHEDULER_JOB_NAME="trr-news-videos-daily-0300-et"
SCHEDULER_SA="trr-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --schedule "0 3 * * *" \
  --time-zone "America/New_York" \
  --uri "https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/${REGION}/jobs/${JOB_NAME}:run" \
  --http-method POST \
  --oauth-service-account-email "${SCHEDULER_SA}" \
  --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" \
  --message-body '{}'
```

## Manual Execution
```bash
gcloud run jobs execute "${JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --wait
```

## Monitoring + Alerts
Inspect run logs by job execution ID and monitor:
- hard failures (non-zero exit)
- high show failure ratio
- repeated zero-processed runs

Suggested alert condition:
- Any run exits non-zero, or
- `shows_processed == 0` for 3 consecutive scheduled runs.

## Troubleshooting
1. `No targets found` across phases:
- verify show links exist (`bravotv.com` show links, `google_news_url` links).
- verify filters/`--show-id` scope.

2. Google phase failures:
- verify `google_news_url` link quality and status (`approved`/`pending`).
- inspect job logs for per-show parser or mirror errors.

3. Thumbnail sync failures:
- check image source URL availability in snapshots.
- verify media mirror credentials and storage connectivity.
