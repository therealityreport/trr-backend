# Render Deployment (TRR-Backend)

This is the canonical public-hosting target for the final AWS exit state:

- public FastAPI host on Render
- long-running work and admin vision on Modal

The repo already contains the two deployment artifacts for this path:

- `/Users/thomashulihan/Projects/TRR/TRR-Backend/render.yaml`
- `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/render/sync_render_service_from_aws.py`

## Service Contract

Render service defaults are locked to:

- service name: `trr-backend-api`
- runtime: `docker`
- plan: `standard`
- region: `virginia`
- health check: `/health`
- auto deploy: `off`

`TRR_API_URL` on Vercel Preview and Production should point at the Render
service URL after validation. Modal remains the executor for:

- `run_admin_operation`
- `run_google_news_sync`
- `run_reddit_refresh`
- `run_social_job`
- `sweep_social_dispatch_queue`
- `run_admin_vision`

## Required Operator Inputs

Export these locally before running the sync script:

```bash
export RENDER_API_KEY=...
export BETTER_STACK_SOURCE_TOKEN=...
export BETTER_STACK_INGESTING_HOST=in.logs.betterstack.com
export CORS_ALLOW_ORIGINS=https://trr-app.vercel.app,https://preview.example.vercel.app
export OBJECT_STORAGE_PROVIDER=r2
export OBJECT_STORAGE_BUCKET=trr-backend
export OBJECT_STORAGE_REGION=auto
export OBJECT_STORAGE_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com
export OBJECT_STORAGE_ACCESS_KEY_ID=...
export OBJECT_STORAGE_SECRET_ACCESS_KEY=...
export OBJECT_STORAGE_PUBLIC_BASE_URL=https://media.thereality.report
```

Notes:

- `RENDER_API_KEY` is required to call the Render API.
- `BETTER_STACK_*` values are passed through into the Render service env so the
  backend and Modal jobs can ship structured logs over HTTP.
- Default operator choice: create or use a Better Stack free HTTP source first;
  do not upgrade until actual log volume or retention needs justify it.
- `CORS_ALLOW_ORIGINS` should explicitly include the Vercel origins that need
  credentialed access.
- `OBJECT_STORAGE_*` values are passed through into the Render service env so
  the public API can cut over from AWS S3 to Cloudflare R2 without changing
  object keys.

## Create Or Update The Service

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/render/sync_render_service_from_aws.py
```

What the script does:

1. resolves the Render workspace owner
2. reads `/etc/trr-api.env` from the legacy AWS API host over SSM
3. overlays `/trr/staging/*` SSM parameters
4. overlays local pass-through env for:
   - `OBJECT_STORAGE_PROVIDER`
   - `OBJECT_STORAGE_BUCKET`
   - `OBJECT_STORAGE_REGION`
   - `OBJECT_STORAGE_ENDPOINT_URL`
   - `OBJECT_STORAGE_ACCESS_KEY_ID`
   - `OBJECT_STORAGE_SECRET_ACCESS_KEY`
   - `OBJECT_STORAGE_SESSION_TOKEN`
   - `OBJECT_STORAGE_PROFILE`
   - `OBJECT_STORAGE_PUBLIC_BASE_URL`
   - `OBJECT_STORAGE_PREFIX`
   - `BETTER_STACK_SOURCE_TOKEN`
   - `LOGTAIL_SOURCE_TOKEN`
   - `BETTER_STACK_INGESTING_HOST`
   - `LOGTAIL_INGESTING_HOST`
   - `BETTER_STACK_LOG_TIMEOUT_SECONDS`
   - `BETTER_STACK_FAILURE_COOLDOWN_SECONDS`
   - `CORS_ALLOW_ORIGINS`
5. creates or updates Render service `trr-backend-api`
6. syncs Render env vars and secret files
7. triggers a deploy unless `--skip-trigger-deploy` is passed

To inspect the payload before mutating Render:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/render/sync_render_service_from_aws.py --print-payload-only
```

## Validation Before Cutover

Validate the Render URL directly:

- `GET /health`
- `GET /openapi.json`
- authenticated admin routes
- show refresh kickoff
- Google News kickoff
- Reddit refresh kickoff
- covered image-analysis routes
- `/api/v1/screenalytics/*` and `/api/v1/screenalytics/v2/*` with service token
- Supabase read/write paths used by admin and social flows

Also verify Better Stack ingestion from:

- the Render API process
- Modal jobs triggered from the Render host

## Cutover

1. update Vercel Preview `TRR_API_URL` to the Render service URL
2. redeploy Preview and verify
3. update Vercel Production `TRR_API_URL` to the Render service URL
4. redeploy Production and verify
5. keep the current Modal API URL available for rollback during the 24-hour observation window

Do not remove the AWS ALB/NAT stack until the observation window passes cleanly.

When that window ends, use the dedicated teardown artifacts:

- `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/deploy/aws_teardown.md`
- `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/ops/aws_teardown_pass.py`

## Current Known Blocker

The Render service now exists and is serving the production cutover path.
The remaining blocker is not Render creation; it is the still-open rollback
observation window and the pending Better Stack follow-up before broader
CloudWatch reduction.
