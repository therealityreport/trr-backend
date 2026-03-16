# Render Deployment (TRR-Backend)

Render is the canonical public host for `TRR-Backend`.

Live runtime split:

- public FastAPI host on Render
- long-running and admin execution on Modal
- hosted media on S3-compatible object storage

## Canonical Deployment Artifact

- `/Users/thomashulihan/Projects/TRR/TRR-Backend/render.yaml`

## Required Operator Inputs

Export these locally before deploying or updating the Render service:

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

## Validation

Verify the deployed Render URL directly:

- `GET /health`
- `GET /openapi.json`
- authenticated admin routes
- social kickoff routes
- image-analysis routes
- `/api/v1/screenalytics/*` and `/api/v1/screenalytics/v2/*` with service token

Also verify:

- Better Stack ingestion from the Render API process
- Modal jobs triggered from the Render host
- object-storage reads/writes using the `OBJECT_STORAGE_*` contract
