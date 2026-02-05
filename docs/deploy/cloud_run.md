# Cloud Run Deployment (TRR-Backend)

This guide deploys the FastAPI app on Google Cloud Run using the repo Dockerfile.
Defaults use region `us-east1`, service name `trr-backend`, and min instances `0`.

## One-Time GCP Setup

1. Enable APIs in your GCP project: Cloud Run API, Cloud Build API, Artifact Registry API, Secret Manager API, IAM API.
2. Choose a region and keep Cloud Run and Artifact Registry in the same region. Recommended: `us-east1`.
3. Create an Artifact Registry repository (Docker format). Example repo name: `trr-backend`. Region: `us-east1`.
4. Create a runtime service account named `trr-backend-runtime`.
5. Grant IAM roles: `roles/secretmanager.secretAccessor` on required secrets, and `roles/artifactregistry.reader` if Cloud Run pulls from Artifact Registry.

## Secrets and Environment Variables

### Required for API Runtime

Set exactly one database URL. `SUPABASE_DB_URL` is preferred in production.

- `SUPABASE_DB_URL` (recommended)
- `DATABASE_URL`
- `TRR_DB_URL`

### Required for Auth-Protected Endpoints

- `SUPABASE_JWT_SECRET` (Secret Manager)

### Non-Secret Config

- `CORS_ALLOW_ORIGINS`
- `ADMIN_EMAIL_ALLOWLIST`

CORS guidance for Vercel. Include your production domain and any Vercel preview domains you want to allow.

### Optional / Feature-Gated

Screenalytics service endpoints.
- `SCREENALYTICS_SERVICE_TOKEN` (Secret Manager). Required for `/screenalytics/*`.
- `SCREENALYTICS_API_URL` (plain env). Required for admin auto-count endpoints.

Realtime and WebSockets.
- `REDIS_URL` (Secret Manager). Required for multi-instance realtime. If unset, an in-memory broker is used.

S3 media mirroring and admin media endpoints.
- `AWS_REGION` (plain env)
- `AWS_S3_BUCKET` (plain env)
- `AWS_CDN_BASE_URL` (plain env)
- `AWS_ACCESS_KEY_ID` (Secret Manager)
- `AWS_SECRET_ACCESS_KEY` (Secret Manager)
- `AWS_S3_PREFIX` (optional)

### Not Required for Cloud Run API (Pipeline Only)

These are used by ingest/sync scripts and do not need to be set for the API service.

- `TMDB_API_KEY`
- `IMDB_API_KEY`
- `OPENAI_API_KEY`
- `GEMINI_API_KEY` and other Google/Gemini vars
- `FIRECRAWL_API_KEY`
- Firebase credentials
- Google Sheets credentials
- `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`

## Cloud Run Service Settings (Recommended Defaults)

- Service name: `trr-backend`
- Region: `us-east1`
- Request timeout: `3600s`
- Min instances: `0`
- Max instances: `10`
- Concurrency: default
- CPU / Memory: `1 vCPU / 1GiB`

WebSocket guidance:
If you need multi-instance WebSockets, set `REDIS_URL`. If you see connection instability, lower concurrency and raise max instances.

## Continuous Deployment from GitHub (Console)

1. Cloud Run console -> Create service.
2. Select "Deploy one revision from a source repository".
3. Connect GitHub repo: `TRR-Backend`.
4. Branch: `main`.
5. Build type: Dockerfile.
6. Dockerfile path: `Dockerfile`.
7. Service name: `trr-backend`.
8. Region: `us-east1`.
9. Set env vars and secrets.
10. Deploy.

## Troubleshooting

- Placeholder page after deploy. Check Cloud Run Build History and logs. Common failures include Dockerfile not found, pip install errors, port binding, or missing env and secrets.
- Container start failures. Check Cloud Run logs for stack traces and import errors.
- Permission issues. Verify Secret Manager access and Artifact Registry reader role.

## Verification Checklist

- `GET /openapi.json` returns 200.
- WebSocket endpoint connects (if used).
- Logs appear in Cloud Logging.
- Vercel frontend can call backend with expected CORS and auth behavior.

## Optional Add-Ons

- Rollback. Redeploy a previous revision.
- Staging vs production. Maintain two services and two secret sets.
