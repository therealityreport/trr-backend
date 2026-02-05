# Cloud Run Deployment (TRR-Backend)

This guide deploys the FastAPI app on Google Cloud Run using the repo `Dockerfile` at the repo root.
Defaults use region `us-east1`, service name `trr-backend`, and min instances `0`.

## Step 0: Reconcile Deployed Truth (Before PRs)

1. Run `git fetch --all --prune`.
2. Verify the Dockerfile on main with `git show origin/main:Dockerfile`.
3. In Cloud Run, open Build History and record the last successful build commit SHA and Dockerfile path.
4. Confirm the Cloud Run service source configuration: repo is the intended GitHub repo, branch is `main`, Dockerfile path is `Dockerfile` (no leading `/`), and build context is the repo root.

Rule: Cloud Build + Cloud Run history wins over memory.

## One-Time GCP Setup

1. Enable APIs in your GCP project: Cloud Run API, Cloud Build API, Artifact Registry API, Secret Manager API, IAM API.
2. Choose a region and keep Cloud Run and Artifact Registry in the same region. Recommended: `us-east1`.
3. Create an Artifact Registry repository (Docker format). Example repo name: `trr-backend`. Region: `us-east1`.
4. Create a runtime service account named `trr-backend-runtime`.
5. Grant IAM roles: `roles/secretmanager.secretAccessor` on required secrets, and `roles/artifactregistry.reader` if Cloud Run pulls from Artifact Registry.

## Secrets and Environment Variables

The table below is sourced from current code and `.env.example`.
Put secrets in Secret Manager and reference them in Cloud Run. Do not paste secrets into plain env var fields.

Set only one DB URL in Cloud Run. If multiple are set, you may connect to the wrong database depending on precedence.
Precedence is `SUPABASE_DB_URL` → `DATABASE_URL` → `TRR_DB_URL`.

| Variable name | Required? | Secret? | Where it comes from | Notes / expected format |
| --- | --- | --- | --- | --- |
| `SUPABASE_DB_URL` | Y | Y | Supabase | Preferred DB connection string for production. Example: `postgresql://...` |
| `DATABASE_URL` | N (fallback) | Y | Postgres | Standard connection string. Used only if `SUPABASE_DB_URL` is unset. |
| `TRR_DB_URL` | N (fallback) | Y | Legacy | Legacy alias. Used only if `SUPABASE_DB_URL` and `DATABASE_URL` are unset. |
| `SUPABASE_JWT_SECRET` | Y | Y | Supabase | JWT signing secret used to verify Supabase access tokens. |
| `CORS_ALLOW_ORIGINS` | N | N | App config | Comma-separated origins. Include Vercel prod and any preview domains you need. Credentials require explicit origins. |
| `ADMIN_EMAIL_ALLOWLIST` | N | N | App config | Comma-separated emails for allowlist-only admin endpoints. |
| `SCREENALYTICS_SERVICE_TOKEN` | N (required for `/screenalytics` endpoints) | Y | Screenalytics | Bearer token for service-to-service auth. |
| `SCREENALYTICS_API_URL` | N (required for auto-count) | N | Screenalytics | Base URL for Screenalytics API. |
| `REDIS_URL` | N (required for multi-instance realtime) | Y | Redis provider | `redis://...` for pub/sub fanout. |

## Cloud Run Service Settings (Recommended Defaults)

- Service name: `trr-backend`
- Region: `us-east1`
- Request timeout: `3600s`
- Min instances: `0`
- Max instances: `10`
- Concurrency: default (currently 80; WebSockets consume concurrency while connected)
- CPU / Memory: `1 vCPU / 1GiB`
- Service account: `trr-backend-runtime` with `Secret Manager Secret Accessor`

Notes:
- Setting min instances to `1` reduces cold starts but increases cost.
- Lowering concurrency can reduce memory pressure if you see OOMs.

## Continuous Deployment from GitHub (Console)

1. Cloud Run console -> Create service.
2. Select "Deploy one revision from a source repository".
3. Connect GitHub repo: `TRR-Backend`.
4. Branch: `main`.
5. Build type: Dockerfile.
6. Dockerfile path: `Dockerfile` (no leading `/`).
7. Build context: repo root.
8. Service name: `trr-backend`.
9. Region: `us-east1`.
10. Set env vars and secrets.
11. Deploy.

## Verification

1. Boot check: `GET /openapi.json` returns 200.
2. DB check: pick a DB-backed endpoint listed in `/openapi.json` and confirm its auth requirements.

Example DB check (public, no auth required):

```bash
curl -i https://<service-url>/api/v1/shows?limit=1
```

Interpretation:
- `200` + JSON list: DB connectivity and query path are working (empty list is OK).
- `401/403`: endpoint is authenticated; do not use it for a public DB check.
- `500/502`: DB env/secrets likely misconfigured or DB unreachable.

If you choose an authenticated endpoint, include the required `Authorization: Bearer ...` header explicitly.

## WebSockets and Realtime

Instances are stateless. Do not rely on in-memory state surviving across requests or connections.
If you scale above 1 instance or Cloud Run replaces an instance, clients may reconnect to a different instance.
Any per-connection state must live outside the container (DB/Redis).
Cross-instance fanout requires `REDIS_URL`. Without it, behavior is single-instance only.
Cloud Run allows long-lived requests up to the service timeout; clients must handle reconnect.
WebSocket connections may still be disrupted by network issues or tab sleep.

## Troubleshooting

- Build History location: Cloud Run console -> Service -> Build History.
- Placeholder revision after deploy: check Cloud Run Build History and logs.
- Build errors: wrong Dockerfile path (`/Dockerfile` vs `Dockerfile`).
- Build errors: missing Dockerfile on branch.
- Runtime errors: container not listening on `$PORT`.
- Build errors: `pip install` failures due to missing system deps.

## Phase 2 Acceptance Criteria

- Doc can be followed end-to-end by a new dev and results in a working Cloud Run revision.
- Env vars and secrets list is explicit and correct.
- No secrets committed; placeholders only.

## Phase 3 TODOs / Tasks (CI/CD + Production Hardening)

Pick one deploy path and disable the other.

Option A (recommended): GitHub Actions → Artifact Registry → Cloud Run

- Create deploy workflow `deploy_cloud_run.yml`.
- On push to `main`: run tests, build image, push to Artifact Registry, deploy to Cloud Run.
- Tag image with commit SHA.
- Auth via Workload Identity Federation (no JSON keys).
- Bind to deploy service account (example: `trr-backend-deployer`).
- Minimal roles: Cloud Run Admin (or Cloud Run Developer + additional permissions), Artifact Registry Writer, Service Account User (on runtime SA).
- Disable Cloud Run continuous deploy trigger for this service or set it to manual.

Option B: Cloud Build trigger only

- Keep Cloud Build trigger on `main`.
- Add checked-in `cloudbuild.yaml` for reproducible builds.
- Lock Dockerfile path and build context to repo root.
- Add build-time tests step.
- Do not add a second deployer that updates the same service and branch.

Cross-cutting Phase 3 improvements

- Staging vs prod: `trr-backend-staging` and `trr-backend` with separate Secret Manager secrets.
- Operational guardrails: max instances cap in both environments and confirm logs are clean (no secrets).
- WebSocket behavior: clients should handle reconnect gracefully.

Rollback

- Redeploy a prior Cloud Run revision, or redeploy a previous image tag (commit SHA) from Artifact Registry.

## Phase 3 Acceptance Criteria

- Merge to `main` updates Cloud Run with a traceable image tag.
- Deploy auth is keyless (WIF) or otherwise locked down.
- Staging/prod separation plan exists, even if staging is not created yet.
