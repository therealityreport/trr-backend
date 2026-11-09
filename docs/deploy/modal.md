# Modal production deployment

TRR uses Vercel for the web application and Modal for the API and background jobs.
This setup was verified against production on 2026-11-09.

## Production targets

- Vercel team: `team_EUsG2kN9TAvVDGOu4yZVEoCX`; project: `trr-app`.
- Production application: `https://trr-app.vercel.app`.
- Vercel server-only `TRR_API_URL`: `https://admin-56995--trr-backend-api.modal.run`.
- Modal profile/workspace: `admin-56995`; environment: `main`.
- Modal app: `trr-backend-jobs`; API function: `serve_backend_api`.
- Supabase: `vwxfvzutyufrkhfgoeaa` (`trr-core`).

The workspace `docs/workspace/deployment-targets.json` owns target identity.
Render is retired; no Render API, blueprint, deployment, or billing plan is
required for this request path. See [the retirement pointer](render.md) for dates.

## Deployment

From `TRR-Backend`, use the maintained deployment wrapper. It validates the
workspace and app identity, runs readiness, and performs an API cold-start canary:

```bash
.venv/bin/python scripts/modal/deploy_backend.py --env main --app-name trr-backend-jobs
```

Confirm the exact source revision and review all runtime changes before deployment.
Preserve unrelated checkout edits; use a reviewed source export if the checkout
contains work that is not ready for release. Keep the prior Modal deployment ID
and rollback command in the release record.

Runtime credentials belong in the existing Modal named secrets
`trr-backend-runtime` and `trr-social-auth`. Never commit or print secret values.
Vercel keeps the server-side credentials required by its API proxy. Hosted media
uses the `OBJECT_STORAGE_*` contract; see [R2 setup](R2-setup.md).

## Verification

Use the same explicit environment for deployment and verification:

```bash
.venv/bin/python scripts/modal/verify_modal_readiness.py --env main --json
```

Require all required functions and named secrets to resolve. Then verify:

- Modal `GET /health/live`, `/health`, and `/health/runtime`.
- Database connectivity and the expected execution backend.
- An authenticated Vercel `/api/admin/trr-api/*` request through the production alias.
- A targeted worker or admin-operation smoke for the changed path.
- Dispatching consumers point at the same Modal app and environment.

Readiness and a source test alone do not establish successful production behavior.
Record deployment identity, source identity, verification outcomes, and rollback.

## Local development

Use workspace `make dev-hybrid` and the declared HTTPS routes:
`https://admin.trr.localhost`, `https://trr.localhost`, and `https://api.trr.localhost`.
Local startup checks database, Modal, and scraper dependencies without a Render
subscription. See [the API development guide](../api/run.md).
