# Final Supabase connection audit and donor transition inventory — Task 24 Plan

Repo: TRR-Backend
Last updated: 2026-04-02

## Goal
Finish the Supabase/Postgres connection audit across `TRR-Backend`, `screenalytics`, and `TRR-APP`, remove the remaining runtime env ambiguity, and capture the donor/runtime inventory the upcoming DeepFace reset will need.

## Status Snapshot
Implemented for the current runtime contract audit. The canonical runtime lane is now documented and verified as `TRR_DB_URL` first, then `TRR_DB_FALLBACK_URL`, with the Supavisor session pooler on `pooler.supabase.com:5432` as the default lane. Remaining unresolved items are migration inputs for the DeepFace reset, not active DB contract drift.

## Current Repo Truth

| Repo | Branch | HEAD | Worktree |
|---|---|---|---|
| TRR-Backend | `feat/supabase-unified-hardening` | `e0a74c9bca9f468f37c6ba9df576f63729a0f355` | Dirty in unrelated `admin_show_reads` files; preserved |
| screenalytics | `feat/supabase-unified-hardening` | `ec9c764cc54a3b3a46ebb47690b6f947b6c17d0a` | Clean |
| TRR-APP | `feat/supabase-unified-hardening` | `7632151729bdd447253020640d568cbafe468507` | Dirty in unrelated app/docs/test files; preserved |

## Scope

### Phase 1: Canonical runtime contract audit
Confirm the live runtime Postgres contract in all three repos and update only the code/docs that still imply ambiguous env precedence or unsupported lanes.

Files changed:
- `screenalytics/apps/api/main.py`
- `screenalytics/apps/api/services/supabase_db.py`
- `screenalytics/apps/api/services/trr_metadata_db.py`
- `screenalytics/apps/api/services/runs_v2.py`
- `screenalytics/apps/api/services/run_export.py`
- `screenalytics/apps/api/routers/config.py`
- `screenalytics/apps/api/routers/video_assets_v2.py`
- `screenalytics/apps/api/routers/shows_v2.py`
- `screenalytics/apps/api/routers/facebank_v2.py`
- `screenalytics/apps/api/routers/episodes.py`
- `screenalytics/tools/dev-up.sh`
- `TRR-APP/apps/web/.env.example`
- `TRR-APP/apps/web/README.md`
- `TRR-APP/apps/web/POSTGRES_SETUP.md`

### Phase 2: Donor transition inventory
Capture the app-visible `screenalytics` dependencies, the donor modules that must be preserved, and the env/service dependencies the DeepFace reset must eliminate.

Files changed:
- `TRR-Backend/docs/cross-collab/TASK24/*`
- `screenalytics/docs/cross-collab/TASK13/*`
- `TRR-APP/docs/cross-collab/TASK23/*`

## Locked Contracts
- Runtime Postgres precedence is `TRR_DB_URL` then `TRR_DB_FALLBACK_URL`.
- The default runtime lane is Supavisor session mode on `pooler.supabase.com:5432`.
- Transaction pooler (`:6543`) and direct host (`db.<project>.supabase.co`) are not default runtime lanes anywhere.
- `SUPABASE_JWT_SECRET` in `TRR-Backend` is local JWT verification only, not a network connection.
- `TRR-APP` server-admin Supabase access remains an active server-side dependency.
- `TRR-APP` browser Supabase remains Flashback-scoped, not app-global.

## Final Audit Matrix

| Repo | File path | Connection type | Env vars used | Host/port class | Runtime vs tooling vs test | Current status | Action taken |
|---|---|---|---|---|---|---|---|
| TRR-Backend | `trr_backend/db/connection.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL` | `session`, `local` | Runtime | Correct | No code change; remains canonical source-of-truth for runtime DB resolution |
| TRR-Backend | `trr_backend/db/pg.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL`, `TRR_DB_POOL_*`, timeout vars | `session`, `local` | Runtime | Correct | No code change; pooling and lane classification already enforce the canonical contract |
| TRR-Backend | `api/main.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL` | `session`, `local` | Runtime | Correct | No code change; startup validation already rejects transaction/direct lanes |
| TRR-Backend | `trr_backend/security/jwt.py` | `jwt_local_validation` | `SUPABASE_JWT_SECRET`, `TRR_CORE_SUPABASE_URL`, `SUPABASE_URL`, `TRR_DB_URL`, `TRR_DB_FALLBACK_URL` | N/A | Runtime | Correct | No code change; explicitly classified as local JWT verification only |
| TRR-Backend | `trr_backend/db/preflight.py` | `tooling_only` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL`, `DATABASE_URL` | Mixed | Tooling | Correct / deprecated legacy gated | No code change; tooling-only `DATABASE_URL` support stays quarantined |
| TRR-Backend | `scripts/_db_url.py` | `tooling_only` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL`, `DATABASE_URL`, `SUPABASE_DB_URL` | Mixed | Tooling | Correct / deprecated legacy gated | No code change; explicit tooling compatibility remains quarantined |
| screenalytics | `apps/api/services/supabase_db.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL` | `session`, `local` | Runtime | Correct after wording cleanup | Updated active runtime errors to mention both canonical envs; `transition runtime` + `donor logic to port` |
| screenalytics | `apps/api/main.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL`, `SCREENALYTICS_SERVICE_TOKEN` | `session`, `local` | Runtime | Correct after wording cleanup | Updated startup missing-env logs to stop implying only `TRR_DB_URL` exists; `transition runtime` |
| screenalytics | `apps/api/services/trr_metadata_db.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL` | `session`, `local` | Runtime | Correct after wording cleanup | Updated runtime docstrings/errors to reflect fallback support; `donor logic to port` |
| screenalytics | `apps/api/services/runs_v2.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL` | `session`, `local` | Runtime | Correct after wording cleanup | Updated DB-required error text; `transition runtime`, `donor only if cast-screentime survives` |
| screenalytics | `apps/api/routers/config.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL`, `SCREENALYTICS_FAKE_DB` | `session`, `local` | Runtime | Correct after wording cleanup | Updated DB health route text to reflect both runtime envs; `transition runtime` |
| screenalytics | `apps/api/services/run_export.py` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL`, `SCREENALYTICS_FAKE_DB` | `session`, `local` | Runtime | Correct after wording cleanup | Updated operator-facing report text for DB-not-configured cases; `transition runtime`, `retire after parity` |
| screenalytics | `tools/dev-up.sh` | `tooling_only` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL`, `SCREENALYTICS_FAKE_DB` | Mixed | Tooling | Correct after wording cleanup | Updated local operator messaging; `retire after parity` |
| screenalytics | `docs/ops/deployment/DEPLOYMENT_RENDER.md` | `tooling_only` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL` | `session` expected | Deploy docs | Correct | No code/doc change; current deploy surface remains transitional but accurate enough for rollback/migration safety |
| TRR-APP | `apps/web/src/lib/server/postgres.ts` | `raw_postgres` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL` | `session`, `local` | Runtime | Correct | No code change; server-side raw Postgres already uses the canonical contract |
| TRR-APP | `apps/web/src/lib/server/supabase-trr-admin.ts` | `supabase_rest_server_admin` | `TRR_CORE_SUPABASE_URL`, `TRR_CORE_SUPABASE_SERVICE_ROLE_KEY` | HTTPS Supabase REST | Runtime | Correct / active | No code change; docs updated so these are treated as active server-auth envs |
| TRR-APP | `apps/web/src/lib/server/auth.ts` | `supabase_rest_server_admin` | `TRR_AUTH_PROVIDER`, `TRR_CORE_SUPABASE_URL`, `TRR_CORE_SUPABASE_SERVICE_ROLE_KEY` | HTTPS Supabase REST | Runtime | Correct / active | No code change; audit confirms this is live server auth infrastructure |
| TRR-APP | `apps/web/src/lib/supabase/client.ts` | `supabase_rest_browser` | `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY` | HTTPS Supabase REST | Runtime (browser) | Correct / route-scoped | No code change; remains Flashback-only browser client |
| TRR-APP | `apps/web/src/lib/flashback/supabase.ts` | `supabase_rest_browser` | `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY` | HTTPS Supabase REST | Runtime (browser) | Route-scoped, non-core | No code change; missing env still breaks Flashback routes specifically, not the app build/runtime overall |
| TRR-APP | `apps/web/scripts/auto-categorize-flairs.ts` | `tooling_only` | `TRR_DB_URL`, `TRR_DB_FALLBACK_URL`, `DATABASE_URL` | Mixed | Tooling | Correct / tooling-only | No code change; keep `DATABASE_URL` quarantined to tooling |
| TRR-APP | `apps/web/scripts/run-migrations.mjs` | `tooling_only` | `DATABASE_URL` | Mixed | Tooling | Correct / tooling-only | No code change; migration script remains explicitly `DATABASE_URL`-driven |

## Active App-Facing Dependencies That Still Touch Screenalytics
- `/screenalytics` picker in TRR-APP is app-owned UI and does **not** directly call the separate `screenalytics` service. It remains a parity entry point for the reset.
- Admin person-gallery facebank seed toggle remains app-facing through `TRR-APP` proxy -> `TRR-Backend` endpoint. The proxy is app-owned, but related image-analysis/fallback behavior in `TRR-Backend` still references `trr_backend.clients.screenalytics`.
- Admin image-analysis flows on show/person workspaces still indirectly depend on backend `screenalytics` HTTP client behavior for people count, face centroid, and auto-thumbnail crop fallback.
- Admin cast-screentime flow remains app-facing through `TRR-APP` -> `TRR-Backend` proxy routes, and `TRR-Backend` still delegates execution to the separate `screenalytics` runtime today.

## Donor Files the DeepFace Reset Must Preserve
- `screenalytics/apps/api/services/trr_metadata_db.py`
- `screenalytics/apps/api/services/supabase_db.py`
- `screenalytics/apps/api/services/trr_ingest.py`
- `screenalytics/apps/api/routers/cast.py`
- `screenalytics/apps/api/routers/metadata.py`
- `screenalytics/docs/reference/api/API_SURFACE.md`
- `screenalytics/docs/reference/facebank.md`
- `screenalytics/apps/api/services/cast_screentime.py` and `screenalytics/apps/api/services/runs_v2.py` only if cast-screentime parity remains in scope

## Env And Service Dependencies The DeepFace Reset Must Eliminate
- `TRR-Backend` outbound HTTP dependency on `SCREENALYTICS_API_URL`
- `TRR-Backend` and `screenalytics` mutual service auth dependency on `SCREENALYTICS_SERVICE_TOKEN`
- `TRR-Backend` code paths that still assume a separate `screenalytics` HTTP runtime:
  - `trr_backend/clients/screenalytics.py`
  - `trr_backend/clients/screenalytics_cast_screentime.py`
  - `api/routers/admin_cast_screentime.py`
  - image-analysis fallback paths in `api/routers/admin_person_images.py`, `api/routers/admin_scrape.py`, and `api/routers/admin_show_sync.py`
- Backend storage/runtime tied directly to `screenalytics.*` tables:
  - `trr_backend/repositories/cast_screentime.py`
  - `trr_backend/repositories/screenalytics_runs.py`
  - `trr_backend/vision/people_count_engine.py`

## Still Missing
- `TRR-Backend` still needs the separate `screenalytics` runtime for cast-screentime and some image-analysis fallback paths. That is a real migration dependency, not a DB contract ambiguity.
- Flashback browser envs remain route-scoped in `TRR-APP`; they are not core-app blockers, but `/flashback/*` still needs `NEXT_PUBLIC_SUPABASE_URL` and `NEXT_PUBLIC_SUPABASE_ANON_KEY`.
- `screenalytics` test helper scripts still mention `TRR_DB_URL` only. They are tooling/test-only and were intentionally left alone.

## Out of Scope
- Reworking the `screenalytics` service architecture before the DeepFace reset
- Wiring new Flashback behavior
- Replacing the old recognition pipeline in this task
- Deleting historical archive/evidence docs

## Acceptance Criteria
1. The runtime DB contract is unambiguous in active code and operator-facing docs.
2. `screenalytics` wording drift no longer implies `TRR_DB_URL` is the only live runtime source when `TRR_DB_FALLBACK_URL` is also supported.
3. `TRR-APP` docs correctly classify server-side Supabase auth envs and remove the stale `SCREENALYTICS_API_URL` app env entry.
4. The task docs capture the final audit matrix, app-facing dependency list, donor file list, and migration dependencies to eliminate.
5. Targeted validations pass for changed repos.
