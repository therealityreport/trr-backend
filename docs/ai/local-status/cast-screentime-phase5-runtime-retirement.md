# Cast Screentime Phase 5 Runtime Retirement

Date: 2026-04-03

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-04-03
  current_phase: "phase 5 runtime retirement implemented"
  next_action: "Run one real screentime asset through the backend-only lane for final operational confidence."
  detail: self
```

## What Landed
- `trr_backend/services/retained_cast_screentime_dispatch.py` is now backend-only for screentime execution and clip generation.
- The rollback-only donor client `trr_backend/clients/screenalytics_cast_screentime.py` was removed from the active backend runtime.
- `api/main.py` no longer treats `SCREENALYTICS_SERVICE_TOKEN` as a required deployed-runtime secret for screentime production operation.
- Legacy `screenalytics`-tagged backend routes now accept internal-admin JWT auth, so screentime no longer depends on a dedicated Screenalytics service token.

## Canonical Runtime After Phase 5
- Backend-owned screentime execution:
  - `trr_backend/services/retained_cast_screentime_runtime.py`
  - `trr_backend/services/retained_cast_screentime_dispatch.py`
- Backend-owned screentime review/publication:
  - `trr_backend/services/retained_cast_screentime_review.py`
  - `api/routers/admin_cast_screentime.py`
- Stable operator surface:
  - `TRR-APP/apps/web/src/app/admin/cast-screentime/CastScreentimePageClient.tsx`
  - `TRR-APP/apps/web/src/app/api/admin/trr-api/cast-screentime/[...path]/route.ts`

## Residual Legacy Surfaces
- `/api/v1/screenalytics/*` and `/api/v1/screenalytics/v2/*` remain compatibility routes, not active screentime production dependencies.
- `trr_backend/clients/screenalytics.py` still exists for non-screentime naming and image-analysis transition work outside this phase.
- The standalone `screenalytics` repo remains donor/reference material until broader retirement work closes unrelated legacy paths.

## Verification
- `pytest -q TRR-Backend/tests/services/test_retained_cast_screentime_dispatch.py TRR-Backend/tests/test_startup_config.py TRR-Backend/tests/api/test_startup_validation.py TRR-Backend/tests/api/test_screenalytics_runs_v2.py TRR-Backend/tests/api/test_screenalytics_ingest_endpoints.py TRR-Backend/tests/api/test_admin_cast_screentime.py`
- `ruff check TRR-Backend/api/main.py TRR-Backend/api/screenalytics_auth.py TRR-Backend/trr_backend/services/retained_cast_screentime_dispatch.py TRR-Backend/tests/services/test_retained_cast_screentime_dispatch.py TRR-Backend/tests/test_startup_config.py TRR-Backend/tests/api/test_startup_validation.py TRR-Backend/tests/api/test_screenalytics_runs_v2.py TRR-Backend/tests/api/test_screenalytics_ingest_endpoints.py TRR-Backend/tests/api/test_admin_cast_screentime.py`
- `ruff format --check TRR-Backend/api/main.py TRR-Backend/api/screenalytics_auth.py TRR-Backend/trr_backend/services/retained_cast_screentime_dispatch.py TRR-Backend/tests/services/test_retained_cast_screentime_dispatch.py TRR-Backend/tests/test_startup_config.py TRR-Backend/tests/api/test_startup_validation.py TRR-Backend/tests/api/test_screenalytics_runs_v2.py TRR-Backend/tests/api/test_screenalytics_ingest_endpoints.py TRR-Backend/tests/api/test_admin_cast_screentime.py`
