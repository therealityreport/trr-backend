# Cast Screentime Phase 3 Backend Execution Port

Date: 2026-04-03

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-04-03
  current_phase: "phase 3 backend execution port implemented"
  next_action: "Plan Phase 4 review/publication cutover against the retained backend runtime."
  detail: self
```

## What Landed
- `TRR-Backend` now owns a retained screentime runtime in `trr_backend/services/retained_cast_screentime_runtime.py`.
- `retained_cast_screentime_dispatch.py` is no longer a hard-coded donor proxy. It selects between backend execution and donor HTTP through `CAST_SCREENTIME_RUNTIME_MODE`.
- Backend mode supports asynchronous run enqueue, retained run finalization, retained artifact persistence, backend-owned proof-frame uploads, and backend-generated segment clips.
- The admin route surface stays stable. `api/routers/admin_cast_screentime.py` still launches runs and clip generation through the retained dispatch seam.

## Canonical Contracts After Phase 3
- Run launch remains on `POST /api/v1/admin/cast-screentime/video-assets/{video_asset_id}/runs`.
- Segment clip generation remains on `POST /api/v1/admin/cast-screentime/runs/{run_id}/segments/{segment_key}/clip`.
- Backend execution persists retained outputs into:
  - `ml.screentime_runs`
  - `ml.screentime_artifacts`
  - `ml.screentime_segments`
  - `ml.screentime_evidence`
  - `ml.screentime_review_state` for excluded sections
  - `ml.screentime_person_metrics`
- Runtime config is enriched per run with:
  - `execution_backend`
  - `artifact_schema_version`
  - `embedding_contract_key`
  - `sampling_stride_seconds`
  - `candidate_cast_snapshot_count`

## Runtime Ownership
- Backend primary lane:
  - `trr_backend/services/retained_cast_screentime_runtime.py`
  - default dispatch mode unless `CAST_SCREENTIME_RUNTIME_MODE=donor_http`
- Rollback lane:
  - `trr_backend/clients/screenalytics_cast_screentime.py`
  - still available for explicit fallback while parity and downstream review/publication cutover finish

## Known Limits
- The backend analyzer is intentionally lean in this phase. It provides retained backend execution, persisted artifacts, and generated clips, but richer title-card/confessional/flashback heuristics remain a later quality pass.
- `SCREENALYTICS_API_URL` and `SCREENALYTICS_SERVICE_TOKEN` are still part of the rollback path and are not removed yet.
- `TRR-APP` stayed unchanged in this phase; Phase 4 owns canonical review/publication cutover.

## Verification
- `ruff check TRR-Backend/api/routers/admin_cast_screentime.py TRR-Backend/trr_backend/services/retained_cast_screentime_dispatch.py TRR-Backend/trr_backend/services/retained_cast_screentime_runtime.py TRR-Backend/trr_backend/repositories/cast_screentime.py TRR-Backend/tests/api/test_admin_cast_screentime.py TRR-Backend/tests/services/test_retained_cast_screentime_dispatch.py TRR-Backend/tests/services/test_retained_cast_screentime_runtime.py`
- `ruff format --check TRR-Backend/api/routers/admin_cast_screentime.py TRR-Backend/trr_backend/services/retained_cast_screentime_dispatch.py TRR-Backend/trr_backend/services/retained_cast_screentime_runtime.py TRR-Backend/trr_backend/repositories/cast_screentime.py TRR-Backend/tests/api/test_admin_cast_screentime.py TRR-Backend/tests/services/test_retained_cast_screentime_dispatch.py TRR-Backend/tests/services/test_retained_cast_screentime_runtime.py`
- `pytest -q TRR-Backend/tests/api/test_admin_cast_screentime.py TRR-Backend/tests/services/test_retained_cast_screentime_dispatch.py TRR-Backend/tests/services/test_retained_cast_screentime_runtime.py`
