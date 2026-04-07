# Cast Screentime Phase 4 Review Publication Cutover

Date: 2026-04-03

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-04-03
  current_phase: "phase 4 review and publication cutover implemented"
  next_action: "Plan Phase 5 runtime retirement and remove the remaining rollback-only Screenalytics boundary."
  detail: self
```

## What Landed
- `TRR-Backend` now owns the canonical screentime review-summary contract in `trr_backend/services/retained_cast_screentime_review.py`.
- `api/routers/admin_cast_screentime.py` exposes `GET /api/v1/admin/cast-screentime/runs/{run_id}/review-summary` and uses reviewed summaries for publication snapshots.
- Episode-class runs still publish into canonical rollups, but supplementary assets can now publish as explicit internal references without affecting canonical episode, season, or show totals.
- `TRR-APP` now renders reviewed totals and publication-mode-aware messaging against the backend contract while keeping the existing admin path stable.

## Canonical Contracts After Phase 4
- Immutable run facts remain:
  - `ml.screentime_segments`
  - `ml.screentime_evidence`
  - `ml.screentime_artifacts`
  - `ml.screentime_person_metrics`
- Mutable review/publication lineage remains separate:
  - `ml.screentime_review_state`
  - `screenalytics.cast_screentime_publish_versions` legacy publication lineage until Phase 5 retirement replaces the remaining donor boundary
- Canonical review/publication reads now expose:
  - `publication_mode`
  - `is_canonical_publication`
  - `reviewed_leaderboard`
  - `excluded_section_count`
  - `excluded_overlap_ms`
  - `decision_counts`
  - `current_publish_version`

## Operator Surface
- `TRR-APP/apps/web/src/app/admin/cast-screentime/CastScreentimePageClient.tsx`
  - loads `review-summary`
  - renders reviewed totals
  - distinguishes canonical publication from supplementary reference publication
  - preserves segments, exclusions, evidence, and generated clips as the review substrate
- `TRR-APP/apps/web/src/app/admin/cast-screentime/run-state.ts`
  - treats supplementary assets as internal-reference publishable instead of permanently standalone

## Known Limits
- Phase 4 does not remove the rollback-only Screenalytics boundary yet.
- `SCREENALYTICS_API_URL` and `SCREENALYTICS_SERVICE_TOKEN` still exist for the remaining donor fallback and retirement work.
- Broader `TRR-APP` build/test debt still exists outside the screentime slice.

## Verification
- `pytest -q TRR-Backend/tests/services/test_retained_cast_screentime_review.py TRR-Backend/tests/api/test_admin_cast_screentime.py`
- `pytest -q TRR-Backend/tests/repositories/test_cast_screentime_repository.py`
- `ruff check TRR-Backend/api/routers/admin_cast_screentime.py TRR-Backend/trr_backend/services/retained_cast_screentime_review.py TRR-Backend/tests/services/test_retained_cast_screentime_review.py TRR-Backend/tests/api/test_admin_cast_screentime.py`
- `ruff format --check TRR-Backend/api/routers/admin_cast_screentime.py TRR-Backend/trr_backend/services/retained_cast_screentime_review.py TRR-Backend/tests/services/test_retained_cast_screentime_review.py TRR-Backend/tests/api/test_admin_cast_screentime.py`
- `pnpm -C TRR-APP/apps/web exec vitest run tests/cast-screentime-page.test.tsx tests/cast-screentime-run-state.test.ts tests/cast-screentime-proxy-route.test.ts`
