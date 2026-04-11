# Control-plane read seams phase 1B-2A

Last updated: 2026-04-11

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: recent
  last_updated: 2026-04-11
  current_phase: "run-read, shared-status read, and run-lifecycle entrypoints extracted into the control-plane package with the legacy monolith left as a compatibility facade"
  next_action: "continue Phase 2A by extracting deeper run-lifecycle helpers and sync-session mutation paths without mixing in dispatch, stale recovery, or worker-claim policy"
  detail: self
```

## What changed

- Added `trr_backend/socials/control_plane/run_reads.py` for `list_runs`, `list_run_summaries`, and `get_run_progress_snapshot`.
- Added `trr_backend/socials/control_plane/shared_status_reads.py` for `get_season_shared_status` and `list_shared_runs`.
- Added `trr_backend/socials/control_plane/run_lifecycle.py` for `_set_run_status`, `_update_run_summary`, `_finalize_run_status`, and `reconcile_run_summaries`.
- Updated `trr_backend/socials/control_plane/dispatch.py`, `shared_accounts.py`, and `recovery.py` so canonical control-plane imports now resolve through extracted modules instead of re-exporting the legacy monolith implementations for those seams.
- Reduced `trr_backend/repositories/social_season_analytics.py` to a compatibility facade for the extracted read and lifecycle entrypoints, preserving the legacy import path for routes and existing callers.
- Moved focused coverage into:
  - `tests/repositories/test_social_run_reads_repository.py`
  - `tests/repositories/test_social_shared_status_reads_repository.py`
  - `tests/repositories/test_social_run_lifecycle_repository.py`
- Removed the moved run-read, shared-status, and lifecycle cases from `tests/repositories/test_social_season_analytics.py` so the hotspot test file continues shrinking.

## Contract notes

- `api/routers/socials.py` still uses the legacy repository import path; route behavior and response shapes were intentionally kept stable.
- No `screenalytics` or `TRR-APP` follow-through was required in this slice because backend contracts did not change.
- The extracted lifecycle module intentionally still relies on legacy helper internals for some summary and classification support. This is a narrow Phase 2A cut, not the full write-path extraction.

## Validation snapshot

- `python -m pytest tests/repositories/test_social_run_reads_repository.py tests/repositories/test_social_shared_status_reads_repository.py tests/repositories/test_social_run_lifecycle_repository.py tests/repositories/test_social_control_plane_imports.py -q` -> `23 passed`
- `python -m pytest tests/api/routers/test_socials_season_analytics.py -q -k "queue_status or live_status or sync_session or run_progress"` -> `19 passed, 174 deselected`
- `ruff check trr_backend/socials/control_plane/run_reads.py trr_backend/socials/control_plane/shared_status_reads.py trr_backend/socials/control_plane/run_lifecycle.py trr_backend/socials/control_plane/dispatch.py trr_backend/socials/control_plane/shared_accounts.py trr_backend/socials/control_plane/recovery.py trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_run_reads_repository.py tests/repositories/test_social_shared_status_reads_repository.py tests/repositories/test_social_run_lifecycle_repository.py tests/repositories/test_social_control_plane_imports.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` -> clean
- `ruff format --check trr_backend/socials/control_plane/run_reads.py trr_backend/socials/control_plane/shared_status_reads.py trr_backend/socials/control_plane/run_lifecycle.py trr_backend/socials/control_plane/dispatch.py trr_backend/socials/control_plane/shared_accounts.py trr_backend/socials/control_plane/recovery.py trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_run_reads_repository.py tests/repositories/test_social_shared_status_reads_repository.py tests/repositories/test_social_run_lifecycle_repository.py tests/repositories/test_social_control_plane_imports.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` -> clean

## Remaining risks

- The extracted lifecycle seam still calls legacy helper internals such as `_recompute_run_summary_from_jobs`, `_persist_run_counters_and_summary`, and shared-catalog classify follow-up logic. That coupling is intentional for the first safe Phase 2A slice.
- Dispatch, stale recovery, worker-claim policy, and media-mirror orchestration remain in the monolith and should stay separate from the next extraction slice.
- A broader repository subset still contains one unrelated pre-existing failure in `tests/repositories/test_social_season_analytics.py::test_build_catalog_run_progress_alerts_surfaces_runtime_drift_and_backlog`. That alerting failure was not widened into this seam extraction.
