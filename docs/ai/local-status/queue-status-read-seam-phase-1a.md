# Queue-status read seam phase 1A

Last updated: 2026-04-11

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: recent
  last_updated: 2026-04-11
  current_phase: "queue-status read seam extracted into the control-plane package with the legacy monolith left as a compatibility facade"
  next_action: "continue Phase 1 with sync-session and adjacent live/admin read surfaces that share the queue-health seam"
  detail: self
```

## What changed

- Added `trr_backend/socials/control_plane/queue_status.py` as the extracted Phase 1A read seam for queue-status assembly and cache invalidation.
- Updated `trr_backend/socials/control_plane/worker_health.py` so the canonical control-plane import surface now serves `get_queue_status` from the extracted seam instead of re-exporting the legacy monolith implementation.
- Reduced `trr_backend/repositories/social_season_analytics.py` to a compatibility facade for `get_queue_status` and `_invalidate_queue_status_cache`, preserving the legacy public import path for routes and callers.
- Moved focused queue-status repository coverage into `tests/repositories/test_social_queue_status.py`.
- Removed the moved queue-status cases from `tests/repositories/test_social_season_analytics.py` so the hotspot test file shrinks instead of duplicating coverage.

## Contract notes

- `api/routers/socials.py` still imports `get_queue_status` from `trr_backend.repositories.social_season_analytics`; route behavior and response shapes were intentionally kept stable.
- No `screenalytics` or `TRR-APP` follow-through was required because this slice preserved the backend import and payload contracts.

## Validation snapshot

- `python -m pytest tests/repositories/test_social_queue_status.py tests/repositories/test_social_control_plane_imports.py -q` -> `14 passed`
- `python -m pytest tests/api/routers/test_socials_season_analytics.py -q -k "queue_status or live_status"` -> `7 passed, 186 deselected`
- `python -m pytest tests/repositories -q -k "queue_status"` -> `12 passed, 857 deselected`
- `ruff check trr_backend/socials/control_plane/queue_status.py trr_backend/socials/control_plane/worker_health.py trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_queue_status.py tests/repositories/test_social_control_plane_imports.py tests/repositories/test_social_season_analytics.py` -> clean
- `ruff format --check trr_backend/socials/control_plane/queue_status.py trr_backend/socials/control_plane/worker_health.py trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_queue_status.py tests/repositories/test_social_control_plane_imports.py tests/repositories/test_social_season_analytics.py` -> clean

## Remaining risks

- Phase 1A extracted the queue-status read seam only. Adjacent sync-session and admin-operation read helpers still live in the monolith and will continue to pull on the same dependency graph until Phase 1B.
- The extracted seam still depends on legacy helper functions and cache storage inside `social_season_analytics.py`; this is intentional for compatibility, but it means the hotspot is reduced rather than eliminated in this slice.
