# Admin “Sync/Refresh Shows” Endpoints — Task 6 Plan

Repo: TRR-Backend
Last updated: February 24, 2026

## Goal

Expose admin-only endpoints that let TRR-APP trigger the existing TRR-Backend show import/sync logic (the same code paths used by the sync scripts).

## Status Snapshot

Implemented (router + wiring + tests). Follow-up stabilization applied on February 24, 2026 for RHOSLC closeout:
- immediate first-event emission in `refresh/stream` and `refresh-photos/stream`
- heartbeat continuation during long steps
- person refresh/reprocess stream startup + heartbeat + request-id diagnostics finalized

## Scope

### Phase 1: Admin Router + List Sync Endpoint

Add an admin router that supports a one-click "Sync from Lists" action.

Files to change:
- `api/routers/admin_show_sync.py` — `POST /api/v1/admin/shows/sync-from-lists`

Behavior:
- Imports candidates from IMDb/TMDb lists and enriches show metadata.
- Does **not** fetch TMDb images during list sync.

### Phase 2: Per-Show Refresh Endpoint

Add a per-show endpoint that maps refresh targets to existing sync script entrypoints.

Files to change:
- `api/routers/admin_show_sync.py` — `POST /api/v1/admin/shows/{show_id}/refresh`

Targets:
- `details`
- `seasons_episodes`
- `photos` (show + season + episode images; includes IMDb mediaindex via default script source)
- `cast_credits` (credits + credit occurrences)

### Phase 2.5: Streaming Progress (SSE)

Expose a streaming variant for per-show refresh so TRR-APP can render accurate progress bars.

Files to change:
- `api/routers/admin_show_sync.py` — `POST /api/v1/admin/shows/{show_id}/refresh/stream`

Behavior:
- Streams `progress` events with `current/total` step counts and a final `complete` event with the same response payload as the non-stream endpoint.

### Phase 3: Wire Router Into FastAPI App

Files to change:
- `api/main.py` — include `admin_show_sync.router` under `/api/v1`

### Phase 4: Tests

Files to change:
- `tests/api/routers/test_admin_show_sync.py` — endpoint auth + basic behavior with patched dependencies

## Out of Scope

- Background jobs / async workers / queues.
- Changing any existing TRR-Backend public API response shapes.

## Locked Contracts

- Auth: endpoints must require `AdminUser` (service role JWT allowed).
- TRR-APP calls TRR-Backend via `TRR_API_URL` normalized to `/api/v1`.
- "Sync from Lists" is **Import + Enrich** and does **not** fetch show images.

## Acceptance Criteria

1. `POST /api/v1/admin/shows/sync-from-lists` is admin-only and returns created/updated/skipped counts.
2. `POST /api/v1/admin/shows/{show_id}/refresh` is admin-only and returns per-target status.
3. New endpoints are additive; no existing endpoint shapes change.
4. `ruff check . && ruff format --check . && pytest -q` passes.
5. Cross-collab docs are aligned with TRR-APP TASK5.
