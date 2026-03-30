# Show refresh full pipeline gallery media — Task 13 Plan

Repo: TRR-Backend
Last updated: 2026-03-27

## Goal
Show refresh full pipeline gallery media

## Status Snapshot
Implemented and validated.

## Scope

### Phase 1: Implement
Add a gallery-only fast-path contract to the photos refresh stream so the app can run
full refresh as unified refresh plus gallery media without duplicating cast-photo work.

Files to change:
- `api/routers/admin_show_sync.py`
- `tests/api/routers/test_admin_show_sync.py`

## Out of Scope
- Items owned by other repos unless explicitly required.

## Locked Contracts
- Keep shared API/schema contracts synchronized across affected repos.

## Acceptance Criteria
1. TRR-Backend changes complete and validated.
2. Cross-repo dependency order is respected.
3. Fast checks pass for TRR-Backend.
4. Task docs remain synchronized.

## Delivered
- Added `skip_cast_photos` to `RefreshShowPhotosRequest`.
- Updated `/refresh-photos/stream` to skip cast-photo fetch, mirror, prune, auto-count,
  and text-overlay stages when `skip_cast_photos=true`.
- Kept show, season, and episode media stages available for the gallery pass.
- Added backend coverage for the gallery-only skip path.
