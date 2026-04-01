# Instagram catalog gap analysis and operator guidance — Task 16 Plan

Repo: TRR-Backend
Last updated: 2026-03-30

## Goal
Instagram catalog gap analysis and operator guidance

## Status Snapshot
Implemented in TRR-Backend and validated with targeted repository and router tests.

## Scope

### Phase 1: Implement
Add an owner-vs-catalog gap classifier for Instagram account catalogs, expose it through an admin read route, and preserve existing recovery semantics.

Files to change:
- `trr_backend/repositories/social_season_analytics.py`
- `api/routers/socials.py`
- `tests/repositories/test_social_season_analytics.py`
- `tests/api/routers/test_socials_season_analytics.py`

## Out of Scope
- Items owned by other repos unless explicitly required.
- Changing `Backfill Posts` semantics away from full-history frontier crawling.
- Adding a brand new ingestion mode beyond bounded-window reuse.

## Locked Contracts
- Keep shared API/schema contracts synchronized across affected repos.
- New additive read route: `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/gap-analysis`
- Existing write routes remain unchanged: `backfill`, `sync-recent`, `sync-newer`, `resume-tail`

## Acceptance Criteria
1. TRR-Backend changes complete and validated.
2. Cross-repo dependency order is respected.
3. Targeted backend checks pass for the new gap-analysis route and classifier behavior.
4. Task docs remain synchronized.
