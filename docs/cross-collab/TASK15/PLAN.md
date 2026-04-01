# Credits slug and IMDb refresh — Task 15 Plan

Repo: TRR-Backend
Last updated: 2026-03-30

## Goal
Credits slug and IMDb refresh

## Status Snapshot
Implementation complete; targeted backend validation passed.

## Scope

### Phase 1: Implement
Implement IMDb full-credits crew ingestion, cast-only consumer views, and a dedicated admin credits read endpoint.

Files to change:
- `trr_backend/integrations/imdb/fullcredits_cast_parser.py`
- `trr_backend/ingestion/show_importer.py`
- `trr_backend/repositories/admin_show_reads.py`
- `api/routers/admin_show_reads.py`
- `supabase/migrations/20260330113000_make_v_show_cast_self_only.sql`
- `tests/integrations/imdb/test_fullcredits_cast_parser.py`

## Out of Scope
- Items owned by other repos unless explicitly required.

## Locked Contracts
- Keep shared API/schema contracts synchronized across affected repos.

## Acceptance Criteria
1. TRR-Backend changes complete and validated.
2. Cross-repo dependency order is respected.
3. Fast checks pass for TRR-Backend.
4. Task docs remain synchronized.
