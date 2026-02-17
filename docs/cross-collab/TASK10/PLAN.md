# Social Admin Incremental Sync + Runs UX Hardening — Task 10 Plan

Repo: TRR-Backend
Last updated: February 17, 2026

## Goal

Ship backend-owned incremental social ingest reconciliation so reruns avoid unnecessary comment resync, while preserving additive API/database contracts for admin UX upgrades.

## Status Snapshot

Implemented and validated in TRR-Backend; downstream screenalytics compatibility check completed; TRR-APP consumer updates completed.

## Scope

### Phase 1: Additive schema + ingest contract
- Add migration `0126_social_comment_lifecycle_flags.sql` for comment lifecycle flags and supporting indexes.
- Extend ingest request contract with `sync_strategy` (`incremental` default, `full_refresh` override).
- Persist `sync_strategy` and requested platform scope in run config.

Files to change:
- `supabase/migrations/0126_social_comment_lifecycle_flags.sql` — additive columns/indexes.
- `api/routers/socials.py` — request model and pass-through.
- `trr_backend/repositories/social_season_analytics.py` — ingest strategy wiring + run config.

### Phase 2: Incremental reconciliation behavior
- Replace simplistic comment refresh skip with policy-driven decision matrix.
- Add per-post lifecycle snapshot loading for active/missing counts and check recency.
- Upsert observed comments as seen (`is_missing=false`, `missing_at=null`, `last_seen_*` updates).
- Apply conservative missing marking only for complete fetches.

Files to change:
- `trr_backend/repositories/social_season_analytics.py` — decision logic + lifecycle updates.

### Phase 3: Verification coverage
- Add repository tests for decision matrix, missing-mark safety, reappearance clearing, run config persistence.
- Add router tests for `sync_strategy` default/override pass-through.

Files to change:
- `tests/repositories/test_social_season_analytics.py`
- `tests/api/routers/test_socials_season_analytics.py`

## Out of Scope

- Breaking existing social analytics response shapes.
- Queue semantics/status transition redesign.
- screenalytics runtime refactors.

## Locked Contracts

### Additive-only API and DB changes
- Existing consumers remain valid without `sync_strategy`.
- Existing analytics totals behavior remains unchanged.

### Missing-comment policy
- Missing comments are flagged, not deleted.
- Missing rows remain included in analytics totals (diagnostic flag only).

## Acceptance Criteria

1. Ingest accepts `sync_strategy` and stores it in run config.
2. Incremental reruns refresh comments only when policy conditions require it.
3. Missing comments are marked conservatively and reappearance clears missing flags.
4. Task 10 docs are synchronized with TRR-APP TASK9 and screenalytics TASK7.
