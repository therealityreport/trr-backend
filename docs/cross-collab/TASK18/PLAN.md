# Social backfill remediation for Instagram and TikTok — Task 18 Plan

Repo: TRR-Backend
Last updated: 2026-03-30

## Goal
Harden the Instagram and TikTok backfill jobs by fixing retry semantics, stale-write ordering, canonical URL and saves parsing helpers, inline-worker orchestration, and operator diagnostics without widening the change into unrelated scraper or cross-repo contract work.

## Status Snapshot
Implemented in TRR-Backend. Targeted lint and tests passed. Full schema-doc validation is blocked by unrelated pre-existing schema doc drift outside this remediation slice.

## Scope

### Phase 0: Repository contracts and shared helpers
Add Instagram metadata retry-state support, preserve stronger stored `post_format`, centralize TikTok canonical URL and saves parsing, expose TikTok comment-media mirror counters, and harden shared diagnostics / resume helpers.

Files to change:
- `trr_backend/repositories/social_season_analytics.py` — repository contracts, helpers, inline worker registration, frontier resume, TikTok/Instagram remediation
- `supabase/migrations/20260330213000_add_instagram_metadata_retry_state.sql` — minimal retry-state columns for Instagram metadata enrichment

### Phase 1: Instagram backfill scripts
Repair the Instagram metadata/media and reel-view backfills so they respect retry eligibility, persist enriched rows before enqueueing mirror work, fail fast on auth/cap misconfiguration, and report degraded detail-refresh outcomes clearly.

Files to change:
- `scripts/socials/backfill_instagram_metadata_and_media.py`
- `scripts/socials/backfill_instagram_reel_views_full_history.py`
- `tests/scripts/test_backfill_instagram_metadata_and_media.py`
- `tests/scripts/test_backfill_instagram_reel_views_full_history.py`

### Phase 2: TikTok backfill scripts
Replace one-off saves parsing with shared behavior, require explicit targeting, filter candidates to missing-or-zero saves, and use canonical-first URL retry order.

Files to change:
- `scripts/socials/backfill_tiktok_saves.py`
- `tests/scripts/test_backfill_tiktok_saves.py`

### Phase 3: Shared orchestration and API wiring
Allow inline fallback to claim child jobs safely, preserve resumable frontier cursors, guard concurrent catalog starts, and enforce TikTok preview auth preflight.

Files to change:
- `api/routers/socials.py`
- `tests/repositories/test_social_backfill_remediation.py`
- `tests/api/routers/test_socials_tiktok_preview.py`
- `tests/scripts/test_backfill_social_media_mirror_jobs.py`

## Out of Scope
- screenalytics or TRR-APP follow-up, because this pass did not change shared public contracts those repos consume.
- Broad schema-doc refresh for unrelated tables already drifting in the connected database.
- Any scraper architecture rewrite beyond the backfill and orchestration fixes listed above.

## Locked Contracts
- `metadata_scraped_at` remains success-only for Instagram metadata enrichment.
- TikTok saves normalization and canonical URL resolution are shared helpers, not per-script bespoke logic.
- Shared account catalog resume must reuse stored frontier state instead of bootstrapping a new cursor.
- Inline execution must clean up ephemeral worker state in `finally`.

## Acceptance Criteria
1. Instagram and TikTok backfill fixes land in `TRR-Backend` with repository support changes and tests.
2. Targeted validation passes: remediation-file `ruff check`, remediation-file `ruff format --check`, and targeted pytest for the affected scripts/repository/router tests.
3. Cross-repo docs state clearly that no consumer follow-up was required for this pass.
4. TASK18 docs and generated handoff output are synchronized.
