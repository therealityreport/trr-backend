# Status — Task 4 (Supabase Schema Cleanup)

Repo: TRR-Backend
Last updated: February 9, 2026

## Phase Status

| Phase | Description | Status | Notes |
|-------|-------------|--------|-------|
| 6a | Drop `games.*` schema (migration 0106) | Implemented | `supabase/migrations/0106_drop_games_schema.sql` + seed cleanup |
| 6b | Data migration: cast tables -> credits model | Implemented | Backfill/verify scripts updated; run on staging before applying 0107 |
| 6c | Drop legacy cast tables (migration 0107) + replacement view | Implemented | `supabase/migrations/0107_drop_legacy_cast_tables.sql` provides compatibility views (incl `core.v_show_cast`) |
| 6d | Modify `core.shows` (migration 0108) | Implemented | `supabase/migrations/0108_modify_core_shows_consolidate_columns.sql` + app/pipeline updates |
| 6e | Enrich `core.people` (migration 0109) | Implemented | `supabase/migrations/0109_enrich_core_people_multisource.sql` + pipeline upserts + TRR-APP display |
| 6f | Enrich `core.credit_occurrences` (migration 0110) | Implemented | `supabase/migrations/0110_enrich_core_credit_occurrences.sql` |
| 6g | Social columns on dimension tables (migration 0111) | Implemented | `supabase/migrations/0111_add_social_columns_dimension_tables.sql` |
| 6h | Expand `people_overrides` handles (migration 0112) | Implemented | `supabase/migrations/0112_expand_people_overrides_handles.sql` |
| 6i | Extend `scrape_jobs` for Reddit (migration 0113) | Implemented | `supabase/migrations/0113_extend_social_scrape_jobs_platforms.sql` |
| 6j | Reddit scrape tables | Blocked | Reddit scraping not implemented (deferred) |
| 6k | Drop legacy image tables | Blocked | Image code migration incomplete (deferred) |
| 6l | Create `v_cast_summary` view | Implemented | `supabase/migrations/0114_create_core_v_cast_summary.sql` |

## Blockers

- 6j: Reddit scraping not implemented.
- 6k: Image subsystem not fully migrated to `media_assets`/`media_links`.

## Recent Activity

- February 8, 2026: Task folder created.
- February 9, 2026: Implemented migrations 0106-0114 (except deferred 6j/6k) and updated TRR-Backend code paths; `ruff` + `pytest` passing.
- February 9, 2026: `supabase db reset --yes` succeeded; migrations apply cleanly in local Supabase.
- February 9, 2026: PR #48 merged to `main`; staging Supabase up to migrations 0102-0115 (`supabase db push --linked` up to date).
