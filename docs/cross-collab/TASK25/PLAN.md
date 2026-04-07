# TRR-APP Supabase Simplification and Index Hardening — Task 25 Plan

Repo: TRR-Backend
Last updated: 2026-04-02

## Goal
Land the backend-first database work for TRR-APP Supabase simplification: add the missing performance-advisor indexes, fix the hot `core.season_images` query path, and remove the duplicate indexes that should not survive the cleanup.

## Status Snapshot
Implementation in progress. The live audit confirmed that the app-side raw Postgres lane is already correct and that the urgent backend work is the index batch: one hot-path composite partial index on `core.season_images`, one supporting `show_id` index, the currently flagged foreign-key indexes, and duplicate-index cleanup on `core.media_links` and `core.show_images`.

## Current Repo Truth

| Repo | Branch | HEAD | Worktree |
|---|---|---|---|
| TRR-Backend | `feat/supabase-unified-hardening` | `e0a74c9bca9f468f37c6ba9df576f63729a0f355` | Dirty in unrelated `api/routers/admin_show_links.py`; preserved |
| TRR-APP | `feat/supabase-unified-hardening` | `7632151729bdd447253020640d568cbafe468507` | Dirty in unrelated app/docs/test files; preserved |

## Scope

### Phase 1: Index hardening migration
Add one new Supabase migration that covers the approved performance-advisor cleanup and duplicate-index removals.

Files to change:
- `supabase/migrations/20260402213000_supabase_connection_index_hardening.sql`

Planned index additions:
- `core_season_images_show_id_idx` on `core.season_images(show_id)`
- `core_season_images_show_season_hosted_idx` on `core.season_images(show_id, season_number) WHERE hosted_url IS NOT NULL`
- `admin_brand_family_wikipedia_show_links_matched_show_id_idx` on `admin.brand_family_wikipedia_show_links(matched_show_id)`
- `core_show_cast_role_assignments_person_id_idx` on `core.show_cast_role_assignments(person_id)`
- `core_show_cast_role_assignments_season_id_idx` on `core.show_cast_role_assignments(season_id) WHERE season_id IS NOT NULL`
- `core_show_source_latest_source_id_idx` on `core.show_source_latest(source_id)`
- `core_show_source_history_source_id_idx` on `core.show_source_history(source_id)`
- `core_season_source_latest_source_id_idx` on `core.season_source_latest(source_id)`
- `core_season_source_history_source_id_idx` on `core.season_source_history(source_id)`
- `core_episode_source_latest_source_id_idx` on `core.episode_source_latest(source_id)`
- `core_episode_source_history_source_id_idx` on `core.episode_source_history(source_id)`
- `core_person_source_latest_source_id_idx` on `core.person_source_latest(source_id)`
- `core_person_source_history_source_id_idx` on `core.person_source_history(source_id)`
- `core_media_uploads_media_asset_id_idx` on `core.media_uploads(media_asset_id) WHERE media_asset_id IS NOT NULL`
- `core_media_uploads_media_link_id_idx` on `core.media_uploads(media_link_id) WHERE media_link_id IS NOT NULL`
- `public_surveys_current_episode_id_idx` on `public.surveys(current_episode_id) WHERE current_episode_id IS NOT NULL`
- `core_shows_primary_backdrop_image_id_idx` on `core.shows(primary_backdrop_image_id) WHERE primary_backdrop_image_id IS NOT NULL`
- `core_shows_primary_logo_image_id_idx` on `core.shows(primary_logo_image_id) WHERE primary_logo_image_id IS NOT NULL`
- `core_shows_primary_poster_image_id_idx` on `core.shows(primary_poster_image_id) WHERE primary_poster_image_id IS NOT NULL`
- `screenalytics_cast_screentime_reference_fingerprints_episode_id_idx`
- `screenalytics_cast_screentime_reference_fingerprints_run_id_idx`
- `screenalytics_cast_screentime_reference_fingerprints_season_id_idx`
- `screenalytics_cast_screentime_suggestion_decisions_episode_id_idx`
- `screenalytics_cast_screentime_suggestion_decisions_person_id_idx`
- `screenalytics_cast_screentime_suggestion_decisions_season_id_idx`
- `screenalytics_cast_screentime_suggestion_decisions_video_asset_id_idx`
- `screenalytics_cast_screentime_unknown_review_state_candidate_person_id_idx`
- `screenalytics_cast_screentime_unknown_review_state_episode_id_idx`
- `screenalytics_cast_screentime_unknown_review_state_season_id_idx`
- `screenalytics_cast_screentime_unknown_review_state_video_asset_id_idx`

Planned duplicate-index cleanup:
- Drop `core.media_links.media_links_one_primary_uq` and keep `core.media_links.media_links_one_primary_per_entity_kind`
- Drop `core.show_images.show_images_source_unique` and keep the constraint-backed `core.show_images.show_images_show_source_source_image_id_key`

### Phase 2: Validation and live advisor confirmation
Apply the migration through Supabase, rerun performance advisors, and verify the hot `core.season_images` query plan.

Files to change:
- `docs/cross-collab/TASK25/PLAN.md`
- `docs/cross-collab/TASK25/STATUS.md`
- `docs/cross-collab/TASK25/OTHER_PROJECTS.md`

## Out of Scope
- RLS or security-definer remediation unrelated to the approved performance batch
- New Flashback indexes
- Reworking app routes or browser Supabase usage in this repo

## Locked Contracts
- Raw Postgres runtime remains `TRR_DB_URL` then `TRR_DB_FALLBACK_URL`.
- TRR-APP stays on the existing session-mode Supavisor lane in `apps/web/src/lib/server/postgres.ts`.
- This task adds a new migration only; historical migrations stay untouched.
- The index cleanup is performance-advisor scoped, not a broader schema refactor.

## Acceptance Criteria
1. The new migration adds the approved season-image, foreign-key, and screenalytics indexes without editing existing migrations.
2. Duplicate-index cleanup preserves the intended surviving indexes and drops only the redundant partners.
3. Supabase performance advisors no longer report the targeted `unindexed_foreign_keys` and duplicate-index findings after the migration is applied.
4. `EXPLAIN` for the hot `core.season_images` query uses the new `(show_id, season_number) WHERE hosted_url IS NOT NULL` index.
5. `ruff check .`, `ruff format --check .`, `pytest -q`, and `make schema-docs-check` pass or any remaining failures are documented precisely.
6. Task docs remain synchronized with the actual implementation state.
