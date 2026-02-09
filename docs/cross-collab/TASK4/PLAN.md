# Supabase Schema Cleanup — Task 4 Plan

Repo: TRR-Backend
Last updated: February 8, 2026

## Goal

Execute owner-approved schema cleanup: drop unused tables, consolidate redundant columns, enrich entity tables, and unify the credits model. All changes are documented in `docs/database-schema.md` (v2).

## Status Snapshot

Not yet started. Most sub-phases are independent and can run in parallel, except the cast model migration (6b -> 6c -> 6f/6l) which is sequential.

## Scope

### Phase 6a: Drop `games.*` Schema (Migration 0106)

Drop all 7 unused tables. No runtime app code depends on `games.*` today, but seeds/docs may reference it. Update/remove `supabase/seed.sql` fixtures and any docs that mention `games.*`.

```sql
DROP SCHEMA games CASCADE;
```

### Phase 6b: Data Migration — Cast Tables to Credits Model

Write a data migration script to move data from `cast_memberships`, `episode_cast`, `show_cast`, `episode_appearances` into `credits` + `credit_occurrences`. Run on staging first, verify row counts match.

### Phase 6c: Drop Legacy Cast Tables (Migration 0107)

**Prerequisite**: Phase 6b data migration verified. All consumers must switch before running the DROP.

**Critical**: Create replacement view `core.v_show_cast` with equivalent consumer contract before dropping `core.show_cast`. Both TRR-APP and screenalytics read from these tables.

```sql
DROP TABLE core.episode_cast;
DROP TABLE core.cast_memberships;
DROP TABLE core.episode_appearances;
DROP TABLE core.show_cast;
```

Code changes (TRR-Backend):
- Switch all routers/services from `show_cast`/`episode_appearances` to `credits` + `credit_occurrences`
- Remove `show_cast`/`episode_appearances` triggers
- Update API response shapes

### Phase 6d: Modify `core.shows` (Migration 0108)

Consolidate `most_recent_episode_*` columns into single `most_recent_episode` jsonb. Drop `network`, `streaming` (superseded by arrays), social IDs (moved to networks/providers), and resolution flags (pipeline concern).

Code changes (TRR-Backend):
- Pipeline code writing `most_recent_episode_*` -> write single jsonb
- Code reading `network`/`streaming` -> read `networks[]`/`streaming_providers[]`
- API serializers -> update response shapes
- Pipeline code using `needs_tmdb_resolution`/`needs_imdb_resolution` -> move to pipeline metadata

### Phase 6e: Enrich `core.people` (Migration 0109)

Add multi-source canonical fields (`birthday`, `gender`, `biography`, `place_of_birth`, `homepage`, `profile_image_url`) as jsonb keyed by source.

Code changes (TRR-Backend):
- Pipeline stages writing to `cast_tmdb`/`cast_fandom` -> also upsert canonical fields
- API endpoints -> include canonical fields with source-resolution logic

### Phase 6f: Enrich `core.credit_occurrences` (Migration 0110)

Add `air_year`, `credit_text`, `attributes` jsonb, `is_archive_footage` from dropped `episode_appearances`.

Depends on Phase 6c (cast table drops).

### Phase 6g: Social Columns on Dimension Tables (Migration 0111)

Add `facebook_id`, `instagram_id`, `twitter_id`, `tiktok_id` to `core.networks` and `core.watch_providers`.

### Phase 6h: Expand `people_overrides` Handles (Migration 0112)

Add `tiktok_handle`, `twitter_handle`, `youtube_handle` to `core.people_overrides`.

### Phase 6i: Extend `scrape_jobs` for Reddit (Migration 0113)

Update CHECK constraint on `social.scrape_jobs.platform` to include `'reddit'`.

### Phase 6j: Reddit Scrape Tables (Future)

Add `social.reddit_posts` and `social.reddit_comments` tables. Blocked until Reddit scraping is implemented.

### Phase 6k: Drop Legacy Image Tables (Future)

Drop `cast_photos`, `person_images`, `episode_images`, `season_images`, `show_images`. Blocked until all image code uses `media_assets` + `media_links`.

### Phase 6l: Create `v_cast_summary` View

Aggregation view over `credits` + `credit_occurrences` for cast summary data. Depends on Phase 6c.

## Out of Scope

- TRR-APP code changes for cast query updates (owned by TRR-APP TASK3)
- screenalytics `trr_metadata_db.py` updates (owned by screenalytics TASK5)
- Screenalytics data layer unification (separate task: TRR-Backend TASK3)

## Locked Contracts

### Credits Model Shape
```
credits: {person_id, show_id, credit_category, billing_order, role, ...}
credit_occurrences: {credit_id, episode_id, air_year, credit_text, attributes, is_archive_footage, ...}
```

### `most_recent_episode` JSONB Schema
```json
{
  "imdb": {"season": 3, "episode": 12, "title": "...", "air_date": "2025-03-15", "imdb_id": "tt1234567"},
  "tmdb": {"season": 3, "episode": 12, "title": "...", "air_date": "..."}
}
```

### `core.people` Multi-Source Field Convention
```json
{"tmdb": "1990-05-15", "fandom": "May 15, 1990"}
```
Each field stores values keyed by source name. Resolution order: tmdb > fandom > manual.

### Replacement View `core.v_show_cast`
Must maintain equivalent consumer contract for screenalytics:
- `person_id`, `show_id`, `credit_category`, `billing_order`
- Built on top of `credits` + `credit_occurrences`

## Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Cast data migration (6b) incomplete/mismatched | High | Write migration with row count verification; run on staging first |
| Shows column drops (6d) break API consumers | High | Deploy code changes first, then run migration |
| `most_recent_episode` jsonb shape disagreements | Medium | Define canonical schema in docs; validate in pipeline code |
| Legacy image table drops (6k) — bridge trigger removal | High | Verify ALL image code uses media_assets first; this is the last phase |

## Acceptance Criteria

1. All migrations 0106-0113 apply cleanly on staging.
2. Cast data migration (6b) verified: row counts match source tables.
3. Replacement view `core.v_show_cast` returns equivalent data.
4. All TRR-Backend tests pass after code changes.
5. API response shapes updated for dropped/modified columns.
6. No references to dropped tables remain in TRR-Backend code.
7. Task 4 docs are synchronized across repos.
