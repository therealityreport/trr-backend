# Status — Task 7 (Bravo Import + Cast Eligibility + Videos/News)

Repo: TRR-Backend
Last updated: March 16, 2026

## Phase Status

| Phase | Description | Status | Notes |
|------:|-------------|--------|-------|
| 1 | Bravo source migration | Implemented | `supabase/migrations/0117_add_bravo_source.sql` |
| 2 | Bravo parser service | Implemented | `trr_backend/scraping/bravo_parser.py` |
| 3 | Admin Bravo endpoints | Implemented | `api/routers/admin_show_bravo.py`, wired in `api/main.py` |
| 4 | Snapshot persistence + commit behavior | Implemented | show/person latest+history + fill-missing social merge |
| 5 | Targeted tests | Implemented | parser + router tests added and passing |

## Blockers

None.

## Recent Activity

- March 16, 2026: Implemented unified show refresh orchestration stages and explicit rerun semantics for the admin show page.
  - Expanded additive refresh targets in `api/routers/admin_show_sync.py`:
    - `show_core`
    - `links`
    - `bravo`
    - `cast_profiles`
    - `cast_media`
  - The default full show refresh stream now reports explicit stage metadata (`pipeline_stage`, `pipeline_status`, `skip_reason`) instead of collapsing work into ambiguous `Cast`/`People` buckets.
  - Folded legacy standalone workflows under the unified pipeline:
    - show/season/person link discovery now runs under `links`
    - Bravo sync readiness/eligibility checks now run under `bravo`
    - cast profile enrichment and cast media ingest now run as separate downstream stages
  - Added `force_new_operation` request handling so explicit `Run`/`Rerun` actions start a fresh operation instead of attaching to an existing one.
  - Hardened operation stream replay in `trr_backend/pipeline/admin_operations.py` so terminal-stage events are not dropped on fast operations.
  - Tests:
    - `tests/api/routers/test_admin_show_sync.py`
  - Validation:
    - `ruff check api/routers/admin_show_sync.py trr_backend/pipeline/admin_operations.py tests/api/routers/test_admin_show_sync.py` (pass)
    - `pytest -q tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_show_bravo.py` (pass, `66 passed`)
    - `python -m py_compile api/routers/admin_show_sync.py trr_backend/pipeline/admin_operations.py tests/api/routers/test_admin_show_sync.py` (pass)

- February 17, 2026: Implemented global cast-matrix sync and Bravo auto-trigger integration.
  - Added endpoint:
    - `POST /api/v1/admin/shows/{show_id}/cast-matrix/sync`
  - Added Wiki/Fandom cast table parser + merge:
    - `trr_backend/ingestion/show_cast_matrix_scraper.py`
  - Extended Fandom person parser for relationship/family variants:
    - `trr_backend/ingestion/fandom_person_scraper.py`
  - Added role sync service with auto-source replacement preserving manual rows:
    - `api/routers/admin_show_roles.py`
  - Added Bravo commit auto-sync flag (`sync_cast_matrix=true` default):
    - `api/routers/admin_show_bravo.py`
  - Fixed Bravo snapshot variant mismatch in link discovery read path:
    - `api/routers/admin_show_links.py` now uses variant `default`.
  - Validation:
    - `pytest -q tests/ingestion/test_show_cast_matrix_scraper.py tests/ingestion/test_fandom_person_scraper.py tests/api/routers/test_admin_show_roles.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py` (`21 passed`)

- February 13, 2026: Implemented Bravo profile-picture content typing + thumbnail metadata persistence for gallery performance.
  - `api/routers/admin_show_bravo.py`
    - Bravo person image import now writes:
      - `context_section: "bravo_profile"`
      - `context_type: "profile_picture"` (legacy `profile` replaced)
      - `asset_name/caption`: "Bravo profile picture"
  - `trr_backend/media/image_variants.py`
    - Base variant metadata now persists `thumb_url` in addition to `display_url`/`detail_url`.
  - Validation:
    - `ruff check api/routers/admin_show_bravo.py trr_backend/media/image_variants.py` (pass)
    - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)

- February 12, 2026: Tightened people link discovery to require fandom profile presence for person Wikipedia/Fandom links.
  - Updated `api/routers/admin_show_links.py`:
    - person `wikipedia` and `fandom` links are now emitted only when `core.cast_fandom` has a fandom URL for that person.
  - RHOSLC immediate data cleanup + rediscovery run:
    - deleted stale RHOSLC person `wikipedia`/`fandom` link rows
    - reran `POST /api/v1/admin/shows/{show_id}/links/discover`
    - resulting RHOSLC person link kinds: `fandom:66`, `wikipedia:66` (paired)
  - Validation:
    - `ruff check api/routers/admin_show_links.py` (pass)
    - `python -m py_compile api/routers/admin_show_links.py` (pass)

- February 12, 2026: Added RHOSLC backfill runbook for discovered links + cast-role suggestions.
  - Runbook:
    - `docs/runbooks/rhoslc-show-admin-backfill.md`
  - Includes executable `curl` + `psql` commands for:
    - season-scoped Bravo commit backfill
    - explicit links discovery
    - pending link review/approval
    - role suggestion persistence verification

- February 12, 2026: Optimized persisted Bravo read endpoints to reduce slow show-page loads.
  - `GET /api/v1/admin/shows/{show_id}/bravo/videos` and `GET /api/v1/admin/shows/{show_id}/bravo/news` now prefer embedded normalized person items already stored in the show snapshot.
  - Person snapshot fallback queries now run only for older snapshots missing embedded `videos_person` / `news_person`, avoiding redundant large snapshot joins on every read.
  - File:
    - `api/routers/admin_show_bravo.py`
  - Validation:
    - `ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
    - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)

- February 12, 2026: Fixed season-scoped Bravo description persistence behavior.
  - `POST /api/v1/admin/shows/{show_id}/import-bravo/commit` now persists Bravo description to `core.seasons.overview` when `season_number` is provided (resolved to a real season).
  - Season-scoped sync no longer overwrites `core.shows.description` with current-season marketing copy.
  - Show-level sync behavior remains unchanged: when no `season_number` is provided, `core.shows.description` is updated as before.
  - Files:
    - `api/routers/admin_show_bravo.py`
    - `tests/api/routers/test_admin_show_bravo.py`
  - Validation:
    - `ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
    - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)

- February 12, 2026: Implemented image-variant persistence foundation for gallery performance and crop durability.
  - Added migration `supabase/migrations/0119_create_media_asset_variants.sql` for `core.media_asset_variants`.
  - Added generator `trr_backend/media/image_variants.py` for base (`thumb/card/detail`) and crop (`crop_card/crop_detail`) variants with S3 upload + metadata URL fields (`display_url`, `detail_url`, `crop_display_url`, `crop_detail_url`).
  - Added admin endpoint `POST /api/v1/admin/media-assets/{asset_id}/variants` in `api/routers/admin_media_assets.py`.
  - Hooked variant generation into scrape import and auto-count crop flows (`api/routers/admin_scrape.py`, `api/routers/admin_image_counts.py`).
  - Added backfill tooling:
    - `scripts/media/backfill_media_asset_variants.py`
    - `scripts/backfill_media_asset_variants.py`

- February 11, 2026: Fixed Bravo snapshot persistence failure in environments missing `core.sources.id='bravo'`.
  - Applied migration `supabase/migrations/0117_add_bravo_source.sql` to active DB.
  - Added backend guard in `api/routers/admin_show_bravo.py` to verify/create Bravo source row before show/person snapshot writes.
  - Improved error detail logging/HTTP messages for show/person snapshot persistence failures.
  - Validation: `pytest -q tests/api/routers/test_admin_show_bravo.py` and `ruff check api/routers/admin_show_bravo.py`.
- February 11, 2026: Added fast-mode controls for show photo refresh pipeline to reduce long-running refreshes.
  - New `POST /admin/shows/{show_id}/refresh-photos/stream` request fields:
    - `skip_auto_count`
    - `skip_word_detection`
    - `imdb_mediaindex_max_pages`
    - `imdb_mediaindex_max_images`
  - Fast mode can now skip expensive AI post-processing and reduce IMDb mediaindex crawl depth.
  - File: `api/routers/admin_show_sync.py`
- February 11, 2026: Added Bravo sync readiness guard in admin Bravo endpoints.
  - `POST /import-bravo/preview` and `POST /import-bravo/commit` now enforce preconditions: synced seasons + episodes + cast must exist, otherwise return `409` with explicit missing sections.
  - File: `api/routers/admin_show_bravo.py`
- February 11, 2026: Applied Fandom source policy for cast/person refresh.
  - Show photos refresh now skips Fandom/Fandom gallery stages for non-`Real Housewives` shows.
  - Person refresh (sync + stream) now removes Fandom sources and skips Fandom profile sync when show context is non-`Real Housewives`.
  - Files: `api/routers/admin_show_sync.py`, `api/routers/admin_person_images.py`
- February 11, 2026: Verification for this update:
  - `ruff check api/routers/admin_show_sync.py api/routers/admin_person_images.py api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `pytest -q tests/api/routers/test_admin_show_bravo.py tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_person_images.py` (`31 passed`)
- February 11, 2026: Implemented Bravo parser, admin endpoints, migration, and tests.
- February 11, 2026: Finalized default `merge_person_sources=true` for `GET /api/v1/admin/shows/{show_id}/bravo/videos` to match task defaults.
- February 11, 2026: Validation complete:
  - `ruff check api/routers/admin_show_bravo.py trr_backend/scraping/bravo_parser.py tests/api/routers/test_admin_show_bravo.py tests/scraping/test_bravo_parser.py`
  - `pytest -q tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (6 passed)
- February 11, 2026: Refined Bravo show parser heuristics to capture day/time airs strings and suppress video/news thumbnail noise in show image candidates.
- February 11, 2026: Added `published_at` extraction for Bravo videos/news and switched show-video scraping to prefer `/watch/videos` (season-filtered feed).
- February 11, 2026: Added per-image Bravo show import type support for commit payloads.
  - `POST /api/v1/admin/shows/{show_id}/import-bravo/commit` now accepts optional `selected_show_images: [{url, kind}]` where `kind` can be `poster|backdrop|logo|episode_still|cast|promo|intro|reunion|other`.
  - Backward compatibility retained for existing `selected_show_image_urls[]` callers (defaults to `promo`).
  - Commit ingest now forwards selected `kind` values to `admin_scrape.import_images` instead of hardcoded `promo`.
  - Added router test coverage asserting selected kinds are passed through.
- February 11, 2026: Bravo commit now ingests each resolved cast member hero image through the person media import pipeline (mirrored to S3 via existing `admin_scrape.import_images` path).
  - Added commit counts: `imported_person_images`, `skipped_person_images`, and `person_image_import_errors`.
  - Person profile updates remain: biography/homepage/profile image source fields + social handles fill-missing-only.
- February 11, 2026: Improved person tagging durability for videos/news.
  - Deduping now merges `person_tags` for duplicate `clip_url`/`article_url` items instead of dropping later tags.
- February 11, 2026: Added optional season-scoped Bravo import filtering for videos.
  - `POST /import-bravo/preview` and `POST /import-bravo/commit` now accept `season_number` and filter show/person videos to that season before preview/persist.
- February 11, 2026: Tightened Bravo show-news relevance filtering to exclude unrelated sidebar/global items (e.g., "Latest Bravo News" cards not tied to the current show/cast).
  - `parse_show_news(...)` now filters by show slug/title and cast person slug relevance before returning persisted news.
  - Added parser test that keeps Summer House-related card and drops unrelated Rachel Zoe item from the same page payload.
- February 12, 2026: Implemented show-admin backend foundations for links + roles + ingest enrichment.
  - Added migration `0120_show_admin_links_and_roles.sql` with `core.entity_links`, `core.show_role_catalog`, `core.show_cast_role_assignments`, and `core.v_show_cast_roles_enriched`.
  - Added admin routers for links discovery/review and show-scoped role assignment/cast-role member listing.
  - Bravo commit now persists pending link discovery and cast-role suggestions from cast-announcement content.
  - Season image import path now links imported media to both season and show entities.
  - Validation:
    - `ruff check api/routers/admin_show_bravo.py api/routers/admin_show_roles.py api/main.py api/routers/admin_show_links.py api/routers/admin_scrape.py` (pass)
    - `python -m py_compile api/routers/admin_show_bravo.py api/routers/admin_show_roles.py api/main.py api/routers/admin_show_links.py api/routers/admin_scrape.py` (pass)
    - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)
- March 16, 2026: Completed unified show refresh backend validation for the app-side Health Center rollout.
  - Verified additive stage model (`show_core`, `links`, `bravo`, `cast_profiles`, `cast_media`) via targeted router/orchestration tests.
  - Verified explicit reruns force a fresh operation instead of attaching to an in-flight run.
  - Verified Bravo skip semantics and additive stream payload fields align with the updated TRR-APP health center pipeline.

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: archived
  last_updated: 2026-03-24
  current_phase: "archived continuity note"
  next_action: "See newer task status notes if follow-up is needed"
  detail: self
```
