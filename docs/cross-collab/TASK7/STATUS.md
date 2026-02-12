# Status — Task 7 (Bravo Import + Cast Eligibility + Videos/News)

Repo: TRR-Backend
Last updated: February 11, 2026

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
