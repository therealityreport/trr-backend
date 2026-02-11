# Session Handoff (TRR-Backend)

Purpose: persistent state for multi-turn AI agent sessions in `TRR-Backend`. Update before ending a session or requesting handoff.

## Goal

- Implement Supabase unification and schema cleanup work for screenalytics + TRR (migrations + API/ingestion updates), coordinated across TRR-Backend → screenalytics → TRR-APP.

## Status

- Implemented TRR-Backend migrations `0102`-`0115` (screenalytics missing tables + schema cleanup, excluding deferred phases 6j/6k).
- Updated TRR-Backend ingestion/admin routes and pipeline scripts to align with credits model + enriched people fields + new cast views.
- Cross-collab task docs updated:
  - `docs/cross-collab/TASK5/*` (screenalytics data layer unification: backend-owned pieces)
  - `docs/cross-collab/TASK4/*` (schema cleanup: backend-owned pieces)
  - `docs/cross-collab/TASK1/*`, `docs/cross-collab/TASK2/*`, `docs/cross-collab/TASK3/*` (status corrections for Dev Dashboard task scanning; Task 2 marked complete via screenalytics implementation; Task 1 complete; Task 3 complete)
- Admin show sync/refresh endpoints shipped (Task 6):
  - `POST /api/v1/admin/shows/sync-from-lists` (`api/routers/admin_show_sync.py`) — import candidates from IMDb/TMDb lists + enrich metadata (no images).
  - `POST /api/v1/admin/shows/{show_id}/refresh` (`api/routers/admin_show_sync.py`) — synchronous refresh targets via existing sync scripts (details, seasons/episodes, photos, cast/credits).
  - `POST /api/v1/admin/shows/{show_id}/refresh-photos/stream` (`api/routers/admin_show_sync.py`) — high-fidelity show photo refresh with live SSE stage updates and accurate progress counts (IMDb/TMDb show + season/episode + cast photos, S3 mirroring, auto-count + word id).
  - Tests: `tests/api/routers/test_admin_show_sync.py`
  - Cross-collab docs: `docs/cross-collab/TASK6/*`
- Admin media workflow enhancements shipped (Task 1):
  - `POST /api/v1/admin/media-assets/{asset_id}/detect-text-overlay?force={bool}` (`api/routers/admin_media_assets.py`)
  - `POST /api/v1/admin/cast-photos/{photo_id}/detect-text-overlay?force={bool}` (`api/routers/admin_cast_photos.py`)
  - Detector implementation tracked in `trr_backend/vision/text_overlay.py` (Gemini; gated by env; returns `503` if not configured)
- Person gallery refresh improvements:
  - `POST /api/v1/admin/person/{person_id}/refresh-images` now also refreshes TMDb + Fandom profiles best-effort and performs word-id detection best-effort.
  - `POST /api/v1/admin/person/{person_id}/refresh-images/stream` now emits live stage messages (`sync_imdb`, `sync_tmdb`, `sync_fandom`, `sync_fandom_gallery`, `mirroring`, `auto_count`, `word_id`) with accurate `current/total` progress, and can tag all fetched photos with show context (`show_id`/`show_name`) so “This Show” filtering works across sources.
- Media asset mirroring now persists mirror timestamps in `core.media_assets.metadata`:
  - `metadata.mirrored_at` (UTC ISO timestamp)
  - `metadata.mirrored_from` (source_url)
  - Files:
    - `api/routers/admin_media_assets.py`
    - `trr_backend/repositories/media_assets.py`
- Multi-person dedup backend piece shipped (Task 3):
  - `trr_backend/media/s3_mirror.py#mirror_cast_photo_row` mirrors cast photos to shared content-addressed keys (`media/{sha256[:2]}/{sha256}{ext}`)
- Fast checks: `ruff check . && ruff format --check . && pytest -q` (421 passed, 18 skipped).

Pending / not executed:
- Supabase migrations were validated locally via `supabase db reset` and applied to staging Supabase via `supabase db push --linked` (linked project: `trr-core`) in this session; prod not updated.
- Credits backfill (Phase 6b) needs to be run/verified on staging before applying 0107 drop (Phase 6c) in any environment that still relies on legacy cast tables.

## Notes / Constraints

- Local dev API runs on `http://127.0.0.1:8000` by default (`TRR_BACKEND_PORT` override supported).
- In the multi-repo workspace, `make dev/stop/logs` from this repo delegates to the workspace root (`../Makefile`).
- `start-api.sh` now fails fast if `.venv/` is missing and uses `exec uvicorn ...` so stop scripts can reliably terminate the server process.
- Workspace runner (`/Users/thomashulihan/Projects/TRR/make dev`) wires:
  - `SCREENALYTICS_API_URL` (default `http://127.0.0.1:8001`)
  - `CORS_ALLOW_ORIGINS` for TRR-APP (`:3000`)
- Shared DB env var contract: `TRR_DB_URL` is canonical; `SUPABASE_DB_URL` is a deprecated alias during transition.

## Next Steps

1. (Prod) Apply migrations in order; reload PostgREST schema cache if needed after function/view changes.
2. Run/verify the credits backfill (Phase 6b) and parity checks before applying 0107 in any environment with existing data.
3. Ensure TRR-APP + screenalytics deploy alongside the backend migrations (consumers now rely on `core.v_show_cast` / `core.v_episode_cast` and people multi-source fields).

## Verification Commands

```bash
source .venv/bin/activate
ruff check . && ruff format --check . && pytest
```

If schema/migrations changed:
```bash
supabase db reset --yes
make schema-docs-check
```

---

Last updated: 2026-02-10
Updated by: Claude Opus 4.6

Workspace integration (this session):
- Verified `codex/admin-show-sync-refresh` branch was already squash-merged to origin/main; deleted local branch
- Dropped 2 local stashes (both superseded by merged branch work)
- Repo is clean on main, up to date with origin

## Admin Gallery Actions (This Session)

- Added unified admin endpoints to archive/star assets across tables:
  - `POST /api/v1/admin/assets/archive` and `POST /api/v1/admin/assets/star`
  - Implementation: `api/routers/admin_asset_flags.py`
- Added Supabase migration to support archiving unified/show assets:
  - `supabase/migrations/0116_archive_media_assets_and_show_images.sql`
  - Adds `archived_at`, `archived_by_firebase_uid`, `archived_reason` to `core.media_assets` and `core.show_images`
- Updated S3 mirror candidate queries to skip archived rows:
  - `trr_backend/repositories/show_images.py`
  - `trr_backend/repositories/season_images.py`
  - `trr_backend/repositories/episode_images.py`
  - `trr_backend/repositories/cast_photos.py`
- Extended web scrape import contracts to persist richer per-image metadata:
  - `context_section`, `context_type`, `source_logo`, `asset_name`
  - Implementation: `api/routers/admin_scrape.py`

Face-count and text-overlay URL fallback hardening (this session):
- Normalized stale Fandom static URLs (strips `/revision/latest...`) in `trr_backend/media/s3_mirror.py`.
- Cast photo mirroring now tries multiple candidate URLs (normalized Fandom + raw + thumb) before failing.
- Person-image auto-count now builds URL candidates (hosted + normalized/raw source URLs) and retries per image instead of failing on the first bad URL.
- `/admin/cast-photos/{id}/auto-count` and `/admin/media-assets/{id}/auto-count` now retry candidate URLs, including Fandom static URL normalization.
- Text-overlay detection for cast photos and media assets now retries candidate URLs and supports normalized Fandom static URLs.
- Added/updated regression tests:
  - `tests/api/routers/test_admin_person_images.py`
  - `tests/api/routers/test_admin_image_counts_fallback.py`
  - `tests/media/test_s3_mirror.py`
  - `tests/vision/test_text_overlay_fallback.py`
- Verification:
  - `ruff check ...` (touched backend files) passed
  - `pytest -q tests/api/routers/test_admin_person_images.py tests/api/routers/test_admin_image_counts_fallback.py tests/media/test_s3_mirror.py tests/vision/test_text_overlay_fallback.py` passed (`49 passed`)

Gemini env-var compatibility update (this session):
- Text-overlay model lookup now accepts `GEMINI-MODEL` in addition to `GEMINI_MODEL` and `GOOGLE_GEMINI_MODEL`.
- File: `trr_backend/vision/text_overlay.py`
- Docs/examples updated to list model aliases:
  - `.env.example`
  - `README.md`
- Verification:
  - `ruff check trr_backend/vision/text_overlay.py` passed
  - `pytest -q tests/vision/test_text_overlay_fallback.py` passed (`2 passed`)

Person image refresh backfill + centroid persistence fixes (this session):
- Updated person refresh/reprocess auto-count behavior in `api/routers/admin_person_images.py`:
  - Removed skip condition that excluded rows when `cast_photos.people_names` was populated.
  - Refresh now backfills count/text jobs across all missing eligible photos, not only newly-upserted IDs.
- Updated per-photo auto-count endpoints in `api/routers/admin_image_counts.py`:
  - After successful auto-count, now also writes auto `thumbnail_crop` face centroid for cast photos (`metadata.thumbnail_crop`) unless existing crop mode is manual.
  - For media assets, applies centroid to linked person `media_links.context.thumbnail_crop` unless manual.
- Hardened centroid helper in `trr_backend/clients/screenalytics.py`:
  - `face_centroid()` now handles result objects without `detections` attribute safely.

Validation run:
- `ruff check trr_backend/clients/screenalytics.py api/routers/admin_image_counts.py api/routers/admin_person_images.py` (pass)
- `pytest -q tests/api/routers/test_admin_person_images.py tests/api/routers/test_admin_image_counts_fallback.py` (pass; 20 passed)

Gemini text-overlay empty-candidate handling fix (this session):
- Fixed `trr_backend/vision/text_overlay.py` to avoid relying solely on `response.text` quick accessor.
- Added safe response extraction helpers:
  - `_extract_gemini_response_text()` parses candidate `content.parts[].text` when quick accessor is unavailable.
  - Includes finish reason extraction for clearer diagnostics.
- Added one retry in strict JSON response mode (`response_mime_type='application/json'`, larger token cap) when first Gemini response has no text parts.
- New explicit error when still empty: `Gemini returned no candidate text content (finish_reason=...)`.
- Added unit tests covering empty/quick-accessor-failure behavior in `tests/vision/test_text_overlay_fallback.py`.

Validation run:
- `ruff check trr_backend/vision/text_overlay.py tests/vision/test_text_overlay_fallback.py` (pass)
- `pytest -q tests/vision/test_text_overlay_fallback.py` (pass; 4 passed)

Show-level scrape import + metadata controls (this session):
- Extended scrape import entity support from `season|person` to `season|show|person`.
  - `show` imports now require `show_id` and accept optional season metadata (`season_number`/`season_id`) in link context.
  - File: `api/routers/admin_scrape.py`
- Added show-identifier resolution helper (`IMDb ID` preferred, show UUID fallback) for show-image S3 key paths.
  - File: `api/routers/admin_scrape.py`
- Non-stream and stream import handlers now build show image S3 keys via `build_show_image_s3_key`.
  - File: `api/routers/admin_scrape.py`
- Stream importer link creation now mirrors non-stream behavior:
  - always creates entity links (not only when cast fuzzy-match succeeds),
  - persists `context_section`, `context_type`, `source_logo`, `asset_name`,
  - applies optional source-page timestamp metadata consistently.
  - File: `api/routers/admin_scrape.py`

Verification (this session):
- `ruff check api/routers/admin_scrape.py` (pass)
- `python -m compileall api/routers/admin_scrape.py` (pass)
- `pytest -q tests/test_api_smoke.py` (pass; 12 passed)

Show scrape import kind expansion (this session):
- Added `logo` to scrape-import `ImageKind` validation so show-level import drawer can ingest logos.
  - File: `api/routers/admin_scrape.py`

Verification (this session):
- `ruff check api/routers/admin_scrape.py` (pass)
- `python -m compileall api/routers/admin_scrape.py` (pass)

Auto-crop/centering end-to-end hardening (this session):
- Extended Screenalytics client parsing to support mixed detection kinds (`face` + `person`) and introduced deterministic face+torso crop synthesis:
  - `trr_backend/clients/screenalytics.py`
  - Added `auto_thumbnail_crop()` strategy (`face_torso_v2`) with clamped `x/y/zoom` and `kind`-aware fallback behavior.
- Updated admin auto-count endpoints to persist richer auto crop payloads (`strategy`, `generated_at`) and respect manual crops:
  - `api/routers/admin_image_counts.py`
- Expanded person refresh/reprocess backend jobs to include media-link gallery rows (not only cast_photos) for count/text/crop updates:
  - `api/routers/admin_person_images.py`
  - Added media-links auto-count and text-overlay passes.
  - Added/propagated real `centering_cropping` stage payloads with numeric counts in stream responses.
  - Added refresh/reprocess summaries for centering counters (`centering_attempted/succeeded/failed/skipped_manual`).
- Added framing unit tests:
  - `tests/vision/test_people_count_auto_crop.py`

Verification (this session):
- `ruff check trr_backend/clients/screenalytics.py api/routers/admin_image_counts.py api/routers/admin_person_images.py tests/vision/test_people_count_auto_crop.py tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_person_images.py` (pass)
- `pytest -q tests/vision/test_people_count_auto_crop.py tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_person_images.py` (pass; 24 passed)
- `python -m py_compile api/routers/admin_person_images.py trr_backend/clients/screenalytics.py api/routers/admin_image_counts.py` (pass)

MSN + Bravo official-season-announcement scrape support (this session):
- Extended URL image scraper with MSN detail API fallback for client-rendered MSN article pages:
  - Detects MSN article IDs from URLs (e.g. `/ar-AA1TfjXH`)
  - Fetches `https://assets.msn.com/content/view/v2/Detail/{locale}/{articleId}`
  - Builds candidates from `imageResources` with `cmsId` body-order matching
  - Extracts cast-friendly context as `Name\nBio` from MSN body HTML (heading + following paragraphs)
  - Propagates MSN `publishedDateTime` and title into scrape result metadata
  - File: `trr_backend/scraping/url_image_scraper.py`
- Improved generic nearby-text extraction for article layouts (including Bravo cast bio pages):
  - Added sibling-context extraction (nearest heading + bio paragraphs)
  - Trims boilerplate heading suffixes like `"'s official bio for ..."`
  - Prefers richer heading+bio context over short inline captions/figcaptions
  - File: `trr_backend/scraping/url_image_scraper.py`
- Added/updated scraper tests:
  - heading+bio extraction for article-style image blocks
  - richer context preference over figcaption/inline caption
  - MSN detail API fallback extraction + metadata fields
  - File: `tests/scraping/test_url_image_scraper.py`

Verification (this session):
- `ruff check trr_backend/scraping/url_image_scraper.py tests/scraping/test_url_image_scraper.py` (pass)
- `pytest -q tests/scraping/test_url_image_scraper.py` (pass; 7 passed)
- Live smoke checks against:
  - `https://www.msn.com/en-us/entertainment/news/the-valley-persian-style-cast-previews-age-appropriate-drama/ar-AA1TfjXH`
  - `https://www.bravotv.com/the-daily-dish/the-valley-persian-style-season-1-cast-official-photos-bios-details`
  confirmed cast-photo candidates now include populated name/bio context.

Show refresh + scrape-source normalization fixes (this session):
- Fixed cast/credits refresh delete filter bug in sync scripts by replacing unsupported `.neq(...)` calls with DbSession-compatible `.not_.eq(...)`:
  - `scripts/sync/sync_show_cast.py`
  - `scripts/sync/sync_episode_appearances.py`
  - `scripts/sync/sync_people.py`
  - `trr_backend/ingestion/show_importer.py`
- Added `external_ids` fallback support for show ID extraction used by sync scripts:
  - `scripts/_sync_common.py`
  - `SHOW_SELECT_FIELDS` now includes `external_ids`
  - `extract_imdb_series_id()` and `extract_tmdb_series_id()` now fallback to `external_ids.{imdb|imdb_id|tmdb|tmdb_id}`.
- Improved show photo refresh robustness for schemas missing `archived_at`:
  - Added fallback query path (without archived filter) when `archived_at` column is absent in:
    - `trr_backend/repositories/show_images.py`
    - `trr_backend/repositories/season_images.py`
    - `trr_backend/repositories/episode_images.py`
    - `trr_backend/repositories/cast_photos.py`
- Added IMDb ID fallback from `external_ids` in show photo stream refresh bootstrap:
  - `api/routers/admin_show_sync.py`

Validation (this session):
- `ruff check scripts/_sync_common.py scripts/sync/sync_show_cast.py scripts/sync/sync_episode_appearances.py scripts/sync/sync_people.py trr_backend/ingestion/show_importer.py api/routers/admin_show_sync.py trr_backend/repositories/show_images.py trr_backend/repositories/season_images.py trr_backend/repositories/episode_images.py trr_backend/repositories/cast_photos.py` (pass)
- Reproduced and verified fixes on show `940ca82c-e4a9-45b4-9d85-946a654925ce`:
  - `PYTHONPATH=. python scripts/sync/sync_show_cast.py --show-id ... --force --verbose` (pass; `credits_inserted=19`, `failures=0`)
  - `PYTHONPATH=. python scripts/sync/sync_show_images.py --show-id ... --force --verbose` (pass; IMDb images mirrored to S3)
  - DB spot checks:
    - `core.credits` Self count for show restored to `19`
    - `core.show_images` has IMDb hosted rows (`imdb|2|2`)

Gemini text-overlay malformed JSON fallback (this session):
- Hardened JSON extraction in `trr_backend/vision/text_overlay.py` so truncated Gemini payloads (e.g. `{"`) no longer always hard-fail:
  - tries robust `JSONDecoder.raw_decode` scanning first,
  - falls back to extracting `has_text_overlay`/`confidence` from partial key-value text when possible,
  - adds a structured-JSON retry path when initial parse fails (not only when text is empty).
- Added regression tests in `tests/vision/test_text_overlay_fallback.py`:
  - incomplete JSON object recovery,
  - truncated confidence recovery,
  - explicit error for non-parseable junk input.

Verification (this session):
- `ruff check trr_backend/vision/text_overlay.py tests/vision/test_text_overlay_fallback.py` (pass)
- `pytest -q tests/vision/test_text_overlay_fallback.py` (pass; 7 passed)
- `python -m py_compile trr_backend/vision/text_overlay.py` (pass)

Text-overlay unknown-state fallback for empty Gemini candidates (this session):
- Updated `trr_backend/vision/text_overlay.py` so text-overlay detection no longer hard-fails when Gemini returns no candidate text (or unparseable payload after retries).
- `TextOverlayResult` now supports unknown outcomes:
  - `has_text_overlay: None`
  - `status: "unknown"`
  - persisted diagnostics: `text_overlay_error`, `text_overlay_finish_reason`
- Existing-metadata reader now recognizes persisted unknown status without forcing re-run.
- Admin detect endpoints now return `status="unknown"` when detection result is unknown:
  - `api/routers/admin_cast_photos.py`
  - `api/routers/admin_media_assets.py`
- Added tests in `tests/vision/test_text_overlay_fallback.py` for unknown-result creation and persisted unknown extraction.

Verification (this session):
- `ruff check trr_backend/vision/text_overlay.py api/routers/admin_cast_photos.py api/routers/admin_media_assets.py tests/vision/test_text_overlay_fallback.py` (pass)
- `pytest -q tests/vision/test_text_overlay_fallback.py tests/api/routers/test_admin_image_counts_fallback.py` (pass; 13 passed)
- `python -m py_compile trr_backend/vision/text_overlay.py api/routers/admin_cast_photos.py api/routers/admin_media_assets.py` (pass)
