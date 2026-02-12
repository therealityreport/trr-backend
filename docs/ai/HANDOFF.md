# Session Handoff (TRR-Backend)

Purpose: persistent state for multi-turn AI agent sessions in `TRR-Backend`. Update before ending a session or requesting handoff.

## Latest Update (2026-02-11)

- Fixed Bravo snapshot persistence failure (`Failed to persist show bravo snapshot`) for environments missing `core.sources.id='bravo'`:
  - Applied migration `supabase/migrations/0117_add_bravo_source.sql` to active DB.
  - Added `_ensure_bravo_source(...)` guard in `api/routers/admin_show_bravo.py` so commit paths verify/create the Bravo source row before writing show/person snapshots.
  - Improved error detail in snapshot persistence failures to surface underlying DB messages in logs/API detail.
  - Validation: `pytest -q tests/api/routers/test_admin_show_bravo.py` and `ruff check api/routers/admin_show_bravo.py`.
- Fixed silent-failure behavior in episodic cast sync (`scripts/sync/sync_episode_appearances.py`):
  - Root issue observed: IMDb episodic GraphQL call now returns `PersistedQueryNotFound` for operation `TitleEpisodeBottomSheetCredits`, causing zero `core.credit_occurrences` inserts.
  - Script now treats "all cast episodic fetches failed" as a fatal show failure (`exit code 1`) so admin refresh surfaces a failed step instead of a false success.
  - Script no longer deletes `core.credit_occurrences` for credits that did not refresh successfully, preventing accidental wipeout of prior episode evidence during IMDb upstream failures.
  - Added summary metric: `fatal_show_failures`.
  - Validation: `ruff check scripts/sync/sync_episode_appearances.py` and `python -m py_compile scripts/sync/sync_episode_appearances.py`.
- Added resilient episodic GraphQL fallback in `trr_backend/integrations/imdb/episodic_client.py`:
  - When persisted query hashes rotate (`PERSISTED_QUERY_NOT_FOUND`), client now falls back to a direct POST GraphQL query against IMDb to fetch episode credits.
  - Direct response is reshaped to the existing parser contract, so downstream sync code remains unchanged.
  - Validation: `ruff check trr_backend/integrations/imdb/episodic_client.py` and `python -m py_compile trr_backend/integrations/imdb/episodic_client.py`.
  - Live verification on show `940ca82c-e4a9-45b4-9d85-946a654925ce`: `sync_episode_appearances` now succeeds (`occurrences_inserted=51`, `failures=0`).

- Pipeline performance controls added for show photo refresh:
  - `api/routers/admin_show_sync.py` now accepts fast-mode fields on `refresh-photos/stream`:
    - `skip_auto_count`, `skip_word_detection`, `imdb_mediaindex_max_pages`, `imdb_mediaindex_max_images`
  - This allows the app to run a reduced-cost refresh path for everyday syncs and avoid long AI post-processing delays.
- Added Bravo sync-readiness enforcement before preview/commit:
  - `POST /api/v1/admin/shows/{show_id}/import-bravo/preview`
  - `POST /api/v1/admin/shows/{show_id}/import-bravo/commit`
  - Both now require existing synced seasons, episodes, and cast; otherwise return `409` with missing sections.
  - File: `api/routers/admin_show_bravo.py`
- Added Fandom policy guard:
  - Show refresh cast-photo stage skips Fandom sources for non-`Real Housewives` shows.
  - Person refresh (sync + stream) also skips Fandom profile/source fetches for non-`Real Housewives` when show context is provided.
  - Files: `api/routers/admin_show_sync.py`, `api/routers/admin_person_images.py`
- Test/lint validation for this update:
  - `ruff check api/routers/admin_show_sync.py api/routers/admin_person_images.py api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `pytest -q tests/api/routers/test_admin_show_bravo.py tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_person_images.py` (`31 passed`)

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

Refresh stream stability + text-overlay reasoned fallback (this session):
- Extended refresh/reprocess summaries in `api/routers/admin_person_images.py`:
  - Added `text_overlay_unknown`, `text_overlay_failure_reasons`
  - Added metadata enrichment counters: `episode_metadata_tagged`, `show_context_tagged`, `metadata_enrichment_failed`
  - Stream `complete` payload now includes `run_id` and new counters.
- Added explicit metadata enrichment stage progress in refresh stream (`stage=metadata_enrichment`).
- Standardized SSE error payload shape for person/reprocess stream setup errors:
  - `{ run_id, stage, error, detail? }`.
- Updated text-overlay batch helpers in `api/routers/admin_person_images.py`:
  - `_detect_text_overlay_cast_photos` and `_detect_text_overlay_media_links` now return `(attempted, succeeded, unknown, failed)`
  - Capture reason-bucket counts via `reason_counts` map.
- Hardened Gemini text overlay pipeline in `trr_backend/vision/text_overlay.py`:
  - Unknown outcomes now include `reason_code` persisted to metadata (`text_overlay_error_code`).
  - Gemini request/no-text/JSON parse issues now persist `unknown` result instead of hard-failing after bytes are downloaded.
  - Added hosted-key fallback download path: if URL fetch fails, attempt S3 object bytes via `hosted_key`.
  - Added helper `classify_text_overlay_failure_reason(...)` for backend reason buckets.
- Added response field passthrough for per-photo detection endpoints:
  - `api/routers/admin_cast_photos.py`: `text_overlay_error_code`
  - `api/routers/admin_media_assets.py`: `text_overlay_error_code`

Tests and checks:
- `ruff check api/routers/admin_person_images.py trr_backend/vision/text_overlay.py api/routers/admin_cast_photos.py api/routers/admin_media_assets.py tests/vision/test_text_overlay_fallback.py tests/api/routers/test_admin_person_images.py` (pass)
- `pytest -q tests/vision/test_text_overlay_fallback.py tests/api/routers/test_admin_person_images.py` (pass; 26 passed)
- `python -m py_compile api/routers/admin_person_images.py trr_backend/vision/text_overlay.py api/routers/admin_cast_photos.py api/routers/admin_media_assets.py` (pass)

New/updated test coverage:
- `tests/vision/test_text_overlay_fallback.py`
  - reason code persisted for unknown results
  - existing metadata reads persisted `text_overlay_error_code`
  - hosted-key S3 fallback when URL download fails
- `tests/api/routers/test_admin_person_images.py`
  - refresh success payload now asserts presence of new counters/fields.

People-count semantics for source/library tags (this session):
- Updated source import tag context generation in `api/routers/admin_scrape.py` (`_build_people_tags_context`).
  - Source/library-matched person tags now persist only `people_ids` / `people_names`.
  - Removed automatic `people_count` + `people_count_source="manual"` writes from source-tag context.
- Updated manual-detection helpers so manual protection applies to explicit manual count source only:
  - `trr_backend/repositories/cast_photo_tags.py` (`has_manual_tags`)
  - `trr_backend/repositories/media_links.py` (`has_manual_people_tags`)
- Result:
  - Known-person tagging no longer implies authoritative people count.
  - Auto-count flows are no longer blocked merely because person tags exist.
  - Explicit manual counts (where `people_count_source="manual"`) are still protected unless force override is used.

Verification:
- `ruff check api/routers/admin_scrape.py trr_backend/repositories/cast_photo_tags.py trr_backend/repositories/media_links.py` (pass)
- `ruff format api/routers/admin_scrape.py` + `ruff format --check ...` (pass)
- `pytest tests/api/routers/test_admin_image_counts_fallback.py -q` (pass; 4 passed)

Fandom profile mis-attachment guard (this session):
- Root cause discovered: `search_real_housewives_wiki("Reza Farahan")` can return `Sutton_Stracke`, and `admin_person_images._refresh_fandom_profile(...)` previously upserted the parsed profile without person-name validation.
- Added strict name-match safeguards in `api/routers/admin_person_images.py`:
  - `_normalize_name_for_match`, `_names_match`, `_name_from_fandom_url`, `_fandom_profile_matches_person_name`
  - `_refresh_fandom_profile` now skips candidate URLs and parsed payloads when names do not match target person.
- Result: Fandom refresh no longer attaches unrelated pages to a person when search returns an incorrect top result.
- Applied one-time local data correction for Reza (`person_id=d3e56687-6d11-43fb-9f99-b8f45c9b5ff1`):
  - deleted mismatched `core.cast_fandom` row pointing to Sutton page (`id=fed4a4f6-43f5-4c51-b5bf-5d7bcf9dc5f3`).

Verification:
- `ruff check api/routers/admin_person_images.py` (pass)
- `ruff format --check api/routers/admin_person_images.py` (pass)
- `pytest tests/api/routers/test_admin_person_images.py -q` (pass; 16 passed)
- Runtime check: parsed Sutton profile no longer matches expected `Reza Farahan` via new matcher.

Bravo import + cast eligibility support (this session, 2026-02-11):
- Added Bravo source migration:
  - `supabase/migrations/0117_add_bravo_source.sql`
- Added Bravo parser module:
  - `trr_backend/scraping/bravo_parser.py`
- Added admin Bravo endpoints and persistence/commit logic:
  - `api/routers/admin_show_bravo.py`
  - wired in `api/main.py`
- Implemented persisted snapshot read model for videos/news (no live scrape on read), plus person snapshot merge behavior.
- Commit logic includes:
  - show description update
  - person `biography/homepage/profile_image_url` source updates under `bravo`
  - social merge policy fill-missing-only
  - selected show image import via existing scrape pipeline
- Updated videos endpoint default to `merge_person_sources=true` to match task defaults.
- Added tests:
  - `tests/scraping/test_bravo_parser.py`
  - `tests/api/routers/test_admin_show_bravo.py`

Validation (this session):
- `ruff check api/routers/admin_show_bravo.py trr_backend/scraping/bravo_parser.py tests/api/routers/test_admin_show_bravo.py tests/scraping/test_bravo_parser.py` (pass)
- `pytest -q tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (pass, 6 passed)

Bravo parser refinement (this session, 2026-02-11):
- Updated `trr_backend/scraping/bravo_parser.py` to improve show import preview quality:
  - airs extraction now recognizes day/time strings like `Tuesdays at 8/7c`
  - show image candidate extraction now excludes video/news thumbnails, cast headshots, and global social/site icon assets
- Added regression tests in `tests/scraping/test_bravo_parser.py` for:
  - day/time airs extraction
  - video/news image candidate exclusion on show page parsing
- Validation:
  - `ruff check trr_backend/scraping/bravo_parser.py tests/scraping/test_bravo_parser.py` (pass)
  - `pytest -q tests/scraping/test_bravo_parser.py` (pass, 5 passed)
  - live parse check for `https://www.bravotv.com/summer-house` now returns `airs_text = "Tuesdays at 8/7c"` and 3 show image candidates.

Bravo preview metadata/date improvements (this session, 2026-02-11):
- `trr_backend/scraping/bravo_parser.py`
  - Added `published_at` extraction for videos/news.
  - Video list scraping now prefers `/watch/videos` so season feed aligns with Bravo filter defaults (e.g. Summer House Season 10).
  - Hydrates missing `published_at` by reading clip/article pages when needed.
- `api/routers/admin_show_bravo.py`
  - Normalized show/person video/news payloads now carry `published_at` through persisted snapshots + read endpoints.
- Tests updated:
  - `tests/scraping/test_bravo_parser.py` now covers video/news `published_at` extraction.
- Validation:
  - `ruff check trr_backend/scraping/bravo_parser.py api/routers/admin_show_bravo.py tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (pass)
  - `pytest -q tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (pass, 8 passed)

Season Social Analytics V2 (this session, 2026-02-11):
- Added season social analytics schema migration:
  - `supabase/migrations/0118_social_season_analytics.sql`
  - Adds `social.season_targets` and season/job/source lineage columns across `social.*` tables.
- Added backend social analytics + ingest repository:
  - `trr_backend/repositories/social_season_analytics.py`
  - Supports:
    - season target defaults/updates (`bravo` scope)
    - ingest job creation/status updates
    - normalized + `raw_data` persistence for posts/comments across Instagram, TikTok, YouTube, Twitter
    - sentiment scoring + weekly/platform aggregations
    - CSV and PDF export builders
- Extended admin socials router with season endpoints:
  - `GET /api/v1/admin/socials/seasons/{season_id}/targets`
  - `PUT /api/v1/admin/socials/seasons/{season_id}/targets`
  - `POST /api/v1/admin/socials/seasons/{season_id}/ingest`
  - `GET /api/v1/admin/socials/seasons/{season_id}/ingest/jobs`
  - `GET /api/v1/admin/socials/seasons/{season_id}/analytics`
  - `GET /api/v1/admin/socials/seasons/{season_id}/analytics/export.csv`
  - `GET /api/v1/admin/socials/seasons/{season_id}/analytics/export.pdf`
- Added PDF dependency:
  - `requirements.txt` now includes `reportlab>=4.2.0`
- Added tests:
  - `tests/api/routers/test_socials_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`

Validation (this session):
- `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
- `python -m py_compile api/routers/socials.py trr_backend/repositories/social_season_analytics.py` (pass)
- `pytest -q tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass, 5 passed)

Bravo show-image media-type selection support (this session, 2026-02-11):
- File: `api/routers/admin_show_bravo.py`
- Added new commit request field:
  - `selected_show_images: [{url, kind}]` (optional)
  - `kind` enum: `poster|backdrop|logo|episode_still|cast|promo|intro|reunion|other`
- Backward-compatible behavior:
  - if `selected_show_images` is omitted, legacy `selected_show_image_urls[]` is still accepted and imported as `promo`.
- Commit import behavior:
  - selected image `kind` now flows into `ImportImageItem.kind` (previously always `promo`).
- Tests:
  - `tests/api/routers/test_admin_show_bravo.py` adds assertion that selected kinds (`logo`, `poster`) are passed to import request.
- Validation:
  - `uv run ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `uv run pytest tests/api/routers/test_admin_show_bravo.py -q`

Bravo cast hero-image ingest + tag merge + season filter (this session, 2026-02-11):
- File: `api/routers/admin_show_bravo.py`
- Cast image ingest:
  - Added `_import_bravo_person_image(...)` and invoke it during commit for each resolved person with `hero_image_url`.
  - Uses existing `admin_scrape.import_images` with `entity_type=person`, so media assets are mirrored to S3 and linked to person media.
  - Commit response now includes `counts.imported_person_images`, `counts.skipped_person_images`, and `person_image_import_errors`.
- Person/article/video tagging:
  - Enhanced dedupe flow to merge `person_tags` across duplicate URLs instead of keeping only first tag set.
  - Affects show/person merged video/news normalization and read extraction.
- Season filter support:
  - Added optional `season_number` to Bravo preview/commit request models.
  - Applies to show/person videos before preview and persistence (`_filter_bundle_by_season`).
- Tests updated in `tests/api/routers/test_admin_show_bravo.py`:
  - selected show image kind passthrough remains covered.
  - preview season filter behavior covered.
  - dedupe person-tag merge covered.
- Validation:
  - `uv run ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `uv run pytest tests/api/routers/test_admin_show_bravo.py -q` (6 passed)

Bravo show-news relevance filter hardening (this session, 2026-02-11):
- File: `trr_backend/scraping/bravo_parser.py`
- Changes:
  - Added show-news relevance filters to drop unrelated "Latest Bravo News" sidebar cards.
  - Relevance checks now use show slug/title and discovered cast-person slug phrases.
  - `parse_show_news(...)` now accepts optional `show_title` and `person_urls` and applies relevance filtering before return.
  - `parse_bravo_show_bundle(...)` now calls `parse_show_news(...)` with show title + discovered person URLs.
- Tests:
  - Added `test_parse_show_news_ignores_unrelated_latest_sidebar_items` in `tests/scraping/test_bravo_parser.py`.
- Validation:
  - `uv run ruff check trr_backend/scraping/bravo_parser.py tests/scraping/test_bravo_parser.py api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `uv run pytest tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py -q` (12 passed)

Season social week-model update (this session, 2026-02-11):
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `api/routers/socials.py`
  - `tests/repositories/test_social_season_analytics.py`
  - `tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Implemented episode-anchored week windows with explicit `Week 0` support.
  - `Week 0` is now variable length: trailer/first-look/sneak-peek drop -> premiere (episode 1 air datetime).
  - Week windows now follow episode boundaries (`Week 1 = Ep1->Ep2`, etc.) instead of fixed 7-day bins from anchor date.
  - Added `window.week_zero_start` to analytics response.
  - Added `week=0` support to analytics/export endpoints (router validation now `ge=0`).
  - Added Bravo-first platform scoping in analytics row queries so `source_scope=bravo` filters to Bravo-owned accounts/channels.
  - Tightened trailer start fallback to pre-premiere lookback (180d) and season-specific marker preference (`season N` / `sN`).
- Tests:
  - Expanded repository unit tests for trailer-marker parsing and season matching.
  - Added API route test confirming `week=0` is accepted and forwarded.
- Validation:
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass, 8 passed)
  - Live RHOSLC check now returns:
    - `window.week_zero_start = 2025-09-14T21:31:09+00:00`
    - non-zero weeks at 0, 13, 16 under Bravo scope.

Weekly run controls + platform weekly table support (this session, 2026-02-11):
- File: `trr_backend/repositories/social_season_analytics.py`
- Changes:
  - Added `weekly_platform_posts` to analytics response:
    - per-week post counts split by `instagram/youtube/tiktok/twitter` plus `total_posts`.
  - Added Bravo-scope account/channel filtering in analytics row queries (`source_scope='bravo'`):
    - Instagram/TikTok/Twitter restricted to Bravo accounts (`bravotv`/`bravo`).
    - YouTube restricted to Bravo channel title/source account (`bravo`/`bravotv`).
  - Maintains Week 0 variable window and episode-bounded weekly windows from prior update.
- Validation:
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - Live run: week-scoped + platform-scoped ingest confirmed with explicit date_start/date_end in saved scrape_job config.

Pre-Season + 8pm episode boundary update (this session, 2026-02-11):
- File: `trr_backend/repositories/social_season_analytics.py`
- Changes:
  - Week 0 is now labeled `Pre-Season` in analytics output.
  - Episode week boundaries now switch at `20:00 America/New_York` on episode air dates (instead of midnight).
  - Added optional season-target config overrides for Week 0 start:
    - `config.preseason_start`
    - `config.preseason_start_at`
    - `config.week_zero_start`
  - Week 0 start resolution order:
    1. season target config override,
    2. Bravo show snapshot season videos,
    3. season social rows trailer markers,
    4. fallback.
- Operational data update:
  - Persisted RHOSLC S6 Bravo season targets and set `config.preseason_start = 2025-08-14T00:00:00-04:00` for all four platforms.
- Validation:
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - Live RHOSLC check now returns:
    - Week 0 (`Pre-Season`) ET window: `2025-08-14 00:00` -> `2025-09-16 19:59:59.999999`
    - Week 1 start ET: `2025-09-16 20:00`.

RHOSLC term-match ingest criteria update (this session, 2026-02-11):
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `trr_backend/socials/twitter/scraper.py`
  - `tests/repositories/test_social_season_analytics.py`
- Changes:
  - Added canonical Bravo term matching helpers so ingest can match caption/text on any of:
    - `Salt Lake City`
    - `RHOSLC`
    - `#RHOSLC`
  - Updated default Bravo season targets to include RHOSLC aliases in target keywords/hashtags (instead of only full show-name slug).
  - Instagram/TikTok ingest now filters posts with OR semantics across keywords + hashtags via caption/description text matching.
  - YouTube ingest now enforces the same term matching before persistence (title/description text).
  - Twitter ingest now:
    - builds advanced query as `from:{account}` + OR-term clause,
    - supports advanced query passthrough in scraper query builder,
    - applies final text-based term guard before persistence.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py trr_backend/socials/twitter/scraper.py tests/repositories/test_social_season_analytics.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass, 10 passed)
- Note:
  - Ingest now merges season-target terms with show-derived fallback aliases at runtime, so previously saved targets still get RHOSLC/Salt Lake City matching without a manual target reset.

Bravo profile import semantics for season promos (this session, 2026-02-11):
- File: `api/routers/admin_show_bravo.py`
- Changes:
  - Added `_resolve_season_id(show_id, season_number)` guard to validate/resolve season before commit import.
  - `commit_bravo_import` now resolves requested `season_number` and returns `404` if season is missing for the show.
  - `_import_bravo_person_image` now supports season-scoped import and maps Bravo profile hero images to:
    - `kind="promo"`,
    - `context_section="bravo_profile"`,
    - `context_type="profile"`,
    - `asset_name="Bravo profile image"`.
  - For season-scoped imports, hero images are imported as `entity_type="season"` with `person_ids` attached so person-gallery links are also created by existing import logic.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_bravo.py -q` (pass).

Bravo profile social links -> people external_ids normalization (this session, 2026-02-11):
- File: `api/routers/admin_show_bravo.py`
- Changes:
  - Expanded social-link merge logic so Bravo profile socials are normalized into canonical person external ID keys.
  - Incoming social values now hydrate both legacy and canonical keys for compatibility:
    - `instagram` + `instagram_id` (+ `instagram_url`)
    - `twitter` + `twitter_id` (+ `twitter_url`)
    - `facebook` + `facebook_id` (+ `facebook_url`)
    - `tiktok` + `tiktok_id` (+ `tiktok_url`)
    - `youtube` + `youtube_id` (+ `youtube_url`)
  - Added URL/handle normalization and non-overwrite behavior that respects existing values.
- Tests:
  - Updated `tests/api/routers/test_admin_show_bravo.py` to assert canonical social key hydration and URL normalization.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_bravo.py -q` (pass)
  - `python -m py_compile api/routers/admin_show_bravo.py` (pass)

RHOSLC S6 Bravo re-sync execution + social external ID backfill (this session, 2026-02-11):
- Runtime action executed against backend admin endpoint:
  - `POST /api/v1/admin/shows/7782652f-783a-488b-8860-41b97de32e75/import-bravo/commit` with `season_number=6`.
- Commit result summary:
  - `people_updated=8`
  - `unmatched_people=0`
  - `person_snapshots=8`
  - `imported_person_images=0`, `skipped_person_images=8`
- Social external IDs outcome after sync:
  - RHOSLC S6 cast with social IDs on `core.people.external_ids`: 5 members
  - Handles now visible for Instagram/Twitter where present.

YouTube generic placeholder guard (this session, 2026-02-11):
- Files:
  - `api/routers/admin_show_bravo.py`
  - `tests/api/routers/test_admin_show_bravo.py`
- Changes:
  - Prevented generic YouTube placeholders (`user`, `channel`, `c`) from being written as person external IDs.
  - Added regression test for skipping generic YouTube placeholder URLs.
- One-time data cleanup executed:
  - Removed `youtube`/`youtube_id`/`youtube_url` keys for RHOSLC S6 cast rows where value was generic placeholder.
  - Rows cleaned: 8.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_bravo.py -q` (pass)
  - `python -m py_compile api/routers/admin_show_bravo.py` (pass)

PR merge/readiness pass (this session, 2026-02-12):
- Merged `origin/main` into `feat/admin-media-pipeline-enhancements`.
- Resolved merge conflicts in:
  - `api/routers/admin_person_images.py`
  - `docs/ai/HANDOFF.md`
- Validation:
  - `python -m py_compile api/routers/admin_person_images.py`
  - `ruff check api/routers/admin_person_images.py`
  - `pytest -q tests/api/routers/test_admin_person_images.py` (16 passed)
- Branch pushed and PR #56 is merge-clean with passing checks.
