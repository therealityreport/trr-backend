# Task 1 — Media Workflow Enhancements (Backdrops, People Tags, Text Overlay, Advanced Filters)

Repo: TRR-Backend  
Last updated: February 10, 2026

## Summary
This task adds backend support needed by TRR-APP’s upgraded admin media workflows:
- Persist manual People tagging during scrape imports (so SOLO/GROUP filtering works deterministically).
- Add a text-overlay detector and persist results to `core.media_assets.metadata` (and `core.cast_photos.metadata` for legacy cast photos).
- Run “agent-like” text-overlay detection opportunistically during import and refresh flows (capped) to reduce unknowns.
- Improve gallery scrape context extraction for cast-photo style pages (e.g. E! Online) so per-image context includes the person name + caption/bio.
- For cast-photo imports (kind=`cast`), persist article publish date into `core.media_assets.metadata.source_created_at` (and per-link context) so UI can show **Created**.
- Support admin cleanup via `DELETE /api/v1/admin/media-assets/{asset_id}` (delete unified `media_assets` + `media_links`, best-effort S3 delete).

## Status Snapshot (As of February 10, 2026)
Complete.
- Shipped (code present in this repo):
  - Scrape preview `bytes` field (best-effort).
  - Scrape import kind allowlist includes `promo`, `intro`, `reunion`.
  - Manual People tagging context persisted on import (`core.media_links.context.people_*`).
  - Cast-photo imports persist `metadata.source_created_at` and per-link `context.source_created_at`.
  - Admin cleanup endpoint: `DELETE /api/v1/admin/media-assets/{asset_id}`.
- Text overlay detection shipped (backend-owned):
  - Media assets: `POST /api/v1/admin/media-assets/{asset_id}/detect-text-overlay?force={bool}`
  - Cast photos: `POST /api/v1/admin/cast-photos/{photo_id}/detect-text-overlay?force={bool}`
  - Tracked detector module: `trr_backend/vision/text_overlay.py` (Gemini-based, gated by env; returns `503` if not configured).

## Locked Contracts / Interfaces

### URL scrape preview candidate size (admin)
- `POST /api/v1/admin/scrape/preview`
  - Response: each `images[]` item may include `bytes: int | null` (best-effort `Content-Length` via HEAD).
  - Behavior:
    - Additive only; callers must treat missing/`null` size as “unknown” and continue.
    - Preview must not fail if HEAD is blocked or `Content-Length` is absent.

### Scrape import kinds (admin)
- `POST /api/v1/admin/scrape/import` and `POST /api/v1/admin/scrape/import/stream`
  - `images[].kind` allowed values include:
    - Existing: `poster`, `backdrop`, `episode_still`, `cast`, `other`
    - Added: `promo`, `intro`, `reunion`

### Cast photo created date (admin scrape import)
- When `images[].kind="cast"`:
  - Backend will fetch the source page HTML best-effort and extract an article/gallery publish date.
  - Persist into `core.media_assets.metadata.source_created_at` and also include on the created `core.media_links.context`.

### Delete unified media assets (admin)
- `DELETE /api/v1/admin/media-assets/{asset_id}`
  - Deletes `core.media_links` referencing the asset and then deletes the `core.media_assets` row.
  - Best-effort deletes the S3 object (`hosted_key`) when present.

### Text overlay detection endpoint (admin)
- Media assets:
  - `POST /api/v1/admin/media-assets/{asset_id}/detect-text-overlay?force={bool}`
- Cast photos (legacy):
  - `POST /api/v1/admin/cast-photos/{photo_id}/detect-text-overlay?force={bool}`
- Behavior (both):
  - If `metadata.has_text_overlay` already exists and `force=false`, returns existing values.
  - Otherwise downloads the image (`hosted_url` preferred, fallback `source_url`/`url`), classifies with Gemini Vision, persists into the row `metadata`, and returns the persisted fields.

### Import/refresh hooks (agent-like behavior)
- Scrape import and person refresh should attempt text-overlay detection for assets missing `has_text_overlay` (capped per request).
- Responses may include counters:
  - `text_overlay_attempted`, `text_overlay_succeeded`, `text_overlay_failed`

## Storage Contract (Single Source of Truth)
Text overlay results are stored on `core.media_assets.metadata` (and `core.cast_photos.metadata` for legacy cast photos):
- `has_text_overlay: boolean`
- `text_overlay_confidence: number (0..1)`
- `text_overlay_detector: string`
- `text_overlay_model: string | null`
- `text_overlay_detected_at: timestamp string`
- `text_overlay_prompt_version: string`

## Manual People tagging on import
When scrape import payload includes `person_ids`, the created `core.media_links.context` must include:
- `people_ids: string[]`
- `people_names: string[]`
- `people_count: number`
- `people_count_source: "manual"`

## Rollout / Dependency Order
1. Deploy TRR-Backend (this repo).
2. Deploy TRR-APP (UI + proxy + gallery aggregation + filters).
3. SCREENALYTICS: no code changes required for this task set.

## Validation Evidence (Local)
- `ruff check .` passing.
- `ruff format --check .` passing.
- `python -m pytest -q` passing (413 passed, 18 skipped).

Note: `google-generativeai` is lazy-imported inside the detector function to avoid import-time hangs in test environments; the endpoint only requires the dependency when invoked.
