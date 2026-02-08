# Task 1 — Media Workflow Enhancements (Backdrops, People Tags, Text Overlay, Advanced Filters)

Repo: TRR-Backend  
Last updated: February 8, 2026

## Summary
This task adds backend support needed by TRR-APP’s upgraded admin media workflows:
- Persist manual People tagging during scrape imports (so SOLO/GROUP filtering works deterministically).
- Add a text-overlay detector and persist results to `core.media_assets.metadata`.
- Run “agent-like” text-overlay detection opportunistically during import and refresh flows (capped) to reduce unknowns.

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

### Text overlay detection endpoint (admin)
- `POST /api/v1/admin/media-assets/{asset_id}/detect-text-overlay`
  - Body: `{ "force": boolean }` (optional, default false)
  - Behavior:
    - If `metadata.has_text_overlay` already exists and `force=false`, returns existing values.
    - Otherwise downloads the image (`hosted_url` preferred, fallback `source_url`), classifies with Gemini Vision, persists into `core.media_assets.metadata`, and returns the persisted fields.

### Import/refresh hooks (agent-like behavior)
- Scrape import and person refresh should attempt text-overlay detection for assets missing `has_text_overlay` (capped per request).
- Responses may include counters:
  - `text_overlay_attempted`, `text_overlay_succeeded`, `text_overlay_failed`

## Storage Contract (Single Source of Truth)
Text overlay results are stored on `core.media_assets.metadata`:
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
- `env REDIS_URL= pytest -q tests/test_api_smoke.py` passing.

Note: `google-generativeai` is lazy-imported inside the detector function to avoid import-time hangs in test environments; the endpoint only requires the dependency when invoked.
