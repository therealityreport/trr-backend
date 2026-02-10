# Other Projects — Task 1 (Media Workflow Enhancements)

Repo: TRR-Backend  
Last updated: February 10, 2026

Status Snapshot (As of February 10, 2026)
Complete.

## Cross-Repo Dependency Snapshot
- TRR-Backend: text overlay detector endpoints shipped:
  - `POST /api/v1/admin/media-assets/{asset_id}/detect-text-overlay?force={bool}`
  - `POST /api/v1/admin/cast-photos/{photo_id}/detect-text-overlay?force={bool}`
  - Detector source is tracked in `trr_backend/vision/text_overlay.py` (Gemini-based; gated by env; returns `503` if not configured).
- TRR-Backend persists manual People tags during scrape import when `person_ids` are provided.
- TRR-Backend URL scrape preview (`/admin/scrape/preview`) may include `bytes` per candidate for UI display (best-effort).
- TRR-Backend scrape import kind allowlist includes `promo`, `intro`, `reunion` in addition to existing kinds.
- TRR-Backend improves scrape context extraction for cast-photo pages (per-image context includes name + caption).
- TRR-Backend persists article publish date for cast-photo imports into `metadata.source_created_at` (so UI shows **Created**).
- TRR-Backend supports admin cleanup via `DELETE /api/v1/admin/media-assets/{asset_id}`.
- TRR-APP consumes both capabilities and exposes allowlist-only admin UI and proxy routes.
- SCREENALYTICS has no required changes for this task set.

## TRR-APP (Consumes Backend Contracts)
- Proxies detection via:
  - `POST /api/admin/trr-api/media-assets/[assetId]/detect-text-overlay`
  - `POST /api/admin/trr-api/cast-photos/[photoId]/detect-text-overlay`
- Uses `core.media_links.context.people_*` fields (written on import) to drive SOLO/GROUP filtering.
- Expects show/season galleries to render assets linked through `core.media_links` + `core.media_assets`.

## SCREENALYTICS (No Code Change)
- No required changes.
- If SCREENALYTICS later wants to consume text-overlay status, the single source of truth is `core.media_assets.metadata.has_text_overlay`.

## Locked Contracts (No Pending Changes)
1. Backend toggle endpoint: `PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed`.
2. Toggle payload: `{ "facebank_seed": boolean }`.
3. Toggle response: `{ "link_id": string, "person_id": string, "facebank_seed": boolean }`.
4. Screenalytics photos endpoint: `GET /api/v1/screenalytics/people/{person_id}/photos?seed_only={bool}`.
5. Service auth for app proxy path: `Authorization: Bearer <service_role>` + `X-TRR-Internal-Admin-Secret`.
6. Screenalytics must use `served_url` and strict fallback: only call `seed_only=false` after successful empty `seed_only=true`.

## Addendum (Admin Media Enhancements)
- TRR-Backend calls the people-count endpoint; ensure `/vision/people-count` is reachable at `SCREENALYTICS_API_URL` (TRR-Backend side).
- If TRR-Backend is configured with an incompatible path, set `SCREENALYTICS_API_PATH=/vision/people-count` (TRR-Backend) rather than changing SCREENALYTICS routing.

## Responsibility Alignment
- TRR-Backend
  - Owns `core.media_links.facebank_seed` schema + view exposure.
  - Enforces allowlist-only admin auth and scoped service-role + internal-secret auth for toggle endpoint.
- TRR-APP
  - Owns admin UI toggle and proxy route.
  - Must forward `Authorization` service-role token and `X-TRR-Internal-Admin-Secret`.
  - Must require allowlisted admin access before proxying.
- SCREENALYTICS
  - Owns seed-first fetch behavior and strict fallback logic.
  - Uses `served_url` and persists rows through `import_facebank_images`.

## Operational Findings (Completed)
1. Targeted regression suites passed in all repos.
2. Backend local auth guard behavior verified (`403`, `403`, `200`).
3. Backend local `seed_only=true/false` subset behavior verified.
4. Strict fallback request sequence observed in backend logs.
5. Production TRR-APP proxy route verified end-to-end (allowlisted admin).
6. Production backend auth guard verified (service role requires internal shared secret).
7. Production `seed_only` contract verified (seeded subset vs non-seeded superset).
8. SCREENALYTICS `sync_cast_from_trr` import hook verified (seeded + unseeded) with `import_errors=0`.
9. SCREENALYTICS DB side effects verified (`face_bank_images` inserts + dedupe).

## Open Operational Risks / Blockers
- None.

## Dependency Order (For Final Closeout)
1. Deploy TRR-Backend (`main`, includes PR `#44`).
2. Deploy TRR-APP (`main`, includes PR `#18/#19`).
3. Deploy SCREENALYTICS (`main`, includes PR `#187`).
4. Run staging full smoke and DB side-effect checks.
5. Roll production in same order and run minimal smoke.
6. Mark all Task 1 docs completed only after both environments pass. (Now complete as of February 6, 2026.)

## Completion Metadata
- Completion date: February 6, 2026
- PR references:
  - TRR-Backend `#44`
  - TRR-APP `#18`, `#19`
  - SCREENALYTICS `#187`
