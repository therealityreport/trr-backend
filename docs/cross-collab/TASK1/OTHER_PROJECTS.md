# Other Projects — Task 1 (Media Workflow Enhancements)

Repo: TRR-Backend  
Last updated: February 8, 2026

## Cross-Repo Dependency Snapshot
- TRR-Backend provides the text-overlay detector endpoint and persists `has_text_overlay` into `core.media_assets.metadata`.
- TRR-Backend persists manual People tags during scrape import when `person_ids` are provided.
- TRR-Backend URL scrape preview (`/admin/scrape/preview`) may include `bytes` per candidate for UI display (best-effort).
- TRR-Backend scrape import kind allowlist includes `promo`, `intro`, `reunion` in addition to existing kinds.
- TRR-APP consumes both capabilities and exposes allowlist-only admin UI and proxy routes.
- SCREENALYTICS has no required changes for this task set.

## TRR-APP (Consumes Backend Contracts)
- Proxies detection via:
  - `POST /api/admin/trr-api/media-assets/[assetId]/detect-text-overlay`
- Uses `core.media_links.context.people_*` fields (written on import) to drive SOLO/GROUP filtering.
- Expects show/season galleries to render assets linked through `core.media_links` + `core.media_assets`.

## SCREENALYTICS (No Code Change)
- No required changes.
- If SCREENALYTICS later wants to consume text-overlay status, the single source of truth is `core.media_assets.metadata.has_text_overlay`.
