# Other Projects — Task 6 (Admin “Sync/Refresh Shows”)

Repo: TRR-Backend
Last updated: February 10, 2026

## Cross-Repo Snapshot

- TRR-Backend: Implemented admin endpoints. See TRR-Backend TASK6.
- TRR-APP: Add admin proxy routes + UI buttons. See TRR-APP TASK5.
- screenalytics: Not impacted.

## Responsibility Alignment

- TRR-Backend
  - Owns the sync/import logic and exposes admin-only endpoints.
  - Ensures response shapes are stable and additive.
- TRR-APP
  - Owns admin UI buttons and proxy routes (Firebase-admin gated) to call TRR-Backend using service role credentials.
- screenalytics
  - No changes required for this task.

## Dependency Order

1. TRR-Backend: add endpoints + tests (this repo).
2. TRR-APP: add proxy routes and wire UI controls.

## Locked Contracts (Mirrored)

- TRR-APP → TRR-Backend base URL is `TRR_API_URL` normalized to `/api/v1`.
- Admin endpoints must require `AdminUser` and accept service role JWTs.
- List sync does Import + Enrich and does not fetch show images.

