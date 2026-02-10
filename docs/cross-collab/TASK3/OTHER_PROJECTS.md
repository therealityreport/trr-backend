# Other Projects — TASK3 (Multi-Person Tagged Image Dedup)

Repo: TRR-Backend  
Last updated: February 10, 2026

Status Snapshot (As of February 10, 2026)
Complete (TRR-Backend + TRR-APP shipped).

## Cross-Repo Snapshot
- TRR-Backend: shipped — cast-photo mirroring uses shared hosted keys (`media/{sha256[:2]}/{sha256}{ext}`) in `trr_backend/media/s3_mirror.py#mirror_cast_photo_row`.
- TRR-APP: shipped — People gallery dedupes by canonical identity (not just `hosted_url`) and prefers `media_links` rows on collisions (`apps/web/src/lib/server/trr-api/person-photo-utils.ts` + `apps/web/src/lib/server/trr-api/trr-shows-repository.ts#getPhotosByPersonId`).
- SCREENALYTICS: no code changes required.

## Dependency Order
1. Deploy TRR-Backend.
2. Deploy TRR-APP.
3. Run targeted backfill: refresh+force_mirror on affected people.

## Risks / Notes
- If the same logical image is fetched at different transforms (different bytes), backend storage may still have multiple objects; TRR-APP canonical dedupe still prevents duplicate rendering.
