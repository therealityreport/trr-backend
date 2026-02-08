# Other Projects — TASK3 (Multi-Person Tagged Image Dedup)

Repo: TRR-Backend  
Last updated: February 8, 2026

## Cross-Repo Snapshot
- TRR-Backend: writes shared cast-photo hosted keys under `media/{sha256[:2]}/{sha256}{ext}`.
- TRR-APP: dedupes People gallery by canonical image identity (source+source_image_id, fallback hosted_sha256), and prefers media_links rows on collision.
- SCREENALYTICS: no code changes required.

## Dependency Order
1. Deploy TRR-Backend.
2. Deploy TRR-APP.
3. Run targeted backfill: refresh+force_mirror on affected people.

## Risks / Notes
- If the same logical image is fetched at different transforms (different bytes), backend storage may still have multiple objects; TRR-APP canonical dedupe still prevents duplicate rendering.
