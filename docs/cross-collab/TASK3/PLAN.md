# TASK3 — Multi-Person Tagged Image Dedup (Shared File + Shared Gallery)

Repo: TRR-Backend  
Last updated: February 10, 2026

## Goal
Fix multi-person tagged images so:
- Only **one** canonical image exists in storage (single S3/CDN object for identical bytes).
- The image appears **once** per person gallery (no duplicates in a single person’s gallery).
- The same image appears in **all** tagged people’s galleries.

## Background / Problem
IMDb gallery ingestion imports the same logical image into `core.cast_photos` separately per person.
TRR-APP then merges:
- photos owned by the person (`cast_photos.person_id = personId`)
- plus photos where the person’s name appears in `cast_photos.people_names`

Because `mirror_cast_photo_row` previously used person-scoped S3 keys, the same image produced different `hosted_url`s, so UI dedupe-by-URL failed and the image appeared multiple times in one gallery.

## Status Snapshot (As of February 10, 2026)
Complete.

- Shared deterministic `hosted_key` for cast photo mirroring shipped:
  - `trr_backend/media/s3_mirror.py#build_shared_media_s3_key` produces `media/{sha256[:2]}/{sha256}{ext}`.
  - `trr_backend/media/s3_mirror.py#mirror_cast_photo_row` now mirrors cast photos to the shared `media/…` namespace (no per-person hosted keys).
- Identical bytes mirrored for multiple people now converge to the same `hosted_key`/`hosted_url`, enabling canonical UI dedupe and eliminating duplicates in a single person gallery.

## Backend Scope (This Repo)
### 1) Shared deterministic hosted_key for cast photo mirroring
Implemented in `trr_backend/media/s3_mirror.py#mirror_cast_photo_row`:
- Hosted key format: `media/{sha256[:2]}/{sha256}{ext}`
- Behavior:
  - Identical bytes converge to the same `hosted_key` and `hosted_url`.
  - Subsequent mirrors hit `HEAD` and avoid re-upload.

### 2) Prune safety
Person prune remains scoped to the legacy prefix `images/people/{person_identifier}/photos/`.
Shared `media/…` objects are not enumerated by that prefix and therefore are not deleted by per-person prune.
Additionally, per-person prune only considers legacy cast-photo subfolders (e.g. `imdb/`, `tmdb/`, `fandom/`) and will not delete other valid person-gallery objects under the same prefix (e.g. `web_scrape/` media assets).

## Rollout / Backfill
1. Deploy TRR-Backend.
2. Re-run `POST /api/v1/admin/person/{person_id}/refresh-images` with `force_mirror=true` for affected people.
   - This rewrites old person-scoped `hosted_key`s to shared `media/…` keys.
3. Optional: run person prune to remove leftover per-person objects.

## Validation
 - Pick a known multi-person IMDb image (e.g. `rm4170403585`).
 - After refresh+force_mirror for each tagged person:
   - All related cast_photos rows should share the same `hosted_key` and `hosted_url`.
- Repo fast checks: `ruff check . && ruff format --check . && python -m pytest -q` passing.

## Follow-up (Not In Scope)
Face-level tagging UX: detect faces per image, then allow admins to assign which face is which person.
