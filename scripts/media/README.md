Media Mirroring (S3-Compatible Object Storage)
==============================================

This directory contains scripts to mirror media assets (cast photos) to S3-compatible object storage and
store hosted URLs back into Supabase.

Scripts live in this directory:
- `scripts/media/mirror_cast_photos_to_s3.py`
- `scripts/media/mirror_media_assets_to_s3.py`
- `scripts/media/bravotv_get_images.py`
- `scripts/media/sync_bravotv_galleries.py`
- `scripts/media/rebuild_hosted_urls.py`
- `scripts/media/backfill_media_asset_variants.py`
- `scripts/media/restore_person_gallery_base_previews.py` (person-scoped preview metadata rollback only)
- `scripts/media/restore_changed_originals.py` (strict hash audit/repair for true hosted/source mismatches)

Required environment variables
------------------------------

- OBJECT_STORAGE_REGION
- OBJECT_STORAGE_ACCESS_KEY_ID
- OBJECT_STORAGE_SECRET_ACCESS_KEY
- OBJECT_STORAGE_BUCKET
- OBJECT_STORAGE_PREFIX (example: dev, prod)
- OBJECT_STORAGE_PUBLIC_BASE_URL (example: https://cdn.example.com)
- TRR_MEDIA_MIRROR_TO_S3 (optional, feature flag for future ingestion hooks)

Notes:
- OBJECT_STORAGE_PUBLIC_BASE_URL must start with https:// and must not contain placeholder domains (e.g., dxxxx).
- If `OBJECT_STORAGE_PROFILE` is set, boto3 uses that profile.

Example usage
-------------

Mirror Fandom cast photos for a single IMDb person:

```
PYTHONPATH=. python scripts/media/mirror_cast_photos_to_s3.py \
  --source fandom \
  --imdb-person-id nm11883948 \
  --limit 50
```

Mirror all pending fandom images (default):

```
PYTHONPATH=. python scripts/media/mirror_cast_photos_to_s3.py --source fandom --limit 200
```

Dry run (no writes):

```
PYTHONPATH=. python scripts/media/mirror_cast_photos_to_s3.py --source fandom --limit 50 --dry-run
```

Troubleshooting
---------------

- Ensure the object-storage bucket and public base URL are correct.
- If images return 403/404 from Fandom, verify the source_page_url is populated
  so the Referer header can be set on download.
- Re-run the script safely; it is idempotent and skips existing hosted URLs
  unless --force is supplied.

Canonical hosted-URL rebuild
----------------------------

Rewrite stale hosted-media URLs onto the current public base without re-uploading:

```bash
PYTHONPATH=. python scripts/media/rebuild_hosted_urls.py --table all --dry-run
PYTHONPATH=. python scripts/media/rebuild_hosted_urls.py --table all
```

Notes:
- Covers unified gallery tables (`media_assets`, `media_asset_variants`) plus legacy image tables and `cast_photos`.
- Rebuilds `hosted_url` from `hosted_key` when available.
- Rewrites embedded legacy hosted-media URLs inside gallery metadata, including `/media-variants/...`, `/cast-photo-variants/...`, and `/face-crops/...` URLs.
- Use `repair_gallery_hosts.py` afterwards only for rows that are still truly broken or unreachable.

Backfill optimized media variants
---------------------------------

Generate `thumb/card/detail` variants (and optional crop variants) for existing `core.media_assets`:

```
PYTHONPATH=. python scripts/media/backfill_media_asset_variants.py --batch-size 50 --with-crops
```

BRAVOTV media runs
------------------

Run the BRAVOTV multi-source image pipeline from the media script namespace:

```
PYTHONPATH=. python scripts/media/bravotv_get_images.py --person "Lisa Barlow" --output /tmp/bravotv-lisa
PYTHONPATH=. python scripts/media/bravotv_get_images.py --show "The Real Housewives of Salt Lake City" --season 5 --output /tmp/bravotv-rhoslc-s5
```

The legacy `scripts/bravotv_get_images.py` entrypoint remains as a deprecation wrapper.

The legacy `scripts/sync/sync_bravotv_galleries.py` entrypoint remains as a deprecation wrapper around
`scripts/media/sync_bravotv_galleries.py`.

Original Integrity Audit/Repair
-------------------------------

Dry-run IMDb mismatch audit across cast photos + person-gallery media assets:

```
PYTHONPATH=. python scripts/media/restore_changed_originals.py --source imdb --tables both --output-json /tmp/imdb-original-integrity-audit.json
```

Apply repair only to verified mismatches:

```
PYTHONPATH=. python scripts/media/restore_changed_originals.py --source imdb --tables both --apply --output-json /tmp/imdb-original-integrity-apply.json
```

Notes:
- `restore_person_gallery_base_previews.py` is intentionally non-destructive and only resets preview crop metadata/context.
- `restore_changed_originals.py` is the script for original hosted/source integrity verification and repair.
