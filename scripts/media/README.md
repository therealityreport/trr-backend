Media Mirroring (S3)
====================

This directory contains scripts to mirror media assets (cast photos) to S3 and
store hosted URLs back into Supabase.

Scripts live in this directory:
- `scripts/media/mirror_cast_photos_to_s3.py`
- `scripts/media/mirror_media_assets_to_s3.py`
- `scripts/media/rebuild_hosted_urls.py`
- `scripts/media/backfill_media_asset_variants.py`
- `scripts/media/restore_person_gallery_base_previews.py` (person-scoped preview metadata rollback only)
- `scripts/media/restore_changed_originals.py` (strict hash audit/repair for true hosted/source mismatches)

Required environment variables
------------------------------

- AWS_REGION (or AWS_DEFAULT_REGION)
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_S3_BUCKET
- AWS_S3_PREFIX (example: dev, prod)
- AWS_CDN_BASE_URL (example: https://cdn.example.com)
- TRR_MEDIA_MIRROR_TO_S3 (optional, feature flag for future ingestion hooks)

Notes:
- AWS_CDN_BASE_URL must start with https:// and must not contain placeholder domains (e.g., dxxxx).
- If AWS_PROFILE or AWS_DEFAULT_PROFILE is set, boto3 uses that profile.

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

- Ensure the S3 bucket and CDN base URL are correct.
- If images return 403/404 from Fandom, verify the source_page_url is populated
  so the Referer header can be set on download.
- Re-run the script safely; it is idempotent and skips existing hosted URLs
  unless --force is supplied.

Backfill optimized media variants
---------------------------------

Generate `thumb/card/detail` variants (and optional crop variants) for existing `core.media_assets`:

```
PYTHONPATH=. python scripts/media/backfill_media_asset_variants.py --batch-size 50 --with-crops
```

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
