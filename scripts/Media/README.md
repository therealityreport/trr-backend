Media Mirroring (S3)
====================

This directory contains scripts to mirror media assets (cast photos) to S3 and
store hosted URLs back into Supabase.

Required environment variables
------------------------------

- AWS_REGION
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_S3_BUCKET
- AWS_S3_PREFIX (example: dev, prod)
- AWS_CDN_BASE_URL (example: https://d123.cloudfront.net)
- TRR_MEDIA_MIRROR_TO_S3 (optional, feature flag for future ingestion hooks)

Example usage
-------------

Mirror Fandom cast photos for a single IMDb person:

```
PYTHONPATH=. python scripts/mirror_cast_photos_to_s3.py \
  --source fandom \
  --imdb-person-id nm11883948 \
  --limit 50
```

Mirror all pending fandom images (default):

```
PYTHONPATH=. python scripts/mirror_cast_photos_to_s3.py --source fandom --limit 200
```

Dry run (no writes):

```
PYTHONPATH=. python scripts/mirror_cast_photos_to_s3.py --source fandom --limit 50 --dry-run
```

Troubleshooting
---------------

- Ensure the S3 bucket and CDN base URL are correct.
- If images return 403/404 from Fandom, verify the source_page_url is populated
  so the Referer header can be set on download.
- Re-run the script safely; it is idempotent and skips existing hosted URLs
  unless --force is supplied.
