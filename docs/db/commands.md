# Database Table Commands

This is a living list of the commands we use to build/update/populate database tables.
Update this file as scripts or flows change.

## Setup (local Supabase)

```bash
# Start local Supabase (preferred for schema/docs + DB tests)
supabase start --exclude gotrue,realtime,storage-api,imgproxy,kong,mailpit,postgrest,postgres-meta,studio,edge-runtime,logflare,vector,supavisor

# Reset local DB to migrations + seed
supabase db reset --yes

# Note: local auth/storage uses ES256 signing keys in `supabase/signing_key.json` (local-only).
# If `supabase db reset` returns a 502 right after restart, it is a health-check race; run
# `supabase start` once and retry if needed.

# Stop local Supabase
supabase stop --no-backup
```

## Core tables (build/update/populate)

### core.shows

Import shows from lists (IMDb + TMDb):

```bash
python3 scripts/import/import_shows_from_lists.py \
  --imdb-list "<IMDB_LIST_URL>" \
  --tmdb-list "<TMDB_LIST_ID_OR_URL>" \
  --enrich-show-metadata \
  --region US
```

One-stop wrapper (list import + enrichment):

```bash
python3 scripts/import/run_show_import_job.py \
  --imdb-list "<IMDB_LIST_URL>" \
  --tmdb-list "<TMDB_LIST_ID_OR_URL>" \
  --region US
```

Default list sources (no flags required):

```bash
# Set once in .env:
# TMDB_LIST_ID=8301263
# IMDB_LIST_URL=https://www.imdb.com/list/ls4106677119/
python3 scripts/import/run_show_import_job.py
```

Sync/enrich existing show rows (filters available):

```bash
python3 scripts/sync/sync_shows.py --all
# or
python3 scripts/sync/sync_shows.py --show-id <SHOW_UUID>
```

Notes:
- `import_shows_from_lists.py` and `run_show_import_job.py` also populate `core.show_images` and `core.season_images` when TMDb image/season options are enabled.
- `sync_shows.py` performs enrichment (TMDb, IMDb meta, IMDb media images) on existing rows.

### core.seasons

```bash
python3 scripts/sync/sync_seasons.py --all
# or filter by show
python3 scripts/sync/sync_seasons.py --imdb-series-id <tt1234567>
```

### core.episodes

```bash
python3 scripts/sync/sync_episodes.py --all
# or filter by show
python3 scripts/sync/sync_episodes.py --tmdb-show-id <TMDB_ID>
```

### core.people

```bash
python3 scripts/sync/sync_people.py --all
# or filter by show
python3 scripts/sync/sync_people.py --imdb-series-id <tt1234567>
```

### core.show_cast

```bash
python3 scripts/sync/sync_show_cast.py --all
# or filter by show
python3 scripts/sync/sync_show_cast.py --show-id <SHOW_UUID>
```

### core.episode_appearances

Sync aggregated appearances:

```bash
python3 scripts/sync/sync_episode_appearances.py --all
# or filter by show
python3 scripts/sync/sync_episode_appearances.py --imdb-series-id <tt1234567>
```

Build episode appearances from IMDb cast data (single show):

```bash
python3 scripts/import/import_imdb_cast_episode_appearances.py --imdb-series-id <tt1234567>
```

### core.show_images

Images are written as one row per image by:

```bash
# During list import (TMDb images)
python3 scripts/import/import_shows_from_lists.py --tmdb-list "<TMDB_LIST_ID_OR_URL>" --tmdb-fetch-images

# Or via the wrapper
python3 scripts/import/run_show_import_job.py --tmdb-list "<TMDB_LIST_ID_OR_URL>" --tmdb-fetch-images

# Or via enrichment on existing shows (IMDb media images)
python3 scripts/sync/sync_shows.py --all
```

### core.season_images

Season images are written by the list import/enrichment flow when TMDb seasons are fetched:

```bash
python3 scripts/import/import_shows_from_lists.py --tmdb-list "<TMDB_LIST_ID_OR_URL>" --tmdb-fetch-seasons
```

## Multi-table convenience

Run the standard sync pipeline in order:

```bash
python3 scripts/sync/sync_all_tables.py --all
# or only certain tables
python3 scripts/sync/sync_all_tables.py --tables shows,episodes,people --all
```

## Pipeline Orchestrator (Resumable)

Run the staged orchestrator (tracks runs in `pipeline.*` tables):

```bash
python -m trr_backend.cli pipeline run --all --verbose
python -m trr_backend.cli pipeline list
python -m trr_backend.cli pipeline status <run-id>
```

## Fandom enrichment (RHOSLC example)

```bash
python3 scripts/rhoslc_fandom_enrichment.py \
  --episode-appearances <PATH_TO_EPISODE_APPEARANCES_JSON> \
  --imdb-show-id tt11363282 \
  --limit 5
```

## Schema docs

```bash
make schema-docs
make schema-docs-check
```

Notes:
- `make schema-docs-check` auto-resolves the DB URL from `supabase status` when Supabase is running.
- If Supabase is stopped, it will prompt you to start Supabase or set `SUPABASE_DB_URL`.

## CI-style local run (optional)

```bash
make ci-local
```

Notes:
- This target brings Supabase up, resets the DB, runs pytest, checks schema docs, then stops Supabase.

## Media Asset Mirroring (Phase 3)

Mirror media assets from external sources (TMDb, IMDb) to S3 for reliable serving.

### Run mirroring

```bash
# Mirror pending assets (default behavior)
python scripts/media/mirror_media_assets_to_s3.py --status pending --limit 100 --verbose

# Mirror failed assets (retry with exponential backoff)
python scripts/media/mirror_media_assets_to_s3.py --status failed --limit 50 --verbose

# Mirror all pending/failed with concurrency
python scripts/media/mirror_media_assets_to_s3.py --status all --limit 500 --concurrency 10

# Dry run (validate domains, no actual uploads)
python scripts/media/mirror_media_assets_to_s3.py --dry-run --limit 10 --verbose

# Filter by source
python scripts/media/mirror_media_assets_to_s3.py --source tmdb --status pending --limit 100
```

### Monitor ingest progress

```sql
-- Summary by source and status
SELECT * FROM core.v_media_ingest_summary;

-- Find stuck/failed assets
SELECT id, source, source_url, ingest_status, ingest_last_error, ingest_retry_count
FROM core.media_assets
WHERE ingest_status IN ('failed', 'in_progress')
ORDER BY ingest_failed_at DESC
LIMIT 20;

-- Pending queue size by source
SELECT source, count(*) as pending_count
FROM core.media_assets
WHERE ingest_status = 'pending'
GROUP BY source;
```

### Required environment variables

```bash
# S3 configuration (required for actual mirroring)
OBJECT_STORAGE_BUCKET=your-media-bucket
OBJECT_STORAGE_PUBLIC_BASE_URL=https://cdn.example.com
OBJECT_STORAGE_REGION=us-east-1

# Domain allowlist (optional, has sensible defaults)
MEDIA_MIRROR_ALLOWED_DOMAINS=image.tmdb.org,m.media-amazon.com,static.wikia.nocookie.net
```

### Rollout notes

- Safe to deploy before running the mirror worker; `hosted_url` simply stays null
- Served views use `coalesce(hosted_url, source_url)` so external URLs continue working
- The mirror worker is idempotent; re-running on already-hosted assets is a no-op
- Failed assets are retried with exponential backoff (1h, 2h, 4h, 8h, ...)
