# Database Table Commands

This is a living list of the commands we use to build/update/populate database tables.
Update this file as scripts or flows change.

## Setup (local Supabase)

```bash
# Start local Supabase (preferred for schema/docs + DB tests)
supabase start --exclude gotrue,realtime,storage-api,imgproxy,kong,mailpit,postgrest,postgres-meta,studio,edge-runtime,logflare,vector,supavisor

# Reset local DB to migrations + seed
supabase db reset --yes

# Stop local Supabase
supabase stop --no-backup
```

## Core tables (build/update/populate)

### core.shows

Import shows from lists (IMDb + TMDb):

```bash
python3 scripts/import_shows_from_lists.py \
  --imdb-list "<IMDB_LIST_URL>" \
  --tmdb-list "<TMDB_LIST_ID_OR_URL>" \
  --enrich-show-metadata \
  --region US
```

One-stop wrapper (list import + enrichment):

```bash
python3 scripts/run_show_import_job.py \
  --imdb-list "<IMDB_LIST_URL>" \
  --tmdb-list "<TMDB_LIST_ID_OR_URL>" \
  --region US
```

Sync/enrich existing show rows (filters available):

```bash
python3 scripts/sync_shows.py --all
# or
python3 scripts/sync_shows.py --show-id <SHOW_UUID>
```

Notes:
- `import_shows_from_lists.py` and `run_show_import_job.py` also populate `core.show_images` and `core.season_images` when TMDb image/season options are enabled.
- `sync_shows.py` performs enrichment (TMDb, IMDb meta, IMDb media images) on existing rows.

### core.seasons

```bash
python3 scripts/sync_seasons.py --all
# or filter by show
python3 scripts/sync_seasons.py --imdb-series-id <tt1234567>
```

### core.episodes

```bash
python3 scripts/sync_episodes.py --all
# or filter by show
python3 scripts/sync_episodes.py --tmdb-show-id <TMDB_ID>
```

### core.people

```bash
python3 scripts/sync_people.py --all
# or filter by show
python3 scripts/sync_people.py --imdb-series-id <tt1234567>
```

### core.show_cast

```bash
python3 scripts/sync_show_cast.py --all
# or filter by show
python3 scripts/sync_show_cast.py --show-id <SHOW_UUID>
```

### core.episode_appearances

Sync aggregated appearances:

```bash
python3 scripts/sync_episode_appearances.py --all
# or filter by show
python3 scripts/sync_episode_appearances.py --imdb-series-id <tt1234567>
```

Build episode appearances from IMDb cast data (single show):

```bash
python3 scripts/import_imdb_cast_episode_appearances.py --imdb-series-id <tt1234567>
```

### core.show_images

Images are written as one row per image by:

```bash
# During list import (TMDb images)
python3 scripts/import_shows_from_lists.py --tmdb-list "<TMDB_LIST_ID_OR_URL>" --tmdb-fetch-images

# Or via the wrapper
python3 scripts/run_show_import_job.py --tmdb-list "<TMDB_LIST_ID_OR_URL>" --tmdb-fetch-images

# Or via enrichment on existing shows (IMDb media images)
python3 scripts/sync_shows.py --all
```

### core.season_images

Season images are written by the list import/enrichment flow when TMDb seasons are fetched:

```bash
python3 scripts/import_shows_from_lists.py --tmdb-list "<TMDB_LIST_ID_OR_URL>" --tmdb-fetch-seasons
```

## Multi-table convenience

Run the standard sync pipeline in order:

```bash
python3 scripts/sync_all_tables.py --all
# or only certain tables
python3 scripts/sync_all_tables.py --tables shows,episodes,people --all
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
