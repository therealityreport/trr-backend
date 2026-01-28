# Architecture

## What This Repo Is Today

This repository is a Supabase-first data pipeline and API. It pulls reality TV show/cast metadata from external sources (TMDb, IMDb, Fandom, Famous Birthdays, Gemini) and persists normalized data into Supabase (`core.*` schema). The FastAPI app serves data from Supabase, and the sync scripts in `scripts/` handle ingestion/enrichment.

The legacy Google Sheets pipeline is preserved for reference in `docs/legacy/architecture_google_sheets.md`.

## FastAPI App (Supabase)

The repo also contains a FastAPI app under `api/` that serves TRR data from Supabase and supports real-time WebSocket updates.

## Shared Library Code

Code that should be reused by both the API and the pipeline should live under `trr_backend/` (not inside `api/` or `scripts/`).

External metadata clients live under `trr_backend/integrations/` (see `docs/architecture/integrations.md`).

## Pipeline Stages (Supabase)

- **List ingestion:** `scripts/import/import_shows_from_lists.py` (or `scripts/import/run_show_import_job.py`)
- **Show enrichment:** `scripts/sync/resolve_tmdb_ids_via_find.py`, `scripts/backfill/backfill_tmdb_show_details.py`, `scripts/sync/sync_tmdb_show_entities.py`, `scripts/sync/sync_tmdb_watch_providers.py`
- **Shows/Seasons/Episodes:** `scripts/sync/sync_shows.py`, `scripts/sync/sync_seasons_episodes.py`
- **People/Cast:** `scripts/sync/sync_people.py`, `scripts/sync/sync_show_cast.py`, `scripts/sync/sync_episode_appearances.py`
- **Media:** `scripts/sync/sync_show_images.py`, `scripts/sync/sync_season_episode_images.py`, `scripts/sync/sync_cast_photos.py`
- **Single-show:** `scripts/sync/sync_show_complete.py` (metadata, seasons, episodes, cast, and photos)

## Pipeline Orchestrator

The `trr_backend/pipeline/` module provides a resumable, staged orchestrator that records runs in the `pipeline` schema.
Use the CLI entrypoint in `trr_backend/cli/`:

```bash
python -m trr_backend.cli pipeline run --all --verbose
python -m trr_backend.cli pipeline list
```

See `docs/architecture/pipeline.md` for stages, resume logic, manifests, and schema details.

## Source vs. Generated Artifacts

These paths are expected to exist locally during runs, but are excluded from version control:

- `.env` (local secrets/config)
- `keys/` (service account JSONs, Firebase credentials)
- `../artifacts/trr-backend/` (runtime logs/results, caches, debug HTML, and other outputs)

Recommended layout:

- `../artifacts/trr-backend/logs/`
- `../artifacts/trr-backend/debug_html/`
- `../artifacts/trr-backend/out/`
- `../artifacts/trr-backend/.cache/`

For convenience, create symlinks in the repo root (`logs`, `debug_html`, `out`, `.cache`) that point to these external directories.

## Legacy Note

The Google Sheets pipeline and related docs are archived under `docs/legacy/`. Supabase is now the system of record for production workflows.

---

## TMDb Show Enrichment Pipeline

This section documents the Supabase-first workflow for show metadata enrichment.

### Current State

**Tables:**
- `core.shows` — Canonical show table with typed TMDb/IMDb columns
- `core.networks` — TV network dimension table
- `core.production_companies` — Production company dimension table
- `core.watch_providers` — Streaming/rental provider dimension table
- `core.show_watch_providers` — Junction table (show × region × offer_type × provider)

**Scripts:**
- `scripts/sync/resolve_tmdb_ids_via_find.py` — Resolve TMDb IDs via `/find/{imdb_id}`
- `scripts/backfill/backfill_tmdb_show_details.py` — Enrich shows with TMDb `/tv/{id}` data
- `scripts/sync/sync_tmdb_show_entities.py` — Sync networks/production_companies + S3 logos
- `scripts/sync/sync_tmdb_watch_providers.py` — Sync watch_providers + show_watch_providers + S3 logos
- `scripts/sync/sync_shows_all.py` — Wrapper that runs all of the above in sequence

**Migrations:** `0047`, `0048`, `0049`, `0050`, `0051` (see `supabase/migrations/`)

### Data Model Overview

The `core.shows` table stores both TMDb and IMDb metadata in typed columns:

- **IDs:** `tmdb_id`, `imdb_id`
- **TMDb typed columns:** `tmdb_name`, `tmdb_status`, `tmdb_type`, `tmdb_first_air_date`, `tmdb_last_air_date`, `tmdb_vote_average`, `tmdb_vote_count`, `tmdb_popularity`
- **IMDb typed columns:** `imdb_title`, `imdb_content_rating`, `imdb_rating_value`, `imdb_rating_count`
- **JSONB metadata:** `tmdb_meta`, `imdb_meta` (raw API responses for fields not yet typed)
- **Entity ID arrays:** `tmdb_network_ids[]`, `tmdb_production_company_ids[]`
- **Resolution flags:** `needs_tmdb_resolution`, `needs_imdb_resolution`

### Entity Normalization

Entity data is stored in dimension tables rather than embedded in shows:

| Table | Primary Key | Key Columns |
|-------|-------------|-------------|
| `networks` | `id` (int) | `name`, `origin_country`, `hosted_logo_*` |
| `production_companies` | `id` (int) | `name`, `origin_country`, `hosted_logo_*` |
| `watch_providers` | `provider_id` (int) | `provider_name`, `display_priority`, `hosted_logo_*` |
| `show_watch_providers` | composite | `show_id`, `region`, `offer_type`, `provider_id`, `link` |

All dimension tables include `hosted_logo_*` fields for S3-mirrored logos.

### Data Flow

```
1. resolve_tmdb_ids_via_find.py
   TMDb /find/{imdb_id} → core.shows.tmdb_id

2. backfill_tmdb_show_details.py
   TMDb /tv/{id} → typed columns + tmdb_meta

3. sync_tmdb_show_entities.py
   TMDb /tv/{id} → networks + production_companies + S3 logos

4. sync_tmdb_watch_providers.py
   TMDb /tv/{id}/watch/providers → watch_providers + show_watch_providers + S3 logos

5. API serves ShowDetail with resolved entities
```

Alternatively, run `sync_shows_all.py` to execute steps 2-4 in sequence.

### S3 Storage Layout

Logo assets are mirrored to S3 using content-addressed keys:

- **Key builder:** `build_logo_s3_key()` in `trr_backend/media/s3_mirror.py`
- **Pattern:** `images/logos/{kind}/{entity_id}/{sha256}.{ext}`
  - `kind` = `networks`, `production_companies`, or `watch_providers`
- **Content-addressed:** SHA256 computed via `_sha256_bytes()` (s3_mirror.py:263)
- **Deduplication:** Skip upload if `hosted_logo_sha256` matches computed hash
- **Cache headers:** `CacheControl="public, max-age=31536000, immutable"` (s3_mirror.py:343)
- **Prune:** NOT implemented for logos (only for cast_photos, show_images, season_images)

### API Contract

`GET /shows/{show_id}` returns `ShowDetail` (defined in `api/routers/shows.py`):

```python
class ShowDetail(Show):
    tmdb_networks: list[TmdbNetwork] | None
    tmdb_production_companies: list[TmdbProductionCompany] | None
    watch_providers: list[WatchProviderGroup] | None
```

`WatchProviderGroup` contains `region`, `offer_type`, `link`, and `providers[]`.

Entity objects include full `hosted_logo_*` metadata for direct CDN URLs.

### Operational Guidance

- **Run order:** resolve → backfill → entities → providers (or use `sync_shows_all.py`)
- **Idempotency:** All scripts are safe to re-run; SHA256 check prevents duplicate uploads
- **Rate limiting:** Exponential backoff with jitter on 429/5xx (`trr_backend/integrations/tmdb/client.py`)
- **Regions:** ALL regions from TMDb API are stored (no filtering)

### Known Gaps

- IMDb entity sync not implemented (only TMDb sources)
- No automated prune for orphaned S3 logo objects
- No scheduled job runner (scripts run manually or via cron)

---

## Repository Structure

For detailed repository organization, module dependency graphs, and data flow diagrams, see [docs/Repository/README.md](Repository/README.md).
