# TRR Backend Scripts (Supabase Pipeline)

> **Legacy notice:** The old Google Sheets pipeline is preserved in `docs/legacy/google_sheets_pipeline.md`.

## Overview

This directory contains the **Supabase-first** ingestion, enrichment, and sync scripts used by TRR. Scripts read from external APIs and write to Supabase (`core.*` schema). Most entrypoints assume `PYTHONPATH=.` and load `.env` from the repo root.

### Layout

- `scripts/import/` — list ingestion and import jobs
- `scripts/sync/` — core sync pipeline (shows, seasons, episodes, people, cast)
- `scripts/backfill/` — backfill and migration helpers
- `scripts/media/` — media mirroring and cleanup
- `scripts/verify/` — parity and validation checks
- `scripts/enrich/` — targeted enrichment utilities
- `scripts/legacy/` — legacy Google Sheets utilities (historical reference)

Root-level script names remain as thin wrappers for backward compatibility with older commands.

### Legacy Utilities

Legacy Google Sheets tooling is preserved in `scripts/legacy/` for reference only. For example:

- `scripts/legacy/test_connection.py` — Google Sheets connection smoke test (legacy)

## Prerequisites

- Python 3.11+
- `.env` with at least:
  - `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`
  - `TMDB_API_KEY` (for TMDb calls)
  - `IMDB_API_KEY` (for IMDb calls)
- Optional:
  - `SUPABASE_DB_URL` / `TRR_DB_URL` for SQL helpers
  - AWS creds for S3 mirroring

## Core Workflows

### 1) Import shows from lists

```bash
PYTHONPATH=. python scripts/import/import_shows_from_lists.py \
  --imdb-list https://www.imdb.com/list/your_list_id/ \
  --tmdb-list 123456
```

Convenience runner:

```bash
PYTHONPATH=. python scripts/import/run_show_import_job.py --imdb-list ... --tmdb-list ...
```

### 2) Enrich shows (TMDb details + entities + watch providers)

```bash
PYTHONPATH=. python scripts/sync/resolve_tmdb_ids_via_find.py --all --verbose
PYTHONPATH=. python scripts/backfill/backfill_tmdb_show_details.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_tmdb_show_entities.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_tmdb_watch_providers.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_networks_streaming_links.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_show_logos.py --all --verbose
```

`sync_networks_streaming_links.py` notes:
- Processes only names currently used by the full shows inventory:
  - networks from `core.shows.networks`
  - streaming providers from `core.show_watch_providers` (`US`, `flatrate|ads`) plus fallback names from `core.shows.streaming_providers`.
  - production companies from `core.shows.tmdb_production_company_ids` with fallback names from `core.shows.tmdb_meta.production_companies`.
- Supports overrides via `admin.network_streaming_overrides` and tracks per-entity completion in:
  - `admin.network_streaming_completion`
  - `admin.network_streaming_completion_attempts`
- Mirrors and persists all discovered logo candidates (deterministic per-source caps + URL/SHA dedupe) in:
  - `admin.network_streaming_logo_assets`
- External discovery is cache-first:
  - Normal runs reuse persisted `source_url` candidates from `admin.network_streaming_logo_assets` and do not re-query Brandfetch/Logopedia/IMDb for entities that already have cached source URLs.
  - Use `--force` only when you intentionally want to refresh external discovery (this will consume external API credits again).
- Discovery is persisted per entity/source in `admin.network_streaming_discovery_state`:
  - Once a source is attempted, it is discovery-locked and won’t be queried again on normal runs.
  - Use `--refresh-external-sources` to explicitly re-open external discovery for that run.
- SVG logo sources are rasterized to PNG during mirror (via `cairosvg`) so vector-only sources can still be persisted into hosted PNG assets and used for monochrome variant generation.
- Runtime preflight:
  - Script prints `svg_rasterizer_available=true|false`.
  - If SVG logo candidates are encountered while rasterizer support is unavailable, sync fails fast with an explicit error.
- Keeps canonical logo serving fields on `core.networks` / `core.watch_providers` unchanged for primary color + black/white variants:
  - `hosted_logo_*`
  - `hosted_logo_black_*`
  - `hosted_logo_white_*`
  - same parity for `core.production_companies`.
- Use `--unresolved-only` to re-run just unresolved entities from completion state.
- Resumable execution controls:
  - `--batch-size` controls how often run progress is persisted.
  - `--max-runtime-sec` gracefully stops long runs with status `stopped`.
  - `--resume-run-id <run_id>` resumes from stored cursor in `admin.network_streaming_sync_runs`.
  - `--start-after <entity_type:entity_key>` starts after an explicit cursor.
- Prints both machine-readable counters and unresolved logo rows:
  - `completion_total=<count>`
  - `completion_resolved=<count>`
  - `completion_unresolved=<count>`
  - `completion_percent=<0-100>`
  - `logo_assets_discovered=<count>`
  - `logo_assets_mirrored=<count>`
  - `logo_assets_skipped=<count>`
  - `logo_assets_failed=<count>`
  - `unresolved_logos=<count>`
  - `unresolved_logo={\"type\":\"network|streaming|production\",\"id\":\"...\",\"name\":\"...\",\"reason\":\"...\"}`

`sync_show_logos.py` notes:
- Harvests show-logo candidates from show homepage (`core.shows.tmdb_meta.homepage`) and Wikipedia sitelinks resolved via `core.shows.wikidata_id`.
- Imports deduped logo assets into `core.media_assets` + `core.media_links` (`entity_type='show'`, `kind='logo'`).
- Machine-readable counters:
  - `show_logos_discovered=<count>`
  - `show_logos_imported=<count>`
  - `show_logos_skipped=<count>`
  - `show_logo_failures=<count>`

Or run the composite wrapper:

```bash
PYTHONPATH=. python scripts/sync/sync_shows_all.py --all --verbose
```

### 3) Seasons & Episodes

```bash
PYTHONPATH=. python scripts/sync/sync_seasons_episodes.py --all --verbose
```

### 4) People & Cast

```bash
PYTHONPATH=. python scripts/sync/sync_people.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_show_cast.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_episode_appearances.py --all --verbose
```

### 5) Media (images & photos)

```bash
PYTHONPATH=. python scripts/sync/sync_show_images.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_season_episode_images.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_cast_photos.py --all --verbose
```

### 6) Single-show complete sync

```bash
PYTHONPATH=. python scripts/sync/sync_show_complete.py --imdb-id tt1234567 --verbose
```

### 7) Download scraped images locally (no DB import)

```bash
PYTHONPATH=. python scripts/import/download_scraped_images_local.py \
  --url "https://deadline.com/..." \
  --output-dir "~/Downloads/Bachelorette"
```

Wrapper alias:

```bash
PYTHONPATH=. python scripts/download_scraped_images_local.py --url "https://deadline.com/..."
```

## Utilities & Validation

- `scripts/verify/verify_credits_parity.py` — compare legacy vs V2 credits views
- `scripts/verify/verify_media_unification.py` — verify media tables/views parity
- `scripts/verify/validate_supabase_timeouts.py` — validate Supabase client timeout settings
- `scripts/media/cleanup_expired_media_uploads.py` — cleanup stale media uploads
- `scripts/dev/doctor.py` — environment diagnostics (`make doctor`)

## Media Mirroring (S3)

See `scripts/media/README.md`:

- `scripts/media/mirror_cast_photos_to_s3.py`
- `scripts/media/mirror_media_assets_to_s3.py`
- `scripts/media/rebuild_hosted_urls.py`

Bravo video thumbnail backfill:

```bash
PYTHONPATH=. python scripts/backfill/backfill_bravo_video_thumbnails.py
```

Useful options:
- `--show-id <uuid>` to target one show
- `--force` to remirror all Bravo video thumbnails
- `--dry-run` to inspect pending work without writing

## Artifacts Location

Runtime artifacts should live **outside** the repo root in a dedicated directory, for example:

```
../artifacts/trr-backend/
  logs/
  debug_html/
  out/
  .cache/
```

Create symlinks in the repo root (`logs`, `debug_html`, `out`, `.cache`) to point at these external directories.

## Pipeline Orchestrator (Optional)

```bash
python -m trr_backend.cli pipeline run --all --verbose
python -m trr_backend.cli pipeline list
```
