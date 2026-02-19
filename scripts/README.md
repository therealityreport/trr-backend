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
```

`sync_networks_streaming_links.py` notes:
- Processes only names currently used by the full shows inventory:
  - networks from `core.shows.networks`
  - streaming providers from `core.show_watch_providers` (`US`, `flatrate|ads`) plus fallback names from `core.shows.streaming_providers`.
- Mirrors missing base logos and generates black/white transparent variants (`hosted_logo_black_*`, `hosted_logo_white_*`).
- Prints both machine-readable counters and unresolved logo rows:
  - `unresolved_logos=<count>`
  - `unresolved_logo={\"type\":\"network|streaming\",\"id\":\"...\",\"name\":\"...\",\"reason\":\"...\"}`

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
