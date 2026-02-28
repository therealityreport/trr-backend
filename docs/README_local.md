# TRR Backend (Local Development)

Supabase-first data pipeline and API for reality TV show/cast metadata.

## What You Run Locally

- Sync scripts in `scripts/` (ingest/enrich data into Supabase)
- FastAPI app in `api/`
- Shared library code in `trr_backend/`

## Setup

Python 3.11+ is required.

1. **Create venv + install deps**
   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   # edit .env with Supabase + API keys
   ```

   Minimum required for most scripts:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `TMDB_API_KEY`
   - `TVDB_API_KEY`
   - `IMDB_API_KEY`

3. **Verify environment**
   ```bash
   make doctor
   ```

## Common Commands

```bash
# Import shows from lists
PYTHONPATH=. python scripts/import/import_shows_from_lists.py --imdb-list ... --tmdb-list ...

# Enrich shows
PYTHONPATH=. python scripts/sync/sync_shows_all.py --all --verbose

# Seasons + episodes
PYTHONPATH=. python scripts/sync/sync_seasons_episodes.py --all --verbose

# People + cast + photos
PYTHONPATH=. python scripts/sync/sync_people.py --all --verbose
PYTHONPATH=. python scripts/sync/sync_cast_photos.py --all --verbose
```

## Pipeline Orchestrator (Optional)

```bash
python -m trr_backend.cli pipeline run --all --verbose
python -m trr_backend.cli pipeline list
```

## Artifacts (Logs, Debug HTML, Cache)

Runtime artifacts should live **outside** the repo root. Recommended layout:

```
../artifacts/trr-backend/
  logs/
  debug_html/
  out/
  .cache/
```

Create symlinks in the repo root (`logs`, `debug_html`, `out`, `.cache`) pointing to the external directories.

## Legacy Google Sheets Pipeline

The old Google Sheets pipeline docs are preserved under `docs/legacy/`.
