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
   # edit .env with runtime DB/auth + API keys
   ```

   Minimum required for most scripts:
   - `TRR_DB_URL`
   - `SUPABASE_JWT_SECRET`
   - `TRR_INTERNAL_ADMIN_SHARED_SECRET`
   - `TMDB_API_KEY`
   - `TVDB_API_KEY`
   - `IMDB_API_KEY`

   Optional hosted-asset support:
   - `OBJECT_STORAGE_PROVIDER`
   - `OBJECT_STORAGE_BUCKET`
   - `OBJECT_STORAGE_REGION`
   - `OBJECT_STORAGE_ENDPOINT_URL`
   - `OBJECT_STORAGE_ACCESS_KEY_ID`
   - `OBJECT_STORAGE_SECRET_ACCESS_KEY`
   - `OBJECT_STORAGE_PUBLIC_BASE_URL`

3. **Verify environment**
   ```bash
   make doctor
   ```

## Preferred Validation Path

For migration and schema verification, prefer an isolated remote Supabase branch or other disposable database target over a local Docker-backed replay.

1. Create or select an isolated branch/disposable database target.
2. Export `TRR_DB_URL` to that isolated target.
3. Push migrations there:
   ```bash
   supabase db push --db-url "$TRR_DB_URL" --include-all
   ```
4. Run the schema-doc verification against that same isolated target:
   ```bash
   make schema-docs-check
   ```

Safety rules:
- Never run destructive replay or reset verification against production or other long-lived shared persistent databases.
- Use `make schema-docs-reset-check` only as an explicit local Docker fallback when you intentionally need a fully local replay.

## make dev Runtime Reconcile

The workspace `make dev` path now runs a startup reconcile phase before it launches `TRR-Backend` and `TRR-APP`.

- Hosted Supabase is boot-critical. Startup may auto-apply pending migrations only when the pending set is a contiguous local suffix, every version is listed in `scripts/dev/runtime_reconcile_migration_allowlist.txt`, and the count is within `WORKSPACE_RUNTIME_DB_MAX_AUTO_APPLY`.
- Startup will never auto-run `supabase migration repair`, rewrite migration history, or trigger schema-doc generation/checks.
- Modal is boot-critical in the default profile. Startup may auto-apply named secrets and redeploy `trr_backend.modal_jobs` when readiness fails or the tracked runtime fingerprint changes.
- Render and Decodo checks are verify-only and surface as advisories in `make status`.

If startup blocks on remote/local migration history drift, use `docs/runbooks/supabase_migration_history_repair.md` before rerunning `make dev`.

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
