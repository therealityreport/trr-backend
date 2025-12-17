# Architecture

## What This Repo Is Today

This repository is a data pipeline that pulls reality TV show/cast metadata from external sources (TMDb, IMDb, Fandom, Famous Birthdays, Gemini) and writes curated outputs into a Google Sheets workbook (e.g. `ShowInfo`, `CastInfo`, `RealiteaseInfo`, `WWHLinfo`, `FinalList`).

Most pipeline execution lives under `scripts/` and is designed to be run locally or from scheduled jobs.

## FastAPI App (Supabase)

The repo also contains a FastAPI app under `api/` that serves TRR data from Supabase and supports real-time WebSocket updates.

## Shared Library Code

Code that should be reused by both the API and the pipeline should live under `trr_backend/` (not inside `api/` or `scripts/`).

External metadata clients live under `trr_backend/integrations/` (see `docs/architecture/integrations.md`).

## Pipeline Stages

- `scripts/1-ShowInfo/`: discover/populate show metadata
- `scripts/2-CastInfo/`: extract cast per show and enrich episode/season counts
- `scripts/3-RealiteaseInfo/`: person-focused enrichment (bio fields, dedupe, backfills)
- `scripts/4-WWHLInfo/`: Watch What Happens Live episode + guest processing
- `scripts/5-FinalList/`: final curation + optional Firebase publishing helpers

For a single entrypoint, use `scripts/run_pipeline.py`.

## Source vs. Generated Artifacts

These paths are expected to exist locally during runs, but are excluded from version control:

- `.env` (local secrets/config)
- `keys/` (service account JSONs, Firebase credentials)
- `logs/` (runtime logs/results)
- `.cache/` (API caches)
- `debug_html/` and `scripts/**/debug_html/` (HTML snapshots/mirrors for debugging scrapers)

## Future Direction (Supabase)

The long-term direction is to use Supabase as the system of record (database + API) and treat Google Sheets as a lightweight admin/export surface. Environment placeholders for Supabase are included in `.env.example` to support this transition.
