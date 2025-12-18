# Show Import Job (IMDb/TMDb Lists → `core.shows`)

This runbook covers the “one-button” workflow to:

1) Ensure the `core.shows` table exists in Supabase (via migrations), then
2) Run the list importer (Stage 1) + metadata enrichment (Stage 2).

## Required environment variables

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

These scripts load `.env` at the repo root via `python-dotenv`, so it’s fine to put the variables there (or export them via your shell/direnv).

## Optional (recommended) environment variables

- `TMDB_API_KEY` (enables Stage 2 enrichment from TMDb; job still runs without it but with fewer fields)

## Apply migrations (creates/ensures `core.shows`)

Link your project (if needed), then push migrations:

- `supabase link ...`
- `supabase db push`

The `core.shows` table/indexes are ensured by `supabase/migrations/0004_core_shows.sql`.

## Ensure the Supabase API exposes `core`

The importer uses the Supabase API (PostgREST). By default, new projects may not expose non-`public` schemas.

Choose one:

- Supabase Dashboard → Settings → API → “Exposed schemas” → add `core`
- Or run `supabase config push` to sync `supabase/config.toml` to your project (review the diff; this may change auth settings too)

## Run the job

## Flow

- Stage 1: ingest list items (IMDb/TMDb) → union candidates → upsert into `core.shows`
- Stage 2: enrich `core.shows.external_ids.show_meta` using “best available” sources:
  - Prefer TMDb-derived values when a TMDb id exists (or can be resolved).
  - Fall back to IMDb title/episodes parsing only for fields TMDb can’t provide.
- Reruns: completed `show_meta` rows are skipped unless `--force-refresh` is set.

## Recommended runs

Combined run (TMDb + IMDb lists in one pass):

- `python -m scripts.import_shows_from_lists --tmdb-list "8301263" --imdb-list "<IMDB_LIST_URL>" --enrich-show-metadata --region US`

TMDb-only:

- `python -m scripts.import_shows_from_lists --tmdb-list "8301263" --enrich-show-metadata --region US`

IMDb-only:

- `python -m scripts.import_shows_from_lists --imdb-list "<IMDB_LIST_URL>" --enrich-show-metadata --region US`

Useful options:

- `--dry-run` (no DB writes; prints planned CREATE/UPDATE actions)
- `--concurrency 5` (Stage 2 parallelism)
- `--max-enrich 50` (cap Stage 2 for quick runs)
- `--force-refresh` (ignore existing `external_ids.show_meta.fetched_at`)

## Example: shared IMDb list URL

- IMDb list: `https://www.imdb.com/list/ls4106677119/?ref_=ext_shr_lnk`

Example run:

- `python -m scripts.import_shows_from_lists --imdb-list "https://www.imdb.com/list/ls4106677119/?ref_=ext_shr_lnk" --enrich-show-metadata --region US`

## Convenience runner (optional)

If you prefer a thin wrapper that validates env vars and always enables Stage 2:

- `python scripts/run_show_import_job.py --tmdb-list "8301263" --imdb-list "<IMDB_LIST_URL>" --region US`

By default this runs a single combined import; use `--two-pass` only when you need extra resilience against IMDb stalls.
