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

## Run the job

Recommended invocation (Stage 1 + Stage 2):

- `python -m scripts.import_shows_from_lists --imdb-list "<IMDB_LIST_URL>" --enrich-show-metadata --region US`

Optional additional sources:

- `--tmdb-list "<TMDB_LIST_ID_OR_URL>"`

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

- `python scripts/run_show_import_job.py --imdb-list "<IMDB_LIST_URL>"`
