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
  - `TVDB_API_KEY` (for TVDB calls where configured)
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
  - Use `--refresh-external-sources` only when you intentionally want to refresh external discovery (this will consume external API credits again).
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
  - `--entity-type <network|streaming|production>` limits processing to one entity type.
  - `--entity-key <normalized key>` (repeatable) targets specific entities only.
- External lookup timeouts/retries:
  - Brandfetch uses `BRANDFETCH_TIMEOUT_SEC` (read timeout), `BRANDFETCH_RETRY_ATTEMPTS`, and `BRANDFETCH_RETRY_BACKOFF_MS`.
  - Logopedia uses `LOGOPEDIA_TIMEOUT_SEC`, `LOGOPEDIA_RETRY_ATTEMPTS`, and `LOGOPEDIA_RETRY_BACKOFF_MS`.
  - Retry budget only applies to transient failures (`429`, `5xx`, connect/read timeout).
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

### 8) Bravo person-source backfill + diagnostics

Run cleanup + rediscovery for person-source links (IMDb/TMDb/Wikipedia/Wikidata/Fandom/Bravo profile):

```bash
python scripts/shows/backfill_bravo_person_source_links.py --json-summary /tmp/person_sources_dryrun.json
python scripts/shows/backfill_bravo_person_source_links.py --apply --json-summary /tmp/person_sources_apply.json
```

Target specific shows:

```bash
python scripts/shows/backfill_bravo_person_source_links.py \
  --show-id <show-uuid-1> \
  --show-id <show-uuid-2> \
  --apply
```

Threshold/alert mode for automation:

```bash
python scripts/shows/backfill_bravo_person_source_links.py \
  --json-summary /tmp/person_sources_daily.json \
  --warn-fetch-errors 500 \
  --fail-fetch-errors 1500 \
  --warn-pending-person-sources 0 \
  --fail-pending-person-sources 0
```

### 9) Historical social hosted-media cleanup

Use the canonical hosted-URL normalizer first when rows already have hosted assets but still point at a legacy public host:

```bash
PYTHONPATH=. python scripts/socials/repair_social_hosted_urls.py \
  --platforms instagram,tiktok,youtube,twitter,facebook,threads \
  --show-id <show-uuid> \
  --season-number 6 \
  --limit-per-platform 5000 \
  --dry-run
```

Then use the media mirror backfill script for older posts that still need thumbnail/media/avatar repair, remirroring, or targeted cleanup:

```bash
PYTHONPATH=. python scripts/socials/backfill_social_media_mirror_jobs.py \
  --all-history \
  --platforms twitter,tiktok,youtube,facebook,threads,instagram \
  --source-scope bravo \
  --limit-per-platform 5000 \
  --dry-run
```

If historical Threads `media_mirror` jobs failed before Threads support landed, retire those obsolete failures first so queue snapshots stop counting them as current actionable errors:

```bash
PYTHONPATH=. python scripts/socials/retire_stale_threads_media_mirror_failures.py \
  --season-id <season-uuid> \
  --dry-run
```

Apply the retirement once the dry-run count looks correct:

```bash
PYTHONPATH=. python scripts/socials/retire_stale_threads_media_mirror_failures.py \
  --season-id <season-uuid> \
  --apply
```

Target specific shows, seasons, rows, repair classes, or cleanup modes when you want a narrower pass:

```bash
PYTHONPATH=. python scripts/socials/backfill_social_media_mirror_jobs.py \
  --all-history \
  --platforms twitter \
  --show-id <show-uuid> \
  --season-id <season-uuid> \
  --season-number 6 \
  --post-id <post-uuid> \
  --source-id <platform-source-id> \
  --repair-reasons twitter_video_thumbnail,legacy_hosted_url \
  --mirror-only \
  --source-scope bravo
```

Supported repair reasons:
- `legacy_hosted_url`
- `hosted_content`
- `missing_hosted_thumbnail`
- `missing_hosted_media`
- `missing_source_avatar`
- `missing_hosted_avatar`
- `mirror_retry`
- `non_video_hosted_media`
- `source_quality`
- `stale_media_metadata`
- `twitter_video_thumbnail`

Compatibility note:
- `legacy_host` is still accepted as a CLI alias for `legacy_hosted_url`.

Notes:
- `--dry-run` reports eligible historical cleanup rows without enqueueing mirror jobs.
- Without `--all-history`, the script preserves the default recent lookback window (`--weeks`, default `8`).
- `--hosted-html-only` remains available as a narrow compatibility filter for page-wrapper cleanup.
- Recommended Threads repair sequence when stale unsupported failures exist:
  1. retire stale `mirror_platform_not_supported:threads` failures with `retire_stale_threads_media_mirror_failures.py`
  2. rerun `backfill_social_media_mirror_jobs.py --platforms threads ...`
  3. drain `scripts.socials.worker --stage media_mirror --platform threads`
  4. rerun the Threads dry-run until `eligible` reaches zero or only current reproducible failures remain

### 10) Historical show/season/episode/cast image cleanup

Backfill unified media rows for legacy image tables and optionally generate the variant URLs used by gallery, thumbnail, and lightbox surfaces:

```bash
PYTHONPATH=. python scripts/backfill/backfill_media_assets.py \
  --entity-type all \
  --with-variants \
  --with-crops \
  --verbose
```

Target a narrower historical slice:

```bash
PYTHONPATH=. python scripts/backfill/backfill_media_assets.py --entity-type season --with-variants
PYTHONPATH=. python scripts/backfill/backfill_media_assets.py --entity-type episode --with-variants
PYTHONPATH=. python scripts/backfill/backfill_media_assets.py --entity-type cast --with-variants
```

Notes:
- `--entity-type all` now covers `show`, `season`, `episode`, `person`, and `cast`.
- Legacy table aliases now include `show_images`, `season_images`, `episode_images`, `person_images`, and `cast_photos`.
- The backfill now normalizes hosted URLs from `hosted_key` so imported `media_assets` rows use the canonical object-storage public base instead of preserving stale legacy hosts.

Diagnostics for missing approved IMDb/TMDb links (example: Andy Cohen):

```bash
python scripts/shows/backfill_bravo_person_source_links.py \
  --diagnose-missing-person-sources \
  --diagnose-name "Andy Cohen" \
  --diagnostics-json /tmp/person_sources_diagnostics_andy.json
```

Script exit codes:
- `0` success (no failed shows and no fail-threshold breach)
- `1` one or more failed shows
- `2` threshold failure (`--fail-*` options)

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

Canonical hosted-URL rebuild for stale gallery hosts:

```bash
PYTHONPATH=. python scripts/media/rebuild_hosted_urls.py --table all --dry-run
PYTHONPATH=. python scripts/media/rebuild_hosted_urls.py --table all
```

Notes:
- Rewrites stale legacy hosted URLs in `media_assets`, `media_asset_variants`, `cast_photos`, and legacy image tables onto the current object-storage public base.
- Also rewrites embedded metadata URLs, including `media-variants`, `cast-photo-variants`, and `face-crops` URLs consumed by app gallery/lightbox surfaces.
- Follow with `scripts/media/repair_gallery_hosts.py --apply` only for rows that still fail reachability after the canonical rebuild.

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
