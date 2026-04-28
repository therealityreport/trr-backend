# Hot-Path Query Plan Harness

This directory contains repeatable EXPLAIN scaffolding for Supabase connection-capacity work. It is evidence-only: do not add indexes from this directory, and do not treat a candidate index as approved until the route/query evidence and RLS/grants review are recorded.

## Run

Run from `TRR-Backend` with an explicit database URL and route-specific placeholder values:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
PGAPPNAME=trr-hot-path-explain \
psql "$TRR_DB_URL" \
  -v explain_analyze=false \
  -v show_id=00000000-0000-0000-0000-000000000000 \
  -v season_id=00000000-0000-0000-0000-000000000000 \
  -v community_id=00000000-0000-0000-0000-000000000000 \
  -v account_platform=instagram \
  -v account_handle=bravotv \
  -v survey_slug=example-survey \
  -v statement_timeout=8s \
  -f scripts/db/hot_path_explain/hot_path_explain.sql \
  -o /tmp/trr-hot-path-explain.txt
```

Use `explain_analyze=true` only for bounded dev/staging reads after checking the route owner, expected row counts, and timeout. The default is plain `EXPLAIN`, which plans but does not execute the SELECT.

## Parameter Conventions

All placeholders are psql variables. The SQL file supplies conservative defaults, but real evidence must use route-realistic values.

| Variable | Purpose |
| --- | --- |
| `show_id` | TRR `core.shows.id` for social landing, brand/media, and admin show reads |
| `season_id` | TRR `core.seasons.id` for season/week analytics and reddit window reads |
| `community_id` | `admin.reddit_communities.id` for reddit source/window reads |
| `reddit_post_id` | `social.reddit_posts.reddit_post_id` for detail-path follow-up plans |
| `reddit_period_key` | Canonical reddit period/window key, for example `episode-1` |
| `account_platform` | Social account platform, for example `instagram`, `tiktok`, `youtube` |
| `account_platforms_csv` | Comma-separated platforms for landing SocialBlade reads |
| `account_handle` | Social account handle without requiring an `@` prefix |
| `source_scope` | Social source scope, usually `bravo`, `creator`, or `community` |
| `survey_slug` | `firebase_surveys.surveys.slug` and legacy survey-show key placeholder |
| `survey_id` | `firebase_surveys.surveys.id` |
| `survey_run_id` | `firebase_surveys.survey_runs.id` |
| `brand_target_type` | Brand/logo type such as `franchise`, `publication`, `social`, or `other` |
| `brand_q` | Brand/logo search string |
| `media_entity_type` | `core.media_links.entity_type`, usually `show`, `season`, or `person` |
| `media_kind` | Media/gallery kind, for example `poster`, `backdrop`, or `gallery` |
| `search_q` | Admin show/global search string |
| `week_start` / `week_end` | UTC ISO timestamps for bounded week analytics |
| `safe_limit` / `safe_offset` | Result bounds used by the representative hot-path reads |
| `statement_timeout` / `lock_timeout` | Safety limits applied inside the read-only transaction |

## Evidence Rule

For every proposed index, capture:

- Route and UI surface.
- SQL label from `hot_path_explain.sql`.
- Parameter values or a redacted parameter summary.
- Plain EXPLAIN output, and EXPLAIN ANALYZE only when safe.
- Observed problem: scan, sort, misestimate, repeated nested loop, or high buffer read.
- Candidate index with why it matches that route/query.
- RLS/grants impact review for every touched schema/table before any migration.

