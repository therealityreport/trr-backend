# Supabase database schema (TRR backend)

This repo uses Supabase Postgres as the system of record. The authoritative DDL lives in `supabase/migrations/0001_init.sql`.

## Schemas

### `core`

Canonical TV metadata: shows, seasons, episodes, and cast.

> Note (2026-02-09+): legacy `games` schema was removed in migration `0106_drop_games_schema.sql`.
> Bravodle/Realitease runtime data currently lives in TRR-APP Firebase collections, not Supabase `games.*` tables.

### `surveys`

Survey definitions, user responses, user answers, and live aggregates for near-real-time results.

### `pipeline`

Pipeline run tracking and stage-level execution metadata for the orchestrator.

### `social`

Social media content from external platforms (Instagram, TikTok, YouTube, Twitter/X). Includes in-app discussion threads and scraped content from external platforms.

## `core` tables

- `core.shows`: Top-level show record.
- `core.seasons`: Seasons belonging to a show (`show_id`).
- `core.episodes`: Episodes belonging to a season (`season_id`); `episode_number` is unique per season.
- `core.people`: People (cast/hosts/guests).
- `core.cast_memberships`: Links `people` to a `show` (and optionally a specific `season`).
- `core.episode_cast`: Links cast memberships to specific episodes.

Relationship chain: `shows -> seasons -> episodes`, with cast connected via `cast_memberships` and `episode_cast`.

## `core` views (Screenalytics)

- `core.v_episode_cast`: episode-level cast (credit_occurrences joined to credits).
- `core.v_season_cast`: season-level distinct cast, with episode counts per season.
- `core.v_person_images`: person images joined from media_links + media_assets for facebank seeding; includes `facebank_seed` flag from `core.media_links`.

## TMDb entity tables

These tables store normalized TMDb entities with S3-mirrored logos:

- `core.networks`: TV network dimension table (id, name, origin_country, hosted_logo_*)
- `core.production_companies`: Production company dimension table (same structure as networks)
- `core.watch_providers`: Streaming/rental provider dimension table (provider_id, provider_name, display_priority, hosted_logo_*)
- `core.show_watch_providers`: Junction table linking shows to providers by region and offer type

Key columns for dimension tables:
- `name` / `provider_name`: Display name
- `tmdb_logo_path`: Original TMDb logo path
- `hosted_logo_*`: S3-hosted logo metadata (key, url, sha256, content_type, bytes, etag, at)
- `tmdb_meta`: Raw TMDb API response (JSONB)

The `show_watch_providers` junction table has a composite primary key: `(show_id, region, offer_type, provider_id)`.

See `docs/architecture.md` for the full TMDb enrichment pipeline.

## `pipeline` tables

- `pipeline.runs`: One row per orchestrator invocation (status + config + timestamps).
- `pipeline.run_stages`: Per-stage execution status, hashes for resume, manifest keys, and metrics.

## `surveys` tables

- `surveys.surveys`: A survey scoped to a show/season/episode (`show_id` required; `season_id`/`episode_id` optional). Includes `slug` for URL-friendly identifiers.
- `surveys.questions`: Questions for a survey; `question_order` is unique per survey.
- `surveys.options`: Options for a question (for choice questions).
- `surveys.responses`: Per-user response header for a survey (user-scoped). **Unique constraint**: authenticated users can only submit one response per survey.
- `surveys.answers`: Per-response per-question answers (user-scoped via the owning response); includes `survey_id` to enforce response/question scope.
- `surveys.aggregates`: Live aggregates for survey questions (read-only to clients).

### `surveys` RPC functions

- `surveys.submit_response(survey_id uuid, answers jsonb) -> uuid`: Atomically submits a survey response with all answers. Uses `auth.uid()` internally for user identification. Prevents duplicate submissions for authenticated users. Returns the response_id.

## Games vs. surveys

- Legacy `games.*` tables were removed.
- `surveys.*` focuses on opinion/feedback capture via `surveys.responses` + `surveys.answers`, plus live rollups in `surveys.aggregates`.

## `social` tables

### In-app discussion (Reddit-style)

- `social.threads`: Discussion threads tied to episodes (type: episode_live, post_episode, spoilers, general).
- `social.posts`: Posts/comments within threads (supports nesting via `parent_post_id`).
- `social.reactions`: Reactions to posts (upvote, downvote, lol, shade, fire, heart).

### External platform scrape data

- `social.scrape_jobs`: Track scrape operations (platform, job_type, status, items_found).
- `social.instagram_posts`: Instagram posts and reels (shortcode unique key).
- `social.instagram_comments`: Instagram comments with reply support (parent_comment_id), mirrored comment media fields, and hosted commenter-avatar storage.
- `social.tiktok_posts`: TikTok videos (video_id unique key).
- `social.tiktok_comments`: TikTok comments with reply support (parent_comment_id).
- `social.youtube_videos`: YouTube videos (video_id unique key).
- `social.youtube_comments`: YouTube comments with reply support (parent_comment_id).
- `social.youtube_channel_sync_state`: Per-season/per-account YouTube discovery checkpoints, source mode, and diagnostics.
- `social.youtube_video_sync_state`: Per-video freshness state for stats/comments/media resolution and mirror progress.
- `social.twitter_tweets`: Twitter/X tweets including replies (tweet_id unique key, reply_to_tweet_id for threading).

All scrape tables include:
- Platform-specific IDs for deduplication
- `scraped_at` timestamp
- `raw_data` JSONB for full API responses
- Optional `show_id` and `person_id` for entity associations

## RLS defaults

Public read:

- `core.shows`, `core.seasons`, `core.episodes`, `core.people`, `core.cast_memberships`, `core.episode_cast`
- `surveys.surveys`, `surveys.questions`, `surveys.options`, `surveys.aggregates`
- `social.threads`, `social.posts`, `social.reactions` (in-app discussion)
- `social.scrape_jobs`, `social.instagram_posts`, `social.instagram_comments`, `social.tiktok_posts`, `social.tiktok_comments`, `social.youtube_videos`, `social.youtube_comments`, `social.youtube_channel_sync_state`, `social.youtube_video_sync_state`, `social.twitter_tweets` (scraped content)

User-scoped read/write:

- `surveys.responses`, `surveys.answers` (owned by `auth.uid()`)
- `social.threads`, `social.posts`, `social.reactions` (owned by `auth.uid()` for create/update/delete)

Read-only to clients:

- `surveys.aggregates` (no INSERT/UPDATE/DELETE policies)

Service role bypasses RLS (Supabase default).

## Seed data

The seed dataset in `supabase/seed.sql` creates:

- 1 show, 1 season, 2 episodes
- 5 people + cast links
- 1 survey with 3 questions and options, plus empty aggregates
