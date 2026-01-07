# Supabase database schema (TRR backend)

This repo uses Supabase Postgres as the system of record. The authoritative DDL lives in `supabase/migrations/0001_init.sql`.

## Schemas

### `core`

Canonical TV metadata: shows, seasons, episodes, and cast.

### `games`

Interactive "game" content (quiz/poll/prediction/ranking), user sessions, and user responses. Answer keys are stored separately from user responses.

### `surveys`

Survey definitions, user responses, user answers, and live aggregates for near-real-time results.

## `core` tables

- `core.shows`: Top-level show record.
- `core.seasons`: Seasons belonging to a show (`show_id`).
- `core.episodes`: Episodes belonging to a season (`season_id`); `episode_number` is unique per season.
- `core.people`: People (cast/hosts/guests).
- `core.cast_memberships`: Links `people` to a `show` (and optionally a specific `season`).
- `core.episode_cast`: Links cast memberships to specific episodes.

Relationship chain: `shows -> seasons -> episodes`, with cast connected via `cast_memberships` and `episode_cast`.

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

## `games` tables

- `games.games`: A game scoped to a show/season/episode (`show_id` required; `season_id`/`episode_id` optional).
- `games.questions`: Questions for a game; `question_order` is unique per game.
- `games.options`: Options for a question (for choice/ranking styles).
- `games.answer_keys`: Answer keys per question (kept separate from user responses).
- `games.sessions`: Per-user session for playing a game (user-scoped).
- `games.responses`: Per-session per-question answers (user-scoped via the owning session); includes `game_id` to enforce session/question scope.
- `games.stats`: Aggregated/computed stats for games/questions (read-only to clients).

## `surveys` tables

- `surveys.surveys`: A survey scoped to a show/season/episode (`show_id` required; `season_id`/`episode_id` optional).
- `surveys.questions`: Questions for a survey; `question_order` is unique per survey.
- `surveys.options`: Options for a question (for choice questions).
- `surveys.responses`: Per-user response header for a survey (user-scoped).
- `surveys.answers`: Per-response per-question answers (user-scoped via the owning response); includes `survey_id` to enforce response/question scope.
- `surveys.aggregates`: Live aggregates for survey questions (read-only to clients).

## Games vs. surveys

- `games.*` supports scoring via `games.answer_keys` and session-based play via `games.sessions` + `games.responses`.
- `surveys.*` focuses on opinion/feedback capture via `surveys.responses` + `surveys.answers`, plus live rollups in `surveys.aggregates`.

## RLS defaults

Public read:

- `core.shows`, `core.seasons`, `core.episodes`, `core.people`, `core.cast_memberships`, `core.episode_cast`
- `games.games`, `games.questions`, `games.options`, `games.stats`
- `surveys.surveys`, `surveys.questions`, `surveys.options`, `surveys.aggregates`

User-scoped read/write:

- `games.sessions`, `games.responses` (owned by `auth.uid()`)
- `surveys.responses`, `surveys.answers` (owned by `auth.uid()`)

Read-only to clients:

- `games.stats`, `surveys.aggregates` (no INSERT/UPDATE/DELETE policies)

Service role bypasses RLS (Supabase default).

## Seed data

The seed dataset in `supabase/seed.sql` creates:

- 1 show, 1 season, 2 episodes
- 5 people + cast links
- 1 survey with 3 questions and options, plus empty aggregates
- 1 game with 2 questions and options, plus answer keys and empty stats
