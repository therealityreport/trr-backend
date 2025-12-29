# core.shows

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| name | text | NO |  | NO | NEVER |
| network | text | YES |  | NO | NEVER |
| streaming | text | YES |  | NO | NEVER |
| show_total_seasons | integer | YES |  | NO | NEVER |
| show_total_episodes | integer | YES |  | NO | NEVER |
| imdb_series_id | text | YES |  | NO | NEVER |
| tmdb_series_id | integer | YES |  | NO | NEVER |
| most_recent_episode | text | YES |  | NO | NEVER |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| description | text | YES |  | NO | NEVER |
| premiere_date | date | YES |  | NO | NEVER |
| external_ids | jsonb | NO | '{}'::jsonb | NO | NEVER |
| primary_tmdb_poster_path | text | YES |  | NO | NEVER |
| primary_tmdb_backdrop_path | text | YES |  | NO | NEVER |
| primary_tmdb_logo_path | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| imdb_meta | jsonb | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- core_shows_external_ids_gin (non-unique): external_ids
- core_shows_external_ids_imdb_unique (unique): ((external_ids ->> 'imdb'::text))) WHERE (COALESCE((external_ids ->> 'imdb'::text), ''::text) <> ''::text
- core_shows_external_ids_tmdb_unique (unique): ((external_ids ->> 'tmdb'::text))) WHERE (COALESCE((external_ids ->> 'tmdb'::text), ''::text) <> ''::text
- core_shows_imdb_series_id_unique (unique): imdb_series_id) WHERE ((imdb_series_id IS NOT NULL) AND (btrim(imdb_series_id) <> ''::text)
- core_shows_tmdb_series_id_unique (unique): tmdb_series_id) WHERE (tmdb_series_id IS NOT NULL
- shows_pkey (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "name": "example",
  "network": "example",
  "streaming": "example",
  "show_total_seasons": 0,
  "show_total_episodes": 0,
  "imdb_series_id": "example",
  "tmdb_series_id": 0,
  "most_recent_episode": "example",
  "id": "00000000-0000-0000-0000-000000000000",
  "description": "example",
  "premiere_date": "1970-01-01",
  "external_ids": {},
  "primary_tmdb_poster_path": "example",
  "primary_tmdb_backdrop_path": "example",
  "primary_tmdb_logo_path": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "imdb_meta": {}
}
```