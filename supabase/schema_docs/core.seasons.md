# core.seasons

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| show_name | text | YES |  | NO | NEVER |
| name | text | YES |  | NO | NEVER |
| season_number | integer | NO |  | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| title | text | YES |  | NO | NEVER |
| overview | text | YES |  | NO | NEVER |
| air_date | date | YES |  | NO | NEVER |
| premiere_date | date | YES |  | NO | NEVER |
| tmdb_series_id | integer | YES |  | NO | NEVER |
| imdb_series_id | text | YES |  | NO | NEVER |
| tmdb_season_id | integer | YES |  | NO | NEVER |
| tmdb_season_object_id | text | YES |  | NO | NEVER |
| poster_path | text | YES |  | NO | NEVER |
| url_original_poster | text | YES |  | NO | ALWAYS |
| external_tvdb_id | integer | YES |  | NO | NEVER |
| external_wikidata_id | text | YES |  | NO | NEVER |
| external_ids | jsonb | NO | '{}'::jsonb | NO | NEVER |
| language | text | NO | 'en-US'::text | NO | NEVER |
| fetched_at | timestamp with time zone | NO | now() | NO | NEVER |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- show_id, season_number

## Foreign Keys

- show_id -> core.shows.id

## Indexes

- core_seasons_show_id_season_number_idx (non-unique): show_id, season_number
- core_seasons_tmdb_series_season_unique (unique): tmdb_series_id, season_number) WHERE (tmdb_series_id IS NOT NULL
- seasons_pkey (unique): id
- seasons_show_id_idx (non-unique): show_id
- seasons_show_id_season_number_unique (unique): show_id, season_number

## RLS Enabled

true

## Example Row

```json
{
  "show_name": "example",
  "name": "example",
  "season_number": 0,
  "show_id": "00000000-0000-0000-0000-000000000000",
  "title": "example",
  "overview": "example",
  "air_date": "1970-01-01",
  "premiere_date": "1970-01-01",
  "tmdb_series_id": 0,
  "imdb_series_id": "example",
  "tmdb_season_id": 0,
  "tmdb_season_object_id": "example",
  "poster_path": "example",
  "url_original_poster": "example",
  "external_tvdb_id": 0,
  "external_wikidata_id": "example",
  "external_ids": {},
  "language": "example",
  "fetched_at": "1970-01-01T00:00:00Z",
  "id": "00000000-0000-0000-0000-000000000000",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```