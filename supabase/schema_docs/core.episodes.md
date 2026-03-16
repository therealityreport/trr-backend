# core.episodes

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| show_name | text | YES |  | NO | NEVER |
| title | text | YES |  | NO | NEVER |
| season_number | integer | NO |  | NO | NEVER |
| episode_number | integer | NO |  | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| air_date | date | YES |  | NO | NEVER |
| synopsis | text | YES |  | NO | NEVER |
| overview | text | YES |  | NO | NEVER |
| imdb_episode_id | text | YES |  | NO | NEVER |
| imdb_rating | numeric | YES |  | NO | NEVER |
| imdb_vote_count | integer | YES |  | NO | NEVER |
| imdb_primary_image_url | text | YES |  | NO | NEVER |
| imdb_primary_image_caption | text | YES |  | NO | NEVER |
| imdb_primary_image_width | integer | YES |  | NO | NEVER |
| imdb_primary_image_height | integer | YES |  | NO | NEVER |
| tmdb_series_id | integer | YES |  | NO | NEVER |
| tmdb_episode_id | integer | YES |  | NO | NEVER |
| episode_type | text | YES |  | NO | NEVER |
| production_code | text | YES |  | NO | NEVER |
| runtime | integer | YES |  | NO | NEVER |
| still_path | text | YES |  | NO | NEVER |
| url_original_still | text | YES |  | NO | ALWAYS |
| tmdb_vote_average | numeric | YES |  | NO | NEVER |
| tmdb_vote_count | integer | YES |  | NO | NEVER |
| external_ids | jsonb | NO | '{}'::jsonb | NO | NEVER |
| fetched_at | timestamp with time zone | NO | now() | NO | NEVER |
| season_id | uuid | NO |  | NO | NEVER |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- season_id, episode_number

## Foreign Keys

- season_id -> core.seasons.id
- show_id -> core.shows.id

## Indexes

- core_episodes_imdb_episode_id_unique (unique): imdb_episode_id) WHERE (imdb_episode_id IS NOT NULL
- core_episodes_show_season_episode_unique (unique): show_id, season_number, episode_number
- core_episodes_show_season_idx (non-unique): show_id, season_number, episode_number
- core_episodes_tmdb_episode_id_unique (unique): tmdb_episode_id) WHERE (tmdb_episode_id IS NOT NULL
- episodes_pkey (unique): id
- episodes_season_id_episode_number_unique (unique): season_id, episode_number
- episodes_season_id_idx (non-unique): season_id

## RLS Enabled

true

## Example Row

```json
{
  "show_name": "example",
  "title": "example",
  "season_number": 0,
  "episode_number": 0,
  "show_id": "00000000-0000-0000-0000-000000000000",
  "air_date": "1970-01-01",
  "synopsis": "example",
  "overview": "example",
  "imdb_episode_id": "example",
  "imdb_rating": 0,
  "imdb_vote_count": 0,
  "imdb_primary_image_url": "example",
  "imdb_primary_image_caption": "example",
  "imdb_primary_image_width": 0,
  "imdb_primary_image_height": 0,
  "tmdb_series_id": 0,
  "tmdb_episode_id": 0,
  "episode_type": "example",
  "production_code": "example",
  "runtime": 0,
  "still_path": "example",
  "url_original_still": "example",
  "tmdb_vote_average": 0,
  "tmdb_vote_count": 0,
  "external_ids": {},
  "fetched_at": "1970-01-01T00:00:00Z",
  "season_id": "00000000-0000-0000-0000-000000000000",
  "id": "00000000-0000-0000-0000-000000000000",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```