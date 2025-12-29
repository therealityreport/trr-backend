# core.episode_appearances

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| show_name | text | YES |  | NO | NEVER |
| cast_member_name | text | YES |  | NO | NEVER |
| seasons | ARRAY | NO | '{}'::integer[] | NO | NEVER |
| tmdb_season_ids | ARRAY | NO | '{}'::integer[] | NO | NEVER |
| tmdb_show_id | integer | YES |  | NO | NEVER |
| imdb_show_id | text | YES |  | NO | NEVER |
| imdb_episode_title_ids | ARRAY | NO | '{}'::text[] | NO | NEVER |
| tmdb_episode_ids | ARRAY | NO | '{}'::integer[] | NO | NEVER |
| total_episodes | integer | YES |  | NO | ALWAYS |
| show_id | uuid | NO |  | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- show_id, person_id

## Foreign Keys

- person_id -> core.people.id
- show_id -> core.shows.id

## Indexes

- core_episode_appearances_person_id_idx (non-unique): person_id
- core_episode_appearances_show_id_idx (non-unique): show_id
- episode_appearances_pkey (unique): id
- episode_appearances_show_id_person_id_key (unique): show_id, person_id

## RLS Enabled

true

## Example Row

```json
{
  "show_name": "example",
  "cast_member_name": "example",
  "seasons": [],
  "tmdb_season_ids": [],
  "tmdb_show_id": 0,
  "imdb_show_id": "example",
  "imdb_episode_title_ids": [],
  "tmdb_episode_ids": [],
  "total_episodes": 0,
  "show_id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "id": "00000000-0000-0000-0000-000000000000",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```