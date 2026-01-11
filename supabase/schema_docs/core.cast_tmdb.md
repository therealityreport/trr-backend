# core.cast_tmdb

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| tmdb_id | integer | NO |  | NO | NEVER |
| name | text | YES |  | NO | NEVER |
| also_known_as | ARRAY | YES |  | NO | NEVER |
| biography | text | YES |  | NO | NEVER |
| birthday | date | YES |  | NO | NEVER |
| deathday | date | YES |  | NO | NEVER |
| gender | smallint | YES | 0 | NO | NEVER |
| adult | boolean | YES | true | NO | NEVER |
| homepage | text | YES |  | NO | NEVER |
| known_for_department | text | YES |  | NO | NEVER |
| place_of_birth | text | YES |  | NO | NEVER |
| popularity | numeric | YES | 0 | NO | NEVER |
| profile_path | text | YES |  | NO | NEVER |
| imdb_id | text | YES |  | NO | NEVER |
| freebase_mid | text | YES |  | NO | NEVER |
| freebase_id | text | YES |  | NO | NEVER |
| tvrage_id | integer | YES |  | NO | NEVER |
| wikidata_id | text | YES |  | NO | NEVER |
| facebook_id | text | YES |  | NO | NEVER |
| instagram_id | text | YES |  | NO | NEVER |
| tiktok_id | text | YES |  | NO | NEVER |
| twitter_id | text | YES |  | NO | NEVER |
| youtube_id | text | YES |  | NO | NEVER |
| fetched_at | timestamp with time zone | YES | now() | NO | NEVER |
| created_at | timestamp with time zone | YES | now() | NO | NEVER |
| updated_at | timestamp with time zone | YES | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- person_id
- tmdb_id

## Foreign Keys

- person_id -> core.people.id

## Indexes

- cast_tmdb_person_id_unique (unique): person_id
- cast_tmdb_pkey (unique): id
- cast_tmdb_tmdb_id_unique (unique): tmdb_id
- idx_cast_tmdb_imdb_id (non-unique): imdb_id) WHERE (imdb_id IS NOT NULL
- idx_cast_tmdb_instagram_id (non-unique): instagram_id) WHERE (instagram_id IS NOT NULL
- idx_cast_tmdb_person_id (non-unique): person_id
- idx_cast_tmdb_tmdb_id (non-unique): tmdb_id
- idx_cast_tmdb_twitter_id (non-unique): twitter_id) WHERE (twitter_id IS NOT NULL

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "tmdb_id": 0,
  "name": "example",
  "also_known_as": [],
  "biography": "example",
  "birthday": "1970-01-01",
  "deathday": "1970-01-01",
  "gender": 0,
  "adult": false,
  "homepage": "example",
  "known_for_department": "example",
  "place_of_birth": "example",
  "popularity": 0,
  "profile_path": "example",
  "imdb_id": "example",
  "freebase_mid": "example",
  "freebase_id": "example",
  "tvrage_id": 0,
  "wikidata_id": "example",
  "facebook_id": "example",
  "instagram_id": "example",
  "tiktok_id": "example",
  "twitter_id": "example",
  "youtube_id": "example",
  "fetched_at": "1970-01-01T00:00:00Z",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```