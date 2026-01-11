# core.shows

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| name | text | NO |  | NO | NEVER |
| show_total_seasons | integer | YES |  | NO | NEVER |
| show_total_episodes | integer | YES |  | NO | NEVER |
| imdb_id | text | YES |  | NO | NEVER |
| tmdb_id | integer | YES |  | NO | NEVER |
| most_recent_episode | text | YES |  | NO | NEVER |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| description | text | YES |  | NO | NEVER |
| premiere_date | date | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| most_recent_episode_season | integer | YES |  | NO | NEVER |
| most_recent_episode_number | integer | YES |  | NO | NEVER |
| most_recent_episode_title | text | YES |  | NO | NEVER |
| most_recent_episode_air_date | date | YES |  | NO | NEVER |
| most_recent_episode_imdb_id | text | YES |  | NO | NEVER |
| primary_poster_image_id | uuid | YES |  | NO | NEVER |
| primary_backdrop_image_id | uuid | YES |  | NO | NEVER |
| primary_logo_image_id | uuid | YES |  | NO | NEVER |
| needs_imdb_resolution | boolean | NO | false | NO | NEVER |
| genres | ARRAY | YES |  | NO | NEVER |
| keywords | ARRAY | YES |  | NO | NEVER |
| tags | ARRAY | YES |  | NO | NEVER |
| networks | ARRAY | YES |  | NO | NEVER |
| streaming_providers | ARRAY | YES |  | NO | NEVER |
| listed_on | ARRAY | YES |  | NO | NEVER |
| tvdb_id | integer | YES |  | NO | NEVER |
| tvrage_id | integer | YES |  | NO | NEVER |
| wikidata_id | text | YES |  | NO | NEVER |
| facebook_id | text | YES |  | NO | NEVER |
| instagram_id | text | YES |  | NO | NEVER |
| twitter_id | text | YES |  | NO | NEVER |
| needs_tmdb_resolution | boolean | NO | false | NO | NEVER |
| tmdb_name | text | YES |  | NO | NEVER |
| tmdb_status | text | YES |  | NO | NEVER |
| tmdb_type | text | YES |  | NO | NEVER |
| tmdb_first_air_date | date | YES |  | NO | NEVER |
| tmdb_last_air_date | date | YES |  | NO | NEVER |
| tmdb_vote_average | numeric | YES |  | NO | NEVER |
| tmdb_vote_count | integer | YES |  | NO | NEVER |
| tmdb_popularity | numeric | YES |  | NO | NEVER |
| imdb_title | text | YES |  | NO | NEVER |
| imdb_content_rating | text | YES |  | NO | NEVER |
| imdb_rating_value | numeric | YES |  | NO | NEVER |
| imdb_rating_count | integer | YES |  | NO | NEVER |
| imdb_date_published | date | YES |  | NO | NEVER |
| imdb_end_year | integer | YES |  | NO | NEVER |
| tmdb_fetched_at | timestamp with time zone | YES |  | NO | NEVER |
| imdb_fetched_at | timestamp with time zone | YES |  | NO | NEVER |
| tmdb_meta | jsonb | YES |  | NO | NEVER |
| imdb_meta | jsonb | YES |  | NO | NEVER |
| tmdb_network_ids | ARRAY | YES |  | NO | NEVER |
| tmdb_production_company_ids | ARRAY | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- primary_backdrop_image_id -> core.show_images.id
- primary_logo_image_id -> core.show_images.id
- primary_poster_image_id -> core.show_images.id

## Indexes

- core_shows_genres_gin (non-unique): genres
- core_shows_imdb_id_unique (unique): imdb_id) WHERE ((imdb_id IS NOT NULL) AND (btrim(imdb_id) <> ''::text)
- core_shows_keywords_gin (non-unique): keywords
- core_shows_listed_on_gin (non-unique): listed_on
- core_shows_networks_gin (non-unique): networks
- core_shows_streaming_providers_gin (non-unique): streaming_providers
- core_shows_tags_gin (non-unique): tags
- core_shows_tmdb_id_unique (unique): tmdb_id) WHERE (tmdb_id IS NOT NULL
- core_shows_tmdb_network_ids_gin (non-unique): tmdb_network_ids
- core_shows_tmdb_production_company_ids_gin (non-unique): tmdb_production_company_ids
- shows_pkey (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "name": "example",
  "show_total_seasons": 0,
  "show_total_episodes": 0,
  "imdb_id": "example",
  "tmdb_id": 0,
  "most_recent_episode": "example",
  "id": "00000000-0000-0000-0000-000000000000",
  "description": "example",
  "premiere_date": "1970-01-01",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "most_recent_episode_season": 0,
  "most_recent_episode_number": 0,
  "most_recent_episode_title": "example",
  "most_recent_episode_air_date": "1970-01-01",
  "most_recent_episode_imdb_id": "example",
  "primary_poster_image_id": "00000000-0000-0000-0000-000000000000",
  "primary_backdrop_image_id": "00000000-0000-0000-0000-000000000000",
  "primary_logo_image_id": "00000000-0000-0000-0000-000000000000",
  "needs_imdb_resolution": false,
  "genres": [],
  "keywords": [],
  "tags": [],
  "networks": [],
  "streaming_providers": [],
  "listed_on": [],
  "tvdb_id": 0,
  "tvrage_id": 0,
  "wikidata_id": "example",
  "facebook_id": "example",
  "instagram_id": "example",
  "twitter_id": "example",
  "needs_tmdb_resolution": false,
  "tmdb_name": "example",
  "tmdb_status": "example",
  "tmdb_type": "example",
  "tmdb_first_air_date": "1970-01-01",
  "tmdb_last_air_date": "1970-01-01",
  "tmdb_vote_average": 0,
  "tmdb_vote_count": 0,
  "tmdb_popularity": 0,
  "imdb_title": "example",
  "imdb_content_rating": "example",
  "imdb_rating_value": 0,
  "imdb_rating_count": 0,
  "imdb_date_published": "1970-01-01",
  "imdb_end_year": 0,
  "tmdb_fetched_at": "1970-01-01T00:00:00Z",
  "imdb_fetched_at": "1970-01-01T00:00:00Z",
  "tmdb_meta": {},
  "imdb_meta": {},
  "tmdb_network_ids": [],
  "tmdb_production_company_ids": []
}
```