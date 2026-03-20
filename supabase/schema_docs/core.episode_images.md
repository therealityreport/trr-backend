# core.episode_images

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| season_id | uuid | NO |  | NO | NEVER |
| episode_id | uuid | NO |  | NO | NEVER |
| tmdb_series_id | integer | NO |  | NO | NEVER |
| season_number | integer | NO |  | NO | NEVER |
| episode_number | integer | NO |  | NO | NEVER |
| source | text | NO | 'tmdb'::text | NO | NEVER |
| kind | text | NO | 'still'::text | NO | NEVER |
| iso_639_1 | text | YES |  | NO | NEVER |
| file_path | text | NO |  | NO | NEVER |
| url | text | NO |  | NO | NEVER |
| url_original | text | YES |  | NO | ALWAYS |
| source_image_id | text | NO |  | NO | NEVER |
| width | integer | NO |  | NO | NEVER |
| height | integer | NO |  | NO | NEVER |
| aspect_ratio | numeric | NO |  | NO | NEVER |
| caption | text | YES |  | NO | NEVER |
| position | integer | YES |  | NO | NEVER |
| metadata | jsonb | NO | '{}'::jsonb | NO | NEVER |
| fetch_method | text | YES |  | NO | NEVER |
| fetched_from_url | text | YES |  | NO | NEVER |
| fetched_at | timestamp with time zone | NO | now() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| hosted_bucket | text | YES |  | NO | NEVER |
| hosted_key | text | YES |  | NO | NEVER |
| hosted_url | text | YES |  | NO | NEVER |
| hosted_sha256 | text | YES |  | NO | NEVER |
| hosted_content_type | text | YES |  | NO | NEVER |
| hosted_bytes | bigint | YES |  | NO | NEVER |
| hosted_etag | text | YES |  | NO | NEVER |
| hosted_at | timestamp with time zone | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- episode_id -> core.episodes.id
- season_id -> core.seasons.id
- show_id -> core.shows.id

## Indexes

- episode_images_episode_id_idx (non-unique): episode_id
- episode_images_episode_source_image_unique (unique): episode_id, source, source_image_id
- episode_images_fetch_method_idx (non-unique): fetch_method
- episode_images_hosted_at_idx (non-unique): hosted_at) WHERE (hosted_at IS NOT NULL
- episode_images_hosted_sha256_idx (non-unique): hosted_sha256) WHERE (hosted_sha256 IS NOT NULL
- episode_images_metadata_idx (non-unique): metadata
- episode_images_missing_hosted_idx (non-unique): id) WHERE (hosted_url IS NULL
- episode_images_pkey (unique): id
- episode_images_season_id_idx (non-unique): season_id
- episode_images_show_id_idx (non-unique): show_id
- episode_images_source_image_id_idx (non-unique): source, source_image_id) WHERE (source_image_id IS NOT NULL
- episode_images_tmdb_episode_source_file_unique (unique): tmdb_series_id, season_number, episode_number, source, file_path
- episode_images_tmdb_series_season_episode_idx (non-unique): tmdb_series_id, season_number, episode_number

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "season_id": "00000000-0000-0000-0000-000000000000",
  "episode_id": "00000000-0000-0000-0000-000000000000",
  "tmdb_series_id": 0,
  "season_number": 0,
  "episode_number": 0,
  "source": "example",
  "kind": "example",
  "iso_639_1": "example",
  "file_path": "example",
  "url": "example",
  "url_original": "example",
  "source_image_id": "example",
  "width": 0,
  "height": 0,
  "aspect_ratio": 0,
  "caption": "example",
  "position": 0,
  "metadata": {},
  "fetch_method": "example",
  "fetched_from_url": "example",
  "fetched_at": "1970-01-01T00:00:00Z",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "hosted_bucket": "example",
  "hosted_key": "example",
  "hosted_url": "example",
  "hosted_sha256": "example",
  "hosted_content_type": "example",
  "hosted_bytes": 0,
  "hosted_etag": "example",
  "hosted_at": "1970-01-01T00:00:00Z"
}
```