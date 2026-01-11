# core.season_images

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| season_id | uuid | NO |  | NO | NEVER |
| tmdb_series_id | integer | NO |  | NO | NEVER |
| season_number | integer | NO |  | NO | NEVER |
| source | text | NO | 'tmdb'::text | NO | NEVER |
| kind | text | NO | 'poster'::text | NO | NEVER |
| iso_639_1 | text | YES |  | NO | NEVER |
| file_path | text | NO |  | NO | NEVER |
| url_original | text | YES |  | NO | ALWAYS |
| width | integer | NO |  | NO | NEVER |
| height | integer | NO |  | NO | NEVER |
| aspect_ratio | numeric | NO |  | NO | NEVER |
| fetched_at | timestamp with time zone | NO | now() | NO | NEVER |
| hosted_url | text | YES |  | NO | NEVER |
| hosted_sha256 | text | YES |  | NO | NEVER |
| hosted_key | text | YES |  | NO | NEVER |
| hosted_bucket | text | YES |  | NO | NEVER |
| hosted_content_type | text | YES |  | NO | NEVER |
| hosted_bytes | bigint | YES |  | NO | NEVER |
| hosted_etag | text | YES |  | NO | NEVER |
| hosted_at | timestamp with time zone | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- season_id -> core.seasons.id
- show_id -> core.shows.id

## Indexes

- core_season_images_season_id_idx (non-unique): season_id
- core_season_images_tmdb_series_season_idx (non-unique): tmdb_series_id, season_number
- core_season_images_unique (unique): tmdb_series_id, season_number, source, file_path
- season_images_pkey (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "season_id": "00000000-0000-0000-0000-000000000000",
  "tmdb_series_id": 0,
  "season_number": 0,
  "source": "example",
  "kind": "example",
  "iso_639_1": "example",
  "file_path": "example",
  "url_original": "example",
  "width": 0,
  "height": 0,
  "aspect_ratio": 0,
  "fetched_at": "1970-01-01T00:00:00Z",
  "hosted_url": "example",
  "hosted_sha256": "example",
  "hosted_key": "example",
  "hosted_bucket": "example",
  "hosted_content_type": "example",
  "hosted_bytes": 0,
  "hosted_etag": "example",
  "hosted_at": "1970-01-01T00:00:00Z"
}
```