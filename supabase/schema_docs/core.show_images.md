# core.show_images

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| source | text | NO | 'tmdb'::text | NO | NEVER |
| kind | text | NO |  | NO | NEVER |
| iso_639_1 | text | YES |  | NO | NEVER |
| file_path | text | YES |  | NO | NEVER |
| width | integer | YES |  | NO | NEVER |
| height | integer | YES |  | NO | NEVER |
| aspect_ratio | numeric | YES |  | NO | NEVER |
| fetched_at | timestamp with time zone | NO | now() | NO | NEVER |
| tmdb_id | integer | YES |  | NO | NEVER |
| url_original | text | YES |  | NO | ALWAYS |
| source_image_id | text | NO |  | NO | NEVER |
| url | text | NO |  | NO | NEVER |
| url_path | text | YES |  | NO | NEVER |
| caption | text | YES |  | NO | NEVER |
| position | integer | YES |  | NO | NEVER |
| metadata | jsonb | NO | '{}'::jsonb | NO | NEVER |
| image_type | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| fetch_method | text | YES |  | NO | NEVER |
| fetched_from_url | text | YES |  | NO | NEVER |
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

- show_id, source, source_image_id
- tmdb_id, source, kind, file_path

## Foreign Keys

- show_id -> core.shows.id

## Indexes

- core_show_images_kind_lang_idx (non-unique): kind, iso_639_1
- core_show_images_show_id_idx (non-unique): show_id
- core_show_images_show_kind_idx (non-unique): show_id, kind
- core_show_images_source_show_idx (non-unique): source, show_id
- core_show_images_tmdb_kind_idx (non-unique): tmdb_id, kind
- core_show_images_tmdb_kind_lang_idx (non-unique): tmdb_id, kind, iso_639_1) WHERE (tmdb_id IS NOT NULL
- idx_show_images_hosted_at (non-unique): hosted_at) WHERE (hosted_at IS NOT NULL
- idx_show_images_hosted_sha256 (non-unique): hosted_sha256) WHERE (hosted_sha256 IS NOT NULL
- idx_show_images_missing_hosted (non-unique): source, show_id) WHERE (hosted_url IS NULL
- show_images_fetch_method_idx (non-unique): fetch_method
- show_images_pkey (unique): id
- show_images_show_source_source_image_id_key (unique): show_id, source, source_image_id
- show_images_tmdb_source_kind_file_path_key (unique): tmdb_id, source, kind, file_path

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "source": "example",
  "kind": "example",
  "iso_639_1": "example",
  "file_path": "example",
  "width": 0,
  "height": 0,
  "aspect_ratio": 0,
  "fetched_at": "1970-01-01T00:00:00Z",
  "tmdb_id": 0,
  "url_original": "example",
  "source_image_id": "example",
  "url": "example",
  "url_path": "example",
  "caption": "example",
  "position": 0,
  "metadata": {},
  "image_type": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "fetch_method": "example",
  "fetched_from_url": "example",
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