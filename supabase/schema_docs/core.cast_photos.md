# core.cast_photos

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| person_id | uuid | NO |  | NO | NEVER |
| imdb_person_id | text | YES |  | NO | NEVER |
| source | text | NO | 'imdb'::text | NO | NEVER |
| source_image_id | text | YES |  | NO | NEVER |
| viewer_id | text | YES |  | NO | NEVER |
| mediaindex_url_path | text | YES |  | NO | NEVER |
| mediaviewer_url_path | text | YES |  | NO | NEVER |
| url | text | NO |  | NO | NEVER |
| url_path | text | YES |  | NO | NEVER |
| width | integer | YES |  | NO | NEVER |
| height | integer | YES |  | NO | NEVER |
| caption | text | YES |  | NO | NEVER |
| gallery_index | integer | YES |  | NO | NEVER |
| gallery_total | integer | YES |  | NO | NEVER |
| people_imdb_ids | ARRAY | YES |  | NO | NEVER |
| people_names | ARRAY | YES |  | NO | NEVER |
| title_imdb_ids | ARRAY | YES |  | NO | NEVER |
| title_names | ARRAY | YES |  | NO | NEVER |
| fetched_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| metadata | jsonb | YES |  | NO | NEVER |
| source_page_url | text | YES |  | NO | NEVER |
| image_url | text | YES |  | NO | NEVER |
| thumb_url | text | YES |  | NO | NEVER |
| file_name | text | YES |  | NO | NEVER |
| alt_text | text | YES |  | NO | NEVER |
| context_section | text | YES |  | NO | NEVER |
| context_type | text | YES |  | NO | NEVER |
| season | integer | YES |  | NO | NEVER |
| position | integer | YES |  | NO | NEVER |
| image_url_canonical | text | YES |  | NO | NEVER |
| hosted_bucket | text | YES |  | NO | NEVER |
| hosted_key | text | YES |  | NO | NEVER |
| hosted_url | text | YES |  | NO | NEVER |
| hosted_sha256 | text | YES |  | NO | NEVER |
| hosted_content_type | text | YES |  | NO | NEVER |
| hosted_bytes | integer | YES |  | NO | NEVER |
| hosted_etag | text | YES |  | NO | NEVER |
| hosted_at | timestamp with time zone | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

- person_id, source, image_url_canonical
- person_id, source, source_image_id

## Foreign Keys

- person_id -> core.people.id

## Indexes

- cast_photos_hosted_at_idx (non-unique): hosted_at) WHERE (hosted_at IS NOT NULL
- cast_photos_hosted_sha_idx (non-unique): hosted_sha256) WHERE (hosted_sha256 IS NOT NULL
- cast_photos_person_source_idx (non-unique): person_id, source
- cast_photos_person_source_image_url_canonical_key (unique): person_id, source, image_url_canonical
- cast_photos_person_source_source_image_id_key (unique): person_id, source, source_image_id
- cast_photos_pkey (unique): id
- core_cast_photos_imdb_person_id_idx (non-unique): imdb_person_id
- core_cast_photos_person_id_idx (non-unique): person_id
- core_cast_photos_source_source_image_id_idx (non-unique): source, source_image_id
- idx_cast_photos_source_tmdb (non-unique): person_id, source) WHERE (source = 'tmdb'::text

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "person_id": "00000000-0000-0000-0000-000000000000",
  "imdb_person_id": "example",
  "source": "example",
  "source_image_id": "example",
  "viewer_id": "example",
  "mediaindex_url_path": "example",
  "mediaviewer_url_path": "example",
  "url": "example",
  "url_path": "example",
  "width": 0,
  "height": 0,
  "caption": "example",
  "gallery_index": 0,
  "gallery_total": 0,
  "people_imdb_ids": [],
  "people_names": [],
  "title_imdb_ids": [],
  "title_names": [],
  "fetched_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "metadata": {},
  "source_page_url": "example",
  "image_url": "example",
  "thumb_url": "example",
  "file_name": "example",
  "alt_text": "example",
  "context_section": "example",
  "context_type": "example",
  "season": 0,
  "position": 0,
  "image_url_canonical": "example",
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