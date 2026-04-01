# core.media_assets

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| media_type | text | NO | 'image'::text | NO | NEVER |
| source | text | NO |  | NO | NEVER |
| source_asset_id | text | YES |  | NO | NEVER |
| source_url | text | YES |  | NO | NEVER |
| sha256 | text | YES |  | NO | NEVER |
| content_type | text | YES |  | NO | NEVER |
| bytes | bigint | YES |  | NO | NEVER |
| width | integer | YES |  | NO | NEVER |
| height | integer | YES |  | NO | NEVER |
| caption | text | YES |  | NO | NEVER |
| alt_text | text | YES |  | NO | NEVER |
| hosted_bucket | text | YES |  | NO | NEVER |
| hosted_key | text | YES |  | NO | NEVER |
| hosted_url | text | YES |  | NO | NEVER |
| hosted_etag | text | YES |  | NO | NEVER |
| hosted_at | timestamp with time zone | YES |  | NO | NEVER |
| hosted_sha256 | text | YES |  | NO | NEVER |
| hosted_content_type | text | YES |  | NO | NEVER |
| hosted_bytes | bigint | YES |  | NO | NEVER |
| metadata | jsonb | NO | '{}'::jsonb | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| fetched_at | timestamp with time zone | YES |  | NO | NEVER |
| ingest_status | text | NO | 'pending'::text | NO | NEVER |
| ingest_retry_count | integer | NO | 0 | NO | NEVER |
| ingest_last_error | text | YES |  | NO | NEVER |
| ingest_failed_at | timestamp with time zone | YES |  | NO | NEVER |
| ingest_completed_at | timestamp with time zone | YES |  | NO | NEVER |
| ingest_next_retry_at | timestamp with time zone | YES |  | NO | NEVER |
| archived_at | timestamp with time zone | YES |  | NO | NEVER |
| archived_by_firebase_uid | text | YES |  | NO | NEVER |
| archived_reason | text | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- idx_media_assets_archived (non-unique): archived_at) WHERE (archived_at IS NOT NULL
- media_assets_hosted_url_idx (non-unique): hosted_url) WHERE (hosted_url IS NOT NULL
- media_assets_ingest_next_retry_idx (non-unique): ingest_next_retry_at) WHERE ((ingest_status = 'failed'::text) AND (ingest_next_retry_at IS NOT NULL)
- media_assets_ingest_pending_failed_idx (non-unique): source, ingest_status) WHERE (ingest_status = ANY (ARRAY['pending'::text, 'failed'::text])
- media_assets_metadata_idx (non-unique): metadata
- media_assets_pkey (unique): id
- media_assets_sha256_idx (non-unique): sha256) WHERE (sha256 IS NOT NULL
- media_assets_sha256_unique (unique): sha256) WHERE (sha256 IS NOT NULL
- media_assets_source_asset_id_unique (unique): source, source_asset_id) WHERE (source_asset_id IS NOT NULL
- media_assets_source_hosted_sha_uq (unique): source, hosted_sha256) WHERE (hosted_sha256 IS NOT NULL
- media_assets_source_idx (non-unique): source
- media_assets_source_url_unique (unique): source, source_url) WHERE (source_url IS NOT NULL

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "media_type": "example",
  "source": "example",
  "source_asset_id": "example",
  "source_url": "example",
  "sha256": "example",
  "content_type": "example",
  "bytes": 0,
  "width": 0,
  "height": 0,
  "caption": "example",
  "alt_text": "example",
  "hosted_bucket": "example",
  "hosted_key": "example",
  "hosted_url": "example",
  "hosted_etag": "example",
  "hosted_at": "1970-01-01T00:00:00Z",
  "hosted_sha256": "example",
  "hosted_content_type": "example",
  "hosted_bytes": 0,
  "metadata": {},
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "fetched_at": "1970-01-01T00:00:00Z",
  "ingest_status": "example",
  "ingest_retry_count": 0,
  "ingest_last_error": "example",
  "ingest_failed_at": "1970-01-01T00:00:00Z",
  "ingest_completed_at": "1970-01-01T00:00:00Z",
  "ingest_next_retry_at": "1970-01-01T00:00:00Z",
  "archived_at": "1970-01-01T00:00:00Z",
  "archived_by_firebase_uid": "example",
  "archived_reason": "example"
}
```