# core.media_uploads

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| uploader_user_id | uuid | YES |  | NO | NEVER |
| entity_type | text | NO |  | NO | NEVER |
| entity_id | uuid | NO |  | NO | NEVER |
| kind | text | NO |  | NO | NEVER |
| original_filename | text | YES |  | NO | NEVER |
| content_type | text | NO |  | NO | NEVER |
| expected_bytes | bigint | YES |  | NO | NEVER |
| caption | text | YES |  | NO | NEVER |
| alt_text | text | YES |  | NO | NEVER |
| make_primary | boolean | NO | false | NO | NEVER |
| status | text | NO | 'initiated'::text | NO | NEVER |
| error | text | YES |  | NO | NEVER |
| expires_at | timestamp with time zone | NO | (now() + '01:00:00'::interval) | NO | NEVER |
| s3_bucket | text | NO |  | NO | NEVER |
| s3_temp_key | text | NO |  | NO | NEVER |
| media_asset_id | uuid | YES |  | NO | NEVER |
| media_link_id | uuid | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- media_asset_id -> core.media_assets.id
- media_link_id -> core.media_links.id

## Indexes

- media_uploads_entity_idx (non-unique): entity_type, entity_id, kind
- media_uploads_pkey (unique): id
- media_uploads_status_idx (non-unique): status, expires_at
- media_uploads_uploader_idx (non-unique): uploader_user_id) WHERE (uploader_user_id IS NOT NULL

## RLS Enabled

true

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "uploader_user_id": "00000000-0000-0000-0000-000000000000",
  "entity_type": "example",
  "entity_id": "00000000-0000-0000-0000-000000000000",
  "kind": "example",
  "original_filename": "example",
  "content_type": "example",
  "expected_bytes": 0,
  "caption": "example",
  "alt_text": "example",
  "make_primary": false,
  "status": "example",
  "error": "example",
  "expires_at": "1970-01-01T00:00:00Z",
  "s3_bucket": "example",
  "s3_temp_key": "example",
  "media_asset_id": "00000000-0000-0000-0000-000000000000",
  "media_link_id": "00000000-0000-0000-0000-000000000000",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```