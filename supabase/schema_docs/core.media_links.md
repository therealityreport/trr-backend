# core.media_links

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| entity_type | text | NO |  | NO | NEVER |
| entity_id | uuid | NO |  | NO | NEVER |
| media_asset_id | uuid | NO |  | NO | NEVER |
| kind | text | NO |  | NO | NEVER |
| position | integer | YES |  | NO | NEVER |
| is_primary | boolean | NO | false | NO | NEVER |
| context | jsonb | NO | '{}'::jsonb | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| facebank_seed | boolean | NO | false | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- media_asset_id -> core.media_assets.id

## Indexes

- idx_media_links_person_gallery_entity_kind_id (non-unique): entity_type, entity_id, kind, id) WHERE ((entity_type = 'person'::text) AND (kind = 'gallery'::text)
- media_links_entity_idx (non-unique): entity_type, entity_id
- media_links_entity_kind_asset_uq (unique): entity_type, entity_id, kind, media_asset_id
- media_links_facebank_seed_idx (non-unique): entity_type, entity_id, kind, facebank_seed
- media_links_kind_idx (non-unique): entity_type, entity_id, kind
- media_links_kind_position_idx (non-unique): entity_type, entity_id, kind, position
- media_links_media_asset_idx (non-unique): media_asset_id
- media_links_one_primary_per_entity_kind (unique): entity_type, entity_id, kind) WHERE (is_primary = true
- media_links_one_primary_uq (unique): entity_type, entity_id, kind) WHERE (is_primary = true
- media_links_pkey (unique): id

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "entity_type": "example",
  "entity_id": "00000000-0000-0000-0000-000000000000",
  "media_asset_id": "00000000-0000-0000-0000-000000000000",
  "kind": "example",
  "position": 0,
  "is_primary": false,
  "context": {},
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "facebank_seed": false
}
```