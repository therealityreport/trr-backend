# core.media_asset_variants

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| media_asset_id | uuid | NO |  | NO | NEVER |
| variant_key | text | NO |  | NO | NEVER |
| format | text | NO |  | NO | NEVER |
| width | integer | YES |  | NO | NEVER |
| height | integer | YES |  | NO | NEVER |
| bytes | bigint | YES |  | NO | NEVER |
| hosted_bucket | text | NO |  | NO | NEVER |
| hosted_key | text | NO |  | NO | NEVER |
| hosted_url | text | NO |  | NO | NEVER |
| crop_mode | text | YES |  | NO | NEVER |
| crop_x | numeric | YES |  | NO | NEVER |
| crop_y | numeric | YES |  | NO | NEVER |
| crop_zoom | numeric | YES |  | NO | NEVER |
| crop_signature | text | NO | 'base'::text | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- media_asset_id -> core.media_assets.id

## Indexes

- media_asset_variants_asset_idx (non-unique): media_asset_id
- media_asset_variants_asset_variant_format_crop_uq (unique): media_asset_id, variant_key, format, crop_signature
- media_asset_variants_crop_signature_idx (non-unique): crop_signature
- media_asset_variants_hosted_key_uq (unique): hosted_key
- media_asset_variants_pkey (unique): id

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "media_asset_id": "00000000-0000-0000-0000-000000000000",
  "variant_key": "example",
  "format": "example",
  "width": 0,
  "height": 0,
  "bytes": 0,
  "hosted_bucket": "example",
  "hosted_key": "example",
  "hosted_url": "example",
  "crop_mode": "example",
  "crop_x": 0,
  "crop_y": 0,
  "crop_zoom": 0,
  "crop_signature": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```