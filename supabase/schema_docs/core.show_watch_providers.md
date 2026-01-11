# core.show_watch_providers

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| show_id | uuid | NO |  | NO | NEVER |
| region | text | NO |  | NO | NEVER |
| offer_type | text | NO |  | NO | NEVER |
| provider_id | integer | NO |  | NO | NEVER |
| display_priority | integer | YES |  | NO | NEVER |
| link | text | YES |  | NO | NEVER |
| fetched_at | timestamp with time zone | NO | now() | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

show_id, region, offer_type, provider_id

## Unique Constraints

(none)

## Foreign Keys

- provider_id -> core.watch_providers.provider_id
- show_id -> core.shows.id

## Indexes

- show_watch_providers_offer_type_idx (non-unique): offer_type
- show_watch_providers_pkey (unique): show_id, region, offer_type, provider_id
- show_watch_providers_provider_id_idx (non-unique): provider_id
- show_watch_providers_region_idx (non-unique): region
- show_watch_providers_show_id_idx (non-unique): show_id

## RLS Enabled

true

## Example Row

```json
{
  "show_id": "00000000-0000-0000-0000-000000000000",
  "region": "example",
  "offer_type": "example",
  "provider_id": 0,
  "display_priority": 0,
  "link": "example",
  "fetched_at": "1970-01-01T00:00:00Z",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```