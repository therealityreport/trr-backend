# core.show_source_latest

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| show_id | uuid | NO |  | NO | NEVER |
| source_id | text | NO |  | NO | NEVER |
| variant | text | NO | 'default'::text | NO | NEVER |
| fetched_at | timestamp with time zone | NO |  | NO | NEVER |
| fetch_method | text | YES |  | NO | NEVER |
| status | text | NO | 'success'::text | NO | NEVER |
| error | text | YES |  | NO | NEVER |
| payload | jsonb | NO |  | NO | NEVER |
| payload_sha256 | text | NO |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

(none)

## Unique Constraints

- show_id, source_id, variant

## Foreign Keys

- show_id -> core.shows.id
- source_id -> core.sources.id

## Indexes

- show_source_latest_show_id_source_id_variant_key (unique): show_id, source_id, variant

## RLS Enabled

true

## Example Row

```json
{
  "show_id": "00000000-0000-0000-0000-000000000000",
  "source_id": "example",
  "variant": "example",
  "fetched_at": "1970-01-01T00:00:00Z",
  "fetch_method": "example",
  "status": "example",
  "error": "example",
  "payload": {},
  "payload_sha256": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```