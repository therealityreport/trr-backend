# core.show_source_history

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | bigint | NO | nextval('core.show_source_history_id_seq'::regclass) | NO | NEVER |
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

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- show_id -> core.shows.id
- source_id -> core.sources.id

## Indexes

- core_show_source_history_source_id_idx (non-unique): source_id
- show_source_history_lookup_idx (non-unique): show_id, source_id, variant, fetched_at DESC
- show_source_history_pkey (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "id": 0,
  "show_id": "00000000-0000-0000-0000-000000000000",
  "source_id": "example",
  "variant": "example",
  "fetched_at": "1970-01-01T00:00:00Z",
  "fetch_method": "example",
  "status": "example",
  "error": "example",
  "payload": {},
  "payload_sha256": "example",
  "created_at": "1970-01-01T00:00:00Z"
}
```