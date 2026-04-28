# core.external_id_conflicts

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| entity_type | text | NO |  | NO | NEVER |
| entity_id | uuid | NO |  | NO | NEVER |
| source_id | text | NO |  | NO | NEVER |
| external_id | text | NO |  | NO | NEVER |
| conflict_reason | text | NO |  | NO | NEVER |
| detected_at | timestamp with time zone | NO | now() | NO | NEVER |
| payload | jsonb | YES |  | NO | NEVER |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

(none)

## Indexes

- external_id_conflicts_pkey (unique): id

## RLS Enabled

true

## Example Row

```json
{
  "entity_type": "example",
  "entity_id": "00000000-0000-0000-0000-000000000000",
  "source_id": "example",
  "external_id": "example",
  "conflict_reason": "example",
  "detected_at": "1970-01-01T00:00:00Z",
  "payload": {},
  "id": "00000000-0000-0000-0000-000000000000"
}
```
