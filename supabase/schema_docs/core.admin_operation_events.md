# core.admin_operation_events

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | bigint | NO |  | YES | NEVER |
| operation_id | uuid | NO |  | NO | NEVER |
| event_seq | bigint | YES |  | NO | NEVER |
| event_type | text | NO |  | NO | NEVER |
| event_payload | jsonb | NO | '{}'::jsonb | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

- operation_id, event_seq

## Foreign Keys

- operation_id -> core.admin_operations.id

## Indexes

- admin_operation_events_op_seq_unique (unique): operation_id, event_seq
- admin_operation_events_pkey (unique): id
- idx_admin_operation_events_created_at (non-unique): created_at
- idx_admin_operation_events_operation_seq (non-unique): operation_id, event_seq

## RLS Enabled

false

## Example Row

```json
{
  "id": 0,
  "operation_id": "00000000-0000-0000-0000-000000000000",
  "event_seq": 0,
  "event_type": "example",
  "event_payload": {},
  "created_at": "1970-01-01T00:00:00Z"
}
```