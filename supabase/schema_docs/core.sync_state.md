# core.sync_state

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| table_name | text | NO |  | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| status | text | NO | 'in_progress'::text | NO | NEVER |
| last_success_at | timestamp with time zone | YES |  | NO | NEVER |
| last_seen_most_recent_episode | text | YES |  | NO | NEVER |
| last_error | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

(none)

## Unique Constraints

- table_name, show_id

## Foreign Keys

- show_id -> core.shows.id

## Indexes

- core_sync_state_show_id_idx (non-unique): show_id
- core_sync_state_status_idx (non-unique): status
- core_sync_state_table_name_idx (non-unique): table_name
- sync_state_table_name_show_id_key (unique): table_name, show_id

## RLS Enabled

true

## Example Row

```json
{
  "table_name": "example",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "status": "example",
  "last_success_at": "1970-01-01T00:00:00Z",
  "last_seen_most_recent_episode": "example",
  "last_error": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```