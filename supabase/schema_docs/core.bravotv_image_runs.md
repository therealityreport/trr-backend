# core.bravotv_image_runs

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| operation_id | uuid | YES |  | NO | NEVER |
| mode | text | NO |  | NO | NEVER |
| status | text | NO |  | NO | NEVER |
| target_show_id | uuid | YES |  | NO | NEVER |
| target_person_id | uuid | YES |  | NO | NEVER |
| show_name | text | YES |  | NO | NEVER |
| person_name | text | YES |  | NO | NEVER |
| season | integer | YES |  | NO | NEVER |
| episode | integer | YES |  | NO | NEVER |
| selected_sources | jsonb | NO | '[]'::jsonb | NO | NEVER |
| refreshed_artifacts | jsonb | NO | '[]'::jsonb | NO | NEVER |
| artifact_paths | jsonb | NO | '{}'::jsonb | NO | NEVER |
| request_payload | jsonb | NO | '{}'::jsonb | NO | NEVER |
| manifest | jsonb | NO | '{}'::jsonb | NO | NEVER |
| summary | jsonb | NO | '{}'::jsonb | NO | NEVER |
| import_summary | jsonb | NO | '{}'::jsonb | NO | NEVER |
| review_summary | jsonb | NO | '{}'::jsonb | NO | NEVER |
| created_by | text | YES |  | NO | NEVER |
| error_detail | text | YES |  | NO | NEVER |
| started_at | timestamp with time zone | YES |  | NO | NEVER |
| completed_at | timestamp with time zone | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- operation_id -> core.admin_operations.id
- target_person_id -> core.people.id
- target_show_id -> core.shows.id

## Indexes

- bravotv_image_runs_pkey (unique): id
- idx_bravotv_image_runs_mode_created_at (non-unique): mode, created_at DESC
- idx_bravotv_image_runs_operation_id (unique): operation_id) WHERE (operation_id IS NOT NULL
- idx_bravotv_image_runs_person_created_at (non-unique): target_person_id, created_at DESC) WHERE (target_person_id IS NOT NULL
- idx_bravotv_image_runs_show_created_at (non-unique): target_show_id, created_at DESC) WHERE (target_show_id IS NOT NULL
- idx_bravotv_image_runs_status_created_at (non-unique): status, created_at DESC

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "operation_id": "00000000-0000-0000-0000-000000000000",
  "mode": "example",
  "status": "example",
  "target_show_id": "00000000-0000-0000-0000-000000000000",
  "target_person_id": "00000000-0000-0000-0000-000000000000",
  "show_name": "example",
  "person_name": "example",
  "season": 0,
  "episode": 0,
  "selected_sources": {},
  "refreshed_artifacts": {},
  "artifact_paths": {},
  "request_payload": {},
  "manifest": {},
  "summary": {},
  "import_summary": {},
  "review_summary": {},
  "created_by": "example",
  "error_detail": "example",
  "started_at": "1970-01-01T00:00:00Z",
  "completed_at": "1970-01-01T00:00:00Z",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z"
}
```