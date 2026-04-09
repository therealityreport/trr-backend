# core.admin_operations

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| operation_type | text | NO |  | NO | NEVER |
| status | text | NO |  | NO | NEVER |
| initiated_by | text | YES |  | NO | NEVER |
| request_id | text | YES |  | NO | NEVER |
| client_session_id | text | YES |  | NO | NEVER |
| client_workflow_id | text | YES |  | NO | NEVER |
| request_payload | jsonb | NO | '{}'::jsonb | NO | NEVER |
| progress_payload | jsonb | NO | '{}'::jsonb | NO | NEVER |
| result_payload | jsonb | YES |  | NO | NEVER |
| error_payload | jsonb | YES |  | NO | NEVER |
| cancel_requested_at | timestamp with time zone | YES |  | NO | NEVER |
| started_at | timestamp with time zone | YES |  | NO | NEVER |
| completed_at | timestamp with time zone | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| claimed_by_worker_id | text | YES |  | NO | NEVER |
| claim_token | text | YES |  | NO | NEVER |
| lease_expires_at | timestamp with time zone | YES |  | NO | NEVER |
| heartbeat_at | timestamp with time zone | YES |  | NO | NEVER |
| attempt_count | integer | NO | 0 | NO | NEVER |
| next_retry_at | timestamp with time zone | YES |  | NO | NEVER |
| parent_operation_id | uuid | YES |  | NO | NEVER |
| refresh_target | text | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- parent_operation_id -> core.admin_operations.id

## Indexes

- admin_operations_pkey (unique): id
- idx_admin_operations_claim_hotpath (non-unique): status, next_retry_at, created_at
- idx_admin_operations_client_session_created_at (non-unique): client_session_id, created_at DESC) WHERE (client_session_id IS NOT NULL
- idx_admin_operations_client_workflow_status (non-unique): client_session_id, client_workflow_id, status, created_at DESC) WHERE ((client_session_id IS NOT NULL) AND (client_workflow_id IS NOT NULL)
- idx_admin_operations_lease_expires_at (non-unique): lease_expires_at
- idx_admin_operations_operation_type_created_at (non-unique): operation_type, created_at DESC
- idx_admin_operations_parent_id (non-unique): parent_operation_id, created_at) WHERE (parent_operation_id IS NOT NULL
- idx_admin_operations_parent_status (non-unique): parent_operation_id, status) WHERE (parent_operation_id IS NOT NULL
- idx_admin_operations_status_created_at (non-unique): status, created_at DESC
- idx_admin_operations_worker_heartbeat (non-unique): claimed_by_worker_id, heartbeat_at) WHERE (claimed_by_worker_id IS NOT NULL

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "operation_type": "example",
  "status": "example",
  "initiated_by": "example",
  "request_id": "example",
  "client_session_id": "example",
  "client_workflow_id": "example",
  "request_payload": {},
  "progress_payload": {},
  "result_payload": {},
  "error_payload": {},
  "cancel_requested_at": "1970-01-01T00:00:00Z",
  "started_at": "1970-01-01T00:00:00Z",
  "completed_at": "1970-01-01T00:00:00Z",
  "created_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "claimed_by_worker_id": "example",
  "claim_token": "example",
  "lease_expires_at": "1970-01-01T00:00:00Z",
  "heartbeat_at": "1970-01-01T00:00:00Z",
  "attempt_count": 0,
  "next_retry_at": "1970-01-01T00:00:00Z",
  "parent_operation_id": "00000000-0000-0000-0000-000000000000",
  "refresh_target": "example"
}
```