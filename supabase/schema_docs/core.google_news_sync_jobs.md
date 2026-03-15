# core.google_news_sync_jobs

## Columns

| name | type | nullable | default | identity | generated |
| --- | --- | --- | --- | --- | --- |
| id | uuid | NO | gen_random_uuid() | NO | NEVER |
| show_id | uuid | NO |  | NO | NEVER |
| source_id | text | NO | 'google_news'::text | NO | NEVER |
| status | text | NO |  | NO | NEVER |
| requested_async | boolean | NO | false | NO | NEVER |
| force | boolean | NO | false | NO | NEVER |
| requested_by | text | YES |  | NO | NEVER |
| result | jsonb | NO | '{}'::jsonb | NO | NEVER |
| error | text | YES |  | NO | NEVER |
| created_at | timestamp with time zone | NO | now() | NO | NEVER |
| started_at | timestamp with time zone | YES |  | NO | NEVER |
| finished_at | timestamp with time zone | YES |  | NO | NEVER |
| updated_at | timestamp with time zone | NO | now() | NO | NEVER |
| heartbeat_at | timestamp with time zone | YES |  | NO | NEVER |
| claimed_by_worker_id | text | YES |  | NO | NEVER |
| claim_token | text | YES |  | NO | NEVER |
| lease_expires_at | timestamp with time zone | YES |  | NO | NEVER |
| attempt_count | integer | NO | 0 | NO | NEVER |
| next_retry_at | timestamp with time zone | YES |  | NO | NEVER |

## Primary Key

id

## Unique Constraints

(none)

## Foreign Keys

- show_id -> core.shows.id

## Indexes

- google_news_sync_jobs_pkey (unique): id
- idx_google_news_sync_jobs_claim_hotpath (non-unique): status, next_retry_at, created_at
- idx_google_news_sync_jobs_lease_expires_at (non-unique): lease_expires_at
- idx_google_news_sync_jobs_show_created (non-unique): show_id, created_at DESC
- idx_google_news_sync_jobs_status (non-unique): status, updated_at DESC
- idx_google_news_sync_jobs_status_heartbeat (non-unique): status, COALESCE(heartbeat_at, updated_at) DESC
- idx_google_news_sync_jobs_worker_heartbeat (non-unique): claimed_by_worker_id, heartbeat_at) WHERE (claimed_by_worker_id IS NOT NULL

## RLS Enabled

false

## Example Row

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "show_id": "00000000-0000-0000-0000-000000000000",
  "source_id": "example",
  "status": "example",
  "requested_async": false,
  "force": false,
  "requested_by": "example",
  "result": {},
  "error": "example",
  "created_at": "1970-01-01T00:00:00Z",
  "started_at": "1970-01-01T00:00:00Z",
  "finished_at": "1970-01-01T00:00:00Z",
  "updated_at": "1970-01-01T00:00:00Z",
  "heartbeat_at": "1970-01-01T00:00:00Z",
  "claimed_by_worker_id": "example",
  "claim_token": "example",
  "lease_expires_at": "1970-01-01T00:00:00Z",
  "attempt_count": 0,
  "next_retry_at": "1970-01-01T00:00:00Z"
}
```