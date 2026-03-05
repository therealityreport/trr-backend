-- Additive claim/lease metadata for remote worker ownership across long-running jobs.

begin;

alter table if exists core.admin_operations
  add column if not exists claimed_by_worker_id text,
  add column if not exists claim_token text,
  add column if not exists lease_expires_at timestamptz,
  add column if not exists heartbeat_at timestamptz,
  add column if not exists attempt_count integer not null default 0,
  add column if not exists next_retry_at timestamptz;

update core.admin_operations
set heartbeat_at = coalesce(heartbeat_at, updated_at, created_at, now())
where heartbeat_at is null;

create index if not exists idx_admin_operations_claim_hotpath
  on core.admin_operations (status, next_retry_at, created_at);

create index if not exists idx_admin_operations_lease_expires_at
  on core.admin_operations (lease_expires_at);

create index if not exists idx_admin_operations_worker_heartbeat
  on core.admin_operations (claimed_by_worker_id, heartbeat_at)
  where claimed_by_worker_id is not null;

alter table if exists core.google_news_sync_jobs
  add column if not exists claimed_by_worker_id text,
  add column if not exists claim_token text,
  add column if not exists lease_expires_at timestamptz,
  add column if not exists attempt_count integer not null default 0,
  add column if not exists next_retry_at timestamptz;

update core.google_news_sync_jobs
set heartbeat_at = coalesce(heartbeat_at, updated_at, created_at, now())
where heartbeat_at is null;

create index if not exists idx_google_news_sync_jobs_claim_hotpath
  on core.google_news_sync_jobs (status, next_retry_at, created_at);

create index if not exists idx_google_news_sync_jobs_lease_expires_at
  on core.google_news_sync_jobs (lease_expires_at);

create index if not exists idx_google_news_sync_jobs_worker_heartbeat
  on core.google_news_sync_jobs (claimed_by_worker_id, heartbeat_at)
  where claimed_by_worker_id is not null;

alter table if exists social.reddit_refresh_runs
  add column if not exists claimed_by_worker_id text,
  add column if not exists claim_token text,
  add column if not exists lease_expires_at timestamptz,
  add column if not exists heartbeat_at timestamptz,
  add column if not exists attempt_count integer not null default 0,
  add column if not exists next_retry_at timestamptz;

update social.reddit_refresh_runs
set heartbeat_at = coalesce(heartbeat_at, updated_at, created_at, now())
where heartbeat_at is null;

create index if not exists idx_reddit_refresh_runs_claim_hotpath
  on social.reddit_refresh_runs (status, next_retry_at, created_at);

create index if not exists idx_reddit_refresh_runs_lease_expires_at
  on social.reddit_refresh_runs (lease_expires_at);

create index if not exists idx_reddit_refresh_runs_worker_heartbeat
  on social.reddit_refresh_runs (claimed_by_worker_id, heartbeat_at)
  where claimed_by_worker_id is not null;

commit;
