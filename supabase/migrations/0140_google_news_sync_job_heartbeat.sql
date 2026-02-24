begin;

alter table if exists core.google_news_sync_jobs
    add column if not exists heartbeat_at timestamptz;

update core.google_news_sync_jobs
set heartbeat_at = coalesce(heartbeat_at, updated_at, created_at, now())
where heartbeat_at is null;

create index if not exists idx_google_news_sync_jobs_status_heartbeat
    on core.google_news_sync_jobs (status, (coalesce(heartbeat_at, updated_at)) desc);

commit;
