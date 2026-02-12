begin;

create index if not exists scrape_jobs_queue_claim_idx
  on social.scrape_jobs (status, available_at, priority, created_at)
  where status in ('queued', 'retrying');

create index if not exists scrape_jobs_run_created_idx
  on social.scrape_jobs (run_id, created_at desc)
  where run_id is not null;

create index if not exists scrape_jobs_worker_heartbeat_idx
  on social.scrape_jobs (worker_id, heartbeat_at)
  where worker_id is not null;

commit;
