begin;

alter table social.scrape_runs
  add column if not exists total_jobs integer not null default 0,
  add column if not exists completed_jobs integer not null default 0,
  add column if not exists failed_jobs integer not null default 0,
  add column if not exists active_jobs integer not null default 0,
  add column if not exists items_found_total integer not null default 0,
  add column if not exists stage_counts jsonb not null default '{}'::jsonb;

with per_run as (
  select
    j.run_id,
    count(*)::int as total_jobs,
    count(*) filter (where j.status = 'completed')::int as completed_jobs,
    count(*) filter (where j.status = 'failed')::int as failed_jobs,
    count(*) filter (where j.status in ('queued', 'pending', 'retrying', 'running'))::int as active_jobs,
    coalesce(sum(j.items_found), 0)::int as items_found_total
  from social.scrape_jobs j
  where j.run_id is not null
  group by j.run_id
),
per_run_stage as (
  select
    j.run_id,
    coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type, 'unknown') as stage,
    count(*)::int as total,
    count(*) filter (where j.status = 'completed')::int as completed,
    count(*) filter (where j.status = 'failed')::int as failed,
    count(*) filter (where j.status in ('queued', 'pending', 'retrying', 'running'))::int as active
  from social.scrape_jobs j
  where j.run_id is not null
  group by j.run_id, coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type, 'unknown')
),
per_run_stage_json as (
  select
    prs.run_id,
    coalesce(
      jsonb_object_agg(
        prs.stage,
        jsonb_build_object(
          'total', prs.total,
          'completed', prs.completed,
          'failed', prs.failed,
          'active', prs.active
        )
      ),
      '{}'::jsonb
    ) as stage_counts
  from per_run_stage prs
  group by prs.run_id
)
update social.scrape_runs r
set
  total_jobs = coalesce(pr.total_jobs, 0),
  completed_jobs = coalesce(pr.completed_jobs, 0),
  failed_jobs = coalesce(pr.failed_jobs, 0),
  active_jobs = coalesce(pr.active_jobs, 0),
  items_found_total = coalesce(pr.items_found_total, 0),
  stage_counts = coalesce(prs.stage_counts, '{}'::jsonb),
  summary = jsonb_build_object(
    'total_jobs', coalesce(pr.total_jobs, 0),
    'completed_jobs', coalesce(pr.completed_jobs, 0),
    'failed_jobs', coalesce(pr.failed_jobs, 0),
    'active_jobs', coalesce(pr.active_jobs, 0),
    'items_found_total', coalesce(pr.items_found_total, 0),
    'stage_counts', coalesce(prs.stage_counts, '{}'::jsonb)
  )
from per_run pr
left join per_run_stage_json prs on prs.run_id = pr.run_id
where r.id = pr.run_id;

update social.scrape_runs r
set
  summary = jsonb_build_object(
    'total_jobs', coalesce(r.total_jobs, 0),
    'completed_jobs', coalesce(r.completed_jobs, 0),
    'failed_jobs', coalesce(r.failed_jobs, 0),
    'active_jobs', coalesce(r.active_jobs, 0),
    'items_found_total', coalesce(r.items_found_total, 0),
    'stage_counts', coalesce(r.stage_counts, '{}'::jsonb)
  )
where r.id not in (
  select distinct run_id from social.scrape_jobs where run_id is not null
);

create index if not exists scrape_jobs_claim_fairness_idx
  on social.scrape_jobs (status, available_at, run_id, priority, created_at)
  where status in ('queued', 'pending', 'retrying');

create index if not exists scrape_jobs_running_run_heartbeat_idx
  on social.scrape_jobs (run_id, heartbeat_at)
  where status = 'running' and run_id is not null;

commit;
