begin;

-- Additive: retain target evidence with its run; never reinterpret legacy jobs.
create table social.instagram_detail_run_targets (
  run_id uuid not null references social.scrape_runs(id) on delete cascade,
  source_id text not null,
  target_key text not null,
  account_handle text not null,
  required_mask jsonb not null,
  policy_version integer not null,
  state text not null default 'pending' check (state in
    ('pending', 'leased', 'committed', 'cached_satisfied', 'source_unavailable', 'retry_wait', 'failed')),
  attempt_count integer not null default 0 check (attempt_count >= 0),
  request_count integer not null default 0 check (request_count >= 0),
  next_attempt_at timestamptz,
  lease_owner text,
  lease_job_id uuid references social.scrape_jobs(id) on delete set null,
  lease_generation bigint not null default 0,
  lease_expires_at timestamptz,
  fetched_at timestamptz,
  committed_at timestamptz,
  error_code text,
  error_summary text,
  phase_elapsed_ms jsonb not null default '{}'::jsonb,
  primary key (run_id, source_id),
  unique (run_id, target_key),
  check (state <> 'committed' or (fetched_at is not null and committed_at is not null))
);
create index instagram_detail_targets_ready_idx
  on social.instagram_detail_run_targets (run_id, state, next_attempt_at, target_key);
create index instagram_detail_targets_lease_idx
  on social.instagram_detail_run_targets (lease_expires_at) where state = 'leased';
alter table social.instagram_detail_run_targets enable row level security;
revoke all on social.instagram_detail_run_targets from anon, authenticated;
grant all on social.instagram_detail_run_targets to service_role;

-- These fields are deliberately separate from mutable dispatch diagnostics.
alter table social.scrape_jobs
  add column detail_dispatch_generation bigint not null default 0,
  add column detail_dispatch_token text,
  add column detail_dispatch_expires_at timestamptz,
  add column detail_completion_receipt jsonb;

alter table social.social_post_observations
  add column detail_run_id uuid references social.scrape_runs(id) on delete set null,
  add column detail_source_id text;
create unique index social_post_observations_detail_target_idx
  on social.social_post_observations(detail_run_id, detail_source_id)
  where detail_run_id is not null;

commit;
