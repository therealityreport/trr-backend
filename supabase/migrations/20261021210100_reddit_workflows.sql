begin;
-- Expand only. No enrollment/backfill, scheduler activation, or paid analysis.
-- Rollback: disable dispatch, retain these tables until queued jobs and budget
-- reservations are reconciled. No existing capture schema is removed or rewritten.
create table if not exists social.reddit_workflow_settings (
  community_id uuid not null,
  season_id uuid not null references core.seasons(id) on delete cascade,
  configuration jsonb not null default '{"schedule_enabled":false,"ai_enabled":false}'::jsonb,
  revision bigint not null default 1,
  updated_at timestamptz not null default now(),
  primary key(community_id,season_id)
);
create table if not exists social.reddit_workflow_jobs (
  id uuid primary key default gen_random_uuid(),
  community_id uuid not null,
  season_id uuid not null references core.seasons(id) on delete cascade,
  kind text not null check(kind in ('deterministic_analysis','ai_analysis','lifecycle','schedule_capture')),
  state text not null default 'queued' check(state in ('queued','running','completed','failed')),
  payload jsonb not null default '{}'::jsonb,
  result jsonb,
  error text,
  idempotency_key text,
  claim_token uuid,
  lease_expires_at timestamptz,
  next_attempt_at timestamptz not null default now(),
  attempt_count integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(community_id,season_id,kind,idempotency_key)
);
create index if not exists reddit_workflow_jobs_due_idx
  on social.reddit_workflow_jobs(next_attempt_at,created_at) where state in ('queued','running');
create table if not exists social.reddit_lifecycle_enrollment (
  reddit_post_id text primary key references social.reddit_posts(reddit_post_id) on delete cascade,
  run_id uuid references social.reddit_refresh_runs(id) on delete set null,
  reason text not null check(reason in ('new_capture','manual_legacy')),
  next_check_at timestamptz not null default now(),
  last_checked_at timestamptz,
  state text not null default 'active' check(state in ('active','deleted','removed')),
  evidence jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);
create index if not exists reddit_lifecycle_due_idx
  on social.reddit_lifecycle_enrollment(next_check_at) where state='active';
create table if not exists social.reddit_comment_tombstones (
  id text primary key,
  signal text not null check(signal in ('deleted','removed')),
  evidence jsonb not null default '{}'::jsonb,
  deleted_at timestamptz not null default now()
);
create table if not exists social.reddit_author_tombstones (
  author_hash text primary key,
  evidence jsonb not null default '{}'::jsonb,
  deleted_at timestamptz not null default now()
);
create table if not exists social.reddit_analysis_results (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null unique references social.reddit_workflow_jobs(id) on delete cascade,
  community_id uuid not null,
  season_id uuid not null references core.seasons(id) on delete cascade,
  kind text not null,
  input_revision text not null,
  output_revision text not null,
  source_post_ids text[] not null default '{}',
  result jsonb not null,
  valid boolean not null default true,
  invalidated_at timestamptz,
  created_at timestamptz not null default now()
);
create index if not exists reddit_analysis_scope_idx
  on social.reddit_analysis_results(community_id,season_id,created_at desc);
create index if not exists reddit_analysis_cache_idx
  on social.reddit_analysis_results(community_id,season_id,kind,input_revision) where valid;
create index if not exists reddit_analysis_sources_idx
  on social.reddit_analysis_results using gin(source_post_ids);
create table if not exists social.reddit_ai_daily_budgets (
  community_id uuid not null,
  season_id uuid not null references core.seasons(id) on delete cascade,
  budget_day date not null,
  reserved_tokens bigint not null default 0 check(reserved_tokens>=0),
  used_tokens bigint not null default 0 check(used_tokens>=0),
  primary key(community_id,season_id,budget_day)
);
create table if not exists social.reddit_ai_reservations (
  job_id uuid primary key references social.reddit_workflow_jobs(id) on delete cascade,
  community_id uuid not null,
  season_id uuid not null,
  budget_day date not null,
  reserved_tokens integer not null check(reserved_tokens>0),
  used_tokens integer,
  state text not null default 'reserved' check(state in ('reserved','reconciled','uncertain')),
  updated_at timestamptz not null default now(),
  foreign key(community_id,season_id,budget_day)
    references social.reddit_ai_daily_budgets(community_id,season_id,budget_day)
);
create table if not exists social.reddit_reclassification_previews (
  id uuid primary key default gen_random_uuid(),
  community_id uuid not null,
  season_id uuid not null references core.seasons(id) on delete cascade,
  source_revision text not null,
  settings_revision bigint not null,
  proposal jsonb not null,
  classification_summary jsonb not null default '{}'::jsonb,
  applied_at timestamptz,
  expires_at timestamptz not null default now()+interval '1 hour',
  created_at timestamptz not null default now()
);
create table if not exists social.reddit_object_deletion_outbox (
  hosted_key text primary key,
  source_post_ids text[] not null default '{}',
  state text not null default 'pending' check(state in ('pending','deleted','referenced')),
  attempt_count integer not null default 0,
  error text,
  updated_at timestamptz not null default now()
);
create index if not exists reddit_object_deletion_scope_idx
  on social.reddit_object_deletion_outbox using gin(source_post_ids);
create index if not exists reddit_object_deletion_pending_idx
  on social.reddit_object_deletion_outbox(updated_at) where state='pending';
do $$ declare tab text; begin
  foreach tab in array array['reddit_workflow_settings','reddit_workflow_jobs',
    'reddit_lifecycle_enrollment','reddit_comment_tombstones','reddit_author_tombstones','reddit_analysis_results','reddit_ai_daily_budgets',
    'reddit_ai_reservations','reddit_reclassification_previews','reddit_object_deletion_outbox']
  loop
    execute format('alter table social.%I enable row level security',tab);
    execute format('revoke all on social.%I from anon, authenticated',tab);
    execute format('grant all on social.%I to service_role',tab);
  end loop;
end $$;
commit;
