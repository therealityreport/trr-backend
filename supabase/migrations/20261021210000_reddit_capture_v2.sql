begin;

-- Additive rollout: no legacy run or post is enrolled or reclassified here.
alter table social.reddit_refresh_runs
  add column if not exists contract_version integer not null default 1,
  add column if not exists config_hash text,
  add column if not exists control_state text not null default 'active'
    check (control_state in ('active','paused','cancel_requested','cancelled')),
  add column if not exists parent_run_id uuid references social.reddit_refresh_runs(id),
  add column if not exists priority integer not null default 10;
create unique index if not exists reddit_active_config_v2_idx
  on social.reddit_refresh_runs(community_id, season_id, period_key, config_hash)
  where contract_version = 2 and status in ('queued','running');

create table if not exists social.reddit_run_manifests (
  run_id uuid primary key references social.reddit_refresh_runs(id) on delete cascade,
  version integer not null default 2,
  matching_version integer not null default 1,
  configuration jsonb not null,
  requested_phases jsonb not null,
  coverage jsonb not null default '{"state":"unknown"}'::jsonb,
  created_at timestamptz not null default now()
);
create table if not exists social.reddit_work_targets (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references social.reddit_refresh_runs(id) on delete cascade,
  kind text not null check (kind in ('discovery','comments','media')),
  target_key text not null,
  reddit_post_id text,
  state text not null default 'pending' check (state in
    ('pending','running','retry_wait','committed','source_unavailable','failed','cancelled')),
  required boolean not null default true,
  attempt_count integer not null default 0 check (attempt_count >= 0),
  next_retry_at timestamptz,
  checkpoint jsonb not null default '{}'::jsonb,
  result jsonb not null default '{}'::jsonb,
  error text,
  attempted_at timestamptz,
  committed_at timestamptz,
  updated_at timestamptz not null default now(),
  unique(run_id,kind,target_key),
  check(state <> 'committed' or committed_at is not null)
);
create index if not exists reddit_work_targets_due_idx
  on social.reddit_work_targets(run_id,state,next_retry_at);
create table if not exists social.reddit_capture_state (
  reddit_post_id text not null references social.reddit_posts(reddit_post_id) on delete cascade,
  phase text not null check(phase in ('comments','media')),
  state text not null check(state in
    ('pending','running','retry_wait','committed','source_unavailable','failed','cancelled')),
  last_run_id uuid references social.reddit_refresh_runs(id) on delete set null,
  source_revision text,
  evidence jsonb not null default '{}'::jsonb,
  attempted_at timestamptz,
  successful_at timestamptz,
  next_retry_at timestamptz,
  updated_at timestamptz not null default now(),
  primary key(reddit_post_id,phase)
);
create table if not exists social.reddit_assets (
  id uuid primary key default gen_random_uuid(),
  source_url text not null unique,
  media_type text not null,
  state text not null default 'pending' check(state in ('pending','mirrored','failed','source_unavailable')),
  hosted_key text,
  hosted_url text,
  sha256 text,
  content_type text,
  size_bytes bigint,
  error text,
  updated_at timestamptz not null default now()
);
create table if not exists social.reddit_asset_links (
  asset_id uuid not null references social.reddit_assets(id) on delete cascade,
  reddit_post_id text not null references social.reddit_posts(reddit_post_id) on delete cascade,
  reddit_comment_id text references social.reddit_comments(reddit_comment_id) on delete cascade,
  association_key text not null,
  primary key(asset_id,association_key)
);
create table if not exists social.reddit_dispatch_outbox (
  run_id uuid primary key references social.reddit_refresh_runs(id) on delete cascade,
  state text not null default 'pending' check(state in ('pending','dispatching','dispatched','done')),
  attempt_count integer not null default 0,
  next_attempt_at timestamptz not null default now(),
  dispatch_token uuid,
  dispatched_at timestamptz,
  error text,
  updated_at timestamptz not null default now()
);
create table if not exists social.reddit_request_budgets (
  identity_key text primary key,
  window_started_at timestamptz not null default now(),
  used integer not null default 0,
  not_before timestamptz,
  provider_remaining numeric,
  provider_reset_at timestamptz,
  blocked boolean not null default false,
  updated_at timestamptz not null default now()
);
create table if not exists social.reddit_request_leases (
  id uuid primary key,
  identity_key text not null references social.reddit_request_budgets(identity_key),
  lane text not null,
  expires_at timestamptz not null
);
create index if not exists reddit_request_leases_identity_idx
  on social.reddit_request_leases(identity_key,expires_at);

create table if not exists social.reddit_episode_discovery_jobs (
  id uuid primary key default gen_random_uuid(),
  community_id uuid not null,
  config_hash text not null,
  payload jsonb not null,
  checkpoint jsonb not null default '{}'::jsonb,
  result jsonb,
  state text not null default 'queued' check(state in ('queued','running','completed','failed')),
  next_attempt_at timestamptz not null default now(),
  claim_token uuid,
  lease_expires_at timestamptz,
  error text,
  attempt_count integer not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists reddit_episode_active_config_idx
  on social.reddit_episode_discovery_jobs(community_id,config_hash) where state in ('queued','running');

-- Numeric observations permit run comparisons without retaining deleted source text.
create table if not exists social.reddit_run_post_observations (
  run_id uuid not null references social.reddit_refresh_runs(id) on delete cascade,
  reddit_post_id text not null,
  score integer,
  num_comments integer,
  saved_comments integer not null default 0,
  comments_state text,
  media_state text,
  source_revision text,
  observed_at timestamptz not null default now(),
  primary key(run_id,reddit_post_id)
);

do $$ declare tab text; begin
  foreach tab in array array['reddit_run_manifests','reddit_work_targets','reddit_capture_state',
    'reddit_assets','reddit_asset_links','reddit_dispatch_outbox','reddit_request_budgets','reddit_request_leases',
    'reddit_episode_discovery_jobs','reddit_run_post_observations']
  loop
    execute format('alter table social.%I enable row level security',tab);
    execute format('revoke all on social.%I from anon, authenticated',tab);
    execute format('grant all on social.%I to service_role',tab);
  end loop;
end $$;

-- Operational records are served through authenticated admin APIs.
drop policy if exists reddit_refresh_runs_public_read on social.reddit_refresh_runs;
revoke select on social.reddit_refresh_runs from anon, authenticated;
commit;
