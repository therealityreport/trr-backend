begin;

create table if not exists social.shared_account_run_frontiers (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references social.scrape_runs (id) on delete cascade,
  platform text not null
    check (platform in ('instagram', 'tiktok', 'twitter', 'threads')),
  account_handle text not null,
  strategy text not null,
  status text not null default 'queued'
    check (status in ('queued', 'running', 'retrying', 'completed', 'failed', 'cancelled')),
  next_cursor text,
  total_posts integer not null default 0 check (total_posts >= 0),
  posts_checked integer not null default 0 check (posts_checked >= 0),
  posts_saved integer not null default 0 check (posts_saved >= 0),
  pages_scanned integer not null default 0 check (pages_scanned >= 0),
  last_transport text,
  lease_owner text,
  lease_expires_at timestamptz,
  retry_count integer not null default 0 check (retry_count >= 0),
  exhausted boolean not null default false,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, platform, account_handle)
);

create index if not exists shared_account_run_frontiers_lookup_idx
  on social.shared_account_run_frontiers (run_id, platform, account_handle);

create index if not exists shared_account_run_frontiers_status_idx
  on social.shared_account_run_frontiers (run_id, status, platform);

grant select on table social.shared_account_run_frontiers to anon, authenticated;
grant all privileges on table social.shared_account_run_frontiers to service_role;

alter table social.shared_account_run_frontiers enable row level security;

drop policy if exists shared_account_run_frontiers_public_read on social.shared_account_run_frontiers;
create policy shared_account_run_frontiers_public_read on social.shared_account_run_frontiers
for select to anon, authenticated using (true);

commit;
