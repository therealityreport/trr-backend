begin;

create table if not exists social.shared_account_run_partitions (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references social.scrape_runs (id) on delete cascade,
  platform text not null
    check (platform in ('instagram', 'tiktok', 'twitter', 'threads')),
  account_handle text not null,
  partition_strategy text not null
    check (partition_strategy in ('date_window', 'cursor_breakpoints')),
  partition_key text not null,
  shard_index integer not null check (shard_index >= 0),
  shard_total integer not null check (shard_total >= 1),
  cursor_start text,
  cursor_end text,
  boundary_start_at timestamptz,
  boundary_end_at timestamptz,
  status text not null default 'discovered'
    check (status in ('discovered', 'queued', 'running', 'completed', 'failed', 'cancelled')),
  job_id uuid references social.scrape_jobs (id) on delete set null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, partition_key)
);

create index if not exists shared_account_run_partitions_lookup_idx
  on social.shared_account_run_partitions (run_id, platform, account_handle, shard_index);

create index if not exists shared_account_run_partitions_status_idx
  on social.shared_account_run_partitions (run_id, status, platform);

grant select on table social.shared_account_run_partitions to anon, authenticated;
grant all privileges on table social.shared_account_run_partitions to service_role;

alter table social.shared_account_run_partitions enable row level security;

drop policy if exists shared_account_run_partitions_public_read on social.shared_account_run_partitions;
create policy shared_account_run_partitions_public_read on social.shared_account_run_partitions
for select to anon, authenticated using (true);

commit;
