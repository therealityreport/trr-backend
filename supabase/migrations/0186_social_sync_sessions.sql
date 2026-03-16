create table if not exists social.sync_sessions (
  id uuid primary key default gen_random_uuid(),
  season_id uuid not null references core.seasons(id) on delete cascade,
  show_id uuid not null references core.shows(id) on delete cascade,
  source_scope text not null default 'bravo',
  platforms text[] not null default '{instagram}',
  date_start timestamptz not null,
  date_end timestamptz not null,
  dedup_key text not null,
  status text not null default 'initializing'
    check (
      status in (
        'initializing',
        'pass_running',
        'pass_evaluating',
        'completing',
        'completed',
        'failed',
        'cancelling',
        'cancelled'
      )
    ),
  current_pass_kind text
    check (current_pass_kind in ('posts_and_comments', 'comments_only', 'details_refresh')),
  current_pass_attempt int not null default 1,
  current_run_id uuid,
  pass_sequence int not null default 0,
  sync_config jsonb not null default '{}'::jsonb,
  pass_history jsonb not null default '[]'::jsonb,
  completeness_snapshot jsonb not null default '{}'::jsonb,
  follow_up_reason text,
  max_attempts_per_kind int not null default 3,
  retry_base_delay_seconds int not null default 30,
  next_pass_available_at timestamptz,
  initiated_by text,
  client_session_id text,
  client_workflow_id text,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  cancelled_at timestamptz,
  updated_at timestamptz not null default now()
);

create unique index if not exists sync_sessions_dedup_key_active_idx
  on social.sync_sessions (season_id, dedup_key)
  where status in ('initializing', 'pass_running', 'pass_evaluating', 'completing', 'cancelling');

create index if not exists sync_sessions_current_run_idx on social.sync_sessions (current_run_id);
create index if not exists sync_sessions_status_next_pass_idx
  on social.sync_sessions (status, next_pass_available_at, created_at desc);

alter table social.scrape_runs
  add column if not exists sync_session_id uuid,
  add column if not exists pass_kind text,
  add column if not exists pass_attempt int,
  add column if not exists pass_sequence int;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'scrape_runs_sync_session_id_fkey'
  ) then
    alter table social.scrape_runs
      add constraint scrape_runs_sync_session_id_fkey
      foreign key (sync_session_id) references social.sync_sessions(id) on delete set null;
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'sync_sessions_current_run_id_fkey'
  ) then
    alter table social.sync_sessions
      add constraint sync_sessions_current_run_id_fkey
      foreign key (current_run_id) references social.scrape_runs(id) on delete set null;
  end if;
end $$;

create index if not exists scrape_runs_sync_session_id_idx on social.scrape_runs (sync_session_id, created_at desc);
