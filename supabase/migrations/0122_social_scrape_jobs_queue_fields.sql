begin;

alter table social.scrape_jobs
  add column if not exists run_id uuid references social.scrape_runs (id) on delete set null,
  add column if not exists attempt_count integer not null default 0,
  add column if not exists max_attempts integer not null default 3,
  add column if not exists priority integer not null default 100,
  add column if not exists available_at timestamptz not null default now(),
  add column if not exists claimed_at timestamptz,
  add column if not exists heartbeat_at timestamptz,
  add column if not exists worker_id text,
  add column if not exists last_error_code text,
  add column if not exists last_error_class text;

-- Replace legacy status constraint with queue-aware states.
do $$
declare
  r record;
begin
  for r in
    select
      c.conname,
      pg_get_constraintdef(c.oid) as def
    from pg_constraint c
    where c.conrelid = 'social.scrape_jobs'::regclass
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%status%'
      and pg_get_constraintdef(c.oid) ilike '%pending%'
      and pg_get_constraintdef(c.oid) ilike '%running%'
  loop
    execute format('alter table social.scrape_jobs drop constraint %I', r.conname);
  end loop;
end $$;

alter table social.scrape_jobs
  add constraint scrape_jobs_status_check_v2
  check (status in ('queued', 'pending', 'running', 'retrying', 'completed', 'failed', 'cancelled'));

commit;
