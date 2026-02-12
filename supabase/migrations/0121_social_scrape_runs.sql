begin;

create table if not exists social.scrape_runs (
  id uuid primary key default gen_random_uuid(),
  season_id uuid not null references core.seasons (id) on delete cascade,
  show_id uuid not null references core.shows (id) on delete cascade,
  source_scope text not null default 'bravo'
    check (source_scope in ('bravo', 'creator', 'community')),
  status text not null default 'queued'
    check (status in ('queued', 'running', 'completed', 'failed', 'cancelled')),
  initiated_by text,
  config jsonb not null default '{}'::jsonb,
  summary jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  cancelled_at timestamptz
);

create index if not exists scrape_runs_season_id_idx on social.scrape_runs (season_id, created_at desc);
create index if not exists scrape_runs_status_idx on social.scrape_runs (status, created_at desc);

grant select on table social.scrape_runs to anon, authenticated;
grant all privileges on table social.scrape_runs to service_role;

alter table social.scrape_runs enable row level security;

drop policy if exists scrape_runs_public_read on social.scrape_runs;
create policy scrape_runs_public_read on social.scrape_runs
for select to anon, authenticated
using (true);

commit;
