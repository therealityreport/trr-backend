begin;

create schema if not exists core;

-- ---------------------------------------------------------------------------
-- Show source snapshots
-- ---------------------------------------------------------------------------
create table if not exists core.show_source_latest (
  show_id uuid not null references core.shows(id) on delete cascade,
  source_id text not null references core.sources(id),
  variant text not null default 'default',
  fetched_at timestamptz not null,
  fetch_method text,
  status text not null default 'success',
  error text,
  payload jsonb not null,
  payload_sha256 text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (show_id, source_id, variant)
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'show_source_latest_status_check'
      and conrelid = 'core.show_source_latest'::regclass
  ) then
    alter table core.show_source_latest
      add constraint show_source_latest_status_check
      check (status in ('success', 'error'));
  end if;
end $$;

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'show_source_latest_set_updated_at') then
    create trigger show_source_latest_set_updated_at
    before update on core.show_source_latest
    for each row execute function core.set_updated_at();
  end if;
end $$;

create table if not exists core.show_source_history (
  id bigserial primary key,
  show_id uuid not null references core.shows(id) on delete cascade,
  source_id text not null references core.sources(id),
  variant text not null default 'default',
  fetched_at timestamptz not null,
  fetch_method text,
  status text not null default 'success',
  error text,
  payload jsonb not null,
  payload_sha256 text not null,
  created_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'show_source_history_status_check'
      and conrelid = 'core.show_source_history'::regclass
  ) then
    alter table core.show_source_history
      add constraint show_source_history_status_check
      check (status in ('success', 'error'));
  end if;
end $$;

create index if not exists show_source_history_lookup_idx
  on core.show_source_history(show_id, source_id, variant, fetched_at desc);

-- ---------------------------------------------------------------------------
-- Season source snapshots
-- ---------------------------------------------------------------------------
create table if not exists core.season_source_latest (
  season_id uuid not null references core.seasons(id) on delete cascade,
  source_id text not null references core.sources(id),
  variant text not null default 'default',
  fetched_at timestamptz not null,
  fetch_method text,
  status text not null default 'success',
  error text,
  payload jsonb not null,
  payload_sha256 text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (season_id, source_id, variant)
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'season_source_latest_status_check'
      and conrelid = 'core.season_source_latest'::regclass
  ) then
    alter table core.season_source_latest
      add constraint season_source_latest_status_check
      check (status in ('success', 'error'));
  end if;
end $$;

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'season_source_latest_set_updated_at') then
    create trigger season_source_latest_set_updated_at
    before update on core.season_source_latest
    for each row execute function core.set_updated_at();
  end if;
end $$;

create table if not exists core.season_source_history (
  id bigserial primary key,
  season_id uuid not null references core.seasons(id) on delete cascade,
  source_id text not null references core.sources(id),
  variant text not null default 'default',
  fetched_at timestamptz not null,
  fetch_method text,
  status text not null default 'success',
  error text,
  payload jsonb not null,
  payload_sha256 text not null,
  created_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'season_source_history_status_check'
      and conrelid = 'core.season_source_history'::regclass
  ) then
    alter table core.season_source_history
      add constraint season_source_history_status_check
      check (status in ('success', 'error'));
  end if;
end $$;

create index if not exists season_source_history_lookup_idx
  on core.season_source_history(season_id, source_id, variant, fetched_at desc);

-- ---------------------------------------------------------------------------
-- Episode source snapshots
-- ---------------------------------------------------------------------------
create table if not exists core.episode_source_latest (
  episode_id uuid not null references core.episodes(id) on delete cascade,
  source_id text not null references core.sources(id),
  variant text not null default 'default',
  fetched_at timestamptz not null,
  fetch_method text,
  status text not null default 'success',
  error text,
  payload jsonb not null,
  payload_sha256 text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (episode_id, source_id, variant)
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'episode_source_latest_status_check'
      and conrelid = 'core.episode_source_latest'::regclass
  ) then
    alter table core.episode_source_latest
      add constraint episode_source_latest_status_check
      check (status in ('success', 'error'));
  end if;
end $$;

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'episode_source_latest_set_updated_at') then
    create trigger episode_source_latest_set_updated_at
    before update on core.episode_source_latest
    for each row execute function core.set_updated_at();
  end if;
end $$;

create table if not exists core.episode_source_history (
  id bigserial primary key,
  episode_id uuid not null references core.episodes(id) on delete cascade,
  source_id text not null references core.sources(id),
  variant text not null default 'default',
  fetched_at timestamptz not null,
  fetch_method text,
  status text not null default 'success',
  error text,
  payload jsonb not null,
  payload_sha256 text not null,
  created_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'episode_source_history_status_check'
      and conrelid = 'core.episode_source_history'::regclass
  ) then
    alter table core.episode_source_history
      add constraint episode_source_history_status_check
      check (status in ('success', 'error'));
  end if;
end $$;

create index if not exists episode_source_history_lookup_idx
  on core.episode_source_history(episode_id, source_id, variant, fetched_at desc);

-- ---------------------------------------------------------------------------
-- Person source snapshots
-- ---------------------------------------------------------------------------
create table if not exists core.person_source_latest (
  person_id uuid not null references core.people(id) on delete cascade,
  source_id text not null references core.sources(id),
  variant text not null default 'default',
  fetched_at timestamptz not null,
  fetch_method text,
  status text not null default 'success',
  error text,
  payload jsonb not null,
  payload_sha256 text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (person_id, source_id, variant)
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'person_source_latest_status_check'
      and conrelid = 'core.person_source_latest'::regclass
  ) then
    alter table core.person_source_latest
      add constraint person_source_latest_status_check
      check (status in ('success', 'error'));
  end if;
end $$;

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'person_source_latest_set_updated_at') then
    create trigger person_source_latest_set_updated_at
    before update on core.person_source_latest
    for each row execute function core.set_updated_at();
  end if;
end $$;

create table if not exists core.person_source_history (
  id bigserial primary key,
  person_id uuid not null references core.people(id) on delete cascade,
  source_id text not null references core.sources(id),
  variant text not null default 'default',
  fetched_at timestamptz not null,
  fetch_method text,
  status text not null default 'success',
  error text,
  payload jsonb not null,
  payload_sha256 text not null,
  created_at timestamptz not null default now()
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'person_source_history_status_check'
      and conrelid = 'core.person_source_history'::regclass
  ) then
    alter table core.person_source_history
      add constraint person_source_history_status_check
      check (status in ('success', 'error'));
  end if;
end $$;

create index if not exists person_source_history_lookup_idx
  on core.person_source_history(person_id, source_id, variant, fetched_at desc);

-- ---------------------------------------------------------------------------
-- Grants / RLS (service_role only for raw payloads)
-- ---------------------------------------------------------------------------

grant all privileges on table core.show_source_latest to service_role;
grant all privileges on table core.show_source_history to service_role;
grant all privileges on table core.season_source_latest to service_role;
grant all privileges on table core.season_source_history to service_role;
grant all privileges on table core.episode_source_latest to service_role;
grant all privileges on table core.episode_source_history to service_role;
grant all privileges on table core.person_source_latest to service_role;
grant all privileges on table core.person_source_history to service_role;

alter table core.show_source_latest enable row level security;
alter table core.show_source_history enable row level security;
alter table core.season_source_latest enable row level security;
alter table core.season_source_history enable row level security;
alter table core.episode_source_latest enable row level security;
alter table core.episode_source_history enable row level security;
alter table core.person_source_latest enable row level security;
alter table core.person_source_history enable row level security;

commit;
