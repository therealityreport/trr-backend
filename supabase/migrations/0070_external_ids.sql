begin;

create schema if not exists core;

-- ---------------------------------------------------------------------------
-- core.show_external_ids
-- ---------------------------------------------------------------------------
create table if not exists core.show_external_ids (
  id bigserial primary key,
  show_id uuid not null references core.shows(id) on delete cascade,
  source_id text not null references core.sources(id),
  external_id text not null,
  is_primary boolean not null default true,
  valid_from date,
  valid_to date,
  observed_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists show_external_ids_unique_identifiers_uq
  on core.show_external_ids(source_id, external_id)
  where source_id in ('imdb','tmdb','wikidata','tvdb','tvrage');

create unique index if not exists show_external_ids_unique_active_handles_uq
  on core.show_external_ids(source_id, external_id)
  where source_id in ('twitter','instagram','facebook','tiktok','youtube')
    and valid_to is null;

create unique index if not exists show_external_ids_primary_uq
  on core.show_external_ids(show_id, source_id)
  where is_primary = true;

create index if not exists show_external_ids_show_id_idx
  on core.show_external_ids(show_id);

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'show_external_ids_set_updated_at') then
    create trigger show_external_ids_set_updated_at
    before update on core.show_external_ids
    for each row execute function core.set_updated_at();
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- core.season_external_ids
-- ---------------------------------------------------------------------------
create table if not exists core.season_external_ids (
  id bigserial primary key,
  season_id uuid not null references core.seasons(id) on delete cascade,
  source_id text not null references core.sources(id),
  external_id text not null,
  is_primary boolean not null default true,
  valid_from date,
  valid_to date,
  observed_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists season_external_ids_unique_identifiers_uq
  on core.season_external_ids(source_id, external_id)
  where source_id in ('imdb','tmdb','wikidata','tvdb','tvrage');

create unique index if not exists season_external_ids_unique_active_handles_uq
  on core.season_external_ids(source_id, external_id)
  where source_id in ('twitter','instagram','facebook','tiktok','youtube')
    and valid_to is null;

create unique index if not exists season_external_ids_primary_uq
  on core.season_external_ids(season_id, source_id)
  where is_primary = true;

create index if not exists season_external_ids_season_id_idx
  on core.season_external_ids(season_id);

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'season_external_ids_set_updated_at') then
    create trigger season_external_ids_set_updated_at
    before update on core.season_external_ids
    for each row execute function core.set_updated_at();
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- core.episode_external_ids
-- ---------------------------------------------------------------------------
create table if not exists core.episode_external_ids (
  id bigserial primary key,
  episode_id uuid not null references core.episodes(id) on delete cascade,
  source_id text not null references core.sources(id),
  external_id text not null,
  is_primary boolean not null default true,
  valid_from date,
  valid_to date,
  observed_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists episode_external_ids_unique_identifiers_uq
  on core.episode_external_ids(source_id, external_id)
  where source_id in ('imdb','tmdb','wikidata','tvdb','tvrage');

create unique index if not exists episode_external_ids_unique_active_handles_uq
  on core.episode_external_ids(source_id, external_id)
  where source_id in ('twitter','instagram','facebook','tiktok','youtube')
    and valid_to is null;

create unique index if not exists episode_external_ids_primary_uq
  on core.episode_external_ids(episode_id, source_id)
  where is_primary = true;

create index if not exists episode_external_ids_episode_id_idx
  on core.episode_external_ids(episode_id);

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'episode_external_ids_set_updated_at') then
    create trigger episode_external_ids_set_updated_at
    before update on core.episode_external_ids
    for each row execute function core.set_updated_at();
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- core.person_external_ids
-- ---------------------------------------------------------------------------
create table if not exists core.person_external_ids (
  id bigserial primary key,
  person_id uuid not null references core.people(id) on delete cascade,
  source_id text not null references core.sources(id),
  external_id text not null,
  is_primary boolean not null default true,
  valid_from date,
  valid_to date,
  observed_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists person_external_ids_unique_identifiers_uq
  on core.person_external_ids(source_id, external_id)
  where source_id in ('imdb','tmdb','wikidata','tvdb','tvrage');

create unique index if not exists person_external_ids_unique_active_handles_uq
  on core.person_external_ids(source_id, external_id)
  where source_id in ('twitter','instagram','facebook','tiktok','youtube')
    and valid_to is null;

create unique index if not exists person_external_ids_primary_uq
  on core.person_external_ids(person_id, source_id)
  where is_primary = true;

create index if not exists person_external_ids_person_id_idx
  on core.person_external_ids(person_id);

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'person_external_ids_set_updated_at') then
    create trigger person_external_ids_set_updated_at
    before update on core.person_external_ids
    for each row execute function core.set_updated_at();
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- Conflict staging
-- ---------------------------------------------------------------------------
create table if not exists core.external_id_conflicts (
  entity_type text not null,
  entity_id uuid not null,
  source_id text not null,
  external_id text not null,
  conflict_reason text not null,
  detected_at timestamptz not null default now(),
  payload jsonb
);

-- ---------------------------------------------------------------------------
-- Grants / RLS
-- ---------------------------------------------------------------------------

-- show_external_ids
grant select on table core.show_external_ids to anon, authenticated;
grant all privileges on table core.show_external_ids to service_role;

alter table core.show_external_ids enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'core'
      and tablename = 'show_external_ids'
      and policyname = 'core_show_external_ids_public_read'
  ) then
    create policy core_show_external_ids_public_read
      on core.show_external_ids for select to anon, authenticated
      using (true);
  end if;
end $$;

-- season_external_ids
grant select on table core.season_external_ids to anon, authenticated;
grant all privileges on table core.season_external_ids to service_role;

alter table core.season_external_ids enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'core'
      and tablename = 'season_external_ids'
      and policyname = 'core_season_external_ids_public_read'
  ) then
    create policy core_season_external_ids_public_read
      on core.season_external_ids for select to anon, authenticated
      using (true);
  end if;
end $$;

-- episode_external_ids
grant select on table core.episode_external_ids to anon, authenticated;
grant all privileges on table core.episode_external_ids to service_role;

alter table core.episode_external_ids enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'core'
      and tablename = 'episode_external_ids'
      and policyname = 'core_episode_external_ids_public_read'
  ) then
    create policy core_episode_external_ids_public_read
      on core.episode_external_ids for select to anon, authenticated
      using (true);
  end if;
end $$;

-- person_external_ids
grant select on table core.person_external_ids to anon, authenticated;
grant all privileges on table core.person_external_ids to service_role;

alter table core.person_external_ids enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'core'
      and tablename = 'person_external_ids'
      and policyname = 'core_person_external_ids_public_read'
  ) then
    create policy core_person_external_ids_public_read
      on core.person_external_ids for select to anon, authenticated
      using (true);
  end if;
end $$;

-- external_id_conflicts (service_role only by default)
grant all privileges on table core.external_id_conflicts to service_role;

commit;
