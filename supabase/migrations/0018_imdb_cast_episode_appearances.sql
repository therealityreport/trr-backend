begin;

create extension if not exists pgcrypto;
create schema if not exists core;

-- Ensure updated_at helper exists (shared with core.shows).
create or replace function core.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- ---------------------------------------------------------------------------
-- core.people (add updated_at + imdb uniqueness)
-- ---------------------------------------------------------------------------

alter table core.people add column if not exists updated_at timestamptz not null default now();

drop trigger if exists core_people_set_updated_at on core.people;
create trigger core_people_set_updated_at
before update on core.people
for each row
execute function core.set_updated_at();

do $$
begin
  if exists (
    select 1
    from core.people
    where external_ids ? 'imdb'
      and btrim(external_ids->>'imdb') <> ''
    group by external_ids->>'imdb'
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.people external_ids->>imdb. Resolve duplicates before applying unique index core_people_imdb_unique.';
  end if;
end $$;

create unique index if not exists core_people_imdb_unique
on core.people ((external_ids->>'imdb'))
where external_ids ? 'imdb'
  and btrim(external_ids->>'imdb') <> '';

-- ---------------------------------------------------------------------------
-- core.show_cast
-- ---------------------------------------------------------------------------

create table if not exists core.show_cast (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows (id) on delete cascade,
  person_id uuid not null references core.people (id) on delete cascade,
  billing_order integer,
  role text,
  credit_category text not null default 'Self',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (show_id, person_id, credit_category)
);

create index if not exists core_show_cast_show_id_idx
on core.show_cast (show_id);

create index if not exists core_show_cast_person_id_idx
on core.show_cast (person_id);

drop trigger if exists core_show_cast_set_updated_at on core.show_cast;
create trigger core_show_cast_set_updated_at
before update on core.show_cast
for each row
execute function core.set_updated_at();

-- ---------------------------------------------------------------------------
-- core.episode_appearances
-- ---------------------------------------------------------------------------

create table if not exists core.episode_appearances (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows (id) on delete cascade,
  person_id uuid not null references core.people (id) on delete cascade,
  episode_imdb_id text not null,
  season_number integer,
  episode_number integer,
  episode_title text,
  air_year integer,
  credit_category text not null default 'Self',
  credit_text text,
  attributes jsonb not null default '[]'::jsonb,
  is_archive_footage boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (show_id, person_id, episode_imdb_id, credit_category)
);

create index if not exists core_episode_appearances_show_id_idx
on core.episode_appearances (show_id);

create index if not exists core_episode_appearances_person_id_idx
on core.episode_appearances (person_id);

create index if not exists core_episode_appearances_episode_imdb_id_idx
on core.episode_appearances (episode_imdb_id);

drop trigger if exists core_episode_appearances_set_updated_at on core.episode_appearances;
create trigger core_episode_appearances_set_updated_at
before update on core.episode_appearances
for each row
execute function core.set_updated_at();

commit;
