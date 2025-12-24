begin;

create schema if not exists core;

drop view if exists core.v_episode_appearances;

-- Preserve the existing per-episode table if it exists.
do $$
begin
  if to_regclass('core.episode_appearances_old') is null
    and to_regclass('core.episode_appearances') is not null then
    execute 'alter table core.episode_appearances rename to episode_appearances_old';
  end if;
end $$;

-- Avoid index-name collisions when rebuilding episode_appearances.
do $$
declare
  idx record;
  new_name text;
begin
  for idx in
    select
      c.relname as index_name,
      c.oid::regclass as index_regclass,
      i.indrelid::regclass::text as table_name
    from pg_class c
    join pg_index i on i.indexrelid = c.oid
    where c.relnamespace = 'core'::regnamespace
      and c.relname in (
        'core_episode_appearances_show_id_idx',
        'core_episode_appearances_person_id_idx',
        'core_episode_appearances_episode_imdb_id_idx',
        'episode_appearances_show_id_person_id_episode_imdb_id_credit_ca',
        'episode_appearances_show_id_person_id_episode_imdb_id_credit_category_key',
        'episode_appearances_show_id_person_id_key'
      )
  loop
    if idx.table_name like '%_old' then
      new_name := left(idx.index_name, 40) || '_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
      execute format('alter index %s rename to %I', idx.index_regclass, new_name);
    end if;
  end loop;
end $$;

-- ---------------------------------------------------------------------------
-- core.episode_appearances (aggregated per person/show)
-- ---------------------------------------------------------------------------

create table core.episode_appearances (
  show_name text,
  cast_member_name text,
  seasons integer[] not null default '{}'::integer[],
  tmdb_season_ids integer[] not null default '{}'::integer[],
  tmdb_show_id integer,
  imdb_show_id text,
  imdb_episode_title_ids text[] not null default '{}'::text[],
  tmdb_episode_ids integer[] not null default '{}'::integer[],
  total_episodes integer generated always as (
    coalesce(array_length(imdb_episode_title_ids, 1), 0)
  ) stored,
  show_id uuid not null references core.shows (id) on delete cascade,
  person_id uuid not null references core.people (id) on delete cascade,
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (show_id, person_id)
);

-- Restore aggregated data from the old per-episode table (excluding archived footage).
do $$
begin
  if to_regclass('core.episode_appearances_old') is not null then
    insert into core.episode_appearances (
      show_name,
      cast_member_name,
      seasons,
      tmdb_season_ids,
      tmdb_show_id,
      imdb_show_id,
      imdb_episode_title_ids,
      tmdb_episode_ids,
      show_id,
      person_id,
      created_at,
      updated_at
    )
    with base as (
      select distinct
        ea.show_id,
        ea.person_id,
        ea.episode_imdb_id,
        coalesce(e.season_number, ea.season_number) as season_number,
        coalesce(e.episode_number, ea.episode_number) as episode_number,
        e.tmdb_episode_id,
        coalesce(
          e.air_date,
          case
            when ea.air_year is not null then make_date(ea.air_year, 1, 1)
            else null
          end
        ) as air_date,
        s.tmdb_season_id,
        coalesce(sh.name, ea.show_name) as show_name,
        coalesce(p.full_name, ea.cast_member_name) as cast_member_name,
        coalesce(sh.tmdb_series_id, nullif(sh.external_ids->>'tmdb', '')::int) as tmdb_show_id,
        coalesce(sh.imdb_series_id, nullif(sh.external_ids->>'imdb', '')) as imdb_show_id,
        ea.created_at,
        ea.updated_at
      from core.episode_appearances_old ea
      left join core.episodes e
        on e.show_id = ea.show_id
        and e.imdb_episode_id = ea.episode_imdb_id
      left join core.seasons s
        on s.id = e.season_id
      left join core.shows sh
        on sh.id = ea.show_id
      left join core.people p
        on p.id = ea.person_id
      where ea.is_archive_footage is false
    )
    select
      base.show_name,
      base.cast_member_name,
      coalesce(
        (
          select array_agg(season_number order by season_number)
          from (
            select distinct season_number
            from base b2
            where b2.show_id = base.show_id
              and b2.person_id = base.person_id
              and b2.season_number is not null
            order by season_number
          ) seasons
        ),
        '{}'::integer[]
      ) as seasons,
      coalesce(
        (
          select array_agg(tmdb_season_id order by season_number)
          from (
            select
              season_number,
              min(tmdb_season_id) as tmdb_season_id
            from base b2
            where b2.show_id = base.show_id
              and b2.person_id = base.person_id
              and b2.season_number is not null
              and b2.tmdb_season_id is not null
            group by season_number
            order by season_number
          ) seasons
        ),
        '{}'::integer[]
      ) as tmdb_season_ids,
      base.tmdb_show_id,
      base.imdb_show_id,
      coalesce(
        (
          select array_agg(episode_imdb_id order by
            season_number nulls last,
            episode_number nulls last,
            air_date nulls last,
            episode_imdb_id
          )
          from (
            select
              episode_imdb_id,
              min(season_number) as season_number,
              min(episode_number) as episode_number,
              min(air_date) as air_date
            from base b2
            where b2.show_id = base.show_id
              and b2.person_id = base.person_id
              and b2.episode_imdb_id is not null
            group by episode_imdb_id
          ) episodes
        ),
        '{}'::text[]
      ) as imdb_episode_title_ids,
      coalesce(
        (
          select array_agg(tmdb_episode_id order by
            season_number nulls last,
            episode_number nulls last,
            air_date nulls last,
            tmdb_episode_id
          )
          from (
            select
              tmdb_episode_id,
              min(season_number) as season_number,
              min(episode_number) as episode_number,
              min(air_date) as air_date
            from base b2
            where b2.show_id = base.show_id
              and b2.person_id = base.person_id
              and b2.tmdb_episode_id is not null
            group by tmdb_episode_id
          ) episodes
        ),
        '{}'::integer[]
      ) as tmdb_episode_ids,
      base.show_id,
      base.person_id,
      min(base.created_at),
      max(base.updated_at)
    from base
    group by
      base.show_id,
      base.person_id,
      base.show_name,
      base.cast_member_name,
      base.tmdb_show_id,
      base.imdb_show_id;
  end if;
end $$;

create index if not exists core_episode_appearances_show_id_idx
on core.episode_appearances (show_id);

create index if not exists core_episode_appearances_person_id_idx
on core.episode_appearances (person_id);

-- Updated-at trigger.
drop trigger if exists core_episode_appearances_set_updated_at on core.episode_appearances;
create trigger core_episode_appearances_set_updated_at
before update on core.episode_appearances
for each row
execute function core.set_updated_at();

-- Populate denormalized names and show ids.
create or replace function core.set_episode_appearance_names()
returns trigger
language plpgsql
as $$
begin
  select
    s.name,
    coalesce(s.tmdb_series_id, nullif(s.external_ids->>'tmdb', '')::int),
    coalesce(s.imdb_series_id, nullif(s.external_ids->>'imdb', ''))
  into
    new.show_name,
    new.tmdb_show_id,
    new.imdb_show_id
  from core.shows s
  where s.id = new.show_id;

  select p.full_name
  into new.cast_member_name
  from core.people p
  where p.id = new.person_id;

  return new;
end;
$$;

drop trigger if exists core_episode_appearances_set_names on core.episode_appearances;
create trigger core_episode_appearances_set_names
before insert or update on core.episode_appearances
for each row
execute function core.set_episode_appearance_names();

-- Update denormalized names + ids when source tables change.
create or replace function core.propagate_show_name_to_dependents()
returns trigger
language plpgsql
as $$
begin
  update core.seasons
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;

  update core.episodes
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;

  update core.show_cast
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;

  update core.episode_appearances
  set show_name = new.name,
      tmdb_show_id = coalesce(new.tmdb_series_id, nullif(new.external_ids->>'tmdb', '')::int),
      imdb_show_id = coalesce(new.imdb_series_id, nullif(new.external_ids->>'imdb', ''))
  where show_id = new.id
    and (
      show_name is distinct from new.name
      or tmdb_show_id is distinct from coalesce(new.tmdb_series_id, nullif(new.external_ids->>'tmdb', '')::int)
      or imdb_show_id is distinct from coalesce(new.imdb_series_id, nullif(new.external_ids->>'imdb', ''))
    );

  return new;
end;
$$;

drop trigger if exists core_shows_propagate_name_to_dependents on core.shows;
create trigger core_shows_propagate_name_to_dependents
after update of name, tmdb_series_id, imdb_series_id, external_ids on core.shows
for each row
execute function core.propagate_show_name_to_dependents();

create or replace function core.propagate_person_name_to_dependents()
returns trigger
language plpgsql
as $$
begin
  update core.show_cast
  set cast_member_name = new.full_name
  where person_id = new.id
    and cast_member_name is distinct from new.full_name;

  update core.episode_appearances
  set cast_member_name = new.full_name
  where person_id = new.id
    and cast_member_name is distinct from new.full_name;

  return new;
end;
$$;

drop trigger if exists core_people_propagate_name_to_dependents on core.people;
create trigger core_people_propagate_name_to_dependents
after update of full_name on core.people
for each row
execute function core.propagate_person_name_to_dependents();

-- View for export (columns only, no joins).
create view core.v_episode_appearances as
select
  show_name,
  cast_member_name,
  seasons,
  tmdb_season_ids,
  tmdb_show_id,
  imdb_show_id,
  imdb_episode_title_ids,
  tmdb_episode_ids,
  total_episodes
from core.episode_appearances;

grant select on table core.v_episode_appearances to anon, authenticated, service_role;

-- Restore grants + RLS policies on the rebuilt table.
grant usage on schema core to anon, authenticated, service_role;

grant select on table core.episode_appearances to anon, authenticated;
grant all privileges on table core.episode_appearances to service_role;

alter table core.episode_appearances enable row level security;

drop policy if exists core_episode_appearances_public_read on core.episode_appearances;
create policy core_episode_appearances_public_read on core.episode_appearances
for select to anon, authenticated
using (true);

-- Cleanup old table once data is migrated.
drop table if exists core.episode_appearances_old;

commit;
