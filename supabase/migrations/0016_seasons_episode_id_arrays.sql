begin;

-- Denormalized episode id arrays on core.seasons (requested).
-- Stores TMDb episode ids and IMDb tconst ids as arrays (one cell per season row).
-- Kept in sync via a statement-level trigger on core.episodes.

create schema if not exists core;

alter table core.seasons
  add column if not exists imdb_episode_ids text[] not null default '{}'::text[];

alter table core.seasons
  add column if not exists tmdb_episode_ids integer[] not null default '{}'::integer[];

-- Backfill arrays for existing rows (best-effort).
update core.seasons se
set
  imdb_episode_ids = coalesce(ep.imdb_episode_ids, '{}'::text[]),
  tmdb_episode_ids = coalesce(ep.tmdb_episode_ids, '{}'::integer[])
from (
  select
    season_id,
    coalesce(array_agg(imdb_episode_id order by episode_number) filter (where imdb_episode_id is not null), '{}'::text[]) as imdb_episode_ids,
    coalesce(array_agg(tmdb_episode_id order by episode_number) filter (where tmdb_episode_id is not null), '{}'::integer[]) as tmdb_episode_ids
  from core.episodes
  group by season_id
) ep
where ep.season_id = se.id;

-- Refresh helper for a single season.
create or replace function core.refresh_season_episode_id_arrays(season_uuid uuid)
returns void
language plpgsql
as $$
begin
  update core.seasons se
  set
    imdb_episode_ids = coalesce(
      (
        select array_agg(e.imdb_episode_id order by e.episode_number)
        filter (where e.imdb_episode_id is not null)
        from core.episodes e
        where e.season_id = season_uuid
      ),
      '{}'::text[]
    ),
    tmdb_episode_ids = coalesce(
      (
        select array_agg(e.tmdb_episode_id order by e.episode_number)
        filter (where e.tmdb_episode_id is not null)
        from core.episodes e
        where e.season_id = season_uuid
      ),
      '{}'::integer[]
    )
  where se.id = season_uuid;
end;
$$;

-- Statement-level triggers to refresh affected seasons once per upsert batch.
--
-- Postgres does not allow transition tables on a trigger with multiple events,
-- so we create one trigger per event type.

create or replace function core.refresh_season_episode_id_arrays_from_episode_inserts()
returns trigger
language plpgsql
as $$
declare
  sid uuid;
begin
  for sid in
    select distinct season_id
    from new_rows
    where season_id is not null
  loop
    perform core.refresh_season_episode_id_arrays(sid);
  end loop;
  return null;
end;
$$;

create or replace function core.refresh_season_episode_id_arrays_from_episode_deletes()
returns trigger
language plpgsql
as $$
declare
  sid uuid;
begin
  for sid in
    select distinct season_id
    from old_rows
    where season_id is not null
  loop
    perform core.refresh_season_episode_id_arrays(sid);
  end loop;
  return null;
end;
$$;

create or replace function core.refresh_season_episode_id_arrays_from_episode_updates()
returns trigger
language plpgsql
as $$
declare
  sid uuid;
begin
  for sid in
    select distinct season_id
    from (
      select season_id from new_rows
      union
      select season_id from old_rows
    ) s
    where season_id is not null
  loop
    perform core.refresh_season_episode_id_arrays(sid);
  end loop;
  return null;
end;
$$;

drop trigger if exists core_episodes_refresh_season_episode_id_arrays on core.episodes;
drop trigger if exists core_episodes_refresh_season_episode_id_arrays_ins on core.episodes;
drop trigger if exists core_episodes_refresh_season_episode_id_arrays_upd on core.episodes;
drop trigger if exists core_episodes_refresh_season_episode_id_arrays_del on core.episodes;

create trigger core_episodes_refresh_season_episode_id_arrays_ins
after insert on core.episodes
referencing new table as new_rows
for each statement
execute function core.refresh_season_episode_id_arrays_from_episode_inserts();

create trigger core_episodes_refresh_season_episode_id_arrays_upd
after update on core.episodes
referencing new table as new_rows old table as old_rows
for each statement
execute function core.refresh_season_episode_id_arrays_from_episode_updates();

create trigger core_episodes_refresh_season_episode_id_arrays_del
after delete on core.episodes
referencing old table as old_rows
for each statement
execute function core.refresh_season_episode_id_arrays_from_episode_deletes();

commit;
