begin;

-- Seasons + Episodes canonical schema for list/metadata ingestion.
-- Adds season/episode columns needed for IMDb-first episode enumeration and TMDb season enrichment.
-- Idempotent: safe to apply on existing environments.

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
-- core.seasons
-- ---------------------------------------------------------------------------

-- Allow season 0 (TMDb specials).
alter table core.seasons drop constraint if exists seasons_season_number_check;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'core_seasons_season_number_nonnegative'
      and conrelid = 'core.seasons'::regclass
  ) then
    alter table core.seasons
    add constraint core_seasons_season_number_nonnegative
    check (season_number >= 0);
  end if;
end $$;

alter table core.seasons add column if not exists tmdb_series_id integer;
alter table core.seasons add column if not exists imdb_series_id text;

alter table core.seasons add column if not exists tmdb_season_id integer;
alter table core.seasons add column if not exists tmdb_season_object_id text;

alter table core.seasons add column if not exists name text;
alter table core.seasons add column if not exists overview text;
alter table core.seasons add column if not exists air_date date;
alter table core.seasons add column if not exists poster_path text;

alter table core.seasons
  add column if not exists url_original_poster text
  generated always as ('https://image.tmdb.org/t/p/original' || poster_path) stored;

alter table core.seasons add column if not exists external_tvdb_id integer;
alter table core.seasons add column if not exists external_wikidata_id text;

alter table core.seasons add column if not exists language text not null default 'en-US';
alter table core.seasons add column if not exists fetched_at timestamptz not null default now();

alter table core.seasons add column if not exists created_at timestamptz not null default now();
alter table core.seasons add column if not exists updated_at timestamptz not null default now();

drop trigger if exists core_seasons_set_updated_at on core.seasons;
create trigger core_seasons_set_updated_at
before update on core.seasons
for each row
execute function core.set_updated_at();

-- Optional uniqueness for TMDb linkage.
do $$
begin
  if exists (
    select 1
    from core.seasons
    where tmdb_series_id is not null
    group by tmdb_series_id, season_number
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.seasons (tmdb_series_id, season_number). Resolve duplicates before applying unique index core_seasons_tmdb_series_season_unique.';
  end if;
end $$;

create unique index if not exists core_seasons_tmdb_series_season_unique
on core.seasons (tmdb_series_id, season_number)
where tmdb_series_id is not null;

create index if not exists core_seasons_show_id_season_number_idx
on core.seasons (show_id, season_number);

-- ---------------------------------------------------------------------------
-- core.episodes
-- ---------------------------------------------------------------------------

-- Allow episode 0 (some series use episode 0 for specials/recaps).
alter table core.episodes drop constraint if exists episodes_episode_number_check;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'core_episodes_episode_number_nonnegative'
      and conrelid = 'core.episodes'::regclass
  ) then
    alter table core.episodes
    add constraint core_episodes_episode_number_nonnegative
    check (episode_number >= 0);
  end if;
end $$;

alter table core.episodes add column if not exists show_id uuid;
alter table core.episodes add column if not exists season_number integer;

-- Backfill show_id + season_number from seasons when possible.
update core.episodes e
set show_id = s.show_id,
    season_number = s.season_number
from core.seasons s
where e.season_id = s.id
  and (e.show_id is null or e.season_number is null);

do $$
begin
  if exists (
    select 1
    from core.episodes
    where show_id is null
       or season_number is null
  ) then
    raise exception
      'NULL values found in core.episodes.show_id or core.episodes.season_number. Ensure core.seasons data is consistent before applying NOT NULL constraints.';
  end if;
end $$;

alter table core.episodes alter column show_id set not null;
alter table core.episodes alter column season_number set not null;

-- Enforce show_id FK (in addition to season_id FK).
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'core_episodes_show_id_fkey'
      and conrelid = 'core.episodes'::regclass
  ) then
    alter table core.episodes
    add constraint core_episodes_show_id_fkey
    foreign key (show_id)
    references core.shows(id)
    on delete cascade;
  end if;
end $$;

-- Natural key uniqueness (show+season+episode).
do $$
begin
  if exists (
    select 1
    from core.episodes
    group by show_id, season_number, episode_number
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.episodes (show_id, season_number, episode_number). Resolve duplicates before applying unique index core_episodes_show_season_episode_unique.';
  end if;
end $$;

create unique index if not exists core_episodes_show_season_episode_unique
on core.episodes (show_id, season_number, episode_number);

-- IMDb ingestion fields.
alter table core.episodes add column if not exists imdb_episode_id text;
alter table core.episodes add column if not exists overview text;
alter table core.episodes add column if not exists imdb_rating numeric;
alter table core.episodes add column if not exists imdb_vote_count integer;
alter table core.episodes add column if not exists imdb_primary_image_url text;
alter table core.episodes add column if not exists imdb_primary_image_caption text;
alter table core.episodes add column if not exists imdb_primary_image_width integer;
alter table core.episodes add column if not exists imdb_primary_image_height integer;

-- TMDb enrichment fields.
alter table core.episodes add column if not exists tmdb_series_id integer;
alter table core.episodes add column if not exists tmdb_episode_id integer;
alter table core.episodes add column if not exists episode_type text;
alter table core.episodes add column if not exists production_code text;
alter table core.episodes add column if not exists runtime integer;
alter table core.episodes add column if not exists still_path text;
alter table core.episodes
  add column if not exists url_original_still text
  generated always as ('https://image.tmdb.org/t/p/original' || still_path) stored;
alter table core.episodes add column if not exists tmdb_vote_average numeric;
alter table core.episodes add column if not exists tmdb_vote_count integer;

alter table core.episodes add column if not exists fetched_at timestamptz not null default now();
alter table core.episodes add column if not exists created_at timestamptz not null default now();
alter table core.episodes add column if not exists updated_at timestamptz not null default now();

drop trigger if exists core_episodes_set_updated_at on core.episodes;
create trigger core_episodes_set_updated_at
before update on core.episodes
for each row
execute function core.set_updated_at();

-- External id uniqueness.
do $$
begin
  if exists (
    select 1
    from core.episodes
    where imdb_episode_id is not null
    group by imdb_episode_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.episodes.imdb_episode_id. Resolve duplicates before applying unique index core_episodes_imdb_episode_id_unique.';
  end if;
end $$;

create unique index if not exists core_episodes_imdb_episode_id_unique
on core.episodes (imdb_episode_id)
where imdb_episode_id is not null;

do $$
begin
  if exists (
    select 1
    from core.episodes
    where tmdb_episode_id is not null
    group by tmdb_episode_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.episodes.tmdb_episode_id. Resolve duplicates before applying unique index core_episodes_tmdb_episode_id_unique.';
  end if;
end $$;

create unique index if not exists core_episodes_tmdb_episode_id_unique
on core.episodes (tmdb_episode_id)
where tmdb_episode_id is not null;

-- Common query pattern: list episodes for a season.
create index if not exists core_episodes_show_season_idx
on core.episodes (show_id, season_number, episode_number);

commit;

