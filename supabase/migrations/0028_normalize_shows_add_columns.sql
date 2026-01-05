begin;

-- =============================================================================
-- Migration 0028: Add typed columns to core.shows for normalization
-- =============================================================================
-- Phase 1 of schema normalization: Add new columns without removing old ones.
-- This allows dual-write during transition period.

-- Rename existing ID columns for clarity (imdb_series_id -> imdb_id, tmdb_series_id -> tmdb_id)
-- First check if already renamed to avoid errors on re-run
do $$
begin
  if exists (select 1 from information_schema.columns where table_schema = 'core' and table_name = 'shows' and column_name = 'imdb_series_id') then
    alter table core.shows rename column imdb_series_id to imdb_id;
  end if;
  if exists (select 1 from information_schema.columns where table_schema = 'core' and table_name = 'shows' and column_name = 'tmdb_series_id') then
    alter table core.shows rename column tmdb_series_id to tmdb_id;
  end if;
end $$;

-- Add new typed columns for most_recent_episode (replacing the text field)
alter table core.shows add column if not exists most_recent_episode_season integer;
alter table core.shows add column if not exists most_recent_episode_number integer;
alter table core.shows add column if not exists most_recent_episode_title text;
alter table core.shows add column if not exists most_recent_episode_air_date date;
alter table core.shows add column if not exists most_recent_episode_imdb_id text;

-- Add primary image FK columns (will reference show_images)
alter table core.shows add column if not exists primary_poster_image_id uuid;
alter table core.shows add column if not exists primary_backdrop_image_id uuid;
alter table core.shows add column if not exists primary_logo_image_id uuid;

-- Add FKs for primary images (after show_images has been populated)
-- These are deferred to avoid circular dependency issues
do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'shows_primary_poster_image_id_fkey'
  ) then
    alter table core.shows add constraint shows_primary_poster_image_id_fkey
      foreign key (primary_poster_image_id) references core.show_images(id) on delete set null;
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'shows_primary_backdrop_image_id_fkey'
  ) then
    alter table core.shows add constraint shows_primary_backdrop_image_id_fkey
      foreign key (primary_backdrop_image_id) references core.show_images(id) on delete set null;
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'shows_primary_logo_image_id_fkey'
  ) then
    alter table core.shows add constraint shows_primary_logo_image_id_fkey
      foreign key (primary_logo_image_id) references core.show_images(id) on delete set null;
  end if;
end $$;

-- Update indexes for renamed columns (if old indexes exist, rename them)
do $$
begin
  -- Handle imdb_id unique index
  if to_regclass('core.core_shows_imdb_series_id_unique') is not null then
    drop index if exists core.core_shows_imdb_series_id_unique;
  end if;

  -- Handle tmdb_id unique index
  if to_regclass('core.core_shows_tmdb_series_id_unique') is not null then
    drop index if exists core.core_shows_tmdb_series_id_unique;
  end if;
end $$;

-- Create new partial unique indexes for renamed columns
create unique index if not exists core_shows_imdb_id_unique
  on core.shows (imdb_id)
  where imdb_id is not null and btrim(imdb_id) <> '';

create unique index if not exists core_shows_tmdb_id_unique
  on core.shows (tmdb_id)
  where tmdb_id is not null;

commit;
