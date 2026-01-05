begin;

-- =============================================================================
-- Migration 0033: Cleanup legacy JSONB columns from core.shows
-- =============================================================================
-- IMPORTANT: Only run this migration AFTER verifying that:
--   1. All application code has been updated to use typed columns
--   2. sync_shows runs successfully without errors
--   3. Data integrity has been verified (all JSONB data migrated to typed tables)
--
-- This migration drops the following legacy columns/indexes:
--   - core.shows.external_ids (JSONB)
--   - core.shows.imdb_meta (JSONB)
--   - core.shows.primary_tmdb_poster_path (replaced by primary_poster_image_id FK)
--   - core.shows.primary_tmdb_backdrop_path (replaced by primary_backdrop_image_id FK)
--   - core.shows.primary_tmdb_logo_path (replaced by primary_logo_image_id FK)
--   - core.show_images.metadata (kept; still used for raw payloads)

-- ---------------------------------------------------------------------------
-- 0. Drop triggers that depend on legacy columns BEFORE dropping columns
-- ---------------------------------------------------------------------------
-- The trigger core_shows_propagate_name_to_dependents (from 0024) fires on:
--   AFTER UPDATE OF name, tmdb_series_id, imdb_series_id, external_ids
-- PostgreSQL tracks column dependencies by OID, so even after column renames
-- (0028 renamed *_series_id to *_id), the trigger still depends on external_ids.

drop trigger if exists core_shows_propagate_name_to_dependents on core.shows;

-- ---------------------------------------------------------------------------
-- 1. Drop legacy JSONB columns from core.shows
-- ---------------------------------------------------------------------------

-- Drop JSONB indexes first
drop index if exists core.shows_external_ids_gin_idx;
drop index if exists core.shows_external_ids_imdb_idx;
drop index if exists core.shows_external_ids_tmdb_idx;
drop index if exists core.shows_imdb_meta_gin_idx;

-- Drop the JSONB columns
alter table core.shows drop column if exists external_ids;
alter table core.shows drop column if exists imdb_meta;

-- Drop legacy TMDb path columns (replaced by FKs to show_images)
alter table core.shows drop column if exists primary_tmdb_poster_path;
alter table core.shows drop column if exists primary_tmdb_backdrop_path;
alter table core.shows drop column if exists primary_tmdb_logo_path;

-- Drop legacy network/streaming columns (replaced by normalized tables).
alter table core.shows drop column if exists network;
alter table core.shows drop column if exists streaming;

-- ---------------------------------------------------------------------------
-- 2. Update the function and recreate trigger WITHOUT the removed columns
-- ---------------------------------------------------------------------------
-- The function core.propagate_show_name_to_dependents() (from 0024) used:
--   coalesce(new.tmdb_series_id, nullif(new.external_ids->>'tmdb', '')::int)
-- Since columns were renamed in 0028 and external_ids is now dropped, we
-- update it to use the new column names directly.

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
      tmdb_show_id = new.tmdb_id,
      imdb_show_id = new.imdb_id
  where show_id = new.id
    and (
      show_name is distinct from new.name
      or tmdb_show_id is distinct from new.tmdb_id
      or imdb_show_id is distinct from new.imdb_id
    );

  return new;
end;
$$;

-- Original trigger (from 0024) fired on: name, tmdb_series_id, imdb_series_id, external_ids
-- After 0028 column renames and this cleanup, we recreate it with: name, tmdb_id, imdb_id

create trigger core_shows_propagate_name_to_dependents
after update of name, tmdb_id, imdb_id on core.shows
for each row
execute function core.propagate_show_name_to_dependents();

commit;
