begin;

-- ---------------------------------------------------------------------------
-- Add alternative_names to core.shows
-- ---------------------------------------------------------------------------
--
-- This column stores alternative titles for TV shows from TMDb and other sources.
-- Used for search optimization and display of localized/alternative show names.
--
-- Merge behavior: Union + dedupe (never wipe manually-added names)
-- ---------------------------------------------------------------------------

-- Add alternative_names column
alter table core.shows
  add column if not exists alternative_names text[] not null default '{}';

-- Add GIN index for efficient array search
create index if not exists core_shows_alternative_names_gin
  on core.shows using gin (alternative_names);

commit;
