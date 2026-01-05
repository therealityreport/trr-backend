begin;

-- =============================================================================
-- Migration 0031: Update show_images - add typed columns, prepare for metadata removal
-- =============================================================================
-- Adds fetch_method and fetched_from_url columns to replace metadata JSONB.
-- Does NOT drop metadata yet (will be done in cleanup migration after code update).

-- Add new typed columns
alter table core.show_images add column if not exists fetch_method text;
alter table core.show_images add column if not exists fetched_from_url text;

-- Backfill fetch_method from existing metadata if possible
update core.show_images
set fetch_method = coalesce(
  metadata->>'source',
  case
    when source = 'imdb' then 'imdb_section_images'
    when source = 'tmdb' then 'tmdb_images_api'
    else null
  end
)
where fetch_method is null and metadata is not null;

-- Create index on fetch_method for filtering
create index if not exists show_images_fetch_method_idx on core.show_images (fetch_method);

commit;
