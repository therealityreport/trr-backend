begin;

-- Add missing metadata fields to core.season_images to match show_images pattern
-- This enables storing captions, tags (people), and other image metadata from TMDb/IMDb

alter table core.season_images add column if not exists source_image_id text;
alter table core.season_images add column if not exists caption text;
alter table core.season_images add column if not exists image_type text;
alter table core.season_images add column if not exists position integer;
alter table core.season_images add column if not exists url text;
alter table core.season_images add column if not exists url_path text;
alter table core.season_images add column if not exists metadata jsonb not null default '{}'::jsonb;
alter table core.season_images add column if not exists updated_at timestamptz not null default now();
alter table core.season_images add column if not exists created_at timestamptz not null default now();
alter table core.season_images add column if not exists fetch_method text;
alter table core.season_images add column if not exists fetched_from_url text;

-- Backfill source_image_id from file_path for existing rows
update core.season_images
set source_image_id = file_path
where source_image_id is null;

-- Make source_image_id required after backfill
alter table core.season_images alter column source_image_id set not null;

-- Make url required (backfill from url_original if needed)
update core.season_images
set url = url_original
where url is null and url_original is not null;

alter table core.season_images alter column url set not null;

-- Add unique constraint on (season_id, source, source_image_id) like show_images
create unique index if not exists season_images_season_source_image_unique
on core.season_images (season_id, source, source_image_id);

-- Add index for source_image_id lookups
create index if not exists season_images_source_image_id_idx
on core.season_images (source, source_image_id)
where source_image_id is not null;

-- Add index for metadata lookups (for tags/people queries)
create index if not exists season_images_metadata_idx
on core.season_images using gin (metadata);

-- Add index for fetch_method filtering
create index if not exists season_images_fetch_method_idx
on core.season_images (fetch_method);

-- Add updated_at trigger
drop trigger if exists core_season_images_set_updated_at on core.season_images;
create trigger core_season_images_set_updated_at
  before update on core.season_images
  for each row
  execute function core.set_updated_at();

commit;
