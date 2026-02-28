-- Add cast_photos.source_asset_id for source-level asset linkage and repair workflows.
alter table if exists core.cast_photos
  add column if not exists source_asset_id text;

-- Backfill from source_image_id for existing rows where asset id was not persisted.
update core.cast_photos
set source_asset_id = source_image_id
where source_asset_id is null
  and source_image_id is not null;

create index if not exists core_cast_photos_person_source_asset_id_idx
  on core.cast_photos (person_id, source, source_asset_id)
  where source_asset_id is not null;
