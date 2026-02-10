-- Add archive columns for unified media assets and show images.
-- Existing archive columns already exist for core.cast_photos/core.season_images/core.episode_images (0096).
begin;

-- core.media_assets
alter table core.media_assets
  add column if not exists archived_at timestamptz,
  add column if not exists archived_by_firebase_uid text,
  add column if not exists archived_reason text;

create index if not exists idx_media_assets_archived
  on core.media_assets (archived_at)
  where archived_at is not null;

-- core.show_images
alter table core.show_images
  add column if not exists archived_at timestamptz,
  add column if not exists archived_by_firebase_uid text,
  add column if not exists archived_reason text;

create index if not exists idx_show_images_archived
  on core.show_images (archived_at)
  where archived_at is not null;

commit;

