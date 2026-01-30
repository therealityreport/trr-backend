-- Add archive columns to image tables for soft-delete functionality
begin;

-- core.cast_photos
alter table core.cast_photos
  add column if not exists archived_at timestamptz,
  add column if not exists archived_by_firebase_uid text,
  add column if not exists archived_reason text;

create index if not exists idx_cast_photos_archived
  on core.cast_photos (archived_at)
  where archived_at is not null;

-- core.episode_images
alter table core.episode_images
  add column if not exists archived_at timestamptz,
  add column if not exists archived_by_firebase_uid text,
  add column if not exists archived_reason text;

create index if not exists idx_episode_images_archived
  on core.episode_images (archived_at)
  where archived_at is not null;

-- core.season_images
alter table core.season_images
  add column if not exists archived_at timestamptz,
  add column if not exists archived_by_firebase_uid text,
  add column if not exists archived_reason text;

create index if not exists idx_season_images_archived
  on core.season_images (archived_at)
  where archived_at is not null;

commit;
