-- Add YouTube shorts classification fields for analytics and filtering.
alter table social.youtube_videos
  add column if not exists is_short boolean not null default false,
  add column if not exists source_surface text;

create index if not exists youtube_videos_season_short_published_idx
  on social.youtube_videos (season_id, is_short, published_at desc);
