begin;

alter table if exists social.youtube_videos
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

commit;
