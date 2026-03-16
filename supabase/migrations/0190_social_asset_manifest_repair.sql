begin;

alter table if exists social.instagram_posts
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

alter table if exists social.tiktok_posts
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

alter table if exists social.youtube_videos
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

alter table if exists social.twitter_tweets
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

alter table if exists social.facebook_posts
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

alter table if exists social.meta_threads_posts
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

commit;
