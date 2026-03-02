-- Add additive author-avatar columns for non-Instagram, non-Twitter platforms.
alter table if exists social.tiktok_posts
  add column if not exists user_avatar_url text;

alter table if exists social.youtube_videos
  add column if not exists user_avatar_url text;

alter table if exists social.facebook_posts
  add column if not exists user_avatar_url text;

alter table if exists social.meta_threads_posts
  add column if not exists user_avatar_url text;
