-- Add hosted_user_avatar_url for mirrored/cached profile pictures.
-- Content-addressed S3 storage: same image bytes → same key → natural dedup.
-- Old avatars are preserved when an account changes theirs (new SHA256 → new key).
alter table if exists social.tiktok_posts
  add column if not exists hosted_user_avatar_url text;

alter table if exists social.youtube_videos
  add column if not exists hosted_user_avatar_url text;

alter table if exists social.facebook_posts
  add column if not exists hosted_user_avatar_url text;

alter table if exists social.meta_threads_posts
  add column if not exists hosted_user_avatar_url text;

alter table if exists social.twitter_tweets
  add column if not exists hosted_user_avatar_url text;
