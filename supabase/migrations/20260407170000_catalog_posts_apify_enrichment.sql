-- Add columns discovered from Apify's richer Instagram post data:
--   music_info:       audio attribution (song name, artist, original audio flag)
--   audio_url:        direct MP4 link to the audio track
--   paid_partnership: Instagram branded content / paid partnership flag
--   child_posts_data: full carousel slide metadata (per-slide URLs, tags, alt text)
--   owner_username:   actual post owner (may differ from the scraped account on collabs)
--   video_play_count: separate from views — Instagram tracks "plays" vs "views" differently
--   video_duration:   duration in seconds for video/reel posts

begin;

-- Instagram
alter table social.instagram_account_catalog_posts
  add column if not exists music_info jsonb not null default '{}'::jsonb,
  add column if not exists audio_url text,
  add column if not exists paid_partnership boolean not null default false,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb,
  add column if not exists owner_username text,
  add column if not exists video_play_count bigint not null default 0,
  add column if not exists video_duration numeric;

-- TikTok (same shared schema)
alter table social.tiktok_account_catalog_posts
  add column if not exists music_info jsonb not null default '{}'::jsonb,
  add column if not exists audio_url text,
  add column if not exists paid_partnership boolean not null default false,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb,
  add column if not exists owner_username text,
  add column if not exists video_play_count bigint not null default 0,
  add column if not exists video_duration numeric;

-- Twitter
alter table social.twitter_account_catalog_posts
  add column if not exists music_info jsonb not null default '{}'::jsonb,
  add column if not exists audio_url text,
  add column if not exists paid_partnership boolean not null default false,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb,
  add column if not exists owner_username text,
  add column if not exists video_play_count bigint not null default 0,
  add column if not exists video_duration numeric;

-- Threads
alter table social.threads_account_catalog_posts
  add column if not exists music_info jsonb not null default '{}'::jsonb,
  add column if not exists audio_url text,
  add column if not exists paid_partnership boolean not null default false,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb,
  add column if not exists owner_username text,
  add column if not exists video_play_count bigint not null default 0,
  add column if not exists video_duration numeric;

commit;
