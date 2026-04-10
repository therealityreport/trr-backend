-- Add Apify enrichment columns to YouTube and Facebook catalog tables.
-- These were missed in 20260407170000_catalog_posts_apify_enrichment.sql which
-- only covered Instagram, TikTok, Twitter, and Threads.

begin;

-- YouTube
alter table social.youtube_account_catalog_posts
  add column if not exists music_info jsonb not null default '{}'::jsonb,
  add column if not exists audio_url text,
  add column if not exists paid_partnership boolean not null default false,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb,
  add column if not exists owner_username text,
  add column if not exists video_play_count bigint not null default 0,
  add column if not exists video_duration numeric;

-- Facebook
alter table social.facebook_account_catalog_posts
  add column if not exists music_info jsonb not null default '{}'::jsonb,
  add column if not exists audio_url text,
  add column if not exists paid_partnership boolean not null default false,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb,
  add column if not exists owner_username text,
  add column if not exists video_play_count bigint not null default 0,
  add column if not exists video_duration numeric;

commit;
