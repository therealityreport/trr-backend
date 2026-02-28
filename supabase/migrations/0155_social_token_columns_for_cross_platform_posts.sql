begin;

alter table social.tiktok_posts
  add column if not exists mentions jsonb not null default '[]'::jsonb;

alter table social.youtube_videos
  add column if not exists hashtags jsonb not null default '[]'::jsonb,
  add column if not exists mentions jsonb not null default '[]'::jsonb;

alter table social.facebook_posts
  add column if not exists hashtags jsonb not null default '[]'::jsonb,
  add column if not exists mentions jsonb not null default '[]'::jsonb;

alter table social.meta_threads_posts
  add column if not exists hashtags jsonb not null default '[]'::jsonb,
  add column if not exists mentions jsonb not null default '[]'::jsonb;

commit;
