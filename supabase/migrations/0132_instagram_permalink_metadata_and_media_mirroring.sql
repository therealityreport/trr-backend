begin;

alter table social.instagram_posts
  add column if not exists post_format text,
  add column if not exists profile_tags jsonb not null default '[]'::jsonb,
  add column if not exists collaborators jsonb not null default '[]'::jsonb,
  add column if not exists hashtags jsonb not null default '[]'::jsonb,
  add column if not exists mentions jsonb not null default '[]'::jsonb,
  add column if not exists duration_seconds integer,
  add column if not exists metadata_source text,
  add column if not exists metadata_scraped_at timestamptz,
  add column if not exists metadata_error text,
  add column if not exists hosted_thumbnail_url text,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text;

commit;
