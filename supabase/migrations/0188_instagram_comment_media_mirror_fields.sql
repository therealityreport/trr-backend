begin;

alter table social.instagram_comments
  add column if not exists media_urls jsonb not null default '[]'::jsonb,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

create index if not exists idx_instagram_comments_media_mirror_pending
  on social.instagram_comments (season_id, created_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed')
    and jsonb_array_length(coalesce(media_urls, '[]'::jsonb)) > 0;

commit;
