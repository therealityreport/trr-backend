begin;

alter table social.instagram_comments
  add column if not exists media_urls jsonb not null default '[]'::jsonb,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

alter table social.tiktok_comments
  add column if not exists media_urls jsonb not null default '[]'::jsonb,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

alter table social.youtube_comments
  add column if not exists media_urls jsonb not null default '[]'::jsonb,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

-- Widen scrape_jobs job_type check for comment-media mirror jobs.
do $$
declare
  r record;
begin
  for r in
    select c.conname
    from pg_constraint c
    where c.conrelid = 'social.scrape_jobs'::regclass
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%job_type%'
      and pg_get_constraintdef(c.oid) ilike '%instagram_media_mirror%'
  loop
    execute format('alter table social.scrape_jobs drop constraint %I', r.conname);
  end loop;
end $$;

alter table social.scrape_jobs
  add constraint scrape_jobs_job_type_check_v3
  check (
    job_type in (
      'posts',
      'comments',
      'search',
      'replies',
      'instagram_media_mirror',
      'tiktok_media_mirror',
      'youtube_media_mirror',
      'twitter_media_mirror',
      'instagram_comment_media_mirror',
      'tiktok_comment_media_mirror',
      'youtube_comment_media_mirror',
      'twitter_comment_media_mirror'
    )
  );

create index if not exists idx_instagram_comments_media_mirror_pending
  on social.instagram_comments (season_id, created_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

create index if not exists idx_tiktok_comments_media_mirror_pending
  on social.tiktok_comments (season_id, created_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

create index if not exists idx_youtube_comments_media_mirror_pending
  on social.youtube_comments (season_id, created_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

create index if not exists idx_twitter_replies_media_mirror_pending
  on social.twitter_tweets (season_id, created_at desc)
  where is_reply = true and coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

commit;
