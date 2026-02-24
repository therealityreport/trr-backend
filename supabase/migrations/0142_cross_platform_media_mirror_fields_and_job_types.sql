begin;

-- Cross-platform hosted mirror diagnostics/URLs.
alter table social.tiktok_posts
  add column if not exists media_urls jsonb not null default '[]'::jsonb,
  add column if not exists hosted_thumbnail_url text,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

alter table social.youtube_videos
  add column if not exists hosted_thumbnail_url text,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

alter table social.twitter_tweets
  add column if not exists hosted_thumbnail_url text,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

-- Widen job_type check to include media-mirror queue jobs.
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
      and pg_get_constraintdef(c.oid) ilike '%posts%'
      and pg_get_constraintdef(c.oid) ilike '%comments%'
  loop
    execute format('alter table social.scrape_jobs drop constraint %I', r.conname);
  end loop;
end $$;

alter table social.scrape_jobs
  add constraint scrape_jobs_job_type_check_v2
  check (
    job_type in (
      'posts',
      'comments',
      'search',
      'replies',
      'instagram_media_mirror',
      'tiktok_media_mirror',
      'youtube_media_mirror',
      'twitter_media_mirror'
    )
  );

-- Fast requeue scans by platform + status.
create index if not exists idx_tiktok_posts_media_mirror_pending
  on social.tiktok_posts (season_id, posted_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

create index if not exists idx_youtube_videos_media_mirror_pending
  on social.youtube_videos (season_id, published_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

create index if not exists idx_twitter_tweets_media_mirror_pending
  on social.twitter_tweets (season_id, created_at desc)
  where is_reply = false and coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

commit;
