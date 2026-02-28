begin;

alter table social.tiktok_comments
  add column if not exists comment_language text,
  add column if not exists is_author_liked boolean,
  add column if not exists aweme_id text,
  add column if not exists parent_source_comment_id text,
  add column if not exists user_url text,
  add column if not exists user_bio text,
  add column if not exists user_avatar_url text,
  add column if not exists user_region text,
  add column if not exists user_language text,
  add column if not exists media_urls jsonb not null default '[]'::jsonb,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

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

create index if not exists idx_tiktok_comments_media_mirror_pending
  on social.tiktok_comments (season_id, created_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed')
    and jsonb_array_length(coalesce(media_urls, '[]'::jsonb)) > 0;

commit;
