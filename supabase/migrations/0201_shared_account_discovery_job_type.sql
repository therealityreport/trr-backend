begin;

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
  add constraint scrape_jobs_job_type_check_v6
  check (
    job_type in (
      'posts',
      'comments',
      'search',
      'replies',
      'shared_account_posts',
      'shared_account_discovery',
      'post_classify',
      'season_materialize',
      'analytics_refresh',
      'instagram_media_mirror',
      'tiktok_media_mirror',
      'youtube_media_mirror',
      'twitter_media_mirror',
      'facebook_media_mirror',
      'threads_media_mirror',
      'instagram_comment_media_mirror',
      'tiktok_comment_media_mirror',
      'youtube_comment_media_mirror',
      'twitter_comment_media_mirror',
      'facebook_comment_media_mirror',
      'threads_comment_media_mirror'
    )
  );

commit;
