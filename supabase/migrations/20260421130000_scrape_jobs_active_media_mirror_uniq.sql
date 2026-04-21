begin;

create unique index if not exists scrape_jobs_active_media_mirror_uniq
  on social.scrape_jobs (platform, (config->>'post_id'))
  where status in ('queued', 'pending', 'retrying', 'running')
    and coalesce(config->>'stage', metadata->>'stage', job_type) = 'media_mirror';

comment on index social.scrape_jobs_active_media_mirror_uniq is
  'At most one active media_mirror scrape job per (platform, post_id) across runs.';

commit;
