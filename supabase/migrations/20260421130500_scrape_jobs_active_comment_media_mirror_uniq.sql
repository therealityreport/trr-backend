begin;

create unique index if not exists scrape_jobs_active_comment_media_mirror_uniq
  on social.scrape_jobs (
    platform,
    (
      coalesce(
        nullif(config->>'comment_db_id', ''),
        case
          when coalesce(config->>'post_id', '') <> '' and coalesce(config->>'comment_id', '') <> ''
            then (config->>'post_id') || ':' || (config->>'comment_id')
          else null
        end
      )
    )
  )
  where status in ('queued', 'pending', 'retrying', 'running')
    and coalesce(config->>'stage', metadata->>'stage', job_type) = 'comment_media_mirror';

comment on index social.scrape_jobs_active_comment_media_mirror_uniq is
  'At most one active comment_media_mirror scrape job per Instagram comment identity across runs.';

commit;
