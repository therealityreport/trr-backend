begin;

create index if not exists scrape_jobs_claim_shard_scheduler_idx
  on social.scrape_jobs (status, available_at, priority, created_at)
  where status in ('queued', 'pending', 'retrying');

create index if not exists idx_social_instagram_posts_week_summary_account_norm
  on social.instagram_posts (
    season_id,
    posted_at desc,
    (ltrim(lower(coalesce(nullif(username, ''), nullif(source_account, ''), '')), '@'))
  );

create index if not exists idx_social_tiktok_posts_week_summary_account_norm
  on social.tiktok_posts (
    season_id,
    posted_at desc,
    (ltrim(lower(coalesce(nullif(username, ''), nullif(source_account, ''), '')), '@'))
  );

create index if not exists idx_social_twitter_tweets_week_summary_account_norm
  on social.twitter_tweets (
    season_id,
    created_at desc,
    (ltrim(lower(coalesce(nullif(username, ''), nullif(source_account, ''), '')), '@'))
  )
  where is_reply = false;

create index if not exists idx_social_youtube_videos_week_summary_account_norm
  on social.youtube_videos (
    season_id,
    published_at desc,
    (ltrim(lower(coalesce(nullif(channel_title, ''), nullif(source_account, ''), '')), '@'))
  );

commit;
