-- Hot-path indexes for social analytics reads and ingest polling.

create index if not exists idx_social_scrape_runs_season_scope_created_at
  on social.scrape_runs (season_id, source_scope, created_at desc);

create index if not exists idx_social_scrape_runs_season_status_created_at
  on social.scrape_runs (season_id, status, created_at desc);

create index if not exists idx_social_scrape_jobs_season_run_created_at
  on social.scrape_jobs (season_id, run_id, created_at desc);

create index if not exists idx_social_scrape_jobs_season_status_created_at
  on social.scrape_jobs (season_id, status, created_at desc);

create index if not exists idx_social_scrape_jobs_season_platform_created_at
  on social.scrape_jobs (season_id, platform, created_at desc);

create index if not exists idx_social_instagram_posts_season_posted_at
  on social.instagram_posts (season_id, posted_at desc);

create index if not exists idx_social_instagram_posts_season_account_posted_at
  on social.instagram_posts (
    season_id,
    lower(coalesce(nullif(username, ''), source_account, '')),
    posted_at desc
  );

create index if not exists idx_social_instagram_comments_season_created_at
  on social.instagram_comments (season_id, created_at desc);

create index if not exists idx_social_instagram_comments_post_created_at
  on social.instagram_comments (post_id, created_at desc);

create index if not exists idx_social_tiktok_posts_season_posted_at
  on social.tiktok_posts (season_id, posted_at desc);

create index if not exists idx_social_tiktok_posts_season_account_posted_at
  on social.tiktok_posts (
    season_id,
    lower(coalesce(nullif(username, ''), source_account, '')),
    posted_at desc
  );

create index if not exists idx_social_tiktok_comments_season_created_at
  on social.tiktok_comments (season_id, created_at desc);

create index if not exists idx_social_tiktok_comments_post_created_at
  on social.tiktok_comments (post_id, created_at desc);

create index if not exists idx_social_youtube_videos_season_published_at
  on social.youtube_videos (season_id, published_at desc);

create index if not exists idx_social_youtube_videos_season_account_published_at
  on social.youtube_videos (
    season_id,
    lower(coalesce(nullif(channel_title, ''), source_account, '')),
    published_at desc
  );

create index if not exists idx_social_youtube_comments_season_created_at
  on social.youtube_comments (season_id, created_at desc);

create index if not exists idx_social_youtube_comments_video_created_at
  on social.youtube_comments (video_id, created_at desc);

create index if not exists idx_social_twitter_tweets_season_created_at
  on social.twitter_tweets (season_id, created_at desc);

create index if not exists idx_social_twitter_tweets_season_reply_created_at
  on social.twitter_tweets (season_id, is_reply, created_at desc);

create index if not exists idx_social_twitter_tweets_season_account_created_at
  on social.twitter_tweets (
    season_id,
    lower(coalesce(nullif(username, ''), source_account, '')),
    created_at desc
  );
