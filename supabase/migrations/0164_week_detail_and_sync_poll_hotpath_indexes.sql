-- Additional hot-path indexes for week-detail reads and sync polling.

create index if not exists idx_social_instagram_posts_season_posted_at
  on social.instagram_posts (season_id, posted_at desc);

create index if not exists idx_social_tiktok_posts_season_posted_at
  on social.tiktok_posts (season_id, posted_at desc);

create index if not exists idx_social_youtube_videos_season_published_at
  on social.youtube_videos (season_id, published_at desc);

create index if not exists idx_social_twitter_tweets_season_non_reply_created_at
  on social.twitter_tweets (season_id, created_at desc)
  where is_reply = false;

create index if not exists idx_social_facebook_posts_season_posted_at
  on social.facebook_posts (season_id, posted_at desc);

create index if not exists idx_social_meta_threads_posts_season_posted_at
  on social.meta_threads_posts (season_id, posted_at desc);

create index if not exists idx_social_scrape_jobs_season_run_created_at
  on social.scrape_jobs (season_id, run_id, created_at desc);

create index if not exists idx_social_scrape_runs_season_created_at
  on social.scrape_runs (season_id, created_at desc);
