-- Match the normalized-handle predicates used by the admin social landing
-- progress rollup so Postgres can narrow each platform table before aggregate
-- work begins.
CREATE INDEX IF NOT EXISTS idx_social_instagram_posts_landing_account_norm
  ON social.instagram_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_tiktok_posts_landing_account_norm
  ON social.tiktok_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_twitter_tweets_landing_account_norm
  ON social.twitter_tweets ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_youtube_videos_landing_account_norm
  ON social.youtube_videos ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_facebook_posts_landing_account_norm
  ON social.facebook_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_threads_posts_landing_account_norm
  ON social.meta_threads_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_ig_catalog_posts_landing_account_norm
  ON social.instagram_account_catalog_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_tiktok_catalog_posts_landing_account_norm
  ON social.tiktok_account_catalog_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_twitter_catalog_posts_landing_account_norm
  ON social.twitter_account_catalog_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_youtube_catalog_posts_landing_account_norm
  ON social.youtube_account_catalog_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_facebook_catalog_posts_landing_account_norm
  ON social.facebook_account_catalog_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_threads_catalog_posts_landing_account_norm
  ON social.threads_account_catalog_posts ((ltrim(lower(coalesce(source_account, '')), '@')));

CREATE INDEX IF NOT EXISTS idx_social_instagram_profiles_landing_account_norm
  ON social.instagram_profiles (
    (ltrim(lower(coalesce(normalized_username, username, source_account, '')), '@')),
    last_scraped_at DESC NULLS LAST,
    updated_at DESC NULLS LAST,
    id
  );

CREATE INDEX IF NOT EXISTS idx_pipeline_socialblade_landing_platform_handle_norm
  ON pipeline.socialblade_growth_data (
    (lower(coalesce(nullif(platform, ''), 'instagram'))),
    (ltrim(lower(coalesce(nullif(account_handle, ''), instagram_handle, '')), '@'))
  );
