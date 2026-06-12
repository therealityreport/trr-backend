CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_social_instagram_comments_source_account_norm_created_at_id
  ON social.instagram_comments ((ltrim(lower(source_account), '@')), created_at DESC, id DESC)
  WHERE nullif(btrim(source_account), '') IS NOT NULL
    AND coalesce(is_missing, false) = false;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_social_tiktok_comments_source_account_norm_created_at_id
  ON social.tiktok_comments ((ltrim(lower(source_account), '@')), created_at DESC, id DESC)
  WHERE nullif(btrim(source_account), '') IS NOT NULL
    AND coalesce(is_missing, false) = false;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_social_youtube_comments_source_account_norm_created_at_id
  ON social.youtube_comments ((ltrim(lower(source_account), '@')), created_at DESC, id DESC)
  WHERE nullif(btrim(source_account), '') IS NOT NULL
    AND coalesce(is_missing, false) = false;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_social_facebook_comments_source_account_norm_created_at_id
  ON social.facebook_comments ((ltrim(lower(source_account), '@')), created_at DESC, id DESC)
  WHERE nullif(btrim(source_account), '') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_social_meta_threads_comments_source_account_norm_created_at_id
  ON social.meta_threads_comments ((ltrim(lower(source_account), '@')), created_at DESC, id DESC)
  WHERE nullif(btrim(source_account), '') IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_social_twitter_tweets_source_account_norm_interactions_created_at_id
  ON social.twitter_tweets ((ltrim(lower(source_account), '@')), created_at DESC, id DESC)
  WHERE nullif(btrim(source_account), '') IS NOT NULL
    AND coalesce(is_missing, false) = false
    AND (coalesce(is_reply, false) = true OR coalesce(is_quote, false) = true);
