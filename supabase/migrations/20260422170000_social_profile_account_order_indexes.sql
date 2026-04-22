-- Verified against the current runtime schema before landing:
-- social.instagram_posts -> posted_at
-- social.tiktok_posts -> posted_at
-- social.facebook_posts -> posted_at
--
-- social.youtube_posts and social.twitter_posts were intentionally omitted here
-- because those tables were not present in the verified runtime schema for the
-- current social-profile path. This migration only covers the real post tables
-- that currently exist.

BEGIN;

CREATE INDEX IF NOT EXISTS idx_social_instagram_posts_source_account_lower_posted_at_id
ON social.instagram_posts (lower(source_account), posted_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_social_tiktok_posts_source_account_lower_posted_at_id
ON social.tiktok_posts (lower(source_account), posted_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_social_facebook_posts_source_account_lower_posted_at_id
ON social.facebook_posts (lower(source_account), posted_at DESC, id DESC);

COMMIT;
