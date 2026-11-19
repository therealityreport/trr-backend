-- Public observations must distinguish unknown from measured zero/false.
-- Existing data is unchanged; historical default zeros are not reclassified.
-- Keep public persistence gated until live acceptance and consumer checks pass.
BEGIN;
SET LOCAL lock_timeout = '3s';
ALTER TABLE social.twitter_tweets
  ALTER COLUMN is_thread_part DROP NOT NULL, ALTER COLUMN is_thread_part DROP DEFAULT,
  ALTER COLUMN likes DROP NOT NULL, ALTER COLUMN likes DROP DEFAULT,
  ALTER COLUMN retweets DROP NOT NULL, ALTER COLUMN retweets DROP DEFAULT,
  ALTER COLUMN replies_count DROP NOT NULL, ALTER COLUMN replies_count DROP DEFAULT,
  ALTER COLUMN quotes DROP NOT NULL, ALTER COLUMN quotes DROP DEFAULT,
  ALTER COLUMN views DROP NOT NULL, ALTER COLUMN views DROP DEFAULT,
  ALTER COLUMN bookmarks DROP NOT NULL, ALTER COLUMN bookmarks DROP DEFAULT,
  ALTER COLUMN shares DROP NOT NULL, ALTER COLUMN shares DROP DEFAULT,
  ALTER COLUMN user_verified DROP NOT NULL, ALTER COLUMN user_verified DROP DEFAULT,
  ALTER COLUMN is_reply DROP NOT NULL, ALTER COLUMN is_reply DROP DEFAULT,
  ALTER COLUMN is_retweet DROP NOT NULL, ALTER COLUMN is_retweet DROP DEFAULT,
  ALTER COLUMN is_quote DROP NOT NULL, ALTER COLUMN is_quote DROP DEFAULT;
COMMENT ON COLUMN social.twitter_tweets.raw_data IS
  'Source observation and provenance. Public unknowns are null; historical zero defaults are not verified measurements.';
COMMIT;
