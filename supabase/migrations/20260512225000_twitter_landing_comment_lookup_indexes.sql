BEGIN;

CREATE INDEX IF NOT EXISTS twitter_tweets_thread_root_reply_lookup_idx
  ON social.twitter_tweets (thread_root_tweet_id)
  WHERE is_reply IS TRUE AND thread_root_tweet_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS twitter_tweets_quoted_tweet_lookup_idx
  ON social.twitter_tweets (quoted_tweet_id)
  WHERE is_quote IS TRUE AND quoted_tweet_id IS NOT NULL;

COMMIT;
