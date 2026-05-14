BEGIN;

CREATE TABLE IF NOT EXISTS social.twitter_interaction_fetch_state (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  source_account text NOT NULL,
  root_source_id text NOT NULL,
  interaction_kind text NOT NULL,
  strategy text NOT NULL DEFAULT 'default',
  reported_count integer NOT NULL DEFAULT 0,
  saved_count_before integer NOT NULL DEFAULT 0,
  saved_count_after integer NOT NULL DEFAULT 0,
  unique_saved_delta integer NOT NULL DEFAULT 0,
  duplicate_count integer NOT NULL DEFAULT 0,
  off_root_count integer NOT NULL DEFAULT 0,
  pages_scanned integer NOT NULL DEFAULT 0,
  last_cursor text,
  last_ranking text,
  consecutive_no_new_pages integer NOT NULL DEFAULT 0,
  status text NOT NULL DEFAULT 'pending',
  exhaustion_reason text,
  last_job_id uuid,
  last_error_code text,
  next_retry_at timestamptz,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT twitter_interaction_fetch_state_kind_check
    CHECK (interaction_kind IN ('reply', 'quote')),
  CONSTRAINT twitter_interaction_fetch_state_status_check
    CHECK (status IN ('pending', 'running', 'completed', 'exhausted', 'rate_limited', 'auth_blocked', 'failed')),
  CONSTRAINT twitter_interaction_fetch_state_counts_check
    CHECK (
      reported_count >= 0
      AND saved_count_before >= 0
      AND saved_count_after >= 0
      AND unique_saved_delta >= 0
      AND duplicate_count >= 0
      AND off_root_count >= 0
      AND pages_scanned >= 0
      AND consecutive_no_new_pages >= 0
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS twitter_interaction_fetch_state_root_kind_strategy_idx
  ON social.twitter_interaction_fetch_state (
    lower(source_account),
    root_source_id,
    interaction_kind,
    strategy
  );

CREATE INDEX IF NOT EXISTS twitter_interaction_fetch_state_status_retry_idx
  ON social.twitter_interaction_fetch_state (status, next_retry_at);

CREATE INDEX IF NOT EXISTS twitter_interaction_fetch_state_last_job_id_idx
  ON social.twitter_interaction_fetch_state (last_job_id)
  WHERE last_job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS twitter_tweets_account_thread_root_replies_idx
  ON social.twitter_tweets (lower(source_account), thread_root_tweet_id)
  WHERE is_reply IS TRUE AND thread_root_tweet_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS twitter_tweets_account_quoted_quotes_idx
  ON social.twitter_tweets (lower(source_account), quoted_tweet_id)
  WHERE is_quote IS TRUE AND quoted_tweet_id IS NOT NULL;

COMMIT;
