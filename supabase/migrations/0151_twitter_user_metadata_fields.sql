-- Add first-class Twitter/X author metadata fields for replies/quotes UI payloads.

alter table social.twitter_tweets
  add column if not exists user_id text,
  add column if not exists user_profile_url text,
  add column if not exists user_avatar_url text;

