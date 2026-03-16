begin;

alter table if exists social.twitter_tweets
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

commit;
