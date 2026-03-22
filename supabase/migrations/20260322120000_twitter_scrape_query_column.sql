-- 20260322120000_twitter_scrape_query_column.sql
-- Add scrape_query to twitter_tweets for standalone hashtag/mention scrapes.
-- Nullable: existing season-pipeline rows do not need it.

begin;

alter table social.twitter_tweets
  add column if not exists scrape_query text;

comment on column social.twitter_tweets.scrape_query is
  'Search term that produced this row in a standalone (non-season) scrape, e.g. "#RHOSLC" or "@BravoTV". NULL for season-pipeline rows.';

create index if not exists twitter_tweets_scrape_query_idx
  on social.twitter_tweets (scrape_query)
  where scrape_query is not null;

commit;
