-- 20260322153000_twitter_scrape_query_runs.sql
-- Preserve standalone Twitter scrape provenance per query/run without
-- overwriting prior query membership on social.twitter_tweets rows.

begin;

create table if not exists social.twitter_scrape_queries (
  id uuid primary key default gen_random_uuid(),
  scrape_query_label text not null,
  raw_query text not null,
  normalized_search_query text not null,
  window_start_day date not null,
  window_end_day_exclusive date not null,
  requested_via text not null,
  complete boolean not null default false,
  posts_checked integer not null default 0,
  tweets_found integer not null default 0,
  retrieval_meta jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint twitter_scrape_queries_window_order check (window_end_day_exclusive > window_start_day),
  constraint twitter_scrape_queries_requested_via_not_blank check (btrim(requested_via) <> ''),
  constraint twitter_scrape_queries_label_not_blank check (btrim(scrape_query_label) <> ''),
  constraint twitter_scrape_queries_raw_query_not_blank check (btrim(raw_query) <> ''),
  constraint twitter_scrape_queries_normalized_search_query_not_blank check (btrim(normalized_search_query) <> '')
);

create index if not exists twitter_scrape_queries_created_at_idx
  on social.twitter_scrape_queries (created_at desc);

create index if not exists twitter_scrape_queries_raw_query_window_idx
  on social.twitter_scrape_queries (raw_query, window_start_day desc);

create table if not exists social.twitter_scrape_query_tweets (
  scrape_query_id uuid not null references social.twitter_scrape_queries (id) on delete cascade,
  tweet_id text not null references social.twitter_tweets (tweet_id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (scrape_query_id, tweet_id)
);

create index if not exists twitter_scrape_query_tweets_tweet_id_idx
  on social.twitter_scrape_query_tweets (tweet_id);

comment on table social.twitter_scrape_queries is
  'One row per standalone Twitter hashtag/query scrape invocation.';

comment on table social.twitter_scrape_query_tweets is
  'Join table preserving which tweets matched each standalone Twitter scrape run.';

commit;
