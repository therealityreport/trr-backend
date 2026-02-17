begin;

alter table social.instagram_comments
  add column if not exists is_missing boolean not null default false,
  add column if not exists missing_at timestamptz,
  add column if not exists first_seen_at timestamptz not null default now(),
  add column if not exists last_seen_at timestamptz not null default now(),
  add column if not exists last_seen_run_id uuid references social.scrape_runs (id) on delete set null;

alter table social.tiktok_comments
  add column if not exists is_missing boolean not null default false,
  add column if not exists missing_at timestamptz,
  add column if not exists first_seen_at timestamptz not null default now(),
  add column if not exists last_seen_at timestamptz not null default now(),
  add column if not exists last_seen_run_id uuid references social.scrape_runs (id) on delete set null;

alter table social.youtube_comments
  add column if not exists is_missing boolean not null default false,
  add column if not exists missing_at timestamptz,
  add column if not exists first_seen_at timestamptz not null default now(),
  add column if not exists last_seen_at timestamptz not null default now(),
  add column if not exists last_seen_run_id uuid references social.scrape_runs (id) on delete set null;

alter table social.twitter_tweets
  add column if not exists is_missing boolean not null default false,
  add column if not exists missing_at timestamptz,
  add column if not exists first_seen_at timestamptz not null default now(),
  add column if not exists last_seen_at timestamptz not null default now(),
  add column if not exists last_seen_run_id uuid references social.scrape_runs (id) on delete set null;

create index if not exists ig_comments_post_missing_created_idx
  on social.instagram_comments (post_id, is_missing, created_at desc);

create index if not exists tt_comments_post_missing_created_idx
  on social.tiktok_comments (post_id, is_missing, created_at desc);

create index if not exists yt_comments_video_missing_created_idx
  on social.youtube_comments (video_id, is_missing, created_at desc);

create index if not exists tw_tweets_reply_missing_created_idx
  on social.twitter_tweets (reply_to_tweet_id, is_missing, created_at desc)
  where is_reply = true and reply_to_tweet_id is not null;

commit;
