begin;

-- External social media scrape data storage
-- Schema: social
-- Tables for storing scraped content from Instagram, TikTok, YouTube, and Twitter/X

-- ---------------------------------------------------------------------------
-- social.scrape_jobs - Track scrape operations
-- ---------------------------------------------------------------------------

create table social.scrape_jobs (
  id uuid primary key default gen_random_uuid(),
  platform text not null check (platform in ('instagram', 'tiktok', 'youtube', 'twitter')),
  job_type text not null check (job_type in ('posts', 'comments', 'search', 'replies')),
  config jsonb not null default '{}',
  status text not null default 'pending' check (status in ('pending', 'running', 'completed', 'failed')),
  items_found integer not null default 0,
  error_message text,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now(),

  -- Optional entity associations
  show_id uuid references core.shows (id) on delete set null,
  person_id uuid references core.people (id) on delete set null
);

create index scrape_jobs_platform_status_idx on social.scrape_jobs (platform, status);
create index scrape_jobs_created_at_idx on social.scrape_jobs (created_at desc);

-- ---------------------------------------------------------------------------
-- social.instagram_posts - Instagram posts and reels
-- ---------------------------------------------------------------------------

create table social.instagram_posts (
  id uuid primary key default gen_random_uuid(),
  shortcode text not null unique,
  media_id text,
  username text not null,
  user_id text,
  caption text,
  media_type text,
  media_urls jsonb not null default '[]',
  likes integer not null default 0,
  comments_count integer not null default 0,
  views integer,
  posted_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb,

  -- Entity associations
  show_id uuid references core.shows (id) on delete set null,
  person_id uuid references core.people (id) on delete set null
);

create index instagram_posts_username_idx on social.instagram_posts (username);
create index instagram_posts_posted_at_idx on social.instagram_posts (posted_at desc);
create index instagram_posts_show_id_idx on social.instagram_posts (show_id) where show_id is not null;
create index instagram_posts_person_id_idx on social.instagram_posts (person_id) where person_id is not null;

-- ---------------------------------------------------------------------------
-- social.instagram_comments - Instagram comments with reply support
-- ---------------------------------------------------------------------------

create table social.instagram_comments (
  id uuid primary key default gen_random_uuid(),
  comment_id text not null unique,
  post_id uuid not null references social.instagram_posts (id) on delete cascade,
  parent_comment_id uuid references social.instagram_comments (id) on delete cascade,
  username text not null,
  user_id text,
  text text not null,
  likes integer not null default 0,
  is_reply boolean not null default false,
  reply_count integer not null default 0,
  created_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb
);

create index instagram_comments_post_id_idx on social.instagram_comments (post_id);
create index instagram_comments_parent_comment_id_idx on social.instagram_comments (parent_comment_id) where parent_comment_id is not null;
create index instagram_comments_created_at_idx on social.instagram_comments (created_at desc);

-- ---------------------------------------------------------------------------
-- social.tiktok_posts - TikTok videos
-- ---------------------------------------------------------------------------

create table social.tiktok_posts (
  id uuid primary key default gen_random_uuid(),
  video_id text not null unique,
  aweme_id text,
  username text not null,
  user_id text,
  nickname text,
  description text,
  hashtags jsonb not null default '[]',
  music_info jsonb,
  likes integer not null default 0,
  comments_count integer not null default 0,
  shares integer not null default 0,
  views integer not null default 0,
  duration_seconds integer,
  posted_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb,

  -- Entity associations
  show_id uuid references core.shows (id) on delete set null,
  person_id uuid references core.people (id) on delete set null
);

create index tiktok_posts_username_idx on social.tiktok_posts (username);
create index tiktok_posts_posted_at_idx on social.tiktok_posts (posted_at desc);
create index tiktok_posts_show_id_idx on social.tiktok_posts (show_id) where show_id is not null;
create index tiktok_posts_person_id_idx on social.tiktok_posts (person_id) where person_id is not null;

-- ---------------------------------------------------------------------------
-- social.tiktok_comments - TikTok comments with reply support
-- ---------------------------------------------------------------------------

create table social.tiktok_comments (
  id uuid primary key default gen_random_uuid(),
  comment_id text not null unique,
  post_id uuid not null references social.tiktok_posts (id) on delete cascade,
  parent_comment_id uuid references social.tiktok_comments (id) on delete cascade,
  username text not null,
  user_id text,
  nickname text,
  text text not null,
  likes integer not null default 0,
  is_reply boolean not null default false,
  reply_count integer not null default 0,
  created_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb
);

create index tiktok_comments_post_id_idx on social.tiktok_comments (post_id);
create index tiktok_comments_parent_comment_id_idx on social.tiktok_comments (parent_comment_id) where parent_comment_id is not null;
create index tiktok_comments_created_at_idx on social.tiktok_comments (created_at desc);

-- ---------------------------------------------------------------------------
-- social.youtube_videos - YouTube videos
-- ---------------------------------------------------------------------------

create table social.youtube_videos (
  id uuid primary key default gen_random_uuid(),
  video_id text not null unique,
  channel_id text,
  channel_title text,
  title text not null,
  description text,
  duration text,
  duration_seconds integer,
  views integer not null default 0,
  likes integer not null default 0,
  comments_count integer not null default 0,
  thumbnail_url text,
  published_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb,

  -- Entity associations
  show_id uuid references core.shows (id) on delete set null,
  person_id uuid references core.people (id) on delete set null
);

create index youtube_videos_channel_id_idx on social.youtube_videos (channel_id);
create index youtube_videos_published_at_idx on social.youtube_videos (published_at desc);
create index youtube_videos_show_id_idx on social.youtube_videos (show_id) where show_id is not null;
create index youtube_videos_person_id_idx on social.youtube_videos (person_id) where person_id is not null;

-- ---------------------------------------------------------------------------
-- social.youtube_comments - YouTube comments with reply support
-- ---------------------------------------------------------------------------

create table social.youtube_comments (
  id uuid primary key default gen_random_uuid(),
  comment_id text not null unique,
  video_id uuid not null references social.youtube_videos (id) on delete cascade,
  parent_comment_id uuid references social.youtube_comments (id) on delete cascade,
  author text not null,
  author_channel_id text,
  text text not null,
  likes integer not null default 0,
  is_reply boolean not null default false,
  reply_count integer not null default 0,
  created_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb
);

create index youtube_comments_video_id_idx on social.youtube_comments (video_id);
create index youtube_comments_parent_comment_id_idx on social.youtube_comments (parent_comment_id) where parent_comment_id is not null;
create index youtube_comments_created_at_idx on social.youtube_comments (created_at desc);

-- ---------------------------------------------------------------------------
-- social.twitter_tweets - Twitter/X tweets (includes original tweets and replies)
-- ---------------------------------------------------------------------------

create table social.twitter_tweets (
  id uuid primary key default gen_random_uuid(),
  tweet_id text not null unique,
  username text not null,
  display_name text,
  user_verified boolean not null default false,
  text text not null,
  hashtags jsonb not null default '[]',
  mentions jsonb not null default '[]',
  media_urls jsonb not null default '[]',
  likes integer not null default 0,
  retweets integer not null default 0,
  replies_count integer not null default 0,
  quotes integer not null default 0,
  views integer not null default 0,
  is_reply boolean not null default false,
  is_retweet boolean not null default false,
  is_quote boolean not null default false,
  reply_to_tweet_id text,
  quoted_tweet_id text,
  created_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb,

  -- Entity associations
  show_id uuid references core.shows (id) on delete set null,
  person_id uuid references core.people (id) on delete set null
);

create index twitter_tweets_username_idx on social.twitter_tweets (username);
create index twitter_tweets_created_at_idx on social.twitter_tweets (created_at desc);
create index twitter_tweets_reply_to_tweet_id_idx on social.twitter_tweets (reply_to_tweet_id) where reply_to_tweet_id is not null;
create index twitter_tweets_show_id_idx on social.twitter_tweets (show_id) where show_id is not null;
create index twitter_tweets_person_id_idx on social.twitter_tweets (person_id) where person_id is not null;

-- ---------------------------------------------------------------------------
-- Grants (privileges) for Supabase API roles
-- ---------------------------------------------------------------------------

-- Public read on scrape data
grant select on table
  social.scrape_jobs,
  social.instagram_posts,
  social.instagram_comments,
  social.tiktok_posts,
  social.tiktok_comments,
  social.youtube_videos,
  social.youtube_comments,
  social.twitter_tweets
to anon, authenticated;

-- Service role can manage everything (used by backend scripts)
grant all privileges on table
  social.scrape_jobs,
  social.instagram_posts,
  social.instagram_comments,
  social.tiktok_posts,
  social.tiktok_comments,
  social.youtube_videos,
  social.youtube_comments,
  social.twitter_tweets
to service_role;

-- ---------------------------------------------------------------------------
-- Row Level Security (RLS)
-- ---------------------------------------------------------------------------

alter table social.scrape_jobs enable row level security;
alter table social.instagram_posts enable row level security;
alter table social.instagram_comments enable row level security;
alter table social.tiktok_posts enable row level security;
alter table social.tiktok_comments enable row level security;
alter table social.youtube_videos enable row level security;
alter table social.youtube_comments enable row level security;
alter table social.twitter_tweets enable row level security;

-- Public read policies (scraped data is public)
create policy scrape_jobs_public_read on social.scrape_jobs
for select to anon, authenticated
using (true);

create policy instagram_posts_public_read on social.instagram_posts
for select to anon, authenticated
using (true);

create policy instagram_comments_public_read on social.instagram_comments
for select to anon, authenticated
using (true);

create policy tiktok_posts_public_read on social.tiktok_posts
for select to anon, authenticated
using (true);

create policy tiktok_comments_public_read on social.tiktok_comments
for select to anon, authenticated
using (true);

create policy youtube_videos_public_read on social.youtube_videos
for select to anon, authenticated
using (true);

create policy youtube_comments_public_read on social.youtube_comments
for select to anon, authenticated
using (true);

create policy twitter_tweets_public_read on social.twitter_tweets
for select to anon, authenticated
using (true);

-- Service role bypass for all operations (no explicit policies needed, RLS bypassed)

commit;
