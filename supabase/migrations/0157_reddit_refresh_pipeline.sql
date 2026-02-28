-- Canonical Reddit refresh storage for backend-owned period discovery.

create table if not exists social.reddit_refresh_runs (
  id uuid primary key default gen_random_uuid(),
  community_id uuid not null,
  season_id uuid not null references core.seasons (id) on delete cascade,
  period_key text not null,
  subreddit text not null,
  status text not null default 'queued'
    check (status in ('queued', 'running', 'completed', 'partial', 'failed', 'cancelled')),
  request_payload jsonb not null default '{}'::jsonb,
  diagnostics jsonb not null default '{}'::jsonb,
  error_message text,
  total_rows integer not null default 0,
  tracked_flair_rows integer not null default 0,
  matched_rows integer not null default 0,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists reddit_refresh_runs_community_season_period_idx
  on social.reddit_refresh_runs (community_id, season_id, period_key, created_at desc);
create index if not exists reddit_refresh_runs_status_idx
  on social.reddit_refresh_runs (status, created_at desc);

create table if not exists social.reddit_posts (
  id uuid primary key default gen_random_uuid(),
  reddit_post_id text not null unique,
  subreddit text not null,
  title text not null,
  selftext text,
  url text,
  permalink text,
  author text,
  score integer not null default 0,
  num_comments integer not null default 0,
  posted_at timestamptz,
  link_flair_text text,
  canonical_flair_key text,
  source_sorts jsonb not null default '[]'::jsonb,
  raw_payload jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists reddit_posts_subreddit_posted_at_idx
  on social.reddit_posts (subreddit, posted_at desc);
create index if not exists reddit_posts_canonical_flair_key_idx
  on social.reddit_posts (canonical_flair_key);
create index if not exists reddit_posts_created_at_idx
  on social.reddit_posts (created_at desc);

create table if not exists social.reddit_comments (
  id uuid primary key default gen_random_uuid(),
  reddit_comment_id text not null unique,
  reddit_post_id text not null references social.reddit_posts (reddit_post_id) on delete cascade,
  parent_comment_id text,
  author text,
  body text not null default '',
  score integer not null default 0,
  depth integer not null default 0,
  created_at_utc timestamptz,
  raw_payload jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists reddit_comments_post_idx
  on social.reddit_comments (reddit_post_id, created_at_utc desc);
create index if not exists reddit_comments_created_at_idx
  on social.reddit_comments (created_at desc);

create table if not exists social.reddit_period_post_matches (
  id uuid primary key default gen_random_uuid(),
  community_id uuid not null,
  season_id uuid not null references core.seasons (id) on delete cascade,
  period_key text not null,
  period_start timestamptz,
  period_end timestamptz,
  reddit_post_id text not null references social.reddit_posts (reddit_post_id) on delete cascade,
  run_id uuid references social.reddit_refresh_runs (id) on delete set null,
  is_show_match boolean not null default false,
  passes_flair_filter boolean not null default true,
  matched_terms jsonb not null default '[]'::jsonb,
  matched_cast_terms jsonb not null default '[]'::jsonb,
  cross_show_terms jsonb not null default '[]'::jsonb,
  match_score integer not null default 0,
  source_sorts jsonb not null default '[]'::jsonb,
  link_flair_text text,
  canonical_flair_key text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (community_id, season_id, period_key, reddit_post_id)
);

create index if not exists reddit_period_post_matches_period_idx
  on social.reddit_period_post_matches (community_id, season_id, period_key);
create index if not exists reddit_period_post_matches_created_at_idx
  on social.reddit_period_post_matches (created_at desc);
create index if not exists reddit_period_post_matches_flair_key_idx
  on social.reddit_period_post_matches (canonical_flair_key);

grant select on table
  social.reddit_refresh_runs,
  social.reddit_posts,
  social.reddit_comments,
  social.reddit_period_post_matches
to anon, authenticated;

grant all privileges on table
  social.reddit_refresh_runs,
  social.reddit_posts,
  social.reddit_comments,
  social.reddit_period_post_matches
to service_role;

alter table social.reddit_refresh_runs enable row level security;
alter table social.reddit_posts enable row level security;
alter table social.reddit_comments enable row level security;
alter table social.reddit_period_post_matches enable row level security;

drop policy if exists reddit_refresh_runs_public_read on social.reddit_refresh_runs;
create policy reddit_refresh_runs_public_read on social.reddit_refresh_runs
  for select using (true);

drop policy if exists reddit_posts_public_read on social.reddit_posts;
create policy reddit_posts_public_read on social.reddit_posts
  for select using (true);

drop policy if exists reddit_comments_public_read on social.reddit_comments;
create policy reddit_comments_public_read on social.reddit_comments
  for select using (true);

drop policy if exists reddit_period_post_matches_public_read on social.reddit_period_post_matches;
create policy reddit_period_post_matches_public_read on social.reddit_period_post_matches
  for select using (true);
