begin;

create table if not exists social.instagram_account_catalog_posts (
  id uuid primary key default gen_random_uuid(),
  source_id text not null,
  source_account text not null,
  posted_at timestamptz,
  permalink text,
  title text,
  caption text,
  description text,
  text text,
  media_type text,
  media_urls jsonb not null default '[]'::jsonb,
  thumbnail_url text,
  hashtags jsonb not null default '[]'::jsonb,
  mentions jsonb not null default '[]'::jsonb,
  collaborators jsonb not null default '[]'::jsonb,
  profile_tags jsonb not null default '[]'::jsonb,
  likes bigint not null default 0,
  comments_count bigint not null default 0,
  views bigint not null default 0,
  shares bigint not null default 0,
  retweets bigint not null default 0,
  replies_count bigint not null default 0,
  quotes bigint not null default 0,
  raw_data jsonb not null default '{}'::jsonb,
  assignment_status text not null default 'unassigned'
    check (assignment_status in ('assigned', 'unassigned', 'ambiguous', 'needs_review')),
  assigned_show_id uuid references core.shows (id) on delete set null,
  assigned_season_id uuid references core.seasons (id) on delete set null,
  assignment_source text,
  candidate_matches jsonb not null default '[]'::jsonb,
  last_backfill_run_id uuid references social.scrape_runs (id) on delete set null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source_id)
);

create table if not exists social.tiktok_account_catalog_posts (
  id uuid primary key default gen_random_uuid(),
  source_id text not null,
  source_account text not null,
  posted_at timestamptz,
  permalink text,
  title text,
  caption text,
  description text,
  text text,
  media_type text,
  media_urls jsonb not null default '[]'::jsonb,
  thumbnail_url text,
  hashtags jsonb not null default '[]'::jsonb,
  mentions jsonb not null default '[]'::jsonb,
  collaborators jsonb not null default '[]'::jsonb,
  profile_tags jsonb not null default '[]'::jsonb,
  likes bigint not null default 0,
  comments_count bigint not null default 0,
  views bigint not null default 0,
  shares bigint not null default 0,
  retweets bigint not null default 0,
  replies_count bigint not null default 0,
  quotes bigint not null default 0,
  raw_data jsonb not null default '{}'::jsonb,
  assignment_status text not null default 'unassigned'
    check (assignment_status in ('assigned', 'unassigned', 'ambiguous', 'needs_review')),
  assigned_show_id uuid references core.shows (id) on delete set null,
  assigned_season_id uuid references core.seasons (id) on delete set null,
  assignment_source text,
  candidate_matches jsonb not null default '[]'::jsonb,
  last_backfill_run_id uuid references social.scrape_runs (id) on delete set null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source_id)
);

create table if not exists social.twitter_account_catalog_posts (
  id uuid primary key default gen_random_uuid(),
  source_id text not null,
  source_account text not null,
  posted_at timestamptz,
  permalink text,
  title text,
  caption text,
  description text,
  text text,
  media_type text,
  media_urls jsonb not null default '[]'::jsonb,
  thumbnail_url text,
  hashtags jsonb not null default '[]'::jsonb,
  mentions jsonb not null default '[]'::jsonb,
  collaborators jsonb not null default '[]'::jsonb,
  profile_tags jsonb not null default '[]'::jsonb,
  likes bigint not null default 0,
  comments_count bigint not null default 0,
  views bigint not null default 0,
  shares bigint not null default 0,
  retweets bigint not null default 0,
  replies_count bigint not null default 0,
  quotes bigint not null default 0,
  raw_data jsonb not null default '{}'::jsonb,
  assignment_status text not null default 'unassigned'
    check (assignment_status in ('assigned', 'unassigned', 'ambiguous', 'needs_review')),
  assigned_show_id uuid references core.shows (id) on delete set null,
  assigned_season_id uuid references core.seasons (id) on delete set null,
  assignment_source text,
  candidate_matches jsonb not null default '[]'::jsonb,
  last_backfill_run_id uuid references social.scrape_runs (id) on delete set null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source_id)
);

create table if not exists social.threads_account_catalog_posts (
  id uuid primary key default gen_random_uuid(),
  source_id text not null,
  source_account text not null,
  posted_at timestamptz,
  permalink text,
  title text,
  caption text,
  description text,
  text text,
  media_type text,
  media_urls jsonb not null default '[]'::jsonb,
  thumbnail_url text,
  hashtags jsonb not null default '[]'::jsonb,
  mentions jsonb not null default '[]'::jsonb,
  collaborators jsonb not null default '[]'::jsonb,
  profile_tags jsonb not null default '[]'::jsonb,
  likes bigint not null default 0,
  comments_count bigint not null default 0,
  views bigint not null default 0,
  shares bigint not null default 0,
  retweets bigint not null default 0,
  replies_count bigint not null default 0,
  quotes bigint not null default 0,
  raw_data jsonb not null default '{}'::jsonb,
  assignment_status text not null default 'unassigned'
    check (assignment_status in ('assigned', 'unassigned', 'ambiguous', 'needs_review')),
  assigned_show_id uuid references core.shows (id) on delete set null,
  assigned_season_id uuid references core.seasons (id) on delete set null,
  assignment_source text,
  candidate_matches jsonb not null default '[]'::jsonb,
  last_backfill_run_id uuid references social.scrape_runs (id) on delete set null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source_id)
);

create index if not exists instagram_account_catalog_posts_account_posted_at_idx
  on social.instagram_account_catalog_posts (lower(source_account), posted_at desc nulls last);
create index if not exists tiktok_account_catalog_posts_account_posted_at_idx
  on social.tiktok_account_catalog_posts (lower(source_account), posted_at desc nulls last);
create index if not exists twitter_account_catalog_posts_account_posted_at_idx
  on social.twitter_account_catalog_posts (lower(source_account), posted_at desc nulls last);
create index if not exists threads_account_catalog_posts_account_posted_at_idx
  on social.threads_account_catalog_posts (lower(source_account), posted_at desc nulls last);

create index if not exists instagram_account_catalog_posts_assignment_status_idx
  on social.instagram_account_catalog_posts (assignment_status, posted_at desc nulls last);
create index if not exists tiktok_account_catalog_posts_assignment_status_idx
  on social.tiktok_account_catalog_posts (assignment_status, posted_at desc nulls last);
create index if not exists twitter_account_catalog_posts_assignment_status_idx
  on social.twitter_account_catalog_posts (assignment_status, posted_at desc nulls last);
create index if not exists threads_account_catalog_posts_assignment_status_idx
  on social.threads_account_catalog_posts (assignment_status, posted_at desc nulls last);

create table if not exists social.account_hashtag_review_queue (
  id uuid primary key default gen_random_uuid(),
  platform text not null
    check (platform in ('instagram', 'tiktok', 'twitter', 'threads')),
  source_scope text not null default 'bravo'
    check (source_scope in ('bravo', 'creator', 'community')),
  account_handle text not null,
  normalized_hashtag text not null,
  display_hashtag text,
  review_status text not null default 'pending'
    check (review_status in ('pending', 'resolved_show_hashtag', 'resolved_non_show')),
  usage_count integer not null default 0,
  sample_post_ids jsonb not null default '[]'::jsonb,
  sample_source_ids jsonb not null default '[]'::jsonb,
  suggested_show_ids jsonb not null default '[]'::jsonb,
  resolved_show_id uuid references core.shows (id) on delete set null,
  resolved_season_id uuid references core.seasons (id) on delete set null,
  resolution_action text,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  resolved_at timestamptz,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (platform, source_scope, account_handle, normalized_hashtag)
);

create index if not exists account_hashtag_review_queue_status_idx
  on social.account_hashtag_review_queue (review_status, last_seen_at desc);

grant select on table social.instagram_account_catalog_posts to anon, authenticated;
grant select on table social.tiktok_account_catalog_posts to anon, authenticated;
grant select on table social.twitter_account_catalog_posts to anon, authenticated;
grant select on table social.threads_account_catalog_posts to anon, authenticated;
grant select on table social.account_hashtag_review_queue to anon, authenticated;

grant all privileges on table social.instagram_account_catalog_posts to service_role;
grant all privileges on table social.tiktok_account_catalog_posts to service_role;
grant all privileges on table social.twitter_account_catalog_posts to service_role;
grant all privileges on table social.threads_account_catalog_posts to service_role;
grant all privileges on table social.account_hashtag_review_queue to service_role;

alter table social.instagram_account_catalog_posts enable row level security;
alter table social.tiktok_account_catalog_posts enable row level security;
alter table social.twitter_account_catalog_posts enable row level security;
alter table social.threads_account_catalog_posts enable row level security;
alter table social.account_hashtag_review_queue enable row level security;

drop policy if exists instagram_account_catalog_posts_public_read on social.instagram_account_catalog_posts;
create policy instagram_account_catalog_posts_public_read on social.instagram_account_catalog_posts
for select to anon, authenticated using (true);

drop policy if exists tiktok_account_catalog_posts_public_read on social.tiktok_account_catalog_posts;
create policy tiktok_account_catalog_posts_public_read on social.tiktok_account_catalog_posts
for select to anon, authenticated using (true);

drop policy if exists twitter_account_catalog_posts_public_read on social.twitter_account_catalog_posts;
create policy twitter_account_catalog_posts_public_read on social.twitter_account_catalog_posts
for select to anon, authenticated using (true);

drop policy if exists threads_account_catalog_posts_public_read on social.threads_account_catalog_posts;
create policy threads_account_catalog_posts_public_read on social.threads_account_catalog_posts
for select to anon, authenticated using (true);

drop policy if exists account_hashtag_review_queue_public_read on social.account_hashtag_review_queue;
create policy account_hashtag_review_queue_public_read on social.account_hashtag_review_queue
for select to anon, authenticated using (true);

commit;
