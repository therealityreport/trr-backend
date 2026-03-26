begin;

create table if not exists social.facebook_account_catalog_posts (
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

create index if not exists facebook_account_catalog_posts_account_posted_at_idx
  on social.facebook_account_catalog_posts (lower(source_account), posted_at desc nulls last);

create index if not exists facebook_account_catalog_posts_assignment_status_idx
  on social.facebook_account_catalog_posts (assignment_status, posted_at desc nulls last);

alter table social.account_hashtag_review_queue
  drop constraint if exists account_hashtag_review_queue_platform_check;

alter table social.account_hashtag_review_queue
  add constraint account_hashtag_review_queue_platform_check
  check (platform in ('instagram', 'tiktok', 'twitter', 'youtube', 'facebook', 'threads'));

grant select on table social.facebook_account_catalog_posts to anon, authenticated;
grant all privileges on table social.facebook_account_catalog_posts to service_role;

alter table social.facebook_account_catalog_posts enable row level security;

drop policy if exists facebook_account_catalog_posts_public_read on social.facebook_account_catalog_posts;
create policy facebook_account_catalog_posts_public_read on social.facebook_account_catalog_posts
for select to anon, authenticated using (true);

commit;
