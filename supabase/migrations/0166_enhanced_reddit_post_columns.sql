-- Sprint 3: Enhanced Reddit post metadata, match classification, and media mirrors.
--
-- 1. reddit_posts — add deep-scrape metadata columns
-- 2. reddit_period_post_matches — add match_type + admin_approved
-- 3. reddit_media_mirrors — new table for mirrored media assets

begin;

-- ---------------------------------------------------------------------------
-- 1. social.reddit_posts — additional metadata columns
-- ---------------------------------------------------------------------------

alter table social.reddit_posts
  add column if not exists upvote_ratio real,
  add column if not exists is_self boolean default false,
  add column if not exists post_type text,
  add column if not exists thumbnail text,
  add column if not exists media_metadata jsonb default '{}'::jsonb,
  add column if not exists poll_data jsonb,
  add column if not exists content_url text,
  add column if not exists is_nsfw boolean default false,
  add column if not exists is_spoiler boolean default false,
  add column if not exists author_flair_text text,
  add column if not exists detail_scraped_at timestamptz;

comment on column social.reddit_posts.upvote_ratio      is 'Reddit upvote ratio (0.0–1.0)';
comment on column social.reddit_posts.is_self            is 'True for text/self posts, false for link posts';
comment on column social.reddit_posts.post_type          is 'Classified type: text, link, image, video, gallery, poll';
comment on column social.reddit_posts.media_metadata     is 'Gallery/media info from Reddit API response';
comment on column social.reddit_posts.poll_data          is 'Poll options and vote counts when post_type = poll';
comment on column social.reddit_posts.content_url        is 'The actual linked URL (distinct from Reddit permalink)';
comment on column social.reddit_posts.detail_scraped_at  is 'Null until the deep-scrape pass populates extended fields';

-- Index for finding posts that still need a deep scrape
create index if not exists reddit_posts_detail_scraped_at_null_idx
  on social.reddit_posts (subreddit, posted_at desc)
  where detail_scraped_at is null;

-- Index for filtering by post type
create index if not exists reddit_posts_post_type_idx
  on social.reddit_posts (post_type)
  where post_type is not null;

-- ---------------------------------------------------------------------------
-- 2. social.reddit_period_post_matches — match classification columns
-- ---------------------------------------------------------------------------

alter table social.reddit_period_post_matches
  add column if not exists match_type text default 'flair',
  add column if not exists admin_approved boolean default null;

-- Wrap CHECK constraint in a DO block so it is idempotent
do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'reddit_period_post_matches_match_type_check'
      and conrelid = 'social.reddit_period_post_matches'::regclass
  ) then
    alter table social.reddit_period_post_matches
      add constraint reddit_period_post_matches_match_type_check
      check (match_type in ('flair', 'scan', 'all'));
  end if;
end
$$;

comment on column social.reddit_period_post_matches.match_type      is 'How this post was matched: flair filter, full scan, or all-posts ingest';
comment on column social.reddit_period_post_matches.admin_approved  is 'Null = not reviewed, true = approved, false = rejected by admin';

create index if not exists reddit_period_post_matches_match_type_idx
  on social.reddit_period_post_matches (match_type);

create index if not exists reddit_period_post_matches_admin_approved_idx
  on social.reddit_period_post_matches (admin_approved)
  where admin_approved is not null;

-- ---------------------------------------------------------------------------
-- 3. social.reddit_media_mirrors — mirrored media assets
-- ---------------------------------------------------------------------------

create table if not exists social.reddit_media_mirrors (
  id uuid primary key default gen_random_uuid(),
  reddit_post_id text not null
    references social.reddit_posts (reddit_post_id) on delete cascade,
  reddit_comment_id text
    references social.reddit_comments (reddit_comment_id) on delete cascade,
  source_url text not null,
  media_type text not null
    check (media_type in ('image', 'video', 'gif', 'thumbnail')),
  hosted_key text,
  hosted_url text,
  sha256 text,
  size_bytes bigint,
  content_type text,
  status text not null default 'pending'
    check (status in ('pending', 'mirrored', 'failed', 'skipped')),
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

comment on table  social.reddit_media_mirrors              is 'Locally mirrored copies of Reddit media (images, video, gifs, thumbnails)';
comment on column social.reddit_media_mirrors.hosted_key   is 'Object-storage key once mirrored';
comment on column social.reddit_media_mirrors.hosted_url   is 'Public/signed URL to the mirrored asset';
comment on column social.reddit_media_mirrors.sha256       is 'Content hash for dedup and integrity checks';
comment on column social.reddit_media_mirrors.status       is 'Mirror pipeline status: pending -> mirrored | failed | skipped';

create index if not exists reddit_media_mirrors_post_idx
  on social.reddit_media_mirrors (reddit_post_id);

create index if not exists reddit_media_mirrors_comment_idx
  on social.reddit_media_mirrors (reddit_comment_id)
  where reddit_comment_id is not null;

create index if not exists reddit_media_mirrors_status_idx
  on social.reddit_media_mirrors (status)
  where status = 'pending';

-- Permissions — match existing reddit table grants from 0157
grant select on table social.reddit_media_mirrors
  to anon, authenticated;

grant all privileges on table social.reddit_media_mirrors
  to service_role;

alter table social.reddit_media_mirrors enable row level security;

drop policy if exists reddit_media_mirrors_public_read on social.reddit_media_mirrors;
create policy reddit_media_mirrors_public_read on social.reddit_media_mirrors
  for select using (true);

commit;
