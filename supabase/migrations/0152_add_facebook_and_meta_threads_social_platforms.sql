begin;

-- External Meta Threads naming intentionally uses meta_threads_* to avoid
-- collision/ambiguity with existing internal discussion tables:
--   social.threads (internal discussion threads)
--   social.posts (internal posts within internal threads)

-- ---------------------------------------------------------------------------
-- Platform constraints
-- ---------------------------------------------------------------------------

alter table social.scrape_jobs
  drop constraint if exists scrape_jobs_platform_check;

alter table social.scrape_jobs
  add constraint scrape_jobs_platform_check
  check (platform in ('instagram', 'tiktok', 'youtube', 'twitter', 'reddit', 'facebook', 'threads'));

do $$
declare
  r record;
begin
  for r in
    select c.conname
    from pg_constraint c
    where c.conrelid = 'social.season_targets'::regclass
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%platform%'
      and pg_get_constraintdef(c.oid) ilike '%instagram%'
      and pg_get_constraintdef(c.oid) ilike '%reddit%'
  loop
    execute format('alter table social.season_targets drop constraint %I', r.conname);
  end loop;
end $$;

alter table social.season_targets
  add constraint season_targets_platform_check
  check (platform in ('instagram', 'tiktok', 'youtube', 'twitter', 'reddit', 'facebook', 'threads'));

-- ---------------------------------------------------------------------------
-- facebook posts/comments
-- ---------------------------------------------------------------------------

create table if not exists social.facebook_posts (
  id uuid primary key default gen_random_uuid(),
  post_id text not null unique,
  page_id text,
  username text not null,
  user_id text,
  caption text,
  post_type text,
  media_type text,
  media_urls jsonb not null default '[]'::jsonb,
  thumbnail_url text,
  likes integer not null default 0,
  comments_count integer not null default 0,
  shares integer not null default 0,
  views integer not null default 0,
  posted_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb,
  show_id uuid references core.shows (id) on delete set null,
  person_id uuid references core.people (id) on delete set null,
  season_id uuid references core.seasons (id) on delete set null,
  job_id uuid references social.scrape_jobs (id) on delete set null,
  source_account text,
  hosted_thumbnail_url text,
  hosted_media_urls jsonb not null default '[]'::jsonb,
  media_mirror_status text,
  media_mirror_error text,
  media_mirror_attempt_count integer,
  media_mirror_last_attempt_at timestamptz,
  media_mirror_last_job_id text
);

create index if not exists facebook_posts_username_idx on social.facebook_posts (username);
create index if not exists facebook_posts_posted_at_idx on social.facebook_posts (posted_at desc);
create index if not exists facebook_posts_show_id_idx on social.facebook_posts (show_id) where show_id is not null;
create index if not exists facebook_posts_person_id_idx on social.facebook_posts (person_id) where person_id is not null;
create index if not exists facebook_posts_season_id_idx on social.facebook_posts (season_id) where season_id is not null;
create index if not exists facebook_posts_job_id_idx on social.facebook_posts (job_id) where job_id is not null;
create index if not exists idx_facebook_posts_media_mirror_pending
  on social.facebook_posts (season_id, posted_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

create table if not exists social.facebook_comments (
  id uuid primary key default gen_random_uuid(),
  comment_id text not null unique,
  post_id uuid not null references social.facebook_posts (id) on delete cascade,
  parent_comment_id uuid references social.facebook_comments (id) on delete cascade,
  parent_source_comment_id text,
  username text not null,
  user_id text,
  text text not null,
  likes integer not null default 0,
  is_reply boolean not null default false,
  reply_count integer not null default 0,
  created_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb,
  season_id uuid references core.seasons (id) on delete set null,
  job_id uuid references social.scrape_jobs (id) on delete set null,
  source_account text,
  media_urls jsonb not null default '[]'::jsonb,
  hosted_media_urls jsonb not null default '[]'::jsonb,
  media_mirror_status text,
  media_mirror_error text,
  media_mirror_attempt_count integer,
  media_mirror_last_attempt_at timestamptz,
  media_mirror_last_job_id text
);

create index if not exists facebook_comments_post_id_idx on social.facebook_comments (post_id);
create index if not exists facebook_comments_parent_comment_id_idx
  on social.facebook_comments (parent_comment_id) where parent_comment_id is not null;
create index if not exists facebook_comments_created_at_idx on social.facebook_comments (created_at desc);
create index if not exists facebook_comments_season_id_idx on social.facebook_comments (season_id) where season_id is not null;
create index if not exists facebook_comments_job_id_idx on social.facebook_comments (job_id) where job_id is not null;
create index if not exists idx_facebook_comments_media_mirror_pending
  on social.facebook_comments (season_id, created_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed')
    and jsonb_array_length(coalesce(media_urls, '[]'::jsonb)) > 0;

-- ---------------------------------------------------------------------------
-- meta threads posts/comments
-- ---------------------------------------------------------------------------

create table if not exists social.meta_threads_posts (
  id uuid primary key default gen_random_uuid(),
  post_id text not null unique,
  thread_item_id text,
  username text not null,
  user_id text,
  text text,
  media_type text,
  media_urls jsonb not null default '[]'::jsonb,
  thumbnail_url text,
  likes integer not null default 0,
  replies_count integer not null default 0,
  reposts integer not null default 0,
  quotes integer not null default 0,
  views integer not null default 0,
  posted_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb,
  show_id uuid references core.shows (id) on delete set null,
  person_id uuid references core.people (id) on delete set null,
  season_id uuid references core.seasons (id) on delete set null,
  job_id uuid references social.scrape_jobs (id) on delete set null,
  source_account text,
  hosted_thumbnail_url text,
  hosted_media_urls jsonb not null default '[]'::jsonb,
  media_mirror_status text,
  media_mirror_error text,
  media_mirror_attempt_count integer,
  media_mirror_last_attempt_at timestamptz,
  media_mirror_last_job_id text
);

create index if not exists meta_threads_posts_username_idx on social.meta_threads_posts (username);
create index if not exists meta_threads_posts_posted_at_idx on social.meta_threads_posts (posted_at desc);
create index if not exists meta_threads_posts_show_id_idx on social.meta_threads_posts (show_id) where show_id is not null;
create index if not exists meta_threads_posts_person_id_idx on social.meta_threads_posts (person_id) where person_id is not null;
create index if not exists meta_threads_posts_season_id_idx on social.meta_threads_posts (season_id) where season_id is not null;
create index if not exists meta_threads_posts_job_id_idx on social.meta_threads_posts (job_id) where job_id is not null;
create index if not exists idx_meta_threads_posts_media_mirror_pending
  on social.meta_threads_posts (season_id, posted_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed');

create table if not exists social.meta_threads_comments (
  id uuid primary key default gen_random_uuid(),
  comment_id text not null unique,
  post_id uuid not null references social.meta_threads_posts (id) on delete cascade,
  parent_comment_id uuid references social.meta_threads_comments (id) on delete cascade,
  parent_source_comment_id text,
  username text not null,
  user_id text,
  text text not null,
  likes integer not null default 0,
  is_reply boolean not null default false,
  reply_count integer not null default 0,
  created_at timestamptz,
  scraped_at timestamptz not null default now(),
  raw_data jsonb,
  season_id uuid references core.seasons (id) on delete set null,
  job_id uuid references social.scrape_jobs (id) on delete set null,
  source_account text,
  media_urls jsonb not null default '[]'::jsonb,
  hosted_media_urls jsonb not null default '[]'::jsonb,
  media_mirror_status text,
  media_mirror_error text,
  media_mirror_attempt_count integer,
  media_mirror_last_attempt_at timestamptz,
  media_mirror_last_job_id text
);

create index if not exists meta_threads_comments_post_id_idx on social.meta_threads_comments (post_id);
create index if not exists meta_threads_comments_parent_comment_id_idx
  on social.meta_threads_comments (parent_comment_id) where parent_comment_id is not null;
create index if not exists meta_threads_comments_created_at_idx on social.meta_threads_comments (created_at desc);
create index if not exists meta_threads_comments_season_id_idx on social.meta_threads_comments (season_id) where season_id is not null;
create index if not exists meta_threads_comments_job_id_idx on social.meta_threads_comments (job_id) where job_id is not null;
create index if not exists idx_meta_threads_comments_media_mirror_pending
  on social.meta_threads_comments (season_id, created_at desc)
  where coalesce(media_mirror_status, '') in ('pending', 'partial', 'failed')
    and jsonb_array_length(coalesce(media_urls, '[]'::jsonb)) > 0;

-- ---------------------------------------------------------------------------
-- Job types for post/comment media mirrors
-- ---------------------------------------------------------------------------

do $$
declare
  r record;
begin
  for r in
    select c.conname
    from pg_constraint c
    where c.conrelid = 'social.scrape_jobs'::regclass
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%job_type%'
  loop
    execute format('alter table social.scrape_jobs drop constraint %I', r.conname);
  end loop;
end $$;

alter table social.scrape_jobs
  add constraint scrape_jobs_job_type_check_v4
  check (
    job_type in (
      'posts',
      'comments',
      'search',
      'replies',
      'instagram_media_mirror',
      'tiktok_media_mirror',
      'youtube_media_mirror',
      'twitter_media_mirror',
      'facebook_media_mirror',
      'threads_media_mirror',
      'instagram_comment_media_mirror',
      'tiktok_comment_media_mirror',
      'youtube_comment_media_mirror',
      'twitter_comment_media_mirror',
      'facebook_comment_media_mirror',
      'threads_comment_media_mirror'
    )
  );

-- ---------------------------------------------------------------------------
-- Grants + RLS
-- ---------------------------------------------------------------------------

grant select on table
  social.facebook_posts,
  social.facebook_comments,
  social.meta_threads_posts,
  social.meta_threads_comments
to anon, authenticated;

grant all privileges on table
  social.facebook_posts,
  social.facebook_comments,
  social.meta_threads_posts,
  social.meta_threads_comments
to service_role;

alter table social.facebook_posts enable row level security;
alter table social.facebook_comments enable row level security;
alter table social.meta_threads_posts enable row level security;
alter table social.meta_threads_comments enable row level security;

drop policy if exists facebook_posts_public_read on social.facebook_posts;
create policy facebook_posts_public_read on social.facebook_posts
for select to anon, authenticated
using (true);

drop policy if exists facebook_comments_public_read on social.facebook_comments;
create policy facebook_comments_public_read on social.facebook_comments
for select to anon, authenticated
using (true);

drop policy if exists meta_threads_posts_public_read on social.meta_threads_posts;
create policy meta_threads_posts_public_read on social.meta_threads_posts
for select to anon, authenticated
using (true);

drop policy if exists meta_threads_comments_public_read on social.meta_threads_comments;
create policy meta_threads_comments_public_read on social.meta_threads_comments
for select to anon, authenticated
using (true);

commit;
