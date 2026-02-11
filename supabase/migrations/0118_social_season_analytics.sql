begin;

-- Season-scoped social analytics support
-- - season target configuration (Bravo-first, extensible)
-- - season/job lineage columns on existing social tables

create table if not exists social.season_targets (
  season_id uuid not null references core.seasons (id) on delete cascade,
  show_id uuid not null references core.shows (id) on delete cascade,
  platform text not null check (platform in ('instagram', 'tiktok', 'youtube', 'twitter', 'reddit')),
  source_scope text not null default 'bravo' check (source_scope in ('bravo', 'creator', 'community')),
  timezone text not null default 'America/New_York',
  accounts jsonb not null default '[]'::jsonb,
  hashtags jsonb not null default '[]'::jsonb,
  keywords jsonb not null default '[]'::jsonb,
  is_active boolean not null default true,
  config jsonb not null default '{}'::jsonb,
  updated_by text,
  updated_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  primary key (season_id, platform, source_scope)
);

create index if not exists season_targets_show_id_idx
  on social.season_targets (show_id);

create index if not exists season_targets_platform_idx
  on social.season_targets (platform, source_scope);

alter table social.scrape_jobs
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists source_scope text not null default 'bravo' check (source_scope in ('bravo', 'creator', 'community')),
  add column if not exists metadata jsonb not null default '{}'::jsonb,
  add column if not exists initiated_by text;

create index if not exists scrape_jobs_season_id_idx on social.scrape_jobs (season_id);

alter table social.instagram_posts
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;
create index if not exists instagram_posts_season_id_idx on social.instagram_posts (season_id) where season_id is not null;
create index if not exists instagram_posts_job_id_idx on social.instagram_posts (job_id) where job_id is not null;

alter table social.instagram_comments
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;
create index if not exists instagram_comments_season_id_idx on social.instagram_comments (season_id) where season_id is not null;
create index if not exists instagram_comments_job_id_idx on social.instagram_comments (job_id) where job_id is not null;

alter table social.tiktok_posts
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;
create index if not exists tiktok_posts_season_id_idx on social.tiktok_posts (season_id) where season_id is not null;
create index if not exists tiktok_posts_job_id_idx on social.tiktok_posts (job_id) where job_id is not null;

alter table social.tiktok_comments
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;
create index if not exists tiktok_comments_season_id_idx on social.tiktok_comments (season_id) where season_id is not null;
create index if not exists tiktok_comments_job_id_idx on social.tiktok_comments (job_id) where job_id is not null;

alter table social.youtube_videos
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;
create index if not exists youtube_videos_season_id_idx on social.youtube_videos (season_id) where season_id is not null;
create index if not exists youtube_videos_job_id_idx on social.youtube_videos (job_id) where job_id is not null;

alter table social.youtube_comments
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;
create index if not exists youtube_comments_season_id_idx on social.youtube_comments (season_id) where season_id is not null;
create index if not exists youtube_comments_job_id_idx on social.youtube_comments (job_id) where job_id is not null;

alter table social.twitter_tweets
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;
create index if not exists twitter_tweets_season_id_idx on social.twitter_tweets (season_id) where season_id is not null;
create index if not exists twitter_tweets_job_id_idx on social.twitter_tweets (job_id) where job_id is not null;

-- Grants + RLS for new table

grant select on table social.season_targets to anon, authenticated;
grant all privileges on table social.season_targets to service_role;

alter table social.season_targets enable row level security;

drop policy if exists season_targets_public_read on social.season_targets;
create policy season_targets_public_read on social.season_targets
for select to anon, authenticated
using (true);

commit;
