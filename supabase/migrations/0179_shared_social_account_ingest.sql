begin;

alter table social.scrape_runs
  alter column season_id drop not null,
  alter column show_id drop not null;

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
  add constraint scrape_jobs_job_type_check_v5
  check (
    job_type in (
      'posts',
      'comments',
      'search',
      'replies',
      'shared_account_posts',
      'post_classify',
      'season_materialize',
      'analytics_refresh',
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

create table if not exists social.shared_account_sources (
  id uuid primary key default gen_random_uuid(),
  platform text not null
    check (platform in ('instagram', 'tiktok', 'youtube', 'twitter', 'facebook', 'threads')),
  source_scope text not null default 'bravo'
    check (source_scope in ('bravo', 'creator', 'community')),
  account_handle text not null,
  is_active boolean not null default true,
  scrape_priority integer not null default 100,
  metadata jsonb not null default '{}'::jsonb,
  last_scrape_status text,
  last_scrape_run_id uuid references social.scrape_runs (id) on delete set null,
  last_scrape_job_id uuid references social.scrape_jobs (id) on delete set null,
  last_scrape_at timestamptz,
  last_classified_at timestamptz,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (platform, source_scope, account_handle)
);

create index if not exists shared_account_sources_active_idx
  on social.shared_account_sources (source_scope, is_active, platform, scrape_priority, account_handle);

create table if not exists social.shared_post_matches (
  id uuid primary key default gen_random_uuid(),
  platform text not null
    check (platform in ('instagram', 'tiktok', 'youtube', 'twitter', 'facebook', 'threads')),
  source_scope text not null default 'bravo'
    check (source_scope in ('bravo', 'creator', 'community')),
  post_row_id uuid not null,
  source_id text not null,
  source_account text,
  matched_show_id uuid references core.shows (id) on delete set null,
  matched_season_id uuid references core.seasons (id) on delete set null,
  status text not null
    check (status in ('matched', 'ambiguous', 'unmatched', 'archived'))
    default 'unmatched',
  score integer not null default 0,
  reason text,
  candidate_matches jsonb not null default '[]'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  run_id uuid references social.scrape_runs (id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  matched_at timestamptz,
  unique (platform, source_scope, source_id)
);

create index if not exists shared_post_matches_status_idx
  on social.shared_post_matches (status, updated_at desc);

create index if not exists shared_post_matches_season_idx
  on social.shared_post_matches (matched_season_id, updated_at desc)
  where matched_season_id is not null;

create table if not exists social.shared_post_review_queue (
  id uuid primary key default gen_random_uuid(),
  platform text not null
    check (platform in ('instagram', 'tiktok', 'youtube', 'twitter', 'facebook', 'threads')),
  source_scope text not null default 'bravo'
    check (source_scope in ('bravo', 'creator', 'community')),
  post_row_id uuid not null,
  source_id text not null,
  source_account text,
  review_status text not null
    check (review_status in ('open', 'resolved', 'ignored'))
    default 'open',
  review_reason text not null
    check (review_reason in ('ambiguous_match', 'unmatched')),
  resolution_action text,
  resolved_show_id uuid references core.shows (id) on delete set null,
  resolved_season_id uuid references core.seasons (id) on delete set null,
  payload jsonb not null default '{}'::jsonb,
  run_id uuid references social.scrape_runs (id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  resolved_at timestamptz,
  unique (platform, source_scope, source_id)
);

create index if not exists shared_post_review_queue_status_idx
  on social.shared_post_review_queue (review_status, created_at desc);

create index if not exists shared_post_review_queue_season_idx
  on social.shared_post_review_queue (resolved_season_id, updated_at desc)
  where resolved_season_id is not null;

grant select on table social.shared_account_sources to anon, authenticated;
grant select on table social.shared_post_matches to anon, authenticated;
grant select on table social.shared_post_review_queue to anon, authenticated;

grant all privileges on table social.shared_account_sources to service_role;
grant all privileges on table social.shared_post_matches to service_role;
grant all privileges on table social.shared_post_review_queue to service_role;

alter table social.shared_account_sources enable row level security;
alter table social.shared_post_matches enable row level security;
alter table social.shared_post_review_queue enable row level security;

drop policy if exists shared_account_sources_public_read on social.shared_account_sources;
create policy shared_account_sources_public_read on social.shared_account_sources
for select to anon, authenticated
using (true);

drop policy if exists shared_post_matches_public_read on social.shared_post_matches;
create policy shared_post_matches_public_read on social.shared_post_matches
for select to anon, authenticated
using (true);

drop policy if exists shared_post_review_queue_public_read on social.shared_post_review_queue;
create policy shared_post_review_queue_public_read on social.shared_post_review_queue
for select to anon, authenticated
using (true);

commit;
