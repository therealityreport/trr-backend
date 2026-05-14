begin;

create table if not exists pipeline.socialblade_growth_snapshots (
  id uuid primary key default gen_random_uuid(),
  growth_data_id uuid references pipeline.socialblade_growth_data(id) on delete set null,
  person_id uuid references core.people(id) on delete set null,
  platform text not null default 'instagram',
  account_handle text not null,
  instagram_handle text,
  scraped_at timestamptz not null default now(),
  stats_refreshed boolean not null default false,
  profile_stats jsonb not null default '{}'::jsonb,
  rankings jsonb not null default '{}'::jsonb,
  daily_channel_metrics_60day jsonb not null default '{}'::jsonb,
  daily_total_followers_chart jsonb,
  raw_response jsonb not null default '{}'::jsonb,
  snapshot_source text,
  refresh_source text,
  refresh_forced boolean not null default false,
  created_at timestamptz not null default now()
);

create index if not exists socialblade_growth_snapshots_platform_account_scraped_idx
  on pipeline.socialblade_growth_snapshots (platform, account_handle, scraped_at desc, id);

create index if not exists socialblade_growth_snapshots_person_platform_scraped_idx
  on pipeline.socialblade_growth_snapshots (person_id, platform, scraped_at desc, id)
  where person_id is not null;

create index if not exists socialblade_growth_snapshots_growth_data_scraped_idx
  on pipeline.socialblade_growth_snapshots (growth_data_id, scraped_at desc, id)
  where growth_data_id is not null;

comment on table pipeline.socialblade_growth_snapshots is
  'Immutable SocialBlade scrape snapshots. The latest row remains pipeline.socialblade_growth_data; this table keeps every observed scrape for trend/audit history.';

create table if not exists social.instagram_profile_following_snapshots (
  id uuid primary key default gen_random_uuid(),
  owner_profile_id uuid not null references social.instagram_profiles(id) on delete cascade,
  owner_instagram_profile_id text,
  owner_username text not null,
  owner_normalized_username text not null,
  source_scope text not null default 'network',
  observed_at timestamptz not null default now(),
  relationships_fetched integer not null default 0 check (relationships_fetched >= 0),
  relationships_upserted integer not null default 0 check (relationships_upserted >= 0),
  relationships_missing integer not null default 0 check (relationships_missing >= 0),
  source_is_complete boolean not null default false,
  pages_fetched integer,
  has_more boolean,
  next_cursor text,
  max_pages integer,
  max_relationships integer,
  retrieval_meta jsonb not null default '{}'::jsonb,
  last_scrape_job_id uuid references social.scrape_jobs(id) on delete set null,
  last_scrape_run_id uuid references social.scrape_runs(id) on delete set null,
  created_at timestamptz not null default now()
);

create index if not exists instagram_profile_following_snapshots_owner_observed_idx
  on social.instagram_profile_following_snapshots (owner_profile_id, observed_at desc, id);

create index if not exists instagram_profile_following_snapshots_owner_username_observed_idx
  on social.instagram_profile_following_snapshots (owner_normalized_username, observed_at desc, id);

create table if not exists social.instagram_profile_relationship_snapshot_items (
  id uuid primary key default gen_random_uuid(),
  following_snapshot_id uuid not null references social.instagram_profile_following_snapshots(id) on delete cascade,
  relationship_row_id uuid references social.instagram_profile_relationships(id) on delete set null,
  owner_profile_id uuid not null references social.instagram_profiles(id) on delete cascade,
  owner_instagram_profile_id text,
  owner_username text not null,
  owner_normalized_username text not null,
  relationship_type text not null check (relationship_type = 'following'),
  related_user_id text,
  related_username text not null,
  related_normalized_username text,
  related_full_name text,
  related_is_private boolean,
  related_is_verified boolean,
  related_profile_pic_url text,
  hosted_related_profile_pic_url text,
  is_present boolean not null,
  source_rank integer,
  source_page_ordinal integer,
  source_cursor text,
  raw_data jsonb not null default '{}'::jsonb,
  observed_at timestamptz not null default now(),
  last_scrape_job_id uuid references social.scrape_jobs(id) on delete set null,
  last_scrape_run_id uuid references social.scrape_runs(id) on delete set null,
  created_at timestamptz not null default now()
);

create unique index if not exists instagram_relationship_snapshot_items_user_id_key
  on social.instagram_profile_relationship_snapshot_items (following_snapshot_id, relationship_type, related_user_id)
  where related_user_id is not null;

create unique index if not exists instagram_relationship_snapshot_items_username_key
  on social.instagram_profile_relationship_snapshot_items (
    following_snapshot_id,
    relationship_type,
    related_normalized_username
  )
  where related_user_id is null and related_normalized_username is not null;

create index if not exists instagram_relationship_snapshot_items_owner_related_observed_idx
  on social.instagram_profile_relationship_snapshot_items (
    owner_profile_id,
    relationship_type,
    related_normalized_username,
    observed_at desc
  );

create index if not exists instagram_relationship_snapshot_items_snapshot_present_rank_idx
  on social.instagram_profile_relationship_snapshot_items (following_snapshot_id, is_present, source_rank);

comment on table social.instagram_profile_following_snapshots is
  'Immutable Instagram following-list scrape attempts. source_is_complete=false means the snapshot is evidence of observed follows only and must not be used to infer unfollows.';

comment on table social.instagram_profile_relationship_snapshot_items is
  'Per-relationship rows for each Instagram following snapshot. is_present=false rows are inferred only from complete snapshots.';

alter table pipeline.socialblade_growth_snapshots enable row level security;
alter table social.instagram_profile_following_snapshots enable row level security;
alter table social.instagram_profile_relationship_snapshot_items enable row level security;

grant all privileges on table
  pipeline.socialblade_growth_snapshots,
  social.instagram_profile_following_snapshots,
  social.instagram_profile_relationship_snapshot_items
to service_role;

revoke all on table
  pipeline.socialblade_growth_snapshots,
  social.instagram_profile_following_snapshots,
  social.instagram_profile_relationship_snapshot_items
from public, anon, authenticated;

commit;
