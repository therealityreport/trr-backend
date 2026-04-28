begin;

-- Phase 2 additive Instagram queryable profile schema.
--
-- Profile snapshot and following retrieval use existing social.scrape_jobs rows
-- with config.stage markers such as instagram_profile_snapshot or
-- instagram_profile_following. This migration intentionally does not alter the
-- scrape_jobs job_type constraint.
--
-- Transitional raw exposure note: legacy Instagram raw-data-bearing tables keep
-- their existing broad read grants in this pass for compatibility. New profile
-- raw-bearing tables below are service-role-only until curated backend/admin
-- reads are wired.

create table if not exists social.instagram_profiles (
  id uuid primary key default gen_random_uuid(),
  shared_account_source_id uuid references social.shared_account_sources(id) on delete set null,
  source_scope text not null default 'bravo'
    check (source_scope in ('bravo', 'creator', 'community')),
  source_account text,
  profile_id text,
  input_url text,
  username text,
  normalized_username text,
  url text,
  full_name text,
  biography text,
  country text,
  date_joined text,
  date_joined_at timestamptz,
  date_verified text,
  date_verified_at timestamptz,
  former_usernames_count integer check (former_usernames_count is null or former_usernames_count >= 0),
  followers_count bigint check (followers_count is null or followers_count >= 0),
  follows_count bigint check (follows_count is null or follows_count >= 0),
  posts_count bigint check (posts_count is null or posts_count >= 0),
  highlight_reel_count integer check (highlight_reel_count is null or highlight_reel_count >= 0),
  igtv_video_count integer check (igtv_video_count is null or igtv_video_count >= 0),
  is_business_account boolean,
  joined_recently boolean,
  has_channel boolean,
  business_category_name text,
  is_private boolean,
  is_verified boolean,
  external_url text,
  external_url_shimmed text,
  profile_pic_url text,
  profile_pic_url_hd text,
  hosted_profile_pic_url text,
  hosted_profile_pic_url_hd text,
  about_raw jsonb not null default '{}'::jsonb,
  raw_data jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_scraped_at timestamptz,
  last_scrape_job_id uuid references social.scrape_jobs(id) on delete set null,
  last_scrape_run_id uuid references social.scrape_runs(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (profile_id is null or (btrim(profile_id) <> '')),
  check (
    normalized_username is null
    or (normalized_username = lower(btrim(normalized_username)) and normalized_username <> '')
  )
);

create unique index if not exists instagram_profiles_profile_id_key
  on social.instagram_profiles (profile_id)
  where profile_id is not null;

create unique index if not exists instagram_profiles_source_scope_normalized_username_key
  on social.instagram_profiles (source_scope, normalized_username)
  where profile_id is null;

create index if not exists instagram_profiles_scope_username_lookup_idx
  on social.instagram_profiles (source_scope, normalized_username)
  where normalized_username is not null;

create index if not exists instagram_profiles_shared_source_idx
  on social.instagram_profiles (shared_account_source_id)
  where shared_account_source_id is not null;

create index if not exists instagram_profiles_last_scraped_idx
  on social.instagram_profiles (last_scraped_at desc nulls last, id);

create table if not exists social.instagram_profile_external_links (
  id uuid primary key default gen_random_uuid(),
  profile_id uuid not null references social.instagram_profiles(id) on delete cascade,
  instagram_profile_id text,
  username text,
  normalized_username text,
  link_index integer not null default 0 check (link_index >= 0),
  title text,
  url text not null check (btrim(url) <> ''),
  shim_url text,
  normalized_url text,
  normalized_domain text,
  link_type text,
  raw_data jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_scrape_job_id uuid references social.scrape_jobs(id) on delete set null,
  last_scrape_run_id uuid references social.scrape_runs(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (profile_id, link_index, url),
  check (
    normalized_username is null
    or (normalized_username = lower(btrim(normalized_username)) and normalized_username <> '')
  ),
  check (
    normalized_domain is null
    or (normalized_domain = lower(btrim(normalized_domain)) and normalized_domain <> '')
  )
);

create index if not exists instagram_profile_external_links_profile_idx
  on social.instagram_profile_external_links (profile_id, link_index);

create index if not exists instagram_profile_external_links_domain_idx
  on social.instagram_profile_external_links (normalized_domain, profile_id)
  where normalized_domain is not null;

create index if not exists instagram_profile_external_links_normalized_url_idx
  on social.instagram_profile_external_links (normalized_url)
  where normalized_url is not null;

create table if not exists social.instagram_profile_relationships (
  id uuid primary key default gen_random_uuid(),
  owner_profile_id uuid not null references social.instagram_profiles(id) on delete cascade,
  owner_instagram_profile_id text,
  owner_username text,
  owner_normalized_username text,
  relationship_type text not null check (relationship_type = 'following'),
  related_user_id text,
  related_username text not null,
  related_normalized_username text,
  related_full_name text,
  related_is_private boolean,
  related_is_verified boolean,
  related_profile_pic_url text,
  hosted_related_profile_pic_url text,
  raw_data jsonb not null default '{}'::jsonb,
  source_page_ordinal integer check (source_page_ordinal is null or source_page_ordinal >= 0),
  source_cursor text,
  source_page_size integer check (source_page_size is null or source_page_size >= 0),
  source_rank integer check (source_rank is null or source_rank >= 0),
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  missing_at timestamptz,
  is_missing boolean not null default false,
  last_scrape_job_id uuid references social.scrape_jobs(id) on delete set null,
  last_scrape_run_id uuid references social.scrape_runs(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (
    owner_normalized_username is null
    or (owner_normalized_username = lower(btrim(owner_normalized_username)) and owner_normalized_username <> '')
  ),
  check (
    related_normalized_username is null
    or (related_normalized_username = lower(btrim(related_normalized_username)) and related_normalized_username <> '')
  )
);

create unique index if not exists instagram_profile_relationships_related_user_id_key
  on social.instagram_profile_relationships (owner_profile_id, relationship_type, related_user_id)
  where related_user_id is not null;

create unique index if not exists instagram_profile_relationships_related_username_key
  on social.instagram_profile_relationships (owner_profile_id, relationship_type, related_normalized_username)
  where related_user_id is null and related_normalized_username is not null;

create index if not exists instagram_profile_relationships_owner_type_rank_idx
  on social.instagram_profile_relationships (
    owner_profile_id,
    relationship_type,
    source_rank nulls last,
    related_normalized_username
  );

create index if not exists instagram_profile_relationships_related_username_idx
  on social.instagram_profile_relationships (related_normalized_username)
  where related_normalized_username is not null;

create index if not exists instagram_profile_relationships_related_user_id_idx
  on social.instagram_profile_relationships (related_user_id)
  where related_user_id is not null;

create index if not exists instagram_profile_relationships_owner_verified_idx
  on social.instagram_profile_relationships (owner_profile_id, relationship_type, related_is_verified)
  where related_is_verified is not null;

create index if not exists instagram_profile_relationships_owner_private_idx
  on social.instagram_profile_relationships (owner_profile_id, relationship_type, related_is_private)
  where related_is_private is not null;

-- Canonical post entity extension for Instagram tagged users and locations.
--
-- Index lock/rollback note: replacing this CHECK constraint requires an
-- access-exclusive table lock. Apply during a maintenance window if
-- social.social_post_entities is large. Roll back by restoring the previous
-- constraint value set if any downstream writer fails validation.
alter table social.social_post_entities
  drop constraint if exists social_post_entities_entity_type_check;

alter table social.social_post_entities
  add constraint social_post_entities_entity_type_check
  check (
    entity_type in (
      'hashtag',
      'mention',
      'collaborator',
      'tagged_user',
      'location',
      'sound',
      'thread',
      'flair',
      'url',
      'external_id'
    )
  );

-- Instagram post compatibility/search bridge columns. These keep currently
-- scraped post fields queryable while reads migrate toward social.social_posts
-- and canonical child tables.
alter table social.instagram_posts
  add column if not exists source_input_url text,
  add column if not exists source_post_id text,
  add column if not exists permalink text,
  add column if not exists caption_id text,
  add column if not exists caption_is_edited boolean,
  add column if not exists caption_has_translation boolean,
  add column if not exists owner_user_id text,
  add column if not exists owner_username text,
  add column if not exists owner_profile_pic_url_hd text,
  add column if not exists location_id text,
  add column if not exists location_name text,
  add column if not exists location_raw jsonb not null default '{}'::jsonb,
  add column if not exists original_width integer check (original_width is null or original_width >= 0),
  add column if not exists original_height integer check (original_height is null or original_height >= 0),
  add column if not exists like_and_view_counts_disabled boolean,
  add column if not exists comments_disabled boolean,
  add column if not exists commenting_disabled_for_viewer boolean,
  add column if not exists media_repost_count integer check (media_repost_count is null or media_repost_count >= 0),
  add column if not exists is_paid_partnership boolean,
  add column if not exists is_advertisement boolean,
  add column if not exists can_viewer_reshare boolean,
  add column if not exists has_audio boolean,
  add column if not exists audio_url text;

-- Index lock/rollback note: these narrow B-tree indexes support immediate owner,
-- permalink/source-id, and location filters. For a large live table, create
-- equivalent indexes concurrently outside this transaction or during a
-- maintenance window. Disable by dropping the named indexes; no broad
-- GIN/trigram indexes are added.
create index if not exists instagram_posts_owner_username_idx
  on social.instagram_posts (lower(owner_username))
  where owner_username is not null;

create index if not exists instagram_posts_owner_user_id_idx
  on social.instagram_posts (owner_user_id)
  where owner_user_id is not null;

create index if not exists instagram_posts_source_post_id_idx
  on social.instagram_posts (source_post_id)
  where source_post_id is not null;

create index if not exists instagram_posts_location_id_idx
  on social.instagram_posts (location_id)
  where location_id is not null;

-- Guarded nullable comment columns only; the write-path/backfill phase can
-- populate them without forcing a table rewrite in this schema pass.
alter table social.instagram_comments
  add column if not exists author_full_name text,
  add column if not exists author_profile_pic_url_hd text,
  add column if not exists parent_comment_external_id text,
  add column if not exists root_comment_id uuid references social.instagram_comments(id) on delete set null,
  add column if not exists reply_depth integer check (reply_depth is null or reply_depth >= 0),
  add column if not exists source_snapshot_type text;

-- Index lock/rollback note: these are narrow B-tree indexes tied to immediate
-- profile, relationship, and comment query paths. They run transactionally for
-- Supabase reset/test databases. For a large live table, apply equivalent
-- indexes concurrently during a maintenance window. Disable by dropping the
-- named indexes if a route regresses; no broad GIN/trigram indexes are added.
create index if not exists instagram_comments_post_parent_created_idx
  on social.instagram_comments (post_id, parent_comment_id, created_at asc);

create index if not exists instagram_comments_username_created_idx
  on social.instagram_comments (username, created_at desc);

create index if not exists instagram_comments_root_comment_id_idx
  on social.instagram_comments (root_comment_id)
  where root_comment_id is not null;

create index if not exists instagram_comments_parent_external_id_idx
  on social.instagram_comments (post_id, parent_comment_external_id)
  where parent_comment_external_id is not null;

alter table social.instagram_profiles enable row level security;
alter table social.instagram_profile_external_links enable row level security;
alter table social.instagram_profile_relationships enable row level security;

grant all privileges on table
  social.instagram_profiles,
  social.instagram_profile_external_links,
  social.instagram_profile_relationships
to service_role;

revoke all on table
  social.instagram_profiles,
  social.instagram_profile_external_links,
  social.instagram_profile_relationships
from public, anon, authenticated;

comment on table social.instagram_profiles is
  'Private Instagram profile snapshots with raw_data/about_raw retained for backend service-role diagnostics.';
comment on column social.instagram_profiles.about_raw is
  'Private service-role-only account-about payload; expose curated fields through backend/admin APIs.';
comment on column social.instagram_profiles.raw_data is
  'Private service-role-only raw profile payload; legacy raw table exposure is transitional and unchanged here.';
comment on table social.instagram_profile_external_links is
  'Private Instagram profile external-link rows; raw_data is service-role-only until curated reads are added.';
comment on table social.instagram_profile_relationships is
  'Private Instagram profile following relationships; relationship_type is intentionally following-only.';

commit;
