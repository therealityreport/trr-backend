-- Isolated characterization schema. Table definitions and additive column
-- statements copied from maintained migrations; no configured service is used.
create schema core; create schema social;
create table core.shows(id uuid primary key); create table core.seasons(id uuid primary key);
create table core.people(id uuid primary key); create table social.scrape_jobs(id uuid primary key); create table social.scrape_runs(id uuid primary key);

-- 0101_social_scrape_tables.sql
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

-- 0118_social_season_analytics.sql
alter table social.instagram_posts
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;

-- 0124_social_thumbnails_and_reddit_sources.sql
alter table social.instagram_posts
  add column if not exists thumbnail_url text;

-- 0132_instagram_permalink_metadata_and_media_mirroring.sql
alter table social.instagram_posts
  add column if not exists post_format text,
  add column if not exists profile_tags jsonb not null default '[]'::jsonb,
  add column if not exists collaborators jsonb not null default '[]'::jsonb,
  add column if not exists hashtags jsonb not null default '[]'::jsonb,
  add column if not exists mentions jsonb not null default '[]'::jsonb,
  add column if not exists duration_seconds integer,
  add column if not exists metadata_source text,
  add column if not exists metadata_scraped_at timestamptz,
  add column if not exists metadata_error text,
  add column if not exists hosted_thumbnail_url text,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text;

-- 0137_instagram_media_mirror_diagnostics.sql
alter table social.instagram_posts
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

-- 0187_social_asset_manifests_and_avatar_registry.sql
alter table if exists social.instagram_posts
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

-- 0190_social_asset_manifest_repair.sql
alter table if exists social.instagram_posts
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

-- 0191_instagram_asset_manifest_repair.sql
alter table if exists social.instagram_posts
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

-- 20260323173500_add_instagram_post_search_columns.sql
alter table social.instagram_posts
  add column if not exists search_text text not null default '',
  add column if not exists search_hashtags text[] not null default array[]::text[],
  add column if not exists search_handles text[] not null default array[]::text[],
  add column if not exists search_handle_identities text[] not null default array[]::text[];

-- 20260330213000_add_instagram_metadata_retry_state.sql
alter table social.instagram_posts
  add column if not exists metadata_last_attempted_at timestamptz,
  add column if not exists metadata_last_failed_at timestamptz,
  add column if not exists metadata_consecutive_failures integer not null default 0;

-- 20260421133959_reconcile_instagram_enhanced_metadata.sql
alter table social.instagram_posts
  add column if not exists tagged_users_detail jsonb not null default '[]'::jsonb,
  add column if not exists collaborators_detail jsonb not null default '[]'::jsonb,
  add column if not exists owner_profile_pic_url text,
  add column if not exists owner_full_name text,
  add column if not exists owner_is_verified boolean,
  add column if not exists product_type text,
  add column if not exists video_play_count integer,
  add column if not exists alt_text text,
  add column if not exists width integer,
  add column if not exists height integer,
  add column if not exists is_comments_disabled boolean,
  add column if not exists music_info jsonb,
  add column if not exists video_duration numeric,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb;

-- 20260421133959_reconcile_instagram_enhanced_metadata.sql
alter table social.instagram_posts
  add column if not exists hosted_owner_profile_pic_url text,
  add column if not exists hosted_tagged_profile_pics jsonb not null default '{}'::jsonb,
  add column if not exists profile_pic_mirror_status text,
  add column if not exists profile_pic_mirror_error text;

-- 20260428145222_instagram_queryable_profile_schema.sql
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

-- 20260503013000_instagram_facebook_crosspost_comments.sql
alter table if exists social.instagram_posts
  add column if not exists fb_comment_count integer,
  add column if not exists fb_like_count integer,
  add column if not exists is_shared_to_fb boolean,
  add column if not exists crosspost_metadata jsonb not null default '{}'::jsonb,
  add column if not exists social_context jsonb not null default '{}'::jsonb,
  add column if not exists facebook_post_id text,
  add column if not exists facebook_post_url text,
  add column if not exists facebook_crosspost_observed_at timestamptz,
  add column if not exists facebook_crosspost_source text;

-- 0101_social_scrape_tables.sql
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

-- 0118_social_season_analytics.sql
alter table social.instagram_comments
  add column if not exists season_id uuid references core.seasons (id) on delete set null,
  add column if not exists job_id uuid references social.scrape_jobs (id) on delete set null,
  add column if not exists source_account text;

-- 0126_social_comment_lifecycle_flags.sql
alter table social.instagram_comments
  add column if not exists is_missing boolean not null default false,
  add column if not exists missing_at timestamptz,
  add column if not exists first_seen_at timestamptz not null default now(),
  add column if not exists last_seen_at timestamptz not null default now(),
  add column if not exists last_seen_run_id uuid references social.scrape_runs (id) on delete set null;

-- 0188_instagram_comment_media_mirror_fields.sql
alter table social.instagram_comments
  add column if not exists media_urls jsonb not null default '[]'::jsonb,
  add column if not exists hosted_media_urls jsonb not null default '[]'::jsonb,
  add column if not exists media_mirror_status text,
  add column if not exists media_mirror_error text,
  add column if not exists media_mirror_attempt_count integer,
  add column if not exists media_mirror_last_attempt_at timestamptz,
  add column if not exists media_mirror_last_job_id text;

-- 20260421124500_instagram_comment_hosted_author_profile_pic.sql
alter table social.instagram_comments
  add column if not exists hosted_author_profile_pic_url text;

-- 20260421132000_instagram_comments_nullable_text_deleted_at.sql
alter table social.instagram_comments
  add column if not exists deleted_at timestamptz null;

-- 20260421133959_reconcile_instagram_enhanced_metadata.sql
alter table social.instagram_comments
  add column if not exists author_profile_pic_url text,
  add column if not exists author_is_verified boolean;

-- 20260428145222_instagram_queryable_profile_schema.sql
alter table social.instagram_comments
  add column if not exists author_full_name text,
  add column if not exists author_profile_pic_url_hd text,
  add column if not exists parent_comment_external_id text,
  add column if not exists root_comment_id uuid references social.instagram_comments(id) on delete set null,
  add column if not exists reply_depth integer check (reply_depth is null or reply_depth >= 0),
  add column if not exists source_snapshot_type text;

-- 20260501222751_instagram_comments_owner_metadata.sql
alter table social.instagram_comments
  add column if not exists comment_url text,
  add column if not exists author_fbid_v2 text,
  add column if not exists author_is_mentionable boolean,
  add column if not exists author_is_private boolean,
  add column if not exists author_latest_reel_media bigint
    check (author_latest_reel_media is null or author_latest_reel_media >= 0),
  add column if not exists author_profile_pic_id text;

-- 20260505173000_instagram_comment_api_phase_metadata.sql
alter table social.instagram_comments
  add column if not exists is_covered boolean not null default false,
  add column if not exists is_ranked boolean not null default false,
  add column if not exists comment_index integer,
  add column if not exists phase text,
  add column if not exists did_report_as_spam boolean not null default false,
  add column if not exists status text not null default 'Active',
  add column if not exists is_edited boolean not null default false,
  add column if not exists is_pinned boolean not null default false,
  add column if not exists meta_ai_comment_type text not null default 'NONE',
  add column if not exists child_comment_count integer not null default 0,
  add column if not exists liked_by_media_coauthors boolean not null default false,
  add column if not exists cursor_min_id text,
  add column if not exists cursor_param text,
  add column if not exists cursor_payload jsonb not null default '{}'::jsonb,
  add column if not exists comment_filter_param text;

-- 0199_shared_account_catalog_backfill.sql
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

-- 20260407170000_catalog_posts_apify_enrichment.sql
alter table social.instagram_account_catalog_posts
  add column if not exists music_info jsonb not null default '{}'::jsonb,
  add column if not exists audio_url text,
  add column if not exists paid_partnership boolean not null default false,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb,
  add column if not exists owner_username text,
  add column if not exists video_play_count bigint not null default 0,
  add column if not exists video_duration numeric;

-- 20260428114333_social_post_canonical_foundation.sql
create table if not exists social.social_posts (
  id uuid primary key default gen_random_uuid(),
  platform text not null check (platform in ('instagram', 'tiktok', 'twitter', 'facebook', 'threads', 'youtube', 'reddit')),
  source_id text not null,
  owner_handle text,
  owner_handle_norm text,
  owner_id text,
  canonical_url text,
  title text,
  body text,
  media_type text,
  posted_at timestamptz,
  like_count bigint not null default 0 check (like_count >= 0),
  comment_count bigint not null default 0 check (comment_count >= 0),
  share_count bigint not null default 0 check (share_count >= 0),
  view_count bigint not null default 0 check (view_count >= 0),
  save_count bigint not null default 0 check (save_count >= 0),
  quote_count bigint not null default 0 check (quote_count >= 0),
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_scraped_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (source_id = btrim(source_id) and source_id <> ''),
  check (owner_handle_norm is null or owner_handle_norm = lower(btrim(owner_handle_norm))),
  unique (platform, source_id),
  unique (platform, id)
);

-- 20260428114333_social_post_canonical_foundation.sql
create table if not exists social.social_post_observations (
  id uuid primary key default gen_random_uuid(),
  platform text not null check (platform in ('instagram', 'tiktok', 'twitter', 'facebook', 'threads', 'youtube', 'reddit')),
  post_id uuid not null,
  source_table text not null,
  source_pk text,
  source_url text,
  scrape_run_id uuid references social.scrape_runs(id) on delete set null,
  observed_at timestamptz not null default now(),
  raw_payload jsonb not null default '{}'::jsonb,
  normalized_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  foreign key (platform, post_id) references social.social_posts(platform, id) on delete cascade,
  check (source_table = btrim(source_table) and source_table <> '')
);

-- 20260911120000_instagram_detail_run_targets.sql
alter table social.social_post_observations
  add column if not exists detail_run_id uuid references social.scrape_runs(id) on delete set null,
  add column if not exists detail_source_id text;

-- 20260428114333_social_post_canonical_foundation.sql
create table if not exists social.social_post_legacy_refs (
  platform text not null check (platform in ('instagram', 'tiktok', 'twitter', 'facebook', 'threads', 'youtube', 'reddit')),
  post_id uuid not null,
  legacy_schema text not null default 'social',
  legacy_table text not null,
  legacy_pk text not null,
  legacy_source_id text not null,
  created_at timestamptz not null default now(),
  primary key (platform, legacy_table, legacy_pk),
  unique (platform, legacy_table, legacy_source_id),
  foreign key (platform, post_id) references social.social_posts(platform, id) on delete cascade,
  check (legacy_schema = btrim(legacy_schema) and legacy_schema <> ''),
  check (legacy_table = btrim(legacy_table) and legacy_table <> ''),
  check (legacy_pk = btrim(legacy_pk) and legacy_pk <> ''),
  check (legacy_source_id = btrim(legacy_source_id) and legacy_source_id <> '')
);

-- 20260428114333_social_post_canonical_foundation.sql
create table if not exists social.social_post_entities (
  platform text not null check (platform in ('instagram', 'tiktok', 'twitter', 'facebook', 'threads', 'youtube', 'reddit')),
  entity_type text not null check (entity_type in ('hashtag', 'mention', 'collaborator', 'sound', 'thread', 'flair', 'url', 'external_id')),
  entity_key text not null,
  entity_key_norm text not null,
  post_id uuid not null,
  entity_payload jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (platform, entity_type, entity_key_norm, post_id),
  foreign key (platform, post_id) references social.social_posts(platform, id) on delete cascade,
  check (entity_key = btrim(entity_key) and entity_key <> ''),
  check (entity_key_norm = lower(btrim(entity_key_norm)) and entity_key_norm <> '')
);

-- 20260428114333_social_post_canonical_foundation.sql
create table if not exists social.social_post_media_assets (
  id uuid primary key default gen_random_uuid(),
  platform text not null check (platform in ('instagram', 'tiktok', 'twitter', 'facebook', 'threads', 'youtube', 'reddit')),
  post_id uuid not null,
  position integer not null default 0 check (position >= 0),
  media_type text,
  source_url text,
  hosted_url text,
  thumbnail_url text,
  hosted_thumbnail_url text,
  width integer check (width is null or width >= 0),
  height integer check (height is null or height >= 0),
  duration_seconds integer check (duration_seconds is null or duration_seconds >= 0),
  mirror_status text,
  mirror_error text,
  mirror_last_attempt_at timestamptz,
  media_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (platform, post_id, position),
  foreign key (platform, post_id) references social.social_posts(platform, id) on delete cascade
);

-- 20260713150000_instagram_current_payload_sidecars.sql
create table if not exists social.instagram_post_payloads (
  post_id uuid primary key references social.instagram_posts(id) on delete cascade,
  raw_data jsonb,
  asset_manifest jsonb not null default '{}'::jsonb,
  child_posts_data jsonb not null default '[]'::jsonb,
  payload_updated_at timestamptz not null default now()
);

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_post_payloads add column if not exists post_id uuid;

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_post_payloads add column if not exists raw_data jsonb;

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_post_payloads add column if not exists asset_manifest jsonb;

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_post_payloads add column if not exists child_posts_data jsonb;

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_post_payloads add column if not exists payload_updated_at timestamptz;

-- 20260713150000_instagram_current_payload_sidecars.sql
create table if not exists social.instagram_account_catalog_post_payloads (
  catalog_post_id uuid primary key references social.instagram_account_catalog_posts(id) on delete cascade,
  raw_data jsonb not null default '{}'::jsonb,
  child_posts_data jsonb not null default '[]'::jsonb,
  payload_updated_at timestamptz not null default now()
);

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_account_catalog_post_payloads add column if not exists catalog_post_id uuid;

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_account_catalog_post_payloads add column if not exists raw_data jsonb;

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_account_catalog_post_payloads add column if not exists child_posts_data jsonb;

-- 20260713150000_instagram_current_payload_sidecars.sql
alter table social.instagram_account_catalog_post_payloads add column if not exists payload_updated_at timestamptz;

-- 20260428114500_instagram_catalog_post_collaborators.sql
create table if not exists social.instagram_account_catalog_post_collaborators (
  catalog_post_id uuid not null references social.instagram_account_catalog_posts(id) on delete cascade,
  source_id text not null,
  source_account text not null,
  collaborator_handle text not null,
  collaborator_source text not null check (collaborator_source in ('collaborators', 'collaborators_detail')),
  posted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (catalog_post_id, collaborator_handle)
);

alter table social.instagram_comments drop constraint instagram_comments_comment_id_key;
alter table social.instagram_comments add unique (post_id,comment_id);
alter table social.social_post_entities drop constraint social_post_entities_entity_type_check;
alter table social.social_post_entities add check (entity_type in
 ('hashtag','mention','collaborator','tagged_user','location','sound','thread','flair','url','external_id'));
