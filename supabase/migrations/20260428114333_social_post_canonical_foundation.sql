begin;

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

create table if not exists social.social_post_memberships (
  platform text not null check (platform in ('instagram', 'tiktok', 'twitter', 'facebook', 'threads', 'youtube', 'reddit')),
  membership_type text not null check (membership_type in ('account', 'community', 'show', 'season', 'person', 'channel')),
  membership_key text not null,
  membership_key_norm text not null,
  post_id uuid not null,
  assignment_status text not null default 'unassigned'
    check (assignment_status in ('assigned', 'unassigned', 'ambiguous', 'needs_review')),
  assigned_show_id uuid references core.shows(id) on delete set null,
  assigned_season_id uuid references core.seasons(id) on delete set null,
  assigned_person_id uuid references core.people(id) on delete set null,
  assignment_source text,
  candidate_matches jsonb not null default '[]'::jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_backfill_run_id uuid references social.scrape_runs(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (platform, membership_type, membership_key_norm, post_id),
  foreign key (platform, post_id) references social.social_posts(platform, id) on delete cascade,
  check (membership_key = btrim(membership_key) and membership_key <> ''),
  check (membership_key_norm = lower(btrim(membership_key_norm)) and membership_key_norm <> '')
);

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

create index if not exists social_posts_platform_owner_posted_idx
  on social.social_posts (platform, owner_handle_norm, posted_at desc nulls last, id);

create index if not exists social_posts_platform_posted_idx
  on social.social_posts (platform, posted_at desc nulls last, id);

create index if not exists social_posts_owner_handle_norm_idx
  on social.social_posts (owner_handle_norm)
  where owner_handle_norm is not null;

create index if not exists social_post_observations_post_observed_idx
  on social.social_post_observations (platform, post_id, observed_at desc);

create index if not exists social_post_observations_source_idx
  on social.social_post_observations (platform, source_table, source_pk)
  where source_pk is not null;

create index if not exists social_post_legacy_refs_post_idx
  on social.social_post_legacy_refs (platform, post_id);

create index if not exists social_post_legacy_refs_source_id_idx
  on social.social_post_legacy_refs (platform, legacy_source_id);

create index if not exists social_post_memberships_lookup_idx
  on social.social_post_memberships (platform, membership_type, membership_key_norm, last_seen_at desc);

create index if not exists social_post_memberships_post_idx
  on social.social_post_memberships (platform, post_id);

create index if not exists social_post_memberships_assignment_idx
  on social.social_post_memberships (assignment_status, platform, last_seen_at desc);

create index if not exists social_post_entities_lookup_idx
  on social.social_post_entities (platform, entity_type, entity_key_norm, last_seen_at desc);

create index if not exists social_post_entities_post_idx
  on social.social_post_entities (platform, post_id);

create index if not exists social_post_media_assets_post_idx
  on social.social_post_media_assets (platform, post_id, position);

create index if not exists social_post_media_assets_mirror_pending_idx
  on social.social_post_media_assets (platform, mirror_status, created_at desc)
  where mirror_status in ('pending', 'partial', 'failed');

drop trigger if exists set_social_posts_updated_at on social.social_posts;
create trigger set_social_posts_updated_at
before update on social.social_posts
for each row
execute function public.set_current_timestamp_updated_at();

drop trigger if exists set_social_post_memberships_updated_at on social.social_post_memberships;
create trigger set_social_post_memberships_updated_at
before update on social.social_post_memberships
for each row
execute function public.set_current_timestamp_updated_at();

drop trigger if exists set_social_post_entities_updated_at on social.social_post_entities;
create trigger set_social_post_entities_updated_at
before update on social.social_post_entities
for each row
execute function public.set_current_timestamp_updated_at();

drop trigger if exists set_social_post_media_assets_updated_at on social.social_post_media_assets;
create trigger set_social_post_media_assets_updated_at
before update on social.social_post_media_assets
for each row
execute function public.set_current_timestamp_updated_at();

alter table social.social_posts enable row level security;
alter table social.social_post_observations enable row level security;
alter table social.social_post_legacy_refs enable row level security;
alter table social.social_post_memberships enable row level security;
alter table social.social_post_entities enable row level security;
alter table social.social_post_media_assets enable row level security;

grant select on table
  social.social_posts,
  social.social_post_memberships,
  social.social_post_entities,
  social.social_post_media_assets
to anon, authenticated;

grant all privileges on table
  social.social_posts,
  social.social_post_observations,
  social.social_post_legacy_refs,
  social.social_post_memberships,
  social.social_post_entities,
  social.social_post_media_assets
to service_role;

revoke all on table social.social_post_observations from public, anon, authenticated;
revoke all on table social.social_post_legacy_refs from public, anon, authenticated;

drop policy if exists social_posts_public_read on social.social_posts;
create policy social_posts_public_read
on social.social_posts
for select to anon, authenticated using (true);

drop policy if exists social_post_memberships_public_read on social.social_post_memberships;
create policy social_post_memberships_public_read
on social.social_post_memberships
for select to anon, authenticated using (true);

drop policy if exists social_post_entities_public_read on social.social_post_entities;
create policy social_post_entities_public_read
on social.social_post_entities
for select to anon, authenticated using (true);

drop policy if exists social_post_media_assets_public_read on social.social_post_media_assets;
create policy social_post_media_assets_public_read
on social.social_post_media_assets
for select to anon, authenticated using (true);

commit;
