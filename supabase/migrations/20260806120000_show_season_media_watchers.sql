-- Durable, additive persistence for configurable show + season media watchers.
-- These tables intentionally retain runs, observations, and revisions when a
-- watch is paused; a pause fences an active worker instead of deleting history.

create unique index if not exists core_seasons_id_show_number_watcher_uq
  on core.seasons (id, show_id, season_number);

create table if not exists core.show_season_media_watches (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete restrict,
  season_id uuid not null,
  target_season_number integer not null check (target_season_number > 0),
  nbcumv_show_id text not null check (btrim(nbcumv_show_id) <> ''),
  bravo_show_uuid uuid not null,
  source_season_rules jsonb not null default '{}'::jsonb
    check (jsonb_typeof(source_season_rules) = 'object'),
  qualification_rules_version text not null
    check (qualification_rules_version ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  status text not null default 'active'
    check (status in ('active', 'paused', 'disabled')),
  sources jsonb not null default '["nbcumv", "bravo"]'::jsonb
    check (jsonb_typeof(sources) = 'array' and jsonb_array_length(sources) > 0),
  resource_types jsonb not null default '["image"]'::jsonb
    check (jsonb_typeof(resource_types) = 'array' and jsonb_array_length(resource_types) > 0),
  poll_interval_seconds integer not null default 60
    check (poll_interval_seconds between 60 and 604800),
  backfill_mode boolean not null default false,
  overlap_seconds integer not null default 300
    check (overlap_seconds between 0 and 3600),
  r2_prefix text not null
    check (r2_prefix ~ '^[A-Za-z0-9][A-Za-z0-9._/-]{0,511}$' and position('..' in r2_prefix) = 0),
  desktop_folder_name text not null
    check (desktop_folder_name ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
  next_check_at timestamptz not null default now(),
  lease_owner text,
  lease_expires_at timestamptz,
  lease_fence bigint not null default 0 check (lease_fence >= 0),
  lease_heartbeat_at timestamptz,
  source_state jsonb not null default '{}'::jsonb
    check (jsonb_typeof(source_state) = 'object'),
  baseline_completed_at timestamptz,
  last_checked_at timestamptz,
  last_success_at timestamptz,
  consecutive_failures integer not null default 0 check (consecutive_failures >= 0),
  last_error text,
  created_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint show_season_media_watches_season_identity_fkey
    foreign key (season_id, show_id, target_season_number)
    references core.seasons (id, show_id, season_number)
    on delete restrict
);

create unique index if not exists show_season_media_watches_active_identity_uq
  on core.show_season_media_watches (show_id, season_id)
  where status = 'active';

create index if not exists show_season_media_watches_due_idx
  on core.show_season_media_watches (next_check_at, id)
  where status = 'active';

create index if not exists show_season_media_watches_lease_idx
  on core.show_season_media_watches (lease_expires_at)
  where lease_expires_at is not null;

create table if not exists core.show_season_media_watch_baseline_generations (
  id uuid primary key default gen_random_uuid(),
  watch_id uuid not null references core.show_season_media_watches(id) on delete restrict,
  generation integer not null check (generation > 0),
  qualification_rules_version text not null,
  source_season_rules_snapshot jsonb not null
    check (jsonb_typeof(source_season_rules_snapshot) = 'object'),
  status text not null default 'running'
    check (status in ('running', 'completed', 'failed', 'superseded')),
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  created_by text,
  created_at timestamptz not null default now(),
  unique (watch_id, generation)
);

create table if not exists core.show_season_media_watch_runs (
  id uuid primary key default gen_random_uuid(),
  watch_id uuid not null references core.show_season_media_watches(id) on delete restrict,
  lease_fence bigint not null check (lease_fence > 0),
  baseline_generation_id uuid references core.show_season_media_watch_baseline_generations(id) on delete restrict,
  bravotv_image_run_id uuid references core.bravotv_image_runs(id) on delete set null,
  status text not null default 'running'
    check (status in ('running', 'incomplete', 'completed', 'failed', 'fenced')),
  source_state_before jsonb not null default '{}'::jsonb
    check (jsonb_typeof(source_state_before) = 'object'),
  source_state_after jsonb not null default '{}'::jsonb
    check (jsonb_typeof(source_state_after) = 'object'),
  cursor_journal jsonb not null default '{}'::jsonb
    check (jsonb_typeof(cursor_journal) = 'object'),
  candidate_journal jsonb not null default '{}'::jsonb
    check (jsonb_typeof(candidate_journal) = 'object'),
  summary jsonb not null default '{}'::jsonb
    check (jsonb_typeof(summary) = 'object'),
  continuation jsonb not null default '{}'::jsonb
    check (jsonb_typeof(continuation) = 'object'),
  error_detail text,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (watch_id, lease_fence)
);

create index if not exists show_season_media_watch_runs_watch_created_idx
  on core.show_season_media_watch_runs (watch_id, created_at desc);

create table if not exists core.show_season_media_watch_observations (
  id uuid primary key default gen_random_uuid(),
  watch_id uuid not null references core.show_season_media_watches(id) on delete restrict,
  baseline_generation_id uuid references core.show_season_media_watch_baseline_generations(id) on delete restrict,
  source text not null check (source in ('nbcumv', 'bravo')),
  source_asset_id text not null check (btrim(source_asset_id) <> ''),
  source_updated_at timestamptz,
  source_fingerprint jsonb not null default '{}'::jsonb
    check (jsonb_typeof(source_fingerprint) = 'object'),
  source_url text,
  raw_season_fields jsonb not null default '{}'::jsonb
    check (jsonb_typeof(raw_season_fields) = 'object'),
  metadata jsonb not null default '{}'::jsonb
    check (jsonb_typeof(metadata) = 'object'),
  acquisition_state text not null default 'observed_without_bytes'
    check (acquisition_state in ('observed_without_bytes', 'discovered', 'downloaded', 'r2_uploaded', 'db_committed', 'rejected')),
  revalidate_after timestamptz,
  last_acquired_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (watch_id, source, source_asset_id)
);

create index if not exists show_season_media_watch_observations_acquisition_idx
  on core.show_season_media_watch_observations (watch_id, acquisition_state, revalidate_after);

create table if not exists core.media_source_revisions (
  id uuid primary key default gen_random_uuid(),
  watch_id uuid not null references core.show_season_media_watches(id) on delete restrict,
  media_asset_id uuid not null references core.media_assets(id) on delete restrict,
  source text not null check (source in ('nbcumv', 'bravo')),
  source_asset_id text not null check (btrim(source_asset_id) <> ''),
  source_updated_at timestamptz,
  sha256 text not null check (sha256 ~ '^[A-Fa-f0-9]{64}$'),
  content_type text,
  bytes bigint check (bytes is null or bytes >= 0),
  width integer check (width is null or width > 0),
  height integer check (height is null or height > 0),
  etag text,
  source_url text,
  hosted_bucket text,
  hosted_key text,
  hosted_url text,
  fetched_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb
    check (jsonb_typeof(metadata) = 'object'),
  acquisition_state text not null default 'db_committed'
    check (acquisition_state in ('discovered', 'downloaded', 'r2_uploaded', 'db_committed', 'rejected')),
  state_updated_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  unique (media_asset_id, sha256)
);

create index if not exists media_source_revisions_watch_created_idx
  on core.media_source_revisions (watch_id, created_at desc);

create index if not exists media_source_revisions_source_identity_idx
  on core.media_source_revisions (source, source_asset_id, source_updated_at desc);

alter table core.bravotv_image_runs
  add column if not exists watch_id uuid references core.show_season_media_watches(id) on delete set null;

create index if not exists idx_bravotv_image_runs_watch_created_at
  on core.bravotv_image_runs (watch_id, created_at desc)
  where watch_id is not null;

create or replace function core.enforce_show_season_media_watch_rules_immutable()
returns trigger
language plpgsql
set search_path = core, pg_temp
as $$
begin
  if new.qualification_rules_version is distinct from old.qualification_rules_version
     or new.source_season_rules is distinct from old.source_season_rules then
    raise exception 'watch qualification and source-season rules are immutable; create a new baseline generation or watch';
  end if;
  return new;
end;
$$;

create or replace function core.enforce_media_source_revision_immutability()
returns trigger
language plpgsql
set search_path = core, pg_temp
as $$
begin
  if new.id is distinct from old.id
     or new.watch_id is distinct from old.watch_id
     or new.media_asset_id is distinct from old.media_asset_id
     or new.source is distinct from old.source
     or new.source_asset_id is distinct from old.source_asset_id
     or new.source_updated_at is distinct from old.source_updated_at
     or new.sha256 is distinct from old.sha256
     or new.content_type is distinct from old.content_type
     or new.bytes is distinct from old.bytes
     or new.width is distinct from old.width
     or new.height is distinct from old.height
     or new.etag is distinct from old.etag
     or new.source_url is distinct from old.source_url
     or new.hosted_bucket is distinct from old.hosted_bucket
     or new.hosted_key is distinct from old.hosted_key
     or new.hosted_url is distinct from old.hosted_url
     or new.fetched_at is distinct from old.fetched_at
     or new.metadata is distinct from old.metadata
     or new.created_at is distinct from old.created_at then
    raise exception 'media source revisions are immutable';
  end if;
  if new.acquisition_state is distinct from old.acquisition_state
     and not ((old.acquisition_state = 'discovered' and new.acquisition_state in ('downloaded', 'rejected'))
       or (old.acquisition_state = 'downloaded' and new.acquisition_state in ('r2_uploaded', 'rejected'))
       or (old.acquisition_state = 'r2_uploaded' and new.acquisition_state = 'db_committed')) then
    raise exception 'invalid immutable media source revision acquisition-state transition';
  end if;
  return new;
end;
$$;

drop trigger if exists core_show_season_media_watches_rules_immutable on core.show_season_media_watches;
create trigger core_show_season_media_watches_rules_immutable
before update on core.show_season_media_watches
for each row execute function core.enforce_show_season_media_watch_rules_immutable();

drop trigger if exists core_media_source_revisions_immutable on core.media_source_revisions;
create trigger core_media_source_revisions_immutable
before update on core.media_source_revisions
for each row execute function core.enforce_media_source_revision_immutability();

drop trigger if exists core_show_season_media_watches_set_updated_at on core.show_season_media_watches;
create trigger core_show_season_media_watches_set_updated_at
before update on core.show_season_media_watches
for each row execute function core.set_updated_at();

drop trigger if exists core_show_season_media_watch_runs_set_updated_at on core.show_season_media_watch_runs;
create trigger core_show_season_media_watch_runs_set_updated_at
before update on core.show_season_media_watch_runs
for each row execute function core.set_updated_at();

drop trigger if exists core_show_season_media_watch_observations_set_updated_at on core.show_season_media_watch_observations;
create trigger core_show_season_media_watch_observations_set_updated_at
before update on core.show_season_media_watch_observations
for each row execute function core.set_updated_at();

alter table core.show_season_media_watches enable row level security;
alter table core.show_season_media_watch_baseline_generations enable row level security;
alter table core.show_season_media_watch_runs enable row level security;
alter table core.show_season_media_watch_observations enable row level security;
alter table core.media_source_revisions enable row level security;

grant all privileges on table core.show_season_media_watches,
  core.show_season_media_watch_baseline_generations,
  core.show_season_media_watch_runs,
  core.show_season_media_watch_observations,
  core.media_source_revisions to service_role;
revoke all on table core.show_season_media_watches,
  core.show_season_media_watch_baseline_generations,
  core.show_season_media_watch_runs,
  core.show_season_media_watch_observations,
  core.media_source_revisions from public, anon, authenticated;

drop policy if exists show_season_media_watches_service_role_all on core.show_season_media_watches;
create policy show_season_media_watches_service_role_all on core.show_season_media_watches
  for all to service_role using (true) with check (true);
drop policy if exists show_season_media_watch_baselines_service_role_all on core.show_season_media_watch_baseline_generations;
create policy show_season_media_watch_baselines_service_role_all on core.show_season_media_watch_baseline_generations
  for all to service_role using (true) with check (true);
drop policy if exists show_season_media_watch_runs_service_role_all on core.show_season_media_watch_runs;
create policy show_season_media_watch_runs_service_role_all on core.show_season_media_watch_runs
  for all to service_role using (true) with check (true);
drop policy if exists show_season_media_watch_observations_service_role_all on core.show_season_media_watch_observations;
create policy show_season_media_watch_observations_service_role_all on core.show_season_media_watch_observations
  for all to service_role using (true) with check (true);
drop policy if exists media_source_revisions_service_role_all on core.media_source_revisions;
create policy media_source_revisions_service_role_all on core.media_source_revisions
  for all to service_role using (true) with check (true);
