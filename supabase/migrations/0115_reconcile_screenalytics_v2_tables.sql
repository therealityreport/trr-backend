-- Migration 0115: Reconcile screenalytics v2 tables (drift repair)
--
-- Some hosted environments may have migration history drift where migration 0093
-- was recorded but one or more `screenalytics.*` v2 tables are missing.
--
-- This migration is intentionally defensive and idempotent: it creates the v2
-- tables if missing, and ensures RLS is enabled with service_role-only policies.

begin;

create schema if not exists screenalytics;

-- Ensure the v2 FK root exists (created in 0093; also guarded in 0103).
create table if not exists screenalytics.video_assets (
    id uuid primary key default gen_random_uuid(),
    episode_id uuid null references core.episodes(id) on delete cascade,
    season_id uuid null references core.seasons(id) on delete cascade,
    show_id uuid null references core.shows(id) on delete cascade,
    media_asset_id uuid null references core.media_assets(id) on delete set null,
    source_url text null,
    duration_seconds numeric null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (episode_id is not null or season_id is not null or show_id is not null),
    check (media_asset_id is not null or source_url is not null)
);

create index if not exists screenalytics_video_assets_episode_idx on screenalytics.video_assets (episode_id);
create index if not exists screenalytics_video_assets_season_idx on screenalytics.video_assets (season_id);
create index if not exists screenalytics_video_assets_show_idx on screenalytics.video_assets (show_id);

create table if not exists screenalytics.runs_v2 (
    id uuid primary key default gen_random_uuid(),
    video_asset_id uuid not null references screenalytics.video_assets(id) on delete restrict,
    status text not null default 'pending'
        check (status in ('pending', 'running', 'success', 'failed', 'cancelled')),
    run_config_json jsonb not null default '{}'::jsonb,
    config_hash text null,
    candidate_cast_snapshot_json jsonb not null default '[]'::jsonb,
    manifest_key text null,
    started_at timestamptz null,
    completed_at timestamptz null,
    error_message text null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists screenalytics_runs_v2_status_idx on screenalytics.runs_v2 (status);
create index if not exists screenalytics_runs_v2_video_asset_idx on screenalytics.runs_v2 (video_asset_id);

create table if not exists screenalytics.run_artifacts (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
    artifact_key text not null,
    artifact_kind text not null,
    s3_key text not null,
    schema_version text null,
    content_type text null,
    checksum_sha256 text null,
    row_count bigint null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (run_id, artifact_key)
);

create index if not exists screenalytics_run_artifacts_run_idx on screenalytics.run_artifacts (run_id);

create table if not exists screenalytics.run_person_metrics (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
    person_id uuid not null references core.people(id) on delete restrict,
    screen_time_seconds numeric not null default 0,
    frame_count integer not null default 0,
    confidence_avg numeric null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (run_id, person_id)
);

create index if not exists screenalytics_run_person_metrics_run_idx on screenalytics.run_person_metrics (run_id);
create index if not exists screenalytics_run_person_metrics_person_idx on screenalytics.run_person_metrics (person_id);

create table if not exists screenalytics.unknown_clusters (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
    cluster_id text not null,
    track_count integer not null default 0,
    preview_s3_key text null,
    assigned_person_id uuid null references core.people(id) on delete restrict,
    assigned_by text null,
    assigned_at timestamptz null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (run_id, cluster_id)
);

create index if not exists screenalytics_unknown_clusters_run_idx on screenalytics.unknown_clusters (run_id);
create index if not exists screenalytics_unknown_clusters_unassigned_idx
    on screenalytics.unknown_clusters (run_id)
    where assigned_person_id is null;

-- Grants (service_role only)
grant usage on schema screenalytics to service_role;
grant all privileges on all tables in schema screenalytics to service_role;

-- RLS with explicit service_role policies
alter table screenalytics.video_assets enable row level security;
alter table screenalytics.runs_v2 enable row level security;
alter table screenalytics.run_artifacts enable row level security;
alter table screenalytics.run_person_metrics enable row level security;
alter table screenalytics.unknown_clusters enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'screenalytics' and tablename = 'video_assets' and policyname = 'service_role_all_video_assets'
  ) then
    create policy "service_role_all_video_assets"
    on screenalytics.video_assets for all to service_role
    using (true) with check (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'screenalytics' and tablename = 'runs_v2' and policyname = 'service_role_all_runs_v2'
  ) then
    create policy "service_role_all_runs_v2"
    on screenalytics.runs_v2 for all to service_role
    using (true) with check (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'screenalytics' and tablename = 'run_artifacts' and policyname = 'service_role_all_run_artifacts'
  ) then
    create policy "service_role_all_run_artifacts"
    on screenalytics.run_artifacts for all to service_role
    using (true) with check (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'screenalytics' and tablename = 'run_person_metrics' and policyname = 'service_role_all_run_person_metrics'
  ) then
    create policy "service_role_all_run_person_metrics"
    on screenalytics.run_person_metrics for all to service_role
    using (true) with check (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'screenalytics' and tablename = 'unknown_clusters' and policyname = 'service_role_all_unknown_clusters'
  ) then
    create policy "service_role_all_unknown_clusters"
    on screenalytics.unknown_clusters for all to service_role
    using (true) with check (true);
  end if;
end $$;

commit;

