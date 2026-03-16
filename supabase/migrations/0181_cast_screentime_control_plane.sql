-- Migration 0181: cast screentime control-plane foundation

begin;

create schema if not exists screenalytics;

alter table if exists screenalytics.video_assets
  add column if not exists source_json jsonb not null default '{}'::jsonb;

alter table if exists screenalytics.runs_v2
  add column if not exists run_type text not null default 'v2_visual',
  add column if not exists pipeline_version text null,
  add column if not exists execution_backend text null,
  add column if not exists review_status text not null default 'draft',
  add column if not exists worker_heartbeat_at timestamptz null,
  add column if not exists effective_runtime_seconds numeric null,
  add column if not exists reviewed_at timestamptz null,
  add column if not exists reviewed_by text null,
  add column if not exists review_notes_json jsonb not null default '{}'::jsonb;

update screenalytics.runs_v2
set run_type = 'v2_visual'
where coalesce(nullif(trim(run_type), ''), 'v2_visual') <> 'cast_screentime';

alter table if exists screenalytics.runs_v2
  drop constraint if exists screenalytics_runs_v2_run_type_check;

alter table if exists screenalytics.runs_v2
  add constraint screenalytics_runs_v2_run_type_check
  check (run_type in ('v2_visual', 'cast_screentime'));

alter table if exists screenalytics.runs_v2
  drop constraint if exists screenalytics_runs_v2_review_status_check;

alter table if exists screenalytics.runs_v2
  add constraint screenalytics_runs_v2_review_status_check
  check (review_status in ('draft', 'ready_for_review', 'in_review', 'approved', 'rejected'));

create index if not exists screenalytics_runs_v2_run_type_idx
  on screenalytics.runs_v2 (run_type);

create index if not exists screenalytics_runs_v2_review_status_idx
  on screenalytics.runs_v2 (review_status);

create index if not exists screenalytics_runs_v2_worker_heartbeat_idx
  on screenalytics.runs_v2 (worker_heartbeat_at)
  where status = 'running';

create table if not exists screenalytics.media_upload_sessions (
  id uuid primary key default gen_random_uuid(),
  show_id uuid null references core.shows(id) on delete set null,
  season_id uuid null references core.seasons(id) on delete set null,
  episode_id uuid null references core.episodes(id) on delete set null,
  created_by uuid null,
  status text not null
    check (status in ('pending_upload', 'uploaded', 'verified', 'promoted', 'failed', 'expired')),
  temp_object_key text not null,
  content_type text null,
  expected_size_bytes bigint null,
  expected_checksum_sha256 text null,
  verified_size_bytes bigint null,
  verified_checksum_sha256 text null,
  verification_json jsonb not null default '{}'::jsonb,
  expires_at timestamptz not null,
  verified_at timestamptz null,
  failed_at timestamptz null,
  promoted_video_asset_id uuid null references screenalytics.video_assets(id) on delete set null,
  error_text text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (show_id is not null or season_id is not null or episode_id is not null)
);

create index if not exists screenalytics_media_upload_sessions_status_idx
  on screenalytics.media_upload_sessions (status);

create index if not exists screenalytics_media_upload_sessions_show_idx
  on screenalytics.media_upload_sessions (show_id);

create table if not exists screenalytics.cast_screentime_segments (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
  segment_key text not null,
  person_id uuid null references core.people(id) on delete restrict,
  start_ms integer not null,
  end_ms integer not null,
  duration_ms integer not null,
  frame_count integer not null default 0,
  confidence_score numeric null,
  similarity_score numeric null,
  pose_bucket text null,
  assignment_source text not null,
  is_counted boolean not null default true,
  classification_json jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, segment_key)
);

create index if not exists screenalytics_cast_screentime_segments_run_idx
  on screenalytics.cast_screentime_segments (run_id);

create index if not exists screenalytics_cast_screentime_segments_person_idx
  on screenalytics.cast_screentime_segments (person_id);

create table if not exists screenalytics.cast_screentime_evidence (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
  segment_key text not null,
  evidence_key text not null,
  evidence_type text not null,
  timestamp_ms integer not null,
  object_key text not null,
  content_type text null,
  ttl_expires_at timestamptz null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, evidence_key)
);

create index if not exists screenalytics_cast_screentime_evidence_run_idx
  on screenalytics.cast_screentime_evidence (run_id);

create table if not exists screenalytics.cast_screentime_excluded_sections (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
  section_key text not null,
  section_type text not null,
  start_ms integer not null,
  end_ms integer not null,
  duration_ms integer not null,
  detection_source text not null,
  confidence_score numeric null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, section_key)
);

create index if not exists screenalytics_cast_screentime_excluded_sections_run_idx
  on screenalytics.cast_screentime_excluded_sections (run_id);

create table if not exists screenalytics.cast_screentime_show_settings (
  show_id uuid primary key references core.shows(id) on delete cascade,
  enabled boolean not null default true,
  default_run_config_json jsonb not null default '{}'::jsonb,
  review_policy_json jsonb not null default '{}'::jsonb,
  retention_policy text not null default 'stills_only',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists screenalytics_media_upload_sessions_set_updated_at on screenalytics.media_upload_sessions;
create trigger screenalytics_media_upload_sessions_set_updated_at
before update on screenalytics.media_upload_sessions
for each row execute function core.set_updated_at();

drop trigger if exists screenalytics_cast_screentime_segments_set_updated_at on screenalytics.cast_screentime_segments;
create trigger screenalytics_cast_screentime_segments_set_updated_at
before update on screenalytics.cast_screentime_segments
for each row execute function core.set_updated_at();

drop trigger if exists screenalytics_cast_screentime_evidence_set_updated_at on screenalytics.cast_screentime_evidence;
create trigger screenalytics_cast_screentime_evidence_set_updated_at
before update on screenalytics.cast_screentime_evidence
for each row execute function core.set_updated_at();

drop trigger if exists screenalytics_cast_screentime_excluded_sections_set_updated_at on screenalytics.cast_screentime_excluded_sections;
create trigger screenalytics_cast_screentime_excluded_sections_set_updated_at
before update on screenalytics.cast_screentime_excluded_sections
for each row execute function core.set_updated_at();

drop trigger if exists screenalytics_cast_screentime_show_settings_set_updated_at on screenalytics.cast_screentime_show_settings;
create trigger screenalytics_cast_screentime_show_settings_set_updated_at
before update on screenalytics.cast_screentime_show_settings
for each row execute function core.set_updated_at();

grant usage on schema screenalytics to service_role;
grant all privileges on table screenalytics.media_upload_sessions to service_role;
grant all privileges on table screenalytics.cast_screentime_segments to service_role;
grant all privileges on table screenalytics.cast_screentime_evidence to service_role;
grant all privileges on table screenalytics.cast_screentime_excluded_sections to service_role;
grant all privileges on table screenalytics.cast_screentime_show_settings to service_role;

alter table screenalytics.media_upload_sessions enable row level security;
alter table screenalytics.cast_screentime_segments enable row level security;
alter table screenalytics.cast_screentime_evidence enable row level security;
alter table screenalytics.cast_screentime_excluded_sections enable row level security;
alter table screenalytics.cast_screentime_show_settings enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'screenalytics' and tablename = 'media_upload_sessions' and policyname = 'service_role_all_media_upload_sessions'
  ) then
    create policy "service_role_all_media_upload_sessions"
      on screenalytics.media_upload_sessions for all to service_role
      using (true) with check (true);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'screenalytics' and tablename = 'cast_screentime_segments' and policyname = 'service_role_all_cast_screentime_segments'
  ) then
    create policy "service_role_all_cast_screentime_segments"
      on screenalytics.cast_screentime_segments for all to service_role
      using (true) with check (true);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'screenalytics' and tablename = 'cast_screentime_evidence' and policyname = 'service_role_all_cast_screentime_evidence'
  ) then
    create policy "service_role_all_cast_screentime_evidence"
      on screenalytics.cast_screentime_evidence for all to service_role
      using (true) with check (true);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'screenalytics' and tablename = 'cast_screentime_excluded_sections' and policyname = 'service_role_all_cast_screentime_excluded_sections'
  ) then
    create policy "service_role_all_cast_screentime_excluded_sections"
      on screenalytics.cast_screentime_excluded_sections for all to service_role
      using (true) with check (true);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'screenalytics' and tablename = 'cast_screentime_show_settings' and policyname = 'service_role_all_cast_screentime_show_settings'
  ) then
    create policy "service_role_all_cast_screentime_show_settings"
      on screenalytics.cast_screentime_show_settings for all to service_role
      using (true) with check (true);
  end if;
end $$;

commit;
