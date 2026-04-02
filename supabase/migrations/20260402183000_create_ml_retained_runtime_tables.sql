create extension if not exists vector;

create schema if not exists ml;

create table if not exists ml.face_reference_images (
  id uuid primary key default gen_random_uuid(),
  person_id uuid not null references core.people(id) on delete cascade,
  media_link_id uuid not null unique references core.media_links(id) on delete cascade,
  media_asset_id uuid not null references core.media_assets(id) on delete cascade,
  is_active boolean not null default true,
  approved boolean not null default true,
  embedding_status text not null default 'pending',
  source_url text,
  hosted_url text,
  hosted_sha256 text,
  metadata jsonb not null default '{}'::jsonb,
  last_enqueued_at timestamptz,
  deactivated_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists ml_face_reference_images_person_idx
  on ml.face_reference_images (person_id, is_active, approved);

create table if not exists ml.face_reference_embeddings (
  id uuid primary key default gen_random_uuid(),
  reference_image_id uuid not null references ml.face_reference_images(id) on delete cascade,
  provider text not null,
  model_name text not null,
  model_version text,
  embedding_status text not null default 'pending',
  embedding vector(512),
  metadata jsonb not null default '{}'::jsonb,
  error_message text,
  generated_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists ml_face_reference_embeddings_reference_idx
  on ml.face_reference_embeddings (reference_image_id, embedding_status);

create unique index if not exists ml_face_reference_embeddings_unique_idx
  on ml.face_reference_embeddings (reference_image_id, provider, model_name, coalesce(model_version, ''));

create table if not exists ml.analysis_media_upload_sessions (
  id uuid primary key default gen_random_uuid(),
  show_id uuid references core.shows(id) on delete cascade,
  season_id uuid references core.seasons(id) on delete cascade,
  episode_id uuid references core.episodes(id) on delete cascade,
  created_by text,
  status text not null default 'pending_upload',
  temp_object_key text,
  content_type text,
  expected_size_bytes bigint,
  expected_checksum_sha256 text,
  verification_json jsonb not null default '{}'::jsonb,
  expires_at timestamptz,
  video_class text not null default 'episode',
  promo_subtype text,
  media_type text not null default 'episode',
  media_kind text,
  source_import_type text not null default 'upload',
  owner_scope text not null default 'season',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists ml.analysis_media_assets (
  id uuid primary key default gen_random_uuid(),
  episode_id uuid references core.episodes(id) on delete cascade,
  season_id uuid references core.seasons(id) on delete cascade,
  show_id uuid references core.shows(id) on delete cascade,
  media_asset_id uuid references core.media_assets(id) on delete set null,
  source_url text,
  source_json jsonb not null default '{}'::jsonb,
  duration_seconds integer,
  metadata jsonb not null default '{}'::jsonb,
  video_class text not null default 'episode',
  promo_subtype text,
  media_type text not null default 'episode',
  media_kind text,
  source_import_type text not null default 'upload',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists ml.analysis_media_cast_candidates (
  video_asset_id uuid not null references ml.analysis_media_assets(id) on delete cascade,
  person_id uuid not null references core.people(id) on delete cascade,
  source text,
  confidence double precision,
  credit_category text,
  billing_order integer,
  role text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (video_asset_id, person_id)
);

create table if not exists ml.screentime_runs (
  id uuid primary key default gen_random_uuid(),
  video_asset_id uuid not null references ml.analysis_media_assets(id) on delete cascade,
  status text not null default 'pending',
  run_type text not null default 'cast_screentime',
  pipeline_version text,
  execution_backend text,
  review_status text not null default 'draft',
  run_config_json jsonb not null default '{}'::jsonb,
  config_hash text,
  candidate_cast_snapshot_json jsonb not null default '[]'::jsonb,
  candidate_scope_policy_json jsonb not null default '{}'::jsonb,
  cast_coverage_summary_json jsonb not null default '{}'::jsonb,
  dispatch_status text,
  dispatch_job_id text,
  dispatch_accepted_at timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  error_message text,
  worker_heartbeat_at timestamptz,
  reviewed_at timestamptz,
  reviewed_by text,
  review_notes_json jsonb not null default '{}'::jsonb,
  effective_runtime_seconds double precision,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists ml_screentime_runs_video_asset_idx
  on ml.screentime_runs (video_asset_id, created_at desc);

create table if not exists ml.screentime_artifacts (
  run_id uuid not null references ml.screentime_runs(id) on delete cascade,
  artifact_key text not null,
  artifact_kind text,
  s3_key text,
  schema_version text,
  content_type text,
  checksum_sha256 text,
  row_count integer,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (run_id, artifact_key)
);

create table if not exists ml.screentime_person_metrics (
  run_id uuid not null references ml.screentime_runs(id) on delete cascade,
  person_id uuid not null references core.people(id) on delete cascade,
  screen_time_seconds double precision,
  frame_count integer,
  confidence_avg double precision,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (run_id, person_id)
);

create table if not exists ml.screentime_segments (
  run_id uuid not null references ml.screentime_runs(id) on delete cascade,
  segment_key text not null,
  person_id uuid references core.people(id) on delete set null,
  start_ms integer,
  end_ms integer,
  duration_ms integer,
  frame_count integer not null default 0,
  confidence_score double precision,
  similarity_score double precision,
  pose_bucket text,
  assignment_source text,
  is_counted boolean not null default true,
  classification_json jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (run_id, segment_key)
);

create table if not exists ml.screentime_evidence (
  run_id uuid not null references ml.screentime_runs(id) on delete cascade,
  segment_key text,
  evidence_key text not null,
  evidence_type text,
  timestamp_ms integer,
  object_key text,
  content_type text,
  ttl_expires_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (run_id, evidence_key)
);

create table if not exists ml.screentime_review_state (
  id uuid primary key default gen_random_uuid(),
  run_id uuid references ml.screentime_runs(id) on delete cascade,
  show_id uuid references core.shows(id) on delete cascade,
  season_id uuid references core.seasons(id) on delete cascade,
  episode_id uuid references core.episodes(id) on delete cascade,
  owner_scope text not null,
  owner_entity_id uuid not null,
  video_asset_id uuid references ml.analysis_media_assets(id) on delete cascade,
  review_kind text not null,
  review_key text not null,
  person_id uuid references core.people(id) on delete set null,
  candidate_person_id uuid references core.people(id) on delete set null,
  queue_group text,
  decision text,
  section_type text,
  start_ms integer,
  end_ms integer,
  duration_ms integer,
  detection_source text,
  confidence_score double precision,
  escalation_level text,
  recommended_action text,
  notes_json jsonb not null default '{}'::jsonb,
  payload_json jsonb not null default '{}'::jsonb,
  decided_by text,
  decided_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (owner_scope, owner_entity_id, review_kind, review_key)
);

create index if not exists ml_screentime_review_state_run_idx
  on ml.screentime_review_state (run_id, review_kind, decided_at desc);

create table if not exists ml.screentime_publications (
  id uuid primary key default gen_random_uuid(),
  video_asset_id uuid not null references ml.analysis_media_assets(id) on delete cascade,
  run_id uuid not null unique references ml.screentime_runs(id) on delete cascade,
  version_number integer not null,
  published_by text,
  notes_json jsonb not null default '{}'::jsonb,
  metrics_snapshot_json jsonb not null default '{}'::jsonb,
  is_current boolean not null default true,
  published_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists ml_screentime_publications_current_idx
  on ml.screentime_publications (video_asset_id)
  where is_current = true;

create table if not exists ml.screentime_reference_fingerprints (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  season_id uuid references core.seasons(id) on delete cascade,
  episode_id uuid references core.episodes(id) on delete cascade,
  video_asset_id uuid not null references ml.analysis_media_assets(id) on delete cascade,
  run_id uuid not null references ml.screentime_runs(id) on delete cascade,
  scene_key text,
  fingerprint_type text,
  fingerprint_hash text,
  start_ms integer,
  end_ms integer,
  duration_ms integer,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists ml.screentime_unknown_clusters (
  run_id uuid not null references ml.screentime_runs(id) on delete cascade,
  cluster_id text not null,
  track_count integer not null default 0,
  preview_s3_key text,
  metadata jsonb not null default '{}'::jsonb,
  assigned_person_id uuid references core.people(id) on delete set null,
  assigned_by text,
  assigned_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (run_id, cluster_id)
);
