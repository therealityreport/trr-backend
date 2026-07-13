begin;

alter table if exists ml.analysis_media_assets
  add column if not exists subtitle_extraction_status text not null default 'not_requested',
  add column if not exists subtitle_extraction_error text null,
  add column if not exists subtitle_extraction_attempts integer not null default 0,
  add column if not exists subtitle_extraction_requested_at timestamptz null,
  add column if not exists subtitle_extraction_started_at timestamptz null,
  add column if not exists subtitle_extraction_completed_at timestamptz null;

alter table if exists ml.analysis_media_assets
  drop constraint if exists analysis_media_assets_subtitle_extraction_status_check;

alter table if exists ml.analysis_media_assets
  add constraint analysis_media_assets_subtitle_extraction_status_check
  check (subtitle_extraction_status in (
    'not_requested', 'queued', 'running', 'complete', 'partial', 'unavailable', 'failed'
  ));

alter table if exists ml.analysis_media_assets
  drop constraint if exists analysis_media_assets_subtitle_extraction_attempts_check;

alter table if exists ml.analysis_media_assets
  add constraint analysis_media_assets_subtitle_extraction_attempts_check
  check (subtitle_extraction_attempts >= 0);

create index if not exists ml_analysis_media_assets_subtitle_extraction_status_idx
  on ml.analysis_media_assets (subtitle_extraction_status);

create table if not exists ml.analysis_media_subtitle_tracks (
  id uuid primary key default gen_random_uuid(),
  video_asset_id uuid not null references ml.analysis_media_assets(id) on delete cascade,
  stream_index integer not null check (stream_index >= 0),
  codec_name text not null,
  language_raw text null,
  language_normalized text null,
  title text null,
  handler_name text null,
  is_default boolean not null default false,
  is_forced boolean not null default false,
  is_primary boolean not null default false,
  selection_status text not null check (selection_status in (
    'eligible_english', 'skipped_non_english', 'skipped_unknown_language', 'unsupported_codec'
  )),
  extraction_status text not null check (extraction_status in (
    'detected', 'queued', 'extracting', 'complete', 'unsupported', 'skipped', 'failed'
  )),
  srt_object_key text null,
  cue_json_object_key text null,
  srt_content_type text null,
  cue_json_content_type text null,
  cue_count integer null check (cue_count is null or cue_count >= 0),
  first_cue_start_ms bigint null check (first_cue_start_ms is null or first_cue_start_ms >= 0),
  last_cue_end_ms bigint null check (last_cue_end_ms is null or last_cue_end_ms >= 0),
  srt_size_bytes bigint null check (srt_size_bytes is null or srt_size_bytes >= 0),
  cue_json_size_bytes bigint null check (cue_json_size_bytes is null or cue_json_size_bytes >= 0),
  srt_sha256 text null,
  cue_json_sha256 text null,
  error_text text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (video_asset_id, stream_index)
);

create index if not exists ml_analysis_media_subtitle_tracks_video_asset_idx
  on ml.analysis_media_subtitle_tracks (video_asset_id);

create index if not exists ml_analysis_media_subtitle_tracks_asset_status_idx
  on ml.analysis_media_subtitle_tracks (video_asset_id, extraction_status);

create unique index if not exists ml_analysis_media_subtitle_tracks_one_primary_uidx
  on ml.analysis_media_subtitle_tracks (video_asset_id)
  where is_primary = true;

drop trigger if exists analysis_media_subtitle_tracks_set_updated_at
  on ml.analysis_media_subtitle_tracks;
create trigger analysis_media_subtitle_tracks_set_updated_at
before update on ml.analysis_media_subtitle_tracks
for each row execute function core.set_updated_at();

grant usage on schema ml to service_role;
grant all privileges on table ml.analysis_media_subtitle_tracks to service_role;

alter table ml.analysis_media_subtitle_tracks enable row level security;

drop policy if exists service_role_all_analysis_media_subtitle_tracks
  on ml.analysis_media_subtitle_tracks;
create policy service_role_all_analysis_media_subtitle_tracks
  on ml.analysis_media_subtitle_tracks
  for all to service_role
  using (true)
  with check (true);

commit;
