create table if not exists screenalytics.cast_screentime_publish_versions (
  id uuid primary key default gen_random_uuid(),
  video_asset_id uuid not null references screenalytics.video_assets(id) on delete cascade,
  run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
  version_number integer not null,
  published_by text null,
  published_at timestamptz not null default now(),
  notes_json jsonb not null default '{}'::jsonb,
  metrics_snapshot_json jsonb not null default '{}'::jsonb,
  is_current boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists cast_screentime_publish_versions_asset_version_idx
  on screenalytics.cast_screentime_publish_versions (video_asset_id, version_number);

create unique index if not exists cast_screentime_publish_versions_run_idx
  on screenalytics.cast_screentime_publish_versions (run_id);

create unique index if not exists cast_screentime_publish_versions_current_idx
  on screenalytics.cast_screentime_publish_versions (video_asset_id)
  where is_current = true;

create index if not exists cast_screentime_publish_versions_published_at_idx
  on screenalytics.cast_screentime_publish_versions (published_at desc);

create table if not exists screenalytics.cast_screentime_reference_fingerprints (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  season_id uuid null references core.seasons(id) on delete set null,
  episode_id uuid null references core.episodes(id) on delete set null,
  video_asset_id uuid not null references screenalytics.video_assets(id) on delete cascade,
  run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
  scene_key text not null,
  fingerprint_type text not null,
  fingerprint_hash text not null,
  start_ms integer not null,
  end_ms integer not null,
  duration_ms integer not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists cast_screentime_reference_fingerprints_show_hash_idx
  on screenalytics.cast_screentime_reference_fingerprints (show_id, fingerprint_type, fingerprint_hash);

create index if not exists cast_screentime_reference_fingerprints_video_asset_idx
  on screenalytics.cast_screentime_reference_fingerprints (video_asset_id, created_at desc);

do $$
begin
  if to_regprocedure('core.set_updated_at()') is not null then
    if not exists (
      select 1
      from pg_trigger
      where tgname = 'set_updated_at_cast_screentime_publish_versions'
    ) then
      create trigger set_updated_at_cast_screentime_publish_versions
      before update on screenalytics.cast_screentime_publish_versions
      for each row execute function core.set_updated_at();
    end if;
  end if;
end $$;
