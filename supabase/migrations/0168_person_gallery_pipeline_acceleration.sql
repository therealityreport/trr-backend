-- High-throughput person gallery pipeline acceleration
-- Adds resumable reprocess job/event tables and query-path indexes.

create schema if not exists admin;

create table if not exists admin.person_reprocess_jobs (
  id uuid primary key default gen_random_uuid(),
  person_id uuid not null references core.people(id) on delete cascade,
  requested_by text,
  status text not null default 'queued',
  execution_profile text not null default 'speed',
  request_payload jsonb not null default '{}'::jsonb,
  progress_payload jsonb not null default '{}'::jsonb,
  summary_payload jsonb not null default '{}'::jsonb,
  error text,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

comment on table admin.person_reprocess_jobs is
  'Resumable person-gallery reprocess jobs used by hybrid queue/SSE execution.';

create table if not exists admin.person_reprocess_job_events (
  id bigint generated always as identity primary key,
  job_id uuid not null references admin.person_reprocess_jobs(id) on delete cascade,
  event_index integer not null,
  stage text not null,
  event_type text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (job_id, event_index)
);

comment on table admin.person_reprocess_job_events is
  'Append-only persisted SSE-style events for person gallery reprocess jobs.';

create index if not exists idx_person_reprocess_jobs_person_created
  on admin.person_reprocess_jobs (person_id, created_at desc);

create index if not exists idx_person_reprocess_jobs_status_created
  on admin.person_reprocess_jobs (status, created_at desc);

create index if not exists idx_person_reprocess_job_events_job_created
  on admin.person_reprocess_job_events (job_id, created_at asc);

-- Person gallery target lookup path used by reprocess/scoped filtering.
create index if not exists idx_media_links_person_gallery_entity_kind_id
  on core.media_links (entity_type, entity_id, kind, id)
  where entity_type = 'person' and kind = 'gallery';

-- Asset lookup path used by mirror/tagging update joins.
create index if not exists idx_media_assets_id_source_hosted
  on core.media_assets (id, source, hosted_url);

-- Cast people-tag update path used by auto-count persistence.
create index if not exists idx_cast_photo_people_tags_cast_source
  on admin.cast_photo_people_tags (cast_photo_id, people_count_source);

-- Cast photo scan path used by person gallery stages.
create index if not exists idx_cast_photos_person_source_id_hosted
  on core.cast_photos (person_id, source, id, hosted_url);
