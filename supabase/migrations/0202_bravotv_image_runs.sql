create table if not exists core.bravotv_image_runs (
  id uuid primary key default gen_random_uuid(),
  operation_id uuid references core.admin_operations(id) on delete set null,
  mode text not null check (mode in ('show', 'person')),
  status text not null check (status in ('pending', 'running', 'completed', 'failed', 'cancelled')),
  target_show_id uuid references core.shows(id) on delete set null,
  target_person_id uuid references core.people(id) on delete set null,
  show_name text,
  person_name text,
  season integer,
  episode integer,
  selected_sources jsonb not null default '[]'::jsonb,
  refreshed_artifacts jsonb not null default '[]'::jsonb,
  artifact_paths jsonb not null default '{}'::jsonb,
  request_payload jsonb not null default '{}'::jsonb,
  manifest jsonb not null default '{}'::jsonb,
  summary jsonb not null default '{}'::jsonb,
  import_summary jsonb not null default '{}'::jsonb,
  review_summary jsonb not null default '{}'::jsonb,
  created_by text,
  error_detail text,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_bravotv_image_runs_status_created_at
  on core.bravotv_image_runs(status, created_at desc);

create index if not exists idx_bravotv_image_runs_mode_created_at
  on core.bravotv_image_runs(mode, created_at desc);

create index if not exists idx_bravotv_image_runs_show_created_at
  on core.bravotv_image_runs(target_show_id, created_at desc)
  where target_show_id is not null;

create index if not exists idx_bravotv_image_runs_person_created_at
  on core.bravotv_image_runs(target_person_id, created_at desc)
  where target_person_id is not null;

create unique index if not exists idx_bravotv_image_runs_operation_id
  on core.bravotv_image_runs(operation_id)
  where operation_id is not null;

drop trigger if exists core_bravotv_image_runs_set_updated_at on core.bravotv_image_runs;
create trigger core_bravotv_image_runs_set_updated_at
before update on core.bravotv_image_runs
for each row
execute function core.set_updated_at();
