begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';

create schema if not exists admin;

create table if not exists admin.private_video_object_migration_runs (
  id uuid primary key default gen_random_uuid(),
  inventory_run_id uuid not null references admin.object_storage_inventory_runs(id) on delete restrict,
  source_bucket text not null,
  destination_bucket text not null,
  status text not null default 'pending'
    check (status in ('pending', 'running', 'complete', 'failed', 'deleted', 'rolled_back')),
  discovered_count bigint not null default 0 check (discovered_count >= 0),
  migrated_count bigint not null default 0 check (migrated_count >= 0),
  failed_count bigint not null default 0 check (failed_count >= 0),
  deleted_count bigint not null default 0 check (deleted_count >= 0),
  error_message text,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (source_bucket = btrim(source_bucket) and source_bucket <> ''),
  check (destination_bucket = btrim(destination_bucket) and destination_bucket <> ''),
  check (source_bucket <> destination_bucket)
);

create table if not exists admin.private_video_object_migration_items (
  id bigint generated always as identity primary key,
  run_id uuid not null references admin.private_video_object_migration_runs(id) on delete restrict,
  video_asset_id uuid not null references screenalytics.video_assets(id) on delete restrict,
  inventory_item_id bigint not null references admin.object_storage_inventory_items(id) on delete restrict,
  source_bucket text not null,
  destination_bucket text not null,
  object_key text not null,
  expected_size_bytes bigint not null check (expected_size_bytes >= 0),
  source_last_modified timestamptz not null,
  source_etag text not null,
  expected_sha256 text not null check (expected_sha256 ~ '^[0-9a-f]{64}$'),
  destination_size_bytes bigint,
  destination_etag text,
  destination_sha256 text check (destination_sha256 is null or destination_sha256 ~ '^[0-9a-f]{64}$'),
  old_source_url text,
  new_source_url text,
  old_source_json jsonb not null,
  new_source_json jsonb not null,
  status text not null default 'pending'
    check (status in ('pending', 'copying', 'verified', 'db_updated', 'source_deleted', 'rolled_back', 'failed')),
  attempt_count integer not null default 0 check (attempt_count >= 0),
  error_message text,
  copied_at timestamptz,
  database_updated_at timestamptz,
  source_deleted_at timestamptz,
  rolled_back_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, video_asset_id),
  check (source_bucket = btrim(source_bucket) and source_bucket <> ''),
  check (destination_bucket = btrim(destination_bucket) and destination_bucket <> ''),
  check (source_bucket <> destination_bucket),
  check (object_key = btrim(object_key) and object_key <> ''),
  check (source_etag = btrim(source_etag) and source_etag <> '')
);

create index if not exists private_video_migration_runs_inventory_idx
  on admin.private_video_object_migration_runs (inventory_run_id, created_at desc);

create index if not exists private_video_migration_items_status_idx
  on admin.private_video_object_migration_items (run_id, status, id);

create index if not exists private_video_migration_items_asset_idx
  on admin.private_video_object_migration_items (video_asset_id, id desc);

alter table admin.private_video_object_migration_runs enable row level security;
alter table admin.private_video_object_migration_items enable row level security;

do $migration$
declare
  relation_name text;
  policy_name text;
begin
  foreach relation_name in array array[
    'private_video_object_migration_runs',
    'private_video_object_migration_items'
  ]
  loop
    policy_name := 'deny_api_access_admin_' || relation_name;
    if not exists (
      select 1 from pg_policy
      where polrelid = format('admin.%I', relation_name)::regclass
        and polname = policy_name
    ) then
      execute format(
        'create policy %I on admin.%I as restrictive for all to public using (false) with check (false)',
        policy_name,
        relation_name
      );
    end if;
  end loop;
end;
$migration$;

grant usage on schema admin to service_role;
grant select, insert, update on admin.private_video_object_migration_runs to service_role;
grant select, insert, update on admin.private_video_object_migration_items to service_role;
grant usage, select on all sequences in schema admin to service_role;

comment on table admin.private_video_object_migration_runs is
  'Operator-only runs that move DB-owned legacy screenalytics videos from the public R2 bucket to the private bucket.';
comment on table admin.private_video_object_migration_items is
  'Reversible per-video audit ledger. Public deletion is separately gated after private verification and DB promotion.';

commit;
