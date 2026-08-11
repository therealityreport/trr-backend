begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';

do $upgrade$
declare
  has_old_item_shape boolean;
  has_reference_table boolean;
  run_count bigint;
  item_count bigint;
  reference_count bigint := 0;
begin
  if to_regclass('admin.private_video_object_migration_runs') is null
     or to_regclass('admin.private_video_object_migration_items') is null then
    raise exception 'private video migration base tables are missing; apply 20260722200000 first';
  end if;

  select exists (
    select 1 from information_schema.columns
    where table_schema = 'admin'
      and table_name = 'private_video_object_migration_items'
      and column_name in ('video_asset_id', 'old_source_url', 'new_source_url', 'old_source_json', 'new_source_json')
  ) into has_old_item_shape;

  has_reference_table := to_regclass('admin.private_video_object_migration_references') is not null;
  execute 'select count(*) from admin.private_video_object_migration_runs' into run_count;
  execute 'select count(*) from admin.private_video_object_migration_items' into item_count;
  if has_reference_table then
    execute 'select count(*) from admin.private_video_object_migration_references' into reference_count;
  end if;

  if has_old_item_shape and (run_count <> 0 or item_count <> 0 or reference_count <> 0) then
    raise exception using
      message = 'refusing private video ledger shape upgrade because the old tables contain rows',
      detail = format('runs=%s items=%s references=%s', run_count, item_count, reference_count),
      hint = 'Preserve and explicitly migrate the existing ledger rows before applying this migration.';
  end if;

  if not has_old_item_shape and not has_reference_table and (run_count <> 0 or item_count <> 0) then
    raise exception using
      message = 'refusing partial private video ledger upgrade with populated object tables and no reference ledger',
      detail = format('runs=%s items=%s', run_count, item_count);
  end if;

  if has_old_item_shape then
    alter table admin.private_video_object_migration_items
      drop column if exists video_asset_id,
      drop column if exists old_source_url,
      drop column if exists new_source_url,
      drop column if exists old_source_json,
      drop column if exists new_source_json;
  end if;
end;
$upgrade$;

alter table admin.private_video_object_migration_runs
  add column if not exists reference_count bigint not null default 0 check (reference_count >= 0);

create unique index if not exists private_video_migration_items_run_source_key_uidx
  on admin.private_video_object_migration_items (run_id, source_bucket, object_key);

create table if not exists admin.private_video_object_migration_references (
  id bigint generated always as identity primary key,
  item_id bigint not null references admin.private_video_object_migration_items(id) on delete restrict,
  reference_kind text not null
    check (reference_kind in ('ml_analysis_media_assets', 'screenalytics_video_assets')),
  row_id uuid not null,
  old_source_url text,
  new_source_url text,
  old_source_json jsonb not null,
  new_source_json jsonb not null,
  status text not null default 'pending'
    check (status in ('pending', 'db_updated', 'rolled_back')),
  database_updated_at timestamptz,
  rolled_back_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (item_id, reference_kind, row_id)
);

create index if not exists private_video_migration_references_item_idx
  on admin.private_video_object_migration_references (item_id, reference_kind, row_id);

alter table admin.private_video_object_migration_references enable row level security;

do $policy$
begin
  if not exists (
    select 1 from pg_policy
    where polrelid = 'admin.private_video_object_migration_references'::regclass
      and polname = 'deny_api_access_admin_private_video_object_migration_references'
  ) then
    execute 'create policy deny_api_access_admin_private_video_object_migration_references '
      'on admin.private_video_object_migration_references '
      'as restrictive for all to public using (false) with check (false)';
  end if;
end;
$policy$;

grant select, insert, update on admin.private_video_object_migration_references to service_role;
grant usage, select on all sequences in schema admin to service_role;

comment on table admin.private_video_object_migration_runs is
  'Operator-only object-level runs that move canonical and legacy DB-owned videos into private R2.';
comment on table admin.private_video_object_migration_items is
  'One independently verified R2 object per union key. Public deletion is gated on every DB reference.';
comment on table admin.private_video_object_migration_references is
  'Reversible old/new ownership ledger for every canonical ML and legacy screenalytics row referencing an object.';

commit;
