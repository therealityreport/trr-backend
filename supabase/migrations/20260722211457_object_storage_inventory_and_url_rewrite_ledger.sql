begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';

create schema if not exists admin;

create table if not exists admin.object_storage_inventory_runs (
  id uuid primary key default gen_random_uuid(),
  provider text not null default 'r2',
  bucket text not null,
  object_prefix text not null default '',
  endpoint_host text,
  scan_generation uuid not null default gen_random_uuid(),
  status text not null default 'pending'
    check (status in ('pending', 'running', 'complete', 'failed')),
  object_count bigint not null default 0 check (object_count >= 0),
  total_bytes bigint not null default 0 check (total_bytes >= 0),
  hashed_count bigint not null default 0 check (hashed_count >= 0),
  reused_count bigint not null default 0 check (reused_count >= 0),
  failed_count bigint not null default 0 check (failed_count >= 0),
  continuation_token text,
  error_message text,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (bucket = btrim(bucket) and bucket <> ''),
  check (object_prefix = btrim(object_prefix)),
  check (completed_at is null or status in ('complete', 'failed'))
);

create table if not exists admin.object_storage_inventory_items (
  id bigint generated always as identity primary key,
  run_id uuid not null references admin.object_storage_inventory_runs(id) on delete cascade,
  scan_generation uuid not null,
  bucket text not null,
  object_key text not null,
  size_bytes bigint not null check (size_bytes >= 0),
  last_modified timestamptz not null,
  etag text not null,
  sha256 text,
  content_type text,
  hash_status text not null default 'pending'
    check (hash_status in ('pending', 'hashed', 'reused', 'failed')),
  reused_from_item_id bigint references admin.object_storage_inventory_items(id) on delete set null,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (run_id, bucket, object_key),
  check (bucket = btrim(bucket) and bucket <> ''),
  check (object_key = btrim(object_key) and object_key <> ''),
  check (etag = btrim(etag) and etag <> ''),
  check (sha256 is null or sha256 ~ '^[0-9a-f]{64}$'),
  check (
    (hash_status in ('hashed', 'reused') and sha256 is not null and error_message is null)
    or (hash_status = 'failed' and error_message is not null)
    or hash_status = 'pending'
  )
);

create table if not exists admin.object_storage_url_rewrite_ledger (
  id bigint generated always as identity primary key,
  rewrite_run_id uuid not null,
  inventory_run_id uuid not null references admin.object_storage_inventory_runs(id) on delete restrict,
  inventory_item_id bigint not null references admin.object_storage_inventory_items(id) on delete restrict,
  batch_number bigint not null check (batch_number >= 0),
  schema_name text not null,
  table_name text not null,
  primary_key_column text not null,
  row_primary_key text not null,
  column_name text not null,
  json_path text[] not null default '{}'::text[],
  trusted_object_key text not null,
  database_sha256 text,
  inventory_sha256 text not null,
  old_value jsonb not null,
  new_value jsonb not null,
  status text not null default 'applied'
    check (status in ('applied', 'rolled_back')),
  applied_at timestamptz not null default now(),
  rolled_back_at timestamptz,
  rollback_reason text,
  unique (
    rewrite_run_id,
    schema_name,
    table_name,
    primary_key_column,
    row_primary_key,
    column_name,
    json_path
  ),
  check (schema_name = btrim(schema_name) and schema_name <> ''),
  check (table_name = btrim(table_name) and table_name <> ''),
  check (primary_key_column = btrim(primary_key_column) and primary_key_column <> ''),
  check (row_primary_key = btrim(row_primary_key) and row_primary_key <> ''),
  check (column_name = btrim(column_name) and column_name <> ''),
  check (trusted_object_key = btrim(trusted_object_key) and trusted_object_key <> ''),
  check (database_sha256 is null or database_sha256 ~ '^[0-9a-f]{64}$'),
  check (inventory_sha256 ~ '^[0-9a-f]{64}$'),
  check (
    (status = 'applied' and rolled_back_at is null)
    or (status = 'rolled_back' and rolled_back_at is not null)
  )
);

create index if not exists object_storage_inventory_runs_bucket_created_idx
  on admin.object_storage_inventory_runs (bucket, created_at desc);

create index if not exists object_storage_inventory_items_run_key_idx
  on admin.object_storage_inventory_items (run_id, object_key);

create index if not exists object_storage_inventory_items_run_generation_idx
  on admin.object_storage_inventory_items (run_id, scan_generation, id);

create index if not exists object_storage_inventory_items_reuse_signature_idx
  on admin.object_storage_inventory_items (bucket, object_key, size_bytes, last_modified, etag)
  where sha256 is not null and hash_status in ('hashed', 'reused');

create index if not exists object_storage_inventory_items_reused_from_idx
  on admin.object_storage_inventory_items (reused_from_item_id)
  where reused_from_item_id is not null;

create index if not exists object_storage_url_rewrite_ledger_inventory_run_idx
  on admin.object_storage_url_rewrite_ledger (inventory_run_id, id);

create index if not exists object_storage_url_rewrite_ledger_inventory_item_idx
  on admin.object_storage_url_rewrite_ledger (inventory_item_id);

create index if not exists object_storage_url_rewrite_ledger_rewrite_run_idx
  on admin.object_storage_url_rewrite_ledger (rewrite_run_id, id);

create index if not exists object_storage_url_rewrite_ledger_active_row_idx
  on admin.object_storage_url_rewrite_ledger (
    schema_name,
    table_name,
    primary_key_column,
    row_primary_key
  )
  where status = 'applied';

alter table admin.object_storage_inventory_runs enable row level security;
alter table admin.object_storage_inventory_items enable row level security;
alter table admin.object_storage_url_rewrite_ledger enable row level security;

do $migration$
declare
  relation_name text;
  policy_name text;
begin
  foreach relation_name in array array[
    'object_storage_inventory_runs',
    'object_storage_inventory_items',
    'object_storage_url_rewrite_ledger'
  ]
  loop
    policy_name := 'deny_api_access_admin_' || relation_name;
    if not exists (
      select 1
      from pg_policy
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
grant select, insert, update, delete on admin.object_storage_inventory_runs to service_role;
grant select, insert, update, delete on admin.object_storage_inventory_items to service_role;
grant select, insert, update, delete on admin.object_storage_url_rewrite_ledger to service_role;
grant usage, select on all sequences in schema admin to service_role;

comment on table admin.object_storage_inventory_runs is
  'Operator-only complete object-storage inventory runs used to gate hosted URL reconciliation.';
comment on table admin.object_storage_inventory_items is
  'Per-object immutable observations; SHA-256 reuse requires an exact bucket/key/size/last-modified/ETag match.';
comment on table admin.object_storage_url_rewrite_ledger is
  'Reversible per-value ledger for trusted hosted URL changes applied against a complete inventory.';

commit;
