begin;

-- Make casts from timestamp-without-time-zone/text deterministic while
-- repairing an experimental table shape.
set local timezone = 'UTC';

create table if not exists social.instagram_post_payloads (
  post_id uuid primary key references social.instagram_posts(id) on delete cascade,
  raw_data jsonb,
  asset_manifest jsonb not null default '{}'::jsonb,
  child_posts_data jsonb not null default '[]'::jsonb,
  payload_updated_at timestamptz not null default now()
);

create table if not exists social.instagram_account_catalog_post_payloads (
  catalog_post_id uuid primary key references social.instagram_account_catalog_posts(id) on delete cascade,
  raw_data jsonb not null default '{}'::jsonb,
  child_posts_data jsonb not null default '[]'::jsonb,
  payload_updated_at timestamptz not null default now()
);

-- CREATE TABLE IF NOT EXISTS does not repair an earlier experimental table.
-- Add every required column before reconciling types and constraints.
alter table social.instagram_post_payloads add column if not exists post_id uuid;
alter table social.instagram_post_payloads add column if not exists raw_data jsonb;
alter table social.instagram_post_payloads add column if not exists asset_manifest jsonb;
alter table social.instagram_post_payloads add column if not exists child_posts_data jsonb;
alter table social.instagram_post_payloads add column if not exists payload_updated_at timestamptz;

alter table social.instagram_account_catalog_post_payloads add column if not exists catalog_post_id uuid;
alter table social.instagram_account_catalog_post_payloads add column if not exists raw_data jsonb;
alter table social.instagram_account_catalog_post_payloads add column if not exists child_posts_data jsonb;
alter table social.instagram_account_catalog_post_payloads add column if not exists payload_updated_at timestamptz;

-- Type changes can be blocked by an experimental PK/unique/FK on the same
-- column. Drop only those local constraints when a type actually differs;
-- this transaction recreates the canonical PK/FK below, and any later error
-- rolls the drops back. An inbound FK is not silently rewritten.
do $prepare_type_reconciliation$
declare
  item record;
  current_type regtype;
  column_attnum smallint;
  constraint_row record;
  inbound_count bigint;
begin
  for item in
    select *
    from (values
      ('instagram_post_payloads', 'post_id', 'uuid'),
      ('instagram_post_payloads', 'raw_data', 'jsonb'),
      ('instagram_post_payloads', 'asset_manifest', 'jsonb'),
      ('instagram_post_payloads', 'child_posts_data', 'jsonb'),
      ('instagram_post_payloads', 'payload_updated_at', 'timestamp with time zone'),
      ('instagram_account_catalog_post_payloads', 'catalog_post_id', 'uuid'),
      ('instagram_account_catalog_post_payloads', 'raw_data', 'jsonb'),
      ('instagram_account_catalog_post_payloads', 'child_posts_data', 'jsonb'),
      ('instagram_account_catalog_post_payloads', 'payload_updated_at', 'timestamp with time zone')
    ) as columns_to_reconcile(table_name, column_name, target_type)
  loop
    select a.atttypid::regtype, a.attnum
      into current_type, column_attnum
    from pg_attribute a
    join pg_class c on c.oid = a.attrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'social'
      and c.relname = item.table_name
      and a.attname = item.column_name
      and not a.attisdropped;

    if current_type <> item.target_type::regtype then
      select count(*)
        into inbound_count
      from pg_constraint
      where confrelid = format('social.%I', item.table_name)::regclass
        and contype = 'f'
        and column_attnum = any(confkey);
      if inbound_count > 0 then
        raise exception 'Cannot safely change social.%.% type: % inbound foreign key(s) depend on it',
          item.table_name, item.column_name, inbound_count;
      end if;

      for constraint_row in
        select conname
        from pg_constraint
        where conrelid = format('social.%I', item.table_name)::regclass
          and contype in ('p', 'u', 'f')
          and column_attnum = any(conkey)
      loop
        execute format(
          'alter table social.%I drop constraint %I',
          item.table_name,
          constraint_row.conname
        );
      end loop;

      execute format(
        'alter table social.%I alter column %I drop default',
        item.table_name,
        item.column_name
      );
    end if;
  end loop;
end
$prepare_type_reconciliation$;

-- Reconcile required ID, payload, and timestamp types only when every existing
-- value can be cast. Each ALTER is statement-atomic; unsafe data aborts the
-- whole migration instead of being rewritten, deleted, or defaulted away.
do $reconcile_column_types$
declare
  item record;
  current_type regtype;
  using_expression text;
begin
  for item in
    select *
    from (values
      ('instagram_post_payloads', 'post_id', 'uuid'),
      ('instagram_post_payloads', 'raw_data', 'jsonb'),
      ('instagram_post_payloads', 'asset_manifest', 'jsonb'),
      ('instagram_post_payloads', 'child_posts_data', 'jsonb'),
      ('instagram_post_payloads', 'payload_updated_at', 'timestamp with time zone'),
      ('instagram_account_catalog_post_payloads', 'catalog_post_id', 'uuid'),
      ('instagram_account_catalog_post_payloads', 'raw_data', 'jsonb'),
      ('instagram_account_catalog_post_payloads', 'child_posts_data', 'jsonb'),
      ('instagram_account_catalog_post_payloads', 'payload_updated_at', 'timestamp with time zone')
    ) as columns_to_reconcile(table_name, column_name, target_type)
  loop
    select a.atttypid::regtype
      into current_type
    from pg_attribute a
    join pg_class c on c.oid = a.attrelid
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'social'
      and c.relname = item.table_name
      and a.attname = item.column_name
      and not a.attisdropped;

    if current_type <> item.target_type::regtype then
      using_expression := case
        when item.target_type = 'uuid' then format('%I::text::uuid', item.column_name)
        when item.target_type = 'jsonb' then format('%I::text::jsonb', item.column_name)
        when item.target_type = 'timestamp with time zone'
             and current_type = 'timestamp without time zone'::regtype
          then format('%I at time zone ''UTC''', item.column_name)
        when item.target_type = 'timestamp with time zone'
          then format('%I::text::timestamptz', item.column_name)
      end;
      begin
        execute format(
          'alter table social.%I alter column %I type %s using (%s)',
          item.table_name,
          item.column_name,
          item.target_type,
          using_expression
        );
      exception when others then
        raise exception 'Cannot safely cast social.%.% to %: %',
          item.table_name, item.column_name, item.target_type, sqlerrm;
      end;
    end if;
  end loop;
end
$reconcile_column_types$;

-- Apply defaults and NOT NULL only after retaining rows and backfilling nulls.
update social.instagram_post_payloads
set asset_manifest = '{}'::jsonb
where asset_manifest is null;
update social.instagram_post_payloads
set child_posts_data = '[]'::jsonb
where child_posts_data is null;
update social.instagram_post_payloads
set payload_updated_at = now()
where payload_updated_at is null;

alter table social.instagram_post_payloads alter column post_id drop default;
alter table social.instagram_post_payloads alter column raw_data drop default;
alter table social.instagram_post_payloads alter column raw_data drop not null;
alter table social.instagram_post_payloads alter column asset_manifest set default '{}'::jsonb;
alter table social.instagram_post_payloads alter column asset_manifest set not null;
alter table social.instagram_post_payloads alter column child_posts_data set default '[]'::jsonb;
alter table social.instagram_post_payloads alter column child_posts_data set not null;
alter table social.instagram_post_payloads alter column payload_updated_at set default now();
alter table social.instagram_post_payloads alter column payload_updated_at set not null;

update social.instagram_account_catalog_post_payloads
set raw_data = '{}'::jsonb
where raw_data is null;
update social.instagram_account_catalog_post_payloads
set child_posts_data = '[]'::jsonb
where child_posts_data is null;
update social.instagram_account_catalog_post_payloads
set payload_updated_at = now()
where payload_updated_at is null;

alter table social.instagram_account_catalog_post_payloads alter column catalog_post_id drop default;
alter table social.instagram_account_catalog_post_payloads alter column raw_data set default '{}'::jsonb;
alter table social.instagram_account_catalog_post_payloads alter column raw_data set not null;
alter table social.instagram_account_catalog_post_payloads alter column child_posts_data set default '[]'::jsonb;
alter table social.instagram_account_catalog_post_payloads alter column child_posts_data set not null;
alter table social.instagram_account_catalog_post_payloads alter column payload_updated_at set default now();
alter table social.instagram_account_catalog_post_payloads alter column payload_updated_at set not null;

-- A partial table cannot be repaired safely if its key is null, duplicated, or
-- points at no parent row. Abort explicitly; never delete or invent ownership
-- for an experimental row.
do $validate_keys$
declare
  item record;
  violation_count bigint;
begin
  for item in
    select *
    from (values
      ('instagram_post_payloads', 'post_id', 'instagram_posts'),
      ('instagram_account_catalog_post_payloads', 'catalog_post_id', 'instagram_account_catalog_posts')
    ) as targets(table_name, key_column, parent_table)
  loop
    execute format(
      'select count(*) from social.%I where %I is null',
      item.table_name,
      item.key_column
    ) into violation_count;
    if violation_count > 0 then
      raise exception 'Cannot safely constrain social.%.%: % row(s) have null keys',
        item.table_name, item.key_column, violation_count;
    end if;

    execute format(
      'select count(*) from (select %I from social.%I group by %I having count(*) > 1) duplicates',
      item.key_column,
      item.table_name,
      item.key_column
    ) into violation_count;
    if violation_count > 0 then
      raise exception 'Cannot safely constrain social.%.%: % duplicate key value(s)',
        item.table_name, item.key_column, violation_count;
    end if;

    execute format(
      'select count(*) from social.%I s left join social.%I p on p.id = s.%I where p.id is null',
      item.table_name,
      item.parent_table,
      item.key_column
    ) into violation_count;
    if violation_count > 0 then
      raise exception 'Cannot safely constrain social.%.%: % orphan row(s)',
        item.table_name, item.key_column, violation_count;
    end if;

    execute format(
      'alter table social.%I alter column %I set not null',
      item.table_name,
      item.key_column
    );
  end loop;
end
$validate_keys$;

-- Keep an already-correct key. Drop only a conflicting/incorrect key, then add
-- the canonical one-to-one PK and cascading FK if it is absent.
do $reconcile_constraints$
declare
  item record;
  constraint_row record;
  key_attnum smallint;
  parent_attnum smallint;
  has_correct boolean;
begin
  for item in
    select *
    from (values
      ('instagram_post_payloads', 'post_id', 'instagram_posts',
       'instagram_post_payloads_pkey', 'instagram_post_payloads_post_id_fkey'),
      ('instagram_account_catalog_post_payloads', 'catalog_post_id', 'instagram_account_catalog_posts',
       'instagram_account_catalog_post_payloads_pkey',
       'instagram_account_catalog_post_payloads_catalog_post_id_fkey')
    ) as targets(table_name, key_column, parent_table, pk_name, fk_name)
  loop
    select attnum into key_attnum
    from pg_attribute
    where attrelid = format('social.%I', item.table_name)::regclass
      and attname = item.key_column and not attisdropped;
    select attnum into parent_attnum
    from pg_attribute
    where attrelid = format('social.%I', item.parent_table)::regclass
      and attname = 'id' and not attisdropped;

    has_correct := false;
    for constraint_row in
      select oid, conname, conkey
      from pg_constraint
      where conrelid = format('social.%I', item.table_name)::regclass
        and (contype = 'p' or conname = item.pk_name)
    loop
      if constraint_row.conkey = array[key_attnum]::smallint[] then
        has_correct := true;
      else
        execute format(
          'alter table social.%I drop constraint %I',
          item.table_name,
          constraint_row.conname
        );
      end if;
    end loop;
    if not has_correct then
      execute format(
        'alter table social.%I add constraint %I primary key (%I)',
        item.table_name,
        item.pk_name,
        item.key_column
      );
    end if;

    has_correct := false;
    for constraint_row in
      select conname, conkey, confkey, confrelid, confdeltype
      from pg_constraint
      where conrelid = format('social.%I', item.table_name)::regclass
        and ((contype = 'f' and key_attnum = any(conkey)) or conname = item.fk_name)
    loop
      if constraint_row.conkey = array[key_attnum]::smallint[]
         and constraint_row.confkey = array[parent_attnum]::smallint[]
         and constraint_row.confrelid = format('social.%I', item.parent_table)::regclass
         and constraint_row.confdeltype = 'c' then
        has_correct := true;
      else
        execute format(
          'alter table social.%I drop constraint %I',
          item.table_name,
          constraint_row.conname
        );
      end if;
    end loop;
    if not has_correct then
      execute format(
        'alter table social.%I add constraint %I foreign key (%I) references social.%I(id) on delete cascade',
        item.table_name,
        item.fk_name,
        item.key_column,
        item.parent_table
      );
    end if;
  end loop;
end
$reconcile_constraints$;

-- Reset any policy left by a partial/experimental application. These tables
-- intentionally expose no end-user RLS policy; service_role bypasses RLS.
do $reset_policies$
declare
  policy_row record;
begin
  for policy_row in
    select schemaname, tablename, policyname
    from pg_policies
    where schemaname = 'social'
      and tablename in (
        'instagram_post_payloads',
        'instagram_account_catalog_post_payloads'
      )
  loop
    execute format(
      'drop policy if exists %I on %I.%I',
      policy_row.policyname,
      policy_row.schemaname,
      policy_row.tablename
    );
  end loop;
end
$reset_policies$;

alter table social.instagram_post_payloads enable row level security;
alter table social.instagram_account_catalog_post_payloads enable row level security;

revoke all on table
  social.instagram_post_payloads,
  social.instagram_account_catalog_post_payloads
from public, anon, authenticated;

grant all privileges on table
  social.instagram_post_payloads,
  social.instagram_account_catalog_post_payloads
to service_role;

commit;
