-- Supabase security advisor follow-up for app-owned objects.
--
-- This migration intentionally avoids access-expanding policy decisions. Tables
-- that already had RLS enabled with no policies were already default-deny for
-- API roles; adding restrictive false policies records that intent and clears
-- the no-policy advisor without granting access. Managed Supabase schemas
-- (auth, storage, realtime) are excluded.
--
-- Function changes are limited to app-owned, non-extension functions that have
-- no fixed search_path. Public SECURITY DEFINER survey submission and the
-- public vector extension remain documented exceptions for explicit review.

begin;

do $$
declare
  target record;
  policy_name text;
begin
  for target in
    select n.nspname as schema_name, c.relname as table_name
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where c.relkind in ('r', 'p')
      and c.relrowsecurity
      and n.nspname in ('admin', 'core', 'firebase_surveys', 'public', 'social')
      and not exists (
        select 1
        from pg_policy pol
        where pol.polrelid = c.oid
      )
    order by n.nspname, c.relname
  loop
    policy_name := left(
      'deny_api_access_' || target.schema_name || '_' || target.table_name,
      50
    ) || '_' || substr(md5(target.schema_name || '.' || target.table_name), 1, 8);

    execute format(
      'drop policy if exists %I on %I.%I',
      policy_name,
      target.schema_name,
      target.table_name
    );
    execute format(
      'create policy %I on %I.%I as restrictive for all to public using (false) with check (false)',
      policy_name,
      target.schema_name,
      target.table_name
    );
    execute format(
      'comment on policy %I on %I.%I is %L',
      policy_name,
      target.schema_name,
      target.table_name,
      'Documents intentional API default-deny for an RLS-enabled app table; service_role/direct owner access is unchanged.'
    );
  end loop;
end;
$$;

do $$
declare
  target record;
begin
  for target in
    select
      n.nspname as schema_name,
      p.proname as function_name,
      pg_get_function_identity_arguments(p.oid) as identity_args
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where p.prokind = 'f'
      and n.nspname in ('core', 'public', 'social')
      and not exists (
        select 1
        from unnest(coalesce(p.proconfig, array[]::text[])) cfg
        where cfg like 'search_path=%'
      )
      and not exists (
        select 1
        from pg_depend d
        where d.classid = 'pg_proc'::regclass
          and d.objid = p.oid
          and d.deptype = 'e'
      )
    order by n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)
  loop
    if target.schema_name = 'core' then
      execute format(
        'alter function %I.%I(%s) set search_path = core, public, pg_temp',
        target.schema_name,
        target.function_name,
        target.identity_args
      );
    elsif target.schema_name = 'social' then
      execute format(
        'alter function %I.%I(%s) set search_path = social, core, public, pg_temp',
        target.schema_name,
        target.function_name,
        target.identity_args
      );
    elsif target.schema_name = 'public' then
      execute format(
        'alter function %I.%I(%s) set search_path = public, pg_temp',
        target.schema_name,
        target.function_name,
        target.identity_args
      );
    end if;
  end loop;
end;
$$;

commit;
