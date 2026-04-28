\set ON_ERROR_STOP on
\pset pager off

-- Verifies the safety hotfix plus Phase 1 RLS performance cleanup after the
-- migrations have been applied. This file is safe to run inside a transaction
-- after including the migrations and before ROLLBACK for dry-run validation.

do $$
declare
  rec record;
  privilege_name text;
  ledger_exists boolean;
  expected_count int;
  actual_count int;
begin
  select to_regclass('public.__migrations') is not null into ledger_exists;

  if ledger_exists then
    select count(*) into actual_count
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relname = '__migrations'
      and c.relrowsecurity = true;

    if actual_count <> 1 then
      raise exception 'public.__migrations must have RLS enabled when present';
    end if;

    select count(*) into actual_count
    from information_schema.role_table_grants
    where table_schema = 'public'
      and table_name = '__migrations'
      and grantee in ('anon', 'authenticated', 'PUBLIC', 'public');

    if actual_count <> 0 then
      raise exception 'public.__migrations still has API/public grants';
    end if;

    select count(*) into actual_count
    from pg_policies
    where schemaname = 'public'
      and tablename = '__migrations'
      and policyname = '__migrations_no_api_access'
      and permissive = 'RESTRICTIVE'
      and cmd = 'ALL'
      and roles::text = '{public}'
      and qual like '%false%'
      and with_check like '%false%';

    if actual_count <> 1 then
      raise exception 'public.__migrations deny policy is missing or weakened';
    end if;
  end if;

  for rec in
    select
      n.nspname as schema_name,
      p.proname as function_name,
      pg_get_function_identity_arguments(p.oid) as args,
      has_function_privilege('anon', p.oid, 'EXECUTE') as anon_execute,
      has_function_privilege('authenticated', p.oid, 'EXECUTE') as authenticated_execute,
      has_function_privilege('public', p.oid, 'EXECUTE') as public_execute,
      has_function_privilege('service_role', p.oid, 'EXECUTE') as service_role_execute
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where p.prosecdef
      and (
        (n.nspname = 'core' and p.proname in (
          'merge_shows',
          'set_primary_media_link',
          'upsert_cast_photos_by_canonical',
          'upsert_cast_photos_by_identity',
          'upsert_person_images',
          'upsert_show_images_by_identity',
          'upsert_tmdb_show_images_by_identity'
        ))
        or (n.nspname = 'social' and p.proname = 'get_or_create_direct_conversation')
      )
  loop
    if rec.anon_execute or rec.authenticated_execute or rec.public_execute then
      raise exception '%.%(%) remains executable by anon/authenticated/public', rec.schema_name, rec.function_name, rec.args;
    end if;
    if not rec.service_role_execute then
      raise exception '%.%(%) is not executable by service_role', rec.schema_name, rec.function_name, rec.args;
    end if;
  end loop;

  for rec in
    select *
    from (values
      ('core.networks','service_role',array['SELECT','INSERT','UPDATE','DELETE']),
      ('core.production_companies','service_role',array['SELECT','INSERT','UPDATE','DELETE']),
      ('core.show_watch_providers','service_role',array['SELECT','INSERT','UPDATE','DELETE']),
      ('core.watch_providers','service_role',array['SELECT','INSERT','UPDATE','DELETE']),
      ('public.show_icons','service_role',array['SELECT','INSERT','UPDATE','DELETE']),
      ('public.flashback_quizzes','service_role',array['SELECT','INSERT','UPDATE','DELETE']),
      ('public.flashback_events','service_role',array['SELECT','INSERT','UPDATE','DELETE'])
    ) as expected(table_name, role_name, privileges)
  loop
    foreach privilege_name in array rec.privileges loop
      if not has_table_privilege(rec.role_name, rec.table_name, privilege_name) then
        raise exception 'Role % lacks % privilege on %', rec.role_name, privilege_name, rec.table_name;
      end if;
    end loop;
  end loop;

  select count(*) into expected_count
  from (values
    ('core','networks','core_networks_service_role_insert','INSERT','{service_role}'),
    ('core','networks','core_networks_service_role_update','UPDATE','{service_role}'),
    ('core','networks','core_networks_service_role_delete','DELETE','{service_role}'),
    ('core','production_companies','core_production_companies_service_role_insert','INSERT','{service_role}'),
    ('core','production_companies','core_production_companies_service_role_update','UPDATE','{service_role}'),
    ('core','production_companies','core_production_companies_service_role_delete','DELETE','{service_role}'),
    ('core','show_watch_providers','core_show_watch_providers_service_role_insert','INSERT','{service_role}'),
    ('core','show_watch_providers','core_show_watch_providers_service_role_update','UPDATE','{service_role}'),
    ('core','show_watch_providers','core_show_watch_providers_service_role_delete','DELETE','{service_role}'),
    ('core','watch_providers','core_watch_providers_service_role_insert','INSERT','{service_role}'),
    ('core','watch_providers','core_watch_providers_service_role_update','UPDATE','{service_role}'),
    ('core','watch_providers','core_watch_providers_service_role_delete','DELETE','{service_role}'),
    ('public','show_icons','show_icons_service_role_insert','INSERT','{service_role}'),
    ('public','show_icons','show_icons_service_role_update','UPDATE','{service_role}'),
    ('public','show_icons','show_icons_service_role_delete','DELETE','{service_role}'),
    ('public','flashback_quizzes','flashback_quizzes_service_role_insert','INSERT','{service_role}'),
    ('public','flashback_quizzes','flashback_quizzes_service_role_update','UPDATE','{service_role}'),
    ('public','flashback_quizzes','flashback_quizzes_service_role_delete','DELETE','{service_role}'),
    ('public','flashback_events','flashback_events_service_role_insert','INSERT','{service_role}'),
    ('public','flashback_events','flashback_events_service_role_update','UPDATE','{service_role}'),
    ('public','flashback_events','flashback_events_service_role_delete','DELETE','{service_role}')
  ) as expected(schemaname, tablename, policyname, cmd, roles_text);

  select count(*) into actual_count
  from (values
    ('core','networks','core_networks_service_role_insert','INSERT','{service_role}'),
    ('core','networks','core_networks_service_role_update','UPDATE','{service_role}'),
    ('core','networks','core_networks_service_role_delete','DELETE','{service_role}'),
    ('core','production_companies','core_production_companies_service_role_insert','INSERT','{service_role}'),
    ('core','production_companies','core_production_companies_service_role_update','UPDATE','{service_role}'),
    ('core','production_companies','core_production_companies_service_role_delete','DELETE','{service_role}'),
    ('core','show_watch_providers','core_show_watch_providers_service_role_insert','INSERT','{service_role}'),
    ('core','show_watch_providers','core_show_watch_providers_service_role_update','UPDATE','{service_role}'),
    ('core','show_watch_providers','core_show_watch_providers_service_role_delete','DELETE','{service_role}'),
    ('core','watch_providers','core_watch_providers_service_role_insert','INSERT','{service_role}'),
    ('core','watch_providers','core_watch_providers_service_role_update','UPDATE','{service_role}'),
    ('core','watch_providers','core_watch_providers_service_role_delete','DELETE','{service_role}'),
    ('public','show_icons','show_icons_service_role_insert','INSERT','{service_role}'),
    ('public','show_icons','show_icons_service_role_update','UPDATE','{service_role}'),
    ('public','show_icons','show_icons_service_role_delete','DELETE','{service_role}'),
    ('public','flashback_quizzes','flashback_quizzes_service_role_insert','INSERT','{service_role}'),
    ('public','flashback_quizzes','flashback_quizzes_service_role_update','UPDATE','{service_role}'),
    ('public','flashback_quizzes','flashback_quizzes_service_role_delete','DELETE','{service_role}'),
    ('public','flashback_events','flashback_events_service_role_insert','INSERT','{service_role}'),
    ('public','flashback_events','flashback_events_service_role_update','UPDATE','{service_role}'),
    ('public','flashback_events','flashback_events_service_role_delete','DELETE','{service_role}')
  ) as expected(schemaname, tablename, policyname, cmd, roles_text)
  join pg_policies p
    on p.schemaname = expected.schemaname
   and p.tablename = expected.tablename
   and p.policyname = expected.policyname
   and p.cmd = expected.cmd
   and p.roles::text = expected.roles_text;

  if actual_count <> expected_count then
    raise exception 'Expected % target policies, found %', expected_count, actual_count;
  end if;

  select count(*) into actual_count
  from pg_policies
  where (schemaname, tablename) in (
    ('core','networks'),
    ('core','production_companies'),
    ('core','show_watch_providers'),
    ('core','watch_providers'),
    ('public','show_icons'),
    ('public','flashback_quizzes'),
    ('public','flashback_events')
  )
    and cmd = 'INSERT'
    and roles::text = '{service_role}'
    and qual is null
    and with_check like '%auth.role%'
    and with_check like '%service_role%';

  if actual_count <> 7 then
    raise exception 'Expected 7 service_role INSERT policies with WITH CHECK only, found %', actual_count;
  end if;

  select count(*) into actual_count
  from pg_policies
  where (schemaname, tablename) in (
    ('core','networks'),
    ('core','production_companies'),
    ('core','show_watch_providers'),
    ('core','watch_providers'),
    ('public','show_icons'),
    ('public','flashback_quizzes'),
    ('public','flashback_events')
  )
    and cmd = 'UPDATE'
    and roles::text = '{service_role}'
    and qual like '%auth.role%'
    and qual like '%service_role%'
    and with_check like '%auth.role%'
    and with_check like '%service_role%';

  if actual_count <> 7 then
    raise exception 'Expected 7 service_role UPDATE policies with USING and WITH CHECK, found %', actual_count;
  end if;

  select count(*) into actual_count
  from pg_policies
  where (schemaname, tablename) in (
    ('core','networks'),
    ('core','production_companies'),
    ('core','show_watch_providers'),
    ('core','watch_providers'),
    ('public','show_icons'),
    ('public','flashback_quizzes'),
    ('public','flashback_events')
  )
    and cmd = 'DELETE'
    and roles::text = '{service_role}'
    and qual like '%auth.role%'
    and qual like '%service_role%'
    and with_check is null;

  if actual_count <> 7 then
    raise exception 'Expected 7 service_role DELETE policies with USING only, found %', actual_count;
  end if;

  select count(*) into actual_count
  from pg_policies
  where (schemaname, tablename) in (
    ('core','networks'),
    ('core','production_companies'),
    ('core','show_watch_providers'),
    ('core','watch_providers'),
    ('public','show_icons'),
    ('public','flashback_quizzes'),
    ('public','flashback_events'),
    ('firebase_surveys','responses'),
    ('firebase_surveys','answers')
  )
    and cmd in ('INSERT', 'UPDATE', 'DELETE', 'ALL')
    and roles && array['public'::name, 'anon'::name, 'authenticated'::name];

  if actual_count <> 0 then
    raise exception 'Found % unsafe broad write policies on Phase 1 targets', actual_count;
  end if;

  select count(*) into actual_count
  from information_schema.role_table_grants
  where table_schema = 'core'
    and table_name in ('networks', 'production_companies', 'show_watch_providers', 'watch_providers')
    and grantee in ('anon', 'authenticated', 'PUBLIC', 'public')
    and privilege_type in ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER');

  if actual_count <> 0 then
    raise exception 'Found % unsafe broad core table grants', actual_count;
  end if;

  select count(*) into actual_count
  from information_schema.role_table_grants
  where table_schema = 'firebase_surveys'
    and table_name in ('responses', 'answers')
    and grantee in ('anon', 'authenticated', 'service_role', 'PUBLIC', 'public')
    and privilege_type in ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER');

  if actual_count <> 0 then
    raise exception 'Found % unsafe firebase_surveys table grants', actual_count;
  end if;

  select count(*) into actual_count
  from pg_policies
  where (schemaname, tablename, policyname) in (
    ('core','networks','core_tmdb_networks_service_role'),
    ('core','production_companies','core_tmdb_production_companies_service_role'),
    ('core','show_watch_providers','core_show_watch_providers_service_role'),
    ('core','watch_providers','core_tmdb_watch_providers_service_role'),
    ('public','show_icons','Allow service role all on show_icons'),
    ('public','flashback_quizzes','Service role full access to quizzes'),
    ('public','flashback_events','Service role full access to events'),
    ('firebase_surveys','responses','responses_admin_all'),
    ('firebase_surveys','responses','responses_select_own'),
    ('firebase_surveys','responses','responses_insert_own'),
    ('firebase_surveys','responses','responses_update_own'),
    ('firebase_surveys','answers','answers_admin_all'),
    ('firebase_surveys','answers','answers_select_own'),
    ('firebase_surveys','answers','answers_insert_own'),
    ('firebase_surveys','answers','answers_update_own'),
    ('firebase_surveys','responses','responses_select_access'),
    ('firebase_surveys','responses','responses_insert_access'),
    ('firebase_surveys','responses','responses_update_access'),
    ('firebase_surveys','responses','responses_delete_access'),
    ('firebase_surveys','answers','answers_select_access'),
    ('firebase_surveys','answers','answers_insert_access'),
    ('firebase_surveys','answers','answers_update_access'),
    ('firebase_surveys','answers','answers_delete_access')
  );

  if actual_count <> 0 then
    raise exception 'Found % retired Phase 0 policies still present', actual_count;
  end if;

  select count(*) into actual_count
  from (
    select distinct schemaname, tablename
    from pg_policies
    where (schemaname, tablename) in (
      ('core','networks'),
      ('core','production_companies'),
      ('core','show_watch_providers'),
      ('core','watch_providers'),
      ('public','show_icons'),
      ('public','flashback_quizzes'),
      ('public','flashback_events')
    )
    and cmd = 'SELECT'
    and roles::text = '{public}'
  ) public_reads;

  if actual_count <> 7 then
    raise exception 'Expected 7 public SELECT policies to remain, found %', actual_count;
  end if;

  select count(*) into actual_count
  from pg_policies
  where schemaname = 'firebase_surveys'
    and tablename in ('responses', 'answers')
    and cmd in ('ALL', 'SELECT', 'INSERT', 'UPDATE', 'DELETE');

  if actual_count <> 0 then
    raise exception 'Legacy firebase_surveys app RLS policies must be disabled, found % remaining policies', actual_count;
  end if;

  if to_regprocedure('surveys.submit_response(uuid, jsonb)') is null then
    raise exception 'Supabase-auth surveys.submit_response(uuid, jsonb) is required before disabling legacy firebase_surveys collection';
  end if;
end;
$$;

select
  schemaname,
  tablename,
  policyname,
  roles,
  cmd,
  qual,
  with_check
from pg_policies
where (schemaname, tablename) in (
  ('core','networks'),
  ('core','production_companies'),
  ('core','show_watch_providers'),
  ('core','watch_providers'),
  ('public','show_icons'),
  ('public','flashback_quizzes'),
  ('public','flashback_events')
)
order by schemaname, tablename, cmd, policyname;
