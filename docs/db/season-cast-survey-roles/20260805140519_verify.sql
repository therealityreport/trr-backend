-- Read-only pre/post-apply verification for the canonical DDL promotion.
-- Returns aggregate/catalog evidence only; no role rows or values are emitted.

begin;
set transaction read only;
set local statement_timeout = '30s';

select
  ordinal_position,
  column_name,
  data_type,
  udt_name,
  is_nullable,
  column_default
from information_schema.columns
where table_schema = 'admin'
  and table_name = 'season_cast_survey_roles'
order by ordinal_position;

select
  conname,
  contype,
  pg_get_constraintdef(oid, true) as definition
from pg_constraint
where conrelid = 'admin.season_cast_survey_roles'::regclass
order by conname;

select indexname, indexdef
from pg_indexes
where schemaname = 'admin'
  and tablename = 'season_cast_survey_roles'
order by indexname;

select
  trigger_name,
  event_manipulation,
  action_timing,
  action_statement
from information_schema.triggers
where event_object_schema = 'admin'
  and event_object_table = 'season_cast_survey_roles'
order by trigger_name, event_manipulation;

select
  c.relrowsecurity,
  c.relforcerowsecurity
from pg_class c
where c.oid = 'admin.season_cast_survey_roles'::regclass;

select
  polname,
  polpermissive,
  array(
    select case when role_oid = 0 then 'public' else role_oid::regrole::text end
    from unnest(polroles) role_oid
    order by role_oid
  ) as roles,
  pg_get_expr(polqual, polrelid) as using_expression,
  pg_get_expr(polwithcheck, polrelid) as check_expression
from pg_policy
where polrelid = 'admin.season_cast_survey_roles'::regclass
order by polname;

select
  grantee,
  privilege_type,
  is_grantable
from information_schema.role_table_grants
where table_schema = 'admin'
  and table_name = 'season_cast_survey_roles'
order by grantee, privilege_type;

select
  p.oid::regprocedure::text as function_name,
  p.prosecdef as security_definer,
  p.proconfig,
  pg_get_functiondef(p.oid) as definition
from pg_proc p
where p.oid = to_regprocedure('admin.set_updated_at()');

select
  count(*) as row_count,
  count(*) filter (where season_number <= 0) as invalid_season_number_count,
  count(*) filter (where role not in ('main', 'friend_of')) as invalid_role_count,
  count(*) filter (
    where id is null
       or trr_show_id is null
       or season_number is null
       or person_id is null
       or role is null
       or created_at is null
       or updated_at is null
  ) as null_required_field_count
from admin.season_cast_survey_roles;

select count(*) as duplicate_identity_group_count
from (
  select trr_show_id, season_number, person_id
  from admin.season_cast_survey_roles
  group by trr_show_id, season_number, person_id
  having count(*) > 1
) duplicate_groups;

rollback;
