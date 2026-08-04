begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';

-- Canonical backend-owned promotion of the skipped TRR-APP migration
-- 022_create_admin_season_cast_survey_roles.sql. Keep the original composite
-- show/season key: survey roles may be authored before core.seasons exists.
create schema if not exists admin;

do $migration$
begin
  if to_regprocedure('admin.set_updated_at()') is null then
    execute $ddl$
      create function admin.set_updated_at()
      returns trigger
      language plpgsql
      set search_path = admin, pg_temp
      as $function$
      begin
        new.updated_at = now();
        return new;
      end;
      $function$
    $ddl$;
  end if;
end;
$migration$;

-- Preserve the later backend hardening contract even on a database where this
-- promotion creates the helper for the first time.
alter function admin.set_updated_at()
  set search_path = admin, pg_temp;

create table if not exists admin.season_cast_survey_roles (
  id uuid primary key default gen_random_uuid(),
  trr_show_id uuid not null,
  season_number integer not null,
  person_id uuid not null,
  role text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint season_cast_survey_roles_season_number_check
    check (season_number > 0),
  constraint season_cast_survey_roles_role_check
    check (role in ('main', 'friend_of')),
  constraint season_cast_survey_roles_trr_show_id_season_number_person_id_key
    unique (trr_show_id, season_number, person_id)
);

create index if not exists idx_season_cast_survey_roles_show_season
  on admin.season_cast_survey_roles (trr_show_id, season_number);

create index if not exists idx_season_cast_survey_roles_person
  on admin.season_cast_survey_roles (person_id);

do $migration$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgrelid = 'admin.season_cast_survey_roles'::regclass
      and tgname = 'set_season_cast_survey_roles_updated_at'
      and not tgisinternal
  ) then
    execute $ddl$
      create trigger set_season_cast_survey_roles_updated_at
        before update on admin.season_cast_survey_roles
        for each row execute function admin.set_updated_at()
    $ddl$;
  end if;
end;
$migration$;

-- Reconcile the backend's 20260417 RLS hardening and 20260511 explicit
-- default-deny posture for databases where this table is created later.
alter table admin.season_cast_survey_roles enable row level security;

do $migration$
begin
  if not exists (
    select 1
    from pg_policy
    where polrelid = 'admin.season_cast_survey_roles'::regclass
      and polname = 'deny_api_access_admin_season_cast_survey_roles_67106756'
  ) then
    execute $ddl$
      create policy deny_api_access_admin_season_cast_survey_roles_67106756
        on admin.season_cast_survey_roles
        as restrictive
        for all
        to public
        using (false)
        with check (false)
    $ddl$;
  end if;
end;
$migration$;

comment on policy deny_api_access_admin_season_cast_survey_roles_67106756
  on admin.season_cast_survey_roles is
  'Documents intentional API default-deny for an RLS-enabled app table; service_role/direct owner access is unchanged.';

-- The legacy app runtime uses trr_app when that optional role is installed.
-- Backend owner/service-role connections keep their existing access model.
do $migration$
begin
  if to_regrole('trr_app') is not null then
    execute 'grant usage on schema admin to trr_app';
    execute 'grant select, insert, update, delete on admin.season_cast_survey_roles to trr_app';
  end if;
end;
$migration$;

comment on table admin.season_cast_survey_roles is
  'Backend-owned survey eligibility roles keyed by TRR show, season number, and person.';

commit;
