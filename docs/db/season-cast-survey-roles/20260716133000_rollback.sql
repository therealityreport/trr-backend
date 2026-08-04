-- Destructive rollback for 20260716133000_admin_season_cast_survey_roles.sql.
--
-- Do not run this against preview or production merely to roll back the API.
-- The table predates the canonical backend promotion and must normally remain
-- in place while the application/backend release is reverted.
--
-- This file is only for a brand-new environment where Gate 4 evidence proves:
--   1. the promotion created the table,
--   2. no earlier app migration or consumer owns it,
--   3. the table is empty, and
--   4. the operator has explicitly approved destructive rollback.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';

do $rollback_guard$
begin
  if exists (select 1 from admin.season_cast_survey_roles limit 1) then
    raise exception using
      errcode = '55000',
      message = 'rollback refused: admin.season_cast_survey_roles is not empty';
  end if;
end;
$rollback_guard$;

drop table admin.season_cast_survey_roles;

-- Intentionally retain admin.set_updated_at(), the admin schema, and trr_app
-- grants because other pre-existing app/backend objects may depend on them.

commit;
