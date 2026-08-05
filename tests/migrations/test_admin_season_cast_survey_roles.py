from __future__ import annotations

import hashlib
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = REPO_ROOT / "supabase/migrations/20260805140519_admin_season_cast_survey_roles_20260805.sql"
ROLLBACK = REPO_ROOT / "docs/db/season-cast-survey-roles/20260805140519_rollback.sql"
VERIFY = REPO_ROOT / "docs/db/season-cast-survey-roles/20260805140519_verify.sql"
ROLLOUT = REPO_ROOT / "docs/db/season-cast-survey-roles/20260805140519_rollout.md"
APP_BACKLOG = REPO_ROOT.parent / "TRR-APP/apps/web/db/migrations/022_create_admin_season_cast_survey_roles.sql"
LATER_RLS = REPO_ROOT / "supabase/migrations/20260417130000_supabase_security_advisor_hardening.sql"
LATER_FUNCTION_HARDENING = (
    REPO_ROOT / "supabase/migrations/20260629143025_supabase_security_advisor_rpc_and_vector_hardening.sql"
)
REPOSITORY = REPO_ROOT / "trr_backend/repositories/season_cast_survey_roles.py"


def _normalized(path: Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8").lower()).strip()


def test_backend_migration_faithfully_promotes_the_app_backlog_contract() -> None:
    sql = _normalized(MIGRATION)
    # GitHub checks out the backend as a standalone repository, while local
    # workspace validation may also have the app checkout available beside it.
    # The explicit fragments below are the promoted contract in both layouts;
    # compare the app backlog too whenever that optional sibling is present.
    contract_sqls = {"backend": sql}
    if APP_BACKLOG.is_file():
        contract_sqls["app_backlog"] = _normalized(APP_BACKLOG)

    for fragment in (
        "create table if not exists admin.season_cast_survey_roles",
        "id uuid primary key default gen_random_uuid()",
        "trr_show_id uuid not null",
        "season_number integer not null",
        "person_id uuid not null",
        "role text not null",
        "created_at timestamptz not null default now()",
        "updated_at timestamptz not null default now()",
        "check (season_number > 0)",
        "check (role in ('main', 'friend_of'))",
        "unique (trr_show_id, season_number, person_id)",
        "idx_season_cast_survey_roles_show_season",
        "idx_season_cast_survey_roles_person",
        "set_season_cast_survey_roles_updated_at",
    ):
        for source, contract_sql in contract_sqls.items():
            assert fragment in contract_sql, f"{fragment!r} missing from {source} contract"

    assert "references core.seasons" not in sql
    assert "references core.shows" not in sql
    assert "references core.people" not in sql


def test_migration_reconciles_later_security_contracts_without_broadening_api_access() -> None:
    sql = _normalized(MIGRATION)
    policy_name = (
        "deny_api_access_admin_season_cast_survey_roles_"
        + hashlib.md5(b"admin.season_cast_survey_roles").hexdigest()[:8]
    )

    assert "alter table admin.season_cast_survey_roles enable row level security" in sql
    assert policy_name in sql
    assert "as restrictive for all to public using (false) with check (false)" in sql
    assert "grant usage on schema admin to trr_app" in sql
    assert "grant select, insert, update, delete on admin.season_cast_survey_roles to trr_app" in sql
    assert "if to_regrole('trr_app') is not null" in sql
    assert "set search_path = admin, pg_temp" in sql
    assert "alter table admin.season_cast_survey_roles enable row level security" in _normalized(LATER_RLS)
    assert "alter function admin.set_updated_at() set search_path = admin, pg_temp" in _normalized(
        LATER_FUNCTION_HARDENING
    )
    assert "to anon" not in sql
    assert "to authenticated" not in sql


def test_migration_is_bounded_idempotent_and_expansion_only() -> None:
    sql = _normalized(MIGRATION)

    assert sql.startswith("begin;")
    assert "set local lock_timeout = '5s'" in sql
    assert "set local statement_timeout = '60s'" in sql
    assert "create schema if not exists admin" in sql
    assert "if to_regprocedure('admin.set_updated_at()') is null" in sql
    assert sql.count("create index if not exists") == 2
    assert "if not exists ( select 1 from pg_trigger" in sql
    assert "if not exists ( select 1 from pg_policy" in sql
    assert "drop table" not in sql
    assert "drop trigger" not in sql
    assert "alter table admin.season_cast_survey_roles add" not in sql
    assert "delete from" not in sql
    assert "update admin.season_cast_survey_roles" not in sql
    assert "truncate" not in sql
    assert sql.endswith("commit;")


def test_repository_contract_matches_promoted_table_columns_and_conflict_key() -> None:
    sql = _normalized(MIGRATION)
    repository = _normalized(REPOSITORY)

    for column in (
        "id",
        "trr_show_id",
        "season_number",
        "person_id",
        "role",
        "created_at",
        "updated_at",
    ):
        assert re.search(rf"\b{column}\b", sql)
        assert re.search(rf"\b{column}\b", repository)
    assert "on conflict (trr_show_id, season_number, person_id)" in repository
    assert "admin.season_cast_survey_roles" in repository


def test_rollback_is_guarded_and_rollout_names_required_gate_4_proof() -> None:
    rollback = _normalized(ROLLBACK)
    verify = _normalized(VERIFY)
    rollout = ROLLOUT.read_text(encoding="utf-8").lower()

    assert "if exists (select 1 from admin.season_cast_survey_roles limit 1)" in rollback
    assert "rollback refused" in rollback
    assert "drop table admin.season_cast_survey_roles" in rollback
    assert "drop function admin.set_updated_at" not in rollback
    assert "set transaction read only" in verify
    assert "information_schema.columns" in verify
    assert "pg_constraint" in verify
    assert "pg_indexes" in verify
    assert "information_schema.triggers" in verify
    assert "pg_policy" in verify
    assert "information_schema.role_table_grants" in verify
    assert "invalid_season_number_count" in verify
    assert "invalid_role_count" in verify
    assert "null_required_field_count" in verify
    assert "duplicate_identity_group_count" in verify
    assert "rollback;" in verify
    assert "pending migration set" in rollout
    assert "sha-256" in rollout
    assert "information_schema.columns" in rollout
    assert "duplicate" in rollout
    assert "preview apply" in rollout
    assert "explicit named approval" in rollout
