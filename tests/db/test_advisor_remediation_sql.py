from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SECURITY_MIGRATION = REPO_ROOT / "supabase/migrations/20260428110000_security_hotfix_public_migrations_rpc_exec.sql"
RLS_MIGRATION = REPO_ROOT / "supabase/migrations/20260428111000_advisor_rls_policy_cleanup.sql"
EXTERNAL_ID_CONFLICTS_PK_MIGRATION = (
    REPO_ROOT / "supabase/migrations/20260428112000_advisor_external_id_conflicts_primary_key.sql"
)
FLASHBACK_GAMEPLAY_REMOVAL_MIGRATION = (
    REPO_ROOT / "supabase/migrations/20260428113000_remove_flashback_gameplay_write_path.sql"
)
EXTERNAL_ID_BACKFILL_MIGRATION = REPO_ROOT / "supabase/migrations/0073_backfill_external_ids.sql"
ROLLBACK_SQL = REPO_ROOT / "docs/db/advisor-performance/20260428111000_advisor_rls_policy_cleanup_rollback.sql"
VERIFY_SQL = REPO_ROOT / "scripts/db/verify_advisor_remediation_phase1.sql"

SECURITY_DEFINER_SIGNATURES = (
    "core.merge_shows(uuid, uuid)",
    "core.set_primary_media_link(text, uuid, text, uuid)",
    "core.upsert_cast_photos_by_canonical(jsonb)",
    "core.upsert_cast_photos_by_identity(jsonb)",
    "core.upsert_person_images(jsonb)",
    "core.upsert_show_images_by_identity(jsonb)",
    "core.upsert_tmdb_show_images_by_identity(jsonb)",
    "social.get_or_create_direct_conversation(uuid)",
)

SERVICE_ROLE_POLICY_PREFIXES = (
    "core_networks_service_role",
    "core_production_companies_service_role",
    "core_show_watch_providers_service_role",
    "core_watch_providers_service_role",
    "show_icons_service_role",
    "flashback_quizzes_service_role",
    "flashback_events_service_role",
)

PHASE0_POLICY_NAMES = (
    "core_tmdb_networks_service_role",
    "core_tmdb_production_companies_service_role",
    "core_show_watch_providers_service_role",
    "core_tmdb_watch_providers_service_role",
    '"Allow service role all on show_icons"',
    '"Service role full access to quizzes"',
    '"Service role full access to events"',
    "responses_admin_all",
    "responses_select_own",
    "responses_insert_own",
    "responses_update_own",
    "answers_admin_all",
    "answers_select_own",
    "answers_insert_own",
    "answers_update_own",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _policy_block(sql: str, policy_name: str) -> str:
    escaped = re.escape(policy_name)
    match = re.search(rf"create policy {escaped}\b.*?;", sql, flags=re.IGNORECASE | re.DOTALL)
    assert match, f"missing create policy block for {policy_name}"
    return match.group(0).lower()


def test_security_hotfix_locks_migrations_table_and_revokes_exposed_rpc_execute() -> None:
    sql = _read(SECURITY_MIGRATION).lower()

    assert "to_regclass('public.__migrations')" in sql
    assert "alter table public.__migrations enable row level security" in sql
    for role in ("public", "anon", "authenticated"):
        assert f"revoke all on table public.__migrations from {role}" in sql
    assert 'create policy "__migrations_no_api_access"' in sql
    assert "using (false)" in sql
    assert "with check (false)" in sql
    assert not re.findall(r"\bgrant\b.*?\bto\s+(public|anon|authenticated)\b", sql)

    for signature in SECURITY_DEFINER_SIGNATURES:
        signature_sql = signature.lower()
        assert f"revoke execute on function {signature_sql} from public, anon, authenticated;" in sql
        assert f"grant execute on function {signature_sql} to service_role;" in sql


def test_rls_cleanup_keeps_grants_out_and_uses_command_specific_policy_semantics() -> None:
    sql = _read(RLS_MIGRATION)
    lower_sql = sql.lower()

    assert " grant " not in lower_sql
    assert "revoke " not in lower_sql
    assert "public.__migrations" not in lower_sql

    for prefix in SERVICE_ROLE_POLICY_PREFIXES:
        insert_block = _policy_block(sql, f"{prefix}_insert")
        assert "for insert" in insert_block
        assert "to service_role" in insert_block
        assert "with check ((select auth.role()) = 'service_role')" in insert_block
        assert "\nusing " not in insert_block

        update_block = _policy_block(sql, f"{prefix}_update")
        assert "for update" in update_block
        assert "to service_role" in update_block
        assert "using ((select auth.role()) = 'service_role')" in update_block
        assert "with check ((select auth.role()) = 'service_role')" in update_block

        delete_block = _policy_block(sql, f"{prefix}_delete")
        assert "for delete" in delete_block
        assert "to service_role" in delete_block
        assert "using ((select auth.role()) = 'service_role')" in delete_block
        assert "with check" not in delete_block


def test_firebase_policy_lane_is_disabled_for_supabase_survey_collection() -> None:
    sql = _read(RLS_MIGRATION)
    lower_sql = sql.lower()

    assert "survey collection is moving through the supabase-auth surveys.* path" in lower_sql
    assert "new survey submissions should use surveys.submit_response" in lower_sql
    assert "create role trr_app nologin" not in lower_sql
    assert "app.firebase_uid" not in lower_sql
    assert "::uuid" not in lower_sql

    for policy_name in (
        "responses_admin_all",
        "responses_select_own",
        "responses_insert_own",
        "responses_update_own",
        "responses_select_access",
        "responses_insert_access",
        "responses_update_access",
        "responses_delete_access",
        "answers_admin_all",
        "answers_select_own",
        "answers_insert_own",
        "answers_update_own",
        "answers_select_access",
        "answers_insert_access",
        "answers_update_access",
        "answers_delete_access",
    ):
        assert f"drop policy if exists {policy_name}" in lower_sql
        assert f"create policy {policy_name}" not in lower_sql


def test_rollback_recreates_phase0_policies_by_name_without_create_or_replace() -> None:
    sql = _read(ROLLBACK_SQL).lower()

    assert "create or replace policy" not in sql
    for policy_name in PHASE0_POLICY_NAMES:
        assert f"drop policy if exists {policy_name.lower()}" in sql
        assert f"create policy {policy_name.lower()}" in sql


def test_phase1_verifier_checks_policy_shape_public_reads_and_security_hotfix() -> None:
    sql = _read(VERIFY_SQL).lower()

    assert "public.__migrations must have rls enabled when present" in sql
    assert "public.__migrations still has api/public grants" in sql
    assert "public.__migrations deny policy is missing or weakened" in sql
    assert "remains executable by anon/authenticated/public" in sql
    assert "expected 7 public select policies to remain" in sql
    assert "expected 7 service_role insert policies with with check only" in sql
    assert "expected 7 service_role update policies with using and with check" in sql
    assert "expected 7 service_role delete policies with using only" in sql
    assert "legacy firebase_surveys app rls policies must be disabled" in sql
    assert "unsafe broad write policies on phase 1 targets" in sql
    assert "unsafe broad core table grants" in sql
    assert "unsafe firebase_surveys table grants" in sql
    assert "information_schema.role_table_grants" in sql
    assert "surveys.submit_response(uuid, jsonb) is required" in sql
    assert "set local role trr_app" not in sql
    assert "owner context expected 1 visible verifier response" not in sql
    assert "rollback;" not in sql
    for prefix in SERVICE_ROLE_POLICY_PREFIXES:
        for command in ("insert", "update", "delete"):
            assert f"{prefix}_{command}" in sql


def test_external_id_conflicts_primary_key_migration_adds_defaulted_surrogate_key() -> None:
    sql = _read(EXTERNAL_ID_CONFLICTS_PK_MIGRATION).lower()

    assert "alter table core.external_id_conflicts" in sql
    assert "add column if not exists id uuid" in sql
    assert "alter column id set default gen_random_uuid()" in sql
    assert "set id = gen_random_uuid()" in sql
    assert "where id is null" in sql
    assert "alter column id set not null" in sql
    assert "contype = 'p'" in sql
    assert "add constraint external_id_conflicts_pkey primary key (id)" in sql
    assert "grant " not in sql
    assert "drop index" not in sql


def test_flashback_gameplay_removal_drops_only_disabled_write_path() -> None:
    sql = _read(FLASHBACK_GAMEPLAY_REMOVAL_MIGRATION).lower()

    assert "drop function if exists public.flashback_get_or_create_session(text, uuid)" in sql
    assert "drop function if exists public.flashback_save_placement(uuid, jsonb, integer, integer, boolean)" in sql
    assert "drop function if exists public.flashback_update_user_stats(text, integer, boolean)" in sql
    assert "drop table if exists public.flashback_user_stats" in sql
    assert "drop table if exists public.flashback_sessions" in sql
    assert "drop table if exists public.flashback_quizzes" not in sql
    assert "drop table if exists public.flashback_events" not in sql
    assert " cascade" not in sql


def test_external_id_conflicts_backfill_insert_still_omits_surrogate_key() -> None:
    sql = _read(EXTERNAL_ID_BACKFILL_MIGRATION).lower()

    match = re.search(r"insert into core\.external_id_conflicts\((.*?)\)", sql, flags=re.DOTALL)
    assert match, "missing external_id_conflicts insert"
    inserted_columns = {column.strip() for column in match.group(1).split(",")}

    assert inserted_columns == {
        "entity_type",
        "entity_id",
        "source_id",
        "external_id",
        "conflict_reason",
    }
    assert "id" not in inserted_columns
