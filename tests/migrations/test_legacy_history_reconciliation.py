from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = REPO_ROOT / "supabase/migrations"
HOSTED_0147 = MIGRATIONS / "0147_network_streaming_resolution_policy.sql"
SHOW_ICONS_0150 = MIGRATIONS / "0150_add_show_icons_table.sql"
INSTAGRAM_RECONCILIATION = MIGRATIONS / "20260421133959_reconcile_instagram_enhanced_metadata.sql"


def _normalized(path: Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8").lower()).strip()


def test_legacy_versions_are_unique_and_0147_matches_the_hosted_contract() -> None:
    versions: dict[str, list[Path]] = {}
    for migration in MIGRATIONS.glob("*.sql"):
        version = migration.name.split("_", 1)[0]
        versions.setdefault(version, []).append(migration)

    duplicates = {version: paths for version, paths in versions.items() if len(paths) > 1}
    assert duplicates == {}

    sql = _normalized(HOSTED_0147)
    assert "add column if not exists resolution_policy text" in sql
    assert "add column if not exists logo_required boolean" in sql
    assert "network_streaming_completion_resolution_policy_check" in sql
    assert "where entity_type = 'production'" in sql
    assert "where entity_type in ('network', 'streaming')" in sql


def test_instagram_reconciliation_precedes_its_first_dependent_migration() -> None:
    sql = _normalized(INSTAGRAM_RECONCILIATION)

    assert INSTAGRAM_RECONCILIATION.name < "20260421134000_hosted_tagged_profile_pics_object_shape.sql"
    assert sql.startswith("begin;")
    assert sql.endswith("commit;")
    for column in (
        "tagged_users_detail",
        "collaborators_detail",
        "hosted_owner_profile_pic_url",
        "hosted_tagged_profile_pics",
        "author_profile_pic_url",
        "author_is_verified",
    ):
        assert f"add column if not exists {column}" in sql


def test_show_icons_policy_sql_is_replay_parser_safe_and_idempotent() -> None:
    sql = _normalized(SHOW_ICONS_0150)

    assert "do $$" not in sql
    assert sql.count("drop policy if exists") == 2
    assert sql.count("create policy") == 2
    assert 'create policy "allow public read on show_icons"' in sql
    assert 'create policy "allow service role all on show_icons"' in sql
    assert "using (auth.role() = 'service_role')" in sql
    assert "with check (auth.role() = 'service_role')" in sql
