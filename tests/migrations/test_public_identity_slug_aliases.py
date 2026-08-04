from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = REPO_ROOT / "supabase/migrations/20260716040000_public_identity_slug_aliases.sql"


def _sql() -> str:
    return re.sub(r"\s+", " ", MIGRATION.read_text(encoding="utf-8").lower())


def _normalizer_body() -> str:
    sql = MIGRATION.read_text(encoding="utf-8")
    match = re.search(
        r"as \$normalize_public_identity_slug\$(.*?)\$normalize_public_identity_slug\$;",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match is not None
    return match.group(1)


def _evaluate_normalizer_contract(raw_value: str | None) -> str | None:
    """Evaluate the SQL body's documented operation order for fixed cases."""
    normalized = re.sub(r"&", " and ", raw_value or "", flags=re.IGNORECASE)
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized, flags=re.IGNORECASE).strip("-")
    normalized = normalized[:120].rstrip("-").lower()
    return normalized or None


def test_alias_tables_use_uuid_owners_and_cascade_with_normalized_slugs() -> None:
    sql = _sql()
    assert "create table if not exists core.show_slug_aliases" in sql
    assert "show_id uuid not null references core.shows(id) on delete cascade" in sql
    assert "create table if not exists core.person_slug_aliases" in sql
    assert "person_id uuid not null references core.people(id) on delete cascade" in sql
    assert sql.count("slug = lower(btrim(slug))") == 2
    assert sql.count("length(slug) between 1 and 160") == 2
    assert "create table if not exists core.season_slug_aliases" not in sql


def test_alias_tables_enforce_one_canonical_per_entity_and_global_canonical_uniqueness() -> None:
    sql = _sql()
    assert "on core.show_slug_aliases (show_id) where is_canonical = true" in sql
    assert "on core.show_slug_aliases (slug) where is_canonical = true" in sql
    assert "on core.person_slug_aliases (person_id) where is_canonical = true" in sql
    assert "on core.person_slug_aliases (slug) where is_canonical = true" in sql
    assert "having count(a.id) <> 1" in sql
    assert "every core.shows row must have exactly one canonical show slug alias" in sql
    assert "every core.people row must have exactly one canonical person slug alias" in sql


def test_backfill_retains_direct_aliases_and_adds_collision_safe_canonical_slugs() -> None:
    sql = _sql()
    assert "row_number() over (partition by base_slug order by show_id) as base_rank" in sql
    assert "row_number() over (partition by base_slug order by person_id) as base_rank" in sql
    assert "else base_slug || '--' || replace(show_id::text, '-', '')" in sql
    assert "else base_slug || '--' || replace(person_id::text, '-', '')" in sql
    assert "(s.slug, 'legacy:slug')" in sql
    assert "(s.name, 'legacy:name')" in sql
    assert "'legacy:alternative-name'" in sql
    assert "'legacy:article-variant'" in sql
    assert "when a.slug like 'the-%' then substring(a.slug from 5)" in sql
    assert "else 'the-' || a.slug" in sql
    assert "'legacy:full-name'" in sql
    assert "'legacy:collision-prefix'" in sql
    assert "'show-' || replace(s.id::text, '-', '')" in sql
    assert "'person-' || replace(p.id::text, '-', '')" in sql
    assert 120 + len("--") + 32 <= 160


def test_future_insert_and_rename_triggers_are_safe_stable_and_idempotent() -> None:
    sql = _sql()
    assert "create or replace function core.normalize_public_identity_slug(raw_value text)" in sql
    assert "left( trim(" in sql
    assert "120" in sql
    assert sql.count("security definer") == 2
    assert sql.count("set search_path = pg_catalog") == 3
    assert "pg_catalog.pg_advisory_xact_lock(" in sql
    assert "pg_catalog.hashtextextended('core.show_slug_aliases:' || base_slug, 0)" in sql
    assert "pg_catalog.hashtextextended('core.person_slug_aliases:' || base_slug, 0)" in sql
    assert "if canonical_slug is null then" in sql
    assert "canonical_slug := base_slug || '--' || replace(new.id::text, '-', '')" in sql
    assert "foreach raw_alias in array coalesce(new.alternative_names, array[]::text[])" in sql
    assert "values (new.id, normalized_alias, false, 'trigger:alternative-name')" in sql
    assert sql.count("values (new.id, article_alias, false, 'trigger:article-variant')") == 2
    assert "when normalized_alias like 'the-%' then substring(normalized_alias from 5)" in sql
    assert "after insert or update of slug, name, alternative_names on core.shows" in sql
    assert "after insert or update of full_name on core.people" in sql
    assert "drop trigger if exists core_shows_sync_public_identity_aliases on core.shows" in sql
    assert "drop trigger if exists core_people_sync_public_identity_aliases on core.people" in sql
    assert "on conflict (show_id, slug) do nothing" in sql
    assert "on conflict (person_id, slug) do nothing" in sql
    assert "revoke all on function core.sync_show_public_identity_aliases() from public, anon, authenticated" in sql
    assert "revoke all on function core.sync_person_public_identity_aliases() from public, anon, authenticated" in sql


def test_normalizer_body_explicitly_lowercases_representative_slug_outputs() -> None:
    body = re.sub(r"\s+", " ", _normalizer_body().strip().lower())
    assert body.startswith("select nullif( lower( rtrim( left( trim(")
    assert "regexp_replace(coalesce(raw_value, ''), '&', ' and ', 'gi')" in body
    assert "'[^a-z0-9]+', '-', 'gi'" in body
    assert "), 120 ), '-' ) ), '' );" in body

    cases = {
        "The Real Housewives & Friends": "the-real-housewives-and-friends",
        "RHOBH": "rhobh",
        "Déjà VU & Café": "d-j-vu-and-caf",
        "A" * 119 + " & B": "a" * 119,
        "---": None,
    }
    assert {value: _evaluate_normalizer_contract(value) for value in cases} == cases


def test_alias_rls_is_public_read_and_service_role_write() -> None:
    sql = _sql()
    assert "grant select on table core.show_slug_aliases, core.person_slug_aliases to anon, authenticated" in sql
    assert (
        "revoke insert, update, delete on table core.show_slug_aliases, "
        "core.person_slug_aliases from anon, authenticated" in sql
    )
    for table in ("core.show_slug_aliases", "core.person_slug_aliases"):
        assert f"alter table {table} enable row level security" in sql
    assert sql.count("for select to anon, authenticated using (true)") == 2
    assert sql.count("for all to service_role using (true) with check (true)") == 2
