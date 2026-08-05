from __future__ import annotations

import pytest

from trr_backend.repositories import core_people_reads

PERSON_ID = "11111111-1111-1111-1111-111111111111"
SHOW_ID = "22222222-2222-2222-2222-222222222222"


def _compact(sql: str) -> str:
    return " ".join(sql.lower().split())


def test_search_people_preserves_prefix_match_order_and_app_pagination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [
            {
                "id": PERSON_ID,
                "full_name": "Lisa Barlow",
                "known_for": "The Real Housewives of Salt Lake City",
                "external_ids": {"imdb": "nm0000001"},
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
            }
        ]

    monkeypatch.setattr(core_people_reads.pg, "fetch_all", fake_fetch_all)

    rows, query_count = core_people_reads.search_people("Lisa", limit=900, offset=-4)

    assert query_count == 1
    assert rows[0]["full_name"] == "Lisa Barlow"
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert ("select id, full_name, known_for, external_ids, created_at, updated_at from core.people") in normalized_sql
    assert "where full_name ilike %s" in normalized_sql
    assert "order by full_name asc" in normalized_sql
    assert params == ["Lisa%", 500, 0]


def test_get_person_by_id_preserves_the_full_core_people_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []
    expected = {
        "id": PERSON_ID,
        "full_name": "Lisa Barlow",
        "known_for": "The Real Housewives of Salt Lake City",
        "external_ids": {"imdb": "nm0000001"},
        "birthday": {"tmdb": "1974-12-21"},
        "gender": {"fandom": "Female"},
        "biography": {"tmdb": "Biography"},
        "place_of_birth": {"tmdb": "New York"},
        "homepage": {},
        "profile_image_url": {"tmdb": "/profile.jpg"},
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
    }

    def fake_fetch_one(sql: str, params: list[object]) -> dict[str, object]:
        calls.append((sql, params))
        return expected

    monkeypatch.setattr(core_people_reads.pg, "fetch_one", fake_fetch_one)

    row, query_count = core_people_reads.get_person_by_id(PERSON_ID)

    assert row == expected
    assert query_count == 1
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "select * from core.people" in normalized_sql
    assert "where id = %s::uuid" in normalized_sql
    assert "limit 1" in normalized_sql
    assert params == [PERSON_ID]


def test_relationships_stop_after_active_kid_assignments_have_no_parent_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [{"relationship_from": None}, {"relationship_from": "  "}]

    monkeypatch.setattr(core_people_reads.pg, "fetch_all", fake_fetch_all)

    relationships, query_count = core_people_reads.get_deduced_family_relationships_by_person_id(PERSON_ID)

    assert relationships == {}
    assert query_count == 1
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "from core.show_cast_role_assignments as sra" in normalized_sql
    assert "join core.show_role_catalog as rc on rc.id = sra.role_id" in normalized_sql
    assert "lower(rc.name) = 'kid'" in normalized_sql
    assert "sra.person_id = %s::uuid" in normalized_sql
    assert params == [PERSON_ID]


def test_relationships_match_people_and_fandom_gender_then_label_parents_and_siblings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []
    parent_ids = [
        "30000000-0000-0000-0000-000000000001",
        "30000000-0000-0000-0000-000000000002",
        "30000000-0000-0000-0000-000000000003",
        "30000000-0000-0000-0000-000000000004",
    ]
    parent_names = ["Mother Person", "Father Person", "Spouse Person", "Unknown Person"]

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        normalized_sql = _compact(sql)
        if "relationship_from" in normalized_sql and "select distinct nullif" in normalized_sql:
            return [
                {"relationship_from": "Mother Person"},
                {"relationship_from": "Father Person"},
                {"relationship_from": "Spouse Person"},
                {"relationship_from": "Unknown Person"},
                {"relationship_from": "Mother Person"},
            ]
        if "from unnest(%s::text[]) as rel(name)" in normalized_sql:
            return [
                {"person_id": parent_ids[0], "full_name": parent_names[0], "fandom_gender": "Female"},
                {"person_id": parent_ids[1], "full_name": parent_names[1], "fandom_gender": "MALE"},
                {"person_id": parent_ids[2], "full_name": parent_names[2], "fandom_gender": None},
                {"person_id": parent_ids[3], "full_name": parent_names[3], "fandom_gender": "unknown"},
            ]
        if "lower(rc.name) as role_name" in normalized_sql:
            return [
                {"person_id": parent_ids[2], "role_name": "boyfriend"},
                {"person_id": parent_ids[3], "role_name": "friend"},
            ]
        if "sra.person_id <> %s::uuid" in normalized_sql:
            return [
                {
                    "person_id": "40000000-0000-0000-0000-000000000001",
                    "full_name": "Sister Person",
                    "fandom_gender": "female",
                },
                {
                    "person_id": "40000000-0000-0000-0000-000000000002",
                    "full_name": "Brother Person",
                    "fandom_gender": "M",
                },
                {
                    "person_id": "40000000-0000-0000-0000-000000000003",
                    "full_name": "Sibling Person",
                    "fandom_gender": None,
                },
            ]
        raise AssertionError(normalized_sql)

    monkeypatch.setattr(core_people_reads.pg, "fetch_all", fake_fetch_all)

    relationships, query_count = core_people_reads.get_deduced_family_relationships_by_person_id(PERSON_ID)

    assert query_count == 4
    assert relationships == {
        "Mother Person": "Mom",
        "Father Person": "Dad",
        "Spouse Person": "Dad",
        "Unknown Person": "Mom",
        "Sister Person": "Sister",
        "Brother Person": "Brother",
        "Sibling Person": "Sibling",
    }
    parent_people_sql = _compact(calls[1][0])
    assert "join core.people as p on lower(p.full_name) = lower(rel.name)" in parent_people_sql
    assert "left join core.cast_fandom as cf" in parent_people_sql
    assert "cf.source = 'fandom'" in parent_people_sql
    assert calls[1][1] == [parent_names]
    assert calls[2][1] == [parent_ids]
    assert calls[3][1] == [PERSON_ID, parent_names]


def test_relationships_apply_optional_show_scope_and_keep_single_unknown_parent_generic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []
    parent_id = "30000000-0000-0000-0000-000000000001"

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        normalized_sql = _compact(sql)
        if "select distinct nullif" in normalized_sql:
            return [{"relationship_from": "Unknown Parent"}]
        if "from unnest(%s::text[]) as rel(name)" in normalized_sql:
            return [{"person_id": parent_id, "full_name": "Unknown Parent", "fandom_gender": None}]
        if "lower(rc.name) as role_name" in normalized_sql:
            return []
        if "sra.person_id <> %s::uuid" in normalized_sql:
            return []
        raise AssertionError(normalized_sql)

    monkeypatch.setattr(core_people_reads.pg, "fetch_all", fake_fetch_all)

    relationships, query_count = core_people_reads.get_deduced_family_relationships_by_person_id(
        PERSON_ID,
        SHOW_ID,
    )

    assert relationships == {"Unknown Parent": "Parent"}
    assert query_count == 4
    assert "sra.show_id = %s::uuid" in _compact(calls[0][0])
    assert calls[0][1] == [PERSON_ID, SHOW_ID]
    assert "join core.v_show_cast as sc" in _compact(calls[1][0])
    assert "sc.show_id = %s::uuid" in _compact(calls[1][0])
    assert calls[1][1] == [["Unknown Parent"], SHOW_ID]
    assert calls[2][1] == [[parent_id], SHOW_ID]
    assert calls[3][1] == [PERSON_ID, ["Unknown Parent"], SHOW_ID]
    sibling_sql = _compact(calls[3][0])
    assert sibling_sql.index("any(%s::text[])") < sibling_sql.index("sra.show_id = %s::uuid")
