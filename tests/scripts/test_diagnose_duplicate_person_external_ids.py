from __future__ import annotations

from unittest.mock import patch

import scripts.shows.diagnose_duplicate_person_external_ids as mod


def test_parse_args_defaults() -> None:
    args = mod._parse_args([])
    assert args.show_id == []
    assert args.json_summary == ""


def test_list_scope_show_ids_uses_explicit_values() -> None:
    out = mod._list_scope_show_ids(["show-a", "show-a", " show-b "])
    assert out == ["show-a", "show-b"]


def test_list_scope_show_ids_defaults_to_bravo_scope() -> None:
    with patch.object(mod.pg, "fetch_all", return_value=[{"show_id": "s2"}, {"show_id": "s1"}]) as fetch_all:
        out = mod._list_scope_show_ids([])

    assert out == ["s2", "s1"]
    query = fetch_all.call_args.args[0]
    assert "ORDER BY show_id" in query


def test_group_duplicate_conflicts_requires_cast_link_and_conflicting_ids() -> None:
    rows = [
        {
            "person_id": "p1",
            "full_name": "Katie Maloney",
            "name_key": "katie maloney",
            "imdb_id": "nm111",
            "tmdb_id": "10",
            "wikidata_id": None,
            "cast_show_count": 2,
            "cast_show_ids": ["s1", "s2"],
        },
        {
            "person_id": "p2",
            "full_name": "Katie Maloney",
            "name_key": "katie maloney",
            "imdb_id": "nm222",
            "tmdb_id": "10",
            "wikidata_id": None,
            "cast_show_count": 0,
            "cast_show_ids": [],
        },
        {
            "person_id": "p3",
            "full_name": "Lisa Barlow",
            "name_key": "lisa barlow",
            "imdb_id": "nm333",
            "tmdb_id": "20",
            "wikidata_id": None,
            "cast_show_count": 3,
            "cast_show_ids": ["s3"],
        },
        {
            "person_id": "p4",
            "full_name": "Lisa Barlow",
            "name_key": "lisa barlow",
            "imdb_id": "nm333",
            "tmdb_id": "20",
            "wikidata_id": None,
            "cast_show_count": 0,
            "cast_show_ids": [],
        },
    ]

    findings = mod._group_duplicate_conflicts(rows)
    assert len(findings) == 1
    finding = findings[0]
    assert finding["name"] == "Katie Maloney"
    assert finding["recommended_canonical_person_id"] == "p1"
    assert finding["conflicts"]["imdb_id"] == ["nm111", "nm222"]


def test_main_writes_json_summary() -> None:
    with patch.object(mod, "load_env"):
        with patch.object(mod, "_list_scope_show_ids", return_value=["show-1"]):
            with patch.object(mod, "_fetch_duplicate_people_rows", return_value=[]):
                with patch.object(mod, "_group_duplicate_conflicts", return_value=[]):
                    with patch.object(mod, "_write_json") as write_json:
                        code = mod.main(["--json-summary", "/tmp/dupes.json"])
    assert code == 0
    write_json.assert_called_once()
