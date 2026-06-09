from __future__ import annotations

from scripts.socials import sync_bravo_show_hashtag_aliases as cli


def test_show_alias_candidates_include_full_name_slug_and_housewives_acronym() -> None:
    aliases = cli._show_alias_candidates(
        {
            "name": "The Real Housewives of Beverly Hills",
            "slug": "the-real-housewives-of-beverly-hills",
            "alternative_names": ["RHOBH"],
        }
    )

    assert "TheRealHousewivesofBeverlyHills" in aliases
    assert "RHOBH" in aliases
    assert len({cli._compact_alias(alias) for alias in aliases}) == len(aliases)


def test_show_alias_candidates_include_southern_charm_hashtag_form() -> None:
    aliases = cli._show_alias_candidates(
        {
            "name": "Southern Charm",
            "slug": "southern-charm",
            "alternative_names": [],
        }
    )

    assert aliases[0] == "SouthernCharm"
    assert cli._compact_alias("SouthernCharm") in {cli._compact_alias(alias) for alias in aliases}


def test_alias_rows_for_shows_uses_requested_source() -> None:
    rows = cli._alias_rows_for_shows(
        [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "Southern Charm",
                "slug": "southern-charm",
                "alternative_names": [],
            }
        ],
        source="bravo_hashtag",
    )

    assert rows
    assert {row["show_id"] for row in rows} == {"00000000-0000-0000-0000-000000000001"}
    assert {row["source"] for row in rows} == {"bravo_hashtag"}
    assert "SouthernCharm" in {row["name"] for row in rows}


def test_cast_alias_candidates_include_full_name_hashtag_form() -> None:
    aliases = cli._cast_alias_candidates(
        {
            "full_name": "Paige DeSorbo",
            "cast_member_name": "Paige DeSorbo",
            "alternative_names": [],
        }
    )

    assert aliases == ["PaigeDeSorbo"]


def test_alias_rows_for_cast_uses_person_id() -> None:
    rows = cli._alias_rows_for_cast(
        [
            {
                "person_id": "00000000-0000-0000-0000-000000000002",
                "full_name": "Craig Conover",
                "cast_member_name": None,
                "alternative_names": [],
            }
        ]
    )

    assert rows == [
        {
            "person_id": "00000000-0000-0000-0000-000000000002",
            "person_name": "Craig Conover",
            "name": "CraigConover",
        }
    ]
