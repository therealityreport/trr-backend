from __future__ import annotations

from typing import Any

from trr_backend.repositories import public_identities


def test_show_alias_candidates_are_narrow_and_deterministic(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]) -> list[dict[str, Any]]:
        calls.append((query, params))
        return [
            {
                "show_id": "00000000-0000-0000-0000-000000000001",
                "show_name": "Bravo Show",
                "canonical_slug": "bravo-show",
                "matched_is_canonical": True,
            }
        ]

    monkeypatch.setattr(public_identities.pg, "fetch_all", fake_fetch_all)

    rows = public_identities.list_show_slug_candidates("bravo-show")

    assert rows == [
        {
            "show_id": "00000000-0000-0000-0000-000000000001",
            "show_name": "Bravo Show",
            "canonical_slug": "bravo-show",
            "matched_is_canonical": True,
        }
    ]
    assert len(calls) == 1
    assert calls[0][1] == ["bravo-show"]
    assert "matched.slug = %s" in calls[0][0]
    assert "order by bool_or(matched.is_canonical) desc" in calls[0][0].lower()


def test_season_identity_is_one_show_number_query(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_one(query: str, params: list[object]) -> dict[str, Any]:
        calls.append((query, params))
        return {
            "season_id": "00000000-0000-0000-0000-000000000014",
            "show_id": "00000000-0000-0000-0000-000000000001",
            "season_number": 14,
            "season_title": "Season 14",
        }

    monkeypatch.setattr(public_identities.pg, "fetch_one", fake_fetch_one)

    row = public_identities.get_season_identity(
        show_id="00000000-0000-0000-0000-000000000001",
        season_number=14,
    )

    assert row is not None
    assert row["season_number"] == 14
    assert len(calls) == 1
    assert calls[0][1] == ["00000000-0000-0000-0000-000000000001", 14]
    assert "season.show_id = %s::uuid" in calls[0][0]


def test_person_alias_query_applies_optional_show_context_in_sql(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]) -> list[dict[str, Any]]:
        calls.append((query, params))
        return [
            {
                "person_id": "00000000-0000-0000-0000-000000000002",
                "full_name": "Alex Smith",
                "canonical_slug": "alex-smith--00000000",
                "matched_is_canonical": False,
            }
        ]

    monkeypatch.setattr(public_identities.pg, "fetch_all", fake_fetch_all)
    rows = public_identities.list_person_slug_candidates(
        slug="alex-smith",
        show_id="00000000-0000-0000-0000-000000000001",
    )

    assert rows[0]["matched_is_canonical"] is False
    assert len(calls) == 1
    assert calls[0][1] == ["00000000-0000-0000-0000-000000000001", "alex-smith"]
    assert "from core.v_show_cast" in calls[0][0].lower()
    assert "show_cast.person_id = person.id" in calls[0][0].lower()
