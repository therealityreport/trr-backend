from __future__ import annotations

import pytest

from trr_backend.repositories import core_show_reads

SHOW_ID = "11111111-1111-1111-1111-111111111111"
SEASON_ID = "22222222-2222-2222-2222-222222222222"
EPISODE_ID = "33333333-3333-3333-3333-333333333333"


def _compact(sql: str) -> str:
    return " ".join(sql.lower().split())


def test_search_shows_uses_name_and_alternative_name_match_with_image_joins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [
            {
                "id": SHOW_ID,
                "name": "The Real Housewives of Beverly Hills",
                "alternative_names": None,
                "genres": None,
                "networks": ["Bravo"],
                "streaming_providers": None,
                "tags": None,
                "canonical_slug": "rhobh",
                "poster_url": "https://cdn.example/poster.jpg",
                "backdrop_url": None,
                "logo_url": None,
            }
        ]

    monkeypatch.setattr(core_show_reads.pg, "fetch_all", fake_fetch_all)

    rows, query_count = core_show_reads.search_shows("housewives", limit=1000, offset=-4)

    assert query_count == 1
    assert rows[0]["alternative_names"] == []
    assert rows[0]["genres"] == []
    assert rows[0]["networks"] == ["Bravo"]
    assert rows[0]["streaming_providers"] == []
    assert rows[0]["tags"] == []
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "with shows_with_slug as" in normalized_sql
    assert "left join core.show_images as poster" in normalized_sql
    assert "left join core.show_images as backdrop" in normalized_sql
    assert "left join core.show_images as logo" in normalized_sql
    assert "s.name ilike %s" in normalized_sql
    assert "unnest(coalesce(s.alternative_names, array[]::text[]))" in normalized_sql
    assert "order by s.name asc" in normalized_sql
    assert params == ["%housewives%", "%housewives%", 500, 0]


def test_get_show_by_id_returns_none_for_missing_row(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_one(sql: str, params: list[object]) -> None:
        calls.append((sql, params))
        return None

    monkeypatch.setattr(core_show_reads.pg, "fetch_one", fake_fetch_one)

    row, query_count = core_show_reads.get_show_by_id(SHOW_ID)

    assert row is None
    assert query_count == 1
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "from shows_with_slug as s" in normalized_sql
    assert "where s.id = %s::uuid" in normalized_sql
    assert params == [SHOW_ID]


def test_get_seasons_by_show_id_can_include_episode_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [
            {
                "id": SEASON_ID,
                "show_id": SHOW_ID,
                "season_number": 14,
                "episode_airdate_count": 3,
                "has_scheduled_or_aired_episode": True,
            }
        ]

    monkeypatch.setattr(core_show_reads.pg, "fetch_all", fake_fetch_all)

    rows, query_count = core_show_reads.get_seasons_by_show_id(
        SHOW_ID,
        limit=10,
        offset=2,
        include_episode_signal=True,
    )

    assert query_count == 1
    assert rows[0]["has_scheduled_or_aired_episode"] is True
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "left join lateral" in normalized_sql
    assert "from core.episodes as e" in normalized_sql
    assert "where e.season_id = s.id" in normalized_sql
    assert "and e.air_date is not null" in normalized_sql
    assert "order by s.season_number desc" in normalized_sql
    assert params == [SHOW_ID, 10, 2]


def test_season_and_episode_point_reads_return_one_query(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_one(sql: str, params: list[object]) -> dict[str, object]:
        calls.append((sql, params))
        return {"id": params[0]}

    monkeypatch.setattr(core_show_reads.pg, "fetch_one", fake_fetch_one)

    assert core_show_reads.get_season_by_id(SEASON_ID) == ({"id": SEASON_ID}, 1)
    assert core_show_reads.get_season_by_show_and_number(SHOW_ID, 3) == ({"id": SHOW_ID}, 1)
    assert core_show_reads.get_episode_by_id(EPISODE_ID) == ({"id": EPISODE_ID}, 1)

    assert "from core.seasons" in _compact(calls[0][0])
    assert calls[0][1] == [SEASON_ID]
    assert "season_number = %s::int" in _compact(calls[1][0])
    assert calls[1][1] == [SHOW_ID, 3]
    assert "from core.episodes" in _compact(calls[2][0])
    assert calls[2][1] == [EPISODE_ID]


def test_episode_list_reads_preserve_episode_number_order(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [{"id": EPISODE_ID, "episode_number": 1}]

    monkeypatch.setattr(core_show_reads.pg, "fetch_all", fake_fetch_all)

    by_season, season_query_count = core_show_reads.get_episodes_by_season_id(SEASON_ID, limit=5, offset=1)
    by_show, show_query_count = core_show_reads.get_episodes_by_show_and_season(SHOW_ID, 2, limit=6, offset=0)

    assert by_season == [{"id": EPISODE_ID, "episode_number": 1}]
    assert by_show == [{"id": EPISODE_ID, "episode_number": 1}]
    assert season_query_count == 1
    assert show_query_count == 1
    assert "where season_id = %s::uuid" in _compact(calls[0][0])
    assert "order by episode_number asc" in _compact(calls[0][0])
    assert calls[0][1] == [SEASON_ID, 5, 1]
    assert "where show_id = %s::uuid and season_number = %s::int" in _compact(calls[1][0])
    assert "order by episode_number asc" in _compact(calls[1][0])
    assert calls[1][1] == [SHOW_ID, 2, 6, 0]


def test_search_episodes_matches_title_episode_label_and_sxe_with_expected_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [
            {
                "id": EPISODE_ID,
                "title": "Reunion",
                "episode_number": 18,
                "season_number": 14,
                "show_slug": "rhobh",
            }
        ]

    monkeypatch.setattr(core_show_reads.pg, "fetch_all", fake_fetch_all)

    rows, query_count = core_show_reads.search_episodes("Re", limit=2, offset=3)

    assert query_count == 1
    assert rows[0]["show_slug"] == "rhobh"
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "join shows_with_slug as sws on sws.id = e.show_id" in normalized_sql
    assert "coalesce(e.title, '') ilike %s" in normalized_sql
    assert "concat('episode ', coalesce(e.episode_number::text, '')) ilike %s" in normalized_sql
    compact_episode_number = (
        "concat('s', coalesce(e.season_number::text, ''), 'e', coalesce(e.episode_number::text, '')) ilike %s"
    )
    assert compact_episode_number in normalized_sql
    assert "e.air_date desc nulls last" in normalized_sql
    assert "e.updated_at desc nulls last" in normalized_sql
    assert "e.id asc" in normalized_sql
    assert params == ["%Re%", "%Re%", "%Re%", "Re%", 2, 3]
