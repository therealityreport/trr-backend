from __future__ import annotations

from typing import Any

import pytest

from trr_backend.services import core_show_reads

SHOW_ID = "11111111-1111-1111-1111-111111111111"
SEASON_ID = "22222222-2222-2222-2222-222222222222"
EPISODE_ID = "33333333-3333-3333-3333-333333333333"


def test_service_delegates_show_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def record(name: str, result: object):
        def inner(*args: Any, **kwargs: Any) -> object:
            calls.append((name, args, kwargs))
            return result

        return inner

    monkeypatch.setattr(core_show_reads.repository, "search_shows", record("search_shows", ([{"id": SHOW_ID}], 1)))
    monkeypatch.setattr(core_show_reads.repository, "get_show_by_id", record("get_show_by_id", (None, 1)))

    assert core_show_reads.search_shows("bravo", limit=5, offset=2) == ([{"id": SHOW_ID}], 1)
    assert core_show_reads.get_show_by_id(SHOW_ID) == (None, 1)
    assert calls == [
        ("search_shows", ("bravo",), {"limit": 5, "offset": 2}),
        ("get_show_by_id", (SHOW_ID,), {}),
    ]


def test_service_delegates_season_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def record(name: str, result: object):
        def inner(*args: Any, **kwargs: Any) -> object:
            calls.append((name, args, kwargs))
            return result

        return inner

    monkeypatch.setattr(
        core_show_reads.repository,
        "get_seasons_by_show_id",
        record("get_seasons_by_show_id", ([{"id": SEASON_ID}], 1)),
    )
    monkeypatch.setattr(core_show_reads.repository, "get_season_by_id", record("get_season_by_id", (None, 1)))
    monkeypatch.setattr(
        core_show_reads.repository,
        "get_season_by_show_and_number",
        record("get_season_by_show_and_number", ({"id": SEASON_ID}, 1)),
    )

    assert core_show_reads.get_seasons_by_show_id(
        SHOW_ID,
        limit=4,
        offset=1,
        include_episode_signal=True,
    ) == ([{"id": SEASON_ID}], 1)
    assert core_show_reads.get_season_by_id(SEASON_ID) == (None, 1)
    assert core_show_reads.get_season_by_show_and_number(SHOW_ID, 7) == ({"id": SEASON_ID}, 1)
    assert calls == [
        (
            "get_seasons_by_show_id",
            (SHOW_ID,),
            {"limit": 4, "offset": 1, "include_episode_signal": True},
        ),
        ("get_season_by_id", (SEASON_ID,), {}),
        ("get_season_by_show_and_number", (SHOW_ID, 7), {}),
    ]


def test_service_delegates_episode_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def record(name: str, result: object):
        def inner(*args: Any, **kwargs: Any) -> object:
            calls.append((name, args, kwargs))
            return result

        return inner

    monkeypatch.setattr(
        core_show_reads.repository,
        "get_episodes_by_season_id",
        record("get_episodes_by_season_id", ([{"id": EPISODE_ID}], 1)),
    )
    monkeypatch.setattr(
        core_show_reads.repository,
        "get_episodes_by_show_and_season",
        record("get_episodes_by_show_and_season", ([{"id": EPISODE_ID}], 1)),
    )
    monkeypatch.setattr(core_show_reads.repository, "get_episode_by_id", record("get_episode_by_id", (None, 1)))
    monkeypatch.setattr(
        core_show_reads.repository,
        "search_episodes",
        record("search_episodes", ([{"id": EPISODE_ID}], 1)),
    )

    assert core_show_reads.get_episodes_by_season_id(SEASON_ID, limit=2, offset=8) == ([{"id": EPISODE_ID}], 1)
    assert core_show_reads.get_episodes_by_show_and_season(SHOW_ID, 4, limit=3, offset=1) == (
        [{"id": EPISODE_ID}],
        1,
    )
    assert core_show_reads.get_episode_by_id(EPISODE_ID) == (None, 1)
    assert core_show_reads.search_episodes("reunion", limit=9, offset=0) == ([{"id": EPISODE_ID}], 1)
    assert calls == [
        ("get_episodes_by_season_id", (SEASON_ID,), {"limit": 2, "offset": 8}),
        ("get_episodes_by_show_and_season", (SHOW_ID, 4), {"limit": 3, "offset": 1}),
        ("get_episode_by_id", (EPISODE_ID,), {}),
        ("search_episodes", ("reunion",), {"limit": 9, "offset": 0}),
    ]
