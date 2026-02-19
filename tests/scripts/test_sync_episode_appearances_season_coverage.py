from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.sync import sync_episode_appearances as module
from trr_backend.integrations.imdb.fullcredits_cast_parser import CastRow


def _cast_row() -> CastRow:
    return CastRow(
        name_id="nm1234567",
        name="Test Person",
        billing_order=1,
        raw_role_text="Self",
        job_category_id=None,
    )


def test_fetch_episode_index_includes_season_number() -> None:
    db = MagicMock()
    db.schema.return_value.table.return_value.select.return_value.eq.return_value.execute.return_value = (
        SimpleNamespace(
            error=None,
            data=[
                {
                    "id": "ep-1",
                    "imdb_episode_id": "tt0000001",
                    "air_date": "2024-01-01",
                    "season_number": 3,
                }
            ],
        )
    )

    index = module._fetch_episode_index(db, show_id="show-1")

    assert "tt0000001" in index
    assert index["tt0000001"].id == "ep-1"
    assert index["tt0000001"].season_number == 3


def test_fetch_episodic_credits_prefers_episode_index_seasons(
    monkeypatch,
) -> None:
    class DummyClient:
        def __init__(self) -> None:
            self.available_calls = 0
            self.credit_calls: list[tuple[int, ...]] = []

        def fetch_available_seasons(self, *_args, **_kwargs) -> list[int]:
            self.available_calls += 1
            return [8, 9]

        def fetch_episode_credits_for_seasons(
            self,
            *_args,
            seasons,
            **_kwargs,
        ) -> list:
            self.credit_calls.append(tuple(seasons))
            return []

    instances: list[DummyClient] = []

    def _factory(*_args, **_kwargs):
        client = DummyClient()
        instances.append(client)
        return client

    monkeypatch.setattr(module, "HttpImdbEpisodicClient", _factory)

    result = module._fetch_episodic_credits(
        series_id="tt1111111",
        cast_row=_cast_row(),
        season_numbers_from_episodes=[1, 3, 3, 2],
        extra_headers=None,
    )

    assert result.error is None
    assert result.season_source == "episodes_index"
    assert result.seasons_used == (1, 2, 3)
    assert instances[0].available_calls == 0
    assert instances[0].credit_calls == [(1, 2, 3)]


def test_fetch_episodic_credits_falls_back_to_imdb_available_seasons(
    monkeypatch,
) -> None:
    class DummyClient:
        def __init__(self) -> None:
            self.available_calls = 0
            self.credit_calls: list[tuple[int, ...]] = []

        def fetch_available_seasons(self, *_args, **_kwargs) -> list[int]:
            self.available_calls += 1
            return [4, 5]

        def fetch_episode_credits_for_seasons(
            self,
            *_args,
            seasons,
            **_kwargs,
        ) -> list:
            self.credit_calls.append(tuple(seasons))
            return []

    instances: list[DummyClient] = []

    def _factory(*_args, **_kwargs):
        client = DummyClient()
        instances.append(client)
        return client

    monkeypatch.setattr(module, "HttpImdbEpisodicClient", _factory)

    result = module._fetch_episodic_credits(
        series_id="tt1111111",
        cast_row=_cast_row(),
        season_numbers_from_episodes=[],
        extra_headers=None,
    )

    assert result.error is None
    assert result.season_source == "imdb_available_seasons"
    assert result.seasons_used == (4, 5)
    assert instances[0].available_calls == 1
    assert instances[0].credit_calls == [(4, 5)]
