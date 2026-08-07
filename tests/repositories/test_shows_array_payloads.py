from __future__ import annotations

from typing import cast

from trr_backend.db.session import DbSession
from trr_backend.models.shows import ShowUpsert
from trr_backend.repositories.shows import insert_show


class _FakeResponse:
    def __init__(self, *, data=None, error=None):  # noqa: ANN001
        self.data = data or []
        self.error = error


class _RecordingClient:
    def __init__(self) -> None:
        self.insert_payload: dict | None = None

    def schema(self, _name: str):  # noqa: ANN001
        return self

    def table(self, _name: str):  # noqa: ANN001
        return self

    def insert(self, payload: dict):  # noqa: ANN001
        self.insert_payload = dict(payload)
        return self

    def execute(self) -> _FakeResponse:
        return _FakeResponse(data=[{"id": "test-uuid", "name": "Test Show"}])


def test_insert_show_converts_array_columns_to_pg_literals() -> None:
    client = _RecordingClient()
    show = ShowUpsert(
        name="Test Show",
        genres=["Reality TV"],
        listed_on=["imdb", "tmdb"],
        networks=["Bravo", "Peacock"],
    )

    result = insert_show(cast(DbSession, client), show)

    assert result["name"] == "Test Show"
    assert client.insert_payload is not None
    assert client.insert_payload["genres"] == '{"Reality TV"}'
    assert client.insert_payload["listed_on"] == '{"imdb","tmdb"}'
    assert client.insert_payload["networks"] == '{"Bravo","Peacock"}'
