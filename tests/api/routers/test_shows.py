from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import uuid4

from fastapi.testclient import TestClient

from api import deps
from api.main import app


@dataclass
class _FakeResponse:
    data: object
    error: object = None


class _FakeQuery:
    def __init__(self, dataset: dict[str, list[dict]], table: str):
        self._dataset = dataset
        self._table = table
        self._order: tuple[str, bool] | None = None
        self._range: tuple[int, int] | None = None
        self._single = False
        self._eq_filters: list[tuple[str, object]] = []
        self._in_filters: list[tuple[str, list[object]]] = []

    def select(self, _columns: str = "*", count: str | None = None) -> _FakeQuery:
        return self

    def eq(self, column: str, value: object) -> _FakeQuery:
        self._eq_filters.append((column, value))
        return self

    def in_(self, column: str, values: list[object]) -> _FakeQuery:
        self._in_filters.append((column, list(values)))
        return self

    def order(self, column: str, *, desc: bool = False, nullsfirst: bool | None = None) -> _FakeQuery:
        self._order = (column, desc)
        return self

    def range(self, start: int, end: int) -> _FakeQuery:
        self._range = (start, end)
        return self

    def single(self) -> _FakeQuery:
        self._single = True
        return self

    def execute(self) -> _FakeResponse:
        rows = [dict(row) for row in self._dataset.get(self._table, [])]
        for column, value in self._eq_filters:
            rows = [row for row in rows if str(row.get(column)) == str(value)]
        for column, values in self._in_filters:
            allowed = {str(value) for value in values}
            rows = [row for row in rows if str(row.get(column)) in allowed]
        if self._order is not None:
            column, desc = self._order
            rows.sort(key=lambda row: row.get(column), reverse=desc)
        if self._range is not None:
            start, end = self._range
            rows = rows[start : end + 1]
        if self._single:
            return _FakeResponse(rows[0] if rows else None)
        return _FakeResponse(rows)


class _FakeSchema:
    def __init__(self, dataset: dict[str, list[dict]]):
        self._dataset = dataset

    def table(self, table: str) -> _FakeQuery:
        return _FakeQuery(self._dataset, table)


class _FakeDb:
    def __init__(self, dataset: dict[str, list[dict]]):
        self._dataset = dataset

    def schema(self, name: str) -> _FakeSchema:
        assert name == "core"
        return _FakeSchema(self._dataset)


def _client_with_dataset(dataset: dict[str, list[dict]]) -> TestClient:
    fake_db = _FakeDb(dataset)
    app.dependency_overrides[deps.get_supabase_client] = lambda: fake_db
    app.dependency_overrides[deps.get_supabase_admin_client] = lambda: fake_db
    return TestClient(app)


def test_list_seasons_serializes_premiere_date() -> None:
    show_id = uuid4()
    dataset = {
        "seasons": [
            {
                "id": str(uuid4()),
                "show_id": str(show_id),
                "season_number": 1,
                "title": "Season 1",
                "premiere_date": date(2024, 1, 15),
                "external_ids": {},
            }
        ]
    }

    client = _client_with_dataset(dataset)
    try:
        response = client.get(f"/api/v1/shows/{show_id}/seasons")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()[0]["premiere_date"] == "2024-01-15"


def test_get_show_uses_watch_provider_lookup_without_nested_selects() -> None:
    show_id = uuid4()
    dataset = {
        "shows": [
            {
                "id": str(show_id),
                "name": "Sample Show",
                "description": "desc",
                "premiere_date": date(2023, 9, 1),
                "tmdb_id": 99,
                "imdb_id": "tt1234567",
                "tmdb_network_ids": [],
                "tmdb_production_company_ids": [],
            }
        ],
        "show_watch_providers": [
            {
                "show_id": str(show_id),
                "region": "US",
                "offer_type": "flatrate",
                "display_priority": 1,
                "link": "https://example.com/watch",
                "provider_id": 8,
            }
        ],
        "watch_providers": [
            {
                "provider_id": 8,
                "provider_name": "Netflix",
                "display_priority": 1,
                "tmdb_logo_path": "/logo.png",
                "logo_path": "logos/netflix.png",
                "hosted_logo_key": "logos/netflix.png",
                "hosted_logo_url": "https://cdn.example/logos/netflix.png",
                "hosted_logo_sha256": "abc",
                "hosted_logo_content_type": "image/png",
                "hosted_logo_bytes": 1234,
                "hosted_logo_etag": "etag",
                "hosted_logo_at": datetime(2024, 3, 1, 12, 0, tzinfo=UTC),
            }
        ],
    }

    client = _client_with_dataset(dataset)
    try:
        response = client.get(f"/api/v1/shows/{show_id}")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["premiere_date"] == "2023-09-01"
    assert payload["external_ids"]["tmdb_id"] == 99
    assert payload["watch_providers"] == [
        {
            "region": "US",
            "offer_type": "flatrate",
            "link": "https://example.com/watch",
            "providers": [
                {
                    "provider_id": 8,
                    "provider_name": "Netflix",
                    "display_priority": 1,
                    "tmdb_logo_path": "/logo.png",
                    "logo_path": "logos/netflix.png",
                    "hosted_logo_key": "logos/netflix.png",
                    "hosted_logo_url": "https://cdn.example/logos/netflix.png",
                    "hosted_logo_sha256": "abc",
                    "hosted_logo_content_type": "image/png",
                    "hosted_logo_bytes": 1234,
                    "hosted_logo_etag": "etag",
                    "hosted_logo_at": "2024-03-01T12:00:00+00:00",
                }
            ],
        }
    ]
