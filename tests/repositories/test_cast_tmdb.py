from __future__ import annotations

from typing import Any, cast
from uuid import UUID

import pytest

from trr_backend.db.session import DbSession
from trr_backend.repositories.cast_tmdb import CastTMDbRepositoryError, upsert_cast_tmdb


class _FakeResponse:
    def __init__(self, *, data: list[dict[str, Any]] | None = None, error: Any = None) -> None:
        self.data = data or []
        self.error = error


class _FakeTable:
    def __init__(self, *, response: _FakeResponse | None = None, raise_exc: Exception | None = None) -> None:
        self.response = response or _FakeResponse(data=[{"id": "row-1"}])
        self.raise_exc = raise_exc
        self.upsert_payload: dict[str, Any] | None = None
        self.upsert_on_conflict: str | None = None

    def upsert(self, payload: dict[str, Any], *, on_conflict: str):
        self.upsert_payload = payload
        self.upsert_on_conflict = on_conflict
        return self

    def execute(self) -> _FakeResponse:
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.response


class _FakeDb:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table

    def schema(self, _name: str):
        return self

    def table(self, _name: str) -> _FakeTable:
        return self._table


@pytest.mark.parametrize(
    "raw_value,expected",
    [
        ([" Alan Cumming ", "", None, "Darren Criss"], ["Alan Cumming", "Darren Criss"]),
        ('["Alan Cumming", " Darren Criss ", ""]', ["Alan Cumming", "Darren Criss"]),
        (" Alan Cumming ", ["Alan Cumming"]),
        (None, []),
    ],
)
def test_upsert_cast_tmdb_normalizes_also_known_as(raw_value: Any, expected: list[str]) -> None:
    table = _FakeTable()
    db = _FakeDb(table)

    row = {
        "person_id": UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        "tmdb_id": 123,
        "also_known_as": raw_value,
    }

    result = upsert_cast_tmdb(cast(DbSession, db), row)

    assert result == {"id": "row-1"}
    assert table.upsert_on_conflict == "person_id"
    assert isinstance(table.upsert_payload, dict)
    assert table.upsert_payload is not None
    assert table.upsert_payload["person_id"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert table.upsert_payload["also_known_as"] == expected
    assert isinstance(table.upsert_payload["also_known_as"], list)


def test_upsert_cast_tmdb_adds_context_to_upsert_errors() -> None:
    table = _FakeTable(raise_exc=RuntimeError("boom"))
    db = _FakeDb(table)

    row = {
        "person_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "tmdb_id": 123,
        "also_known_as": '["Alan Cummings"]',
    }

    with pytest.raises(CastTMDbRepositoryError) as exc_info:
        upsert_cast_tmdb(cast(DbSession, db), row)

    message = str(exc_info.value)
    assert "Supabase error upserting cast_tmdb" in message
    assert "field=also_known_as" in message
    assert "type=list" in message
    assert "Alan Cummings" in message


def test_upsert_cast_tmdb_adds_context_to_response_error() -> None:
    table = _FakeTable(response=_FakeResponse(error="bad request"))
    db = _FakeDb(table)

    row = {
        "person_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "tmdb_id": 123,
        "also_known_as": "Alan Cummings",
    }

    with pytest.raises(CastTMDbRepositoryError) as exc_info:
        upsert_cast_tmdb(cast(DbSession, db), row)

    message = str(exc_info.value)
    assert "Supabase error upserting cast_tmdb" in message
    assert "field=also_known_as" in message
    assert "type=list" in message
    assert "Alan Cummings" in message
