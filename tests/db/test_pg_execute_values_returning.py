from __future__ import annotations

from contextlib import contextmanager
from typing import Any, cast

from trr_backend.db import pg


class _FakeCursor:
    def fetchall(self) -> list[dict[str, Any]]:
        return [{"id": f"last-page-{index}"} for index in range(100)]


def test_execute_values_returning_aggregates_execute_values_fetch(monkeypatch) -> None:
    fetched_rows = [{"id": f"row-{index}"} for index in range(250)]
    fake_cursor = _FakeCursor()
    calls: list[dict[str, Any]] = []

    @contextmanager
    def _fake_db_cursor(**_kwargs):
        yield fake_cursor

    def _fake_execute_values(cur, query, rows, *, fetch=False):  # noqa: ANN001
        calls.append(
            {
                "cursor": cur,
                "query": query,
                "row_count": len(rows),
                "fetch": fetch,
            }
        )
        return fetched_rows if fetch else None

    monkeypatch.setattr(pg, "db_cursor", _fake_db_cursor)
    monkeypatch.setattr(pg, "execute_values", _fake_execute_values)

    result = pg.execute_values_returning(
        "insert into social.test_table (id) values %s returning id",
        [(index,) for index in range(250)],
        conn=cast(Any, object()),
    )

    assert result == fetched_rows
    assert calls == [
        {
            "cursor": fake_cursor,
            "query": "insert into social.test_table (id) values %s returning id",
            "row_count": 250,
            "fetch": True,
        }
    ]
