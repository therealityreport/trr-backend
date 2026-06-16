"""Tests for the lightweight DbSession helpers."""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from trr_backend.db import session


class _FakeCursor:
    def __init__(self, executed: list[tuple[str, list[object] | None]]) -> None:
        self._executed = executed
        self._last_sql = ""

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def execute(self, sql: str, params: list[object] | None = None) -> None:
        self._last_sql = sql
        self._executed.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        if "rpc_result" in self._last_sql:
            return [{"rpc_result": 7}]
        return [{"id": 1}]

    def fetchone(self) -> tuple[int]:
        return (4,)


class _FakeConnection:
    def __init__(self, executed: list[tuple[str, list[object] | None]]) -> None:
        self._executed = executed

    def cursor(self, cursor_factory=None):  # noqa: ANN001, ARG002
        return _FakeCursor(self._executed)


def _checkout_counter(counter: dict[str, int], key: str, executed: list[tuple[str, list[object] | None]]):
    @contextmanager
    def _manager():
        counter[key] += 1
        yield _FakeConnection(executed)

    return _manager


def test_select_uses_read_checkout_and_exact_count_single_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    counter = {"read": 0, "write": 0}
    executed: list[tuple[str, list[object] | None]] = []
    monkeypatch.setattr(session, "db_read_connection", _checkout_counter(counter, "read", executed))
    monkeypatch.setattr(session, "db_connection", _checkout_counter(counter, "write", executed))

    response = session.DbSession().schema("public").table("shows").select("id", count="exact").execute()

    assert counter == {"read": 1, "write": 0}
    assert response.data == [{"id": 1}]
    assert response.count == 4
    assert len(executed) == 2
    assert executed[0][0] == "SELECT id FROM public.shows"
    assert executed[1][0] == "SELECT COUNT(*) FROM public.shows"


def test_rpc_uses_read_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    counter = {"read": 0, "write": 0}
    executed: list[tuple[str, list[object] | None]] = []
    monkeypatch.setattr(session, "db_read_connection", _checkout_counter(counter, "read", executed))
    monkeypatch.setattr(session, "db_connection", _checkout_counter(counter, "write", executed))

    response = session.DbSession().schema("public").rpc("rpc_result", {"show_id": 9}).execute()

    assert counter == {"read": 1, "write": 0}
    assert response.data == [{"rpc_result": 7}]
    assert executed == [("SELECT * FROM public.rpc_result(show_id := %s)", [9])]


def test_insert_uses_write_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    counter = {"read": 0, "write": 0}
    executed: list[tuple[str, list[object] | None]] = []
    monkeypatch.setattr(session, "db_read_connection", _checkout_counter(counter, "read", executed))
    monkeypatch.setattr(session, "db_connection", _checkout_counter(counter, "write", executed))

    def _fake_execute_values(cur: _FakeCursor, sql: str, values: list[list[object]]) -> None:
        cur.execute(sql, [values])

    monkeypatch.setattr(session, "execute_values", _fake_execute_values)

    response = session.DbSession().schema("public").table("shows").insert({"id": 1}).execute()

    assert counter == {"read": 0, "write": 1}
    assert response.data == [{"id": 1}]
    assert executed == [("INSERT INTO public.shows (id) VALUES %s RETURNING *", [[[1]]])]


def test_or_filter_supports_null_now_and_not_ilike(monkeypatch: pytest.MonkeyPatch) -> None:
    counter = {"read": 0, "write": 0}
    executed: list[tuple[str, list[object] | None]] = []
    monkeypatch.setattr(session, "db_read_connection", _checkout_counter(counter, "read", executed))
    monkeypatch.setattr(session, "db_connection", _checkout_counter(counter, "write", executed))

    response = (
        session.DbSession()
        .schema("core")
        .table("cast_photos")
        .select("*")
        .or_("hosted_url.is.null,ingest_next_retry_at.lt.now(),hosted_content_type.not.ilike.image/%")
        .execute()
    )

    assert counter == {"read": 1, "write": 0}
    assert response.data == [{"id": 1}]
    assert executed == [
        (
            "SELECT * FROM core.cast_photos WHERE "
            "(hosted_url IS NULL OR ingest_next_retry_at < NOW() OR NOT (hosted_content_type ILIKE %s))",
            ["image/%"],
        )
    ]


def test_json_text_in_filter_validates_and_parameterizes_json_key(monkeypatch: pytest.MonkeyPatch) -> None:
    counter = {"read": 0, "write": 0}
    executed: list[tuple[str, list[object] | None]] = []
    monkeypatch.setattr(session, "db_read_connection", _checkout_counter(counter, "read", executed))
    monkeypatch.setattr(session, "db_connection", _checkout_counter(counter, "write", executed))

    response = (
        session.DbSession()
        .schema("core")
        .table("people")
        .select("id,external_ids")
        .json_text_in("external_ids", "imdb", ["nm1", "nm2"])
        .execute()
    )

    assert counter == {"read": 1, "write": 0}
    assert response.data == [{"id": 1}]
    assert executed == [
        ("SELECT id,external_ids FROM core.people WHERE external_ids ->> %s IN (%s,%s)", ["imdb", "nm1", "nm2"])
    ]


@pytest.mark.parametrize(
    ("builder", "expected"),
    [
        (lambda: session.DbSession().schema("bad-name"), "Invalid SQL identifier: bad-name"),
        (
            lambda: session.DbSession().schema("public").table("shows;drop"),
            "Invalid SQL identifier: shows;drop",
        ),
        (
            lambda: session.DbSession().schema("public").table("shows").eq("id desc", 1),
            "Invalid SQL identifier: id desc",
        ),
        (
            lambda: session.DbSession().schema("public").table("shows").order("created_at;drop"),
            "Invalid SQL identifier: created_at;drop",
        ),
        (
            lambda: session.DbSession().schema("public").rpc("reload-cache", {}),
            "Invalid SQL identifier: reload-cache",
        ),
        (
            lambda: session.DbSession().schema("public").rpc("reload_cache", {"bad-key": 1}),
            "Invalid SQL identifier: bad-key",
        ),
        (
            lambda: session.DbSession().schema("public").table("shows").or_("id.drop.table.users"),
            "Unsupported OR expression operator: drop",
        ),
    ],
)
def test_dynamic_identifiers_must_be_safe(builder, expected: str) -> None:  # noqa: ANN001
    with pytest.raises(ValueError, match=expected):
        builder()
