from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime

import pytest

from trr_backend.repositories import season_cast_survey_roles as roles_repo

SHOW_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PERSON_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
ROLE_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"


def _role(person_id: str = PERSON_ID, role: str = "main") -> dict[str, object]:
    return {
        "id": ROLE_ID,
        "trr_show_id": SHOW_ID,
        "season_number": 3,
        "person_id": person_id,
        "role": role,
        "created_at": datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    }


def test_list_roles_uses_the_existing_show_season_key_and_stable_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_fetch(query, params):
        captured.update(query=query, params=params)
        return [_role()]

    monkeypatch.setattr(roles_repo.pg, "fetch_all", fake_fetch)

    rows, query_count = roles_repo.list_roles(show_id=SHOW_ID, season_number=3)

    assert rows == [_role()]
    assert query_count == 1
    assert captured["params"] == [SHOW_ID, 3]
    assert "ORDER BY role ASC, created_at ASC, id ASC" in str(captured["query"])
    assert "core.seasons" not in str(captured["query"])


def test_upsert_and_delete_are_bounded_to_show_season_person(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_execute(query, params):
        calls.append((query, params))
        return [_role()] if query.lstrip().startswith("INSERT") else [{"id": ROLE_ID}]

    monkeypatch.setattr(roles_repo.pg, "execute_returning", fake_execute)

    row, upsert_queries = roles_repo.upsert_role(
        show_id=SHOW_ID,
        season_number=3,
        person_id=PERSON_ID,
        role="main",
    )
    removed, delete_queries = roles_repo.delete_role(
        show_id=SHOW_ID,
        season_number=3,
        person_id=PERSON_ID,
    )

    assert row == _role()
    assert removed is True
    assert upsert_queries == delete_queries == 1
    assert calls[0][1] == [SHOW_ID, 3, PERSON_ID, "main"]
    assert calls[1][1] == [SHOW_ID, 3, PERSON_ID]
    assert "ON CONFLICT (trr_show_id, season_number, person_id)" in calls[0][0]


def test_replace_is_one_transaction_and_preserves_an_empty_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = object()
    deleted: list[tuple[list[object], object]] = []
    inserted: list[tuple[list[tuple[object, ...]], object]] = []

    @contextmanager
    def fake_connection(*, label):
        assert label == "replace-season-cast-survey-roles"
        yield connection

    def fake_delete(_query, params, *, conn):
        deleted.append((params, conn))

    def fake_insert(_query, rows, *, conn):
        inserted.append((rows, conn))
        return [_role()]

    monkeypatch.setattr(roles_repo.pg, "db_connection", fake_connection)
    monkeypatch.setattr(roles_repo.pg, "execute", fake_delete)
    monkeypatch.setattr(roles_repo.pg, "execute_values_returning", fake_insert)

    empty, empty_queries = roles_repo.replace_roles(show_id=SHOW_ID, season_number=3, roles=[])
    rows, query_count = roles_repo.replace_roles(
        show_id=SHOW_ID,
        season_number=3,
        roles=[(PERSON_ID, "main")],
    )

    assert empty == []
    assert empty_queries == 1
    assert rows == [_role()]
    assert query_count == 2
    assert deleted == [([SHOW_ID, 3], connection), ([SHOW_ID, 3], connection)]
    assert inserted == [([(SHOW_ID, 3, PERSON_ID, "main")], connection)]
