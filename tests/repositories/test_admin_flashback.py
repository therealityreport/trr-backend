from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from trr_backend.repositories import admin_flashback as flashback_repo

QUIZ_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
EVENT_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def test_quiz_and_event_read_write_queries_are_schema_qualified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, object]] = []

    def fetch_all(sql, params=None, **_kwargs):
        calls.append(("fetch_all", sql, params))
        return []

    def execute_returning(sql, params=None, **_kwargs):
        calls.append(("execute_returning", sql, params))
        return [
            {
                "id": QUIZ_ID,
                "title": "Bravo Beginnings",
                "publish_date": "2026-03-30",
                "description": None,
                "is_published": False,
                "created_at": "2026-03-30T12:00:00Z",
                "updated_at": "2026-03-30T12:00:00Z",
            }
        ]

    monkeypatch.setattr(flashback_repo.pg, "fetch_all", fetch_all)
    monkeypatch.setattr(flashback_repo.pg, "execute_returning", execute_returning)

    quizzes, list_count = flashback_repo.list_quizzes()
    quiz, create_count = flashback_repo.create_quiz(
        title="Bravo Beginnings",
        publish_date="2026-03-30",
        description=None,
    )
    updated, update_count = flashback_repo.set_quiz_published(
        quiz_id=QUIZ_ID,
        is_published=True,
    )
    events, event_count = flashback_repo.list_events(quiz_id=QUIZ_ID)

    assert quizzes == []
    assert events == []
    assert quiz["id"] == QUIZ_ID
    assert updated is not None
    assert (list_count, create_count, update_count, event_count) == (1, 1, 1, 1)
    assert all("public.flashback_" in sql for _, sql, _ in calls)
    assert "ORDER BY publish_date DESC" in calls[0][1]
    assert "is_published = %s" in calls[2][1]
    assert calls[2][2] == [True, QUIZ_ID]


def test_create_event_locks_parent_before_allocating_sort_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, Any, object]] = []
    connection = object()

    @contextmanager
    def db_connection(*, label):
        calls.append(("connection", label, None, connection))
        yield connection

    responses = iter([{"id": QUIZ_ID}, {"max_sort_order": 3}])

    def fetch_one(sql, params=None, *, conn=None, **_kwargs):
        calls.append(("fetch_one", sql, params, conn))
        return next(responses)

    def execute_returning(sql, params=None, *, conn=None, **_kwargs):
        calls.append(("execute_returning", sql, params, conn))
        return [
            {
                "id": EVENT_ID,
                "quiz_id": QUIZ_ID,
                "description": "The table flip",
                "image_url": None,
                "year": 2009,
                "sort_order": 4,
                "point_value": 5,
            }
        ]

    monkeypatch.setattr(flashback_repo.pg, "db_connection", db_connection)
    monkeypatch.setattr(flashback_repo.pg, "fetch_one", fetch_one)
    monkeypatch.setattr(flashback_repo.pg, "execute_returning", execute_returning)

    event, query_count = flashback_repo.create_event(
        quiz_id=QUIZ_ID,
        description="The table flip",
        year=2009,
        image_url=None,
        point_value=5,
    )

    assert event is not None
    assert event["sort_order"] == 4
    assert query_count == 3
    assert "FOR UPDATE" in calls[1][1]
    assert "MAX(sort_order)" in calls[2][1]
    assert calls[3][2][-1] == 4
    assert all(call[3] is connection for call in calls[1:])


def test_create_event_returns_none_when_parent_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = object()

    @contextmanager
    def db_connection(*, label):
        assert label == "create-admin-flashback-event"
        yield connection

    monkeypatch.setattr(flashback_repo.pg, "db_connection", db_connection)
    monkeypatch.setattr(flashback_repo.pg, "fetch_one", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        flashback_repo.pg,
        "execute_returning",
        lambda *_args, **_kwargs: pytest.fail("insert should not run"),
    )

    event, query_count = flashback_repo.create_event(
        quiz_id=QUIZ_ID,
        description="A moment",
        year=2010,
        image_url=None,
        point_value=2,
    )

    assert event is None
    assert query_count == 1


def test_delete_event_locks_parent_then_event_before_delete_and_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, object, object]] = []
    connection = object()

    @contextmanager
    def db_connection(*, label):
        calls.append(("connection", label, None, connection))
        yield connection

    responses = iter([{"quiz_id": QUIZ_ID}, {"quiz_id": QUIZ_ID, "sort_order": 2}])

    def fetch_one(sql, params=None, *, conn=None, **_kwargs):
        calls.append(("fetch_one", sql, params, conn))
        return next(responses)

    def execute(sql, params=None, *, conn=None, **_kwargs):
        calls.append(("execute", sql, params, conn))

    monkeypatch.setattr(flashback_repo.pg, "db_connection", db_connection)
    monkeypatch.setattr(flashback_repo.pg, "fetch_one", fetch_one)
    monkeypatch.setattr(flashback_repo.pg, "execute", execute)

    deleted, query_count = flashback_repo.delete_event(event_id=EVENT_ID)

    assert deleted is True
    assert query_count == 4
    assert "FOR UPDATE OF quiz" in calls[1][1]
    assert "FOR UPDATE" in calls[2][1]
    assert "DELETE FROM public.flashback_events" in calls[3][1]
    assert "sort_order = sort_order - 1" in calls[4][1]
    assert calls[4][2] == [QUIZ_ID, 2]
    assert all(call[3] is connection for call in calls[1:])


def test_delete_event_returns_false_before_mutation_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = object()

    @contextmanager
    def db_connection(*, label):
        assert label == "delete-admin-flashback-event"
        yield connection

    monkeypatch.setattr(flashback_repo.pg, "db_connection", db_connection)
    monkeypatch.setattr(flashback_repo.pg, "fetch_one", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        flashback_repo.pg,
        "execute",
        lambda *_args, **_kwargs: pytest.fail("mutation should not run"),
    )

    deleted, query_count = flashback_repo.delete_event(event_id=EVENT_ID)

    assert deleted is False
    assert query_count == 1
