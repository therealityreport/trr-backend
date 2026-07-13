from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from trr_backend.socials.threads.posts_scrapling.persistence import persist_threads_posts


class _FakeCursor:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, statement: str) -> None:
        self._conn.statements.append(statement)


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


def test_persist_threads_posts_uses_canonical_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield _FakeConnection()

    monkeypatch.setattr(pg, "db_connection", _fake_conn)
    monkeypatch.setattr(repo, "get_season_context", lambda _season_id: None)
    monkeypatch.setattr(
        repo,
        "_upsert_meta_threads_post",
        lambda _context, *, job_id, account, post, conn: {
            "id": f"{account}:{getattr(post, 'post_id', '')}:{job_id}:{bool(conn)}"
        },
    )

    result = persist_threads_posts(
        account_handle="bravotv",
        posts=[SimpleNamespace(post_id="th-1", to_dict=lambda: {"post_id": "th-1"})],
        run_id=None,
        job_id="job-1",
        season_id=None,
    )

    assert result.posts_upserted == 1
    assert result.posts_skipped == 0
    assert result.posts_skipped_by_reason == {}


def test_persist_threads_posts_counts_skips_by_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield _FakeConnection()

    def _fake_upsert(_context, *, job_id, account, post, conn):
        del _context, job_id, account, conn
        post_id = str(getattr(post, "post_id", "") or "").strip()
        if post_id == "skip-none":
            return None
        if post_id == "explode":
            raise RuntimeError("upsert exploded")
        return {"id": post_id}

    monkeypatch.setattr(pg, "db_connection", _fake_conn)
    monkeypatch.setattr(repo, "get_season_context", lambda _season_id: None)
    monkeypatch.setattr(repo, "_upsert_meta_threads_post", _fake_upsert)

    result = persist_threads_posts(
        account_handle="bravotv",
        posts=[
            SimpleNamespace(post_id="ok", to_dict=lambda: {"post_id": "ok"}),
            SimpleNamespace(post_id="", to_dict=lambda: {}),
            SimpleNamespace(post_id="skip-none", to_dict=lambda: {"post_id": "skip-none"}),
            SimpleNamespace(post_id="explode", to_dict=lambda: {"post_id": "explode"}),
        ],
        run_id=None,
        job_id="job-1",
        season_id=None,
    )

    assert result.posts_upserted == 1
    assert result.posts_skipped == 3
    assert result.posts_skipped_by_reason == {
        "missing_post_id": 1,
        "canonical_upsert_returned_none": 1,
        "upsert_failed": 1,
    }


def test_persist_threads_posts_shared_catalog_mode_writes_catalog_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    conn = _FakeConnection()
    catalog_calls: list[dict[str, object]] = []

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield conn

    monkeypatch.setattr(pg, "db_connection", _fake_conn)
    monkeypatch.setattr(repo, "get_season_context", lambda _season_id: None)
    monkeypatch.setattr(repo, "_upsert_meta_threads_post", lambda *_args, **_kwargs: {"id": "materialized"})

    def _fake_catalog_upsert(**kwargs):
        catalog_calls.append(kwargs)
        return {"id": "catalog"}

    monkeypatch.setattr(repo, "_upsert_shared_catalog_post", _fake_catalog_upsert)

    result = persist_threads_posts(
        account_handle="bravotv",
        posts=[SimpleNamespace(post_id="th-1", to_dict=lambda: {"post_id": "th-1"})],
        run_id="run-1",
        job_id="job-1",
        season_id=None,
        pipeline_ingest_mode="shared_account_catalog_backfill",
    )

    assert result.posts_upserted == 1
    assert result.catalog_posts_upserted == 1
    assert catalog_calls[0]["platform"] == "threads"
    assert catalog_calls[0]["run_id"] == "run-1"
    assert catalog_calls[0]["account_handle"] == "bravotv"
    assert conn.statements == [
        "SAVEPOINT threads_posts_scrapling_post_1",
        "RELEASE SAVEPOINT threads_posts_scrapling_post_1",
    ]


def test_persist_threads_posts_uses_savepoint_after_per_post_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    conn = _FakeConnection()
    upserted_post_ids: list[str] = []

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield conn

    def _fake_upsert(_context, *, job_id, account, post, conn):
        del _context, job_id, account, conn
        if post.post_id == "explode":
            raise RuntimeError("post failed")
        upserted_post_ids.append(post.post_id)
        return {"id": post.post_id}

    monkeypatch.setattr(pg, "db_connection", _fake_conn)
    monkeypatch.setattr(repo, "get_season_context", lambda _season_id: None)
    monkeypatch.setattr(repo, "_upsert_meta_threads_post", _fake_upsert)

    result = persist_threads_posts(
        account_handle="bravotv",
        posts=[
            SimpleNamespace(post_id="explode", to_dict=lambda: {"post_id": "explode"}),
            SimpleNamespace(post_id="ok", to_dict=lambda: {"post_id": "ok"}),
        ],
        run_id=None,
        job_id="job-1",
        season_id=None,
    )

    assert upserted_post_ids == ["ok"]
    assert result.posts_upserted == 1
    assert result.posts_skipped == 1
    assert result.posts_skipped_by_reason == {"upsert_failed": 1}
    assert conn.statements == [
        "SAVEPOINT threads_posts_scrapling_post_1",
        "ROLLBACK TO SAVEPOINT threads_posts_scrapling_post_1",
        "RELEASE SAVEPOINT threads_posts_scrapling_post_1",
        "SAVEPOINT threads_posts_scrapling_post_2",
        "RELEASE SAVEPOINT threads_posts_scrapling_post_2",
    ]


def test_persist_threads_posts_tracks_required_shared_catalog_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield _FakeConnection()

    monkeypatch.setattr(pg, "db_connection", _fake_conn)
    monkeypatch.setattr(repo, "get_season_context", lambda _season_id: None)
    monkeypatch.setattr(repo, "_upsert_meta_threads_post", lambda *_args, **_kwargs: {"id": "materialized"})

    def _catalog_upsert(*, post, **_kwargs):
        if post.post_id == "bad":
            raise RuntimeError("catalog failed")
        return {"id": "catalog"}

    monkeypatch.setattr(repo, "_upsert_shared_catalog_post", _catalog_upsert)
    result = persist_threads_posts(
        account_handle="bravotv",
        posts=[
            SimpleNamespace(post_id="good", to_dict=lambda: {"post_id": "good"}),
            SimpleNamespace(post_id="bad", to_dict=lambda: {"post_id": "bad"}),
        ],
        run_id="run-1",
        job_id="job-1",
        pipeline_ingest_mode="shared_account_catalog_backfill",
    )

    assert result.posts_upserted == 1
    assert result.catalog_posts_upserted == 1
    assert result.required_shared_persistence_failures == 1
