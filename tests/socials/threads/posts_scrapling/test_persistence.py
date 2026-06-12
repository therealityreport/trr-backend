from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from trr_backend.socials.threads.posts_scrapling.persistence import persist_threads_posts


def test_persist_threads_posts_uses_canonical_upsert(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield object()

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
        yield object()

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

    catalog_calls: list[dict[str, object]] = []

    @contextmanager
    def _fake_conn(*, label: str | None = None):
        del label
        yield object()

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
