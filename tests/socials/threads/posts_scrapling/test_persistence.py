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
