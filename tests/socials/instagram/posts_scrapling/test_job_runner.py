from __future__ import annotations

from types import SimpleNamespace

import pytest


def test_job_runner_rejects_missing_account():
    from trr_backend.socials.instagram.posts_scrapling.job_runner import (
        PostsScraplingRuntimeError,
        run_instagram_posts_scrapling_job,
    )

    job = {"id": "job-1", "run_id": "run-1", "config": {"account": ""}}
    with pytest.raises(PostsScraplingRuntimeError, match="missing an account"):
        run_instagram_posts_scrapling_job(job)


def test_job_runner_emits_post_skip_truthfulness_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.job_runner import run_instagram_posts_scrapling_job
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    captured_finish: dict[str, object] = {}

    class _FakeFetcher:
        runtime_metadata = {"request_count": 1}

        def __init__(self, **_kwargs) -> None:
            pass

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, cursor=None):  # noqa: ANN001
            del cursor
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"shortcode": "abc123"}],
                has_next_page=False,
                end_cursor=None,
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.select_posts_proxy",
        lambda: None,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.InstagramPostsScraplingFetcher",
        _FakeFetcher,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.persist_instagram_posts",
        lambda **_kwargs: PersistedInstagramPosts(
            posts_upserted=0,
            posts_skipped=1,
            posts_skipped_by_reason={"canonical_upsert_returned_none": 1},
        ),
    )
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-21T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: {})

    def _fake_finish_job(job_id, *, status, items_found, error_message=None, metadata=None, **_kwargs):  # noqa: ANN001
        captured_finish["job_id"] = job_id
        captured_finish["status"] = status
        captured_finish["items_found"] = items_found
        captured_finish["error_message"] = error_message
        captured_finish["metadata"] = metadata

    monkeypatch.setattr(repo, "_finish_job", _fake_finish_job)
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.pg.fetch_one",
        lambda *_args, **_kwargs: {},
    )

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {"account": "thetraitorsus", "stage": "posts_scrapling"},
    }
    run_instagram_posts_scrapling_job(job, worker_id="worker-1")

    metadata = dict(captured_finish["metadata"] or {})
    assert metadata["persist_counters"] == {
        "posts_upserted": 0,
        "posts_skipped": 1,
        "posts_skipped_by_reason": {"canonical_upsert_returned_none": 1},
    }
    assert metadata["posts_scrapling_persist_diagnostics"] == {
        "posts_upserted": 0,
        "posts_skipped": 1,
        "posts_skipped_by_reason": {"canonical_upsert_returned_none": 1},
    }
