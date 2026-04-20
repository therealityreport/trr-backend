from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.threads.posts_scrapling.persistence import PersistedThreadsPosts


def test_threads_job_runner_uses_final_fetcher_metadata_not_warmup_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []

    def fake_finish_job(job_id: str, *, status: str, metadata: dict[str, Any], **_kwargs: Any) -> None:
        finish_calls.append({"job_id": job_id, "status": status, "metadata": metadata})

    class _FakeFetcher:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs
            self._request_count = 0

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "request_count": self._request_count,
                "transport": "httpx_after_browser_warmup",
                "fallback_chain": ["scrapling_warmup", "graphql_profile_posts"],
                "stop_reason": "complete",
                "retryable": False,
                "complete": True,
            }

        async def warmup(self, username: str) -> None:
            del username
            self._request_count = 1

        async def fetch_posts(self, username: str, *, max_pages: int | None = None) -> Any:
            del username, max_pages
            self._request_count = 4
            return jr.ThreadsPostsFetchResult(
                posts=[
                    SimpleNamespace(
                        post_id="th-1",
                        username="bravotv",
                        text="hello",
                        media_urls=[],
                        thumbnail_url=None,
                        likes=1,
                        replies=0,
                        reposts=0,
                        quotes=0,
                        views=10,
                        posted_at=None,
                        url="https://www.threads.com/@bravotv/post/abc",
                        to_dict=lambda: {"post_id": "th-1"},
                    )
                ],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                fetch_reason=None,
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        jr,
        "resolve_threads_posts_session",
        lambda: SimpleNamespace(
            raw_cookies={"sessionid": "abc", "csrftoken": "csrf"},
            cookies=[{"name": "sessionid", "value": "abc", "domain": ".threads.com", "path": "/"}],
            cookie_source="canonical",
        ),
    )
    monkeypatch.setattr(jr, "select_threads_posts_proxy", lambda: None)
    monkeypatch.setattr(jr, "ThreadsPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(
        jr,
        "persist_threads_posts",
        lambda **_kwargs: PersistedThreadsPosts(posts_upserted=1, posts_skipped=0),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"status": "completed"})

    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", fake_finish_job)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-16T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 4, 16, tzinfo=UTC))
    monkeypatch.setattr(repo, "_retry_backoff_seconds", lambda _attempt: 10)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda _run_id: {})

    job = {
        "id": "00000000-0000-0000-0000-000000000101",
        "run_id": "00000000-0000-0000-0000-000000000201",
        "platform": "threads",
        "config": {"stage": "threads_posts_scrapling", "account": "bravotv"},
        "metadata": {},
        "attempt_count": 1,
        "max_attempts": 1,
    }

    jr.run_threads_posts_scrapling_job(job, worker_id="test-worker")

    metadata = finish_calls[-1]["metadata"]
    assert metadata["fetch_counters"]["request_count"] == 4
    assert metadata["fetcher_runtime"]["request_count"] == 4
    assert metadata["source_runtime"]["request_count"] == 4
