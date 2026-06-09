from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsFetchResult
from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments
from trr_backend.socials.instagram.scraper import InstagramComment


def _session() -> SimpleNamespace:
    return SimpleNamespace(
        cookies=[],
        browser_account_id="thetraitorsus",
        auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
    )


def _comment(comment_id: str) -> InstagramComment:
    return InstagramComment(
        comment_id=comment_id,
        text="comment",
        username="viewer_account",
        user_id="1",
        created_at=1,
        date_time="1970-01-01 00:00:01",
        likes=0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=0,
        reply_depth=0,
    )


class _FakeLifecycle:
    def __init__(self, finish_calls: list[dict[str, Any]], progress_calls: list[dict[str, Any]]) -> None:
        self._finish_calls = finish_calls
        self._progress_calls = progress_calls

    @staticmethod
    def now_utc() -> datetime:
        return datetime(2026, 5, 28, tzinfo=UTC)

    @staticmethod
    def format_time(_value: Any) -> str:
        return "2026-05-28T00:00:00+00:00"

    @staticmethod
    def new_job_progress_state() -> dict[str, Any]:
        return {}

    @staticmethod
    def touch_job_heartbeat(*_args: Any, **_kwargs: Any) -> bool:
        return True

    def emit_job_progress(self, **kwargs: Any) -> bool:
        self._progress_calls.append(dict(kwargs))
        return True

    def finish_job(self, job_id: str, **kwargs: Any) -> None:
        self._finish_calls.append({"job_id": job_id, **kwargs})

    @staticmethod
    def finalize_run_status(_run_id: str, **_kwargs: Any) -> dict[str, Any]:
        return {}

    @staticmethod
    def metadata_dict(value: Any) -> dict[str, Any]:
        return dict(value or {}) if isinstance(value, dict) else {}

    @staticmethod
    def retry_backoff_seconds(_attempt_count: int) -> int:
        return 1


class _OverlapFetcher:
    def __init__(self, events: list[str], active_samples: list[int], delay_seconds: float) -> None:
        self.events = events
        self.active_samples = active_samples
        self.delay_seconds = delay_seconds
        self.active = 0
        self.runtime_metadata = {"transport": "test", "request_count": 0}

    async def warmup(self) -> None:
        self.events.append("warmup")

    async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
        self.active += 1
        self.runtime_metadata["request_count"] += 1
        self.active_samples.append(self.active)
        self.events.append(f"fetch-start:{shortcode}")
        await asyncio.sleep(self.delay_seconds)
        self.events.append(f"fetch-end:{shortcode}")
        self.active -= 1
        return InstagramCommentsFetchResult(
            comments=[_comment(f"comment-{shortcode}")],
            fetch_failed=False,
            auth_failed=False,
            reported_comment_count=1,
            request_count=int(self.runtime_metadata["request_count"]),
        )

    async def aclose(self) -> None:
        self.events.append("close")


def _patch_runner(
    monkeypatch: pytest.MonkeyPatch,
    jr: Any,
    repo: Any,
    *,
    fetcher: _OverlapFetcher,
    finish_calls: list[dict[str, Any]],
    progress_calls: list[dict[str, Any]],
    events: list[str],
) -> None:
    @contextmanager
    def fake_db_connection(**_kwargs: Any):
        yield SimpleNamespace(commit=lambda: events.append("commit"))

    def fake_fetch_one(sql: str, _params: list[object] | None = None, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(sql.split()).lower()
        if "select metadata from social.scrape_jobs" in normalized:
            return {"metadata": {}}
        if "from social.scrape_jobs" in normalized and "select id::text" in normalized:
            finished = finish_calls[-1] if finish_calls else {"status": "completed", "items_found": 0}
            return {
                "id": "job-1",
                "run_id": "run-1",
                "platform": "instagram",
                "job_type": "comments",
                "status": finished["status"],
                "items_found": finished["items_found"],
                "error_message": None,
                "metadata": dict(finished.get("metadata") or {}),
            }
        if "from social.scrape_jobs" in normalized:
            return {"status": "running", "worker_id": "worker-1", "claimed_at": "2026-05-28T00:00:00+00:00"}
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        return {}

    def fake_persist_instagram_comments_for_post(**kwargs: Any) -> PersistedInstagramComments:
        shortcode = str(kwargs.get("shortcode") or "")
        events.append(f"persist:{shortcode}")
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            stored_parent_comments=1,
        )

    monkeypatch.setattr(jr, "lifecycle", _FakeLifecycle(finish_calls, progress_calls))
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_kwargs: _session())
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_kwargs: fetcher)
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_persisted_replies_by_parent", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_insert_instagram_post_comments_audit", lambda **_kwargs: None)
    monkeypatch.setattr(jr.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(jr.pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist_instagram_comments_for_post)
    monkeypatch.setattr(repo, "_reconcile_post_comment_count", lambda **_kwargs: None)


def _run_job(jr: Any, repo: Any, targets: list[str]) -> dict[str, Any]:
    return jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "status": "running",
            "config": {
                "account": "thetraitorsus",
                "stage": repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                "target_source_ids": targets,
                "fetch_replies": False,
                "comments_cancel_check_every_posts": 1,
            },
        },
        worker_id="worker-1",
    )


def test_comments_per_post_concurrency_env_defaults_and_clamps(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY", raising=False)
    assert jr._resolve_comments_per_post_concurrency() == 1

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY", "0")
    assert jr._resolve_comments_per_post_concurrency() == 1

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY", "9")
    assert jr._resolve_comments_per_post_concurrency() == 8

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY", "garbage")
    assert jr._resolve_comments_per_post_concurrency() == 1


def test_comments_concurrency_one_preserves_serial_fetch_then_persist_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY", "1")
    events: list[str] = []
    active_samples: list[int] = []
    finish_calls: list[dict[str, Any]] = []
    progress_calls: list[dict[str, Any]] = []
    fetcher = _OverlapFetcher(events, active_samples, delay_seconds=0)
    _patch_runner(
        monkeypatch,
        jr,
        repo,
        fetcher=fetcher,
        finish_calls=finish_calls,
        progress_calls=progress_calls,
        events=events,
    )

    payload = _run_job(jr, repo, ["A", "B"])

    assert payload["status"] == "completed"
    assert max(active_samples) == 1
    assert events.index("fetch-end:A") < events.index("persist:A") < events.index("fetch-start:B")
    assert events.index("fetch-end:B") < events.index("persist:B")


def test_comments_concurrency_overlaps_fetches_and_serializes_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY", "3")
    events: list[str] = []
    active_samples: list[int] = []
    finish_calls: list[dict[str, Any]] = []
    progress_calls: list[dict[str, Any]] = []
    fetcher = _OverlapFetcher(events, active_samples, delay_seconds=0.01)
    _patch_runner(
        monkeypatch,
        jr,
        repo,
        fetcher=fetcher,
        finish_calls=finish_calls,
        progress_calls=progress_calls,
        events=events,
    )

    payload = _run_job(jr, repo, ["A", "B", "C"])

    assert payload["status"] == "completed"
    assert max(active_samples) > 1
    assert events.index("fetch-start:B") < events.index("fetch-end:A")
    assert [event for event in events if event.startswith("persist:")] == ["persist:A", "persist:B", "persist:C"]
    scraped_posts = [call["scraped_posts"] for call in progress_calls]
    assert scraped_posts == sorted(scraped_posts)
    assert scraped_posts[-3:] == [1, 2, 3]
