from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsFetchResult
from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments
from trr_backend.socials.instagram.scraper import InstagramComment


def _comment(*, comment_id: str, reply_count: int = 0) -> InstagramComment:
    return InstagramComment(
        comment_id=comment_id,
        text="test",
        username="user",
        user_id="user-id",
        created_at=1_700_000_000,
        date_time="2023-11-14T22:13:20+00:00",
        likes=0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=reply_count,
        post_shortcode="SHORT1",
        post_url="https://www.instagram.com/p/SHORT1/",
    )


def _session() -> SimpleNamespace:
    return SimpleNamespace(
        cookies=[],
        browser_account_id="thetraitorsus",
        auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
    )


def test_reply_only_fast_path_reason_requires_no_top_level_resume() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    assert (
        jr._reply_only_fast_path_reason(
            prior_incomplete_reason="reply_tail_budget_exhausted",
            incomplete_fill_enabled=False,
            resume_cursor="cursor-1",
        )
        is None
    )
    assert (
        jr._reply_only_fast_path_reason(
            prior_incomplete_reason="reply_tail_budget_exhausted",
            incomplete_fill_enabled=False,
            resume_cursor=None,
        )
        == "reply_tail_budget_exhausted"
    )
    assert (
        jr._reply_only_fast_path_reason(
            prior_incomplete_reason=jr._PERSISTED_REPLY_TOPOLOGY_GAP_REASON,
            incomplete_fill_enabled=False,
            resume_cursor=None,
        )
        == jr._PERSISTED_REPLY_TOPOLOGY_GAP_REASON
    )
    assert (
        jr._reply_only_fast_path_reason(
            prior_incomplete_reason="",
            incomplete_fill_enabled=True,
            resume_cursor=None,
        )
        == jr._PERSISTED_REPLY_TOPOLOGY_GAP_REASON
    )


def test_incomplete_fill_uses_reply_only_when_persisted_reply_gap_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials import social_season_analytics_impl as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    fetch_calls: list[dict[str, Any]] = []
    finish_calls: list[dict[str, Any]] = []
    persisted_parent = _comment(comment_id="parent-1", reply_count=2)

    class _FakeLifecycle:
        @staticmethod
        def now_utc() -> datetime:
            return datetime(2026, 5, 6, tzinfo=UTC)

        @staticmethod
        def format_time(_value: Any) -> str:
            return "2026-05-06T00:00:00+00:00"

        @staticmethod
        def new_job_progress_state() -> dict[str, Any]:
            return {}

        @staticmethod
        def touch_job_heartbeat(*_args: Any, **_kwargs: Any) -> bool:
            return True

        @staticmethod
        def emit_job_progress(**_kwargs: Any) -> bool:
            return True

        @staticmethod
        def finish_job(job_id: str, **kwargs: Any) -> None:
            finish_calls.append({"job_id": job_id, **kwargs})

        @staticmethod
        def finalize_run_status(_run_id: str, **_kwargs: Any) -> dict[str, Any]:
            return {}

        @staticmethod
        def metadata_dict(value: Any) -> dict[str, Any]:
            return dict(value or {}) if isinstance(value, dict) else {}

        @staticmethod
        def retry_backoff_seconds(_attempt_count: int) -> int:
            return 1

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(
            self,
            shortcode: str,
            **kwargs: Any,
        ) -> InstagramCommentsFetchResult:
            fetch_calls.append({"shortcode": shortcode, "kwargs": dict(kwargs)})
            return InstagramCommentsFetchResult(
                comments=[persisted_parent],
                fetch_failed=False,
                auth_failed=False,
                reported_comment_count=1,
                request_count=1,
            )

        async def aclose(self) -> None:
            return None

    @contextmanager
    def fake_db_connection(**_kwargs: Any):
        yield SimpleNamespace(commit=lambda: None)

    def fake_fetch_one(sql: str, _params: list[object] | None = None, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(sql.split()).lower()
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        if "from social.scrape_jobs" in normalized and "select id::text" in normalized:
            return finish_calls[-1] if finish_calls else {"id": "job-1", "status": "completed"}
        if "from social.scrape_jobs" in normalized:
            return {"status": "running", "worker_id": "worker-1", "claimed_at": "2026-05-06T00:00:00+00:00"}
        return {}

    monkeypatch.setattr(jr, "lifecycle", _FakeLifecycle())
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_kwargs: _session())
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(jr.pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 1})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_public_replay_guard_rows", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_completion_residual_gap_targets_from_health", lambda **_kwargs: [])
    monkeypatch.setattr(jr, "_load_persisted_replies_by_parent", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_persisted_top_level_comments_for_reply_retry", lambda **_kwargs: [persisted_parent])
    monkeypatch.setattr(jr, "_insert_instagram_post_comments_audit", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_reconcile_post_comment_count", lambda **_kwargs: None)
    monkeypatch.setattr(
        jr,
        "persist_instagram_comments_for_post",
        lambda **_kwargs: PersistedInstagramComments(
            post_id="post-1",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=0,
            comments_refreshed=1,
            comments_changed=0,
            stored_parent_comments=1,
            stored_child_replies=0,
            expected_child_replies=0,
            stored_reply_gap_total=0,
        ),
    )

    payload = jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "status": "running",
            "config": {
                "account": "thetraitorsus",
                "stage": repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                "target_source_ids": ["SHORT1"],
                "target_filter": "incomplete",
                "fetch_replies": True,
                "instagram_scrape_mode": "authenticated",
                # The reply-only fast path only runs on the authenticated cursor
                # lane (``if fetch_replies and not public_comments_mode``). Pin the
                # cursor strategy so the job is not treated as public-first (which
                # is the default when no strategy/mode is supplied) and the
                # persisted-reply-gap reply-only retry is actually exercised.
                "comments_load_strategy": "instagram_comments_endpoint_cursor",
            },
        },
        worker_id="worker-1",
    )

    assert payload["status"] == "completed"
    assert fetch_calls
    assert fetch_calls[0]["kwargs"]["reply_only"] is True
    assert fetch_calls[0]["kwargs"]["persisted_top_level_comments"] == [persisted_parent]


def test_reported_gap_without_fetch_failure_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials import social_season_analytics_impl as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    fetch_calls: list[dict[str, Any]] = []
    finish_calls: list[dict[str, Any]] = []
    fetched_parent = _comment(comment_id="parent-1")

    class _FakeLifecycle:
        @staticmethod
        def now_utc() -> datetime:
            return datetime(2026, 5, 6, tzinfo=UTC)

        @staticmethod
        def format_time(_value: Any) -> str:
            return "2026-05-06T00:00:00+00:00"

        @staticmethod
        def new_job_progress_state() -> dict[str, Any]:
            return {}

        @staticmethod
        def touch_job_heartbeat(*_args: Any, **_kwargs: Any) -> bool:
            return True

        @staticmethod
        def emit_job_progress(**_kwargs: Any) -> bool:
            return True

        @staticmethod
        def finish_job(job_id: str, **kwargs: Any) -> None:
            finish_calls.append({"job_id": job_id, **kwargs})

        @staticmethod
        def finalize_run_status(_run_id: str, **_kwargs: Any) -> dict[str, Any]:
            return {}

        @staticmethod
        def metadata_dict(value: Any) -> dict[str, Any]:
            return dict(value or {}) if isinstance(value, dict) else {}

        @staticmethod
        def retry_backoff_seconds(_attempt_count: int) -> int:
            return 1

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(
            self,
            shortcode: str,
            **kwargs: Any,
        ) -> InstagramCommentsFetchResult:
            fetch_calls.append({"shortcode": shortcode, "kwargs": dict(kwargs)})
            return InstagramCommentsFetchResult(
                comments=[fetched_parent],
                fetch_failed=False,
                auth_failed=False,
                fetch_reason=None,
                reported_comment_count=3,
                request_count=1,
            )

        async def aclose(self) -> None:
            return None

    @contextmanager
    def fake_db_connection(**_kwargs: Any):
        yield SimpleNamespace(commit=lambda: None)

    def fake_fetch_one(sql: str, _params: list[object] | None = None, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(sql.split()).lower()
        if normalized.startswith("select metadata from social.scrape_jobs"):
            return {"metadata": {}}
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        if "from social.scrape_jobs" in normalized and "select id::text" in normalized:
            return finish_calls[-1] if finish_calls else {"id": "job-1", "status": "retrying"}
        if "from social.scrape_jobs" in normalized:
            return {"status": "running", "worker_id": "worker-1", "claimed_at": "2026-05-06T00:00:00+00:00"}
        return {}

    monkeypatch.setattr(jr, "lifecycle", _FakeLifecycle())
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_kwargs: _session())
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(jr.pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 3})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_persisted_replies_by_parent", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_persisted_top_level_comments_for_reply_retry", lambda **_kwargs: [])
    monkeypatch.setattr(jr, "_insert_instagram_post_comments_audit", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_reconcile_post_comment_count", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_update_job_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        jr,
        "persist_instagram_comments_for_post",
        lambda **_kwargs: PersistedInstagramComments(
            post_id="post-1",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=0,
            comments_refreshed=1,
            comments_changed=0,
            stored_parent_comments=1,
            stored_child_replies=0,
            expected_child_replies=0,
            stored_reply_gap_total=0,
        ),
    )

    payload = jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "status": "running",
            "attempt_count": 1,
            "max_attempts": 2,
            "config": {
                "account": "thetraitorsus",
                "stage": repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
                "target_source_ids": ["SHORT1"],
                "fetch_replies": True,
                "instagram_scrape_mode": "authenticated",
            },
        },
        worker_id="worker-1",
    )

    assert payload["status"] == "retrying"
    assert fetch_calls
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_fetch_reasons"] == {"SHORT1": "hidden_comments_unresolved"}
    assert metadata["post_fetch_failures"]["fetch_reasons"] == {"SHORT1": "hidden_comments_unresolved"}
