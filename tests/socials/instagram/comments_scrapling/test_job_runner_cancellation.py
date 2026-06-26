from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsFetchResult
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


def _patch_common_runner_dependencies(monkeypatch: pytest.MonkeyPatch, jr: Any, repo: Any) -> None:
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_kwargs: _session())
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-28T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 4, 28, tzinfo=timezone.utc))
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})


def test_comments_job_runner_checks_cancellation_after_warmup_before_opening_persist_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []
    db_connection_calls: list[str] = []

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(comments=[], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    @contextmanager
    def fake_db_connection(*, label: str | None = None, **_kwargs: Any):
        db_connection_calls.append(label or "")
        yield SimpleNamespace(commit=lambda: None)

    def fake_fetch_one(sql: str, _params: list[object] | None = None, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(sql.split()).lower()
        if "select status from social.scrape_jobs" in normalized:
            return {"status": "cancelled"}
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        return {"id": "job-1", "status": "cancelled", "items_found": 0}

    _patch_common_runner_dependencies(monkeypatch, jr, repo)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(jr.pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))

    payload = jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {
                "account": "thetraitorsus",
                "target_source_ids": ["SHORT1"],
                "comments_cancel_check_every_posts": 1,
            },
        },
        worker_id="worker-1",
    )

    assert payload["status"] == "cancelled"
    # The post-warmup cancellation check must fire before the per-post persist
    # connection is opened, so no persist connection should ever be acquired.
    assert "instagram-comments-scrapling-persist" not in db_connection_calls
    assert fetch_calls == []
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "cancelled"
    assert finish_kwargs["items_found"] == 0
    assert finish_kwargs["metadata"]["cancel_scope"] == "job"


def test_comments_job_runner_reuses_persist_conn_for_loop_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_one_calls: list[dict[str, Any]] = []
    persist_conn = SimpleNamespace(commit=lambda: None)

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, _shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(comments=[], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    @contextmanager
    def fake_db_connection(**_kwargs: Any):
        yield persist_conn

    def fake_fetch_one(sql: str, _params: list[object] | None = None, **kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(sql.split()).lower()
        fetch_one_calls.append({"sql": normalized, "conn": kwargs.get("conn")})
        if "select id::text" in normalized and "from social.scrape_jobs" in normalized:
            return {"id": "job-1", "status": "cancelled", "items_found": 0}
        if "from social.scrape_jobs" in normalized:
            return {"status": "running", "worker_id": "worker-1", "claimed_at": "2026-04-28T00:00:00+00:00"}
        if "select status from social.scrape_runs" in normalized:
            if kwargs.get("conn") is persist_conn:
                return {"status": "cancelled"}
            return {"status": "running"}
        return {"id": "job-1", "status": "cancelled", "items_found": 0}

    _patch_common_runner_dependencies(monkeypatch, jr, repo)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(jr.pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))

    payload = jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {
                "account": "thetraitorsus",
                "target_source_ids": ["SHORT1"],
                "comments_cancel_check_every_posts": 1,
                "fetch_replies": False,
            },
        },
        worker_id="worker-1",
    )

    assert payload["status"] == "cancelled"
    assert any(
        call["conn"] is persist_conn and "select status from social.scrape_runs" in call["sql"]
        for call in fetch_one_calls
    )
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "cancelled"
    assert finish_kwargs["items_found"] == 0
    assert finish_kwargs["metadata"]["cancel_scope"] == "run"


def test_comments_job_runner_passes_single_session_strategy_and_records_saved_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_kwargs_calls: list[dict[str, Any]] = []
    persist_calls: list[dict[str, Any]] = []
    emitted_progress: list[dict[str, Any]] = []
    finished_metadata: dict[str, Any] = {}

    class _FakeFetcher:
        runtime_metadata = {
            "transport": "test",
            "request_count": 1,
            "comments_load_strategy": {"last": {"strategy_decision": {"selected_strategy": "single_session_load_all"}}},
        }

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs_calls.append({"shortcode": shortcode, **kwargs})
            return InstagramCommentsFetchResult(
                comments=[_comment("comment-1")],
                fetch_failed=False,
                auth_failed=False,
                fetch_reason=None,
                reported_comment_count=1,
                diagnostic_metadata={
                    "strategy_decision": {"selected_strategy": "single_session_load_all"},
                    "api_pages_loaded": 1,
                    "api_rows_seen": 1,
                },
            )

        async def aclose(self) -> None:
            return None

    @contextmanager
    def fake_db_connection(**_kwargs: Any):
        yield SimpleNamespace(commit=lambda: None)

    def fake_fetch_one(sql: str, _params: list[object] | None = None, **kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(sql.split()).lower()
        if "select id::text" in normalized and "from social.scrape_jobs" in normalized:
            return {
                "id": "job-1",
                "run_id": "run-1",
                "platform": "instagram",
                "job_type": "comments",
                "status": finish_calls[-1]["status"] if finish_calls else "completed",
                "items_found": finish_calls[-1]["items_found"] if finish_calls else 0,
                "error_message": None,
                "metadata": dict(finished_metadata),
            }
        if "select metadata from social.scrape_jobs" in normalized:
            return {"metadata": {}}
        if "from social.scrape_jobs" in normalized:
            return {"status": "running", "worker_id": "worker-1", "claimed_at": "2026-04-28T00:00:00+00:00"}
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        return {}

    def fake_finish_job(job_id: str, **kwargs: Any) -> None:
        nonlocal finished_metadata
        finished_metadata = dict(kwargs.get("metadata") or {})
        finish_calls.append({"job_id": job_id, **kwargs})

    def fake_persist_instagram_comments_for_post(**kwargs: Any) -> Any:
        persist_calls.append(dict(kwargs))
        return jr.PersistedInstagramComments(
            post_id="post-1",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            stored_parent_comments=1,
        )

    _patch_common_runner_dependencies(monkeypatch, jr, repo)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_insert_instagram_post_comments_audit", lambda **_kwargs: None)
    monkeypatch.setattr(jr.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(jr.pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(
        jr,
        "persist_instagram_comments_for_post",
        fake_persist_instagram_comments_for_post,
    )
    monkeypatch.setattr(repo, "_finish_job", fake_finish_job)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **kwargs: emitted_progress.append(kwargs) or True)

    payload = jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {
                "account": "thetraitorsus",
                "target_source_ids": ["SHORT1"],
                "fetch_replies": False,
                "comments_load_strategy": "single_session_load_all",
                "comments_cancel_check_every_posts": 1,
            },
        },
        worker_id="worker-1",
    )

    assert payload["status"] == "completed"
    assert fetch_kwargs_calls[0]["load_strategy"] == "single_session_load_all"
    assert persist_calls[0]["shortcode"] == "SHORT1"
    assert len(persist_calls) == 1
    finish_metadata = finish_calls[-1]["metadata"]
    assert finish_metadata["comments_load_strategy"] == "single_session_load_all"
    assert finish_metadata["comments_session_scope"] == "profile_single_worker"
    assert finish_metadata["comments_strategy"]["saved_once_per_post"] == {
        "enabled": True,
        "count": 1,
        "target_source_ids": ["SHORT1"],
    }
    assert finish_metadata["post_latency"]["samples"][0]["saved_once_per_post"] is True
    assert emitted_progress[-1]["extra_metadata"]["comments_load_strategy"] == "single_session_load_all"
