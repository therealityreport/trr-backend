"""Focused queue-status repository tests for the extracted control-plane read seam."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime

import pytest

import trr_backend.repositories.social_season_analytics as social_repo
import trr_backend.socials.control_plane.worker_health as worker_health


@pytest.fixture(autouse=True)
def _clear_queue_status_state() -> None:
    social_repo._queue_status_cache = None
    social_repo._queue_status_last_good_cache = None
    social_repo._clear_social_hot_path_caches()
    yield
    social_repo._queue_status_cache = None
    social_repo._queue_status_last_good_cache = None
    social_repo._clear_social_hot_path_caches()


def test_legacy_get_queue_status_delegates_to_control_plane_worker_health(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = {"queue_enabled": True, "queue": {"by_status": {"running": 4}}}

    monkeypatch.setattr(worker_health, "get_queue_status", lambda **_kwargs: expected)

    payload = social_repo.get_queue_status(include_recent_failures=False, include_runs_summary=False)

    assert payload is expected


def test_get_queue_status_uses_cache_ttl_and_skips_recent_failures_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    query_calls: list[str] = []

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, params: list[object] | None = None):
        del params
        query_calls.append(" ".join(sql.split()).lower())
        return [
            {"platform": "instagram", "job_type": "posts", "status": "running", "total": 2},
            {"platform": "instagram", "job_type": "posts", "status": "queued", "total": 1},
        ]

    monkeypatch.setenv("SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS", "20")
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_reconcile_active_queue_runs", lambda **_kwargs: [])
    monkeypatch.setattr(social_repo, "_list_stuck_jobs", lambda limit=100: ([], 0))
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda *_args, **_kwargs: [])

    payload = social_repo.get_queue_status(include_recent_failures=False, include_runs_summary=False)

    assert payload["queue"]["by_status"]["running"] == 2
    assert payload["queue"]["recent_failures"] == []
    assert len(query_calls) == 1
    assert social_repo.SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS_DEFAULT == 20


def test_get_queue_status_fresh_true_bypasses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    query_counter = {"count": 0}

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, params: list[object] | None = None):
        del sql, params
        query_counter["count"] += 1
        return [{"platform": "instagram", "job_type": "posts", "status": "running", "total": 1}]

    monkeypatch.setenv("SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS", "30")
    cached_payload = {
        "queue_enabled": True,
        "workers": {"healthy": True, "healthy_workers": 1},
        "queue": {
            "by_status": {
                "running": 9,
                "queued": 0,
                "pending": 0,
                "retrying": 0,
                "failed": 0,
                "cancelled": 0,
                "completed": 0,
            },
            "by_platform": {},
            "by_job_type": {},
            "recent_failures": [],
            "stuck_jobs": [],
            "stuck_jobs_total": 0,
            "runs_by_status": {
                "running": 0,
                "queued": 0,
                "pending": 0,
                "retrying": 0,
                "failed": 0,
                "cancelled": 0,
                "completed": 0,
            },
            "runs_total": 0,
        },
    }
    monkeypatch.setattr(
        social_repo,
        "_queue_status_cache",
        (
            social_repo.time_module.monotonic(),
            20,
            5000,
            False,
            True,
            social_repo.SOCIAL_QUEUE_STATUS_STUCK_JOBS_LIMIT_DEFAULT,
            False,
            False,
            cached_payload,
        ),
    )
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_reconcile_active_queue_runs", lambda **_kwargs: [])
    monkeypatch.setattr(social_repo, "_list_stuck_jobs", lambda limit=100: ([], 0))
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda *_args, **_kwargs: [])

    cached = social_repo.get_queue_status(include_recent_failures=False, include_runs_summary=False)
    count_after_cached = query_counter["count"]
    fresh = social_repo.get_queue_status(include_recent_failures=False, include_runs_summary=False, fresh=True)

    assert cached["queue"]["by_status"]["running"] == 9
    assert count_after_cached == 0
    assert query_counter["count"] == 1
    assert fresh["queue"]["by_status"]["running"] == 1


def test_get_queue_status_summary_only_skips_expensive_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    query_calls: list[str] = []

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, params: list[object] | None = None):
        del params
        normalized_sql = " ".join(sql.split()).lower()
        query_calls.append(normalized_sql)
        if "from social.scrape_jobs" in normalized_sql and "error_message" in normalized_sql:
            return [
                {
                    "id": "job-1",
                    "run_id": "run-1",
                    "platform": "tiktok",
                    "job_type": "posts",
                    "status": "failed",
                    "error_message": "yt-dlp extractor returned zero posts",
                    "last_error_code": "ytdlp_zero_posts",
                    "last_error_class": "TikTokZeroPostsError",
                    "created_at": None,
                    "completed_at": None,
                }
            ]
        return [{"platform": "instagram", "job_type": "posts", "status": "running", "total": 1}]

    def _unexpected(*_args, **_kwargs):
        raise AssertionError("summary_only should skip expensive queue detail work")

    monkeypatch.setenv("SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_reconcile_active_queue_runs", _unexpected)
    monkeypatch.setattr(social_repo, "recover_dispatch_blocked_no_progress_jobs", _unexpected)
    monkeypatch.setattr(social_repo, "_list_dispatch_blocked_jobs", _unexpected)
    monkeypatch.setattr(social_repo, "_list_stuck_jobs", _unexpected)
    monkeypatch.setattr(
        social_repo,
        "_scrape_jobs_features",
        lambda: {"has_run_id": True, "has_queue_fields": True},
    )
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", _unexpected)

    payload = social_repo.get_queue_status(
        include_recent_failures=True,
        include_stuck_jobs=True,
        include_runs_summary=True,
        summary_only=True,
    )

    assert payload["queue"]["by_status"]["running"] == 1
    assert payload["queue"]["recent_failures"][0]["last_error_code"] == "ytdlp_zero_posts"
    assert payload["queue"]["stuck_jobs"] == []
    assert payload["queue"]["running_jobs"] == []
    assert len(query_calls) == 2


def test_get_queue_status_returns_stale_last_good_on_query_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _raise_query_failure(*_args, **_kwargs):
        raise RuntimeError("aggregate query failed")

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))

    baseline_payload = {
        "queue_enabled": True,
        "workers": {"healthy": True, "healthy_workers": 1},
        "queue": {
            "by_status": {"running": 3, "queued": 4, "pending": 0, "failed": 0},
            "by_platform": {"instagram": {"running": 3, "queued": 4}},
            "by_job_type": {"posts": {"running": 3, "queued": 4}},
            "recent_failures": [],
        },
    }
    monkeypatch.setattr(
        social_repo,
        "_queue_status_last_good_cache",
        (social_repo.time_module.monotonic(), baseline_payload),
    )

    monkeypatch.setenv("SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS", "0")
    monkeypatch.setenv("SOCIAL_QUEUE_STATUS_STALE_FALLBACK_SECONDS", "120")
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _raise_query_failure)
    fallback = social_repo.get_queue_status(include_recent_failures=False, include_runs_summary=False)

    assert fallback == baseline_payload
    assert fallback["queue"]["by_status"]["running"] == 3
    assert fallback["queue"]["by_status"]["queued"] == 4


def test_get_queue_status_includes_stuck_jobs_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, params: list[object] | None = None):
        del sql, params
        return [
            {"platform": "twitter", "job_type": "comments", "status": "running", "total": 2},
            {"platform": "youtube", "job_type": "posts", "status": "queued", "total": 1},
        ]

    def _fake_fetch_all(sql: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        if "where j.status = 'running'" in normalized:
            return []
        assert "from social.scrape_jobs j" in normalized
        return [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "run_id": "22222222-2222-2222-2222-222222222222",
                "platform": "twitter",
                "job_type": "comments",
                "status": "running",
                "worker_id": "social-worker:thomas",
                "created_at": datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
                "heartbeat_at": datetime(2026, 3, 1, 10, 5, tzinfo=UTC),
                "available_at": None,
                "error_message": None,
                "last_error_code": None,
                "stuck_reason": "running_stale_heartbeat",
                "stuck_for_seconds": 700,
            },
            {
                "id": "33333333-3333-3333-3333-333333333333",
                "run_id": "44444444-4444-4444-4444-444444444444",
                "platform": "youtube",
                "job_type": "posts",
                "status": "retrying",
                "worker_id": "social-worker:local",
                "created_at": datetime(2026, 3, 1, 9, 0, tzinfo=UTC),
                "heartbeat_at": datetime(2026, 3, 1, 9, 10, tzinfo=UTC),
                "available_at": datetime(2026, 3, 1, 9, 15, tzinfo=UTC),
                "error_message": "stale_heartbeat_timeout: no heartbeat for >= 300 seconds",
                "last_error_code": "stale_heartbeat_timeout",
                "stuck_reason": "retrying_stale_timeout",
                "stuck_for_seconds": 900,
            },
        ]

    def _fake_fetch_one(sql: str, _params: list[object] | None = None) -> dict[str, object]:
        normalized = " ".join(sql.split()).lower()
        if "count(*)::int as total" in normalized and "from social.scrape_jobs j" in normalized:
            return {"total": 2}
        return {}

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo, "recover_dispatch_blocked_no_progress_jobs", lambda limit=100: [])
    monkeypatch.setattr(social_repo, "_list_dispatch_blocked_jobs", lambda limit=100: ([], 0))
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    payload = social_repo.get_queue_status(include_recent_failures=False)

    assert payload["queue"]["stuck_jobs_total"] == 2
    assert len(payload["queue"]["stuck_jobs"]) == 2
    assert payload["queue"]["stuck_jobs"][0]["stuck_reason"] == "running_stale_heartbeat"
    assert payload["queue"]["stuck_jobs"][1]["stuck_reason"] == "retrying_stale_timeout"


def test_get_queue_status_includes_dispatch_blocked_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, _params: list[object] | None = None):
        normalized = " ".join(sql.split()).lower()
        if "from social.scrape_jobs" in normalized:
            return [
                {
                    "platform": "instagram",
                    "job_type": "post_classify",
                    "status": "queued",
                    "stage": "post_classify",
                    "total": 1,
                }
            ]
        return []

    blocked_rows = [
        {
            "id": "blocked-job-1",
            "run_id": "run-1",
            "platform": "instagram",
            "job_type": "post_classify",
            "status": "queued",
            "worker_id": None,
            "created_at": datetime(2026, 3, 26, 20, 30, tzinfo=UTC),
            "heartbeat_at": None,
            "available_at": datetime(2026, 3, 26, 20, 30, tzinfo=UTC),
            "error_message": "No module named 'modal'",
            "last_error_code": "modal_dispatch_failed",
            "stuck_reason": "modal_sdk_unavailable",
            "stuck_for_seconds": 620,
            "metadata": {
                "dispatch": {
                    "dispatch_backend": "modal",
                    "dispatch_requested_at": "2026-03-26T20:38:53.000Z",
                    "last_dispatch_error_code": "modal_dispatch_failed",
                    "last_dispatch_error": "No module named 'modal'",
                    "remote_blocked_reason": "modal_sdk_unavailable",
                }
            },
        }
    ]

    def _fake_fetch_all(sql: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        if "where j.status = 'running'" in normalized:
            return []
        return []

    def _fake_fetch_one(sql: str, _params: list[object] | None = None) -> dict[str, object]:
        normalized = " ".join(sql.split()).lower()
        if "count(*)::int as total" in normalized and "dispatch_backend" in normalized:
            return {"total": 1}
        return {}

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo, "_reconcile_active_queue_runs", lambda limit=200: [])
    monkeypatch.setattr(social_repo, "recover_dispatch_blocked_no_progress_jobs", lambda limit=100: [])
    monkeypatch.setattr(social_repo, "_list_stuck_jobs", lambda limit=100: ([], 0))
    monkeypatch.setattr(social_repo, "_list_dispatch_blocked_jobs", lambda limit=100: (list(blocked_rows), 1))
    monkeypatch.setattr(
        social_repo,
        "get_worker_health",
        lambda: {
            "healthy": True,
            "healthy_workers": 1,
            "workers": [],
            "reason": None,
            "dispatcher_readiness": {"resolved": False, "reason": "modal_sdk_unavailable"},
        },
    )
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    payload = social_repo.get_queue_status(include_recent_failures=False)

    assert payload["queue"]["dispatch_blocked_jobs_total"] == 1
    assert payload["queue"]["dispatch_blocked_by_reason"]["modal_sdk_unavailable"] == 1
    assert payload["queue"]["waiting_for_claim_jobs_total"] == 0
    assert payload["workers"]["dispatcher_readiness"]["reason"] == "modal_sdk_unavailable"


def test_get_queue_status_includes_runs_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, _params: list[object] | None = None):
        normalized = " ".join(sql.split()).lower()
        if "from social.scrape_jobs" in normalized:
            return [{"platform": "instagram", "job_type": "posts", "status": "queued", "total": 2}]
        if "from social.scrape_runs" in normalized:
            return [{"status": "running", "total": 1}, {"status": "failed", "total": 2}]
        return []

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(social_repo, "_list_stuck_jobs", lambda limit=100: ([], 0))

    payload = social_repo.get_queue_status(include_recent_failures=False)

    runs_by_status = payload["queue"]["runs_by_status"]
    assert runs_by_status["running"] == 1
    assert runs_by_status["failed"] == 2
    assert payload["queue"]["runs_total"] == 3


def test_get_queue_status_recent_failures_include_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, _params: list[object] | None = None):
        normalized = " ".join(sql.split()).lower()
        if "group by 1, 2, 3" in normalized:
            return [{"platform": "twitter", "job_type": "comments", "status": "failed", "total": 1}]
        if "where status = any(%s::text[])" in normalized and "failure_dismissed_at" in normalized:
            return [
                {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "run_id": "22222222-2222-2222-2222-222222222222",
                    "platform": "twitter",
                    "job_type": "comments",
                    "status": "failed",
                    "error_message": "boom",
                    "last_error_code": "x",
                    "last_error_class": "Error",
                    "created_at": datetime(2026, 3, 2, 10, 0, tzinfo=UTC),
                    "completed_at": datetime(2026, 3, 2, 10, 5, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda *_args, **_kwargs: [])

    payload = social_repo.get_queue_status(include_stuck_jobs=False, include_runs_summary=False)

    assert payload["queue"]["recent_failures"][0]["run_id"] == "22222222-2222-2222-2222-222222222222"


def test_get_queue_status_includes_tiktok_single_path_alerts_from_recent_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, _params: list[object] | None = None):
        normalized = " ".join(sql.split()).lower()
        if "group by 1, 2, 3, 4" in normalized:
            return [{"platform": "tiktok", "job_type": "posts", "status": "failed", "stage": "posts", "total": 1}]
        if "where status = any(%s::text[])" in normalized and "failure_dismissed_at" in normalized:
            return [
                {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "run_id": "22222222-2222-2222-2222-222222222222",
                    "platform": "tiktok",
                    "job_type": "posts",
                    "status": "failed",
                    "error_message": "yt-dlp returned no output",
                    "last_error_code": "tiktok_ytdlp_transport_failed",
                    "last_error_class": "RuntimeError",
                    "created_at": datetime(2026, 3, 2, 10, 0, tzinfo=UTC),
                    "completed_at": datetime(2026, 3, 2, 10, 5, tzinfo=UTC),
                }
            ]
        return []

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo, "_reconcile_active_queue_runs", lambda **_kwargs: [])
    monkeypatch.setattr(social_repo, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(social_repo, "_list_stuck_jobs", lambda limit=100: ([], 0))
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda *_args, **_kwargs: [])

    payload = social_repo.get_queue_status(
        include_recent_failures=True,
        include_stuck_jobs=False,
        include_runs_summary=False,
    )

    alert_codes = {str(alert["code"]) for alert in payload["alerts"]}
    assert "tiktok_single_path_risk" in alert_codes
    assert "tiktok_single_path_degraded" in alert_codes
    degraded = next(alert for alert in payload["alerts"] if str(alert["code"]) == "tiktok_single_path_degraded")
    assert degraded["recent_failure_count"] == 1
    assert degraded["recent_failure_codes"] == ["tiktok_ytdlp_transport_failed"]


def test_get_queue_status_includes_running_jobs_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Cursor:
        def execute(self, _sql: str, _params: list[object] | None = None) -> None:
            return None

    def _fake_fetch_all_with_cursor(_cur: object, sql: str, _params: list[object] | None = None):
        normalized = " ".join(sql.split()).lower()
        if "from social.scrape_jobs j" in normalized:
            return [
                {
                    "platform": "instagram",
                    "job_type": "shared_account_posts",
                    "status": "running",
                    "stage": "shared_account_posts",
                    "total": 1,
                }
            ]
        if "from social.scrape_runs r" in normalized:
            return [{"status": "running", "total": 1}]
        return []

    def _fake_fetch_all(sql: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        assert "where j.status = 'running'" in normalized
        return [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "run_id": "22222222-2222-2222-2222-222222222222",
                "platform": "instagram",
                "job_type": "shared_account_posts",
                "stage": "shared_account_posts",
                "account_handle": "bravotv",
                "worker_id": "modal:social:job-1",
                "started_at": datetime(2026, 3, 19, 20, 0, tzinfo=UTC),
                "heartbeat_at": datetime(2026, 3, 19, 20, 5, tzinfo=UTC),
                "dispatch_backend": "modal",
                "required_execution_backend": "modal",
            }
        ]

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_reconcile_active_queue_runs", lambda **_kwargs: [])
    monkeypatch.setattr(social_repo, "_list_stuck_jobs", lambda limit=100: ([], 0))
    monkeypatch.setattr(social_repo, "get_worker_health", lambda: {"healthy": True, "healthy_workers": 1})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(object()))
    monkeypatch.setattr(social_repo.pg, "db_cursor", lambda conn=None: nullcontext(_Cursor()))
    monkeypatch.setattr(social_repo.pg, "fetch_all_with_cursor", _fake_fetch_all_with_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    payload = social_repo.get_queue_status(include_recent_failures=False)

    assert payload["queue"]["running_jobs"] == [
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "run_id": "22222222-2222-2222-2222-222222222222",
            "platform": "instagram",
            "job_type": "shared_account_posts",
            "stage": "shared_account_posts",
            "account_handle": "bravotv",
            "worker_id": "modal:social:job-1",
            "started_at": "2026-03-19T20:00:00+00:00",
            "heartbeat_at": "2026-03-19T20:05:00+00:00",
            "dispatch_backend": "modal",
            "required_execution_backend": "modal",
        }
    ]
