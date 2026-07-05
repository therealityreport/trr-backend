from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from trr_backend.socials.control_plane import dispatch_runtime


def test_dispatch_runtime_skips_same_active_shared_account(monkeypatch: pytest.MonkeyPatch) -> None:
    legacy = dispatch_runtime.legacy
    monkeypatch.setenv("SOCIAL_SHARED_ACCOUNT_POSTS_PLATFORM_CAP_INSTAGRAM", "2")

    candidates = [
        {
            "id": "job-daily-queued",
            "run_id": "run-catalog",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {
                "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
                "account": "bravodailydish",
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            },
            "metadata": {},
        },
        {
            "id": "job-tv-queued",
            "run_id": "run-catalog",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {
                "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
                "account": "bravotv",
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            },
            "metadata": {},
        },
    ]
    dispatched_job_ids: list[str] = []

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(
        legacy,
        "_job_requires_dedicated_worker_lane",
        lambda job_config, *, platform=None: str(job_config.get("required_worker_lane") or "") == "dedicated",
    )
    monkeypatch.setattr(
        legacy,
        "_modal_social_dispatch_resolution",
        lambda: {
            "resolved": True,
            "reason": None,
            "app_name": "trr-backend-jobs",
            "function_name": "run_social_job",
        },
    )
    monkeypatch.setattr(
        legacy,
        "_list_candidate_jobs_for_modal_dispatch",
        lambda *, run_id=None, limit=200: list(candidates),
    )
    monkeypatch.setattr(
        legacy,
        "_current_modal_dispatch_running_counts",
        lambda: (
            {legacy.SHARED_ACCOUNT_POSTS_STAGE: 1},
            {(legacy.SHARED_ACCOUNT_POSTS_STAGE, "instagram"): 1},
            {"run-existing": 1},
            {("run-existing", legacy.SHARED_ACCOUNT_POSTS_STAGE, "instagram"): 1},
            {(legacy.SHARED_ACCOUNT_POSTS_STAGE, "instagram"): {"bravodailydish"}},
        ),
    )
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})
    monkeypatch.setattr(
        legacy,
        "dispatch_social_job",
        lambda *, job_id, stage=None, priority_recovery=False: (
            dispatched_job_ids.append(job_id) or {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}
        ),
    )

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-catalog", limit=2)

    assert result["dispatched_job_ids"] == ["job-tv-queued"]
    assert dispatched_job_ids == ["job-tv-queued"]


def test_dispatch_runtime_allows_modal_detail_shards_with_required_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    monkeypatch.setenv("SOCIAL_SHARED_ACCOUNT_POSTS_PLATFORM_CAP_INSTAGRAM", "1")

    candidates = [
        {
            "id": f"job-shard-{index}",
            "run_id": "run-catalog",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {
                "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
                "account": "bravotv",
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
                "runner_strategy": "parallel_detail_refresh",
                "details_refresh_shard_count": 3,
                "required_worker_lane": chr(ord("a") + index),
                "allow_local_dev_inline_bypass": True,
                "required_runtime_version": {
                    "execution_backend": "modal",
                    "modal_function": "run_social_posts_job",
                },
            },
            "metadata": {},
        }
        for index in range(3)
    ]
    dispatched_job_ids: list[str] = []

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(
        legacy,
        "_modal_social_dispatch_resolution",
        lambda: {"resolved": True, "reason": None},
    )
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(legacy, "_current_modal_dispatch_running_counts", lambda: ({}, {}, {}, {}, {}))
    monkeypatch.setattr(legacy, "_resolve_catalog_run_in_flight_cap", lambda: 3)
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})
    monkeypatch.setattr(
        legacy,
        "dispatch_social_job",
        lambda *, job_id, stage=None, priority_recovery=False: (
            dispatched_job_ids.append(job_id) or {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}
        ),
    )

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-catalog", limit=3)

    assert result["dispatched_job_ids"] == ["job-shard-0", "job-shard-1", "job-shard-2"]
    assert dispatched_job_ids == ["job-shard-0", "job-shard-1", "job-shard-2"]


def test_dispatch_runtime_honors_pause_after_current(monkeypatch: pytest.MonkeyPatch) -> None:
    legacy = dispatch_runtime.legacy
    heartbeat_updates: list[dict[str, object]] = []

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda run_id: run_id == "run-paused")
    monkeypatch.setattr(
        legacy,
        "_touch_modal_social_dispatcher_heartbeat",
        lambda **kwargs: heartbeat_updates.append(dict(kwargs.get("metadata_updates") or {})),
    )
    monkeypatch.setattr(
        legacy,
        "recover_failed_instagram_comments_capacity_jobs",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("pause should block recovery")),
    )

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-paused", limit=1)

    assert result == {"dispatched_job_ids": [], "dispatch_attempts": 0, "reason": "pause_after_current"}
    assert heartbeat_updates[-1]["last_dispatch_blocked_reason"] == "pause_after_current"


def test_dispatch_runtime_skips_paused_run_candidates_during_global_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    dispatched_job_ids: list[str] = []
    candidates = [
        {
            "id": "job-paused",
            "run_id": "run-paused",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {"stage": legacy.SHARED_ACCOUNT_POSTS_STAGE, "account": "bravotv"},
            "metadata": {},
        },
        {
            "id": "job-active",
            "run_id": "run-active",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {"stage": legacy.SHARED_ACCOUNT_POSTS_STAGE, "account": "bravodailydish"},
            "metadata": {},
        },
    ]

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda run_id: run_id == "run-paused")
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_modal_social_dispatch_resolution", lambda: {"resolved": True, "reason": None})
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(legacy, "_current_modal_dispatch_running_counts", lambda: ({}, {}, {}, {}, {}))
    monkeypatch.setattr(legacy, "_modal_comment_recovery_priority_override_slots", lambda: 0)
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})
    monkeypatch.setattr(
        legacy,
        "dispatch_social_job",
        lambda *, job_id, stage=None, priority_recovery=False: (
            dispatched_job_ids.append(job_id) or {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}
        ),
    )

    result = dispatch_runtime.dispatch_due_social_jobs(run_id=None, limit=2)

    assert result["dispatched_job_ids"] == ["job-active"]
    assert dispatched_job_ids == ["job-active"]


def test_dispatch_runtime_clears_expired_pause_for_global_dispatch_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    now = datetime(2026, 6, 30, 18, 0, tzinfo=UTC)
    dispatched_job_ids: list[str] = []
    updated_pause_rows: list[dict[str, object]] = []
    candidates = [
        {
            "id": "job-expired-pause",
            "run_id": "11111111-1111-1111-1111-111111111111",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {"stage": legacy.SHARED_ACCOUNT_POSTS_STAGE, "account": "bravotv"},
            "metadata": {},
        },
    ]

    def _fetch_one(sql: str, params=None, **_kwargs):
        if "select config" in sql and "from social.scrape_runs" in sql:
            return {
                "config": {
                    "dispatch_control": {
                        "pause_after_current": True,
                        "resume_after": "2026-06-30T17:55:00+00:00",
                    }
                }
            }
        if "update social.scrape_runs" in sql:
            updated_pause_rows.append({"sql": sql, "params": list(params or [])})
            return {"id": "11111111-1111-1111-1111-111111111111"}
        return None

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_relation_exists", lambda relation_name: relation_name == "social.scrape_runs")
    monkeypatch.setattr(legacy, "_now_utc", lambda: now)
    monkeypatch.setattr(legacy.pg, "fetch_one", _fetch_one)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_modal_social_dispatch_resolution", lambda: {"resolved": True, "reason": None})
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(legacy, "_current_modal_dispatch_running_counts", lambda: ({}, {}, {}, {}, {}))
    monkeypatch.setattr(legacy, "_modal_comment_recovery_priority_override_slots", lambda: 0)
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})
    monkeypatch.setattr(
        legacy,
        "dispatch_social_job",
        lambda *, job_id, stage=None, priority_recovery=False: (
            dispatched_job_ids.append(job_id) or {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}
        ),
    )

    result = dispatch_runtime.dispatch_due_social_jobs(run_id=None, limit=1)

    assert result["dispatched_job_ids"] == ["job-expired-pause"]
    assert dispatched_job_ids == ["job-expired-pause"]
    assert updated_pause_rows
    config_update = updated_pause_rows[0]["params"][0]
    assert '"pause_after_current": false' in config_update
    assert "resume_after_elapsed" in config_update


def test_dispatch_runtime_recovers_comments_capacity_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    legacy = dispatch_runtime.legacy

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(
        legacy,
        "recover_failed_instagram_comments_capacity_jobs",
        lambda **kwargs: [{"id": "job-capacity", "run_id": kwargs.get("run_id")}],
    )
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(
        legacy,
        "_modal_social_dispatch_resolution",
        lambda: {"resolved": True, "reason": None},
    )
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_current_modal_dispatch_running_counts", lambda: ({}, {}, {}, {}, {}))
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **_kwargs: {})

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-1", limit=1)

    assert result["recovered_capacity_job_ids"] == ["job-capacity"]
    assert result["dispatched_job_ids"] == []


def test_global_recover_and_dispatch_leaves_comments_capacity_to_run_scoped_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy

    monkeypatch.setattr(dispatch_runtime, "reconcile_terminal_modal_running_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_running_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(dispatch_runtime, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])

    def fail_capacity_recovery(**_kwargs):
        raise AssertionError("global dispatch must not recover comments capacity without a run_id")

    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", fail_capacity_recovery)
    monkeypatch.setattr(
        dispatch_runtime,
        "dispatch_due_social_jobs",
        lambda **_kwargs: {"dispatched_job_ids": [], "dispatch_attempts": 0, "reason": None},
    )
    monkeypatch.setattr(
        legacy,
        "dispatch_due_social_jobs",
        lambda **_kwargs: {"dispatched_job_ids": [], "dispatch_attempts": 0, "reason": None},
    )

    result = dispatch_runtime.recover_and_dispatch_due_social_jobs(limit=1)

    assert result["recovered_capacity_jobs"] == []


def test_dispatch_runtime_skips_posts_auth_cooldown_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    candidates = [
        {
            "id": "job-cooldown",
            "run_id": "run-catalog",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {
                "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
                "account": "bravotv",
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            },
            "metadata": {},
        }
    ]

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_modal_social_dispatch_resolution", lambda: {"resolved": True, "reason": None})
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(legacy, "_current_modal_dispatch_running_counts", lambda: ({}, {}, {}, {}, {}))
    monkeypatch.setattr(legacy, "_modal_comment_recovery_priority_override_slots", lambda: 0)
    monkeypatch.setattr(legacy, "_job_blocked_by_posts_auth_cooldown", lambda _job: {"cooldown_until": "later"})
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **_kwargs: {})

    def fail_dispatch(**_kwargs):
        raise AssertionError("cooldown-blocked posts job must not dispatch")

    monkeypatch.setattr(legacy, "dispatch_social_job", fail_dispatch)

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-catalog", limit=1)

    assert result["dispatched_job_ids"] == []
    assert result["dispatch_attempts"] == 0


def test_dispatch_runtime_stamps_remote_function_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    candidates = [
        {
            "id": "job-posts",
            "run_id": "run-catalog",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {
                "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
                "account": "bravotv",
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            },
            "metadata": {},
        }
    ]
    dispatch_writes: list[dict[str, object]] = []

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_modal_social_dispatch_resolution", lambda: {"resolved": True, "reason": None})
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(legacy, "_current_modal_dispatch_running_counts", lambda: ({}, {}, {}, {}, {}))
    monkeypatch.setattr(legacy, "_modal_comment_recovery_priority_override_slots", lambda: 0)
    monkeypatch.setattr(legacy, "_job_blocked_by_posts_auth_cooldown", lambda _job: None)
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: dispatch_writes.append(kwargs))
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **_kwargs: {})
    monkeypatch.setattr(
        legacy,
        "dispatch_social_job",
        lambda **_kwargs: {
            "dispatched": True,
            "call_id": "fc-123",
            "function_name": "trr-backend-jobs.run_social_job",
        },
    )

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-catalog", limit=1)

    assert result["dispatched_job_ids"] == ["job-posts"]
    assert dispatch_writes[-1]["remote_invocation_id"] == "fc-123"
    assert dispatch_writes[-1]["remote_function_name"] == "trr-backend-jobs.run_social_job"


def test_reconcile_terminal_modal_running_jobs_completes_successful_stale_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    now = datetime(2026, 6, 12, 4, 20, tzinfo=UTC)
    finished: list[dict[str, object]] = []
    refreshed: list[dict[str, object]] = []
    row = {
        "id": "job-terminal-success",
        "run_id": "run-catalog",
        "platform": "instagram",
        "status": "running",
        "items_found": 66,
        "attempt_count": 2,
        "max_attempts": 15,
        "heartbeat_at": now - timedelta(seconds=301),
        "started_at": now - timedelta(minutes=20),
        "claimed_at": now - timedelta(minutes=16),
        "created_at": now - timedelta(minutes=30),
        "config": {
            "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
            "account": "bravotv",
        },
        "metadata": {
            "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
            "account": "bravotv",
            "activity": {
                "phase": "catalog_fetch_page",
                "posts_checked": 66,
                "saved_posts": 66,
            },
            "dispatch": {
                "dispatch_backend": "modal",
                "remote_invocation_id": "fc-success",
                "remote_invocation_status": "running",
            },
        },
    }

    monkeypatch.setattr(legacy, "_scrape_jobs_features", lambda: {"has_queue_fields": True})
    monkeypatch.setattr(legacy, "_now_utc", lambda: now)
    monkeypatch.setattr(
        legacy,
        "_resolve_stale_seconds_for_job",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("stage stale window should not gate terminal success")),
    )
    monkeypatch.setattr(legacy.pg, "fetch_all", lambda *_args, **_kwargs: [dict(row)])
    monkeypatch.setattr(
        legacy,
        "_refresh_remote_modal_invocation_state",
        lambda job, **kwargs: refreshed.append({"job": job, **kwargs})
        or {
            "function_call_id": "fc-success",
            "status": "completed",
            "raw_status": "success",
            "checked_at": now.isoformat(),
            "reason": None,
            "task_id": None,
        },
    )
    monkeypatch.setattr(
        legacy,
        "_finish_job",
        lambda job_id, **kwargs: finished.append({"job_id": job_id, **kwargs}),
    )

    result = dispatch_runtime.reconcile_terminal_modal_running_jobs(run_id="run-catalog", limit=10)

    assert result == [
        {
            "id": "job-terminal-success",
            "run_id": "run-catalog",
            "platform": "instagram",
            "status": "completed",
            "remote_invocation_id": "fc-success",
        }
    ]
    assert refreshed and refreshed[0]["lease_expires_at"] is None
    assert finished[0]["status"] == "completed"
    assert finished[0]["items_found"] == 66
    metadata = finished[0]["metadata"]
    assert metadata["dispatch"]["remote_invocation_status"] == "completed"
    assert metadata["terminal_modal_reconciliation"]["function_call_id"] == "fc-success"
    assert metadata["terminal_modal_reconciliation"]["reason"] == "modal_call_completed_but_db_job_still_running"


def test_reconcile_terminal_modal_running_jobs_requeues_unfinished_frontier_after_failed_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    now = datetime(2026, 6, 12, 4, 20, tzinfo=UTC)
    released: list[dict[str, object]] = []
    finished: list[dict[str, object]] = []
    enqueued: list[dict[str, object]] = []
    row = {
        "id": "job-frontier",
        "run_id": "run-catalog",
        "platform": "instagram",
        "source_scope": "network",
        "status": "running",
        "priority": 101,
        "items_found": 200,
        "attempt_count": 12,
        "max_attempts": 12,
        "worker_id": None,
        "heartbeat_at": now - timedelta(seconds=301),
        "started_at": now - timedelta(minutes=20),
        "claimed_at": now - timedelta(minutes=16),
        "created_at": now - timedelta(minutes=30),
        "config": {
            "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
            "platform": "instagram",
            "source_scope": "network",
            "account": "bravotv",
            "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            "partition_strategy": legacy.CATALOG_FULL_HISTORY_FRONTIER_STRATEGY,
            "runner_strategy": legacy.CATALOG_FULL_HISTORY_FRONTIER_STRATEGY,
            "runner_count": 4,
        },
        "metadata": {
            "dispatch": {
                "dispatch_backend": "modal",
                "remote_invocation_id": "fc-failed",
                "remote_invocation_status": "running",
            },
        },
    }

    monkeypatch.setattr(legacy, "_scrape_jobs_features", lambda: {"has_queue_fields": True})
    monkeypatch.setattr(legacy, "_now_utc", lambda: now)
    monkeypatch.setattr(legacy.pg, "fetch_all", lambda *_args, **_kwargs: [dict(row)])
    monkeypatch.setattr(
        legacy,
        "_refresh_remote_modal_invocation_state",
        lambda _job, **_kwargs: {
            "function_call_id": "fc-failed",
            "status": "failed",
            "raw_status": "failed",
            "checked_at": now.isoformat(),
            "reason": "modal_failure",
            "task_id": None,
        },
    )
    monkeypatch.setattr(
        legacy,
        "_get_shared_account_run_frontier",
        lambda **_kwargs: {
            "status": "running",
            "next_cursor": "cursor-1",
            "total_posts": 27000,
            "last_transport": "public",
            "lease_owner": "job-frontier",
            "exhausted": False,
            "metadata": {"bounded_window_start_reached": False},
        },
    )
    monkeypatch.setattr(
        legacy,
        "_enqueue_shared_posts_job",
        lambda **kwargs: enqueued.append(dict(kwargs)) or {"status": "queued", "job_id": "job-continuation"},
    )
    monkeypatch.setattr(
        legacy,
        "_release_shared_account_run_frontier",
        lambda **kwargs: released.append(dict(kwargs)) or {"status": kwargs.get("status")},
    )
    monkeypatch.setattr(
        legacy,
        "_finish_job",
        lambda job_id, **kwargs: finished.append({"job_id": job_id, **kwargs}),
    )

    result = dispatch_runtime.reconcile_terminal_modal_running_jobs(run_id="run-catalog", limit=10)

    assert result[0]["id"] == "job-frontier"
    assert result[0]["status"] == "cancelled"
    assert result[0]["continuation_job_id"] == "job-continuation"
    assert enqueued[0]["partition_strategy"] == legacy.CATALOG_FULL_HISTORY_FRONTIER_STRATEGY
    assert enqueued[0]["expected_total_posts"] == 27000
    assert released == [
        {
            "run_id": "run-catalog",
            "platform": "instagram",
            "account_handle": "bravotv",
            "lease_owner": "job-frontier",
            "status": "queued",
        }
    ]
    assert finished[0]["status"] == "cancelled"
    assert finished[0]["last_error_code"] == "terminal_modal_frontier_requeued"
    assert (
        finished[0]["metadata"]["terminal_modal_frontier_reconciliation"]["continuation_job_id"] == "job-continuation"
    )


def test_reconcile_terminal_modal_running_jobs_defers_unfinished_frontier_during_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    now = datetime(2026, 6, 12, 4, 20, tzinfo=UTC)
    cooldown_until = now + timedelta(minutes=15)
    released: list[dict[str, object]] = []
    finished: list[dict[str, object]] = []
    row = {
        "id": "job-frontier",
        "run_id": "run-catalog",
        "platform": "instagram",
        "source_scope": "network",
        "status": "running",
        "priority": 101,
        "items_found": 200,
        "attempt_count": 12,
        "max_attempts": 12,
        "worker_id": None,
        "heartbeat_at": now - timedelta(seconds=301),
        "started_at": now - timedelta(minutes=20),
        "claimed_at": now - timedelta(minutes=16),
        "created_at": now - timedelta(minutes=30),
        "config": {
            "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
            "platform": "instagram",
            "source_scope": "network",
            "account": "bravotv",
            "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            "partition_strategy": legacy.CATALOG_FULL_HISTORY_FRONTIER_STRATEGY,
            "runner_strategy": legacy.CATALOG_FULL_HISTORY_FRONTIER_STRATEGY,
        },
        "metadata": {
            "dispatch": {
                "dispatch_backend": "modal",
                "remote_invocation_id": "fc-failed",
                "remote_invocation_status": "running",
            },
        },
    }

    monkeypatch.setattr(legacy, "_scrape_jobs_features", lambda: {"has_queue_fields": True})
    monkeypatch.setattr(legacy, "_now_utc", lambda: now)
    monkeypatch.setattr(legacy.pg, "fetch_all", lambda *_args, **_kwargs: [dict(row)])
    monkeypatch.setattr(
        legacy,
        "_refresh_remote_modal_invocation_state",
        lambda _job, **_kwargs: {
            "function_call_id": "fc-failed",
            "status": "failed",
            "raw_status": "failed",
            "checked_at": now.isoformat(),
            "reason": "modal_failure",
            "task_id": None,
        },
    )
    monkeypatch.setattr(
        legacy,
        "_get_shared_account_run_frontier",
        lambda **_kwargs: {
            "status": "running",
            "next_cursor": "cursor-1",
            "total_posts": 27000,
            "last_transport": "public",
            "lease_owner": "job-frontier",
            "exhausted": False,
            "metadata": {"bounded_window_start_reached": False},
        },
    )
    monkeypatch.setattr(
        legacy,
        "_enqueue_shared_posts_job",
        lambda **_kwargs: {
            "status": "deferred_by_cooldown",
            "cooldown": {"cooldown_until": cooldown_until.isoformat()},
        },
    )
    monkeypatch.setattr(
        legacy,
        "_release_shared_account_run_frontier",
        lambda **kwargs: released.append(dict(kwargs)) or {"status": kwargs.get("status")},
    )
    monkeypatch.setattr(
        legacy,
        "_finish_job",
        lambda job_id, **kwargs: finished.append({"job_id": job_id, **kwargs}),
    )

    result = dispatch_runtime.reconcile_terminal_modal_running_jobs(run_id="run-catalog", limit=10)

    assert result[0]["status"] == "retrying"
    assert result[0]["deferred_by_cooldown"] is True
    assert released[0]["status"] == "retrying"
    assert finished[0]["status"] == "retrying"
    assert finished[0]["next_available_at"] == cooldown_until
    assert finished[0]["last_error_code"] == "terminal_modal_frontier_deferred_by_cooldown"


def test_dispatch_runtime_uses_capacity_stage_and_job_config_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    comments_stage = legacy.INSTAGRAM_COMMENTS_SCRAPLING_STAGE
    dispatched_job_ids: list[str] = []
    dispatched_priority_recovery: list[bool] = []
    dispatched_priority_recovery: list[bool] = []
    cap_calls: list[dict[str, object]] = []
    candidates = [
        {
            "id": "job-dedicated",
            "run_id": "run-1",
            "platform": "instagram",
            "job_type": legacy.SHARED_ACCOUNT_POSTS_JOB_TYPE,
            "status": "queued",
            "config": {
                "stage": legacy.SHARED_ACCOUNT_POSTS_STAGE,
                "account": "bravotv",
                "required_worker_lane": "dedicated",
            },
            "metadata": {},
        },
        {
            "id": "job-comments",
            "run_id": "run-1",
            "platform": "instagram",
            "job_type": "comments",
            "status": "queued",
            "config": {
                "stage": comments_stage,
                "account": "bravotv",
                "comments_shard_count": 4,
            },
            "metadata": {},
        },
    ]

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(
        legacy,
        "_job_requires_dedicated_worker_lane",
        lambda job_config, *, platform=None: str(job_config.get("required_worker_lane") or "") == "dedicated",
    )
    monkeypatch.setattr(
        legacy,
        "_modal_social_dispatch_resolution",
        lambda: {"resolved": True, "reason": None},
    )
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(
        legacy,
        "_current_modal_dispatch_running_counts",
        lambda: (
            {comments_stage: 99, "comments": 0},
            {(comments_stage, "instagram"): 99, ("comments", "instagram"): 0},
            {},
            {},
            {},
        ),
    )
    monkeypatch.setattr(legacy, "_modal_dispatch_stage_global_cap", lambda _stage: 10)
    monkeypatch.setattr(
        legacy,
        "_modal_dispatch_effective_platform_cap",
        lambda stage, platform, *, active_account_count=1, job_config=None: (
            cap_calls.append(
                {
                    "stage": stage,
                    "platform": platform,
                    "active_account_count": active_account_count,
                    "job_config": dict(job_config or {}),
                }
            )
            or 4
        ),
    )
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})

    def fake_dispatch_social_job(
        *,
        job_id: str,
        stage: str | None = None,
        priority_recovery: bool = False,
    ) -> dict[str, object]:
        dispatched_job_ids.append(job_id)
        dispatched_priority_recovery.append(priority_recovery)
        return {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}

    monkeypatch.setattr(legacy, "dispatch_social_job", fake_dispatch_social_job)

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-1", limit=2)

    assert result["dispatched_job_ids"] == ["job-comments"]
    assert dispatched_job_ids == ["job-comments"]
    assert cap_calls[-1]["job_config"]["comments_shard_count"] == 4


def test_dispatch_runtime_priority_comment_recovery_bypasses_comments_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    comments_stage = legacy.INSTAGRAM_COMMENTS_SCRAPLING_STAGE
    dispatched_job_ids: list[str] = []
    dispatched_priority_recovery: list[bool] = []
    candidates = [
        {
            "id": "job-recovery",
            "run_id": "run-1",
            "platform": "instagram",
            "job_type": "comments",
            "status": "queued",
            "priority": 104,
            "config": {
                "stage": comments_stage,
                "account": "bravotv",
                "target_source_ids": ["DTgXh94kXyo"],
                "comments_audit_cursor_retry": True,
                "comments_priority_recovery_override": True,
            },
            "metadata": {},
        }
    ]

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_modal_social_dispatch_resolution", lambda: {"resolved": True, "reason": None})
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(
        legacy,
        "_current_modal_dispatch_running_counts",
        lambda: ({"comments": 8}, {("comments", "instagram"): 8}, {}, {}, {}),
    )
    monkeypatch.setattr(legacy, "_modal_dispatch_stage_global_cap", lambda _stage: 8)
    monkeypatch.setattr(legacy, "_modal_dispatch_effective_platform_cap", lambda *_args, **_kwargs: 8)
    monkeypatch.setattr(legacy, "_modal_comment_recovery_priority_override_slots", lambda: 1)
    monkeypatch.setattr(legacy, "_current_modal_priority_comment_recovery_running_count", lambda: 0)
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})

    def fake_dispatch_social_job(
        *,
        job_id: str,
        stage: str | None = None,
        priority_recovery: bool = False,
    ) -> dict[str, object]:
        dispatched_job_ids.append(job_id)
        dispatched_priority_recovery.append(priority_recovery)
        return {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}

    monkeypatch.setattr(legacy, "dispatch_social_job", fake_dispatch_social_job)

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-1", limit=1)

    assert result["dispatched_job_ids"] == ["job-recovery"]
    assert dispatched_job_ids == ["job-recovery"]
    assert dispatched_priority_recovery == [True]


def test_priority_comment_recovery_requires_explicit_override_marker() -> None:
    legacy = dispatch_runtime.legacy
    comments_stage = legacy.INSTAGRAM_COMMENTS_SCRAPLING_STAGE
    job = {
        "id": "job-recovery",
        "platform": "instagram",
        "job_type": "comments",
        "priority": 104,
        "config": {
            "stage": comments_stage,
            "comments_audit_cursor_retry": True,
        },
        "metadata": {},
    }

    assert legacy._job_is_priority_comment_recovery(job, job["config"], stage=comments_stage) is False

    marked_config = {**job["config"], "comments_priority_recovery_override": True}

    assert legacy._job_is_priority_comment_recovery(job, marked_config, stage=comments_stage) is True


def test_dispatch_runtime_uses_normal_function_when_priority_override_slot_busy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    comments_stage = legacy.INSTAGRAM_COMMENTS_SCRAPLING_STAGE
    dispatched_job_ids: list[str] = []
    dispatched_priority_recovery: list[bool] = []
    candidates = [
        {
            "id": "job-recovery",
            "run_id": "run-1",
            "platform": "instagram",
            "job_type": "comments",
            "status": "queued",
            "priority": 104,
            "config": {
                "stage": comments_stage,
                "account": "bravotv",
                "target_source_ids": ["DTgXh94kXyo"],
                "comments_audit_cursor_retry": True,
                "comments_priority_recovery_override": True,
            },
            "metadata": {},
        }
    ]

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_modal_social_dispatch_resolution", lambda: {"resolved": True, "reason": None})
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(legacy, "_current_modal_dispatch_running_counts", lambda: ({}, {}, {}, {}, {}))
    monkeypatch.setattr(legacy, "_modal_dispatch_stage_global_cap", lambda _stage: 8)
    monkeypatch.setattr(legacy, "_modal_dispatch_effective_platform_cap", lambda *_args, **_kwargs: 8)
    monkeypatch.setattr(legacy, "_modal_comment_recovery_priority_override_slots", lambda: 1)
    monkeypatch.setattr(legacy, "_current_modal_priority_comment_recovery_running_count", lambda: 1)
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})

    def fake_dispatch_social_job(
        *,
        job_id: str,
        stage: str | None = None,
        priority_recovery: bool = False,
    ) -> dict[str, object]:
        dispatched_job_ids.append(job_id)
        dispatched_priority_recovery.append(priority_recovery)
        return {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}

    monkeypatch.setattr(legacy, "dispatch_social_job", fake_dispatch_social_job)

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-1", limit=1)

    assert result["dispatched_job_ids"] == ["job-recovery"]
    assert dispatched_job_ids == ["job-recovery"]
    assert dispatched_priority_recovery == [False]


def test_dispatch_runtime_clears_stale_pending_modal_capacity_before_rerun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = dispatch_runtime.legacy
    now = datetime(2026, 6, 12, tzinfo=UTC)
    old_requested_at = now - timedelta(minutes=10)
    touched: list[dict[str, object]] = []
    dispatched_job_ids: list[str] = []
    job = {
        "id": "job-recovery",
        "run_id": "run-1",
        "platform": "instagram",
        "job_type": "comments",
        "status": "queued",
        "priority": 104,
        "config": {
            "stage": legacy.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
            "account": "bravotv",
            "target_source_ids": ["DTgXh94kXyo"],
            "comments_audit_cursor_retry": True,
        },
        "metadata": {
            "dispatch": {
                "dispatch_backend": "modal",
                "dispatch_requested_at": old_requested_at.isoformat(),
                "dispatch_attempt_count": 1,
                "remote_invocation_id": "fc-pending",
                "remote_invocation_status": "pending",
                "remote_pending_since": old_requested_at.isoformat(),
                "remote_blocked_reason": "modal_capacity_pending",
            }
        },
    }

    monkeypatch.setattr(legacy, "_now_utc", lambda: now)
    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_modal_social_dispatch_resolution", lambda: {"resolved": True, "reason": None})
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: [dict(job)])
    monkeypatch.setattr(legacy, "_current_modal_dispatch_running_counts", lambda: ({}, {}, {}, {}, {}))
    monkeypatch.setattr(legacy, "_modal_comment_recovery_priority_override_slots", lambda: 1)
    monkeypatch.setattr(legacy, "_current_modal_priority_comment_recovery_running_count", lambda: 0)
    monkeypatch.setattr(legacy, "_resolve_stale_seconds_for_job", lambda **_kwargs: 300)
    monkeypatch.setattr(
        legacy,
        "_refresh_remote_modal_invocation_state",
        lambda _job, **_kwargs: {
            "status": "pending",
            "reason": "modal_capacity_pending",
            "checked_at": now.isoformat(),
            "task_id": None,
        },
    )
    monkeypatch.setattr(
        legacy,
        "_touch_job_dispatch_metadata",
        lambda job_id, **kwargs: touched.append({"job_id": job_id, **kwargs}),
    )
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})
    monkeypatch.setattr(
        legacy,
        "dispatch_social_job",
        lambda *, job_id, stage=None, priority_recovery=False: (
            dispatched_job_ids.append(job_id) or {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}
        ),
    )

    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-1", limit=1)

    assert result["dispatched_job_ids"] == ["job-recovery"]
    assert dispatched_job_ids == ["job-recovery"]
    assert any(call.get("remote_blocked_reason") == "stale_modal_pending_capacity_cleared" for call in touched)
    assert any(call.get("remote_invocation_id") == "call-job-recovery" for call in touched)


def test_dispatch_runtime_marks_claimed_modal_dispatch_running(monkeypatch: pytest.MonkeyPatch) -> None:
    legacy = dispatch_runtime.legacy
    claimed_job = {
        "id": "job-claimed",
        "run_id": "run-1",
        "metadata": {"dispatch": {"dispatch_backend": "modal", "remote_invocation_id": "fc-pending"}},
    }
    marked: list[dict[str, object]] = []

    monkeypatch.setattr(legacy, "_claim_job_by_id", lambda *, job_id, worker_id: claimed_job)
    monkeypatch.setattr(legacy, "_set_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        legacy,
        "_mark_claimed_modal_dispatch_running",
        lambda job: marked.append(dict(job)),
    )
    monkeypatch.setattr(
        legacy,
        "process_claimed_job",
        lambda job, *, worker_id=None: {"id": job["id"], "worker_id": worker_id},
    )

    def dispatch_due_social_jobs_stub(*, run_id=None, limit=None):
        return {"dispatched_job_ids": []}

    monkeypatch.setattr(legacy, "dispatch_due_social_jobs", dispatch_due_social_jobs_stub)

    result = dispatch_runtime.claim_and_process_social_job(job_id="job-claimed", worker_id="modal:test")

    assert result["claimed"] is True
    assert marked == [claimed_job]


# --- REVISED §4: per-run Instagram public-comments worker-cap claim bound -------


def _setup_public_comments_cap_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    running_by_run_count: int,
    run_worker_cap: int | None,
    shard_count: int = 3,
) -> list[str]:
    """Stub the dispatch surface for a public Instagram comments run.

    Returns the list that ``dispatch_social_job`` appends claimed job ids to.
    The run config returned by ``legacy.pg.fetch_one`` carries
    ``comments_worker_cap_current`` (the §4 cap) when ``run_worker_cap`` is set.
    """
    legacy = dispatch_runtime.legacy
    comments_stage = legacy.INSTAGRAM_COMMENTS_SCRAPLING_STAGE
    dispatched_job_ids: list[str] = []

    candidates = [
        {
            "id": f"job-comments-{index}",
            "run_id": "run-public",
            "platform": "instagram",
            "job_type": "comments",
            "status": "queued",
            "config": {
                "stage": comments_stage,
                "account": "bravotv",
                "comments_shard_count": shard_count,
                "instagram_scrape_mode": "public_first",
            },
            "metadata": {},
        }
        for index in range(shard_count)
    ]

    monkeypatch.setattr(legacy, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(legacy, "is_modal_remote_executor_enabled", lambda: True)
    monkeypatch.setattr(legacy, "_run_pause_after_current_requested", lambda _run_id: False)
    monkeypatch.setattr(legacy, "recover_failed_instagram_comments_capacity_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_dispatch_blocked_no_progress_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "recover_stale_unclaimed_dispatched_jobs", lambda **_kwargs: [])
    monkeypatch.setattr(legacy, "_modal_social_dispatch_resolution", lambda: {"resolved": True, "reason": None})
    monkeypatch.setattr(legacy, "_list_candidate_jobs_for_modal_dispatch", lambda **_kwargs: list(candidates))
    monkeypatch.setattr(
        legacy,
        "_current_modal_dispatch_running_counts",
        lambda: (
            {comments_stage: 0, "comments": 0},
            {(comments_stage, "instagram"): 0, ("comments", "instagram"): 0},
            {"run-public": running_by_run_count},
            {},
            {},
        ),
    )
    # Keep the generic stage/platform caps wide open so only the §4 cap can bind.
    monkeypatch.setattr(legacy, "_modal_dispatch_stage_global_cap", lambda _stage: 1000)
    monkeypatch.setattr(
        legacy,
        "_modal_dispatch_effective_platform_cap",
        lambda stage, platform, *, active_account_count=1, job_config=None: 1000,
    )
    monkeypatch.setattr(legacy, "_touch_job_dispatch_metadata", lambda job_id, **kwargs: None)
    monkeypatch.setattr(legacy, "_touch_modal_social_dispatcher_heartbeat", lambda **kwargs: {})
    monkeypatch.setattr(
        legacy,
        "dispatch_social_job",
        lambda *, job_id, stage=None, priority_recovery=False: (
            dispatched_job_ids.append(job_id) or {"dispatched": True, "reason": None, "call_id": f"call-{job_id}"}
        ),
    )

    run_config = {"comments_worker_cap_current": run_worker_cap} if run_worker_cap is not None else {}
    monkeypatch.setattr(legacy.pg, "fetch_one", lambda *_args, **_kwargs: {"config": run_config})

    return dispatched_job_ids


def test_dispatch_runtime_bounds_public_comments_claims_by_worker_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # cap_current=6, already 4 active for the run => headroom 2 => claim only 2 of
    # the 3 queued shards even though limit allows more.
    dispatched = _setup_public_comments_cap_dispatch(
        monkeypatch,
        running_by_run_count=4,
        run_worker_cap=6,
        shard_count=3,
    )
    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-public", limit=10)
    assert result["dispatched_job_ids"] == ["job-comments-0", "job-comments-1"]
    assert dispatched == ["job-comments-0", "job-comments-1"]


def test_dispatch_runtime_blocks_public_comments_claims_when_cap_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # cap_current=6, already 6 active => no headroom => nothing claimed.
    dispatched = _setup_public_comments_cap_dispatch(
        monkeypatch,
        running_by_run_count=6,
        run_worker_cap=6,
        shard_count=3,
    )
    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-public", limit=10)
    assert result["dispatched_job_ids"] == []
    assert dispatched == []


def test_dispatch_runtime_without_worker_cap_is_unbounded_by_section4(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Run config has no comments_worker_cap_current => §4 bound does not apply, so
    # all queued shards dispatch (subject only to the wide-open generic caps).
    dispatched = _setup_public_comments_cap_dispatch(
        monkeypatch,
        running_by_run_count=20,
        run_worker_cap=None,
        shard_count=3,
    )
    result = dispatch_runtime.dispatch_due_social_jobs(run_id="run-public", limit=10)
    assert result["dispatched_job_ids"] == ["job-comments-0", "job-comments-1", "job-comments-2"]
    assert dispatched == ["job-comments-0", "job-comments-1", "job-comments-2"]
