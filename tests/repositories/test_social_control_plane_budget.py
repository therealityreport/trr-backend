"""Focused tests for adaptive social control-plane budget decisions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import trr_backend.socials.control_plane.budget as budget

NOW = datetime(2026, 6, 23, 12, 0, tzinfo=UTC)
LANE = "instagram_backfill"
ACCOUNT = "bravotv"


def _future(seconds: int = 600) -> str:
    return (NOW + timedelta(seconds=seconds)).isoformat()


def _health(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "generated_at": NOW.isoformat(),
        "queue_enabled": True,
        "queue": {"queue_enabled": True, "queue_depth": 0, "by_status": {}},
        "totals": {"auth_failures_total": 0},
        "runs": [],
        "cooldowns": [],
        "worker_auth": {"instagram_authenticated": True},
        "worker_health": {"healthy": True, "healthy_workers": 1, "active_workers": 0},
        "bandwidth": {"gb_total": 0.0, "derived_usd": None, "cost_available": False},
    }
    payload.update(overrides)
    return payload


def _queue(**queue_overrides: Any) -> dict[str, Any]:
    queue_payload: dict[str, Any] = {
        "by_status": {
            "queued": 0,
            "pending": 0,
            "running": 0,
            "retrying": 0,
            "failed": 0,
            "cancelled": 0,
            "completed": 0,
        },
        "recent_failures": [],
        "running_jobs": [],
        "stuck_jobs": [],
        "stuck_jobs_total": 0,
        "stale_claims": {"total": 0, "by_reason": {}, "by_platform": {}, "by_stage": {}},
        "media_stale_claims": {"total": 0, "by_stage": {}, "by_platform": {}},
        "silent_drop_warnings_total": 0,
    }
    queue_payload.update(queue_overrides)
    return {"queue_enabled": True, "queue": queue_payload}


def test_default_budget_decision_is_normal_without_live_reads() -> None:
    decision = budget.build_budget_decision(
        lane=LANE,
        account=f"@{ACCOUNT}",
        backfill_health=_health(),
        queue_status=_queue(),
        include_live=False,
        now=NOW,
    )

    assert decision["state"] == budget.STATE_NORMAL
    assert decision["lane"] == LANE
    assert decision["account"] == ACCOUNT
    assert decision["reasons"] == ["within_default_budget"]
    assert decision["generated_at"] == NOW.isoformat()
    assert decision["ttl_seconds"] == budget.DEFAULT_TTL_SECONDS
    assert decision["limits"]["effective_max_concurrent_jobs"] == 2
    assert decision["limits"]["normal_max_concurrent_jobs"] == 2
    assert decision["limits"]["minimum_sample_floor"] == 25
    assert decision["limits"]["cap4_canary_max_concurrent_jobs"] == 4
    assert decision["limits"]["cap4_canary_metadata_only"] is True
    assert decision["runbook_state"]["phase"] == "live_apply"
    assert decision["runbook_state"]["runbook_version"] == "v4"
    assert decision["runbook_state"]["mandatory"] is True
    assert decision["runbook_state"]["binding_cap"] == 2
    assert decision["runbook_state"]["current_comments_cap"] == 2
    assert decision["runbook_state"]["speed_canary_optional"] is True
    assert decision["runbook_state"]["speed_canary_cap"] == 4
    assert decision["runbook_state"]["cap4_canary"]["mode"] == "metadata_only"
    assert decision["runbook_state"]["minimum_sample_floor"] == 25
    assert decision["evidence"]["sources"] == {
        "backfill_health": "supplied",
        "queue_status": "supplied",
        "active_cooldowns": "backfill_health",
        "recent_failures": "queue_status",
        "running_jobs": "queue_status",
        "stale_running_jobs": "queue_status",
    }


def test_instagram_db_session_worker_budget_uses_canonical_env_with_legacy_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(budget.INSTAGRAM_DB_SESSION_WORKER_BUDGET_ENV, raising=False)
    monkeypatch.setenv(budget.LEGACY_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET_ENV, "7")
    assert budget.instagram_db_session_worker_budget() == 7

    monkeypatch.setenv(budget.INSTAGRAM_DB_SESSION_WORKER_BUDGET_ENV, "10")
    assert budget.instagram_db_session_worker_budget() == 10


def test_instagram_db_session_capacity_counts_active_and_dispatched_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend import modal_dispatch
    from trr_backend.db import pg

    captured: dict[str, object] = {}
    lock_conn = object()

    def _fetch_all(sql: str, params=None, *, conn=None, pool_name: str = "default"):
        captured["sql"] = sql
        captured["conn"] = conn
        captured["pool_name"] = pool_name
        return [
            *[
                {
                    "job_id": f"active-{index}",
                    "status": "running",
                    "worker_id": f"worker-{index}",
                    "claimed_at": "now",
                    "remote_invocation_id": None,
                }
                for index in range(4)
            ],
            *[
                {
                    "job_id": f"dispatched-{index}",
                    "status": "queued",
                    "worker_id": None,
                    "claimed_at": None,
                    "remote_invocation_id": f"call-{index}",
                }
                for index in range(4)
            ],
        ]

    monkeypatch.setenv(budget.INSTAGRAM_DB_SESSION_WORKER_BUDGET_ENV, "10")
    monkeypatch.setattr(pg, "fetch_all", _fetch_all)
    monkeypatch.setattr(modal_dispatch, "inspect_modal_function_call", lambda _call_id: {"status": "running"})
    monkeypatch.setattr(
        budget,
        "_instagram_db_session_pool_usage",
        lambda **_kwargs: {
            "available": True,
            "limit": 15,
            "reserved_sessions": 3,
            "probe_reason": "fresh_session_reservation_succeeded",
            "read_error": None,
        },
    )

    capacity = budget.get_instagram_db_session_capacity(requested_workers=3, conn=lock_conn)

    assert capacity["worker_budget"] == 10
    assert capacity["active_db_jobs"] == 4
    assert capacity["dispatched_unclaimed_jobs"] == 4
    assert capacity["active_workers"] == 8
    assert capacity["remaining_workers"] == 2
    assert capacity["requested_workers"] == 3
    assert capacity["blocked"] is True
    assert capacity["available"] is True
    assert capacity["read_error"] is None
    assert captured["conn"] is lock_conn
    assert captured["pool_name"] == "social_control"
    assert "remote_invocation_id" in str(captured["sql"])
    assert "shared_account_posts" in str(captured["sql"])
    assert "comments_scrapling" in str(captured["sql"])


def test_runbook_live_apply_cap_clamps_overrides_to_two_workers() -> None:
    decision = budget.build_budget_decision(
        lane=LANE,
        account=ACCOUNT,
        backfill_health=_health(),
        queue_status=_queue(),
        benchmark_overrides={
            "limits": {
                "normal_max_concurrent_jobs": 4,
                "minimum_sample_floor": 12,
            }
        },
        include_live=False,
        now=NOW,
    )

    assert decision["state"] == budget.STATE_NORMAL
    assert decision["limits"]["normal_max_concurrent_jobs"] == 2
    assert decision["limits"]["effective_max_concurrent_jobs"] == 2
    assert decision["limits"]["minimum_sample_floor"] == 25
    assert decision["runbook_state"]["speed_canary_cap"] == 4
    assert decision["runbook_state"]["cap4_canary"]["mode"] == "metadata_only"


def test_identity_block_precedes_proxy_cooldown_pause_and_global_pressure() -> None:
    health = _health(
        cooldowns=[
            {
                "platform": "instagram",
                "account_handle": ACCOUNT,
                "blocker_kind": "checkpoint",
                "last_error_code": "challenge_required",
                "cooldown_until": _future(),
            },
            {
                "platform": "instagram",
                "account_handle": ACCOUNT,
                "blocker_kind": "auth",
                "cooldown_until": _future(),
            },
        ],
        queue={"queue_enabled": True, "queue_depth": 500},
    )

    decision = budget.build_budget_decision(
        lane=LANE,
        account=ACCOUNT,
        backfill_health=health,
        queue_status=_queue(by_status={"queued": 500, "running": 10}),
        benchmark_overrides={"account_lane_pauses": [{"lane": LANE, "account": ACCOUNT, "paused": True}]},
        include_live=False,
        now=NOW,
    )

    assert decision["state"] == budget.STATE_IDENTITY_BLOCKED
    assert decision["reasons"] == ["identity_blocked"]
    assert decision["limits"]["effective_max_concurrent_jobs"] == 0
    assert decision["evidence"]["identity_block"]["blocker_kind"] == "checkpoint"
    assert "global_pressure" not in decision["evidence"]


def test_proxy_cooldown_precedes_account_lane_pause() -> None:
    decision = budget.build_budget_decision(
        lane=LANE,
        account=ACCOUNT,
        backfill_health=_health(),
        queue_status=_queue(by_status={"queued": 500, "running": 10}),
        active_cooldowns=[
            {
                "platform": "instagram",
                "account_handle": ACCOUNT,
                "blocker_kind": "auth",
                "last_error_code": "instagram_graphql_cursor_forbidden",
                "cooldown_until": _future(90),
            }
        ],
        benchmark_overrides={"account_lane_pauses": [{"lane": LANE, "account": ACCOUNT, "paused": True}]},
        include_live=False,
        now=NOW,
    )

    assert decision["state"] == budget.STATE_PAUSED
    assert decision["reasons"] == ["proxy_cooldown_active"]
    assert decision["ttl_seconds"] == 90
    assert decision["evidence"]["proxy_cooldown"]["blocker_kind"] == "auth"
    assert "account_lane_pause" not in decision["evidence"]


def test_account_lane_pause_precedes_global_budget_pressure() -> None:
    decision = budget.build_budget_decision(
        lane=LANE,
        account=ACCOUNT,
        backfill_health=_health(queue={"queue_enabled": True, "queue_depth": 500}),
        queue_status=_queue(by_status={"queued": 500, "running": 10}),
        benchmark_overrides={
            "account_lane_pauses": [
                {"lane": LANE, "account": ACCOUNT, "paused": True, "reason": "operator_hold"}
            ]
        },
        include_live=False,
        now=NOW,
    )

    assert decision["state"] == budget.STATE_PAUSED
    assert decision["reasons"] == ["account_lane_paused"]
    assert decision["evidence"]["account_lane_pause"]["reason"] == "operator_hold"
    assert "global_pressure" not in decision["evidence"]


def test_global_budget_reduces_on_queue_depth_and_auth_failure_rate() -> None:
    health = _health(
        queue={"queue_enabled": True, "queue_depth": 30},
        runs=[
            {
                "platform": "instagram",
                "account_handle": ACCOUNT,
                "auth_failure_rate": 0.15,
                "auth_failures": {"auth_failures_total": 2},
            }
        ],
    )

    decision = budget.build_budget_decision(
        lane=LANE,
        account=ACCOUNT,
        backfill_health=health,
        queue_status=_queue(),
        include_live=False,
        now=NOW,
    )

    assert decision["state"] == budget.STATE_REDUCED
    assert "queue_depth_reduced_threshold" in decision["reasons"]
    assert "auth_failure_rate_reduced_threshold" in decision["reasons"]
    assert decision["limits"]["effective_max_concurrent_jobs"] == 1
    assert decision["evidence"]["global_pressure"]["queue_depth"] == 30


def test_global_budget_pauses_on_stale_running_jobs() -> None:
    decision = budget.build_budget_decision(
        lane=LANE,
        account=ACCOUNT,
        backfill_health=_health(),
        queue_status=_queue(media_stale_claims={"total": 1, "by_stage": {"media_mirror": 1}}),
        include_live=False,
        now=NOW,
    )

    assert decision["state"] == budget.STATE_PAUSED
    assert decision["reasons"] == ["stale_running_jobs_present"]
    assert decision["evidence"]["global_pressure"]["stale_running_jobs_total"] == 1


def test_live_reads_are_monkeypatchable_and_queue_read_stays_summary_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    def _fake_backfill_health(**kwargs: Any) -> dict[str, Any]:
        calls["backfill_health"] = kwargs
        return _health()

    def _fake_queue_status(**kwargs: Any) -> dict[str, Any]:
        calls["queue_status"] = kwargs
        return _queue()

    class _Cooldown:
        def to_metadata(self) -> dict[str, Any]:
            return {
                "platform": "instagram",
                "account_handle": ACCOUNT,
                "blocker_kind": "auth",
                "cooldown_until": _future(),
            }

    def _fake_active_cooldown(platform: str, account_handle: str) -> _Cooldown:
        calls["active_cooldown"] = (platform, account_handle)
        return _Cooldown()

    monkeypatch.setattr(budget, "_load_backfill_health", _fake_backfill_health)
    monkeypatch.setattr(budget, "_load_queue_status", _fake_queue_status)
    monkeypatch.setattr(budget, "_load_active_cooldown", _fake_active_cooldown)

    decision = budget.build_budget_decision(lane=LANE, account=ACCOUNT, now=NOW)

    assert decision["state"] == budget.STATE_PAUSED
    assert decision["reasons"] == ["proxy_cooldown_active"]
    assert calls["backfill_health"] == {"include_terminal_runs": True}
    assert calls["queue_status"]["summary_only"] is True
    assert calls["queue_status"]["include_recent_failures"] is True
    assert calls["queue_status"]["include_stuck_jobs"] is False
    assert calls["queue_status"]["fresh"] is True
    assert calls["active_cooldown"] == ("instagram", ACCOUNT)
