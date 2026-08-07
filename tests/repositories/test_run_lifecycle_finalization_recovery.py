"""Race and connection contracts for the run finalization recovery path."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

import trr_backend.socials.control_plane.run_lifecycle as run_lifecycle


def test_set_run_status_conditions_transition_on_observed_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch_one(sql: str, params=None, *, conn=None, **_kwargs):
        captured["sql"] = " ".join(sql.lower().split())
        captured["params"] = list(params or [])
        captured["conn"] = conn
        return {"id": "run-1"}

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)

    assert run_lifecycle._set_run_status("run-1", "completed", expected_status="running") is True
    assert "where id = %s and status = %s" in str(captured["sql"])
    assert captured["params"][-2:] == ["run-1", "running"]


def test_finalize_does_not_overwrite_cancellation_after_status_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_conn = object()
    observed_rows = iter(
        [
            {"status": "running", "config": {"pipeline_ingest_mode": "manual"}},
            {"status": "cancelled", "config": {}},
        ]
    )
    captured: dict[str, Any] = {}

    def fake_fetch_one(sql: str, params=None, *, conn=None, **_kwargs):
        normalized = " ".join(sql.split()).lower()
        if "select status, config from social.scrape_runs" in normalized:
            assert conn is lock_conn
            return next(observed_rows)
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fake_set_status(_run_id: str, _status: str, *, conn=None, expected_status=None) -> bool:
        captured["conn"] = conn
        captured["expected_status"] = expected_status
        return False

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(run_lifecycle, "_update_run_summary", lambda *_args, **_kwargs: {"active_jobs": 0})
    monkeypatch.setattr(
        run_lifecycle,
        "_run_job_status_breakdown",
        lambda *_args, **_kwargs: {"running_jobs": 0, "queued_jobs": 0, "cancelling_jobs": 0},
    )
    monkeypatch.setattr(run_lifecycle, "_set_run_status", fake_set_status)
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "_maybe_enqueue_shared_catalog_classify_jobs_after_fetch",
        lambda **_kwargs: 0,
    )
    monkeypatch.setattr(run_lifecycle._legacy_module(), "_resolve_pipeline_ingest_mode", lambda value: value)
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "_shared_catalog_fetch_has_terminal_error",
        lambda *_a, **_k: False,
    )

    result = run_lifecycle._finalize_run_status_locked("run-1", lock_conn)

    assert captured == {"conn": lock_conn, "expected_status": "running"}
    assert result["status"] == "cancelled"
    assert result["skip_followups"] is True


def test_only_one_deferred_followup_claim_is_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_conn = object()
    stored_followup = {
        "state": "pending",
        "platform": "instagram",
        "account_handle": "bravotv",
    }
    claims: list[dict[str, Any]] = []

    def fake_fetch_one(sql: str, params=None, *, conn=None, **_kwargs):
        normalized = " ".join(sql.split()).lower()
        assert conn is lock_conn
        assert "config->'deferred_comments_followup'->>'state' = 'pending'" in normalized
        assert "launch_claimed_at" in normalized
        payload = json.loads((params or ["{}"])[0])
        candidate = payload["deferred_comments_followup"]
        if stored_followup.get("launch_claimed_at"):
            return None
        stored_followup.update(candidate)
        claims.append(dict(candidate))
        return {
            "status": "completed",
            "config": {"deferred_comments_followup": dict(stored_followup)},
            "summary": {},
        }

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)

    run_config = {"deferred_comments_followup": dict(stored_followup)}
    first = run_lifecycle._claim_deferred_comments_followup_locked(
        run_id="run-1",
        run_config=run_config,
        conn=lock_conn,
    )
    second = run_lifecycle._claim_deferred_comments_followup_locked(
        run_id="run-1",
        run_config=run_config,
        conn=lock_conn,
    )

    assert first is not None
    assert first["config"]["deferred_comments_followup"]["launch_claimed_at"]
    assert second is None
    assert len(claims) == 1


def test_stale_deferred_followup_claim_is_reclaimed_with_a_new_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    old_token = "old-claim-token"
    stored_followup = {
        "state": "pending",
        "platform": "instagram",
        "account_handle": "bravotv",
        "launch_claim_token": old_token,
        "launch_claimed_at": "2026-07-10T11:50:00+00:00",
        "launch_lease_expires_at": "2026-07-10T11:55:00+00:00",
        "launch_group_id": "group-1",
    }
    captured: dict[str, Any] = {}

    monkeypatch.setattr(run_lifecycle.legacy, "_now_utc", lambda: now)

    def fake_fetch_one(sql: str, params=None, *, conn=None, **_kwargs):
        normalized = " ".join(sql.lower().split())
        captured["sql"] = normalized
        captured["params"] = list(params or [])
        payload = json.loads((params or ["{}"])[0])
        return {
            "status": "completed",
            "config": payload,
            "summary": {},
        }

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)

    result = run_lifecycle._claim_deferred_comments_followup_locked(
        run_id="run-1",
        run_config={"deferred_comments_followup": stored_followup},
        conn=object(),
    )

    assert result is not None
    reclaimed = result["config"]["deferred_comments_followup"]
    assert reclaimed["launch_claim_token"] != old_token
    assert reclaimed["launch_claimed_at"] == "2026-07-10T12:00:00+00:00"
    assert reclaimed["launch_lease_expires_at"] == "2026-07-10T12:05:00+00:00"
    assert reclaimed["launch_recovered_from_token"] == old_token
    assert reclaimed["launch_recovery_count"] == 1
    assert result["launch_reclaimed"] is True
    assert "launch_lease_expires_at" in str(captured["sql"])


def test_fresh_deferred_followup_claim_is_excluded_without_a_second_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(run_lifecycle.legacy, "_now_utc", lambda: now)
    monkeypatch.setattr(
        run_lifecycle.legacy.pg,
        "fetch_one",
        lambda *_args, **_kwargs: pytest.fail("fresh claim must not be reclaimed"),
    )

    result = run_lifecycle._claim_deferred_comments_followup_locked(
        run_id="run-1",
        run_config={
            "deferred_comments_followup": {
                "state": "pending",
                "platform": "instagram",
                "account_handle": "bravotv",
                "launch_claim_token": "fresh-token",
                "launch_claimed_at": "2026-07-10T11:59:00+00:00",
                "launch_lease_expires_at": "2026-07-10T12:04:00+00:00",
            }
        },
        conn=object(),
    )

    assert result is None


def test_expired_deferred_followup_claim_sweep_refinalizes_and_isolates_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    finalized: list[str] = []
    captured_sql: list[str] = []

    def fake_fetch_all(sql: str, params=None, **kwargs):
        captured_sql.append(" ".join(sql.lower().split()))
        assert params == [25]
        assert kwargs["pool_name"] == run_lifecycle.SOCIAL_CONTROL_POOL_NAME
        return [{"run_id": "run-good"}, {"run_id": "run-bad"}]

    def fake_finalize(run_id: str, *, force_recompute: bool = False):
        assert force_recompute is True
        if run_id == "run-bad":
            raise RuntimeError("isolated")
        finalized.append(run_id)
        return {}

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(run_lifecycle, "_finalize_run_status", fake_finalize)
    monkeypatch.setattr(run_lifecycle.legacy.logger, "exception", lambda *_args, **_kwargs: None)

    result = run_lifecycle.recover_stale_deferred_comments_followup_claims()

    assert result == {"scanned": 2, "refinalized": 1, "failed": 1}
    assert finalized == ["run-good"]
    assert "status = 'completed'" in captured_sql[0]
    assert "launch_claim_token" not in captured_sql[0]
    assert "launch_lease_expires_at" in captured_sql[0]


def test_stale_deferred_followup_sweep_selects_timestamp_only_claims(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_fetch_all(sql: str, params=None, **kwargs):
        captured_sql.append(" ".join(sql.lower().split()))
        assert params == [25]
        assert kwargs["pool_name"] == run_lifecycle.SOCIAL_CONTROL_POOL_NAME
        return []

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_all", fake_fetch_all)

    result = run_lifecycle.recover_stale_deferred_comments_followup_claims()

    assert result == {"scanned": 0, "refinalized": 0, "failed": 0}
    assert "launch_claimed_at" in captured_sql[0]
    assert "launch_claim_token" not in captured_sql[0]


def test_recovered_deferred_followup_reuses_child_before_launch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claimed_config = {
        "deferred_comments_followup": {
            "state": "pending",
            "platform": "instagram",
            "account_handle": "bravotv",
            "launch_group_id": "group-recovered",
            "launch_claim_token": "claim-recovered",
            "launch_claimed_at": "2026-07-10T11:50:00+00:00",
            "launch_lease_expires_at": "2026-07-10T11:55:00+00:00",
            "launch_recovered_at": "2026-07-10T12:00:00+00:00",
        }
    }
    writes: list[dict[str, Any]] = []
    launches: list[object] = []

    def fake_fetch_one(sql: str, params=None, **_kwargs):
        normalized = " ".join(sql.lower().split())
        if normalized.startswith("select status, config"):
            return {"status": "completed", "config": claimed_config}
        if "where id <> %s::uuid" in normalized:
            return {
                "run_id": "recovered-child",
                "status": "running",
                "config": {
                    "required_runtime_version": {"version": "child"},
                    "created_by_runtime_version": {"version": "child"},
                },
                "summary": {},
            }
        if normalized.startswith("update social.scrape_runs"):
            payload = json.loads((params or ["{}"])[0])
            writes.append(payload)
            return {"status": "completed", "config": payload, "summary": {}}
        raise AssertionError(f"unexpected SQL: {normalized}")

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "_shared_account_catalog_scrape_complete",
        lambda **_kwargs: True,
    )
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "start_social_account_comments_scrape",
        lambda *_args, **_kwargs: launches.append(True) or pytest.fail("recovered child must be reused"),
    )

    result = run_lifecycle._maybe_start_deferred_comments_followup(
        run_id="parent-run",
        run_status="completed",
        run_config=claimed_config,
        summary={},
    )

    assert result is not None
    assert launches == []
    assert writes[0]["deferred_comments_followup"]["state"] == "started"
    assert writes[0]["deferred_comments_followup"]["comments_run_id"] == "recovered-child"
    assert writes[0]["attached_followups"]["comments"]["run_id"] == "recovered-child"


def test_claimed_deferred_followup_rechecks_parent_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claimed_config = {
        "deferred_comments_followup": {
            "state": "pending",
            "launch_claimed_at": "2026-07-10T12:00:00+00:00",
            "platform": "instagram",
            "account_handle": "bravotv",
        }
    }
    updates: list[dict[str, Any]] = []

    monkeypatch.setattr(
        run_lifecycle.legacy.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"status": "cancelled", "config": claimed_config},
    )
    monkeypatch.setattr(
        run_lifecycle,
        "_merge_run_config",
        lambda _run_id, *, config_updates, conn=None: updates.append(config_updates) or config_updates,
    )
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "_shared_account_catalog_scrape_complete",
        lambda **_kwargs: pytest.fail("catalog completion must not run after cancellation"),
    )
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "start_social_account_comments_scrape",
        lambda *_args, **_kwargs: pytest.fail("cancelled parent must not launch comments"),
    )

    result = run_lifecycle._maybe_start_deferred_comments_followup(
        run_id="run-1",
        run_status="completed",
        run_config=claimed_config,
        summary={},
    )

    assert result == {"_deferred_followup_parent_cancelled": True}
    assert updates[0]["deferred_comments_followup"]["state"] == "cancelled"
    assert updates[0]["deferred_comments_followup"]["launch_claimed_at"] is None


def test_deferred_followup_cancels_child_when_parent_wins_attach_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claimed_config = {
        "deferred_comments_followup": {
            "state": "pending",
            "launch_claim_token": "claim-1",
            "launch_claimed_at": "2026-07-10T12:00:00+00:00",
            "launch_lease_expires_at": "2026-07-10T12:05:00+00:00",
            "platform": "instagram",
            "account_handle": "bravotv",
        }
    }
    parent_reads = iter(
        [
            {"status": "completed", "config": claimed_config},
            None,
        ]
    )
    cancelled_children: list[dict[str, Any]] = []
    durable_updates: list[dict[str, Any]] = []
    events: list[str] = []

    def fake_fetch_one(sql: str, params=None, **_kwargs):
        normalized = " ".join(sql.lower().split())
        if normalized.startswith("select status, config"):
            return next(parent_reads)
        if normalized.startswith("update social.scrape_runs") and "jsonb_set" in normalized:
            durable_updates.append(json.loads((params or ["{}"])[0]))
            events.append("persist")
            return {"id": "run-1"}
        if normalized.startswith("update social.scrape_runs"):
            return next(parent_reads)
        raise AssertionError(f"unexpected SQL: {normalized}")

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "_shared_account_catalog_scrape_complete",
        lambda **_kwargs: True,
    )
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "start_social_account_comments_scrape",
        lambda *_args, **_kwargs: {"run_id": "comments-child-1", "status": "queued"},
    )

    def fake_cancel(**kwargs):
        events.append("cancel")
        cancelled_children.append(kwargs)
        return {"status": "cancelled", **kwargs}

    monkeypatch.setattr(run_lifecycle._legacy_module(), "cancel_social_account_comments_run", fake_cancel)
    monkeypatch.setattr(run_lifecycle._legacy_module(), "_resolve_runtime_version_stamp", lambda: {})

    result = run_lifecycle._maybe_start_deferred_comments_followup(
        run_id="run-1",
        run_status="completed",
        run_config=claimed_config,
        summary={},
    )

    assert result is None
    assert events[:2] == ["persist", "cancel"]
    cancellation = durable_updates[0]["child_cancellation"]
    assert cancellation["state"] == "pending"
    assert cancellation["child_run_id"] == "comments-child-1"
    assert cancellation["attempt_count"] == 1
    assert "state" not in durable_updates[0]
    assert "launch_claim_token" not in durable_updates[0]
    assert cancelled_children == [
        {
            "platform": "instagram",
            "account_handle": "bravotv",
            "run_id": "comments-child-1",
            "cancelled_by": "parent_run_cancelled_during_deferred_followup_launch",
        }
    ]


def test_parent_cancellation_drains_already_attached_deferred_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attached_config = {
        "deferred_comments_followup": {
            "state": "started",
            "launch_claim_token": None,
            "launch_claimed_at": None,
            "launch_lease_expires_at": None,
            "platform": "instagram",
            "account_handle": "bravotv",
            "comments_run_id": "comments-child-2",
        },
        "attached_followups": {
            "comments": {"run_id": "comments-child-2", "status": "queued"},
        },
    }
    updates: list[dict[str, Any]] = []
    cancelled_children: list[dict[str, Any]] = []

    monkeypatch.setattr(
        run_lifecycle.legacy.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"status": "cancelled", "config": attached_config},
    )
    monkeypatch.setattr(
        run_lifecycle,
        "_merge_run_config",
        lambda _run_id, *, config_updates, conn=None: updates.append(config_updates) or config_updates,
    )
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "cancel_social_account_comments_run",
        lambda **kwargs: cancelled_children.append(kwargs) or {"status": "cancelled", **kwargs},
    )

    result = run_lifecycle.cancel_deferred_comments_followup(
        "run-1",
        cancelled_by="admin@example.com",
    )

    assert result["child_run_id"] == "comments-child-2"
    assert updates[0]["deferred_comments_followup"]["state"] == "cancelled"
    assert cancelled_children == [
        {
            "platform": "instagram",
            "account_handle": "bravotv",
            "run_id": "comments-child-2",
            "cancelled_by": "admin@example.com",
        }
    ]
    assert updates[0]["attached_followups"]["comments"]["state"] == "cancelling"
    assert updates[1]["attached_followups"]["comments"]["state"] == "cancelled"


def test_attached_child_cancel_failure_stays_retryable_and_nonterminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = {
        "deferred_comments_followup": {
            "state": "started",
            "platform": "instagram",
            "account_handle": "bravotv",
            "comments_run_id": "comments-child-fail",
        },
        "attached_followups": {"comments": {"run_id": "comments-child-fail", "status": "running"}},
    }
    updates: list[dict[str, Any]] = []
    monkeypatch.setattr(
        run_lifecycle.legacy.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"status": "cancelled", "config": config},
    )
    monkeypatch.setattr(
        run_lifecycle,
        "_merge_run_config",
        lambda _run_id, *, config_updates, conn=None: updates.append(config_updates) or config_updates,
    )
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "cancel_social_account_comments_run",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("modal unavailable")),
    )
    monkeypatch.setattr(run_lifecycle.legacy.logger, "exception", lambda *_args, **_kwargs: None)

    result = run_lifecycle.cancel_deferred_comments_followup("parent-run")

    assert result["child_cancellation"]["status"] == "cancel_failed"
    assert updates[0]["attached_followups"]["comments"]["state"] == "cancelling"
    retry = updates[1]["deferred_comments_followup"]["child_cancellation"]
    assert retry["state"] == "retryable"
    assert retry["child_run_id"] == "comments-child-fail"
    assert retry["next_attempt_at"]
    assert "attached_followups" not in updates[1]


@pytest.mark.parametrize(
    ("cancel_result", "expected_key"),
    [
        ({"run_id": "child-1", "status": "cancelled"}, "cancelled"),
        ({"run_id": "child-1", "status": "not_found"}, "not_found"),
    ],
)
def test_deferred_child_cancellation_recovery_persists_terminal_outcome(
    monkeypatch: pytest.MonkeyPatch,
    cancel_result: dict[str, str],
    expected_key: str,
) -> None:
    followup = {
        "state": "cancelled",
        "platform": "instagram",
        "account_handle": "bravotv",
        "child_cancellation": {
            "state": "pending",
            "child_run_id": "child-1",
            "cancelled_by": "admin@example.com",
        },
    }
    persisted_outcomes: list[dict[str, Any]] = []

    monkeypatch.setattr(
        run_lifecycle.legacy.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {
                "run_id": "parent-1",
                "config": {
                    "deferred_comments_followup": followup,
                    "attached_followups": {"comments": {"run_id": "child-1", "status": "running"}},
                },
            }
        ],
    )

    def fake_fetch_one(sql: str, params=None, **_kwargs):
        payload = json.loads((params or ["{}"])[0])
        if "returning config" in " ".join(sql.lower().split()):
            return {
                "config": {
                    **payload,
                    "attached_followups": {"comments": {"run_id": "child-1", "status": "running"}},
                }
            }
        persisted_outcomes.append(payload)
        return {"id": "parent-1"}

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        run_lifecycle,
        "_cancel_deferred_comments_child",
        lambda **_kwargs: cancel_result,
    )

    result = run_lifecycle.recover_deferred_comments_child_cancellations()

    assert result["claimed"] == 1
    assert result[expected_key] == 1
    final = persisted_outcomes[0]["deferred_comments_followup"]["child_cancellation"]
    assert final["state"] == expected_key
    assert persisted_outcomes[0]["attached_followups"]["comments"]["state"] == expected_key


def test_completed_parent_orphan_child_cancellation_is_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    followup = {
        "state": "started",
        "platform": "instagram",
        "account_handle": "bravotv",
        "child_cancellation": {
            "state": "retryable",
            "child_run_id": "orphan-child",
            "attempt_count": 1,
            "next_attempt_at": "2026-07-10T11:59:00+00:00",
            "cancel_reason": "parent_run_cancelled_during_deferred_followup_launch",
        },
    }
    observed_sql: list[str] = []

    def fake_fetch_all(sql: str, *_args, **_kwargs):
        normalized = " ".join(sql.lower().split())
        observed_sql.append(normalized)
        assert "status = 'cancelled'" not in normalized
        return [
            {
                "run_id": "completed-parent",
                "status": "completed",
                "config": {"deferred_comments_followup": followup},
            }
        ]

    def fake_fetch_one(sql: str, params=None, **_kwargs):
        normalized = " ".join(sql.lower().split())
        observed_sql.append(normalized)
        assert "status = 'cancelled'" not in normalized
        payload = json.loads((params or ["{}"])[0])
        if "returning config" in normalized:
            return {"config": payload}
        return {"id": "completed-parent"}

    monkeypatch.setattr(run_lifecycle.legacy, "_now_utc", lambda: now)
    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_all", fake_fetch_all)
    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        run_lifecycle,
        "_cancel_deferred_comments_child",
        lambda **_kwargs: {"run_id": "orphan-child", "status": "cancelled"},
    )

    result = run_lifecycle.recover_deferred_comments_child_cancellations()

    assert result == {
        "scanned": 1,
        "claimed": 1,
        "cancelled": 1,
        "not_found": 0,
        "retryable": 0,
        "skipped": 0,
    }
    assert len(observed_sql) == 3


def test_deferred_child_cancellation_recovery_respects_backoff_and_cas_loser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    future_followup = {
        "state": "cancelled",
        "child_cancellation": {
            "state": "retryable",
            "child_run_id": "child-future",
            "next_attempt_at": "2026-07-10T12:05:00+00:00",
        },
    }
    loser_followup = {
        "state": "cancelled",
        "child_cancellation": {"state": "pending", "child_run_id": "child-loser"},
    }
    monkeypatch.setattr(run_lifecycle.legacy, "_now_utc", lambda: now)
    monkeypatch.setattr(
        run_lifecycle.legacy.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {"run_id": "parent-future", "config": {"deferred_comments_followup": future_followup}},
            {"run_id": "parent-loser", "config": {"deferred_comments_followup": loser_followup}},
        ],
    )
    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        run_lifecycle,
        "_cancel_deferred_comments_child",
        lambda **_kwargs: pytest.fail("backoff/CAS losers must not issue cancellation"),
    )

    result = run_lifecycle.recover_deferred_comments_child_cancellations()

    assert result == {
        "scanned": 2,
        "claimed": 0,
        "cancelled": 0,
        "not_found": 0,
        "retryable": 0,
        "skipped": 2,
    }


@pytest.mark.parametrize("launch_succeeds", [True, False])
def test_claimed_deferred_followup_records_deterministic_launch_outcome(
    monkeypatch: pytest.MonkeyPatch,
    launch_succeeds: bool,
) -> None:
    claimed_config = {
        "deferred_comments_followup": {
            "state": "pending",
            "launch_claimed_at": "2026-07-10T12:00:00+00:00",
            "platform": "instagram",
            "account_handle": "bravotv",
        }
    }
    updates: list[dict[str, Any]] = []

    monkeypatch.setattr(
        run_lifecycle.legacy.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"status": "completed", "config": claimed_config},
    )
    monkeypatch.setattr(
        run_lifecycle._legacy_module(),
        "_shared_account_catalog_scrape_complete",
        lambda **_kwargs: True,
    )
    if launch_succeeds:
        monkeypatch.setattr(
            run_lifecycle._legacy_module(),
            "start_social_account_comments_scrape",
            lambda *_args, **_kwargs: {"run_id": "comments-run-1", "status": "queued"},
        )
    else:
        monkeypatch.setattr(
            run_lifecycle._legacy_module(),
            "start_social_account_comments_scrape",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("launch failed")),
        )
    monkeypatch.setattr(
        run_lifecycle,
        "_merge_run_config",
        lambda _run_id, *, config_updates, conn=None: updates.append(config_updates) or config_updates,
    )
    monkeypatch.setattr(run_lifecycle._legacy_module(), "_resolve_runtime_version_stamp", lambda: {})
    monkeypatch.setattr(run_lifecycle.legacy.logger, "exception", lambda *_args, **_kwargs: None)

    result = run_lifecycle._maybe_start_deferred_comments_followup(
        run_id="run-1",
        run_status="completed",
        run_config=claimed_config,
        summary={},
    )

    assert result is not None
    followup = updates[0]["deferred_comments_followup"]
    assert followup["state"] == ("started" if launch_succeeds else "failed")
    assert followup["launch_claimed_at"] is None


def test_forced_summary_recompute_reads_and_writes_on_supplied_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_conn = object()
    seen: list[tuple[str, object]] = []

    monkeypatch.setattr(run_lifecycle._legacy_module(), "_run_counter_columns_ready", lambda: False)

    def fake_fetch_one(sql: str, params=None, *, conn=None, **_kwargs):
        normalized = " ".join(sql.split()).lower()
        if "run-1" not in {str(param) for param in (params or [])}:
            return {}
        seen.append((normalized, conn))
        assert conn is lock_conn
        if "from social.scrape_jobs" in normalized:
            return {
                "stats": {
                    "total_jobs": 1,
                    "completed_jobs": 1,
                    "failed_jobs": 0,
                    "active_jobs": 0,
                    "items_found_total": 4,
                },
                "stage_counts": {},
            }
        if "select summary from social.scrape_runs" in normalized:
            return {"summary": {}}
        if "update social.scrape_runs" in normalized:
            return {"id": "run-1"}
        raise AssertionError(f"unexpected SQL: {normalized}")

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)

    summary = run_lifecycle._update_run_summary("run-1", force_recompute=True, conn=lock_conn)

    assert summary["completed_jobs"] == 1
    assert any("from social.scrape_jobs" in sql for sql, _conn in seen)
    assert all(conn is lock_conn for _sql, conn in seen)
