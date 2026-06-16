"""Focused run-lifecycle repository tests for extracted control-plane seams."""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from psycopg2 import OperationalError

import trr_backend.repositories.social_season_analytics as social_repo
import trr_backend.socials.control_plane.run_lifecycle as run_lifecycle


def test_legacy_set_run_status_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(run_lifecycle, "_set_run_status", lambda run_id, status: calls.append((run_id, status)))

    social_repo._set_run_status("run-1", "running")

    assert calls == [("run-1", "running")]


def test_legacy_create_run_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = "run-1"
    captured: dict[str, object] = {}

    def _fake_create_run(context, **kwargs):
        captured["context"] = context
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setattr(run_lifecycle, "_create_run", _fake_create_run)

    payload = social_repo._create_run(
        None,
        source_scope="bravo",
        initiated_by="admin@test",
        config={"sync_session_id": "sync-1"},
        status="queued",
    )

    assert payload == expected
    assert captured["context"] is None
    assert captured["kwargs"] == {
        "source_scope": "bravo",
        "initiated_by": "admin@test",
        "config": {"sync_session_id": "sync-1"},
        "status": "queued",
    }


def test_legacy_update_run_summary_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"total_jobs": 6}

    monkeypatch.setattr(
        run_lifecycle,
        "_update_run_summary",
        lambda run_id, force_recompute=False: {"run_id": run_id, "force_recompute": force_recompute, **expected},
    )

    summary = social_repo._update_run_summary("run-1", force_recompute=True)

    assert summary == {"run_id": "run-1", "force_recompute": True, **expected}


def test_legacy_finalize_run_status_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"status": "completed"}

    monkeypatch.setattr(
        run_lifecycle,
        "_finalize_run_status",
        lambda run_id, force_recompute=False: {
            "run_id": run_id,
            "force_recompute": force_recompute,
            **expected,
        },
    )

    summary = social_repo._finalize_run_status("run-1", force_recompute=True)

    assert summary == {"run_id": "run-1", "force_recompute": True, **expected}


def test_legacy_reconcile_run_summaries_delegates_to_control_plane_run_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"reconciled_runs": 1, "run_ids": ["run-1"]}

    monkeypatch.setattr(
        run_lifecycle,
        "reconcile_run_summaries",
        lambda **kwargs: {"kwargs": kwargs, **expected},
    )

    payload = social_repo.reconcile_run_summaries(run_ids=["run-1"], limit=25)

    assert payload == {"kwargs": {"run_ids": ["run-1"], "limit": 25}, **expected}


def test_create_run_writes_sync_session_metadata_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def _fake_fetch_one(sql: str, params: list[object]):
        calls.append((" ".join(sql.lower().split()), list(params)))
        if "insert into social.scrape_runs" in sql.lower():
            return {"id": "run-1"}
        if "update social.scrape_runs" in sql.lower():
            return {"id": "run-1"}
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(social_repo, "_column_exists", lambda *_args, **_kwargs: True)

    payload = social_repo._create_run(
        None,
        source_scope="bravo",
        initiated_by="admin@test",
        config={
            "sync_session_id": "11111111-1111-1111-1111-111111111111",
            "pass_kind": "comments_only",
            "pass_attempt": 2,
            "pass_sequence": 3,
        },
        status="queued",
    )

    assert payload == "run-1"
    assert len(calls) == 2
    assert "insert into social.scrape_runs" in calls[0][0]
    assert "update social.scrape_runs" in calls[1][0]
    assert calls[1][1] == [
        "11111111-1111-1111-1111-111111111111",
        "comments_only",
        2,
        3,
        "run-1",
    ]


def test_create_run_normalizes_pending_run_status_for_scrape_runs_constraint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    insert_params: list[object] = []

    def _fake_fetch_one(sql: str, params: list[object]):
        if "insert into social.scrape_runs" in sql.lower():
            insert_params.extend(params)
            return {"id": "run-1"}
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(social_repo, "_column_exists", lambda *_args, **_kwargs: False)

    payload = social_repo._create_run(
        None,
        source_scope="bravo",
        initiated_by="admin@test",
        config={"stage": "instagram_comments_scrapling"},
        status="pending",
    )

    assert payload == "run-1"
    assert insert_params[3] == "queued"
    assert insert_params[-1] == "queued"


def test_build_run_summary_payload_normalizes_stage_counts() -> None:
    payload = social_repo._build_run_summary_payload(
        total_jobs="4",
        completed_jobs=1,
        failed_jobs=None,
        active_jobs="2",
        items_found_total="7",
        stage_counts={"posts": {"total": "2", "completed": 1, "failed": 0, "active": "1"}},
    )

    assert payload == {
        "total_jobs": 4,
        "completed_jobs": 1,
        "failed_jobs": 0,
        "active_jobs": 2,
        "items_found_total": 7,
        "stage_counts": {"posts": {"total": 2, "completed": 1, "failed": 0, "active": 1}},
    }


def test_set_run_status_invalidates_week_detail_cache_on_terminal_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invalidation_calls: list[str] = []
    monkeypatch.setattr(social_repo.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "run-1"})
    social_repo.register_week_detail_cache_invalidator(lambda: invalidation_calls.append("called"))
    try:
        social_repo._set_run_status("run-1", "running")
        assert invalidation_calls == []
        social_repo._set_run_status("run-1", "completed")
        assert invalidation_calls == ["called"]
    finally:
        social_repo.register_week_detail_cache_invalidator(None)


def test_set_run_status_clears_terminal_timestamps_when_reopened(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_one(sql: str, params: list[object], **_kwargs):
        captured["sql"] = " ".join(sql.lower().split())
        captured["params"] = list(params)
        return {"id": "run-1"}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    social_repo._set_run_status("run-1", "running")

    assert "when %s in ('queued', 'pending', 'retrying', 'running') then null" in str(captured["sql"])
    assert captured["params"] == ["running", "running", "running", "running", "running", "running", "run-1"]


def test_update_run_summary_prefers_incremental_counter_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(social_repo, "_run_counter_columns_ready", lambda: True)

    def _fake_fetch_one(sql: str, params: list[object]):  # noqa: ARG001
        normalized = " ".join(sql.lower().split())
        calls.append(normalized)
        if "from social.scrape_runs where id = %s" in normalized and "select total_jobs" in normalized:
            return {
                "total_jobs": 6,
                "completed_jobs": 3,
                "failed_jobs": 1,
                "active_jobs": 2,
                "items_found_total": 77,
                "stage_counts": {"posts": {"total": 3, "completed": 2, "failed": 0, "active": 1}},
            }
        if "update social.scrape_runs set summary = %s::jsonb" in normalized:
            return {"id": "run-1"}
        if "from social.scrape_jobs" in normalized:
            raise AssertionError("full scrape_jobs aggregation should not run in incremental mode")
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    summary = social_repo._update_run_summary("run-1")

    assert summary["total_jobs"] == 6
    assert summary["completed_jobs"] == 3
    assert summary["failed_jobs"] == 1
    assert summary["active_jobs"] == 2
    assert summary["items_found_total"] == 77
    assert summary["stage_counts"]["posts"]["active"] == 1
    assert any("select total_jobs" in call for call in calls)


def test_recompute_run_summary_ignores_superseded_instagram_comments_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_one(sql: str, params: list[object]):
        captured["sql"] = " ".join(sql.lower().split())
        captured["params"] = list(params)
        return {
            "stats": {
                "total_jobs": 2,
                "completed_jobs": 2,
                "failed_jobs": 0,
                "active_jobs": 0,
                "items_found_total": 44,
            },
            "stage_counts": {
                social_repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE: {
                    "total": 2,
                    "completed": 2,
                    "failed": 0,
                    "active": 0,
                }
            },
        }

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", _fake_fetch_one)

    summary = run_lifecycle._recompute_run_summary_from_jobs("run-1")

    assert summary["failed_jobs"] == 0
    assert summary["stage_counts"][social_repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE]["failed"] == 0
    assert captured["params"] == [social_repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE, "run-1"]
    sql_text = str(captured["sql"])
    assert "superseded_by_comments_rebalance" in sql_text
    assert "comments_retry_rebalance_source_job_id" in sql_text
    assert "where not superseded_by_comments_rebalance" in sql_text


def test_preserve_protected_run_summary_fields_carries_audit_keys() -> None:
    summary = {"total_jobs": 4, "completed_jobs": 2, "failed_jobs": 0, "active_jobs": 2}
    existing = {
        "total_jobs": 99,  # stale count must NOT override the recomputed value
        "cancelled_by": "comments_guarded_restart",
        "cancel_requested_at": "2026-06-16T00:00:00+00:00",
        "cancel_reason": "public_comments_guarded_restart",
        "guarded_restart": True,
        "guarded_restart_from_run_id": "old-run",
        "guarded_restart_to_run_id": "new-run",
        "public_blocked_pause": {"checked": 25, "blocked": 20},
        "dispatch_control": {"pause_after_current": True, "pause_reason": "public_blocked_repeated"},
        "noise_field": "should-not-copy",
    }

    merged = run_lifecycle._preserve_protected_run_summary_fields(summary, existing)

    # Recomputed counts win.
    assert merged["total_jobs"] == 4
    # Protected audit fields preserved.
    assert merged["cancelled_by"] == "comments_guarded_restart"
    assert merged["cancel_requested_at"] == "2026-06-16T00:00:00+00:00"
    assert merged["cancel_reason"] == "public_comments_guarded_restart"
    assert merged["guarded_restart"] is True
    assert merged["guarded_restart_from_run_id"] == "old-run"
    assert merged["guarded_restart_to_run_id"] == "new-run"
    assert merged["public_blocked_pause"] == {"checked": 25, "blocked": 20}
    assert merged["dispatch_control"]["pause_after_current"] is True
    # Non-protected keys are not carried forward.
    assert "noise_field" not in merged


def test_preserve_protected_run_summary_fields_skips_missing_and_null() -> None:
    summary = {"total_jobs": 1}
    existing = {"cancelled_by": None, "dispatch_control": {"pause_after_current": True}}

    merged = run_lifecycle._preserve_protected_run_summary_fields(summary, existing)

    # Null protected values are skipped; present ones are copied.
    assert "cancelled_by" not in merged
    assert merged["dispatch_control"] == {"pause_after_current": True}


def test_update_run_summary_incremental_preserves_audit_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(social_repo, "_run_counter_columns_ready", lambda: True)
    captured: dict[str, object] = {}

    def _fake_fetch_one(sql: str, params: list[object]):  # noqa: ARG001
        normalized = " ".join(sql.lower().split())
        if "from social.scrape_runs where id = %s" in normalized and "select total_jobs" in normalized:
            # The recompute read must include the existing summary.
            assert ", summary" in normalized
            return {
                "total_jobs": 6,
                "completed_jobs": 3,
                "failed_jobs": 1,
                "active_jobs": 2,
                "items_found_total": 77,
                "stage_counts": {"posts": {"total": 3, "completed": 2, "failed": 0, "active": 1}},
                "summary": {
                    "total_jobs": 6,
                    "cancelled_by": "comments_guarded_restart",
                    "cancel_reason": "public_comments_guarded_restart",
                    "guarded_restart": True,
                    "guarded_restart_to_run_id": "new-run-id",
                    "dispatch_control": {"pause_after_current": True},
                },
            }
        if "update social.scrape_runs set summary = %s::jsonb" in normalized:
            captured["written_summary"] = social_repo.json.loads(params[0])
            return {"id": "run-1"}
        return {}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    summary = social_repo._update_run_summary("run-1")

    # Count fields refreshed.
    assert summary["completed_jobs"] == 3
    assert summary["active_jobs"] == 2
    # Audit fields preserved in both the returned dict and the persisted summary.
    assert summary["cancelled_by"] == "comments_guarded_restart"
    assert summary["cancel_reason"] == "public_comments_guarded_restart"
    assert summary["guarded_restart"] is True
    assert summary["guarded_restart_to_run_id"] == "new-run-id"
    assert summary["dispatch_control"] == {"pause_after_current": True}
    written = captured["written_summary"]
    assert written["cancelled_by"] == "comments_guarded_restart"
    assert written["dispatch_control"] == {"pause_after_current": True}


def test_update_run_summary_force_recompute_preserves_audit_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_lifecycle.legacy, "_run_counter_columns_ready", lambda: True)
    existing_summary = {
        "total_jobs": 2,
        "cancelled_by": "comments_guarded_restart",
        "cancel_requested_at": "2026-06-16T00:00:00+00:00",
        "cancel_reason": "public_comments_guarded_restart",
        "guarded_restart": True,
        "guarded_restart_from_run_id": "old-run",
        "guarded_restart_to_run_id": "new-run",
        "public_blocked_pause": {"checked": 25, "blocked": 20},
        "dispatch_control": {"pause_after_current": True, "pause_reason": "public_blocked_repeated"},
    }
    captured: dict[str, object] = {}

    # Recompute aggregation read (plain fetch_one).
    def _fake_fetch_one(sql: str, params: list[object]):  # noqa: ARG001
        return {
            "stats": {
                "total_jobs": 2,
                "completed_jobs": 1,
                "failed_jobs": 0,
                "active_jobs": 1,
                "items_found_total": 10,
            },
            "stage_counts": {
                social_repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE: {
                    "total": 2,
                    "completed": 1,
                    "failed": 0,
                    "active": 1,
                }
            },
        }

    # _persist_run_counters_and_summary reads existing summary + writes via cursor.
    def _fake_fetch_one_with_cursor(cur: object, sql: str, params: list[object]):  # noqa: ARG001
        normalized = " ".join(sql.lower().split())
        if "select summary from social.scrape_runs" in normalized:
            return {"summary": existing_summary}
        if "update social.scrape_runs" in normalized:
            captured["written_summary"] = social_repo.json.loads(params[-2])
            return {"id": str(params[-1])}
        return {}

    @contextmanager
    def _fake_db_cursor(*, conn=None):  # noqa: ARG001
        yield object()

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)
    monkeypatch.setattr(run_lifecycle.legacy.pg, "db_cursor", _fake_db_cursor)

    summary = run_lifecycle._update_run_summary(
        "11111111-1111-1111-1111-111111111111",
        force_recompute=True,
        conn=object(),
    )

    # Count fields recomputed from jobs.
    assert summary["completed_jobs"] == 1
    assert summary["active_jobs"] == 1
    written = captured["written_summary"]
    # Audit fields preserved through the recompute persistence.
    assert written["cancelled_by"] == "comments_guarded_restart"
    assert written["cancel_reason"] == "public_comments_guarded_restart"
    assert written["guarded_restart"] is True
    assert written["guarded_restart_from_run_id"] == "old-run"
    assert written["guarded_restart_to_run_id"] == "new-run"
    assert written["public_blocked_pause"] == {"checked": 25, "blocked": 20}
    assert written["dispatch_control"]["pause_after_current"] is True
    # And recomputed counts still win over the stale existing summary value.
    assert written["total_jobs"] == 2
    assert written["completed_jobs"] == 1


def test_finalize_run_status_reuses_lock_connection_for_all_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_conn = object()
    seen_fetch_conns: list[object | None] = []
    seen_lock_pools: list[str] = []

    @contextmanager
    def fake_advisory_lock(lock_key, *, label, pool_name="default"):
        del lock_key, label
        seen_lock_pools.append(pool_name)
        yield lock_conn

    def fake_fetch_one(sql: str, params=None, *, conn=None):
        del params
        seen_fetch_conns.append(conn)
        normalized = " ".join(sql.split()).lower()
        if "select status, config from social.scrape_runs" in normalized:
            return {"status": "running", "config": {"pipeline_ingest_mode": "manual"}}
        raise AssertionError(f"Unexpected query: {normalized}")

    monkeypatch.setattr(run_lifecycle.legacy.pg, "advisory_session_lock", fake_advisory_lock)
    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(
        run_lifecycle,
        "_update_run_summary",
        lambda *_args, **_kwargs: {"active_jobs": 0, "failed_jobs": 0, "stage_counts": {}},
    )
    monkeypatch.setattr(
        run_lifecycle,
        "_run_job_status_breakdown",
        lambda *_args, **_kwargs: {"running_jobs": 0, "queued_jobs": 0, "cancelling_jobs": 0},
    )
    monkeypatch.setattr(run_lifecycle, "_set_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_lifecycle, "_maybe_start_deferred_comments_followup", lambda **_kwargs: None)
    monkeypatch.setattr(run_lifecycle.legacy, "_resolve_pipeline_ingest_mode", lambda value: value)
    monkeypatch.setattr(
        run_lifecycle.legacy,
        "_shared_catalog_fetch_has_terminal_error",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(run_lifecycle.legacy, "_column_exists", lambda *_args, **_kwargs: False)

    run_lifecycle._finalize_run_status("run-1")

    assert seen_fetch_conns == [lock_conn]
    assert seen_lock_pools == ["social_control"]


def test_finalize_run_status_force_recomputes_before_failed_terminal_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_conn = object()
    update_calls: list[bool] = []
    statuses: list[str] = []

    @contextmanager
    def fake_advisory_lock(lock_key, *, label, pool_name="default"):
        del lock_key, label, pool_name
        yield lock_conn

    def fake_fetch_one(sql: str, params=None, *, conn=None):
        del params
        assert conn is lock_conn
        normalized = " ".join(sql.split()).lower()
        if "select status, config from social.scrape_runs" in normalized:
            return {"status": "running", "config": {"pipeline_ingest_mode": "manual"}}
        if "select sync_session_id::text" in normalized:
            return {"sync_session_id": None}
        raise AssertionError(f"Unexpected query: {normalized}")

    def fake_update_summary(_run_id: str, *, force_recompute: bool = False, conn=None):
        assert conn is lock_conn
        update_calls.append(force_recompute)
        if not force_recompute:
            return {"active_jobs": 0, "failed_jobs": 1, "stage_counts": {}}
        return {"active_jobs": 0, "failed_jobs": 0, "stage_counts": {}}

    monkeypatch.setattr(run_lifecycle.legacy.pg, "advisory_session_lock", fake_advisory_lock)
    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(run_lifecycle, "_update_run_summary", fake_update_summary)
    monkeypatch.setattr(
        run_lifecycle,
        "_run_job_status_breakdown",
        lambda *_args, **_kwargs: {"running_jobs": 0, "queued_jobs": 0, "cancelling_jobs": 0},
    )
    monkeypatch.setattr(run_lifecycle, "_set_run_status", lambda _run_id, status, **_kwargs: statuses.append(status))
    monkeypatch.setattr(run_lifecycle, "_maybe_start_deferred_comments_followup", lambda **_kwargs: None)
    monkeypatch.setattr(run_lifecycle.legacy, "_resolve_pipeline_ingest_mode", lambda value: value)
    monkeypatch.setattr(
        run_lifecycle.legacy,
        "_shared_catalog_fetch_has_terminal_error",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(run_lifecycle.legacy, "_column_exists", lambda *_args, **_kwargs: False)

    run_lifecycle._finalize_run_status("run-1")

    assert update_calls == [False, True]
    assert statuses == ["completed"]


def test_finalize_run_status_lock_contention_reads_from_social_control_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_fetch_pools: list[str] = []

    @contextmanager
    def fake_advisory_lock(lock_key, *, label, pool_name="default"):
        del lock_key, label, pool_name
        raise run_lifecycle.legacy.pg.AdvisoryLockUnavailable(123)
        yield  # pragma: no cover

    def fake_fetch_one(sql: str, params=None, *, pool_name="default"):
        del params
        assert "select status from social.scrape_runs" in " ".join(sql.split()).lower()
        seen_fetch_pools.append(pool_name)
        return {"status": "running"}

    monkeypatch.setattr(run_lifecycle.legacy.pg, "advisory_session_lock", fake_advisory_lock)
    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one", fake_fetch_one)

    payload = run_lifecycle._finalize_run_status("run-1")

    assert payload == {"status": "running"}
    assert seen_fetch_pools == ["social_control"]


def test_finalize_run_status_defers_connection_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    @contextmanager
    def fake_advisory_lock(lock_key, *, label, pool_name="default"):
        del lock_key, label, pool_name
        raise OperationalError("server closed the connection unexpectedly")
        yield  # pragma: no cover

    monkeypatch.setattr(run_lifecycle.legacy.pg, "advisory_session_lock", fake_advisory_lock)

    payload = run_lifecycle._finalize_run_status("run-1")

    assert payload["status"] == "finalize_deferred"
    assert payload["finalize_deferred"] is True
    assert "server closed the connection unexpectedly" in str(payload["error"])
