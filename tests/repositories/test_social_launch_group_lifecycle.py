from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any

import pytest

from trr_backend.socials.control_plane import shared_accounts
from trr_backend.socials.pipelines.account_catalog import launch, progress


def test_launch_owner_uses_transaction_advisory_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    @contextmanager
    def _connection(**kwargs):
        captured["connection_kwargs"] = kwargs
        yield object()

    @contextmanager
    def _cursor(**kwargs):
        captured["cursor_kwargs"] = kwargs
        yield object()

    def _fetch(_cursor_value, query, params):
        captured["query"] = query
        captured["params"] = params
        return {"locked": True}

    monkeypatch.setattr(launch.pg, "db_connection", _connection)
    monkeypatch.setattr(launch.pg, "db_cursor", _cursor)
    monkeypatch.setattr(launch.pg, "fetch_one_with_cursor", _fetch)

    with launch._catalog_launch_group_transaction_lock("group-1") as acquired:
        assert acquired is True

    assert "pg_try_advisory_xact_lock" in captured["query"]
    assert captured["connection_kwargs"] == {"label": "catalog-launch-group-owner"}


def _patch_finalizer_state(monkeypatch: pytest.MonkeyPatch, state: dict[str, Any]) -> None:
    monkeypatch.setattr(launch, "_sync_core_overrides", lambda: None)
    monkeypatch.setattr(
        launch,
        "_catalog_launch_parent_snapshot",
        lambda _run_id: {
            "id": _run_id,
            "run_id": _run_id,
            "status": state["status"],
            "launch_state": state["launch_state"],
            "launch_group_id": "group-1",
            "config": {
                "launch_state": state["launch_state"],
                "launch_group_id": "group-1",
                "selected_tasks": ["post_details", "comments", "media"],
            },
        },
    )
    monkeypatch.setattr(
        launch,
        "_catalog_launch_parent_cancelled",
        lambda _run_id: state["status"] == "cancelled" or state["launch_state"] == "cancelled",
    )

    def _cas(*, from_states, to_state, **_kwargs):
        if state["status"] == "cancelled" or state["launch_state"] not in set(from_states):
            return {}
        state["launch_state"] = to_state
        return {"id": "run-1", "status": state["status"], "config": {"launch_state": to_state}}

    monkeypatch.setattr(launch, "_cas_catalog_launch_state", _cas)
    monkeypatch.setattr(
        launch,
        "_cancel_launch_group_if_parent_cancelled",
        lambda *, run_id, **_kwargs: (
            launch._catalog_launch_parent_result(launch._catalog_launch_parent_snapshot(str(run_id)))
            if state["status"] == "cancelled" or state["launch_state"] == "cancelled"
            else None
        ),
    )
    monkeypatch.setattr(launch, "_merge_catalog_run_config", lambda **_kwargs: {})
    monkeypatch.setattr(launch, "_record_social_account_catalog_launch_failure", lambda **_kwargs: None)


def test_finalize_timeout_keeps_single_owner_until_original_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: dict[str, Any] = {"status": "queued", "launch_state": "reserved"}
    owner_active = False
    owner_guard = threading.Lock()
    release = threading.Event()
    finished = threading.Event()
    launch_calls = 0
    _patch_finalizer_state(monkeypatch, state)

    @contextmanager
    def _lock(_launch_group_id: str):
        nonlocal owner_active
        with owner_guard:
            acquired = not owner_active
            if acquired:
                owner_active = True
        try:
            yield acquired
        finally:
            if acquired:
                with owner_guard:
                    owner_active = False

    def _slow_launch(*_args, **_kwargs):
        nonlocal launch_calls
        launch_calls += 1
        release.wait(timeout=2)
        finished.set()
        return {"run_id": "run-1"}

    monkeypatch.setattr(launch, "_catalog_launch_group_transaction_lock", _lock)
    monkeypatch.setattr(launch, "_room_callable", lambda *_args: _slow_launch)
    monkeypatch.setattr(launch, "_catalog_finalize_launch_timeout_seconds", lambda: 0.01)

    first = launch.finalize_social_account_catalog_backfill_launch(
        "instagram", "bravotv", run_id="run-1", launch_group_id="group-1"
    )
    second = launch.finalize_social_account_catalog_backfill_launch(
        "instagram", "bravotv", run_id="run-1", launch_group_id="group-1"
    )

    assert first["launch_state"] == "finalizing"
    assert second["finalizer_owner_active"] is True
    assert launch_calls == 1

    release.set()
    assert finished.wait(timeout=2)
    for _ in range(100):
        if state["launch_state"] == "ready":
            break
        time.sleep(0.01)
    assert state["launch_state"] == "ready"


def test_finalize_does_not_attach_ready_after_parent_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: dict[str, Any] = {"status": "queued", "launch_state": "reserved"}
    _patch_finalizer_state(monkeypatch, state)

    @contextmanager
    def _lock(_launch_group_id: str):
        yield True

    def _create_child_then_cancel(*_args, **_kwargs):
        state.update(status="cancelled", launch_state="cancelled")
        return {"run_id": "run-1", "comments_run_id": "child-1"}

    monkeypatch.setattr(launch, "_catalog_launch_group_transaction_lock", _lock)
    monkeypatch.setattr(launch, "_room_callable", lambda *_args: _create_child_then_cancel)
    monkeypatch.setattr(launch, "_catalog_finalize_launch_timeout_seconds", lambda: 0)

    result = launch.finalize_social_account_catalog_backfill_launch(
        "instagram", "bravotv", run_id="run-1", launch_group_id="group-1"
    )

    assert result["launch_state"] == "cancelled"
    assert state["launch_state"] == "cancelled"


def test_launch_group_cancel_includes_unattached_child_and_remote_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queries: list[str] = []

    def _fetch_one(query, _params=None, **_kwargs):
        queries.append(query)
        if "select id::text as id, config" in query:
            return {"id": "parent-1", "config": {"launch_group_id": "group-1"}}
        return {"id": "job-child"}

    def _fetch_all(query, _params=None, **_kwargs):
        queries.append(query)
        if "from social.scrape_runs" in query:
            return [
                {"id": "parent-1", "status": "running", "config": {}},
                {"id": "child-1", "status": "running", "config": {}},
            ]
        return [
            {
                "id": "job-child",
                "run_id": "child-1",
                "status": "running",
                "remote_invocation_id": "call-child",
            }
        ]

    monkeypatch.setattr(shared_accounts.pg, "fetch_one", _fetch_one)
    monkeypatch.setattr(shared_accounts.pg, "fetch_all", _fetch_all)
    monkeypatch.setattr(
        shared_accounts.pg,
        "execute_returning",
        lambda query, *_args, **_kwargs: (
            [{"id": "job-child"}] if "update social.scrape_jobs" in query else [{"id": "parent-1"}]
        ),
    )
    monkeypatch.setattr(
        shared_accounts,
        "cancel_modal_function_call",
        lambda call_id: {
            "function_call_id": call_id,
            "cancel_requested": True,
            "cancel_requested_at": "now",
            "checked_at": "now",
            "draining": True,
            "inspection": {"status": "running", "checked_at": "now"},
        },
    )

    result = shared_accounts.cancel_social_account_catalog_run(
        platform="instagram",
        account_handle="bravotv",
        run_id="parent-1",
        cancelled_by="admin",
    )

    assert result["cancelled_run_ids"] == ["parent-1", "child-1"]
    assert result["draining_remote_call_ids"] == ["call-child"]
    assert result["remote_drain_complete"] is False
    assert any("config->>'launch_group_id'" in query for query in queries)


def test_all_parts_status_waits_for_every_selected_lane() -> None:
    run_config = {"effective_selected_tasks": ["post_details", "comments", "media"]}
    pending = progress._all_parts_status_payload(
        run_config=run_config,
        payload={
            "run_status": "completed",
            "stage_graph": {"detail_refresh": {"status": "completed"}},
            "attached_followups": {
                "comments": {"status": "running"},
                "media": {"status": "completed"},
            },
            "media_completion": {"completed": True},
        },
    )
    complete = progress._all_parts_status_payload(
        run_config=run_config,
        payload={
            "run_status": "completed",
            "stage_graph": {"detail_refresh": {"status": "completed"}},
            "attached_followups": {
                "comments": {"status": "completed"},
                "media": {"status": "completed"},
            },
            "media_completion": {"completed": True},
        },
    )

    assert pending["all_parts_status"] == "running"
    assert pending["all_parts_completed"] is False
    assert complete["all_parts_status"] == "completed"
    assert complete["all_parts_completed"] is True

