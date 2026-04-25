"""Tests for bounded local social control-plane background queues."""

from __future__ import annotations

from threading import Event

from trr_backend.socials.control_plane import background_tasks


def test_background_task_queue_rejects_duplicate_key_while_running() -> None:
    started = Event()
    release = Event()

    def _blocked_task() -> None:
        started.set()
        release.wait(timeout=2)

    first = background_tasks.submit_named_background_task(
        group="test-duplicate-running",
        key="run-1",
        thread_name="test-duplicate-running:run-1",
        target=_blocked_task,
    )
    assert first["submitted"] is True
    assert first["state"] == "queued"
    assert started.wait(timeout=1)

    duplicate = background_tasks.submit_named_background_task(
        group="test-duplicate-running",
        key="run-1",
        thread_name="test-duplicate-running:run-1-again",
        target=_blocked_task,
    )

    release.set()

    assert duplicate["submitted"] is False
    assert duplicate["state"] == "duplicate"


def test_background_task_queue_runs_second_distinct_key() -> None:
    ran_first = Event()
    ran_second = Event()

    first = background_tasks.submit_named_background_task(
        group="test-distinct-keys",
        key="run-1",
        thread_name="test-distinct-keys:run-1",
        target=ran_first.set,
    )
    second = background_tasks.submit_named_background_task(
        group="test-distinct-keys",
        key="run-2",
        thread_name="test-distinct-keys:run-2",
        target=ran_second.set,
    )

    assert first["submitted"] is True
    assert second["submitted"] is True
    assert ran_first.wait(timeout=1)
    assert ran_second.wait(timeout=1)


def test_background_task_snapshot_exposes_group_counts() -> None:
    started = Event()
    release = Event()

    def _blocked_task() -> None:
        started.set()
        release.wait(timeout=2)

    submitted = background_tasks.submit_named_background_task(
        group="test-snapshot",
        key="run-1",
        thread_name="test-snapshot:run-1",
        target=_blocked_task,
    )
    assert submitted["submitted"] is True
    assert started.wait(timeout=1)

    snapshot = background_tasks.background_task_snapshot()
    release.set()

    group = snapshot["groups"]["test-snapshot"]
    assert group["active_count"] == 1
    assert group["queued_count"] == 0
    assert group["queue_size"] == 0
    assert group["exception_count"] == 0

