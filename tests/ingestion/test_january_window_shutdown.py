"""Lost grant acknowledgments and Darwin signal errors do not bypass cleanup."""

from types import SimpleNamespace

import pytest

from scripts.ingestion import recover_january_window as operator
from tests.ingestion.test_january_window_report import Connection


def test_committed_authorization_is_reconciled_without_another_grant(monkeypatch):
    monkeypatch.setattr(operator, "ACCOUNT", "bravotv")
    window = {
        "run_id": operator.RUN,
        "account": "bravotv",
        "expected_generation": 2,
        "generation": 3,
        "expected_intent_hash": operator.INTENT,
        "expected_workflow_version": "workflow",
        "source_revision": "source",
        "initiated_by": "instagram-one-hour-operator",
        "priority": "comments_first",
        "duration_seconds": 3600,
        "deadline_at": "2026-10-26T22:00:00+00:00",
    }

    class JournalConnection(Connection):
        def fetchone(self):
            return {"result": window}

    monkeypatch.setattr(operator, "connect", lambda: JournalConnection([]))
    recovered = operator.reconcile_window("source", "workflow")
    assert recovered is not None
    assert recovered is window and recovered["generation"] == 3
    assert recovered["deadline_at"] == "2026-10-26T22:00:00+00:00"
    with pytest.raises(RuntimeError, match="different window"):
        operator.reconcile_window("another-source", "workflow")


def test_bootout_error_acknowledgment_with_unloaded_service_is_reconciled(monkeypatch):
    calls = []

    def command(args, **_):
        calls.append(args)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(operator.subprocess, "run", command)
    assert operator.stop_native("gui/test/worker") is True
    assert [args[1] for args in calls] == ["bootout", "print"]


def test_bootout_waits_for_asynchronous_launchctl_unregistration(monkeypatch):
    calls = []
    outcomes = iter([0, 0, 113])

    def command(args, **_):
        calls.append(args[1])
        return SimpleNamespace(returncode=next(outcomes))

    monkeypatch.setattr(operator.subprocess, "run", command)
    monkeypatch.setattr(operator.time, "sleep", lambda _: None)
    assert operator.stop_native("gui/test/worker") is True
    assert calls == ["bootout", "print", "print"]


def test_delayed_unregistration_keeps_final_reconciliation_engaged(monkeypatch):
    attempted = []

    def delayed(_):
        assert attempted == [True]
        raise RuntimeError("Native bootout is not confirmed")

    monkeypatch.setattr(operator, "stop_native", delayed)
    with pytest.raises(RuntimeError, match="not confirmed"):
        operator.shutdown_native_attempt("gui/test/worker", lambda: attempted.append(True))
    # The same branch used by main's finally must run despite the raised acknowledgment.
    assert attempted == [True]
    states = iter([0, 113, 113])
    monkeypatch.setattr(operator.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=next(states)))
    monkeypatch.setattr(operator, "processes", lambda _: {12, 13})
    monkeypatch.setattr(operator, "alive", lambda _: False)
    monkeypatch.setattr(operator, "grants", lambda: [])
    monkeypatch.setattr(operator.time, "sleep", lambda _: None)
    operator.wait_native_drain("gui/test/worker", 12)


def test_darwin_eperm_on_exited_child_still_reaps_and_proves_absence(monkeypatch):
    waits = []
    child = SimpleNamespace(pid=123, returncode=125, wait=lambda **kwargs: waits.append(kwargs))
    monkeypatch.setattr(operator, "processes", lambda _: {123, 124})
    monkeypatch.setattr(operator, "alive", lambda _: False)

    def denied(*_):
        raise PermissionError

    monkeypatch.setattr(operator.os, "killpg", denied)
    result = operator.stop_owned_child(child)
    assert waits == [{"timeout": 20}]
    assert result["owned_processes_absent"] == [123, 124]
    assert result["signal_errors"][0]["error_class"] == "PermissionError"


def test_surviving_descendant_keeps_restoration_blocked(monkeypatch):
    child = SimpleNamespace(pid=123, returncode=0, wait=lambda **_: None)
    monkeypatch.setattr(operator, "processes", lambda _: {123, 124})
    monkeypatch.setattr(operator, "alive", lambda pid: pid == 124)
    monkeypatch.setattr(operator.os, "killpg", lambda *_: None)
    moments = iter([0, 4])
    monkeypatch.setattr(operator.time, "monotonic", lambda: next(moments))
    with pytest.raises(RuntimeError, match="process remains"):
        operator.stop_owned_child(child)


def test_window_progress_unions_ids_and_proofs_without_alternating_subset_credit():
    frontiers = {}
    first = operator.merge_frontier(frontiers, "post", {"a", "b"}, {"a"}, {"parent"}, 2)
    operator.merge_frontier(frontiers, "post", {"a"}, {"a"}, set(), 1)
    assert operator.merge_frontier(frontiers, "post", {"b"}, set(), {"parent"}, 1) == first
    improved = operator.merge_frontier(frontiers, "post", {"b", "c"}, {"b"}, {"parent", "child"}, 2)
    assert improved == {"observed_ids": 3, "supported_ids": 2, "proof_count": 2, "best_observed_total": 2}


def test_comment_children_are_drained_before_media_phase_command(monkeypatch):
    from scripts.ingestion.local_worker import advance_recovery_media
    from trr_backend.ingestion import bounded_recovery_phase

    events = []
    active = {"child": object()}
    dispatch = SimpleNamespace(active=active)

    def drain(store, *, finish):
        assert finish is True
        events.append("drained")
        active.clear()

    dispatch.drain = drain

    def transition(*_, **kwargs):
        assert not active and events == ["drained"]
        events.append("media")
        return kwargs

    monkeypatch.setattr(bounded_recovery_phase, "transition", transition)
    result = advance_recovery_media(
        object(),
        dispatch,
        {"run_id": "run", "account": "bravotv", "generation": 5},
        {"window_id": "window", "source_revision": "source", "expected_intent_hash": "digest"},
    )
    assert result is not None
    assert result["to_phase"] == "media" and events == ["drained", "media"]


def test_failed_comment_drain_cannot_publish_media_phase(monkeypatch):
    from scripts.ingestion.local_worker import advance_recovery_media
    from trr_backend.ingestion import bounded_recovery_phase

    monkeypatch.setattr(bounded_recovery_phase, "transition", lambda *a, **k: pytest.fail("must not transition"))
    dispatch = SimpleNamespace(active={"child": object()}, drain=lambda *a, **k: None)
    with pytest.raises(RuntimeError, match="children remain"):
        advance_recovery_media(object(), dispatch, {}, {})
