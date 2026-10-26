"""Lost grant acknowledgments and Darwin signal errors do not bypass cleanup."""

from types import SimpleNamespace

import pytest

from scripts.ingestion import recover_january_window as operator
from tests.ingestion.test_january_window_report import Connection


def test_committed_authorization_is_reconciled_without_another_grant(monkeypatch):
    window = {
        "run_id": operator.RUN,
        "account": "bravotv",
        "expected_generation": 2,
        "generation": 3,
        "expected_intent_hash": operator.INTENT,
        "expected_workflow_version": "workflow",
        "source_revision": "source",
        "initiated_by": "january-one-hour-operator",
        "priority": "comments_first",
        "duration_seconds": 3600,
        "deadline_at": "2026-10-26T22:00:00+00:00",
    }

    class JournalConnection(Connection):
        def fetchone(self):
            return {"result": window}

    monkeypatch.setattr(operator, "connect", lambda: JournalConnection([]))
    recovered = operator.reconcile_window("source", "workflow")
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
