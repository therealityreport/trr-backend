"""Abort origin evidence must never weaken cancellation or expose error text."""

import json
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from scripts.ingestion.local_driver import (
    CoordinatorLease,
    DiagnosticAbort,
    HeartbeatSample,
    foreground_phase,
    request_abort,
)
from scripts.ingestion.local_worker import Interrupted, LocalDispatch, run_partition, start_abort_reporter
from trr_backend.ingestion.domain import LostOwnership


def test_first_cause_survives_cleanup_and_excludes_error_messages():
    abort = DiagnosticAbort()
    error = type("OperationalError", (Exception,), {"sqlstate": "08006"})("secret DSN/password")
    request_abort(
        abort,
        "lease_heartbeat_exception",
        error=error,
        lease_backend_pid=123,
        heartbeat_age_seconds=1.2,
        select_inflight_seconds=0.9,
    )
    first = dict(abort.diagnostic)
    request_abort(abort, "dispatch_shutdown")
    request_abort(abort, "lease_cleanup_failure", error=RuntimeError("token"))
    abort.lease_metadata = lambda: {"last_select_seconds": 99.0}
    assert abort.is_set() and abort.diagnostic == first
    first.update(abort.context())
    assert first["exception_class"] == "OperationalError" and first["sqlstate"] == "08006"
    assert first["lease_backend_pid"] == 123
    assert "last_select_seconds" not in first
    assert first["report_time_lease"]["last_select_seconds"] == 99.0
    assert not any(secret in json.dumps(first) for secret in ["secret", "password", "token"])


def test_nonlease_abort_includes_cached_lease_context_and_tolerates_failed_context():
    abort = DiagnosticAbort()
    abort.lease_metadata = lambda: {"lease_backend_pid": 314, "heartbeat_age_seconds": 0.1}
    request_abort(abort, "control_owner_lost", operation="decision")
    assert abort.context()["lease_backend_pid"] == 314
    assert abort.context()["heartbeat_age_seconds"] == 0.1
    broken = DiagnosticAbort()
    broken.lease_metadata = lambda: (_ for _ in ()).throw(RuntimeError("secret"))
    request_abort(broken, "signal_requested")
    assert broken.is_set() and broken.diagnostic["origin"] == "signal_requested"


def test_static_class_and_field_allowlists():
    abort = DiagnosticAbort()
    error = type("secret_token", (Exception,), {"sqlstate": "TOKEN"})("secret")
    request_abort(
        abort,
        "control_owner_lost",
        error=error,
        operation="secret",
        lease_backend_pid=True,
        heartbeat_age_seconds=float("nan"),
        arbitrary="secret",
    )
    assert abort.context()["exception_class"] == "OtherError"
    assert not any(
        k in abort.context()
        for k in ["operation", "sqlstate", "arbitrary", "lease_backend_pid", "heartbeat_age_seconds"]
    )


def test_contended_latch_does_not_delay_abort():
    abort = DiagnosticAbort()
    abort._diagnostic_lock.acquire()
    try:
        request_abort(abort, "lease_heartbeat_deadline")
        assert abort.is_set()
    finally:
        abort._diagnostic_lock.release()


def test_failed_instrumentation_and_plain_event_still_abort(monkeypatch):
    abort = DiagnosticAbort()
    monkeypatch.setattr(abort, "record", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("secret")))
    request_abort(abort, "lease_heartbeat_exception")
    assert abort.is_set()
    plain = threading.Event()
    request_abort(plain, "dispatch_shutdown")
    assert plain.is_set()


class OneTurn:
    def wait(self, _):
        return False


def lease(abort):
    item = CoordinatorLease(None, SimpleNamespace(lock_namespace=11, lock_key=22), abort, clock=lambda: 10.0)
    item.stop = OneTurn()
    item.last_heartbeat = 7.9
    item.select_started = 8.2
    item.last_select_seconds = 0.01
    item.backend_pid = 987
    item.heartbeat_sample = HeartbeatSample(7, 6, 7.9, 8.2, 0.01, 987)
    abort.lease_metadata = item._diagnostic_metadata
    return item


def test_watchdog_records_age_without_touching_connection():
    abort = DiagnosticAbort()
    item = lease(abort)
    item.connection = object()
    item._watch_heartbeat()
    assert abort.is_set()
    assert abort.diagnostic["origin"] == "lease_heartbeat_deadline"
    assert abort.context()["heartbeat_age_seconds"] == pytest.approx(2.1)
    assert abort.context()["select_inflight_seconds"] == pytest.approx(1.8)
    item.clock = lambda: 99.0
    assert abort.context()["heartbeat_age_seconds"] == pytest.approx(2.1)
    assert abort.context()["select_inflight_seconds"] == pytest.approx(1.8)
    assert abort.context()["heartbeat_age_scope"] == "watchdog_abort_comparison"
    assert abort.context()["timing_scope"] == "abort_snapshot"
    assert abort.context()["abort_heartbeat"]["query_sequence"] == 7
    assert abort.context()["abort_heartbeat"]["completion_sequence"] == 6


def test_monitor_records_safe_database_failure():
    abort = DiagnosticAbort()
    item = lease(abort)
    error = type("OperationalError", (Exception,), {"sqlstate": "57P05"})("secret")

    @contextmanager
    def cursor():
        yield SimpleNamespace(execute=lambda *args: (_ for _ in ()).throw(error))

    item.connection = SimpleNamespace(cursor=cursor)
    item._monitor()
    assert abort.is_set()
    assert abort.diagnostic["origin"] == "lease_heartbeat_exception"
    assert abort.context()["sqlstate"] == "57P05"


@pytest.mark.parametrize("identity", [(987, False), (988, True), None, (987,)])
def test_monitor_rejects_lost_or_changed_lease(identity):
    abort = DiagnosticAbort()
    item = lease(abort)

    @contextmanager
    def cursor():
        yield SimpleNamespace(execute=lambda *args: None, fetchone=lambda: identity)

    item.connection = SimpleNamespace(cursor=cursor)
    item._monitor()
    assert abort.is_set()
    assert abort.diagnostic["origin"] == "lease_heartbeat_exception"
    assert item.heartbeat_sample.completion_sequence == 6


def test_preexisting_pooled_grant_never_stacks_or_serves():
    commands = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, query, args):
            commands.append(query)

        def fetchone(self):
            return (True,)

    connection = SimpleNamespace(cursor=Cursor, close=lambda: None)
    with pytest.raises(Exception, match="already holds an unqualified lease"):
        with CoordinatorLease(lambda: connection, SimpleNamespace(lock_namespace=11, lock_key=22), DiagnosticAbort()):
            pytest.fail("Unqualified owner served work")
    assert len(commands) == 1
    assert "pg_try_advisory_lock" not in commands[0]


def test_lease_metadata_failure_does_not_prevent_abort(monkeypatch):
    abort = DiagnosticAbort()
    item = lease(abort)
    monkeypatch.setattr(item, "_diagnostic_metadata", lambda: (_ for _ in ()).throw(RuntimeError("secret")))
    item._abort("lease_cleanup_failure")
    assert abort.is_set() and abort.diagnostic["origin"] == "lease_cleanup_failure"


def test_control_owner_loss_is_distinct_and_cleanup_keeps_first_cause():
    abort = DiagnosticAbort()

    class Dispatch:
        active = {"owned": object()}
        latest = None
        comments_submitted = 0
        limit = 6

        def bind(self, *args):
            pass

        def drain(self, *args):
            pass

    def decision(*args):
        raise LostOwnership("secret reason/DSN")

    with pytest.raises(Interrupted):
        run_partition(
            {"run_id": "r", "generation": 1},
            store=SimpleNamespace(decision=decision),
            scheduler=SimpleNamespace(clock=lambda: 0),
            dispatch=Dispatch(),
            abort=abort,
        )
    assert abort.is_set() and abort.diagnostic["origin"] == "control_owner_lost"
    assert abort.diagnostic["operation"] == "decision"
    assert abort.diagnostic["foreground_phase"] == "decision"
    assert "secret" not in json.dumps(abort.diagnostic)
    request_abort(abort, "dispatch_shutdown")
    assert abort.diagnostic["origin"] == "control_owner_lost"


def test_reporter_failure_and_blocked_sink_cannot_delay_abort():
    abort = DiagnosticAbort()
    entered, release = threading.Event(), threading.Event()

    def emit(_):
        entered.set()
        release.wait(2)
        raise RuntimeError("secret")

    reporter = start_abort_reporter(abort, emit)
    request_abort(abort, "signal_requested")
    assert abort.is_set() and entered.wait(1)
    assert reporter.daemon and reporter.is_alive()
    release.set()
    reporter.join(1)
    assert not reporter.is_alive()


def test_reporter_start_failure_is_best_effort(monkeypatch):
    monkeypatch.setattr(threading.Thread, "start", lambda _: (_ for _ in ()).throw(RuntimeError("secret")))
    abort = DiagnosticAbort()
    assert start_abort_reporter(abort, lambda _: None) is None
    request_abort(abort, "signal_requested")
    assert abort.is_set()


def test_dispatch_shutdown_with_plain_event_preserves_reaping():
    abort = threading.Event()
    dispatch = LocalDispatch(None, abort)
    dispatch.active = {"owner": object()}
    events = []
    dispatch.pool = SimpleNamespace(shutdown=lambda **kw: events.append(("reaped", kw["wait"])))
    dispatch.drain = lambda *a, **kw: events.append(("drained", kw["closing"]))
    dispatch.close(None)
    assert abort.is_set() and events == [("reaped", True), ("drained", True)]


@pytest.mark.parametrize("blocked", ["lease_metadata", "sqlstate"])
def test_blocked_optional_enrichment_does_not_delay_abort_reaping_or_minimal_report(blocked):
    abort = DiagnosticAbort()
    entered, release, reaped, finished = (threading.Event() for _ in range(4))
    emitted = []

    def hold():
        entered.set()
        release.wait(3)
        return {}

    class FailureError(Exception):
        @property
        def sqlstate(self):
            hold()
            return "08006"

    if blocked == "lease_metadata":
        abort.lease_metadata = hold
    reporter = start_abort_reporter(abort, emitted.append)
    dispatch = LocalDispatch(None, abort)
    dispatch.active = {"owner": object()}
    dispatch.pool = SimpleNamespace(shutdown=lambda **kw: reaped.set())
    dispatch.drain = lambda *a, **kw: None

    def producer():
        if blocked == "sqlstate":
            request_abort(abort, "lease_heartbeat_exception", error=FailureError("secret"))
        dispatch.close(None)
        finished.set()

    producer_thread = threading.Thread(target=producer, daemon=True)
    try:
        producer_thread.start()
        assert entered.wait(1)
        assert abort.is_set() and reaped.wait(1) and finished.wait(1)
        assert emitted[0]["status"] == "abort_diagnostic"
        expected = "lease_heartbeat_exception" if blocked == "sqlstate" else "dispatch_shutdown"
        assert emitted[0]["origin"] == expected and emitted[0]["pid"] > 0 and emitted[0]["at"]
        assert "secret" not in json.dumps(emitted)
        assert not release.is_set()
    finally:
        release.set()
        producer_thread.join(1)
        reporter.join(1)


def test_backend_pid_is_actual_acquisition_sql_result_with_same_lock_namespace():
    commands = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, query, args=()):
            self.query = query
            commands.append((query, args))

        def fetchone(self):
            if "pg_try_advisory_lock" in self.query:
                assert "pg_backend_pid()" in self.query
                return (True, 321)
            if "current_setting" in self.query:
                return ("0",)
            if "from pg_locks" in self.query:
                return (False,)
            if "pg_advisory_unlock" in self.query:
                return (True,)
            return ("runtime", False, True, False, False)

    connection = SimpleNamespace(cursor=Cursor, close=lambda: None, info=SimpleNamespace(backend_pid=999))
    abort = DiagnosticAbort()
    config = SimpleNamespace(lock_namespace=11, lock_key=22, runtime_role="runtime")
    with CoordinatorLease(lambda: connection, config, abort) as item:
        assert item.backend_pid == 321
        request_abort(abort, "signal_requested")
        assert abort.context()["lease_backend_pid"] == 321
    acquisition = [(query, args) for query, args in commands if "pg_try_advisory_lock" in query]
    assert len(acquisition) == 1 and acquisition[0][1] == (11, 22)
    assert sum("pg_backend_pid()" in query for query, _ in commands) >= 3  # preflight, acquisition, release


def test_delayed_watchdog_and_late_report_cannot_mix_select_generations():
    """Real monitor/watchdog threads; SELECT completes while watchdog is asleep."""
    abort = DiagnosticAbort()
    clock = [10.0]
    item = CoordinatorLease(None, None, abort, clock=lambda: clock[0])
    item.backend_pid = 987
    item.config = SimpleNamespace(lock_namespace=11, lock_key=22)
    item.heartbeat_sample = HeartbeatSample(last_completion=10.0, backend_pid=987)
    abort.lease_metadata = item._diagnostic_metadata
    asleep, wake, first_query, first_finish, first_published, second_start, second_query, second_finish = (
        threading.Event() for _ in range(8)
    )
    calls = {"monitor": 0, "watchdog": 0, "sql": 0}

    class Stop:
        def wait(self, _):
            if threading.current_thread().name == "sample-monitor":
                calls["monitor"] += 1
                if calls["monitor"] == 1:
                    return False
                if calls["monitor"] == 2:
                    first_published.set()
                    assert second_start.wait(3)
                    return False
                return True
            calls["watchdog"] += 1
            asleep.set()
            assert wake.wait(3)
            return False

    @contextmanager
    def cursor():
        def execute(sql, args):
            assert "pg_backend_pid()" in sql and args == (11, 22)
            calls["sql"] += 1
            if calls["sql"] == 1:
                first_query.set()
                assert first_finish.wait(3)
            else:
                second_query.set()
                assert second_finish.wait(3)

        yield SimpleNamespace(execute=execute, fetchone=lambda: (987, True))

    item.stop = Stop()
    item.connection = SimpleNamespace(cursor=cursor)
    monitor = threading.Thread(target=item._monitor, name="sample-monitor", daemon=True)
    watchdog = threading.Thread(target=item._watch_heartbeat, name="sample-watchdog", daemon=True)
    try:
        with foreground_phase(abort, "before_finalize"):
            watchdog.start()
            assert asleep.wait(1)
            clock[0] = 10.1
            monitor.start()
            assert first_query.wait(1)
            clock[0] = 10.2
            first_finish.set()
            assert first_published.wait(1)
            assert item.heartbeat_sample.completion_sequence == 1
            clock[0] = 13.0
            wake.set()
            assert abort.wait(1)
        assert abort.diagnostic["foreground_phase"] == "before_finalize"
        captured = abort._diagnostic_metadata["heartbeat_sample"]
        # A later SELECT really runs/completes before the reporter starts.
        clock[0] = 20.0
        second_start.set()
        assert second_query.wait(1)
        clock[0] = 26.0
        second_finish.set()
        monitor.join(1)
        watchdog.join(1)
        assert not monitor.is_alive() and not watchdog.is_alive()
        clock[0] = 30.0
        emitted = []
        reporter = start_abort_reporter(abort, emitted.append)
        reporter.join(1)
        context = emitted[1]
        proof = context["abort_heartbeat"]
        assert captured is not item.heartbeat_sample
        assert proof["query_sequence"] == proof["completion_sequence"] == 1
        assert proof["query_state"] == "complete"  # No SELECT was in flight at comparison.
        assert proof["heartbeat_age_seconds"] == pytest.approx(2.8)
        assert proof["watchdog_wake_gap_seconds"] == pytest.approx(3.0)
        assert proof["last_completed_query_duration_seconds"] == pytest.approx(0.1)
        assert proof["lease_backend_pid"] == 987
        assert context["last_select_seconds"] == pytest.approx(0.1)
        assert context["report_time_lease"]["last_select_seconds"] == pytest.approx(6.0)
        assert context["report_time_lease"]["query_sequence"] == 2
        assert context["report_time_lease"]["completion_sequence"] == 2
        assert context["foreground_phase"] == "before_finalize"
        assert calls["sql"] == 2  # One existing SELECT per monitor turn; no added query.
    finally:
        wake.set()
        first_finish.set()
        second_start.set()
        second_finish.set()
        if monitor.ident is not None:
            monitor.join(1)
        if watchdog.ident is not None:
            watchdog.join(1)


def test_heartbeat_sample_is_immutable_and_phase_labels_are_fixed():
    from dataclasses import FrozenInstanceError

    sample = HeartbeatSample(query_sequence=3, completion_sequence=2)
    with pytest.raises(FrozenInstanceError):
        sample.query_sequence = 4
    abort = DiagnosticAbort()
    with foreground_phase(abort, "decision"):
        with foreground_phase(abort, "wait"):
            request_abort(abort, "signal_requested")
        assert abort.foreground_phase == "decision"
    assert abort.foreground_phase == "initializing"
    assert abort.diagnostic["foreground_phase"] == "wait"
    with pytest.raises(ValueError, match="Unknown foreground"):
        with foreground_phase(abort, "secret dynamic query"):
            pytest.fail("Invalid phase entered")


@pytest.mark.parametrize("waiting", [False, True])
def test_worker_finalizer_and_nested_wait_capture_actual_foreground_phase(monkeypatch, waiting):
    abort = DiagnosticAbort()
    dispatch = LocalDispatch(None, abort)

    def fail_complete(*args, **kwargs):
        pytest.fail("An aborted finalizer must not complete the partition")

    def tick(*args):
        assert abort.foreground_phase == "scheduler_tick"
        return {"stop": "blocked"}

    def wait(_):
        assert abort.foreground_phase == "wait"
        request_abort(abort, "lease_heartbeat_deadline")
        return True

    def finalize(*args, **kwargs):
        assert abort.foreground_phase == "before_finalize"
        if waiting:
            dispatch.wait(5)
        else:
            request_abort(abort, "lease_heartbeat_deadline")
        return True

    monkeypatch.setattr(abort, "wait", wait)
    with pytest.raises(Interrupted):
        run_partition(
            {"run_id": "r", "generation": 1},
            store=SimpleNamespace(complete_partition=fail_complete),
            scheduler=SimpleNamespace(tick=tick),
            dispatch=dispatch,
            abort=abort,
            before_finalize=finalize,
        )
    assert abort.diagnostic["foreground_phase"] == ("wait" if waiting else "before_finalize")
