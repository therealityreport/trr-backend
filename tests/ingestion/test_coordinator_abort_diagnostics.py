"""Abort origin evidence must never weaken cancellation or expose error text."""

import json
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from scripts.ingestion.local_driver import CoordinatorLease, DiagnosticAbort, request_abort
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
    assert abort.is_set() and abort.diagnostic == first
    first.update(abort.context())
    assert first["exception_class"] == "OperationalError" and first["sqlstate"] == "08006"
    assert first["lease_backend_pid"] == 123
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
    item = CoordinatorLease(None, None, abort, clock=lambda: 10.0)
    item.stop = OneTurn()
    item.last_heartbeat = 7.9
    item.select_started = 8.2
    item.last_select_seconds = 0.01
    item.backend_pid = 987
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
    assert abort.context()["heartbeat_age_scope"] == "watchdog_abort_comparison"
    assert abort.context()["timing_scope"] == "report_time"


def test_monitor_records_safe_database_failure():
    abort = DiagnosticAbort()
    item = lease(abort)
    error = type("OperationalError", (Exception,), {"sqlstate": "57P05"})("secret")

    @contextmanager
    def cursor():
        yield SimpleNamespace(execute=lambda _: (_ for _ in ()).throw(error))

    item.connection = SimpleNamespace(cursor=cursor)
    item._monitor()
    assert abort.is_set()
    assert abort.diagnostic["origin"] == "lease_heartbeat_exception"
    assert abort.context()["sqlstate"] == "57P05"


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
    assert sum("pg_backend_pid()" in query for query, _ in commands) == 2  # acquisition and existing cleanup proof
