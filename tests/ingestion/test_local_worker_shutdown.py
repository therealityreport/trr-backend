"""Private subprocesses exercise abort shutdown without a database or source."""

import json
import os
import signal
import subprocess
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

ROOT = Path(__file__).resolve().parents[2]

PROGRAM = r"""
import json, os, signal, socket, subprocess, sys, threading, time
from pathlib import Path

def forbidden(*args, **kwargs):
    raise AssertionError("Shutdown regression attempted a database or network connection")

socket.socket.connect = forbidden
socket.create_connection = forbidden
import psycopg, psycopg2
psycopg.connect = psycopg2.connect = forbidden

from scripts.ingestion import local_worker, local_driver
from trr_backend.db import pg
from trr_backend.ingestion import admission, execution_policy, runtime, workflow_store, workflows

mode, pid_path = sys.argv[1], Path(sys.argv[2])
children = []
abort_event = None
if mode.startswith("startup_"):
    local_worker.LEASE_STARTUP_TIMEOUT_SECONDS = 0.2
    local_worker.ABORT_EXIT_GRACE_SECONDS = 1.0
if mode.startswith("lifetime_"):
    local_worker.ABORT_EXIT_GRACE_SECONDS = 0.5
    if mode != "lifetime_parent_death":
        os.environ["TRR_INSTAGRAM_LOCAL_WORKER_DEADLINE_AT"] = str(time.time() + 0.3)

if mode in {"reporter_delayed", "reporter_blocked"}:
    original_reporter = local_worker.start_abort_reporter
    def start_reporter(abort, sink):
        def emit_diagnostic(value):
            if mode == "reporter_blocked":
                threading.Event().wait()
            time.sleep(0.1)
            sink(value)
        return original_reporter(abort, emit_diagnostic)
    local_worker.start_abort_reporter = start_reporter

def emit(**value):
    print(json.dumps(value), flush=True)

def trigger_abort():
    emit(abort_at=time.monotonic())
    abort_event.set()

class Lease:
    def __init__(self, connect, config, abort):
        global abort_event
        abort_event = abort
    def __enter__(self):
        if mode == "lifetime_startup_stalled":
            emit(startup_entered=True)
            threading.Event().wait()
        if mode.startswith("startup_"):
            emit(startup_entered=True)
            if mode == "startup_stalled":
                threading.Event().wait()
            if mode == "startup_released":
                time.sleep(0.3)
            if mode == "startup_signal":
                pid_path.with_suffix(".ready").write_text("ready")
                assert abort_event.wait(8)
            if mode == "startup_failed":
                raise RuntimeError("offline qualification failure")
        emit(lease_entered=True)
        if mode == "control":
            threading.Timer(0.1, trigger_abort).start()
        return self
    def __exit__(self, *args):
        emit(lease_cleanup=True)

original_spawn = subprocess.Popen
def spawn(argv, **kwargs):
    duration = local_worker.ABORT_EXIT_GRACE_SECONDS + 0.25 if mode == "healthy" else 1000
    code = ("import sys,time,json;sys.stdin.buffer.read();time.sleep(" + str(duration)
            + ");print(json.dumps({'outcome':'committed','stage':'post_details'}),flush=True)")
    child = original_spawn([sys.executable, "-c", code], **kwargs)
    children.append(child)
    pid_path.write_text(str(child.pid))
    emit(activity_started=True)
    if mode == "activity":
        threading.Timer(0.1, trigger_abort).start()
    return child
runtime.subprocess.Popen = spawn

class Store:
    execution_placement = "local"
    def __init__(self, **kwargs):
        pass
    def seed(self, *args):
        raise AssertionError("No business state is seeded")
    def dispatch_pending(self, *args, **kwargs):
        emit(business_called=True)
        if mode in {"lifetime_deadline", "lifetime_parent_death", "lifetime_control_stalled"}:
            pid_path.with_suffix(".ready").write_text("ready")
            if mode == "lifetime_control_stalled":
                threading.Event().wait()
            assert abort_event.wait(8)
        if mode == "control":
            threading.Event().wait()  # A foreground control call never returns.
        if mode == "signal":
            emit(signal_ready=True)
            pid_path.with_suffix(".ready").write_text("ready")
            assert abort_event.wait(8)
        if mode == "cleanup":
            trigger_abort()
        if mode in {"reporter_delayed", "reporter_blocked"}:
            os.kill(os.getpid(), signal.SIGTERM)
    def dispatch_partitions(self, submit, **kwargs):
        if mode in {"activity", "healthy"}:
            submit({"offline": True})
        return 0

if mode == "activity_cap":
    Store.transaction = lambda self: None
    class Policy:
        def __init__(self, **kwargs):
            pass
        def effective_comments_limit(self, item):
            return item["requested"]
    execution_policy.ExecutionPolicy = Policy
    def inspect_limits(**kwargs):
        callback = kwargs["effective_comments_limit"]
        emit(effective_limits=[callback({"requested": limit}) for limit in [1, 2, 4, 6, 8]])
        return {"status": "stopped"}
    local_worker.run_worker = inspect_limits

class Scheduler:
    def __init__(self, store, *, dispatch, **kwargs):
        self.dispatch = dispatch
    def tick(self, item, ordinal):
        self.dispatch(dict(run_id="offline",account="fixture_account",generation=1,
                           stage="post_details",target_key="offline",owner_token="offline"))
        assert children[0].poll() is not None
        emit(activity_reaped=True, child_exit=children[0].returncode)
        if mode == "activity":
            threading.Event().wait()  # Abort cleanup is stuck after reaping.
        os.kill(os.getpid(), signal.SIGTERM)
        return {"ready": True}

def cleanup():
    emit(pool_cleanup_entered=True)
    if mode == "cleanup":
        threading.Event().wait()
    emit(pool_cleanup_finished=True)

original_guard = local_worker.AbortExitGuard
def force_exit(code):
    live = [child for child in children if child.poll() is None]
    emit(forced_exit_at=time.monotonic(), live_children=len(live))
    for child in live:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=3)
    os._exit(126 if live else code)

class Guard(original_guard):
    def __init__(self, abort):
        super().__init__(abort, force_exit=force_exit)
    def close(self):
        super().close()
        emit(guard_stopped=True)

local_worker.AbortExitGuard = Guard
local_driver.CoordinatorLease = Lease
local_driver.validate_configuration = lambda dsn: time.sleep(0.4) if mode == "lifetime_setup_expired" else None
workflow_store.WorkflowStore = Store
workflows.Scheduler = Scheduler
admission.Admission = lambda: type("Admission", (), {"admit": lambda self, item: True})()
pg.close_pool = cleanup
result = local_worker.main()
if mode.startswith("lifetime_"):
    pid_path.with_suffix(".done").write_text(str(result))
raise SystemExit(result)
"""


def _child_environment():
    return {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(ROOT),
        "TRR_TEST_DISABLE_DOTENV": "1",
        "TRR_INSTAGRAM_INGESTION_COORDINATOR_ENABLED": "1",
        "TRR_INSTAGRAM_DBOS_DATABASE_URL": "postgresql://offline.invalid/unused",
        "TRR_INSTAGRAM_DBOS_RUNTIME_ROLE": "offline_runtime",
    }


@pytest.mark.parametrize("mode", ["control", "activity", "cleanup", "healthy", "signal"])
def test_abort_guard_bounds_stuck_work_but_preserves_activity_and_graceful_cleanup(tmp_path, mode):
    pid_path = tmp_path / "activity.pid"
    process = subprocess.Popen(
        [sys.executable, "-B", "-c", PROGRAM, mode, str(pid_path)],
        cwd=ROOT,
        env=_child_environment(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        if mode == "signal":
            until = time.monotonic() + 10
            while not pid_path.with_suffix(".ready").exists():
                assert time.monotonic() < until and process.poll() is None
                time.sleep(0.01)
            process.terminate()
        output, error = process.communicate(timeout=25)
        records = [json.loads(line) for line in output.splitlines()]
        assert not error, error
        if mode in {"control", "activity", "cleanup"}:
            assert process.returncode == 125, records
            began = next(row["abort_at"] for row in records if "abort_at" in row)
            forced = next(row for row in records if "forced_exit_at" in row)
            assert 15 <= forced["forced_exit_at"] - began < 17
            assert forced["live_children"] == 0
            assert not any(row.get("guard_stopped") for row in records)
            if mode == "activity":
                reaped = next(index for index, row in enumerate(records) if row.get("activity_reaped"))
                exited = next(index for index, row in enumerate(records) if "forced_exit_at" in row)
                assert reaped < exited and records[reaped]["child_exit"] == -signal.SIGKILL
            if mode == "cleanup":
                assert any(row.get("lease_cleanup") for row in records)
                assert any(row.get("pool_cleanup_entered") for row in records)
                assert not any(row.get("pool_cleanup_finished") for row in records)
        else:
            assert process.returncode == 0, records
            assert not any("forced_exit_at" in row for row in records)
            finished = next(index for index, row in enumerate(records) if row.get("pool_cleanup_finished"))
            stopped = next(index for index, row in enumerate(records) if row.get("guard_stopped"))
            assert finished < stopped
            assert any(row.get("status") == "stopped" for row in records)
            assert any(row.get("status") == "abort_diagnostic" for row in records)
            if mode == "healthy":
                assert any(row.get("activity_reaped") and row["child_exit"] == 0 for row in records)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
        if pid_path.exists():
            try:
                os.killpg(int(pid_path.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize("mode", ["startup_stalled", "startup_released", "startup_signal", "startup_failed"])
def test_startup_deadline_bounds_qualification_and_never_dispatches_after_abort(tmp_path, mode):
    pid_path = tmp_path / "startup.pid"
    process = subprocess.Popen(
        [sys.executable, "-B", "-c", PROGRAM, mode, str(pid_path)],
        cwd=ROOT,
        env=_child_environment(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        if mode == "startup_signal":
            deadline = time.monotonic() + 10
            while not pid_path.with_suffix(".ready").exists():
                assert time.monotonic() < deadline and process.poll() is None
                time.sleep(0.01)
            process.terminate()
        output, error = process.communicate(timeout=10)
        records = [json.loads(line) for line in output.splitlines()]
        assert not error, error
        assert any(row.get("startup_entered") for row in records)
        assert not any(row.get("business_called") or row.get("activity_started") for row in records)
        assert not any(row.get("status") == "local_worker_lease_acquired" for row in records)
        if mode == "startup_stalled":
            assert process.returncode == 125, records
            diagnostic = next(row for row in records if row.get("status") == "abort_diagnostic")
            assert diagnostic["origin"] == "lease_startup_deadline"
            assert any(row.get("live_children") == 0 for row in records)
        else:
            assert process.returncode == {"startup_released": 125, "startup_signal": 0, "startup_failed": 1}[mode]
            assert any(row.get("guard_stopped") for row in records)
            assert not any("forced_exit_at" in row for row in records)
            if mode != "startup_failed":
                assert any(row.get("lease_cleanup") for row in records)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)


@pytest.mark.parametrize("mode", ["reporter_delayed", "reporter_blocked"])
def test_shutdown_waits_briefly_for_reporter_but_a_blocked_sink_cannot_hold_exit(tmp_path, mode):
    began = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-B", "-c", PROGRAM, mode, str(tmp_path / "reporter.pid")],
        cwd=ROOT,
        env=_child_environment(),
        text=True,
        capture_output=True,
        timeout=10,
    )
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert result.returncode == 0, (records, result.stderr)
    assert not result.stderr
    assert any(row.get("guard_stopped") for row in records)
    assert not any("forced_exit_at" in row for row in records)
    assert time.monotonic() - began < 5
    assert any(row.get("status") == "abort_diagnostic" for row in records) == (mode == "reporter_delayed")


def test_blocked_stdout_diagnostic_does_not_abort_python_finalization():
    program = r"""
import os
from scripts.ingestion.local_driver import DiagnosticAbort, request_abort
from scripts.ingestion.local_worker import emit_abort_diagnostic, start_abort_reporter

read_fd, write_fd = os.pipe()
os.set_blocking(write_fd, False)
for chunk in (b"x" * 4096, b"x"):
    try:
        while True:
            os.write(write_fd, chunk)
    except BlockingIOError:
        pass
os.set_blocking(write_fd, True)
os.dup2(write_fd, 1)
abort = DiagnosticAbort()
reporter = start_abort_reporter(abort, emit_abort_diagnostic)
request_abort(abort, "signal_requested")
reporter.join(timeout=0.1)
assert reporter.is_alive(), "Diagnostic must remain blocked on the full pipe"
# Normal interpreter finalization while the daemon remains in its stdout write.
raise SystemExit(0)
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", program],
        cwd=ROOT,
        env=_child_environment(),
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert not result.stderr


@pytest.mark.parametrize("stage", ["outbox", "partition"])
@pytest.mark.parametrize("mode", ["allowed", "other", "unfenced"])
def test_run_fence_rejects_unrelated_callbacks_before_business_work_or_acknowledgement(monkeypatch, stage, mode):
    import threading

    from scripts.ingestion import local_worker

    allowed = UUID("aaaaaaaa-0000-4000-8000-000000000001")
    other = UUID("bbbbbbbb-0000-4000-8000-000000000002")
    item = {"run_id": allowed if mode == "allowed" else other}
    events = []
    abort = threading.Event()

    class Store:
        execution_placement = "local"

        def seed(self, candidate):
            events.append(("seed", candidate))

        def dispatch_pending(self, submit, **kwargs):
            if stage == "outbox":
                submit(item)
                events.append(("outbox_acknowledged", item))
            return int(stage == "outbox")

        def dispatch_partitions(self, submit, **kwargs):
            if stage == "partition":
                submit(item)
                events.append(("partition_acknowledged", item))
            return int(stage == "partition")

    monkeypatch.setattr(local_worker, "run_partition", lambda candidate, **kwargs: events.append(("run", candidate)))
    arguments = {
        "store": Store(),
        "scheduler": None,
        "dispatch": SimpleNamespace(close=lambda store: events.append(("closed", None)), wait=lambda seconds: None),
        "lease": nullcontext(),
        "abort": abort,
        "workflow_version": "fixture",
        "allowed_run_id": None if mode == "unfenced" else str(allowed).upper(),
        "on_cycle": lambda count: abort.set(),
    }
    if mode == "other":
        with pytest.raises(ValueError, match="run fence rejected"):
            local_worker.run_worker(**arguments)
        assert events == [("closed", None)]
    else:
        local_worker.run_worker(**arguments)
        assert events == [
            ("seed" if stage == "outbox" else "run", item),
            (f"{stage}_acknowledged", item),
            ("closed", None),
        ]


@pytest.mark.parametrize("mode", ["invalid", "mismatched_recovery"])
def test_main_validates_run_fence_before_lease_startup(tmp_path, mode):
    environment = _child_environment()
    environment["TRR_INSTAGRAM_LOCAL_WORKER_RUN_ID"] = (
        "invalid" if mode == "invalid" else "aaaaaaaa-0000-4000-8000-000000000001"
    )
    if mode == "mismatched_recovery":
        environment["TRR_INSTAGRAM_JANUARY_RECOVERY_RUN_ID"] = "bbbbbbbb-0000-4000-8000-000000000002"
    result = subprocess.run(
        [sys.executable, "-B", "-c", PROGRAM, "startup_failed", str(tmp_path / "fence.pid")],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        timeout=10,
    )
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert result.returncode == 1, (records, result.stderr)
    assert not result.stderr
    assert any(row.get("status") == "failed" and row["failure_class"] == "ValueError" for row in records)
    assert not any(row.get("startup_entered") or row.get("business_called") for row in records)


@pytest.mark.parametrize("cap", [None, "1", "2", "4", "6", "0", "3", "5", "7", "invalid"])
def test_main_activity_cap_bounds_policy_and_rejects_unsupported_values_before_startup(tmp_path, cap):
    environment = _child_environment()
    if cap is not None:
        environment["TRR_INSTAGRAM_LOCAL_WORKER_MAX_ACTIVITIES"] = cap
    result = subprocess.run(
        [sys.executable, "-B", "-c", PROGRAM, "activity_cap", str(tmp_path / "cap.pid")],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        timeout=10,
    )
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert not result.stderr
    if cap is None or cap in {"1", "2", "4", "6"}:
        assert result.returncode == 0, records
        effective = next(row["effective_limits"] for row in records if "effective_limits" in row)
        assert effective == [min(limit, int(cap)) if cap is not None else limit for limit in [1, 2, 4, 6, 8]]
    else:
        assert result.returncode == 1, records
        assert any(row.get("status") == "failed" and row["failure_class"] == "ValueError" for row in records)
        assert not any("effective_limits" in row or row.get("lease_entered") for row in records)


@pytest.mark.parametrize(
    "mode", ["lifetime_deadline", "lifetime_control_stalled", "lifetime_startup_stalled", "lifetime_setup_expired"]
)
def test_worker_owned_deadline_stops_healthy_or_stalled_work_without_expired_dispatch(tmp_path, mode):
    result = subprocess.run(
        [sys.executable, "-B", "-c", PROGRAM, mode, str(tmp_path / "deadline.pid")],
        cwd=ROOT,
        env=_child_environment(),
        text=True,
        capture_output=True,
        timeout=10,
    )
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert not result.stderr
    forced = mode in {"lifetime_control_stalled", "lifetime_startup_stalled"}
    assert result.returncode == (125 if forced else 0), records
    assert any(row.get("status") == "abort_diagnostic" and row["origin"] == "signal_requested" for row in records)
    assert any("forced_exit_at" in row for row in records) == forced
    if mode in {"lifetime_startup_stalled", "lifetime_setup_expired"}:
        assert not any(row.get("business_called") or row.get("activity_started") for row in records)
    if mode == "lifetime_setup_expired":
        assert not any(row.get("lease_entered") or row.get("startup_entered") for row in records)


@pytest.mark.parametrize(
    "configuration",
    ["expired", "nan", "infinity", "too_far", "parent_only", "wrong_parent", "parent_zero", "parent_invalid"],
)
def test_lifetime_configuration_is_rejected_before_lease_startup(tmp_path, configuration):
    environment = _child_environment()
    deadline = {"expired": str(time.time() - 1), "nan": "nan", "infinity": "inf", "too_far": str(time.time() + 3605)}
    if configuration in deadline:
        environment["TRR_INSTAGRAM_LOCAL_WORKER_DEADLINE_AT"] = deadline[configuration]
    else:
        environment["TRR_INSTAGRAM_LOCAL_WORKER_PARENT_PID"] = {"parent_zero": "0", "parent_invalid": "invalid"}.get(
            configuration, "1"
        )
        if configuration != "parent_only":
            environment["TRR_INSTAGRAM_LOCAL_WORKER_DEADLINE_AT"] = str(time.time() + 30)
    result = subprocess.run(
        [sys.executable, "-B", "-c", PROGRAM, "startup_failed", str(tmp_path / "invalid.pid")],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
        timeout=10,
    )
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert result.returncode == 1, (records, result.stderr)
    assert not result.stderr
    assert any(row.get("status") == "failed" and row["failure_class"] == "ValueError" for row in records)
    assert not any(
        row.get("lease_entered") or row.get("startup_entered") or row.get("business_called") for row in records
    )


def test_worker_owned_parent_guard_stops_after_real_supervisor_exit_and_reparenting(tmp_path):
    pid_path = tmp_path / "orphan.pid"
    supervisor = r"""
import os, subprocess, sys, time
from pathlib import Path
path = Path(sys.argv[2])
environment = dict(os.environ)
environment["TRR_INSTAGRAM_LOCAL_WORKER_PARENT_PID"] = str(os.getpid())
environment["TRR_INSTAGRAM_LOCAL_WORKER_DEADLINE_AT"] = str(time.time() + 30)
child = subprocess.Popen([sys.executable, "-B", "-c", sys.argv[1], "lifetime_parent_death", str(path)], env=environment)
path.write_text(str(child.pid))
until = time.monotonic() + 5
while not path.with_suffix(".ready").exists():
    assert child.poll() is None and time.monotonic() < until
    time.sleep(0.01)
# Normal supervisor exit changes the worker's actual parent; no signal is sent.
"""
    log_path = tmp_path / "orphan.log"
    try:
        with log_path.open("w") as log:
            parent = subprocess.run(
                [sys.executable, "-B", "-c", supervisor, PROGRAM, str(pid_path)],
                cwd=ROOT,
                env=_child_environment(),
                stdout=log,
                stderr=log,
                timeout=10,
            )
            assert parent.returncode == 0
            deadline = time.monotonic() + 5
            while not pid_path.with_suffix(".done").exists():
                assert time.monotonic() < deadline, log_path.read_text()
                time.sleep(0.01)
        assert pid_path.with_suffix(".done").read_text() == "0"
        records = [json.loads(line) for line in log_path.read_text().splitlines()]
        assert any(row.get("status") == "stopped" for row in records)
        assert any(row.get("status") == "abort_diagnostic" for row in records)
        assert not any("forced_exit_at" in row for row in records)
    finally:
        if pid_path.exists() and not pid_path.with_suffix(".done").exists():
            try:
                os.kill(int(pid_path.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass
