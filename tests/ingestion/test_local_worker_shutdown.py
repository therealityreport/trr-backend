"""Private subprocesses exercise abort shutdown without a database or source."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

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
from trr_backend.ingestion import admission, runtime, workflow_store, workflows

mode, pid_path = sys.argv[1], Path(sys.argv[2])
children = []
abort_event = None

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
        if mode == "control":
            threading.Event().wait()  # A foreground control call never returns.
        if mode == "signal":
            emit(signal_ready=True)
            pid_path.with_suffix(".ready").write_text("ready")
            assert abort_event.wait(8)
        if mode == "cleanup":
            trigger_abort()
    def dispatch_partitions(self, submit, **kwargs):
        if mode in {"activity", "healthy"}:
            submit({"offline": True})
        return 0

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
local_driver.validate_configuration = lambda dsn: None
workflow_store.WorkflowStore = Store
workflows.Scheduler = Scheduler
admission.Admission = lambda: type("Admission", (), {"admit": lambda self, item: True})()
pg.close_pool = cleanup
raise SystemExit(local_worker.main())
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
