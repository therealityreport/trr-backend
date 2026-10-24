#!/usr/bin/env python3
"""Durable Mac worker for explicitly supported Instagram runs placed on `local`.

It runs until stopped (launchd keeps it alive; see
scripts/ingestion/launchd/com.trr.instagram-local-worker.plist). It holds its own
session advisory lease (composition.LOCAL_WORKER_LOCK_KEY), so it never blocks the
Modal coordinator; the per-run placement fence keeps Modal and local work apart.
The API reports the Mac as online while that lease is held.

Each partition runs inline with the real Scheduler. An interrupted partition stays
unacknowledged and is dispatched again after restart; recorded decisions make the
replay safe. Runs are never cancelled on shutdown: targets, cursors and cooldowns
stay durable for the next start. DSNs come only from the environment and are
never printed.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.workflow_store import PARTITION_SIZE
from trr_backend.ingestion.workflows import control_call

ACTIVITY_TIMEOUT_SECONDS = 590
WAIT_RECHECK_SECONDS = 5.0


def retained_certification(path):
    """Keep complete reports; archive interrupted output for a fresh invocation."""
    if not path.exists():
        return False
    try:
        value = json.loads(path.read_text())
        if (
            isinstance(value, dict)
            and type(value.get("final_acceptance")) is bool
            and isinstance(value.get("gates"), dict)
        ):
            return True
    except (ValueError, UnicodeError):
        pass
    path.rename(path.with_name(f"{path.stem}.interrupted-{os.getpid()}-{time.time_ns()}{path.suffix}"))
    return False


def run_owned_certification(command, log, abort, *, timeout_seconds=3600):
    """One private child group; abort/timeout reaps certifier and decoders."""
    child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.monotonic() + timeout_seconds
    try:
        while child.poll() is None:
            if abort.is_set():
                raise Interrupted
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(command[0], timeout_seconds)
            abort.wait(0.1)
        return child.returncode
    finally:
        # The group can contain decoder descendants after its leader exits.
        # Kill only the session created above, before AbortExitGuard's 15s cap.
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait(timeout=3)


ABORT_EXIT_GRACE_SECONDS = 15.0


class AbortExitGuard:
    """Let abort cleanup finish, then exit even if foreground SQL never returns.

    This only bounds shutdown after an actual abort. A healthy lease with a
    separate stuck business connection does not trigger this guard.
    """

    def __init__(self, abort, *, force_exit=os._exit):
        self.abort, self.force_exit = abort, force_exit
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._watch, daemon=True, name="instagram-local-abort-exit")

    def start(self):
        self.thread.start()

    def _watch(self):
        while not self.stopped.wait(0.05):
            if self.abort.is_set():
                if not self.stopped.wait(ABORT_EXIT_GRACE_SECONDS):
                    self.force_exit(125)
                return

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=1)


class Interrupted(Exception):  # noqa: N818 - control-flow signal, not an error
    """Stop requested mid-partition; leave the partition unacknowledged."""


class LocalDispatch:
    """Serial dispatch owner with bounded, independently reaped comment children."""

    def __init__(self, execute, abort):
        self.execute, self.abort = execute, abort
        self.latest = None
        self.active = {}
        self.submitted = set()
        self.pool = None
        self.item = None
        self.limit = 1
        self.comments_submitted = 0

    def bind(self, item, limit):
        limit = 1 if limit is None else limit
        if type(limit) is not int or limit not in {1, 2, 4, 6, 8}:
            raise ValueError("Unsupported local comments limit")
        self.item, self.limit = dict(item), limit

    def wait(self, seconds=1.0):
        if self.active:
            wait([entry[1] for entry in self.active.values()], timeout=seconds, return_when=FIRST_COMPLETED)
        else:
            self.abort.wait(seconds)

    def drain(self, store, *, finish=False, closing=False):
        if finish and self.active:
            wait([entry[1] for entry in self.active.values()])
        for token, (item, future) in list(self.active.items()):
            if not future.done():
                continue
            try:
                result = future.result()
            except Exception:
                # execute owns child reaping before returning OR raising.
                result = {"outcome": "unresolved"}
            try:
                if result.get("outcome") in {"unresolved", "fenced"}:
                    control_call(store.transition, item, token, "expired")
            except LostOwnership:
                if not closing:
                    raise
            del self.active[token]

    def close(self, store):
        if self.active:
            self.abort.set()
        try:
            # Reap every child before any possibly unavailable SQL cleanup.
            if self.pool is not None:
                self.pool.shutdown(wait=True)
            self.drain(store, finish=True, closing=True)
        finally:
            self.pool = None

    def __call__(self, reservation):
        if self.abort.is_set():
            raise Interrupted
        token = reservation["owner_token"]
        if token in self.submitted:
            return None
        if reservation["stage"] == "comments":
            if self.item is None or len(self.active) >= self.limit:
                raise RuntimeError("Local comments dispatch has no available slot")
            if self.pool is None:
                self.pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="instagram-local-comment")
            future = self.pool.submit(
                self.execute,
                reservation,
                execution_placement="local",
                timeout_seconds=ACTIVITY_TIMEOUT_SECONDS,
                abort=self.abort,
            )
            self.active[token] = (dict(self.item), future)
            self.submitted.add(token)
            self.comments_submitted += 1
            return None
        if self.active:
            raise RuntimeError("Non-comment dispatch cannot overlap comment children")
        result = self.execute(
            reservation,
            execution_placement="local",
            timeout_seconds=ACTIVITY_TIMEOUT_SECONDS,
            abort=self.abort,
        )
        self.latest = (reservation, result)
        self.submitted.add(token)
        # Synchronous completion has no remote FunctionCall identity.
        return None


def run_partition(
    item,
    *,
    store,
    scheduler,
    dispatch,
    abort,
    effective_comments_limit=None,
    before_finalize=None,
    on_completed=None,
):
    """Mirror DBOSAdapter.partition without the SDK; return how the partition ended."""
    batch_start = getattr(dispatch, "comments_submitted", 0)
    for ordinal in range(PARTITION_SIZE):
        batch_limit = effective_comments_limit(item) if effective_comments_limit else 1
        prefer_media = getattr(dispatch, "comments_submitted", 0) - batch_start >= (batch_limit or 1)
        decision_item = dict(item, prefer_media=True) if prefer_media else item
        if prefer_media:
            dispatch.drain(store, finish=True)
            batch_start = getattr(dispatch, "comments_submitted", 0)
        while True:
            dispatch.bind(item, effective_comments_limit(item) if effective_comments_limit else 1)
            dispatch.drain(store)
            if abort.is_set():
                raise Interrupted
            if len(dispatch.active) < dispatch.limit:
                break
            dispatch.wait()
        if dispatch.active:
            candidate = control_call(store.decision, decision_item, ordinal, scheduler.clock())
            if candidate.get("stage") not in {None, "comments"} or candidate["kind"] in {"complete", "blocked"}:
                # A committed comment child can still be finishing/reaping.
                # Drain before reserving the next phase or completing this one.
                dispatch.drain(store, finish=True)
                if abort.is_set():
                    raise Interrupted
        dispatch.latest = None
        try:
            decision = scheduler.tick(decision_item, ordinal)
        except LostOwnership:
            if dispatch.active:
                abort.set()
                raise Interrupted from None
            return "fenced"
        latest = dispatch.latest
        if latest is not None and latest[1].get("outcome") in {"unresolved", "fenced"}:
            # The activity's child was reaped before execute returned; no remote
            # call remains. Expire the owner so the target cooldown applies.
            store.transition(item, latest[0]["owner_token"], "expired")
        if decision.get("stop"):
            dispatch.drain(store, finish=True)
            if abort.is_set():
                raise Interrupted
            if before_finalize is not None and before_finalize(item, stop=decision["stop"]):
                # The journaled completion choice precedes a fenced recovery
                # command. The next ordinal sees the newly reopened target.
                continue
            result = _complete(store, item, stop=decision["stop"])
            if result == "stopped" and on_completed is not None:
                on_completed(item)
            return result
        # Journaled waits can outlive their original target deadline after
        # operator recovery. Recheck live scheduling state at the next ordinal;
        # target and source gates still enforce any actual future server wait.
        if decision.get("sleep"):
            dispatch.wait(min(decision["sleep"], WAIT_RECHECK_SECONDS))
            if abort.is_set():
                raise Interrupted
    return _complete(store, item, stop=None)


def _complete(store, item, *, stop):
    try:
        control_call(store.complete_partition, item, continue_run=stop is None, stop_state=stop)
    except LostOwnership:
        return "fenced"
    return "stopped" if stop else "continued"


def run_worker(
    *,
    store,
    scheduler,
    dispatch,
    lease,
    abort,
    workflow_version,
    stop_requested=None,
    poll_seconds=5.0,
    on_cycle=None,
    effective_comments_limit=None,
    before_finalize=None,
    on_completed=None,
    on_started=None,
):
    """Serve local runs until abort. Returns a JSON-safe summary.

    abort is set by a signal (stop_requested is set too) or by lease loss.
    """
    if store.execution_placement != "local":
        raise ValueError("The Mac worker requires a local-fenced workflow store")
    partitions = 0
    with lease:
        try:
            if on_started is not None:
                on_started()
            while not abort.is_set():
                # New runs: seed partition 0 only; dispatch_partitions runs it.
                store.dispatch_pending(store.seed, engine="dbos", workflow_version=workflow_version)
                ran = store.dispatch_partitions(
                    lambda item: run_partition(
                        item,
                        store=store,
                        scheduler=scheduler,
                        dispatch=dispatch,
                        abort=abort,
                        effective_comments_limit=effective_comments_limit,
                        before_finalize=before_finalize,
                        on_completed=on_completed,
                    ),
                    limit=1,
                    workflow_version=workflow_version,
                )
                partitions += ran
                if on_cycle is not None:
                    on_cycle(ran)
                if not ran:
                    dispatch.wait(poll_seconds)
        except Interrupted:
            pass
        finally:
            dispatch.close(store)
    requested = stop_requested is not None and stop_requested.is_set()
    return {"status": "lease_lost" if abort.is_set() and not requested else "stopped", "partitions": partitions}


def main(argv=None):
    abort = threading.Event()
    stop_requested = threading.Event()
    previous = {}
    exit_guard = AbortExitGuard(abort)
    exit_guard.start()

    def request_stop(*_):
        stop_requested.set()
        abort.set()

    try:
        import psycopg

        from scripts.ingestion.local_driver import CoordinatorLease, validate_configuration
        from trr_backend.ingestion.admission import Admission
        from trr_backend.ingestion.composition import (
            LOCAL_WORKER_LOCK_KEY,
            LOCAL_WORKER_LOCK_NAMESPACE,
            WORKFLOW_VERSION,
        )
        from trr_backend.ingestion.execution_policy import ExecutionPolicy
        from trr_backend.ingestion.runtime import coordinator_enabled, run_ingestion_activity
        from trr_backend.ingestion.workflow_store import WorkflowStore
        from trr_backend.ingestion.workflows import RuntimeConfig, Scheduler

        if not coordinator_enabled():
            print(json.dumps({"status": "disabled"}), flush=True)
            return 0
        dsn = os.environ["TRR_INSTAGRAM_DBOS_DATABASE_URL"].strip()
        role = os.environ["TRR_INSTAGRAM_DBOS_RUNTIME_ROLE"].strip()
        validate_configuration(dsn)
        config = RuntimeConfig(
            dsn,
            WORKFLOW_VERSION,
            executor_id="instagram-local-worker",
            lock_namespace=LOCAL_WORKER_LOCK_NAMESPACE,
            lock_key=LOCAL_WORKER_LOCK_KEY,
            runtime_role=role,
        )
        lease = CoordinatorLease(lambda: psycopg.connect(dsn, autocommit=True, connect_timeout=5), config, abort)
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, request_stop)
        store = WorkflowStore(execution_placement="local")
        transaction = getattr(store, "transaction", None)
        policy = ExecutionPolicy(transaction=transaction) if callable(transaction) else None
        dispatch = LocalDispatch(run_ingestion_activity, abort)
        scheduler = Scheduler(store, admit=Admission().admit, dispatch=dispatch, reconcile=lambda _: "unknown")
        recovery_run = os.getenv("TRR_INSTAGRAM_JANUARY_RECOVERY_RUN_ID", "").strip()

        def before_finalize(item, *, stop):
            if not recovery_run or item["run_id"] != recovery_run:
                return False
            from trr_backend.ingestion.comment_recovery import enqueue_next_recovery

            result = enqueue_next_recovery(
                store,
                run_id=recovery_run,
                account=item["account"],
                generation=item["generation"],
                intent_hash=os.environ["TRR_INSTAGRAM_JANUARY_RECOVERY_INTENT_HASH"],
                source_revision=os.environ["TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION"],
                blocked_stage=stop == "blocked",
            )
            if result:
                print(json.dumps({"status": "public_recovery_queued", **result}), flush=True)
            return result is not None

        def certify_completed(item):
            if item["run_id"] != recovery_run:
                return
            output = os.getenv("TRR_INSTAGRAM_JANUARY_CERTIFICATION_OUTPUT", "").strip()
            if not output:
                return
            artifact = Path(output)
            if retained_certification(artifact):
                # A durable artifact is never overwritten by lifecycle replay.
                print(json.dumps({"status": "certification_artifact_retained", "path": output}), flush=True)
                return
            command = [
                sys.executable,
                str(Path(__file__).with_name("certify_january.py")),
                "--run-id",
                recovery_run,
                "--intent-hash",
                os.environ["TRR_INSTAGRAM_JANUARY_RECOVERY_INTENT_HASH"],
                "--generation",
                str(item["generation"]),
                "--account",
                item["account"],
                "--date-start",
                "2026-01-01T00:00:00Z",
                "--date-end",
                "2026-02-01T00:00:00Z",
                "--binding",
                os.environ["TRR_INSTAGRAM_JANUARY_BINDING"],
                "--storage-binding",
                os.environ["TRR_INSTAGRAM_JANUARY_STORAGE_BINDING"],
                "--expected-storage-binding-sha256",
                os.environ["TRR_INSTAGRAM_JANUARY_STORAGE_BINDING_SHA256"],
                "--output",
                output,
            ]
            # Known maintained command only; one bounded child, reaped before
            # this worker resumes. No cron or separate monitoring process.
            log_path = artifact.with_suffix(f".{os.getpid()}-{time.time_ns()}.log")
            try:
                with log_path.open("x") as log:
                    log_path.chmod(0o600)
                    returncode = run_owned_certification(command, log, abort)
                print(
                    json.dumps({"status": "certification_finished", "returncode": returncode, "path": output}),
                    flush=True,
                )
            except subprocess.TimeoutExpired:
                print(json.dumps({"status": "certification_timeout", "log": str(log_path)}), flush=True)

        def resume_certification():
            if not recovery_run:
                return
            with store.transaction() as conn, conn.cursor() as cur:
                cur.execute(
                    """select account,generation,state from social.instagram_ingestion_runs
                    where run_id=%s and intent_hash=%s and execution_placement='local'""",
                    (recovery_run, os.environ["TRR_INSTAGRAM_JANUARY_RECOVERY_INTENT_HASH"]),
                )
                row = cur.fetchone()
            if row and row[2] in {"completed", "blocked"}:
                certify_completed({"run_id": recovery_run, "account": row[0], "generation": row[1]})

        print(json.dumps({"status": "started", "execution_placement": "local"}), flush=True)
        result = run_worker(
            store=store,
            scheduler=scheduler,
            dispatch=dispatch,
            lease=lease,
            abort=abort,
            workflow_version=WORKFLOW_VERSION,
            stop_requested=stop_requested,
            effective_comments_limit=policy.effective_comments_limit if policy else None,
            before_finalize=before_finalize if recovery_run else None,
            on_completed=certify_completed if recovery_run else None,
            on_started=resume_certification if recovery_run else None,
            on_cycle=lambda ran: ran and print(json.dumps({"status": "partition", "count": ran}), flush=True),
        )
        print(json.dumps(result), flush=True)
        return 125 if result["status"] == "lease_lost" else 0
    except Exception as error:
        # No traceback, argument or message may expose a DSN or token.
        print(json.dumps({"status": "failed", "failure_class": type(error).__name__}), flush=True)
        return 3 if type(error).__name__ == "Conflict" else 1
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        try:
            from trr_backend.db import pg

            pg.close_pool()
        except Exception:  # noqa: BLE001 - shutdown must not mask the exit status
            pass
        # Keep the independent guard armed through lease and pool cleanup: a
        # blocked connection must not leave launchd supervising a dead worker.
        exit_guard.close()


if __name__ == "__main__":
    raise SystemExit(main())
