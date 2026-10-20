#!/usr/bin/env python3
"""Durable Mac worker for logged-out Instagram ingestion runs placed on `local`.

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
import sys
import threading
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.workflow_store import PARTITION_SIZE
from trr_backend.ingestion.workflows import control_call

ACTIVITY_TIMEOUT_SECONDS = 590


class Interrupted(Exception):  # noqa: N818 - control-flow signal, not an error
    """Stop requested mid-partition; leave the partition unacknowledged."""


class LocalDispatch:
    """Synchronous dispatch: the activity finishes before the scheduler continues."""

    def __init__(self, execute, abort):
        self.execute, self.abort = execute, abort
        self.latest = None

    def __call__(self, reservation):
        if self.abort.is_set():
            raise Interrupted
        result = self.execute(
            reservation,
            execution_placement="local",
            timeout_seconds=ACTIVITY_TIMEOUT_SECONDS,
            abort=self.abort,
        )
        self.latest = (reservation, result)
        # Synchronous completion has no remote FunctionCall identity.
        return None


def run_partition(item, *, store, scheduler, dispatch, abort):
    """Mirror DBOSAdapter.partition without the SDK; return how the partition ended."""
    for ordinal in range(PARTITION_SIZE):
        if abort.is_set():
            raise Interrupted
        dispatch.latest = None
        try:
            decision = scheduler.tick(item, ordinal)
        except LostOwnership:
            return "fenced"
        latest = dispatch.latest
        if latest is not None and latest[1].get("outcome") in {"unresolved", "fenced"}:
            # The activity's child was reaped before execute returned; no remote
            # call remains. Expire the owner so the target cooldown applies.
            store.transition(item, latest[0]["owner_token"], "expired")
        if decision.get("stop"):
            return _complete(store, item, stop=decision["stop"])
        if decision.get("sleep") and abort.wait(decision["sleep"]):
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
):
    """Serve local runs until abort. Returns a JSON-safe summary.

    abort is set by a signal (stop_requested is set too) or by lease loss.
    """
    if store.execution_placement != "local":
        raise ValueError("The Mac worker requires a local-fenced workflow store")
    partitions = 0
    with lease:
        while not abort.is_set():
            try:
                # New runs: seed partition 0 only; dispatch_partitions runs it.
                store.dispatch_pending(store.seed, engine="dbos", workflow_version=workflow_version)
                ran = store.dispatch_partitions(
                    lambda item: run_partition(item, store=store, scheduler=scheduler, dispatch=dispatch, abort=abort),
                    limit=1,
                    workflow_version=workflow_version,
                )
            except Interrupted:
                break
            partitions += ran
            if on_cycle is not None:
                on_cycle(ran)
            if not ran:
                abort.wait(poll_seconds)
    requested = stop_requested is not None and stop_requested.is_set()
    return {"status": "lease_lost" if abort.is_set() and not requested else "stopped", "partitions": partitions}


def main(argv=None):
    abort = threading.Event()
    stop_requested = threading.Event()
    previous = {}

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
        dispatch = LocalDispatch(run_ingestion_activity, abort)
        scheduler = Scheduler(store, admit=Admission().admit, dispatch=dispatch, reconcile=lambda _: "unknown")
        print(json.dumps({"status": "started", "execution_placement": "local"}), flush=True)
        result = run_worker(
            store=store,
            scheduler=scheduler,
            dispatch=dispatch,
            lease=lease,
            abort=abort,
            workflow_version=WORKFLOW_VERSION,
            stop_requested=stop_requested,
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


if __name__ == "__main__":
    raise SystemExit(main())
