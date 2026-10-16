"""Bounded DBOS 3.3 adapter. Source activities and Modal admission are injected.

No engine retries wrap provider work. Store.acquire is mandatory in the remote
callback before any transport call; its existing generation/lease fence owns
attempt accounting. Only an existing maintenance owner may wake this adapter.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version

from trr_backend.ingestion.domain import Conflict, LostOwnership
from trr_backend.ingestion.workflow_store import PARTITION_SIZE, WorkflowStore


class ControlPlaneError(RuntimeError):
    """Safe error classification; never serialize a connection string or token."""


def control_call(operation: Callable, *args, **kwargs):
    """Retry only one idempotent control operation, never a provider callback."""
    for attempt in range(3):
        try:
            return operation(*args, **kwargs)
        except (LostOwnership, Conflict):
            raise
        except Exception as exc:
            if attempt == 2:
                raise ControlPlaneError(type(exc).__name__) from None
            time.sleep(0.05 * (attempt + 1))


class Scheduler:
    def __init__(
        self,
        store: WorkflowStore,
        *,
        admit: Callable,
        dispatch: Callable,
        reconcile: Callable,
        clock: Callable = lambda: datetime.now(UTC),
    ):
        self.store = store
        self.admit = admit
        self.dispatch = dispatch
        self.reconcile = reconcile
        self.clock = clock

    def tick(self, item, ordinal):
        """One bounded decision. DBOS may replay this after any interruption."""
        decision = control_call(self.store.decision, item, ordinal, self.clock())
        kind = decision["kind"]
        if kind in {"complete", "blocked"}:
            return {"stop": "completed" if kind == "complete" else "blocked"}
        if kind == "wait":
            return {"sleep": decision["seconds"]}
        if kind == "reconcile" or decision.get("owner_token"):
            reservation = control_call(self.store.reservation, item, decision["owner_token"])
        else:
            # Admission is outside SQL transactions and before any reservation.
            try:
                admitted = self.admit(dict(item, **decision))
            except Exception:
                control_call(self.store.note_control_wait, item, ordinal, "ADMISSION_UNAVAILABLE")
                return {"sleep": 1.0, "control_failure": "ADMISSION_UNAVAILABLE"}
            if not admitted:
                return {"sleep": 5.0}
            reservation = control_call(self.store.reserve_decision, item, decision, ordinal=ordinal)
            if not reservation:
                return {"sleep": 0.01}
        token = reservation["owner_token"]
        status = control_call(self.store.local_dispatch_status, item, token, self.clock())
        if status in {"settled", "accepted"}:
            return {"ready": True} if status == "settled" else {"sleep": 1.0}
        if status == "expired":
            control_call(self.store.transition, item, token, "expired")
            return {"sleep": 0.01}
        if status == "sending":
            try:
                remote = self.reconcile(reservation)
            except Exception:
                return {"sleep": 1.0}
            if remote not in {"accepted", "absent", "unknown"}:
                raise ValueError("Reconciler must return accepted, absent or unknown")
            if remote == "accepted":
                control_call(self.store.transition, item, token, "accepted")
                return {"sleep": 1.0}
            if remote == "unknown":
                return {"sleep": 1.0}
            # Proven absence permits resending the identical valid reservation.
            if not control_call(self.store.transition, item, token, "reserved"):
                return {"sleep": 1.0}
        # Persist uncertainty BEFORE external dispatch. Lost responses are
        # reconciled on the next invocation instead of blind redelivery.
        if not control_call(self.store.transition, item, token, "sending"):
            return {"sleep": 1.0}
        try:
            identity = self.dispatch(reservation)
        except Exception:
            return {"sleep": 1.0}
        control_call(self.store.transition, item, token, "accepted", str(identity) if identity is not None else None)
        return {"ready": True}


@dataclass(frozen=True)
class RuntimeConfig:
    system_database_url: str = field(repr=False)
    workflow_version: str
    executor_id: str = "instagram-coordinator-slot-0"
    lock_namespace: int = 48112
    lock_key: int = 20261016
    lifetime_seconds: float = 90.0
    runtime_role: str = "trr_ingestion_runtime"

    def __post_init__(self):
        if not self.workflow_version or not self.executor_id or not self.runtime_role:
            raise ValueError("Explicit compatible version, executor and restricted role required")
        if not 1 <= self.lifetime_seconds <= 110:
            raise ValueError("Runtime budget must leave cleanup time before the existing 120s wakeup")

    def dbos_config(self):
        return {
            "name": "trr-instagram-ingestion",
            "application_version": self.workflow_version,
            "executor_id": self.executor_id,
            "system_database_url": self.system_database_url,
            "sys_db_pool_size": 1,
            "sys_db_polling_concurrency": 1,
            "max_executor_threads": 2,
            "run_migrations": False,
            "admin_server": False,
            "enable_otlp": False,
            "db_engine_kwargs": {
                "max_overflow": 0,
                "pool_timeout": 5,
                "connect_args": {"application_name": "trr-instagram-dbos", "connect_timeout": 3},
            },
        }


class DBOSAdapter:
    """One process binding, registered before launch; immutable run versions only."""

    def __init__(self, scheduler: Scheduler, config: RuntimeConfig):
        if version("dbos") != "3.3.0":
            raise RuntimeError("This adapter is validated only with DBOS 3.3.0")
        from dbos import DBOS

        self.scheduler = scheduler
        self.config = config
        self.dbos = DBOS
        self._engine_scan_after = None

        @DBOS.step(retries_allowed=False)
        def schedule(item, ordinal):
            try:
                return scheduler.tick(item, ordinal)
            except LostOwnership:
                return {"fenced": True}

        @DBOS.step(retries_allowed=False)
        def continuation(item, stop):
            try:
                return control_call(
                    scheduler.store.complete_partition, item, continue_run=stop is None, stop_state=stop
                )
            except LostOwnership:
                return None

        @DBOS.workflow()
        def partition(item):
            for ordinal in range(PARTITION_SIZE):
                result = schedule(item, ordinal)
                if result.get("fenced"):
                    return None
                if result.get("stop"):
                    return continuation(item, result["stop"])
                if result.get("sleep"):
                    DBOS.sleep(result["sleep"])
            return continuation(item, None)

        self.partition = partition

    def submit(self, item):
        from dbos import SetWorkflowID

        if item["engine"] != "dbos" or item["workflow_version"] != self.config.workflow_version:
            raise Conflict("Pinned run requires its original compatible executor")
        with SetWorkflowID(item["execution_id"]):
            return self.dbos.start_workflow(self.partition, item)

    def submit_initial(self, item):
        # The business outbox caller acknowledges only after deterministic submit.
        self.submit(self.scheduler.store.seed(item))

    def reconcile_engine_failures(self):
        # A bounded round-robin scan includes acknowledged partitions: terminal
        # SDK errors are not recovered by DBOS.launch or resume_workflow.
        items = control_call(
            self.scheduler.store.engine_partitions,
            workflow_version=self.config.workflow_version,
            after_execution_id=self._engine_scan_after,
        )
        if not items:
            self._engine_scan_after = None
            return 0
        self._engine_scan_after = items[-1]["execution_id"]
        paused = 0
        for item in items:
            status = self.dbos.get_workflow_status(item["execution_id"])
            if status is None:
                self.submit(item)  # Idempotent recovery of an ambiguous initial submission.
            elif status.status in {"ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "CANCELLED", "SUCCESS"}:
                try:
                    paused += int(control_call(self.scheduler.store.pause_failed_partition, item, status.status))
                except LostOwnership:
                    pass
        return paused

    def run_bounded(self, lease_connect: Callable, *, fatal_exit: Callable = os._exit):
        """Acquire a session lock BEFORE SDK threads; fail closed on lease loss.

        lease_connect must use a session-preserving connection, never transaction
        pooling. Bootstrap/migrations belong to the separate deployment owner.
        Must run in a dedicated subprocess, never a shared API/Modal process.
        After SDK initialization this method terminates the subprocess, including
        normal shutdown; it does not release the lease and return to a caller.
        """
        lease = lease_connect()
        stop = threading.Event()
        deadline = None
        monitor = None
        initialized = False
        exit_code = 1
        try:
            lease.autocommit = True
            with lease.cursor() as cur:
                cur.execute("select pg_try_advisory_lock(%s,%s)", (self.config.lock_namespace, self.config.lock_key))
                if not cur.fetchone()[0]:
                    return "busy"
                # Reject owner/superuser/DDL roles rather than trusting a label.
                cur.execute("""select current_user,rolsuper,has_schema_privilege(current_user,'dbos','USAGE'),
                    has_schema_privilege(current_user,'dbos','CREATE'),
                    exists(select 1 from pg_namespace where nspname='dbos' and nspowner=(
                        select oid from pg_roles where rolname=current_user))
                    from pg_roles where rolname=current_user""")
                role, superuser, usage, create, owner = cur.fetchone()
                if role != self.config.runtime_role or superuser or owner or create or not usage:
                    raise RuntimeError("Restricted pre-migrated DBOS runtime role is required")

            def lease_monitor():
                while not stop.wait(0.25):
                    try:
                        with lease.cursor() as cur:
                            cur.execute("select 1")
                    except Exception:
                        fatal_exit(125)
                        return

            monitor = threading.Thread(target=lease_monitor, daemon=True, name="instagram-coordinator-lease")
            monitor.start()
            # SDK initialization and launch consume the same finite lifetime
            # as scheduling. Retain the existing five-second shutdown allowance.
            started = time.monotonic()
            hard_deadline = started + self.config.lifetime_seconds
            end = started + max(0.0, self.config.lifetime_seconds - 5)
            deadline = threading.Timer(max(0.0, hard_deadline - time.monotonic()), lambda: fatal_exit(124))
            deadline.daemon = True
            deadline.start()
            initialized = True
            self.dbos(config=self.config.dbos_config())
            self.dbos.launch()
            next_error_scan = 0.0
            while time.monotonic() < end:
                if time.monotonic() >= next_error_scan:
                    self.reconcile_engine_failures()
                    next_error_scan = time.monotonic() + 5.0
                self.scheduler.store.dispatch_pending(
                    self.submit_initial, engine="dbos", workflow_version=self.config.workflow_version
                )
                self.scheduler.store.dispatch_partitions(self.submit, workflow_version=self.config.workflow_version)
                stop.wait(1.0)
            exit_code = 0
            return "bounded"
        finally:
            # DBOS 3.3 destroy uses executor.shutdown(wait=False). Only process
            # termination proves no workflow thread survives; retain the session
            # lock until the OS closes it, including on initialization failure.
            if initialized:
                try:
                    self.dbos.destroy(workflow_completion_timeout_sec=2)
                finally:
                    fatal_exit(exit_code)
            stop.set()
            if monitor:
                monitor.join(timeout=1)
            if deadline:
                deadline.cancel()
            lease.close()
