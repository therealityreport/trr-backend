#!/usr/bin/env python3
"""Explicit bounded local placement for the approved public BravoTV 2026 run.

No source/client or credential changes, DBOS SDK, new scheduler, or implicit
cloud fallback. DSNs come only from explicit environment configuration, never
arguments, files, defaults, or output. Without --apply this prints a plan only.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, LostOwnership, RunIntent, fingerprint
from trr_backend.ingestion.workflow_store import PARTITION_SIZE, WorkflowStore
from trr_backend.ingestion.workflows import RuntimeConfig, Scheduler


@dataclass(frozen=True)
class LocalRequest:
    run_id: str
    intent_hash: str
    generation: int
    expected_placement: str
    placement: str = "local"
    max_targets: int = 1
    max_seconds: float = 60

    def __post_init__(self):
        import re

        UUID(self.run_id)
        if not re.fullmatch(r"[0-9a-f]{64}", self.intent_hash):
            raise ValueError("Exact frozen intent hash required")
        if type(self.generation) is not int or self.generation < 1:
            raise ValueError("Exact positive generation required")
        if self.expected_placement not in {"modal", "local"} or self.placement not in {"modal", "local"}:
            raise ValueError("Explicit supported placement required")
        if type(self.max_targets) is not int or not 1 <= self.max_targets <= 100:
            raise ValueError("Target limit must be 1 to 100")
        if not math.isfinite(self.max_seconds) or not 1 <= self.max_seconds <= 600:
            raise ValueError("Time limit must be 1 to 600 seconds")


def validate_run(store, request, *, expected_workflow_version=None):
    """No mutation; exact scope/version/generation before placement and resume."""
    from trr_backend.ingestion.composition import WORKFLOW_VERSION

    expected_workflow_version = expected_workflow_version or WORKFLOW_VERSION
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        run = store._run(cur, request.run_id, "bravotv")
    intent = RunIntent.from_payload(run["intent"])
    if (
        run["state"] != "cancelled"
        or run["generation"] != request.generation
        or run.get("execution_placement", "modal") != request.expected_placement
        or run["intent_hash"] != request.intent_hash
        or intent.digest != request.intent_hash
        or intent.effective_source_profile.access_mode != "public"
        or run.get("execution_hold_reason")
        or run["engine"] != "dbos"
        or run["workflow_version"] != expected_workflow_version
        or intent.account != "bravotv"
        or intent.action != "backfill"
        or intent.scope != "bounded_window"
        or intent.date_start != datetime(2026, 1, 1, tzinfo=UTC)
        or intent.date_end != datetime(2027, 1, 1, tzinfo=UTC)
    ):
        raise Conflict("Local driver requires the exact unheld cancelled public BravoTV calendar-2026 run")
    return run


class DiagnosticAbort(threading.Event):
    """An Event with best-effort first-cause metadata, never exception text."""

    def __init__(self):
        super().__init__()
        self.diagnostic = None
        self._diagnostic_lock = threading.Lock()
        self.lease_metadata = None
        self._diagnostic_error = None
        self._diagnostic_metadata = {}

    def record(self, origin, *, error=None, operation=None, **metadata):
        # Only fixed primitive labels and references here: no clocks, optional
        # callbacks, exception accessors, logging or database operations.
        if (
            origin
            not in {
                "lease_heartbeat_exception",
                "lease_heartbeat_deadline",
                "lease_cleanup_failure",
                "lease_cleanup_deadline",
                "control_owner_lost",
                "dispatch_shutdown",
                "signal_requested",
            }
            or self.diagnostic is not None
        ):
            return
        if not self._diagnostic_lock.acquire(blocking=False):
            return
        try:
            if self.diagnostic is None:
                record = {"origin": origin}
                if operation in {"decision", "scheduler_tick", "transition", "unlock", "close"}:
                    record["operation"] = operation
                self._diagnostic_error = error
                self._diagnostic_metadata = metadata
                self.diagnostic = record
        finally:
            self._diagnostic_lock.release()

    def context(self):
        """Optional enrichment. Called only by the independent reporter."""
        classes = {
            "OperationalError",
            "InterfaceError",
            "DatabaseError",
            "InternalError",
            "Error",
            "ConnectionTimeout",
            "ConnectionFailure",
            "ConnectionDoesNotExist",
            "AdminShutdown",
            "IdleSessionTimeout",
            "QueryCanceled",
            "ProtocolViolation",
            "LostOwnership",
            "RuntimeError",
            "OSError",
            "TimeoutError",
        }
        record = {}
        try:
            metadata = self._diagnostic_metadata
            captured_age = "heartbeat_age_seconds" in metadata
            if self.lease_metadata is not None:
                try:
                    metadata = {**self.lease_metadata(), **metadata}
                except BaseException:
                    pass
            record["timing_scope"] = "report_time"
            if captured_age:
                record["heartbeat_age_scope"] = "watchdog_abort_comparison"
            error = self._diagnostic_error
            if error is not None:
                name = type(error).__name__
                record["exception_class"] = name if name in classes else "OtherError"
                state = getattr(error, "sqlstate", None)
                if type(state) is str and state in {
                    "08000",
                    "08001",
                    "08003",
                    "08004",
                    "08006",
                    "08007",
                    "08P01",
                    "57P01",
                    "57P02",
                    "57P03",
                    "57P05",
                    "57014",
                    "53200",
                    "53300",
                    "53400",
                    "55P03",
                    "40001",
                    "40P01",
                    "XX000",
                }:
                    record["sqlstate"] = state
            for key in ("heartbeat_age_seconds", "select_inflight_seconds", "last_select_seconds"):
                value = metadata.get(key)
                if type(value) in (int, float) and math.isfinite(value) and value >= 0:
                    record[key] = value
            pid = metadata.get("lease_backend_pid")
            if type(pid) is int and 0 < pid < 2**31:
                record["lease_backend_pid"] = pid
        except BaseException:
            pass
        return record


def request_abort(abort, origin, **metadata):
    try:
        if isinstance(abort, DiagnosticAbort):
            abort.record(origin, **metadata)
    except BaseException:
        pass
    finally:
        abort.set()


class CoordinatorLease:
    """The same session-preserving advisory lease as the existing coordinator."""

    def __init__(self, connect, config, abort, *, heartbeat_timeout=2.0, clock=time.monotonic):
        if not math.isfinite(heartbeat_timeout) or not 0 < heartbeat_timeout <= 5:
            raise ValueError("A bounded lease heartbeat deadline is required")
        self.connect, self.config, self.abort = connect, config, abort
        self.clock, self.heartbeat_timeout = clock, heartbeat_timeout
        self.connection = None
        self.stop = threading.Event()
        self.monitor = None
        self.watchdog = None
        self.last_heartbeat = None
        self.acquired = False
        self.previous_idle_timeout = None
        self.backend_pid = None
        self.select_started = None
        self.last_select_seconds = None

    def __enter__(self):
        self.connection = self.connect()
        try:
            self.connection.autocommit = True
            if isinstance(self.abort, DiagnosticAbort):
                self.abort.lease_metadata = self._diagnostic_metadata
            with self.connection.cursor() as cur:
                cur.execute(
                    "select pg_try_advisory_lock(%s,%s),pg_backend_pid()",
                    (self.config.lock_namespace, self.config.lock_key),
                )
                acquired = cur.fetchone()
                self.backend_pid = acquired[1] if len(acquired) > 1 else None
                if not acquired[0]:
                    raise Conflict("The existing ingestion coordinator lease is busy")
                self.acquired = True
                cur.execute("""select current_user,rolsuper,has_schema_privilege(current_user,'dbos','USAGE'),
                    has_schema_privilege(current_user,'dbos','CREATE'),
                    exists(select 1 from pg_namespace where nspname='dbos' and nspowner=(
                        select oid from pg_roles where rolname=current_user))
                    from pg_roles where rolname=current_user""")
                role, superuser, usage, create, owner = cur.fetchone()
                if role != self.config.runtime_role or superuser or owner or create or not usage:
                    raise Conflict("Restricted pre-migrated coordinator runtime role required")
                # Session poolers can retain the backend after a frontend dies.
                # Keep server-side expiry armed until this exact lease releases.
                cur.execute("select current_setting('idle_session_timeout')")
                self.previous_idle_timeout = cur.fetchone()[0]
                cur.execute("select set_config('idle_session_timeout','5000ms',false)")
            self.last_heartbeat = self.clock()
            self.monitor = threading.Thread(target=self._monitor, daemon=True, name="instagram-local-lease")
            self.watchdog = threading.Thread(
                target=self._watch_heartbeat, daemon=True, name="instagram-local-lease-deadline"
            )
            self.watchdog.start()
            self.monitor.start()
            return self
        except BaseException:
            self.stop.set()
            if self.watchdog and self.watchdog.is_alive():
                self.watchdog.join(timeout=1)
            self._close_connection()
            raise

    def _close_connection(self):
        def cleanup():
            try:
                # Never queue SQL behind a heartbeat stuck on the same connection.
                if self.acquired and not (self.monitor and self.monitor.is_alive()):
                    with self.connection.cursor() as cur:
                        cur.execute(
                            "select pg_advisory_unlock(%s,%s)", (self.config.lock_namespace, self.config.lock_key)
                        )
                        if not cur.fetchone()[0]:
                            raise RuntimeError("Coordinator lease release was not confirmed")
                        # Session acquisitions stack: one successful unlock can
                        # leave an older pooled acquisition on this backend.
                        cur.execute(
                            """select exists(select 1 from pg_locks
                            where locktype='advisory' and granted and pid=pg_backend_pid()
                            and database=(select oid from pg_database where datname=current_database())
                            and classid=%s::oid and objid=%s::oid and objsubid=2)""",
                            (self.config.lock_namespace, self.config.lock_key),
                        )
                        if cur.fetchone()[0]:
                            raise RuntimeError("Coordinator lease remains held on this backend")
                        self.acquired = False
                        if self.previous_idle_timeout is not None:
                            cur.execute(
                                "select set_config('idle_session_timeout',%s,false)", (self.previous_idle_timeout,)
                            )
            except Exception as error:
                # Leave server expiry armed on uncertainty; cleanup cannot mask
                # the original failure or defeat the independent abort guard.
                self._abort("lease_cleanup_failure", error=error)
            finally:
                try:
                    self.connection.close()
                except Exception as error:
                    self._abort("lease_cleanup_failure", error=error, operation="close")

        cleanup_thread = threading.Thread(target=cleanup, daemon=True, name="instagram-local-lease-cleanup")
        cleanup_thread.start()
        cleanup_thread.join(timeout=self.heartbeat_timeout)
        if cleanup_thread.is_alive():
            self._abort("lease_cleanup_deadline")

    def _abort(self, origin, **metadata):
        request_abort(self.abort, origin, **metadata)

    def _diagnostic_metadata(self):
        now = self.clock()
        return {
            "lease_backend_pid": self.backend_pid,
            "heartbeat_age_seconds": max(0, now - self.last_heartbeat) if self.last_heartbeat is not None else None,
            "select_inflight_seconds": max(0, now - self.select_started) if self.select_started is not None else None,
            "last_select_seconds": self.last_select_seconds,
        }

    def _monitor(self):
        while not self.stop.wait(0.1):
            try:
                self.select_started = self.clock()
                with self.connection.cursor() as cur:
                    cur.execute("select 1")
                completed = self.clock()
                self.last_select_seconds = max(0, completed - self.select_started)
                self.last_heartbeat = completed
                self.select_started = None
            except Exception as error:
                self._abort("lease_heartbeat_exception", error=error)
                return

    def _watch_heartbeat(self):
        # This thread never touches the connection or its lock. A SELECT that
        # never returns must still abort the runtime, which reaps the activity
        # before run_local attempts cancellation or connection cleanup.
        while not self.stop.wait(min(0.1, self.heartbeat_timeout / 4)):
            if self.abort.is_set():
                return
            age = self.clock() - self.last_heartbeat
            if age >= self.heartbeat_timeout:
                self._abort("lease_heartbeat_deadline", heartbeat_age_seconds=age)
                return

    def __exit__(self, *_):
        self.stop.set()
        if self.watchdog:
            self.watchdog.join(timeout=1)
        if self.monitor:
            self.monitor.join(timeout=2)
        if self.connection:
            self._close_connection()


def run_local(request, *, store, admit, execute, lease, abort, clock=time.monotonic):
    """One serial consumer using the real scheduler and persisted business state.

    execute must reap its private child before returning/raising. The production
    callback is run_ingestion_activity; its local watchdog exits on driver death.
    Tests inject a source-free target execution to verify durable boundaries.
    """
    if store.execution_placement != "local":
        raise ValueError("Local driver requires a local-fenced workflow store")
    count = 0
    outcomes = []
    current = None
    resume_attempted = False
    resume_key = f"local-resume:{request.run_id}:{request.generation}"
    with lease:
        if abort.is_set():
            raise LostOwnership("Local coordinator lease is unavailable")
        validate_run(store, request)
        store.set_execution_placement(
            request.run_id,
            "bravotv",
            expected_generation=request.generation,
            expected_intent_hash=request.intent_hash,
            expected_placement=request.expected_placement,
            placement=request.placement,
            idempotency_key=f"local-placement:{request.run_id}:{request.generation}:{request.placement}",
        )
        if request.placement == "modal":
            return {"status": "placed", "execution_placement": "modal", "targets": 0}
        try:
            if abort.is_set():
                raise LostOwnership("Local coordinator lease was lost before resume")
            resume_attempted = True
            current = store.control(
                request.run_id,
                "bravotv",
                action="resume",
                idempotency_key=resume_key,
                expected_generation=request.generation,
                expected_placement="local",
            )
            end = clock() + request.max_seconds
            latest = None

            def dispatch(reservation):
                nonlocal count, latest
                if abort.is_set() or clock() >= end or count >= request.max_targets:
                    raise LostOwnership("Local execution bound reached")
                count += 1
                result = execute(
                    reservation, execution_placement="local", timeout_seconds=min(590, end - clock()), abort=abort
                )
                latest = (reservation, result)
                outcomes.append({key: result[key] for key in ("outcome", "stage", "code") if key in result})
                # Synchronous completion has no remote FunctionCall identity.
                return None

            scheduler = Scheduler(store, admit=admit, dispatch=dispatch, reconcile=lambda _: "unknown")
            item = store.seed(dict(current, account="bravotv"))
            ordinal = 0
            while not abort.is_set() and clock() < end and count < request.max_targets:
                latest = None
                decision = scheduler.tick(item, ordinal)
                if latest is not None and latest[1].get("outcome") in {"unresolved", "fenced"}:
                    # Child termination was proven by execute's reaper. No
                    # ambiguous remote call remains; preserve target cooldown.
                    store.transition(item, latest[0]["owner_token"], "expired")
                if decision.get("stop"):
                    store.complete_partition(item, continue_run=False, stop_state=decision["stop"])
                    break
                if decision.get("sleep"):
                    abort.wait(min(decision["sleep"], max(0, end - clock())))
                ordinal += 1
                if ordinal == PARTITION_SIZE:
                    successor = store.complete_partition(item)
                    item = dict(item, execution_id=successor, partition_number=item["partition_number"] + 1)
                    ordinal = 0
            return {
                "status": "lease_lost" if abort.is_set() else "bounded",
                "targets": count,
                "execution_placement": "local",
                "outcomes": outcomes,
            }
        finally:
            # Runtime has reaped the active child before control reaches here.
            # Never cancel a different operator's newer generation. Even if DB
            # loss prevents this fence, durable local placement forbids cloud
            # reservation, replay, dispatch and worker execution after lease loss.
            if current is None and resume_attempted:
                # Resolve an accepted resume whose response was lost. This uses
                # the same durable command lock/hash, before releasing the lease.
                current = store.command_result(
                    "bravotv",
                    resume_key,
                    fingerprint(
                        {
                            "command": "resume",
                            "run_id": request.run_id,
                            "selection": None,
                            "expected_generation": request.generation,
                            "expected_placement": "local",
                        }
                    ),
                )
            if current is not None:
                with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                    run = store._run(cur, request.run_id, "bravotv")
                if run["generation"] == current["generation"] and run["state"] != "completed":
                    store.control(
                        request.run_id,
                        "bravotv",
                        action="cancel",
                        idempotency_key=f"local-stop:{request.run_id}:{current['generation']}",
                        expected_generation=current["generation"],
                        expected_placement="local",
                    )


def _project_dsn(value, *, session_only=False):
    from urllib.parse import urlsplit

    parsed = urlsplit(value)
    project = "vwxfvzutyufrkhfgoeaa"
    direct = parsed.hostname == f"db.{project}.supabase.co"
    pool = (parsed.hostname or "").endswith(".pooler.supabase.com") and (parsed.username or "").endswith("." + project)
    return (
        parsed.scheme in {"postgres", "postgresql"}
        and (direct or pool)
        and (parsed.port in {None, 5432} if session_only else parsed.port in {None, 5432, 6543})
    )


def validate_configuration(coordinator_dsn):
    # Validate every possible business/read/probe fallback before any connection,
    # as well as the independent restricted-role coordinator session connection.
    from trr_backend.db.connection import (
        resolve_database_url_candidate_details,
        resolve_session_database_url_candidate_details,
    )

    if not _project_dsn(coordinator_dsn, session_only=True):
        raise Conflict("Explicit TRR session-preserving coordinator DSN required")
    ordinary = resolve_database_url_candidate_details()
    sessions = resolve_session_database_url_candidate_details()
    if not ordinary or not sessions or any(not _project_dsn(item["url"]) for item in (*ordinary, *sessions)):
        raise Conflict("Every explicit business database candidate must target the approved TRR project")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--intent-hash", required=True)
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--expected-placement", choices=("modal", "local"), required=True)
    parser.add_argument("--placement", choices=("local", "modal"), default="local")
    parser.add_argument("--max-targets", type=int, default=1)
    parser.add_argument("--max-seconds", type=float, default=60)
    parser.add_argument("--apply", action="store_true")
    options = parser.parse_args(argv)
    abort = threading.Event()
    previous = {}
    try:
        request = LocalRequest(**{key: getattr(options, key) for key in LocalRequest.__dataclass_fields__})
        if not options.apply:
            print(
                json.dumps(
                    {
                        "status": "plan",
                        "execution_placement": request.placement,
                        "max_targets": request.max_targets,
                        "max_seconds": request.max_seconds,
                    }
                )
            )
            return 0
        import psycopg

        from trr_backend.ingestion.admission import Admission
        from trr_backend.ingestion.composition import WORKFLOW_VERSION
        from trr_backend.ingestion.runtime import coordinator_enabled, run_ingestion_activity

        if not coordinator_enabled():
            raise Conflict("Explicit ingestion execution enablement is required")
        dsn = os.environ["TRR_INSTAGRAM_DBOS_DATABASE_URL"].strip()
        role = os.environ["TRR_INSTAGRAM_DBOS_RUNTIME_ROLE"].strip()
        validate_configuration(dsn)
        config = RuntimeConfig(dsn, WORKFLOW_VERSION, runtime_role=role)
        lease = CoordinatorLease(lambda: psycopg.connect(dsn, autocommit=True, connect_timeout=5), config, abort)
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, lambda *_: abort.set())
        result = run_local(
            request,
            store=WorkflowStore(execution_placement="local"),
            admit=Admission().admit,
            execute=run_ingestion_activity,
            lease=lease,
            abort=abort,
        )
        print(json.dumps(result))
        return 0 if result["status"] != "lease_lost" else 125
    except Exception as error:
        # No traceback/arguments/exception message may expose a DSN or token.
        print(json.dumps({"status": "failed", "failure_class": type(error).__name__}))
        return 1
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        if options.apply:
            from trr_backend.db import pg

            pg.close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
