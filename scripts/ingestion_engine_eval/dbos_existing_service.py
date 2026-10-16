"""Local bounded-service proof; does not invoke Modal or Supabase.

A session advisory lock is acquired BEFORE DBOS launch and retained until its
threads stop or the process dies. Competing wakeups return busy. The lock is a
singleton coordinator guard, not a replacement for business-generation fences.
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import psycopg
import store

LOCK_NAMESPACE = 48112
LOCK_KEY = 20261016
EXECUTOR_ID = "instagram-coordinator-slot-0"
POOL_SIZE = 1
EXECUTOR_THREADS = 2


def worker(args):
    # A separate session is necessary: transaction pooling cannot preserve this
    # lifetime lock. A lost lease session must stop the process, not reconnect.
    lease = psycopg.connect(os.environ["EVAL_DATABASE_URL"], autocommit=True, application_name="eval-lease")
    locked = lease.execute("SELECT pg_try_advisory_lock(%s,%s)", (LOCK_NAMESPACE, LOCK_KEY)).fetchone()[0]
    if not locked:
        print(json.dumps({"status": "BUSY", "dbos_launched": False}), flush=True)
        lease.close()
        return 17

    def timeout():
        print(json.dumps({"status": "BOUNDED_TIMEOUT", "seconds": args.lifetime}), flush=True)
        os._exit(124)

    deadline = threading.Timer(args.lifetime, timeout)
    deadline.daemon = True
    deadline.start()
    stop = threading.Event()

    def check_lease():
        while not stop.wait(0.1):
            try:
                lease.execute("SELECT 1")
            except Exception:
                # Never continue/reconnect under a released ownership lock.
                os._exit(125)

    monitor = threading.Thread(target=check_lease, daemon=True, name="eval-lease-monitor")
    monitor.start()
    from dbos import DBOS, SetWorkflowID
    from dbos_worker import ingest

    runtime = DBOS(
        config={
            "name": "trr-eval-existing-service",
            "application_version": "frozen-v1",
            "executor_id": EXECUTOR_ID,
            "system_database_url": os.environ["EVAL_DATABASE_URL"],
            "sys_db_pool_size": POOL_SIZE,
            "sys_db_polling_concurrency": 1,
            "max_executor_threads": EXECUTOR_THREADS,
            "run_migrations": False,
            "db_engine_kwargs": {
                "max_overflow": 0,
                "pool_timeout": 5,
                "connect_args": {"application_name": "eval-dbos", "connect_timeout": 3},
            },
            "admin_server": False,
            "enable_otlp": False,
        }
    )
    DBOS.launch()
    try:
        store.event(args.run_id, "bounded_service_launched")
        if args.worker == "start":
            with SetWorkflowID(args.run_id):
                handle = DBOS.start_workflow(ingest, args.run_id, 3, args.fault)
        else:
            # DBOS.launch recovers pending work for this stable executor/version.
            handle = DBOS.retrieve_workflow(args.run_id)
        result = handle.get_result()
        print(
            json.dumps(
                {
                    "status": "COMPLETED",
                    "result": result,
                    "executor_id": EXECUTOR_ID,
                    "configured_system_pool": POOL_SIZE,
                    "configured_executor_threads": EXECUTOR_THREADS,
                    "actual_executor_thread_limit": runtime._executor._max_workers,
                }
            ),
            flush=True,
        )
    finally:
        # These workflows finished before cleanup. Public destroy does not join
        # active executor threads; production must retain its lease to process
        # exit rather than treating this prototype ordering as that guarantee.
        DBOS.destroy()
        stop.set()
        monitor.join(timeout=2)
        deadline.cancel()
        lease.close()
    return 0


def wait_event(run_id, stage, child):
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        with store.connection() as conn:
            if conn.execute("SELECT 1 FROM eval_events WHERE run_id=%s AND stage=%s", (run_id, stage)).fetchone():
                return
        assert child.poll() is None, f"worker exited before checkpoint {stage}: {child.returncode}"
        time.sleep(0.03)
    raise AssertionError(f"checkpoint not reached: {stage}")


def main(args):
    root = args.runtime_dir
    root.mkdir(parents=True, exist_ok=False)
    pg_bin = Path("/opt/homebrew/opt/postgresql@17/bin")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "postgres"
    subprocess.run(
        [str(pg_bin / "initdb"), "-D", str(data), "-A", "trust", "-U", "eval", "--no-locale", "--encoding=UTF8"],
        check=True,
        stdout=(root / "initdb.log").open("w"),
    )
    subprocess.run(
        [
            str(pg_bin / "pg_ctl"),
            "-D",
            str(data),
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-p {port} -h 127.0.0.1 -k ''",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    os.environ["EVAL_DATABASE_URL"] = f"postgresql://eval@127.0.0.1:{port}/postgres?application_name=eval-adapter"
    children = []
    stop = threading.Event()
    samples = []
    rows = []

    def sample():
        with psycopg.connect(
            os.environ["EVAL_DATABASE_URL"], autocommit=True, application_name="eval-observer"
        ) as conn:
            while not stop.wait(0.02):
                grouped = conn.execute(
                    "SELECT application_name,count(*) FROM pg_stat_activity "
                    "WHERE datname=current_database() AND pid<>pg_backend_pid() GROUP BY application_name"
                ).fetchall()
                holders = conn.execute(
                    "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=%s AND objid=%s AND granted",
                    (LOCK_NAMESPACE, LOCK_KEY),
                ).fetchone()[0]
                samples.append({"sessions": dict(grouped), "effective_coordinators": holders})

    def spawn(action, run_id, lifetime, fault="", inject=False):
        log = root / f"{len(children):02}-{run_id}-{action}.log"
        with log.open("w") as out:
            child = subprocess.Popen(
                [
                    sys.executable,
                    __file__,
                    "--worker",
                    action,
                    "--run-id",
                    run_id,
                    "--lifetime",
                    str(lifetime),
                    "--fault",
                    fault,
                ],
                env=dict(os.environ, EVAL_INJECT="1" if inject else "0"),
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        children.append(child)
        return child, log

    observer = None
    try:
        store.initialize()
        # Supported DBOS bootstrap uses a separate pool of two connections.
        # Runtime is a non-owner role with schema USAGE and object grants.
        with store.connection() as conn:
            conn.execute("CREATE ROLE eval_runtime LOGIN")
            conn.execute("GRANT USAGE ON SCHEMA public TO eval_runtime")
            conn.execute("GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO eval_runtime")
        from dbos import DBOS

        DBOS.migrate(os.environ["EVAL_DATABASE_URL"], application_role="eval_runtime")
        runtime_url = os.environ["EVAL_DATABASE_URL"].replace("eval@", "eval_runtime@")
        os.environ["EVAL_DATABASE_URL"] = runtime_url
        with store.connection() as conn:
            privileges = conn.execute(
                "SELECT current_user,rolsuper,has_schema_privilege(current_user,'dbos','USAGE'), "
                "has_schema_privilege(current_user,'dbos','CREATE') FROM pg_roles WHERE rolname=current_user"
            ).fetchone()
        assert privileges == ("eval_runtime", False, True, False), privileges
        observer = threading.Thread(target=sample, name="eval-resource-observer")
        observer.start()
        for case in ("cooldown_timeout", "after_receipt_timeout"):
            start = time.monotonic()
            run_id = "bounded-" + case
            store.create_run(run_id, 3, cooldown=8 if case == "cooldown_timeout" else 0)
            first, first_log = spawn(
                "start",
                run_id,
                4,
                "after_commit" if case == "after_receipt_timeout" else "",
                inject=case == "after_receipt_timeout",
            )
            wait_event(run_id, "timer_started" if case == "cooldown_timeout" else "after_commit", first)
            before = store.snapshot(run_id)
            rival, rival_log = spawn("recover", run_id, 20)
            assert rival.wait(10) == 17, rival_log.read_text()
            assert '"dbos_launched": false' in rival_log.read_text()
            assert first.wait(10) == 124, first_log.read_text()
            replacement, replacement_log = spawn("recover", run_id, 20)
            assert replacement.wait(25) == 0, replacement_log.read_text()
            after = store.snapshot(run_id)
            assert after["targets"] == after["dispatches"] == after["receipts"] == 3, after
            assert '"actual_executor_thread_limit": 2' in replacement_log.read_text()
            rows.append(
                {
                    "case": case,
                    "status": "PASS",
                    "at_timeout_boundary": before,
                    "after_replacement": after,
                    "rival_exit": 17,
                    "timeout_exit": 124,
                    "seconds": round(time.monotonic() - start, 3),
                }
            )
        assert samples and max(s["effective_coordinators"] for s in samples) == 1
        peak = max(sum(s["sessions"].values()) for s in samples)
        per_class = {
            name: max(s["sessions"].get(name, 0) for s in samples)
            for name in {name for s in samples for name in s["sessions"]}
        }
        # One pooled metadata session plus one detached LISTEN session.
        assert per_class.get("eval-dbos", 0) <= 2, per_class
        result = {
            "status": "PASS",
            "dbos": "3.3.0",
            "system_pool_size": POOL_SIZE,
            "max_overflow": 0,
            "executor_thread_limit": EXECUTOR_THREADS,
            "stable_executor_id": EXECUTOR_ID,
            "sample_interval_ms": 20,
            "sampled_peak_sessions_excluding_observer": peak,
            "sampled_peak_sessions_by_class": per_class,
            "max_effective_coordinators": 1,
            "local_runtime_role": {
                "name": privileges[0],
                "superuser": privileges[1],
                "schema_usage": privileges[2],
                "schema_create": privileges[3],
            },
            "bootstrap": "DBOS.migrate with separate privileged role and two-connection pool",
            "cases": rows,
        }
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        (root / "samples.json").write_text(json.dumps(samples) + "\n")
        print(json.dumps(result), flush=True)
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()
        stop.set()
        if observer:
            observer.join(timeout=5)
        subprocess.run(
            [str(pg_bin / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
        )
        (root / "cleanup.json").write_text(
            json.dumps(
                {
                    "children_stopped": all(p.poll() is not None for p in children),
                    "postgres_stopped": not (data / "postmaster.pid").exists(),
                }
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", type=Path)
    parser.add_argument("--worker", choices=["start", "recover"])
    parser.add_argument("--run-id")
    parser.add_argument("--lifetime", type=float, default=20)
    parser.add_argument("--fault", default="")
    args = parser.parse_args()
    if args.worker:
        raise SystemExit(worker(args))
    if not args.runtime_dir:
        parser.error("--runtime-dir is required for coordinator")
    main(args)
