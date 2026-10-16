"""Bounded real-engine evaluation. Run only in a dedicated Python 3.11 venv.

Creates a private local Postgres cluster and full Temporal dev server. Every
helper is stopped in finally; runtime logs/databases remain as recovery evidence.
"""

import argparse
import asyncio
import importlib.metadata
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import store
from temporalio.testing import WorkflowEnvironment

HERE = Path(__file__).resolve().parent


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Harness:
    def __init__(self, root):
        self.root = root
        self.children = []
        self.results = []
        self.sequence = 0

    def spawn(self, engine, action, run_id, count=3, fault="", inject=False, version="v1"):
        self.sequence += 1
        env = dict(os.environ, EVAL_INJECT="1" if inject else "0", PYTHONUNBUFFERED="1")
        log = self.root / f"{self.sequence:03}-{engine}-{action}-{run_id}.log"
        with log.open("w") as output:
            child = subprocess.Popen(
                [
                    sys.executable,
                    str(HERE / f"{engine}_worker.py"),
                    action,
                    run_id,
                    "--count",
                    str(count),
                    "--fault",
                    fault,
                    "--version",
                    version,
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
                env=env,
            )
        self.children.append(child)
        return child, log

    def finish(self, item, timeout=40, expect_success=True):
        child, log = item
        try:
            code = child.wait(timeout)
        except subprocess.TimeoutExpired as exc:
            child.kill()
            child.wait()
            raise AssertionError(f"timeout; evidence {log.name}") from exc
        if expect_success and code:
            raise AssertionError(f"exit {code}; evidence {log.name}: {log.read_text()[-1800:]}")
        return code

    def until_event(self, run_id, stage, child, timeout=35):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with store.connection() as conn:
                if conn.execute("SELECT 1 FROM eval_events WHERE run_id=%s AND stage=%s", (run_id, stage)).fetchone():
                    return
            if child.poll() is not None:
                raise AssertionError(f"worker exited before {stage}: {child.returncode}")
            time.sleep(0.05)
        raise AssertionError(f"did not reach {stage}")

    def check_balanced(self, run_id, count):
        result = store.snapshot(run_id)
        assert result["targets"] == result["dispatches"] == result["receipts"] == count, result
        return result

    def record(self, engine, case, func):
        start = time.monotonic()
        stop = threading.Event()
        samples = []

        def sample():
            with store.connection() as conn:
                conn.autocommit = True
                while not stop.is_set():
                    samples.append(
                        conn.execute(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE datname=current_database() AND pid<>pg_backend_pid()"
                        ).fetchone()[0]
                    )
                    stop.wait(0.1)

        with store.connection() as conn:
            before_calls = conn.execute(
                "SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                "WHERE query NOT LIKE 'SELECT count(*) FROM pg_stat_activity%' "
                "AND query NOT LIKE 'SELECT coalesce(sum(calls)%'"
            ).fetchone()[0]
        sampler = threading.Thread(target=sample)
        sampler.start()
        try:
            evidence = func()
            row = {"engine": engine, "case": case, "status": "PASS", "evidence": evidence}
        except Exception as exc:
            row = {"engine": engine, "case": case, "status": "FAIL", "error": str(exc)}
        finally:
            stop.set()
            sampler.join()
        with store.connection() as conn:
            after_calls = conn.execute(
                "SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                "WHERE query NOT LIKE 'SELECT count(*) FROM pg_stat_activity%' "
                "AND query NOT LIKE 'SELECT coalesce(sum(calls)%'"
            ).fetchone()[0]
        row["postgres_statement_calls"] = int(after_calls - before_calls)
        row["sampled_peak_postgres_sessions"] = max(samples, default=0)
        row["seconds"] = round(time.monotonic() - start, 3)
        self.results.append(row)
        (self.root / "results.json").write_text(json.dumps(self.results, indent=2))
        print(json.dumps(row), flush=True)

    def crash(self, engine, fault, count=3):
        run_id = f"{engine}-{fault}"
        store.create_run(run_id, count)
        first = self.spawn(engine, "start", run_id, count, fault, inject=True)
        self.until_event(run_id, fault, first[0])
        before = store.snapshot(run_id)
        first[0].kill()
        first[0].wait()
        self.finish(self.spawn(engine, "recover", run_id, count, fault))
        after = self.check_balanced(run_id, count)
        # A duplicate workflow ID must not dispatch another accepted fake job.
        self.finish(self.spawn(engine, "duplicate", run_id, count, fault))
        duplicate = self.check_balanced(run_id, count)
        assert duplicate == after, (duplicate, after)
        return {"at_sigkill": before, "recovered": after, "duplicate": duplicate}

    def fencing(self, engine):
        run_id = engine + "-fencing"
        store.create_run(run_id, 2)
        with store.connection() as conn:
            conn.execute("UPDATE eval_runs SET owner_epoch=2 WHERE run_id=%s", (run_id,))
        assert store.perform(run_id, 0, 1, "") == "fenced"
        assert store.snapshot(run_id)["dispatches"] == 0
        self.finish(self.spawn(engine, "start", run_id, 2))
        assert store.perform(run_id, 1, 1, "") == "fenced"
        return self.check_balanced(run_id, 2)

    def cooldown(self, engine):
        run_id = engine + "-cooldown"
        store.create_run(run_id, 3, cooldown=5)
        first = self.spawn(engine, "start", run_id)
        self.until_event(run_id, "timer_started", first[0])
        first[0].kill()
        first[0].wait()
        assert store.snapshot(run_id)["dispatches"] == 0
        restart = self.spawn(engine, "recover", run_id)
        time.sleep(0.35)
        assert store.snapshot(run_id)["dispatches"] == 0
        self.finish(restart)
        with store.connection() as conn:
            early = conn.execute(
                "SELECT count(*) FROM eval_dispatches d JOIN eval_runs r USING(run_id) "
                "WHERE d.run_id=%s AND d.accepted_at<r.cooldown_until",
                (run_id,),
            ).fetchone()[0]
        assert early == 0
        return self.check_balanced(run_id, 3)

    def cancellation(self, engine, partial=False):
        run_id = engine + ("-partial-cancel" if partial else "-cancel")
        store.create_run(run_id, 3, cooldown=5)
        if partial:
            # Existing business outcome and retry expenditure predate this
            # execution. Engine reset/resume must preserve both SQL facts.
            store.perform(run_id, 0, 1, "")
            store.perform(run_id, 0, 1, "")
            with store.connection() as conn:
                original_receipt = conn.execute(
                    "SELECT committed_at FROM eval_receipts WHERE run_id=%s AND target=0", (run_id,)
                ).fetchone()[0]
        first = self.spawn(engine, "start", run_id)
        self.until_event(run_id, "timer_started", first[0])
        with store.connection() as conn:
            conn.execute("UPDATE eval_runs SET cancelled=true WHERE run_id=%s", (run_id,))
        self.finish(self.spawn(engine, "cancel", run_id))
        self.finish(first, expect_success=False)
        cancellation_state = self.spawn(engine, "assert-cancelled", run_id)
        self.finish(cancellation_state)
        state_rows = [
            json.loads(line)
            for line in cancellation_state[1].read_text().splitlines()
            if line.startswith('{"terminal_engine_status":')
        ]
        assert len(state_rows) == 1, cancellation_state[1].read_text()
        assert store.snapshot(run_id)["dispatches"] == (1 if partial else 0)
        with store.connection() as conn:
            conn.execute("UPDATE eval_runs SET cancelled=false WHERE run_id=%s", (run_id,))
        self.finish(self.spawn(engine, "resume", run_id))
        if partial:
            with store.connection() as conn:
                preserved_receipt = conn.execute(
                    "SELECT committed_at FROM eval_receipts WHERE run_id=%s AND target=0", (run_id,)
                ).fetchone()[0]
                attempts = conn.execute(
                    "SELECT attempts FROM eval_targets WHERE run_id=%s AND target=0", (run_id,)
                ).fetchone()[0]
            assert original_receipt == preserved_receipt
            assert attempts == 3, attempts
        return {
            "mode": "native_resume" if engine == "dbos" else "history_reset_new_run",
            "terminal_engine_status_before_resume": state_rows[0]["terminal_engine_status"],
            **self.check_balanced(run_id, 3),
        }

    def version(self, engine):
        run_id = engine + "-version"
        store.create_run(run_id, 3)
        first = self.spawn(engine, "start", run_id, fault="before_dispatch", inject=True)
        self.until_event(run_id, "before_dispatch", first[0])
        first[0].kill()
        first[0].wait()
        self.finish(self.spawn(engine, "hold", run_id, version="v2"))
        assert store.snapshot(run_id)["dispatches"] == 0
        self.finish(self.spawn(engine, "recover", run_id))
        return {
            "guard": "DBOS application_version"
            if engine == "dbos"
            else "explicit version-specific task queue; deployment pinning unverified",
            **self.check_balanced(run_id, 3),
        }

    def fixture(self, engine, count):
        run_id = engine + "-bounded"
        store.create_run(run_id, count)
        item = self.spawn(engine, "start", run_id, count)
        self.finish(item, timeout=120)
        return {
            "max_active_adapter_threads": 2 if engine == "temporal" else 1,
            "workflow_payload": "run id and target count; no media/comment body",
            **self.check_balanced(run_id, count),
        }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", type=Path)
    parser.add_argument("--pg-bin", type=Path, default=Path("/opt/homebrew/opt/postgresql@17/bin"))
    parser.add_argument("--targets", type=int, default=120)
    parser.add_argument(
        "--bounded-only", action="store_true", help="Run only the changed-size fixture; reuse other passing evidence"
    )
    parser.add_argument("--partial-cancel-only", action="store_true")
    parser.add_argument("--cancel-only", action="store_true", help="Only cancellation checks affected by review")
    args = parser.parse_args()
    if not 1 <= args.targets <= 1000:
        parser.error("--targets must be between 1 and 1000")
    root = args.runtime_dir or Path(tempfile.mkdtemp(prefix="trr-engine-eval-"))
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if any(root.iterdir()):
        parser.error("runtime directory must be empty; preserve earlier evidence")
    harness = Harness(root)
    pg_data = root / "postgres"
    pg_port = free_port()
    env = None
    pg_started = False
    versions = {name: importlib.metadata.version(name) for name in ("dbos", "temporalio", "psycopg")}
    versions.update(
        python=platform.python_version(),
        postgres=subprocess.check_output([str(args.pg_bin / "postgres"), "--version"], text=True).strip(),
    )
    try:
        subprocess.run(
            [
                str(args.pg_bin / "initdb"),
                "-D",
                str(pg_data),
                "-A",
                "trust",
                "-U",
                "eval",
                "--no-locale",
                "--encoding=UTF8",
            ],
            check=True,
            stdout=(root / "initdb.log").open("w"),
        )
        subprocess.run(
            [
                str(args.pg_bin / "pg_ctl"),
                "-D",
                str(pg_data),
                "-l",
                str(root / "postgres.log"),
                "-o",
                f"-p {pg_port} -h 127.0.0.1 -k '' -c shared_preload_libraries=pg_stat_statements",
                "-w",
                "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        pg_started = True
        os.environ["EVAL_DATABASE_URL"] = f"postgresql://eval@127.0.0.1:{pg_port}/postgres"
        os.environ["EVAL_DBOS_URL"] = os.environ["EVAL_DATABASE_URL"]
        with store.connection() as conn:
            conn.execute("CREATE EXTENSION pg_stat_statements")
        store.initialize()
        (root / "downloads").mkdir()
        env = await WorkflowEnvironment.start_local(
            download_dest_dir=str(root / "downloads"), dev_server_database_filename=str(root / "temporal.db"), ui=False
        )
        os.environ["EVAL_TEMPORAL_ADDRESS"] = env.client.service_client.config.target_host
        # Keep the exact binary fingerprint/version alongside result evidence.
        binaries = list((root / "downloads").iterdir())
        versions["temporal_binary"] = [p.name for p in binaries]
        for binary in binaries:
            if binary.is_file() and os.access(binary, os.X_OK):
                versions["temporal_server"] = subprocess.check_output([str(binary), "--version"], text=True).strip()
        (root / "versions.json").write_text(json.dumps(versions, indent=2))
        print(json.dumps({"runtime_dir": str(root), "versions": versions}), flush=True)
        for engine in ("dbos", "temporal"):
            if args.cancel_only:
                harness.record(engine, "cancel_resume", lambda e=engine: harness.cancellation(e))
                harness.record(
                    engine,
                    "partial_cancel_preserves_outcomes_and_attempts",
                    lambda e=engine: harness.cancellation(e, partial=True),
                )
                continue
            if args.partial_cancel_only:
                harness.record(
                    engine,
                    "partial_cancel_preserves_outcomes_and_attempts",
                    lambda e=engine: harness.cancellation(e, partial=True),
                )
                continue
            if not args.bounded_only:
                for fault in ("before_dispatch", "after_dispatch", "after_commit"):
                    harness.record(
                        engine, fault + "_sigkill_and_duplicate", lambda e=engine, f=fault: harness.crash(e, f)
                    )
                harness.record(engine, "stale_owner_fence", lambda e=engine: harness.fencing(e))
                harness.record(engine, "durable_cooldown_restart", lambda e=engine: harness.cooldown(e))
                harness.record(engine, "cancel_resume", lambda e=engine: harness.cancellation(e))
                harness.record(engine, "version_recovery", lambda e=engine: harness.version(e))
            harness.record(engine, "bounded_fixture", lambda e=engine: harness.fixture(e, args.targets))
        if not args.cancel_only:
            # Restart the actual Temporal service after both workers have exited.
            await env.shutdown()
            env = await WorkflowEnvironment.start_local(
                download_dest_dir=str(root / "downloads"),
                dev_server_database_filename=str(root / "temporal.db"),
                ui=False,
            )
            os.environ["EVAL_TEMPORAL_ADDRESS"] = env.client.service_client.config.target_host
            recovered_id = (
                "temporal-partial-cancel"
                if args.partial_cancel_only
                else "temporal-bounded"
                if args.bounded_only
                else "temporal-after_commit"
            )
            harness.record(
                "temporal",
                "server_restart_history",
                lambda: {"exit": harness.finish(harness.spawn("temporal", "recover", recovered_id))},
            )
    finally:
        for child in harness.children:
            if child.poll() is None:
                child.kill()
                child.wait()
        if env:
            await env.shutdown()
        if pg_started:
            subprocess.run(
                [str(args.pg_bin / "pg_ctl"), "-D", str(pg_data), "-m", "fast", "-w", "stop"],
                check=True,
                stdout=subprocess.DEVNULL,
            )
        (root / "cleanup.json").write_text(
            json.dumps(
                {
                    "children_stopped": all(p.poll() is not None for p in harness.children),
                    "postgres_stopped": not (pg_data / "postmaster.pid").exists(),
                    "temporal_shutdown_called": env is not None,
                }
            )
        )
    if any(row["status"] != "PASS" for row in harness.results):
        raise SystemExit(1)


if __name__ == "__main__":
    # asyncio's default SIGINT schedules cancellation, which cannot run during
    # bounded synchronous subprocess waits. Raise immediately for cleanup.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    asyncio.run(main())
