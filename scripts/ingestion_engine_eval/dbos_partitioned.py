"""Finite 1000-target DBOS partition/outbox recovery proof; no production I/O."""

import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import store
from dbos import DBOS, SetWorkflowID

RUN_ID = "partitioned-1000"
TOTAL = 1000
PARTITION_SIZE = 50
VERSION = "partition-v1"


def workflow_id(partition):
    return f"{RUN_ID}:partition:{partition:03}"


@DBOS.step()
def dispatch(target, epoch):
    return store.perform(RUN_ID, target, epoch, "")


@DBOS.step()
def commit_continuation(partition, start, end, epoch):
    if partition == 0 and os.environ.get("EVAL_CANCEL_AT_CONTINUATION") == "1":
        # Test-only boundary: all target receipts exist, continuation has not
        # acquired its run lock. The coordinator cancels this business run.
        store.event(RUN_ID, "before_continuation_cancel")
        while not store.state(RUN_ID)[1]:
            time.sleep(0.03)
    with store.connection() as conn:
        owner, cancelled = conn.execute(
            "SELECT owner_epoch,cancelled FROM eval_runs WHERE run_id=%s FOR UPDATE", (RUN_ID,)
        ).fetchone()
        assert owner == epoch, "stale partition owner"
        if cancelled:
            # A completed target range is retained, but cancellation cannot
            # advance the business cursor, acknowledge intent or enqueue more.
            return None
        count = conn.execute(
            "SELECT count(*) FROM eval_receipts WHERE run_id=%s AND target>=%s AND target<%s",
            (RUN_ID, start, end),
        ).fetchone()[0]
        assert count == end - start, (partition, count)
        cursor = conn.execute("SELECT next_target FROM eval_partition_cursor WHERE run_id=%s", (RUN_ID,)).fetchone()[0]
        assert cursor == start or cursor >= end, (cursor, start, end)
        if cursor == start:
            conn.execute("UPDATE eval_partition_cursor SET next_target=%s WHERE run_id=%s", (end, RUN_ID))
        conn.execute(
            "UPDATE eval_partition_outbox SET completed=true WHERE run_id=%s AND partition=%s", (RUN_ID, partition)
        )
        if end < TOTAL:
            conn.execute(
                "INSERT INTO eval_partition_outbox(run_id,partition,first_target,last_target,workflow_id) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (RUN_ID, partition + 1, end, min(TOTAL, end + PARTITION_SIZE), workflow_id(partition + 1)),
            )
        # Cursor advancement, old completion, next dispatch intent and fault
        # checkpoint all commit together. There is no process-memory next link.
        if partition == 0:
            conn.execute(
                "INSERT INTO eval_events(run_id,stage) VALUES (%s,'continuation_committed') ON CONFLICT DO NOTHING",
                (RUN_ID,),
            )
    if partition == 0 and os.environ.get("EVAL_INJECT") == "1":
        while True:
            time.sleep(0.05)
    return end


@DBOS.workflow()
def partition_workflow(partition, start, end, epoch):
    assert 0 <= end - start <= PARTITION_SIZE
    for target in range(start, end):
        assert dispatch(target, epoch) == "committed"
    return commit_continuation(partition, start, end, epoch)


def next_partition_intent():
    with store.connection() as conn:
        return conn.execute(
            "SELECT o.partition,o.first_target,o.last_target,o.workflow_id FROM eval_partition_outbox o "
            "JOIN eval_runs r ON r.run_id=o.run_id "
            "WHERE o.run_id=%s AND NOT o.completed AND NOT r.cancelled ORDER BY o.partition LIMIT 1",
            (RUN_ID,),
        ).fetchone()


def worker(root, cancellation_probe=False):
    DBOS(
        config={
            "name": "trr-eval-partitioned",
            "application_version": VERSION,
            "executor_id": "partition-coordinator-slot-0",
            "system_database_url": os.environ["EVAL_DATABASE_URL"],
            "sys_db_pool_size": 2,
            "max_executor_threads": 2,
            "db_engine_kwargs": {"max_overflow": 0},
            "admin_server": False,
            "enable_otlp": False,
        }
    )
    DBOS.launch()
    try:
        while True:
            intent = next_partition_intent()
            if intent is None:
                break
            partition, start, end, identity = intent
            with SetWorkflowID(identity):
                handle = DBOS.start_workflow(partition_workflow, partition, start, end, 1)
            result = handle.get_result()
            if result is None:
                assert next_partition_intent() is None, "cancelled run remains dispatchable"
                break
            assert result == end
        if cancellation_probe:
            assert next_partition_intent() is None
            assert DBOS.retrieve_workflow(workflow_id(0)).get_result() is None
            print(json.dumps({"status": "CANCELLED_CONTINUATION_HELD", "next_intent": None}), flush=True)
            return
        histories = {}
        # Also wait for partition zero's startup recovery after it committed
        # the next intent but died before acknowledging its final step.
        for partition in range(TOTAL // PARTITION_SIZE):
            identity = workflow_id(partition)
            DBOS.retrieve_workflow(identity).get_result()
            histories[identity] = len(DBOS.list_workflow_steps(identity))
        before_duplicate = store.snapshot(RUN_ID)
        with SetWorkflowID(workflow_id(3)):
            DBOS.start_workflow(partition_workflow, 3, 150, 200, 1).get_result()
        assert store.snapshot(RUN_ID) == before_duplicate
        assert len(histories) == 20 and max(histories.values()) <= PARTITION_SIZE + 1, histories
        (root / "history.json").write_text(json.dumps(histories, indent=2) + "\n")
        print(json.dumps({"status": "PASS", "histories": histories, "outcomes": before_duplicate}), flush=True)
    finally:
        DBOS.destroy()


def main(root, cancellation_probe=False):
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
            f"-p {port} -h 127.0.0.1 -k '' -c shared_preload_libraries=pg_stat_statements",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    os.environ["EVAL_DATABASE_URL"] = f"postgresql://eval@127.0.0.1:{port}/postgres"
    children = []
    stop = threading.Event()
    samples = []

    def sample():
        with store.connection() as conn:
            conn.autocommit = True
            while not stop.wait(0.05):
                samples.append(
                    conn.execute(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname=current_database() AND pid<>pg_backend_pid()"
                    ).fetchone()[0]
                )

    def spawn(inject):
        with (root / ("first.log" if inject else "replacement.log")).open("w") as log:
            child = subprocess.Popen(
                [sys.executable, __file__, "--worker", "--runtime-dir", str(root)]
                + (["--cancel-continuation-only"] if cancellation_probe else []),
                env=dict(
                    os.environ,
                    EVAL_INJECT="1" if inject else "0",
                    EVAL_CANCEL_AT_CONTINUATION="1" if cancellation_probe else "0",
                ),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        children.append(child)
        return child

    observer = None
    start_time = time.monotonic()
    try:
        store.initialize()
        store.create_run(RUN_ID, TOTAL, version=VERSION)
        with store.connection() as conn:
            conn.execute("CREATE EXTENSION pg_stat_statements")
            conn.execute("""
                CREATE TABLE eval_partition_cursor(run_id text PRIMARY KEY, next_target int NOT NULL);
                CREATE TABLE eval_partition_outbox(run_id text, partition int, first_target int,
                  last_target int, workflow_id text UNIQUE, completed boolean NOT NULL DEFAULT false,
                  PRIMARY KEY(run_id,partition));
                """)
            conn.execute("INSERT INTO eval_partition_cursor VALUES (%s,0)", (RUN_ID,))
            conn.execute("INSERT INTO eval_partition_outbox VALUES (%s,0,0,50,%s,false)", (RUN_ID, workflow_id(0)))
            before_calls = conn.execute("SELECT coalesce(sum(calls),0) FROM pg_stat_statements").fetchone()[0]
        observer = threading.Thread(target=sample)
        observer.start()
        first = spawn(not cancellation_probe)
        deadline = time.monotonic() + 30
        while True:
            with store.connection() as conn:
                checkpoint = conn.execute(
                    "SELECT 1 FROM eval_events WHERE run_id=%s AND stage=%s",
                    (RUN_ID, "before_continuation_cancel" if cancellation_probe else "continuation_committed"),
                ).fetchone()
            if checkpoint:
                break
            assert first.poll() is None, (root / "first.log").read_text()
            assert time.monotonic() < deadline, "continuation checkpoint timeout"
            time.sleep(0.05)
        if cancellation_probe:
            boundary = store.snapshot(RUN_ID)
            assert boundary["receipts"] == PARTITION_SIZE, boundary
            with store.connection() as conn:
                conn.execute("UPDATE eval_runs SET cancelled=true WHERE run_id=%s", (RUN_ID,))
            assert first.wait(30) == 0, (root / "replacement.log").read_text()
            with store.connection() as conn:
                cursor = conn.execute("SELECT next_target FROM eval_partition_cursor").fetchone()[0]
                intents = conn.execute(
                    "SELECT partition,completed FROM eval_partition_outbox ORDER BY partition"
                ).fetchall()
            assert cursor == 0 and intents == [(0, False)], (cursor, intents)
            assert next_partition_intent() is None
            assert store.snapshot(RUN_ID) == boundary, "cancel changed committed target outcomes"
            result = {
                "status": "PASS",
                "case": "cancel_after_final_receipt_before_continuation",
                "outcomes_retained": boundary,
                "business_cursor": cursor,
                "outbox_intents": intents,
                "successor_created": False,
                "cancelled_dispatch_selection": None,
                "engine_continuation_result": None,
                "seconds": round(time.monotonic() - start_time, 3),
            }
            (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result), flush=True)
            return
        first.kill()
        first.wait()
        boundary = store.snapshot(RUN_ID)
        with store.connection() as conn:
            intent_at_crash = conn.execute(
                "SELECT partition,completed FROM eval_partition_outbox ORDER BY partition"
            ).fetchall()
            cursor_at_crash = conn.execute("SELECT next_target FROM eval_partition_cursor").fetchone()[0]
        assert boundary["receipts"] == 50 and cursor_at_crash == 50
        assert intent_at_crash == [(0, True), (1, False)], intent_at_crash
        replacement = spawn(False)
        assert replacement.wait(120) == 0, (root / "replacement.log").read_text()
        outcomes = store.snapshot(RUN_ID)
        assert outcomes == {"targets": TOTAL, "dispatches": TOTAL, "receipts": TOTAL, "attempts": TOTAL}, outcomes
        with store.connection() as conn:
            final_cursor = conn.execute("SELECT next_target FROM eval_partition_cursor").fetchone()[0]
            outbox = conn.execute("SELECT partition,completed FROM eval_partition_outbox ORDER BY partition").fetchall()
            assert final_cursor == TOTAL and outbox == [(p, True) for p in range(20)], outbox
            database_bytes = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
            relations = conn.execute(
                "SELECT n.nspname,sum(pg_total_relation_size(c.oid)) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','m') "
                "AND n.nspname IN ('public','dbos') GROUP BY n.nspname"
            ).fetchall()
            after_calls = conn.execute(
                "SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                "WHERE query NOT LIKE 'SELECT count(*) FROM pg_stat_activity%'"
            ).fetchone()[0]
        histories = json.loads((root / "history.json").read_text())
        result = {
            "status": "PASS",
            "engine": "DBOS 3.3.0",
            "pinned_application_version": VERSION,
            "business_run_id": RUN_ID,
            "targets_per_partition": PARTITION_SIZE,
            "partitions": len(histories),
            "max_history_steps_per_workflow": max(histories.values()),
            "total_history_steps": sum(histories.values()),
            "at_crash": boundary,
            "cursor_at_crash": cursor_at_crash,
            "continuation_intents_at_crash": intent_at_crash,
            "outcomes": outcomes,
            "final_cursor": final_cursor,
            "all_continuations_completed": True,
            "postgres_statement_calls": int(after_calls - before_calls),
            "postgres_statement_calls_scope": (
                "excludes pg_stat_activity session-sampler SELECTs only; "
                "includes checkpoint, snapshot, outbox and other observer queries"
            ),
            "database_bytes": database_bytes,
            "retained_relation_bytes_by_schema": {name: int(size) for name, size in relations},
            "sampled_peak_sessions_excluding_observer": max(samples),
            "seconds_including_forced_crash_and_recovery": round(time.monotonic() - start_time, 3),
        }
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
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
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--cancel-continuation-only", action="store_true")
    args = parser.parse_args()
    if args.worker:
        worker(args.runtime_dir, args.cancel_continuation_only)
    else:
        main(args.runtime_dir, args.cancel_continuation_only)
