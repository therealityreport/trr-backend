"""Isolated real PostgreSQL and optional real DBOS; no configured TRR services."""

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest

from tests.ingestion.test_persistence import database as database_fixture
from tests.ingestion.test_persistence import launch
from trr_backend.ingestion.domain import Conflict, Failure, LostOwnership, Stage
from trr_backend.ingestion.workflow_store import PARTITION_SIZE, WorkflowStore
from trr_backend.ingestion.workflows import ControlPlaneError, DBOSAdapter, RuntimeConfig, Scheduler

base_database = database_fixture


@pytest.fixture(scope="module")
def database(base_database):
    _, tx = base_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            (
                Path(__file__).resolve().parents[2]
                / "supabase/migrations/20261016070100_instagram_ingestion_workflows.sql"
            ).read_text()
        )
    return WorkflowStore(tx), tx


def run_fixture(database, count=1, *, sealed=True):
    store, tx = database
    run = launch(store)
    with tx() as conn, conn.cursor() as cur:
        # This harness supplies a settled listing; parent tests listing transport.
        cur.execute("delete from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],))
        cur.execute(
            """update social.instagram_ingestion_manifests set discovery_complete=true,
            sealed_at=clock_timestamp() where run_id=%s and stage<>'post_details' """,
            (run["run_id"],),
        )
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=Stage.DETAILS,
        target_keys=[f"post{i:04}" for i in range(count)],
        max_attempts=3,
        cursor_state={},
        expected_cursor_state={},
        page_id="fixture",
        seal=sealed,
    )
    return dict(run, account="bravotv"), store.seed(dict(run, account="bravotv"))


def scheduler(database, *, admit=lambda _: True, dispatch=None, reconcile=lambda _: "unknown", clock=None):
    store, _ = database

    def settle(reservation):
        args = {
            key: reservation[key] for key in ("run_id", "account", "generation", "stage", "target_key", "owner_token")
        }
        args["stage"] = Stage(args["stage"])
        store.acquire(**args)
        store.commit_target(**args, observed_at=datetime.now(UTC), write_canonical=lambda _: {"id": args["target_key"]})
        return args["owner_token"]

    kwargs = {"clock": clock} if clock else {}
    return Scheduler(store, admit=admit, dispatch=dispatch or settle, reconcile=reconcile, **kwargs)


def sql(database, statement, args=()):
    with database[1]() as conn, conn.cursor() as cur:
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def test_partition_identity_and_atomic_generation_fenced_continuation(database):
    store, _ = database
    run, part = run_fixture(database, 0)
    with ThreadPoolExecutor(max_workers=8) as pool:
        seeds = list(pool.map(lambda _: store.seed(run), range(16)))
    assert {p["execution_id"] for p in seeds} == {part["execution_id"]}
    with pytest.raises(Conflict, match="unfinished"):
        store.complete_partition(part)
    # Fill fifty durable decisions (the workflow normally handles terminal first).
    for ordinal in range(PARTITION_SIZE):
        assert store.decision(part, ordinal, datetime.now(UTC))["kind"] == "complete"
    with ThreadPoolExecutor(max_workers=8) as pool:
        successors = list(pool.map(lambda _: store.complete_partition(part), range(16)))
    assert len(set(successors)) == 1
    assert sql(
        database,
        "select partition_number from social.instagram_ingestion_partitions where run_id=%s order by 1",
        (run["run_id"],),
    ) == [(0,), (1,)]
    store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    nextpart = dict(part, execution_id=successors[0], partition_number=1)
    with pytest.raises(LostOwnership):
        store.complete_partition(nextpart)
    resumed = store.control(run["run_id"], "bravotv", action="resume", idempotency_key=str(uuid4()))
    assert resumed["workflow_version"] == run["workflow_version"]
    current = store.seed(dict(resumed, account="bravotv"))
    assert current["generation"] == 3 and current["execution_id"] != part["execution_id"]
    assert sql(
        database,
        "select completed_at,next_execution_id from social.instagram_ingestion_partitions where execution_id=%s",
        (nextpart["execution_id"],),
    ) == [(None, None)]


def test_capacity_sealed_phase_order_and_no_get_mutations(database):
    store, _ = database
    run, part = run_fixture(database, sealed=False)
    assert store.decision(part, 0, datetime.now(UTC))["kind"] == "wait"
    sql(
        database,
        """update social.instagram_ingestion_manifests set discovery_complete=true,sealed_at=clock_timestamp()
        where run_id=%s and stage='post_details'""",
        (run["run_id"],),
    )
    denied = scheduler(database, admit=lambda _: False)
    assert denied.tick(part, 1) == {"sleep": 5.0}
    assert sql(
        database, "select count(*) from social.instagram_ingestion_dispatches where run_id=%s", (run["run_id"],)
    ) == [(0,)]
    assert sql(
        database,
        "select attempts,owner_token from social.instagram_ingestion_targets where run_id=%s",
        (run["run_id"],),
    ) == [(0, None)]
    assert scheduler(database).tick(part, 2) == {"ready": True}
    assert scheduler(database).tick(part, 3) == {"stop": "completed"}


def test_ambiguous_dispatch_reconciles_receipt_and_does_not_duplicate(database):
    store, _ = database
    run, part = run_fixture(database)
    accepted = []
    real = scheduler(database)

    def after_commit(item):
        accepted.append(item["owner_token"])
        real.dispatch(item)
        raise RuntimeError("lost response after committed receipt")

    worker = scheduler(database, dispatch=after_commit)
    assert worker.tick(part, 0) == {"sleep": 1.0}
    # Same interrupted DBOS step: persisted decision and receipt win.
    assert worker.tick(part, 0) == {"ready": True}
    assert worker.tick(part, 1) == {"stop": "completed"}
    assert len(accepted) == 1
    assert sql(
        database, "select attempts from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],)
    ) == [(1,)]


def test_unknown_send_holds_live_lease_then_expires_without_spending_attempt(database):
    store, _ = database
    run, part = run_fixture(database)
    calls = []

    def ambiguous(item):
        calls.append(item["owner_token"])
        raise RuntimeError("transport status unknown")

    worker = scheduler(database, dispatch=ambiguous)
    worker.tick(part, 0)
    assert worker.tick(part, 1) == {"sleep": 1.0}
    assert len(calls) == 1
    sql(
        database,
        """update social.instagram_ingestion_targets set owner_expires_at=clock_timestamp()-interval '1 second'
        where run_id=%s""",
        (run["run_id"],),
    )
    assert worker.tick(part, 2) == {"sleep": 0.01}
    scheduler(database).tick(part, 3)
    assert sql(
        database, "select attempts,outcome from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],)
    ) == [(1, "committed")]
    assert sql(
        database,
        "select state from social.instagram_ingestion_dispatches where run_id=%s order by created_at",
        (run["run_id"],),
    ) == [("expired",), ("accepted",)]


def test_explicit_absence_resends_same_token_and_duplicate_acquire_fails(database):
    store, _ = database
    _, part = run_fixture(database)
    calls = []
    settle = scheduler(database).dispatch

    def dispatch(item):
        calls.append(item["owner_token"])
        if len(calls) == 1:
            raise RuntimeError("lost before acceptance")
        return settle(item)

    worker = scheduler(database, dispatch=dispatch, reconcile=lambda _: "absent")
    worker.tick(part, 0)
    worker.tick(part, 1)
    assert len(calls) == 2 and len(set(calls)) == 1
    reservation = store.reservation(part, calls[0])
    with pytest.raises(LostOwnership):
        store.acquire(
            **dict(
                {k: reservation[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")},
                stage=Stage(reservation["stage"]),
            )
        )


def test_native_cooldown_and_exhausted_phase_pause_preserve_budget(database):
    _, part = run_fixture(database)
    now = datetime.now(UTC)
    sql(
        database,
        "update social.instagram_ingestion_targets set next_attempt_at=%s where run_id=%s",
        (now + timedelta(seconds=20), part["run_id"]),
    )
    worker = scheduler(database, clock=lambda: now)
    assert worker.tick(part, 0) == {"sleep": 20.0}
    sql(
        database,
        "update social.instagram_ingestion_targets set attempts=max_attempts where run_id=%s",
        (part["run_id"],),
    )
    assert worker.tick(part, 1) == {"stop": "blocked"}
    database[0].complete_partition(part, continue_run=False, stop_state="blocked")
    assert sql(database, "select state from social.instagram_ingestion_runs where run_id=%s", (part["run_id"],)) == [
        ("blocked",)
    ]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_partitions where run_id=%s", (part["run_id"],)
    ) == [(1,)]


def test_concurrent_replayed_decision_acquires_and_dispatches_once(database):
    store, _ = database
    run, part = run_fixture(database)
    accepted = []

    def dispatch(reservation):
        args = {k: reservation[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")}
        store.acquire(**args, stage=Stage(reservation["stage"]))
        accepted.append(reservation["owner_token"])
        return reservation["owner_token"]

    worker = scheduler(database, dispatch=dispatch)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: worker.tick(part, 0), range(16)))
    assert len(results) == 16 and len(accepted) == 1
    assert sql(
        database, "select attempts from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],)
    ) == [(1,)]
    with pytest.raises(ValueError, match="bound"):
        store.decision(part, 50, datetime.now(UTC))
    other, _ = run_fixture(database)
    with pytest.raises(LostOwnership, match="different"):
        store.decision(dict(part, run_id=other["run_id"]), 1, datetime.now(UTC))


def test_selected_retry_preserves_fences_and_stops_before_unrelated_work(database):
    store, _ = database
    run, part = run_fixture(database, 2)
    # An earlier incomplete listing cannot expand explicit retry selection.
    sql(
        database,
        """update social.instagram_ingestion_manifests
        set discovery_complete=false,sealed_at=null where run_id=%s and stage='listing'""",
        (run["run_id"],),
    )
    sql(
        database,
        """update social.instagram_ingestion_runs set execution_selection=%s::jsonb where run_id=%s""",
        ('{"stage":"post_details","target_keys":["post0001"]}', run["run_id"]),
    )
    now = datetime.now(UTC)
    sql(
        database,
        """update social.instagram_ingestion_targets set attempts=1,next_attempt_at=%s,
        checkpoint='{"cursor":"retained"}'::jsonb,blocked_reason='IDENTITY_BLOCKED'
        where run_id=%s and target_key='post0001'""",
        (now + timedelta(seconds=20), run["run_id"]),
    )
    worker = scheduler(database, clock=lambda: now)
    assert worker.tick(part, 0) == {"stop": "blocked"}
    assert sql(
        database,
        """select attempts,next_attempt_at,checkpoint,blocked_reason,owner_token
        from social.instagram_ingestion_targets where run_id=%s and target_key='post0001'""",
        (run["run_id"],),
    ) == [(1, now + timedelta(seconds=20), {"cursor": "retained"}, "IDENTITY_BLOCKED", None)]
    # The explicit operator repair clears identity block; scheduler never does.
    sql(
        database,
        """update social.instagram_ingestion_targets set blocked_reason=null
        where run_id=%s and target_key='post0001'""",
        (run["run_id"],),
    )
    assert worker.tick(part, 1) == {"sleep": 20.0}
    sql(
        database,
        """update social.instagram_ingestion_targets set next_attempt_at=%s
        where run_id=%s and target_key='post0001'""",
        (now, run["run_id"]),
    )
    assert worker.tick(part, 2) == {"ready": True}
    assert worker.tick(part, 3) == {"stop": "blocked"}
    store.complete_partition(part, continue_run=False, stop_state="blocked")
    assert sql(
        database,
        """select target_key,attempts,outcome from social.instagram_ingestion_targets
        where run_id=%s order by target_key""",
        (run["run_id"],),
    ) == [("post0000", 0, "unresolved"), ("post0001", 2, "committed")]
    assert sql(database, "select state from social.instagram_ingestion_runs where run_id=%s", (run["run_id"],)) == [
        ("blocked",)
    ]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_partitions where run_id=%s", (run["run_id"],)
    ) == [(1,)]

    complete_run, complete_part = run_fixture(database)
    sql(
        database,
        """update social.instagram_ingestion_runs set execution_selection=%s::jsonb where run_id=%s""",
        ('{"stage":"post_details","target_keys":["post0000"]}', complete_run["run_id"]),
    )
    complete_worker = scheduler(database)
    assert complete_worker.tick(complete_part, 0) == {"ready": True}
    assert complete_worker.tick(complete_part, 1) == {"stop": "completed"}
    store.complete_partition(complete_part, continue_run=False, stop_state="completed")
    assert sql(
        database, "select state from social.instagram_ingestion_runs where run_id=%s", (complete_run["run_id"],)
    ) == [("completed",)]


def test_target_selection_materializes_bounded_rows_without_checkpoints(database):
    store, tx = database
    run, part = run_fixture(database, 1000)
    sql(
        database,
        """update social.instagram_ingestion_targets
        set checkpoint=jsonb_build_object('large_cursor',repeat('x',8192)) where run_id=%s""",
        (run["run_id"],),
    )
    rows = []

    class Cursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def __getattr__(self, key):
            return getattr(self.cursor, key)

        def __enter__(self):
            self.cursor.__enter__()
            return self

        def __exit__(self, *args):
            return self.cursor.__exit__(*args)

        def fetchone(self):
            row = self.cursor.fetchone()
            if row is not None:
                rows.append(dict(row))
            return row

        def fetchall(self):
            result = self.cursor.fetchall()
            rows.extend(dict(row) for row in result)
            return result

    class Connection:
        def __init__(self, connection):
            self.connection = connection

        def cursor(self, *args, **kwargs):
            return Cursor(self.connection.cursor(*args, **kwargs))

    @contextmanager
    def recorded():
        with tx() as conn:
            yield Connection(conn)

    decision = WorkflowStore(recorded).decision(part, 0, datetime.now(UTC))
    assert decision == {"kind": "target", "stage": "post_details", "target_key": "post0000"}
    assert len(rows) <= 6
    assert all("checkpoint" not in row for row in rows)


def test_control_retry_keeps_exact_decision_reservation_after_worker_release(database, monkeypatch):
    store, _ = database
    run, part = run_fixture(database)
    accepted = []

    def failed_activity(reservation):
        args = {k: reservation[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")}
        args["stage"] = Stage(reservation["stage"])
        store.acquire(**args)
        store.checkpoint_failure(**args, failure=Failure("SOURCE_RETRY"), checkpoint={}, requests=0)
        accepted.append(reservation["owner_token"])
        return "accepted"

    original = store.transition

    def unavailable(item, token, state, remote_identity=None):
        result = original(item, token, state, remote_identity)
        if state == "accepted":
            raise RuntimeError("control acknowledgement unavailable")
        return result

    monkeypatch.setattr(store, "transition", unavailable)
    worker = scheduler(database, dispatch=failed_activity)
    with pytest.raises(ControlPlaneError):
        worker.tick(part, 0)
    monkeypatch.setattr(store, "transition", original)
    assert worker.tick(part, 0) == {"sleep": 0.01}
    assert len(accepted) == 1
    assert sql(
        database, "select attempts from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],)
    ) == [(1,)]
    assert scheduler(database).tick(part, 1) == {"ready": True}
    assert sql(
        database, "select attempts,outcome from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],)
    ) == [(2, "committed")]


def test_live_uncertain_or_accepted_owner_does_not_serialize_eligible_target(database):
    store, _ = database
    run, part = run_fixture(database, 2)
    accepted = []

    def asynchronous_dispatch(reservation):
        accepted.append(reservation["target_key"])
        if reservation["target_key"] == "post0000":
            raise RuntimeError("ambiguous remote submission before callback acquisition")
        args = {k: reservation[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")}
        store.acquire(**args, stage=Stage(reservation["stage"]))
        return "accepted-active-worker"

    worker = scheduler(database, dispatch=asynchronous_dispatch)
    assert worker.tick(part, 0) == {"sleep": 1.0}
    assert worker.tick(part, 1) == {"ready": True}
    assert accepted == ["post0000", "post0001"]
    assert worker.tick(part, 2) == {"sleep": 1.0}
    assert len(accepted) == 2
    assert sql(
        database,
        "select target_key,attempts from social.instagram_ingestion_targets where run_id=%s order by target_key",
        (run["run_id"],),
    ) == [("post0000", 0), ("post0001", 1)]

    active_run, active_part = run_fixture(database, 2)

    def accepted_only(reservation):
        args = {k: reservation[k] for k in ("run_id", "account", "generation", "target_key", "owner_token")}
        store.acquire(**args, stage=Stage(reservation["stage"]))
        return "accepted-active-worker"

    active = scheduler(database, dispatch=accepted_only)
    assert active.tick(active_part, 0) == {"ready": True}
    assert active.tick(active_part, 1) == {"ready": True}
    assert active.tick(active_part, 2) == {"sleep": 1.0}


def test_real_dbos_bounded_runtime_restricted_role_and_partition_history(database):
    pytest.importorskip("dbos")
    from dbos import DBOS

    store, tx = database
    run, _ = run_fixture(database, 70)
    with tx() as conn:
        admin_dsn = conn.dsn
        values = conn.get_dsn_parameters()
    role = "ingestion_runtime"
    sql(database, f"create role {role} login")
    url = f"postgresql://{values['user']}@{values['host']}:{values['port']}/{values['dbname']}"
    DBOS.migrate(url, application_role=role)
    runtime_url = url.replace(f"{values['user']}@", f"{role}@")
    # Other isolated test runs are not inputs to this real-engine fixture.
    sql(
        database,
        "update social.instagram_ingestion_outbox set acknowledged_at=clock_timestamp() where run_id<>%s",
        (run["run_id"],),
    )
    sql(
        database,
        "update social.instagram_ingestion_partitions set acknowledged_at=clock_timestamp() where run_id<>%s",
        (run["run_id"],),
    )
    adapter = DBOSAdapter(
        scheduler(database), RuntimeConfig(runtime_url, "test-v1", runtime_role=role, lifetime_seconds=10)
    )
    with psycopg2.connect(admin_dsn) as rival:
        rival.autocommit = True
        with rival.cursor() as cur:
            cur.execute("select pg_advisory_lock(%s,%s)", (48112, 20261016))
            assert adapter.run_bounded(lambda: psycopg2.connect(runtime_url)) == "busy"
            cur.execute("select pg_advisory_unlock(%s,%s)", (48112, 20261016))
    child_code = """
import sys
import psycopg2
from contextlib import contextmanager
from datetime import UTC, datetime
from trr_backend.ingestion.domain import Stage
from trr_backend.ingestion.workflow_store import WorkflowStore
from trr_backend.ingestion.workflows import ControlPlaneError, DBOSAdapter, RuntimeConfig, Scheduler
@contextmanager
def tx():
    conn=psycopg2.connect(sys.argv[1])
    try:
        with conn:
            yield conn
    finally:
        conn.close()
store=WorkflowStore(tx)
def dispatch(reservation):
    args={k:reservation[k] for k in ('run_id','account','generation','stage','target_key','owner_token')}
    args['stage']=Stage(args['stage'])
    store.acquire(**args)
    store.commit_target(**args, observed_at=datetime.now(UTC),write_canonical=lambda _:{'id':args['target_key']})
    return args['owner_token']
mode=sys.argv[4] if len(sys.argv)>4 else ''
failed_admission=False
def admit(_):
    global failed_admission
    if mode=='admission_once' and not failed_admission:
        failed_admission=True
        raise RuntimeError('admission temporarily unavailable')
    return True
if mode=='schedule_terminal':
    def unavailable(*args,**kwargs):
        raise RuntimeError('persistent scheduling database failure')
    store.decision=unavailable
if mode=='continuation_once':
    complete=store.complete_partition
    failed_continuation=False
    def temporary(*args,**kwargs):
        global failed_continuation
        if not failed_continuation:
            failed_continuation=True
            raise RuntimeError('continuation temporarily unavailable')
        return complete(*args,**kwargs)
    store.complete_partition=temporary
scheduler=Scheduler(store,admit=admit,dispatch=dispatch,reconcile=lambda _:'unknown')
config=RuntimeConfig(sys.argv[2],'test-v1',runtime_role='ingestion_runtime',lifetime_seconds=float(sys.argv[3]))
DBOSAdapter(scheduler,config).run_bounded(lambda:psycopg2.connect(sys.argv[2]))
"""
    child = subprocess.run(
        [sys.executable, "-c", child_code, admin_dsn, runtime_url, "14", "continuation_once"],
        capture_output=True,
        text=True,
        timeout=22,
    )
    assert child.returncode == 0, child.stdout + child.stderr
    assert sql(
        database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (run["run_id"],)
    ) == [(70,)]
    assert sql(database, "select state from social.instagram_ingestion_runs where run_id=%s", (run["run_id"],)) == [
        ("completed",)
    ]
    assert sql(
        database,
        """select max(n) from (select count(*) n from social.instagram_ingestion_decisions d
        join social.instagram_ingestion_partitions p using(execution_id) where p.run_id=%s group by execution_id) s""",
        (run["run_id"],),
    ) == [(50,)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_partitions where run_id=%s", (run["run_id"],)
    ) == [(2,)]
    # destroy already ran; persisted DBOS metadata survives its bounded lifecycle.
    assert (
        sql(database, "select max(n) from (select count(*) n from dbos.operation_outputs group by workflow_uuid) s")[0][
            0
        ]
        <= 51
    )

    recovery_run, recovery_partition = run_fixture(database)
    sql(
        database,
        """update social.instagram_ingestion_targets set next_attempt_at=clock_timestamp()+interval '5 seconds'
        where run_id=%s""",
        (recovery_run["run_id"],),
    )
    first = subprocess.run(
        [sys.executable, "-c", child_code, admin_dsn, runtime_url, "6"], capture_output=True, text=True, timeout=12
    )
    assert first.returncode == 0, first.stdout + first.stderr
    assert sql(
        database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (recovery_run["run_id"],)
    ) == [(0,)]
    assert sql(
        database,
        "select status from dbos.workflow_status where workflow_uuid=%s",
        (recovery_partition["execution_id"],),
    ) == [("PENDING",)]
    assert sql(
        database,
        """select count(*) from dbos.operation_outputs
        where workflow_uuid=%s and function_name='DBOS.sleep'""",
        (recovery_partition["execution_id"],),
    ) == [(1,)]
    # The first process is gone before replacement. Its lease must be released,
    # and its native persisted sleep is recovered by the same logical executor.
    with psycopg2.connect(runtime_url) as lease:
        lease.autocommit = True
        with lease.cursor() as cur:
            cur.execute("select pg_try_advisory_lock(%s,%s)", (48112, 20261016))
            assert cur.fetchone()[0]
            cur.execute("select pg_advisory_unlock(%s,%s)", (48112, 20261016))
    replacement = subprocess.run(
        [sys.executable, "-c", child_code, admin_dsn, runtime_url, "10"], capture_output=True, text=True, timeout=18
    )
    assert replacement.returncode == 0, replacement.stdout + replacement.stderr
    assert sql(
        database,
        "select attempts,outcome from social.instagram_ingestion_targets where run_id=%s",
        (recovery_run["run_id"],),
    ) == [(1, "committed")]

    admission_run, _ = run_fixture(database)
    admission = subprocess.run(
        [sys.executable, "-c", child_code, admin_dsn, runtime_url, "10", "admission_once"],
        capture_output=True,
        text=True,
        timeout=18,
    )
    assert admission.returncode == 0, admission.stdout + admission.stderr
    assert sql(
        database,
        "select attempts,outcome from social.instagram_ingestion_targets where run_id=%s",
        (admission_run["run_id"],),
    ) == [(1, "committed")]
    assert sql(
        database,
        """select count(*) from social.instagram_ingestion_decisions d
        join social.instagram_ingestion_partitions p using(execution_id) where p.run_id=%s
        and decision->>'control_failure'='ADMISSION_UNAVAILABLE'""",
        (admission_run["run_id"],),
    ) == [(1,)]
    failed_run, failed_partition = run_fixture(database)
    terminal = subprocess.run(
        [sys.executable, "-c", child_code, admin_dsn, runtime_url, "10", "schedule_terminal"],
        capture_output=True,
        text=True,
        timeout=18,
    )
    assert terminal.returncode == 0, terminal.stdout + terminal.stderr
    assert sql(
        database, "select status from dbos.workflow_status where workflow_uuid=%s", (failed_partition["execution_id"],)
    ) == [("ERROR",)]
    assert sql(
        database, "select state from social.instagram_ingestion_runs where run_id=%s", (failed_run["run_id"],)
    ) == [("blocked",)]
    assert sql(
        database, "select attempts from social.instagram_ingestion_targets where run_id=%s", (failed_run["run_id"],)
    ) == [(0,)]
    assert sql(
        database,
        """select decision->>'control_failure',decision->>'engine_status'
        from social.instagram_ingestion_decisions where execution_id=%s""",
        (failed_partition["execution_id"],),
    ) == [("ENGINE_TERMINAL", "ERROR")]
    resumed = store.control(failed_run["run_id"], "bravotv", action="resume", idempotency_key=str(uuid4()))
    assert resumed["generation"] == 2 and resumed["workflow_version"] == failed_run["workflow_version"]
    recovery = subprocess.run(
        [sys.executable, "-c", child_code, admin_dsn, runtime_url, "10"], capture_output=True, text=True, timeout=18
    )
    assert recovery.returncode == 0, recovery.stdout + recovery.stderr
    assert sql(
        database,
        "select attempts,outcome from social.instagram_ingestion_targets where run_id=%s",
        (failed_run["run_id"],),
    ) == [(1, "committed")]


@pytest.mark.parametrize("startup_seconds,expect_work", [(7.0, True), (86.0, False)])
def test_runtime_startup_consumes_shared_deadline_and_retains_lease_until_exit(
    monkeypatch, startup_seconds, expect_work
):
    """Slow SDK startup must not refresh the work budget or force normal exit124."""
    from types import SimpleNamespace

    from trr_backend.ingestion import workflows

    class ProcessExit(BaseException):
        def __init__(self, code):
            self.code = code

    events, timers, exits = [], [], []
    calls = {name: [] for name in ("pending", "partitions", "reconcile")}
    available_at = {"pending": 111.25, "reconcile": 113.25}
    handled_at = {}
    clock = SimpleNamespace(now=100.0)

    def advance(seconds):
        target = clock.now + seconds
        for timer in timers:
            if timer.armed and timer.when <= target:
                timer.armed = False
                clock.now = timer.when
                timer.callback()
        clock.now = target

    class Event:
        def wait(self, seconds):
            advance(seconds)
            return False

        def set(self):
            events.append("stop")

    class Thread:
        def __init__(self, **kwargs):
            pass

        def start(self):
            events.append("monitor_started")

        def join(self, **kwargs):
            pass

    class Timer:
        def __init__(self, seconds, callback):
            self.when, self.callback, self.armed = clock.now + seconds, callback, False
            timers.append(self)

        def start(self):
            self.armed = True

        def cancel(self):
            self.armed = False

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query, args=None):
            self.query = query

        def fetchone(self):
            return (True,) if "pg_try_advisory_lock" in self.query else ("test_runtime", False, True, False, False)

    lease = SimpleNamespace(cursor=Cursor, closed=False)
    lease.close = lambda: setattr(lease, "closed", True)

    def fatal_exit(code):
        exits.append((code, clock.now, lease.closed))
        raise ProcessExit(code)

    class DBOS:
        def __call__(self, **kwargs):
            assert timers[0].armed
            events.append("sdk_constructed")

        def launch(self):
            advance(startup_seconds)
            events.append("sdk_launched")

        def destroy(self, **kwargs):
            assert kwargs == {"workflow_completion_timeout_sec": 2}
            events.append("destroy")
            advance(2)

    def work(name):
        # Every control-plane operation in this fixture must start before the
        # original deadline minus shutdown allowance, independent of launch time.
        assert clock.now < 185.000001
        events.append(name)
        calls[name].append(clock.now)
        if name in available_at and clock.now >= available_at[name]:
            handled_at.setdefault(name, clock.now)

    adapter = DBOSAdapter.__new__(DBOSAdapter)
    adapter.config = RuntimeConfig("postgresql://synthetic", "test-v1", runtime_role="test_runtime")
    adapter.dbos = DBOS()
    adapter.scheduler = SimpleNamespace(
        store=SimpleNamespace(
            dispatch_pending=lambda *a, **kw: work("pending"),
            dispatch_partitions=lambda *a, **kw: work("partitions"),
        )
    )
    adapter.reconcile_engine_failures = lambda: work("reconcile")
    monkeypatch.setattr(workflows.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(workflows.threading, "Event", Event)
    monkeypatch.setattr(workflows.threading, "Thread", Thread)
    monkeypatch.setattr(workflows.threading, "Timer", Timer)
    with pytest.raises(ProcessExit) as stopped:
        adapter.run_bounded(lambda: lease, fatal_exit=fatal_exit)
    assert stopped.value.code == 0 and exits == [(0, clock.now, False)]
    assert timers[0].when == 190.0 and clock.now < 190.0
    assert ("pending" in events) == expect_work and ("partitions" in events) == expect_work
    assert ("reconcile" in events) == expect_work
    assert events[-1] == "destroy" and "stop" not in events
    assert not lease.closed  # OS process exit, rather than early close, releases the lease.
    if expect_work:
        # New outbox work and terminal errors arriving between polls remain
        # actionable within the approved cadence, without real sleeps.
        assert 0 <= handled_at["pending"] - available_at["pending"] <= 1
        assert 0 <= handled_at["reconcile"] - available_at["reconcile"] <= 5
        assert calls["pending"] == calls["partitions"]
        assert max(b - a for a, b in zip(calls["pending"], calls["pending"][1:], strict=False)) <= 1
        assert max(b - a for a, b in zip(calls["reconcile"], calls["reconcile"][1:], strict=False)) <= 5
        assert calls["reconcile"][0] == 100 + startup_seconds
        assert calls["pending"][-1] >= 184
        assert calls["reconcile"][-1] >= 180
        assert len(calls["pending"]) <= 85
