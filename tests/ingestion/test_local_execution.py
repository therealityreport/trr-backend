"""Local placement uses disposable PG and stubbed execution; no live services."""

import json
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import psycopg2
import pytest

from api.schemas.v2.instagram_ingestion import InstagramRetryTargetsV2
from scripts.ingestion.local_driver import CoordinatorLease, LocalRequest, main, run_local
from tests.ingestion.test_persistence import database as database_fixture
from trr_backend.ingestion.admission import Admission
from trr_backend.ingestion.command_service import CommandService
from trr_backend.ingestion.composition import WORKFLOW_VERSION
from trr_backend.ingestion.domain import Conflict, Failure, LostOwnership, RunIntent, SourceProfile, Stage
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.queries import Queries
from trr_backend.ingestion.runtime import run_ingestion_activity
from trr_backend.ingestion.worker import Worker
from trr_backend.ingestion.workflow_store import WorkflowStore
from trr_backend.ingestion.workflows import RuntimeConfig

base_database = database_fixture
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def database(base_database):
    _, tx = base_database
    with tx() as conn, conn.cursor() as cur:
        for migration in (
            "20261016070100_instagram_ingestion_workflows.sql",
            "20261016173915_instagram_ingestion_execution_placement.sql",
            "20260911121000_instagram_detail_request_lanes.sql",
            "20261022120000_instagram_server_retry_after.sql",
        ):
            cur.execute((ROOT / "supabase/migrations" / migration).read_text())
        cur.execute("""create table social.scrape_jobs(id uuid primary key,run_id uuid,platform text,
            config jsonb default '{}',metadata jsonb default '{}',job_type text,status text,
            worker_id text,claimed_at timestamptz,completed_at timestamptz,heartbeat_at timestamptz,
            started_at timestamptz,created_at timestamptz default clock_timestamp());
            create table social.account_auth_cooldown(platform text,account_handle text,cooldown_until timestamptz,
            blocker_kind text default 'auth');
            create schema dbos; create role local_runtime login;
            grant usage on schema dbos to local_runtime;
        """)
    return Store(tx), WorkflowStore(tx, execution_placement="local"), tx


@pytest.fixture(autouse=True)
def clean(database):
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute("truncate social.instagram_ingestion_runs,social.scrape_jobs cascade")


def sql(database, statement, args=()):
    with database[2]() as conn, conn.cursor() as cur:
        cur.execute(statement, args)
        return cur.fetchall() if cur.description else None


def fixture_run(database, *, profile="public", workflow_version=WORKFLOW_VERSION):
    store, _, _ = database
    intent = RunIntent(
        account="bravotv",
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2027, 1, 1, tzinfo=UTC),
        selected_tasks=(Stage.DETAILS,),
        source_profile=SourceProfile.for_mode(profile),
    )
    run = store.start(
        intent,
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version=workflow_version,
        initiated_by="offline-test",
    )
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=Stage.DETAILS,
        target_keys=["post1"],
        max_attempts=3,
        cursor_state={},
        expected_cursor_state={},
        page_id="fixture",
        seal=True,
    )
    sql(
        database,
        """update social.instagram_ingestion_manifests set discovery_complete=true,
        sealed_at=clock_timestamp() where run_id=%s and stage='listing'""",
        (run["run_id"],),
    )
    cancelled = store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    return cancelled, intent, LocalRequest(run["run_id"], intent.digest, cancelled["generation"], "modal")


def place(database, request, **overrides):
    kwargs = {
        "expected_generation": request.generation,
        "expected_intent_hash": request.intent_hash,
        "expected_placement": request.expected_placement,
        "placement": "local",
        "idempotency_key": str(uuid4()),
    }
    kwargs.update(overrides)
    return database[0].set_execution_placement(request.run_id, "bravotv", **kwargs)


def resume_local(database, request):
    place(database, request)
    return database[1].control(
        request.run_id,
        "bravotv",
        action="resume",
        idempotency_key=str(uuid4()),
        expected_generation=request.generation,
        expected_placement="local",
    )


def owned(reservation):
    result = {key: reservation[key] for key in ("run_id", "account", "generation", "target_key", "owner_token")}
    return dict(result, stage=Stage(reservation["stage"]))


def test_placement_exact_identity_idempotency_and_cancelled_public_only(database):
    run, intent, request = fixture_run(database)
    for changes in ({"expected_generation": 1}, {"expected_intent_hash": "0" * 64}, {"expected_placement": "local"}):
        with pytest.raises(Conflict):
            place(database, request, **changes)
    key = str(uuid4())
    result = place(database, request, idempotency_key=key)
    assert place(database, request, idempotency_key=key) == result
    with pytest.raises(Conflict):
        place(database, request, idempotency_key=key, placement="modal")
    assert sql(database, "select intent,intent_hash,execution_placement from social.instagram_ingestion_runs") == [
        (intent.payload(), intent.digest, "local")
    ]
    with pytest.raises(LookupError):
        database[0].set_execution_placement(
            run["run_id"],
            "wrong",
            expected_generation=2,
            expected_intent_hash=intent.digest,
            expected_placement="local",
            placement="modal",
            idempotency_key=str(uuid4()),
        )
    # Reversal is explicit and also requires the exact cancelled public run.
    place(database, request, expected_placement="local", placement="modal")
    _, _, authenticated = fixture_run(database, profile="authenticated")
    with pytest.raises(Conflict, match="public"):
        place(database, authenticated)


@pytest.mark.parametrize("owner", ["live", "uncertain", "sending", "accepted"])
def test_placement_rejects_live_and_uncertain_owners(database, owner):
    run, _, request = fixture_run(database)
    token = str(uuid4())
    if owner in {"live", "uncertain"}:
        sql(
            database,
            """update social.instagram_ingestion_targets set owner_token=%s,
            owner_expires_at=%s where run_id=%s""",
            (token, datetime.now(UTC) + timedelta(seconds=30) if owner == "live" else None, request.run_id),
        )
    else:
        sql(
            database,
            """insert into social.instagram_ingestion_dispatches
            (owner_token,run_id,generation,stage,target_key,state) values(%s,%s,1,'post_details','post1',%s)""",
            (token, request.run_id, owner),
        )
    with pytest.raises(Conflict, match="owner"):
        place(database, request)
    if owner in {"sending", "accepted"}:
        sql(database, "update social.instagram_ingestion_runs set updated_at=clock_timestamp()-interval '601 seconds'")
        sql(
            database,
            "update social.instagram_ingestion_dispatches set updated_at=clock_timestamp()-interval '601 seconds'",
        )
        assert place(database, request)["execution_placement"] == "local"
        current = database[1].control(request.run_id, "bravotv", action="resume", idempotency_key=str(uuid4()))
        with pytest.raises(LostOwnership):
            database[1].acquire(
                request.run_id, "bravotv", generation=1, stage=Stage.DETAILS, target_key="post1", owner_token=token
            )
        assert current["generation"] == 3


def test_sigkill_durable_placement_fences_cloud_replay_dispatch_worker_and_writes(database, monkeypatch):
    _, _, request = fixture_run(database)
    current = resume_local(database, request)
    cloud = WorkflowStore(database[2])
    local = database[1]
    item = local.seed(dict(current, account="bravotv"))
    # Acknowledged partitions remain excluded from cloud engine recovery.
    sql(database, "update social.instagram_ingestion_partitions set acknowledged_at=clock_timestamp()")
    assert cloud.engine_partitions(workflow_version=WORKFLOW_VERSION) == []
    assert cloud.dispatch_pending(lambda _: pytest.fail("cloud submit")) == 0
    assert cloud.dispatch_partitions(lambda _: pytest.fail("cloud partition")) == 0
    with pytest.raises(LostOwnership, match="placement"):
        cloud.decision(item, 0, datetime.now(UTC))
    with pytest.raises(LostOwnership):
        cloud.pause_failed_partition(item, "ERROR")
    decision = local.decision(item, 0, datetime.now(UTC))
    reservation = local.reserve_decision(item, decision, ordinal=0)
    with pytest.raises(LostOwnership):
        cloud.reserve(
            request.run_id, "bravotv", generation=current["generation"], stage=Stage.DETAILS, target_key="post1"
        )
    with pytest.raises(LostOwnership):
        cloud.acquire(**owned(reservation))
    from trr_backend.ingestion import composition

    monkeypatch.setattr(composition, "Store", lambda: cloud)
    monkeypatch.setattr(composition, "_modal_target", lambda: pytest.fail("provider target accessed"))
    with pytest.raises(LostOwnership):
        composition._dispatch(reservation)
    worker = Worker(
        cloud,
        **{
            name: lambda *_a, **_k: pytest.fail("source constructor")
            for name in ("listing_source", "detail_source", "comment_source", "media_loader", "media_storage")
        },
    )
    assert worker.execute(reservation) == {"outcome": "fenced", "stage": "post_details"}
    local.acquire(**owned(reservation))
    with pytest.raises(LostOwnership):
        cloud.check_owner(**owned(reservation), record_request=True)
    with pytest.raises(LostOwnership):
        cloud.commit_target(
            **owned(reservation), observed_at=datetime.now(UTC), write_canonical=lambda _: pytest.fail("write")
        )
    # Simulated driver SIGKILL: lease disappears, placement persists, no cleanup
    # callback needed to exclude this still-active generation from cloud work.
    assert sql(database, "select execution_placement,state from social.instagram_ingestion_runs") == [
        ("local", "running")
    ]
    assert cloud.dispatch_pending(lambda _: pytest.fail("crash fallback")) == 0


class ObservedLease:
    def __init__(self, database):
        self.database, self.entered, self.exited = database, False, False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *_):
        self.exited = True
        assert sql(self.database, "select state from social.instagram_ingestion_runs") == [("cancelled",)]


@pytest.mark.parametrize("result", ["committed", "source_failure", "lease_lost", "interrupt"])
def test_real_scheduler_one_local_target_receipt_or_preserved_failure_cancel_before_release(database, result):
    _, _, request = fixture_run(database)
    local, tx = database[1:]
    abort = threading.Event()
    lease = ObservedLease(database)
    probes = []
    admission = Admission(
        read_connection=tx, environ={}, fresh_session_probe=lambda **kw: probes.append(kw) or {"available": True}
    )
    sends = []

    def execute(reservation, **kwargs):
        assert lease.entered and not lease.exited and kwargs["execution_placement"] == "local"
        assert kwargs["abort"] is abort and kwargs["timeout_seconds"] <= 60
        args = owned(reservation)
        local.acquire(**args)
        local.check_owner(**args, record_request=True)
        sends.append(reservation)
        if result == "interrupt":
            raise KeyboardInterrupt
        if result == "committed":
            local.commit_target(**args, observed_at=datetime.now(UTC), write_canonical=lambda _: {"post_id": "stable"})
            return {"outcome": "committed", "stage": "post_details"}
        if result == "lease_lost":
            abort.set()
            return {"outcome": "fenced", "stage": "post_details"}
        local.checkpoint_failure(
            **args,
            failure=Failure("public_access_backoff", retry_at=datetime.now(UTC) + timedelta(seconds=37)),
            checkpoint={"cursor": "preserved"},
            requests=0,
        )
        return {"outcome": "unresolved", "stage": "post_details", "code": "public_access_backoff"}

    kwargs = {"store": local, "admit": admission.admit, "execute": execute, "lease": lease, "abort": abort}
    if result == "interrupt":
        with pytest.raises(KeyboardInterrupt):
            run_local(request, **kwargs)
    else:
        summary = run_local(request, **kwargs)
        assert summary["targets"] == 1
    assert lease.exited and len(sends) == 1 and len(probes) == 1
    assert sql(database, "select remote_identity from social.instagram_ingestion_dispatches") == [(None,)]
    row = sql(
        database,
        """select outcome,attempts,requests,owner_token,checkpoint,next_attempt_at
        from social.instagram_ingestion_targets""",
    )[0]
    assert row[1:4] == (1, 1, None)
    assert sql(database, "select count(*) from social.instagram_ingestion_receipts")[0][0] == (result == "committed")
    if result == "source_failure":
        assert row[4]["cursor"] == "preserved" and row[5] > datetime.now(UTC)


def test_existing_admission_counts_local_reservation_and_legacy_function_budget(database):
    _, _, request = fixture_run(database)
    current = resume_local(database, request)
    item = database[1].seed(dict(current, account="bravotv"))
    candidate = dict(item, stage="post_details", target_key="post1")
    admission = Admission(
        read_connection=database[2],
        environ={"TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT": "1"},
        fresh_session_probe=lambda **_: {"available": True},
    )
    assert admission.admit(candidate)
    database[1].reserve_decision(item, database[1].decision(item, 0, datetime.now(UTC)), ordinal=0)
    assert not admission.admit(candidate)
    sql(database, "update social.instagram_ingestion_targets set owner_token=null,owner_expires_at=null")
    sql(
        database,
        """insert into social.scrape_jobs(id,platform,config,status)
        values(%s,'instagram','{"stage":"shared_account_posts","account":"other"}','running')""",
        (str(uuid4()),),
    )
    assert not admission.admit(candidate)


def test_local_progress_strict_response_and_api_resume_retry_rejected_cancel_allowed(database):
    _, _, request = fixture_run(database)
    place(database, request)
    api = CommandService(database[0], Queries(database[2]), engine="dbos", workflow_version=WORKFLOW_VERSION)
    progress = api.progress("bravotv", UUID(request.run_id))
    assert progress.execution_placement == "local" and progress.allowed_actions == []
    assert api.recent("bravotv", source_scope="network", limit=10).runs[0].execution_placement == "local"
    with pytest.raises(Conflict, match="local driver"):
        api.control(
            "bravotv", UUID(request.run_id), action="resume", idempotency_key=str(uuid4()), initiated_by="admin"
        )
    with pytest.raises(Conflict, match="local driver"):
        api.retry(
            "bravotv",
            UUID(request.run_id),
            InstagramRetryTargetsV2(stage="post_details", target_keys=["post1"]),
            idempotency_key=str(uuid4()),
            initiated_by="admin",
        )
    with pytest.raises(LostOwnership):
        database[0].control(request.run_id, "bravotv", action="resume", idempotency_key=str(uuid4()))
    assert (
        api.control(
            "bravotv", UUID(request.run_id), action="cancel", idempotency_key=str(uuid4()), initiated_by="admin"
        ).state
        == "cancelled"
    )


def test_driver_refuses_wrong_generation_before_resume_and_plan_has_no_connections(database, capsys, monkeypatch):
    _, _, request = fixture_run(database)
    bad = LocalRequest(request.run_id, request.intent_hash, 99, "modal")
    with pytest.raises(Conflict):
        run_local(
            bad,
            store=database[1],
            admit=lambda _: pytest.fail("admit"),
            execute=lambda *_: pytest.fail("source"),
            lease=contextmanager(lambda: (yield))(),
            abort=threading.Event(),
        )
    monkeypatch.setattr(Store, "_run", lambda *_: pytest.fail("plan database"))
    assert (
        main(
            [
                "--run-id",
                request.run_id,
                "--intent-hash",
                request.intent_hash,
                "--generation",
                "2",
                "--expected-placement",
                "modal",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "plan"


def test_shared_session_lease_busy_and_loss_sets_abort(database, register_isolated_postgres):
    with database[2]() as conn:
        dsn = conn.dsn
        endpoint = {"host": conn.info.host, "port": conn.info.port, "dbname": conn.info.dbname, "user": "local_runtime"}
    config = RuntimeConfig(dsn, WORKFLOW_VERSION, runtime_role="local_runtime")

    def connect():
        return psycopg2.connect(dsn, user="local_runtime")

    abort = threading.Event()
    with register_isolated_postgres(**endpoint):
        with CoordinatorLease(connect, config, abort) as lease:
            with pytest.raises(Conflict, match="busy"):
                with CoordinatorLease(connect, config, threading.Event()):
                    pytest.fail("second consumer")
            lease.connection.close()
            assert abort.wait(2)


@pytest.mark.parametrize(
    "failure", [None, "busy", "role", "setup", "unlock", "stuck_unlock", "remaining_grant", "grant_query"]
)
def test_lease_pooled_session_cleanup_is_exact_restored_and_bounded(failure):
    commands, closed = [], threading.Event()
    release = threading.Event()

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, query, args=()):
            self.query = query
            commands.append((query, args))
            if "5000ms" in query and failure == "setup":
                raise RuntimeError("unsupported setting")
            if "from pg_locks" in query and failure == "grant_query":
                raise OSError("uncertain grant query")
            if "pg_advisory_unlock" in query:
                if failure == "unlock":
                    raise OSError("lost transport")
                if failure == "stuck_unlock":
                    release.wait(5)

        def fetchone(self):
            if "pg_try_advisory_lock" in self.query:
                return (failure != "busy",)
            if "pg_advisory_unlock" in self.query:
                return (True,)
            if "current_setting" in self.query:
                return ("17s",)
            if "from pg_locks" in self.query:
                return (failure == "remaining_grant",)
            return ("wrong" if failure == "role" else "local_runtime", False, True, False, False)

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            closed.set()

    abort = threading.Event()
    lease = CoordinatorLease(
        Connection,
        RuntimeConfig("unused", WORKFLOW_VERSION, runtime_role="local_runtime"),
        abort,
        heartbeat_timeout=0.1,
    )
    try:
        if failure in {"busy", "role", "setup"}:
            with pytest.raises((Conflict, RuntimeError)):
                lease.__enter__()
        else:
            with lease:
                pass
        unlocks = [(q, args) for q, args in commands if "pg_advisory_unlock" in q]
        assert len(unlocks) == (0 if failure == "busy" else 1)
        if unlocks:
            assert unlocks[0][1] == (lease.config.lock_namespace, lease.config.lock_key)
        restores = [(q, args) for q, args in commands if args == ("17s",)]
        assert bool(restores) == (failure in {None, "setup"})
        if restores:
            assert commands.index(unlocks[0]) < commands.index(restores[0])
        assert abort.is_set() == (failure in {"unlock", "stuck_unlock", "remaining_grant", "grant_query"})
        if failure != "stuck_unlock":
            assert closed.is_set()
    finally:
        release.set()
        assert closed.wait(1)


def test_lease_idle_expiry_releases_abandoned_pooled_backend(database, register_isolated_postgres):
    with database[2]() as conn:
        dsn = conn.dsn
        endpoint = {"host": conn.info.host, "port": conn.info.port, "dbname": conn.info.dbname, "user": "local_runtime"}
    connections = []

    def connect():
        connection = psycopg2.connect(dsn, user="local_runtime")
        connections.append(connection)
        return connection

    config = RuntimeConfig(dsn, WORKFLOW_VERSION, runtime_role="local_runtime")
    lease = CoordinatorLease(connect, config, threading.Event())
    with register_isolated_postgres(**endpoint):
        try:
            lease.__enter__()
            with lease.connection.cursor() as cur:
                cur.execute("show idle_session_timeout")
                assert cur.fetchone() == ("5s",)
            # Model a lost frontend whose pooled backend remains connected:
            # stop heartbeats without invoking unlock or connection close.
            lease.stop.set()
            lease.monitor.join(1)
            lease.watchdog.join(1)
            time.sleep(5.2)
            with CoordinatorLease(connect, config, threading.Event()):
                pass
        finally:
            for connection in connections:
                connection.close()


@pytest.mark.parametrize("stacked", [False, True])
def test_lease_release_restores_surviving_pooled_session(database, register_isolated_postgres, stacked):
    with database[2]() as conn:
        dsn = conn.dsn
        endpoint = {"host": conn.info.host, "port": conn.info.port, "dbname": conn.info.dbname, "user": "local_runtime"}
    with register_isolated_postgres(**endpoint):
        connection = psycopg2.connect(dsn, user="local_runtime")
        connection.autocommit = True

        class PooledFrontend:
            autocommit = True

            def cursor(self):
                return connection.cursor()

            def close(self):
                pass  # Session pooler keeps this backend available to another frontend.

        config = RuntimeConfig(dsn, WORKFLOW_VERSION, runtime_role="local_runtime")
        abort = threading.Event()
        try:
            with connection.cursor() as cur:
                cur.execute("set idle_session_timeout='17s'")
                if stacked:
                    cur.execute("select pg_try_advisory_lock(%s,%s)", (config.lock_namespace, config.lock_key))
                    assert cur.fetchone() == (True,)
            with CoordinatorLease(PooledFrontend, config, abort) as lease:
                pass
            with connection.cursor() as cur:
                cur.execute("show idle_session_timeout")
                assert cur.fetchone() == ("5s" if stacked else "17s",)
                cur.execute(
                    """select exists(select 1 from pg_locks where locktype='advisory' and granted
                    and pid=pg_backend_pid() and classid=%s::oid and objid=%s::oid and objsubid=2
                    and database=(select oid from pg_database where datname=current_database()))""",
                    (config.lock_namespace, config.lock_key),
                )
                assert cur.fetchone() == (stacked,)
            assert abort.is_set() is stacked
            assert lease.acquired is stacked
            if not stacked:
                # Another independently connected consumer can immediately acquire.
                with CoordinatorLease(lambda: psycopg2.connect(dsn, user="local_runtime"), config, threading.Event()):
                    pass
        finally:
            connection.close()


def test_runtime_local_abort_kills_and_reaps_child(monkeypatch):
    from trr_backend.ingestion import runtime

    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    real_popen = subprocess.Popen
    children = []

    def spawn(argv, **kwargs):
        assert argv[3:] == ["--execution-placement", "local", "--expected-parent-pid", str(os.getpid())]
        child = real_popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    abort = threading.Event()
    timer = threading.Timer(0.2, abort.set)
    timer.start()
    reservation = {
        "run_id": str(uuid4()),
        "account": "bravotv",
        "generation": 3,
        "stage": "post_details",
        "target_key": "p",
        "owner_token": str(uuid4()),
    }
    try:
        assert (
            run_ingestion_activity(reservation, execution_placement="local", timeout_seconds=5, abort=abort)["code"]
            == "local_execution_aborted"
        )
    finally:
        timer.cancel()
    assert children[0].poll() == -signal.SIGKILL
    with pytest.raises(ProcessLookupError):
        os.kill(children[0].pid, 0)


def test_local_worker_exits_after_parent_sigkill(tmp_path):
    child_pid = tmp_path / "pid"
    ready = tmp_path / "ready"
    # Real worker.main/watchdog, but execution has no HTTP, credentials or DB.
    child_script = f"""import os,time
from pathlib import Path
from trr_backend.ingestion import worker
from trr_backend.db import pg
pg.close_pool=lambda:None
def execute(*a,**kw):
    Path({str(ready)!r}).write_text('ready')
    time.sleep(30)
worker.execute_reservation=execute
raise SystemExit(worker.main(execution_placement='local',expected_parent_pid=int(__import__('sys').argv[1])))
"""
    outer = f"""import os,subprocess,sys,json,time
from pathlib import Path
child=subprocess.Popen([sys.executable,'-c',{child_script!r},str(os.getpid())],stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
Path({str(child_pid)!r}).write_text(str(child.pid))
child.stdin.write(json.dumps({{'run_id':'r','account':'a','generation':1,'stage':'listing','target_key':'p','owner_token':'o'}}).encode());child.stdin.close()
time.sleep(30)
"""
    driver = subprocess.Popen([sys.executable, "-c", outer], start_new_session=True)
    pid = None
    try:
        end = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < end:
            time.sleep(0.05)
        assert ready.exists()
        pid = int(child_pid.read_text())
        driver.kill()
        driver.wait(timeout=3)
        end = time.monotonic() + 3
        while time.monotonic() < end:
            state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
            if not state or state.startswith("Z"):
                break
            time.sleep(0.05)
        assert not state or state.startswith("Z"), "orphan activity survived driver SIGKILL"
    finally:
        if driver.poll() is None:
            driver.kill()
            driver.wait(timeout=3)
        if pid:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_accepted_resume_lost_response_still_cancels_before_lease_release(database, monkeypatch):
    _, _, request = fixture_run(database)
    local = database[1]
    actual_control = local.control

    def lost_response(*args, **kwargs):
        result = actual_control(*args, **kwargs)
        if kwargs["action"] == "resume":
            raise OSError("synthetic lost command response")
        return result

    monkeypatch.setattr(local, "control", lost_response)
    lease = ObservedLease(database)
    with pytest.raises(OSError):
        run_local(
            request,
            store=local,
            admit=lambda _: pytest.fail("admit after lost resume"),
            execute=lambda *_a, **_k: pytest.fail("source"),
            lease=lease,
            abort=threading.Event(),
        )
    assert lease.exited


def test_explicit_configuration_checks_all_fallback_targets_without_connecting(monkeypatch):
    from scripts.ingestion.local_driver import validate_configuration
    from trr_backend.db import connection

    correct = "postgresql://runtime:synthetic@db.vwxfvzutyufrkhfgoeaa.supabase.co/postgres"
    monkeypatch.setattr(connection, "resolve_database_url_candidate_details", lambda: ({"url": correct},))
    monkeypatch.setattr(connection, "resolve_session_database_url_candidate_details", lambda: ({"url": correct},))
    validate_configuration(correct)
    for wrong in (
        "postgresql://runtime@db.aywqykmrlgzgdhaysajr.supabase.co/postgres",
        "postgresql://runtime@localhost/postgres",
        correct.replace("/postgres", ":6543/postgres"),
    ):
        with pytest.raises(Conflict):
            validate_configuration(wrong)
    monkeypatch.setattr(
        connection,
        "resolve_database_url_candidate_details",
        lambda: ({"url": correct}, {"url": "postgresql://runtime@localhost/postgres"}),
    )
    with pytest.raises(Conflict):
        validate_configuration(correct)


@pytest.mark.parametrize("recent", ["cancellation", "dispatch"])
def test_placement_does_not_expire_send_when_either_drain_timestamp_is_recent(database, recent):
    _, _, request = fixture_run(database)
    token = str(uuid4())
    sql(
        database,
        """insert into social.instagram_ingestion_dispatches
        (owner_token,run_id,generation,stage,target_key,state,updated_at)
        values(%s,%s,1,'post_details','post1','sending',clock_timestamp()-interval '601 seconds')""",
        (token, request.run_id),
    )
    sql(database, "update social.instagram_ingestion_runs set updated_at=clock_timestamp()-interval '601 seconds'")
    table = "instagram_ingestion_runs" if recent == "cancellation" else "instagram_ingestion_dispatches"
    sql(database, f"update social.{table} set updated_at=clock_timestamp()")
    with pytest.raises(Conflict, match="owner"):
        place(database, request)
    assert sql(database, "select state from social.instagram_ingestion_dispatches") == [("sending",)]
    assert sql(database, "select execution_placement from social.instagram_ingestion_runs") == [("modal",)]


@pytest.mark.parametrize("state", ["sending", "accepted"])
def test_aged_transition_expires_old_journal_and_real_single_slot_admission_executes(database, state):
    _, _, request = fixture_run(database)
    old_token = str(uuid4())
    sql(
        database,
        """insert into social.instagram_ingestion_dispatches
        (owner_token,run_id,generation,stage,target_key,state,remote_identity,updated_at)
        values(%s,%s,1,'post_details','post1',%s,'historical-provider-call',
            clock_timestamp()-interval '601 seconds')""",
        (old_token, request.run_id, state),
    )
    sql(
        database,
        """update social.instagram_ingestion_targets set attempts=1,requests=2,
        checkpoint='{"cursor":"preserved"}'""",
    )
    sql(database, "update social.instagram_ingestion_runs set updated_at=clock_timestamp()-interval '601 seconds'")
    local, tx = database[1:]
    admission = Admission(
        read_connection=tx,
        environ={"TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT": "1"},
        fresh_session_probe=lambda **_: {"available": True},
    )
    completed = []

    def execute(reservation, **_):
        assert sql(
            database,
            "select state,remote_identity from social.instagram_ingestion_dispatches where owner_token=%s",
            (old_token,),
        ) == [("expired", "historical-provider-call")]
        args = owned(reservation)
        local.acquire(**args)
        local.check_owner(**args, record_request=True)
        local.commit_target(**args, observed_at=datetime.now(UTC), write_canonical=lambda _: {"post_id": "stable"})
        completed.append(reservation)
        return {"outcome": "committed", "stage": "post_details"}

    result = run_local(
        request,
        store=local,
        admit=admission.admit,
        execute=execute,
        lease=ObservedLease(database),
        abort=threading.Event(),
    )
    assert result["targets"] == len(completed) == 1
    assert sql(database, "select attempts,requests,checkpoint from social.instagram_ingestion_targets") == [
        (2, 3, {"cursor": "preserved"})
    ]
    assert sql(database, "select owner_token from social.instagram_ingestion_receipts") == [
        (completed[0]["owner_token"],)
    ]
    assert sql(database, "select count(*) from social.instagram_ingestion_dispatches") == [(2,)]


def test_aged_dispatch_expiry_rolls_back_with_failed_placement(database):
    _, _, request = fixture_run(database)
    token = str(uuid4())
    sql(
        database,
        """insert into social.instagram_ingestion_dispatches
        (owner_token,run_id,generation,stage,target_key,state,updated_at)
        values(%s,%s,1,'post_details','post1','sending',clock_timestamp()-interval '601 seconds')""",
        (token, request.run_id),
    )
    sql(database, "update social.instagram_ingestion_runs set updated_at=clock_timestamp()-interval '601 seconds'")
    sql(
        database,
        """create function social.reject_test_placement() returns trigger language plpgsql as $$
        begin raise exception 'synthetic placement failure'; end $$;
        create trigger reject_test_placement before update of execution_placement on social.instagram_ingestion_runs
        for each row execute function social.reject_test_placement();""",
    )
    try:
        with pytest.raises(psycopg2.Error):
            place(database, request)
        assert sql(database, "select state from social.instagram_ingestion_dispatches") == [("sending",)]
        assert sql(database, "select execution_placement from social.instagram_ingestion_runs") == [("modal",)]
    finally:
        sql(
            database,
            """drop trigger reject_test_placement on social.instagram_ingestion_runs;
            drop function social.reject_test_placement();""",
        )


@pytest.mark.parametrize("failure", ["stalled_query", "connection_lost"])
def test_heartbeat_failure_aborts_reaps_activity_and_cancels_before_lease_cleanup(database, monkeypatch, failure):
    from trr_backend.ingestion import runtime

    _, _, request = fixture_run(database)
    abort, started, query_entered, release_query = (threading.Event() for _ in range(4))
    real_popen = subprocess.Popen
    children = []
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")

    def spawn(_argv, **kwargs):
        child = real_popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        children.append(child)
        started.set()
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, query, *_):
            self.query = query
            if query == "select 1":
                assert started.wait(2)
                query_entered.set()
                if failure == "connection_lost":
                    raise OSError("synthetic lease connection loss")
                release_query.wait(5)

        def fetchone(self):
            return (True,) if "pg_try_advisory_lock" in self.query else ("local_runtime", False, True, False, False)

    class Connection:
        closed = False

        def cursor(self):
            return Cursor()

        def close(self):
            # Even a blocked heartbeat must not delay activity termination or
            # run cancellation until this later connection-cleanup boundary.
            assert children and children[0].poll() == -signal.SIGKILL
            assert sql(database, "select state from social.instagram_ingestion_runs") == [("cancelled",)]
            assert abort.is_set() and query_entered.is_set()
            self.closed = True
            release_query.set()

    connection = Connection()
    lease = CoordinatorLease(
        lambda: connection,
        RuntimeConfig("unused", WORKFLOW_VERSION, runtime_role="local_runtime"),
        abort,
        heartbeat_timeout=0.5,
    )
    before = time.monotonic()
    try:
        result = run_local(
            request,
            store=database[1],
            # This regression owns heartbeat/reaping, not production admission
            # policy for the deliberately synthetic run.
            admit=lambda _: True,
            execute=runtime.run_ingestion_activity,
            lease=lease,
            abort=abort,
        )
        assert result["status"] == "lease_lost" and result["targets"] == 1
        assert result["outcomes"] == [{"outcome": "fenced", "stage": "post_details", "code": "local_execution_aborted"}]
        assert time.monotonic() - before < 4
        assert connection.closed
    finally:
        release_query.set()
        for child in children:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=3)
