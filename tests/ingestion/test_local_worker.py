"""Durable Mac worker: disposable PG, stubbed activity, no live services."""

import threading
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from scripts.ingestion.local_worker import LocalDispatch, run_partition, run_worker
from tests.ingestion import test_local_execution as local_execution
from tests.ingestion.test_local_execution import fixture_run, owned, resume_local, sql
from trr_backend.ingestion.admission import Admission
from trr_backend.ingestion.command_service import CommandService
from trr_backend.ingestion.composition import (
    LOCAL_WORKER_LOCK_KEY,
    LOCAL_WORKER_LOCK_NAMESPACE,
    WORKFLOW_VERSION,
    local_worker_online,
)
from trr_backend.ingestion.domain import Conflict, LostOwnership, RunIntent, SourceProfile, Stage, fingerprint
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.workflow_store import WorkflowStore
from trr_backend.ingestion.workflows import Scheduler

# Reuse the local-placement disposable database and its per-test truncation.
base_database = local_execution.base_database
clean = local_execution.clean


def test_media_boundary_drains_six_comments_and_recovery_precedes_finalization(monkeypatch):
    monkeypatch.setattr("scripts.ingestion.local_worker.PARTITION_SIZE", 10)
    events = []
    abort = threading.Event()

    class Dispatch:
        active = {}
        latest = None
        comments_submitted = 0
        limit = 6

        def bind(self, item, limit):
            self.limit = limit

        def drain(self, store, *, finish=False):
            if finish:
                events.append("drained")

        def wait(self, *args):
            pass

    class FakeStore:
        def complete_partition(self, item, **kwargs):
            events.append(kwargs["stop_state"])

    class FakeScheduler:
        def tick(self, item, ordinal):
            if ordinal < 6:
                dispatch.comments_submitted += 1
                return {"ready": True}
            if ordinal == 6:
                assert item["prefer_media"] and events[-1] == "drained"
                events.append("media")
                return {"ready": True}
            return {"stop": "completed"}

    recovery_calls = []

    def recover(item, *, stop):
        assert stop == "completed"
        recovery_calls.append(True)
        return len(recovery_calls) == 1

    dispatch = Dispatch()
    result = run_partition(
        {},
        store=FakeStore(),
        scheduler=FakeScheduler(),
        dispatch=dispatch,
        abort=abort,
        effective_comments_limit=lambda _: 6,
        before_finalize=recover,
        on_completed=lambda _: events.append("certify"),
    )
    assert result == "stopped" and len(recovery_calls) == 2
    assert "media" in events and events[-2:] == ["completed", "certify"]


@pytest.mark.parametrize("cause", ["abort", "timeout"])
def test_certification_interruption_reaps_decoder_group(tmp_path, cause):
    import json
    import os
    import subprocess
    import sys
    import time

    from scripts.ingestion.local_worker import Interrupted, run_owned_certification

    identities = tmp_path / "owned-child-pids.json"
    code = """import subprocess,sys,os,signal,json,time
from pathlib import Path
p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])
def stop(*_):
 p.wait(timeout=2)
 raise SystemExit(0)
signal.signal(signal.SIGTERM,stop)
Path(sys.argv[1]).write_text(json.dumps([os.getpid(),p.pid]))
time.sleep(60)
"""
    abort = threading.Event()
    errors = []
    with (tmp_path / "owned.log").open("w") as log:

        def execute():
            try:
                run_owned_certification(
                    [sys.executable, "-c", code, str(identities)],
                    log,
                    abort,
                    timeout_seconds=2 if cause == "timeout" else 30,
                )
            except (Interrupted, subprocess.TimeoutExpired) as error:
                errors.append(type(error))

        thread = threading.Thread(target=execute)
        thread.start()
        deadline = time.monotonic() + 5
        while not identities.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert identities.exists()
        if cause == "abort":
            abort.set()
        thread.join(8)
        assert not thread.is_alive()
    assert errors == [Interrupted if cause == "abort" else subprocess.TimeoutExpired]
    for pid in json.loads(identities.read_text()):
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_interrupted_certification_artifact_is_preserved_and_can_resume(tmp_path):
    import json

    from scripts.ingestion.local_worker import retained_certification

    output = tmp_path / "certificate.json"
    output.write_text("")  # The CLI reserved output before it was interrupted.
    assert retained_certification(output) is False and not output.exists()
    archived = list(tmp_path.glob("certificate.interrupted-*.json"))
    assert len(archived) == 1 and archived[0].read_text() == ""
    output.write_text(json.dumps({"final_acceptance": False, "gates": {"source_count_parity": "blocked"}}))
    assert retained_certification(output) is True and output.exists()


@pytest.fixture(scope="module")
def database(base_database):
    local_database = local_execution.database.__wrapped__(base_database)
    with local_database[2]() as conn, conn.cursor() as cur:
        cur.execute(
            (local_execution.ROOT / "supabase/migrations/20261023140000_instagram_execution_policy.sql").read_text()
        )
        cur.execute(
            (
                local_execution.ROOT / "supabase/migrations/20261023180000_instagram_execution_policy_six_eight.sql"
            ).read_text()
        )
    return local_database


def _worker(database, execute, *, abort, stop, on_cycle=None, effective_comments_limit=None):
    _, local, tx = database
    admission = Admission(read_connection=tx, environ={}, fresh_session_probe=lambda **kw: {"available": True})
    dispatch = LocalDispatch(execute, abort)
    scheduler = Scheduler(local, admit=admission.admit, dispatch=dispatch, reconcile=lambda _: "unknown")
    return run_worker(
        store=local,
        scheduler=scheduler,
        dispatch=dispatch,
        lease=nullcontext(),
        abort=abort,
        workflow_version=WORKFLOW_VERSION,
        stop_requested=stop,
        poll_seconds=0.01,
        on_cycle=on_cycle,
        effective_comments_limit=effective_comments_limit,
    )


def _comment_run(database, count, limit):
    store = database[0]
    intent = RunIntent(
        account="bravotv",
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
        selected_tasks=(Stage.COMMENTS,),
        comments_worker_count=limit,
        source_profile=SourceProfile.for_mode("public"),
    )
    run = store.start(
        intent,
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version=WORKFLOW_VERSION,
        initiated_by="offline-test",
    )
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=Stage.COMMENTS,
        target_keys=[f"comment-post-{index}" for index in range(count)],
        max_attempts=12,
        cursor_state={},
        expected_cursor_state={},
        page_id="fixture-comments",
        seal=True,
    )
    sql(
        database,
        """update social.instagram_ingestion_manifests set discovery_complete=true,
        sealed_at=clock_timestamp() where stage='listing'""",
    )
    cancelled = store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    request = local_execution.LocalRequest(run["run_id"], intent.digest, cancelled["generation"], "modal")
    current = resume_local(database, request)
    return database[1].seed(dict(current, account="bravotv"))


@pytest.mark.parametrize("limit", [2, 4, 6, 8])
def test_comment_children_overlap_but_admission_and_policy_remain_serial(database, limit):
    _comment_run(database, limit * 2, limit)
    local = database[1]
    abort, stop = threading.Event(), threading.Event()
    barrier = threading.Barrier(limit)
    lock = threading.Lock()
    running, peak, calls = set(), [], []
    owner = threading.get_ident()

    def policy(item):
        assert threading.get_ident() == owner
        return limit

    def execute(reservation, **kwargs):
        assert kwargs["abort"] is abort and kwargs["timeout_seconds"] == 590
        args = owned(reservation)
        local.acquire(**args)
        with lock:
            running.add(reservation["owner_token"])
            peak.append(len(running))
            calls.append(reservation["target_key"])
        try:
            barrier.wait(timeout=10)
            local.commit_target(**args, observed_at=datetime.now(UTC), write_canonical=lambda _: {"post_id": "stable"})
            return {"outcome": "committed", "stage": "comments"}
        finally:
            with lock:
                running.remove(reservation["owner_token"])

    admission = Admission(
        read_connection=database[2],
        environ={
            "SOCIAL_WORKER_POOL_COMMENTS": str(limit),
            "TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT": str(limit),
            "SOCIAL_CATALOG_RUN_IN_FLIGHT_CAP": str(limit),
        },
        fresh_session_probe=lambda **kw: {"available": True},
    )

    def admit(item):
        assert threading.get_ident() == owner
        return admission.admit(item)

    dispatch = LocalDispatch(execute, abort)
    scheduler = Scheduler(local, admit=admit, dispatch=dispatch, reconcile=lambda _: "unknown")
    summary = run_worker(
        store=local,
        scheduler=scheduler,
        dispatch=dispatch,
        lease=nullcontext(),
        abort=abort,
        workflow_version=WORKFLOW_VERSION,
        stop_requested=stop,
        poll_seconds=0.01,
        on_cycle=_stop_when_idle(abort, stop),
        effective_comments_limit=policy,
    )
    assert summary["status"] == "stopped" and max(peak) == limit and not running
    assert len(calls) == len(set(calls)) == limit * 2
    assert sql(database, "select attempts,outcome from social.instagram_ingestion_targets order by target_key") == [
        (1, "committed")
    ] * (limit * 2)
    assert not dispatch.active and dispatch.pool is None


def test_comment_registry_deduplicates_and_keeps_original_partition_for_expiry():
    abort, release, entered, reaped = (threading.Event() for _ in range(4))
    calls, transitions = [], []

    def execute(reservation, **kwargs):
        calls.append(reservation["owner_token"])
        entered.set()
        assert release.wait(5)
        reaped.set()
        return {"outcome": "unresolved"}

    class Store:
        def transition(self, item, token, state):
            assert reaped.is_set()
            transitions.append((item, token, state))

    store = Store()
    dispatch = LocalDispatch(execute, abort)
    first = {"execution_id": "partition-0", "generation": 1}
    reservation = {"owner_token": "same-token", "stage": "comments"}
    try:
        dispatch.bind(first, 2)
        dispatch(reservation)
        assert entered.wait(5)
        dispatch(reservation)
        dispatch.bind({"execution_id": "partition-1", "generation": 1}, 2)
        assert len(dispatch.active) == 1 and not transitions
        release.set()
        dispatch.drain(store, finish=True)
        dispatch(reservation)
        assert calls == ["same-token"]
        assert transitions == [(first, "same-token", "expired")]
    finally:
        release.set()
        dispatch.close(store)


@pytest.mark.parametrize("initial,lower", [(2, 1), (8, 6)])
def test_comment_registry_does_not_queue_and_lower_limit_drains_existing_children(initial, lower):
    abort, release = threading.Event(), threading.Event()

    def execute(*args, **kwargs):
        assert release.wait(5)
        return {"outcome": "committed"}

    dispatch = LocalDispatch(execute, abort)
    try:
        dispatch.bind({"generation": 1}, initial)
        tokens = {str(index) for index in range(initial)}
        for token in tokens:
            dispatch({"owner_token": token, "stage": "comments"})
        dispatch.bind({"generation": 1}, lower)
        with pytest.raises(RuntimeError, match="no available slot"):
            dispatch({"owner_token": "never-queued", "stage": "comments"})
        assert dispatch.submitted == tokens and not abort.is_set()
        release.set()
        dispatch.drain(object(), finish=True)
        assert not dispatch.active
    finally:
        release.set()
        dispatch.close(object())


def test_partition_continuation_keeps_live_comment_registry(monkeypatch):
    monkeypatch.setattr("scripts.ingestion.local_worker.PARTITION_SIZE", 1)
    abort, release, entered, reaped = (threading.Event() for _ in range(4))
    transitions = []

    def execute(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        reaped.set()
        return {"outcome": "unresolved"}

    class Store:
        def decision(self, *args):
            return {"kind": "wait", "seconds": 1}

        def transition(self, item, token, state):
            assert reaped.is_set()
            transitions.append((item["execution_id"], token, state))

        def complete_partition(self, *args, **kwargs):
            pass

    class Scheduler:
        clock = staticmethod(lambda: datetime.now(UTC))

        def tick(self, item, ordinal):
            if item["execution_id"] == "partition-0":
                dispatch({"owner_token": "continued", "stage": "comments"})
                assert entered.wait(5)
                return {"ready": True}
            assert "continued" in dispatch.active and not abort.is_set()
            release.set()
            return {"stop": "completed"}

    dispatch = LocalDispatch(execute, abort)
    store, scheduler = Store(), Scheduler()
    try:
        for number, expected in ((0, "continued"), (1, "stopped")):
            assert (
                run_partition(
                    {"execution_id": f"partition-{number}"},
                    store=store,
                    scheduler=scheduler,
                    dispatch=dispatch,
                    abort=abort,
                    effective_comments_limit=lambda _: 2,
                )
                == expected
            )
        assert transitions == [("partition-0", "continued", "expired")]
        assert not abort.is_set() and not dispatch.active
    finally:
        release.set()
        dispatch.close(store)


def test_policy_fence_aborts_and_reaps_comment_child_before_lease_release():
    abort, entered, reaped = (threading.Event() for _ in range(3))

    def execute(*args, **kwargs):
        entered.set()
        assert abort.wait(5)
        reaped.set()
        return {"outcome": "fenced"}

    class Store:
        execution_placement = "local"
        seed = None

        def dispatch_pending(self, *args, **kwargs):
            pass

        def dispatch_partitions(self, submit, **kwargs):
            return submit({"generation": 1})

        def transition(self, *args):
            assert reaped.is_set()
            raise LostOwnership("old generation")

    dispatch = LocalDispatch(execute, abort)
    dispatch.bind({"generation": 1}, 2)
    dispatch({"owner_token": "owned", "stage": "comments"})
    assert entered.wait(5)

    @contextmanager
    def lease():
        try:
            yield
        finally:
            assert reaped.is_set() and not dispatch.active

    def stale(item):
        raise LostOwnership("stale policy generation")

    with pytest.raises(LostOwnership, match="stale policy"):
        run_worker(
            store=Store(),
            scheduler=None,
            dispatch=dispatch,
            lease=lease(),
            abort=abort,
            workflow_version=WORKFLOW_VERSION,
            effective_comments_limit=stale,
        )


def test_phase_change_waits_for_comment_reaping_before_noncomment_reservation():
    abort, release, entered, reaped = (threading.Event() for _ in range(4))
    order = []

    def execute(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        order.append("reaped")
        reaped.set()
        return {"outcome": "committed"}

    class Store:
        def decision(self, *args):
            release.set()
            return {"kind": "target", "stage": "media"}

        def complete_partition(self, *args, **kwargs):
            order.append("complete")

    class Scheduler:
        clock = staticmethod(lambda: datetime.now(UTC))

        def tick(self, *args):
            assert reaped.is_set() and not dispatch.active
            order.append("noncomment-reservation")
            return {"stop": "completed"}

    dispatch = LocalDispatch(execute, abort)
    dispatch.bind({"generation": 1}, 2)
    dispatch({"owner_token": "comment", "stage": "comments"})
    assert entered.wait(5)
    try:
        assert (
            run_partition(
                {"generation": 1},
                store=Store(),
                scheduler=Scheduler(),
                dispatch=dispatch,
                abort=abort,
                effective_comments_limit=lambda _: 2,
            )
            == "stopped"
        )
        assert order == ["reaped", "noncomment-reservation", "complete"]
    finally:
        release.set()
        dispatch.close(Store())


def _commit(local, calls, abort):
    def execute(reservation, **kwargs):
        assert kwargs["execution_placement"] == "local" and kwargs["abort"] is abort
        args = owned(reservation)
        local.acquire(**args)
        local.check_owner(**args, record_request=True)
        calls.append(reservation["target_key"])
        local.commit_target(**args, observed_at=datetime.now(UTC), write_canonical=lambda _: {"post_id": "stable"})
        return {"outcome": "committed", "stage": "post_details"}

    return execute


def _stop_when_idle(abort, stop):
    def on_cycle(ran):
        if not ran:
            stop.set()
            abort.set()

    return on_cycle


def test_worker_runs_a_local_run_to_completion_inline(database):
    _, _, request = fixture_run(database)
    resume_local(database, request)
    abort, stop, calls = threading.Event(), threading.Event(), []
    summary = _worker(
        database, _commit(database[1], calls, abort), abort=abort, stop=stop, on_cycle=_stop_when_idle(abort, stop)
    )
    assert summary["status"] == "stopped" and summary["partitions"] >= 1
    assert calls == ["post1"]
    assert sql(database, "select count(*) from social.instagram_ingestion_receipts")[0][0] == 1
    assert sql(database, "select state from social.instagram_ingestion_runs") == [("completed",)]
    assert sql(database, "select count(*) from social.instagram_ingestion_partitions where completed_at is null") == [
        (0,)
    ]


def test_stop_mid_partition_keeps_it_pending_and_restart_replays_without_resending(database):
    _, _, request = fixture_run(database)
    resume_local(database, request)
    local = database[1]
    abort, stop, calls = threading.Event(), threading.Event(), []
    commit = _commit(local, calls, abort)

    def execute_then_stop(reservation, **kwargs):
        result = commit(reservation, **kwargs)
        stop.set()  # SIGTERM arrives while the partition is still open.
        abort.set()
        return result

    first = _worker(database, execute_then_stop, abort=abort, stop=stop)
    assert first == {"status": "stopped", "partitions": 0}
    # Not acknowledged and not completed: the next start must pick it up again.
    assert sql(
        database,
        """select count(*) from social.instagram_ingestion_partitions
        where acknowledged_at is null and completed_at is null""",
    ) == [(1,)]
    # Shutdown never cancels the run; its durable state waits for the next start.
    assert sql(database, "select state from social.instagram_ingestion_runs") == [("running",)]

    abort2, stop2 = threading.Event(), threading.Event()

    def must_not_resend(reservation, **kwargs):
        raise AssertionError("A committed target must not be sent again after restart")

    second = _worker(database, must_not_resend, abort=abort2, stop=stop2, on_cycle=_stop_when_idle(abort2, stop2))
    assert second["status"] == "stopped" and calls == ["post1"]
    assert sql(database, "select state from social.instagram_ingestion_runs") == [("completed",)]


def test_lease_loss_is_reported_separately_from_a_requested_stop(database):
    abort, stop = threading.Event(), threading.Event()
    abort.set()  # The lease monitor fired; nobody asked the worker to stop.
    summary = _worker(database, lambda *a, **k: None, abort=abort, stop=stop)
    assert summary == {"status": "lease_lost", "partitions": 0}


def test_worker_refuses_a_modal_store(database):
    with pytest.raises(ValueError, match="local-fenced"):
        run_worker(
            store=WorkflowStore(database[2]),
            scheduler=None,
            dispatch=None,
            lease=nullcontext(),
            abort=threading.Event(),
            workflow_version=WORKFLOW_VERSION,
        )


def test_api_reports_the_mac_online_only_while_the_worker_lease_is_held(database):
    tx = database[2]
    assert local_worker_online(tx) is False
    with tx() as conn, conn.cursor() as cur:
        # The Modal coordinator's own lease does not count as the Mac.
        cur.execute("select pg_advisory_lock(%s,%s)", (LOCAL_WORKER_LOCK_NAMESPACE, 20261016))
        assert local_worker_online(tx) is False
        cur.execute("select pg_advisory_lock(%s,%s)", (LOCAL_WORKER_LOCK_NAMESPACE, LOCAL_WORKER_LOCK_KEY))
        assert local_worker_online(tx) is True
        cur.execute("select pg_advisory_unlock_all()")
    assert local_worker_online(tx) is False


def _service(database, profile="public"):
    store, _, _ = database
    return CommandService(
        store,
        None,
        engine="dbos",
        workflow_version=WORKFLOW_VERSION,
        enabled=True,
        capacity=lambda account, payload: None,
        source_profile=SourceProfile.for_mode(profile),
    )


def test_local_launch_is_placed_in_the_creating_transaction(database):
    _, _, tx = database
    payload = InstagramIngestionLaunchV2(execution_placement="local", selected_tasks=["comments", "media"])
    _service(database).launch("bravotv", payload, idempotency_key="local-1", initiated_by="offline-test")
    assert sql(database, "select execution_placement from social.instagram_ingestion_runs") == [("local",)]
    seen_by_modal, seen_by_mac = [], []
    assert Store(tx).dispatch_pending(seen_by_modal.append) == 0
    assert WorkflowStore(tx, execution_placement="local").dispatch_pending(seen_by_mac.append) == 1
    assert not seen_by_modal and len(seen_by_mac) == 1


def test_local_launch_requires_the_public_profile(database):
    payload = InstagramIngestionLaunchV2(execution_placement="local")
    with pytest.raises(Conflict, match="public"):
        _service(database, "authenticated").launch(
            "bravotv", payload, idempotency_key="local-2", initiated_by="offline-test"
        )
    assert sql(database, "select count(*) from social.instagram_ingestion_runs") == [(0,)]


def test_explicit_authenticated_relay_local_launch_freezes_family_and_placement(database):
    service = _service(database)
    service.source_profile = SourceProfile.for_authenticated_relay()
    payload = InstagramIngestionLaunchV2(
        execution_placement="local",
        backfill_scope="bounded_window",
        date_start="2026-01-01T00:00:00Z",
        date_end="2026-02-01T00:00:00Z",
        detail_worker_count=1,
        comments_worker_count=1,
        comments_enable_media_followups=False,
    )
    service.launch("bravotv", payload, idempotency_key="local-www", initiated_by="offline-test")
    saved = sql(database, "select execution_placement,intent from social.instagram_ingestion_runs")[0]
    assert saved[0] == "local"
    assert saved[1]["source_profile"]["comments_family"] == "instagram_authenticated_www_relay"
    assert saved[1]["date_start"].startswith("2026-01-01")
    assert saved[1]["comments_enable_media_followups"] is False
    assert Store(database[2]).dispatch_pending(lambda _: pytest.fail("Modal claimed local work")) == 0


def test_default_modal_launch_keeps_its_historical_command_fingerprint(database):
    payload = InstagramIngestionLaunchV2(selected_tasks=["comments", "media"])
    _service(database).launch("bravotv", payload, idempotency_key="modal-1", initiated_by="offline-test")
    legacy = payload.model_dump(mode="json")
    legacy.pop("execution_placement")
    assert sql(database, "select command_hash from social.instagram_ingestion_commands") == [
        (fingerprint({"command": "start", "intent": legacy}),)
    ]
    assert sql(database, "select execution_placement from social.instagram_ingestion_runs") == [("modal",)]


def test_capabilities_report_the_mac_status_only_when_routing_is_ready(database):
    service = _service(database)
    service.local_worker_status = lambda: True
    assert service.capabilities("bravotv").local_worker_online is True
    service.local_worker_status = lambda: (_ for _ in ()).throw(RuntimeError("db down"))
    assert service.capabilities("bravotv").local_worker_online is None
    service.enabled = False
    service.local_worker_status = lambda: True
    assert service.capabilities("bravotv").local_worker_online is None
