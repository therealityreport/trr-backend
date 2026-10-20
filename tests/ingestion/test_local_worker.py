"""Durable Mac worker: disposable PG, stubbed activity, no live services."""

import threading
from contextlib import nullcontext
from datetime import UTC, datetime

import pytest

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from scripts.ingestion.local_worker import LocalDispatch, run_worker
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
from trr_backend.ingestion.domain import Conflict, SourceProfile, fingerprint
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.workflow_store import WorkflowStore
from trr_backend.ingestion.workflows import Scheduler

# Reuse the local-placement disposable database and its per-test truncation.
base_database = local_execution.base_database
database = local_execution.database
clean = local_execution.clean


def _worker(database, execute, *, abort, stop, on_cycle=None):
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
    )


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
