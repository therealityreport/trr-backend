"""Real transactional checks for explicit workflow commands; no provider calls."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.ingestion.test_persistence import database as database_fixture
from tests.ingestion.test_persistence import launch, target
from trr_backend.ingestion.domain import Failure, LostOwnership, Stage

database = database_fixture


def test_checkpoint_retry_budget_and_late_writer(database):
    store, tx = database
    run = launch(store)
    args = target(store, run)
    for _attempt in range(3):
        reserved = store.reserve(**args)
        owned = dict(args, owner_token=str(reserved["owner_token"]))
        store.acquire(**owned)
        retry_at = datetime.now(UTC) + timedelta(minutes=1)
        store.checkpoint_failure(
            **owned, failure=Failure("RATE_LIMITED", retry_at=retry_at), checkpoint={"cursor": "retained"}, requests=2
        )
        assert store.reserve(**args) is None
        with pytest.raises(LostOwnership):
            store.checkpoint_failure(**owned, failure=Failure("RATE_LIMITED"), checkpoint={}, requests=2)
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """update social.instagram_ingestion_targets set next_attempt_at=clock_timestamp()
                where run_id=%s""",
                (run["run_id"],),
            )
    assert store.reserve(**args) is None
    assert store.reconcile_completion(run["run_id"], "bravotv", generation=1) == "blocked"
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select attempts,requests,checkpoint from social.instagram_ingestion_targets
            where run_id=%s""",
            (run["run_id"],),
        )
        assert cur.fetchone() == (3, 6, {"cursor": "retained"})


def test_completion_requires_every_manifest_sealed_and_every_target_settled(database):
    store, _ = database
    run = launch(store)
    args = target(store, run)
    reserved = store.reserve(**args)
    owned = dict(args, owner_token=str(reserved["owner_token"]))
    store.acquire(**owned)
    assert store.reconcile_completion(run["run_id"], "bravotv", generation=1) == "running"
    store.commit_target(**owned, observed_at=datetime.now(UTC), write_canonical=lambda conn: {"id": "post1"})
    assert store.reconcile_completion(run["run_id"], "bravotv", generation=1) == "running"
    for stage in (Stage.LISTING, Stage.COMMENTS, Stage.MEDIA):
        store.discover(
            run["run_id"],
            "bravotv",
            generation=1,
            stage=stage,
            target_keys=[],
            max_attempts=2,
            cursor_state={},
            expected_cursor_state={},
            page_id="empty",
            seal=True,
        )
    assert store.reconcile_completion(run["run_id"], "bravotv", generation=1) == "completed"


def test_outbox_redelivery_after_submit_crash_uses_same_identity(database):
    store, tx = database
    # Other tests use the shared fixture; acknowledge their unrelated intents.
    with tx() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_ingestion_outbox set acknowledged_at=clock_timestamp()")
    run = launch(store)
    received = set()

    def crash_after_submit(item):
        received.add(item["execution_id"])
        raise RuntimeError("simulated process loss after accepted submission")

    with pytest.raises(RuntimeError, match="simulated"):
        store.dispatch_pending(crash_after_submit)
    assert store.dispatch_pending(lambda item: received.add(item["execution_id"])) == 1
    assert received == {run["execution_id"]}
    assert store.dispatch_pending(lambda item: pytest.fail("Acknowledged work was redelivered")) == 0
    cancelled = launch(store)
    store.control(cancelled["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    assert store.dispatch_pending(lambda item: pytest.fail("Cancelled generation was dispatched")) == 0
