"""Expired side-lane sends release normal admission through exact-token reconciliation."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_media_snapshot as fixtures
from trr_backend.ingestion.admission import Admission
from trr_backend.ingestion.domain import Stage
from trr_backend.ingestion.workflow_store import WorkflowStore
from trr_backend.ingestion.workflows import Scheduler

database = fixtures.database
media_database = fixtures.media_database
snapshot_database = fixtures.snapshot_database
snapshot_workflow_database = fixtures.snapshot_workflow_database


def setup(database, *, proof=True):
    owned, _, _, _ = fixtures.seed(database, selected_comments=True, proof=proof)
    _, tx = database
    store = WorkflowStore(tx)
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts) "
            "values(%s,'comments','pending',3)",
            (owned["run_id"],),
        )
        cur.execute(
            "update social.instagram_ingestion_manifests set "
            "discovery_complete=true,sealed_at=clock_timestamp() where run_id=%s and stage='listing'",
            (owned["run_id"],),
        )
        cur.execute("select * from social.instagram_ingestion_runs where run_id=%s", (owned["run_id"],))
        run = dict(cur.fetchone())
    item = store.seed(run)
    decision = {"kind": "target", "stage": "media", "target_key": owned["target_key"]}
    reservation = store.reserve_decision(item, decision)
    assert reservation is not None
    token = str(reservation["owner_token"])
    store.transition(item, token, "sending")
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set "
            "owner_expires_at=clock_timestamp()-interval '1 second' where run_id=%s and stage='media'"
            " and target_key=%s",
            (owned["run_id"], owned["target_key"]),
        )
    return owned, tx, store, dict(item, prefer_media=True), token  # pyright: ignore[reportArgumentType, reportCallIssue]


def test_actual_decision_scheduler_reconciliation_reopens_admission_with_pending_comments(snapshot_workflow_database):
    owned, tx, store, item, token = setup(snapshot_workflow_database)
    events = []
    policy = Admission(environ={})

    def admits(decision):
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "select count(*) from social.instagram_ingestion_dispatches where run_id=%s and "
                "stage='media' and state='sending'",
                (owned["run_id"],),
            )
            count = cur.fetchone()[0]
        counts = (
            [
                {
                    "stage": "media_mirror",
                    "total": count,
                    "platform": "instagram",
                    "account": owned["account"],
                    "run_id": owned["run_id"],
                }
            ]
            if count
            else []
        )
        return policy._fits(
            {"counts": counts, "blocked": False}, "media_mirror", owned["account"], run_id=owned["run_id"]
        )

    assert not admits({})

    def dispatch(reservation):
        events.append("dispatch")
        args = {
            key: reservation[key] for key in ("run_id", "account", "generation", "stage", "target_key", "owner_token")
        }
        args["stage"] = Stage(args["stage"])
        store.acquire(**args)
        store.commit_target(
            **args, observed_at=datetime.now(UTC), write_canonical=lambda _: {"id": owned["target_key"]}
        )
        return "fixture"

    def admit(decision):
        events.append("admit")
        return admits(decision)

    scheduler = Scheduler(
        store,
        admit=admit,
        dispatch=dispatch,
        reconcile=lambda _: pytest.fail("expired does not need remote reconciliation"),
    )
    assert store.decision(item, 0, datetime.now(UTC)) == {
        "kind": "reconcile",
        "stage": "media",
        "target_key": owned["target_key"],
        "owner_token": token,
    }
    assert scheduler.tick(item, 0)["sleep"] == 0.01
    assert events == [] and admits({})
    scheduler.tick(item, 1)
    assert events == ["admit", "dispatch"]
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select state from social.instagram_ingestion_dispatches where owner_token=%s", (token,))
        assert cur.fetchone()[0] == "expired"
        cur.execute(
            "select outcome,attempts from social.instagram_ingestion_targets where run_id=%s and "
            "stage='media' and target_key=%s",
            (owned["run_id"], owned["target_key"]),
        )
        assert cur.fetchone() == ("committed", 1)
        cur.execute(
            "select outcome from social.instagram_ingestion_targets where run_id=%s and "
            "stage='comments' and target_key='pending'",
            (owned["run_id"],),
        )
        assert cur.fetchone()[0] == "unresolved"


@pytest.mark.parametrize("case", ["future", "generation", "token", "selection", "receipt"])
def test_noncurrent_nonselected_receipted_and_live_sends_are_not_reconciled(snapshot_workflow_database, case):
    owned, tx, store, item, token = setup(snapshot_workflow_database)
    with tx() as conn, conn.cursor() as cur:
        if case == "future":
            cur.execute(
                "update social.instagram_ingestion_targets set "
                "owner_expires_at=clock_timestamp()+interval '5 minutes' where run_id=%s and "
                "stage='media'",
                (owned["run_id"],),
            )
        elif case == "generation":
            cur.execute("update social.instagram_ingestion_dispatches set generation=99 where owner_token=%s", (token,))
        elif case == "token":
            cur.execute(
                "update social.instagram_ingestion_targets set owner_token=%s where run_id=%s and stage='media'",
                (str(uuid4()), owned["run_id"]),
            )
        elif case == "selection":
            cur.execute(
                "update social.instagram_ingestion_runs set execution_selection=%s::jsonb where run_id=%s",
                ('{"stage":"media","target_keys":["another"]}', owned["run_id"]),
            )
        else:
            cur.execute(
                "insert into "
                "social.instagram_ingestion_receipts(receipt_id,run_id,stage,target_key,owner_token,owner_generation,observed_at,outcome,canonical_identity)"
                " select "
                "%s,run_id,stage,target_key,owner_token,owner_generation,clock_timestamp(),'committed','{}'::jsonb"
                " from social.instagram_ingestion_targets where run_id=%s and stage='media'",
                (str(uuid4()), owned["run_id"]),
            )
    # Direct maintained eligibility avoids unrelated selection-count guards.
    result = store.eligible_media(item, datetime.now(UTC))
    assert result is None or result.get("kind") != "reconcile"
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select state from social.instagram_ingestion_dispatches where owner_token=%s", (token,))
        assert cur.fetchone()[0] == "sending"


def test_expired_send_reconciles_even_without_readiness_proof(snapshot_workflow_database):
    owned, _, store, item, token = setup(snapshot_workflow_database, proof=False)
    result = store.decision(item, 0, datetime.now(UTC))
    assert result == {"kind": "reconcile", "stage": "media", "target_key": owned["target_key"], "owner_token": token}
