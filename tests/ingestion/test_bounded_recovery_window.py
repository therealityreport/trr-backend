"""Exact authorization, expiry, priority and preservation in isolated PostgreSQL."""

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_execution_policy as policy_fixture
from trr_backend.ingestion.domain import Conflict, LostOwnership, RunIntent, SourceProfile, Stage

base_database = policy_fixture.base_database
policy_database = policy_fixture.database


@pytest.fixture(scope="module")
def database(policy_database):
    from pathlib import Path

    with policy_database[2]() as conn, conn.cursor() as cur:
        cur.execute(
            (
                Path(__file__).resolve().parents[2]
                / "supabase/migrations/20261016070100_instagram_ingestion_workflows.sql"
            ).read_text()
        )
    return policy_database


@pytest.fixture
def held(database, monkeypatch):
    _, store, tx = database
    intent = RunIntent(
        account="bravotv",
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
        source_profile=SourceProfile.for_mode("public"),
    )
    run = store.start(
        intent, idempotency_key=str(uuid4()), engine="dbos", workflow_version="window-test", initiated_by="test"
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set execution_placement='local' where run_id=%s", (run["run_id"],)
        )
    for stage in (Stage.COMMENTS, Stage.MEDIA):
        store.discover(
            run["run_id"],
            "bravotv",
            generation=1,
            stage=stage,
            target_keys=["post1"],
            max_attempts=3,
            cursor_state={},
            page_id=str(uuid4()),
            expected_cursor_state={},
            seal=True,
        )
    cancelled = store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    store.hold_cancelled_run(
        run["run_id"], "bravotv", cancelled["generation"], "bounded_recovery_window_expired", str(uuid4()), "test"
    )
    args = {
        "run_id": run["run_id"],
        "account": "bravotv",
        "expected_generation": 2,
        "expected_intent_hash": intent.digest,
        "expected_workflow_version": "window-test",
        "source_revision": "qualified-fixture",
        "initiated_by": "test",
    }
    yield store, tx, args, monkeypatch
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_runs set state='cancelled',
                execution_hold_reason='bounded_recovery_window_expired' where run_id=%s""",
            (run["run_id"],),
        )


def authorize(held):
    store, _, args, monkeypatch = held
    window = store.resume_bounded_recovery_window(**args)
    for key, value in {
        "TRR_INSTAGRAM_RECOVERY_WINDOW_ID": window["window_id"],
        "TRR_INSTAGRAM_LOCAL_WORKER_RUN_ID": args["run_id"],
        "TRR_INSTAGRAM_LOCAL_WORKER_GENERATION": str(window["generation"]),
        "TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION": args["source_revision"],
    }.items():
        monkeypatch.setenv(key, value)
    return window


def targets(held):
    with held[1]() as conn, conn.cursor() as cur:
        cur.execute(
            """select (to_jsonb(t)-'owner_token'-'owner_started_at'-'owner_expires_at')
                from social.instagram_ingestion_targets t where run_id=%s order by stage""",
            (held[2]["run_id"],),
        )
        return cur.fetchall()


def shorten(held, window, seconds):
    deadline = datetime.now(UTC) + timedelta(seconds=seconds)
    with held[1]() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_commands set result=jsonb_set(result,'{deadline_at}',
                to_jsonb(%s::text)) where account='bravotv' and idempotency_key=%s""",
            (deadline.isoformat(), window["window_id"]),
        )
    return deadline


def test_exact_grant_preserves_business_history_and_replay_never_extends(held):
    before = targets(held)
    window = authorize(held)
    assert window["generation"] == 3 and window["priority"] == "comments_first"
    assert datetime.fromisoformat(window["deadline_at"]) - datetime.fromisoformat(window["started_at"]) == timedelta(
        hours=1
    )
    assert targets(held) == before
    assert held[0].resume_bounded_recovery_window(**held[2]) == window
    with pytest.raises(Conflict, match="another authorization"):
        held[0].resume_bounded_recovery_window(**dict(held[2], source_revision="another-source"))


@pytest.mark.parametrize(
    "changed", [{"expected_generation": 1}, {"expected_intent_hash": "0" * 64}, {"expected_workflow_version": "wrong"}]
)
def test_wrong_frozen_binding_cannot_release_hold(held, changed):
    before = targets(held)
    with pytest.raises(Conflict):
        held[0].resume_bounded_recovery_window(**dict(held[2], **changed))
    assert targets(held) == before


def test_live_owner_prevents_grant(held):
    with held[1]() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set owner_token=%s,
                owner_expires_at=clock_timestamp()+interval '1 minute' where run_id=%s and stage='comments'""",
            (str(uuid4()), held[2]["run_id"]),
        )
    with pytest.raises(Conflict, match="owner"):
        authorize(held)


@pytest.mark.parametrize("active", [False, True])
def test_capacity_uses_execution_not_cancelled_history_and_preserves_other_runs(held, active):
    store, tx, _, _ = held
    other = store.start(
        RunIntent(account="bravotv"),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="historical-fixture",
        initiated_by="test",
    )
    if not active:
        store.control(other["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select to_jsonb(r) from social.instagram_ingestion_runs r where run_id=%s", (other["run_id"],))
        before = cur.fetchone()
    try:
        if active:
            with pytest.raises(Conflict, match="active or uncertain"):
                authorize(held)
        else:
            assert authorize(held)["generation"] == 3
        with tx() as conn, conn.cursor() as cur:
            cur.execute("select to_jsonb(r) from social.instagram_ingestion_runs r where run_id=%s", (other["run_id"],))
            assert cur.fetchone() == before
    finally:
        if active:
            store.control(other["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))


def test_unbound_or_wrong_worker_cannot_consume_authorized_generation(held):
    window = authorize(held)
    for variable in (
        "TRR_INSTAGRAM_RECOVERY_WINDOW_ID",
        "TRR_INSTAGRAM_LOCAL_WORKER_GENERATION",
        "TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION",
    ):
        with held[3].context() as patch:
            patch.setenv(variable, "wrong")
            with held[1]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur, pytest.raises(LostOwnership):
                held[0]._execution_run(cur, held[2]["run_id"], "bravotv")
    assert window["generation"] == 3


def test_expiry_denies_execution_but_preserves_cancel_and_hold(held):
    window = authorize(held)
    shorten(held, window, -1)
    with (
        held[1]() as conn,
        conn.cursor(cursor_factory=RealDictCursor) as cur,
        pytest.raises(LostOwnership, match="expired"),
    ):
        held[0]._execution_run(cur, held[2]["run_id"], "bravotv")
    cancelled = held[0].control(
        held[2]["run_id"], "bravotv", action="cancel", expected_generation=3, idempotency_key=str(uuid4())
    )
    assert cancelled["generation"] == 4
    held[0].hold_cancelled_run(held[2]["run_id"], "bravotv", 4, "bounded_recovery_window_expired", str(uuid4()), "test")


def test_reservation_is_capped_and_media_cannot_overtake_ready_comments(held):
    window = authorize(held)
    deadline = shorten(held, window, 3)
    args = {"run_id": held[2]["run_id"], "account": "bravotv", "generation": 3, "target_key": "post1"}
    assert held[0].reserve(**args, stage=Stage.MEDIA) is None
    reservation = held[0].reserve(**args, stage=Stage.COMMENTS)
    assert reservation["owner_expires_at"] <= deadline
    assert held[0].reserve(**args, stage=Stage.MEDIA) is not None


def test_admission_target_lock_wait_cannot_cross_window_deadline(held):
    window = authorize(held)
    owned = {
        "run_id": held[2]["run_id"],
        "account": "bravotv",
        "generation": 3,
        "stage": Stage.COMMENTS,
        "target_key": "post1",
    }
    reservation = held[0].reserve(**owned)
    owned["owner_token"] = str(reservation["owner_token"])
    held[0].acquire(**owned)
    with held[1]() as conn, conn.cursor() as cur:
        cur.execute("create table if not exists social.test_window_lane(identity_key text primary key,n int)")
        cur.execute("insert into social.test_window_lane values('lane',0) on conflict(identity_key) do update set n=0")
    shorten(held, window, 0.5)
    from threading import Event

    locked, release = Event(), Event()

    def lock_target():
        with held[1]() as conn, conn.cursor() as cur:
            cur.execute(
                "select * from social.instagram_ingestion_targets where run_id=%s and stage='comments' for update",
                (held[2]["run_id"],),
            )
            locked.set()
            release.wait(3)

    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(lock_target)
        assert locked.wait(1)

        def admission():
            with held[1]() as conn:
                held[0].finalize_request_admission(
                    connection=conn,
                    lane_update="update social.test_window_lane set n=n+1 where identity_key=%s",
                    lane_params=("lane",),
                    **owned,
                )

        admission_future = pool.submit(admission)
        time.sleep(0.7)
        release.set()
        future.result()
        with pytest.raises(LostOwnership):
            admission_future.result()
    with held[1]() as conn, conn.cursor() as cur:
        cur.execute(
            "select requests from social.instagram_ingestion_targets where run_id=%s and stage='comments'",
            (held[2]["run_id"],),
        )
        assert cur.fetchone()[0] == 0
        cur.execute("select n from social.test_window_lane")
        assert cur.fetchone()[0] == 0
