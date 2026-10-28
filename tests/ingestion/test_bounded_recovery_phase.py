"""Phase journaling and drain fences in fixture-owned isolated PostgreSQL."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_bounded_recovery_window as window_fixture
from tests.ingestion import test_execution_policy as policy_fixture
from trr_backend.ingestion.bounded_recovery import USEFUL_PROGRESS_POLICY
from trr_backend.ingestion.bounded_recovery_phase import phase, transition
from trr_backend.ingestion.domain import Conflict, LostOwnership, Stage, stable_json
from trr_backend.ingestion.workflow_store import WorkflowStore

base_database = window_fixture.base_database
policy_database = window_fixture.policy_database
database = window_fixture.database
held = window_fixture.held


@pytest.fixture
def authorized(held):
    store, tx, args, monkeypatch = held
    window = store.resume_bounded_recovery_window(**args, stop_policy=USEFUL_PROGRESS_POLICY)
    for name, value in {
        "TRR_INSTAGRAM_RECOVERY_WINDOW_ID": window["window_id"],
        "TRR_INSTAGRAM_LOCAL_WORKER_RUN_ID": args["run_id"],
        "TRR_INSTAGRAM_LOCAL_WORKER_GENERATION": str(window["generation"]),
        "TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION": args["source_revision"],
    }.items():
        monkeypatch.setenv(name, value)
    request = {
        "run_id": args["run_id"],
        "account": "bravotv",
        "generation": window["generation"],
        "intent_hash": args["expected_intent_hash"],
        "window_id": window["window_id"],
        "source_revision": args["source_revision"],
    }
    try:
        yield store, tx, window, request
    finally:
        # The imported module-scoped cluster is shared across test cases. Drain
        # only this fixture's deliberately fabricated owners before the next grant.
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """update social.instagram_ingestion_targets set owner_token=null,
                    owner_started_at=null,owner_expires_at=null where run_id=%s""",
                (request["run_id"],),
            )
            cur.execute(
                "update social.instagram_ingestion_dispatches set state='expired' where run_id=%s",
                (request["run_id"],),
            )


def current_phase(authorized):
    with authorized[1]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        return phase(cur, authorized[2])


def drain(authorized, **changes):
    return transition(
        authorized[0], **{**authorized[3], "to_phase": "draining_comments", "reason": "comments_stalled", **changes}
    )


def media(authorized, **changes):
    return transition(authorized[0], **{**authorized[3], "to_phase": "media", "reason": "comments_drained", **changes})


def set_owner(authorized, *, expiry):
    token = str(uuid4())
    with authorized[1]() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set owner_token=%s,
                owner_expires_at=case %s when 'live' then clock_timestamp()+interval '1 hour'
                    when 'expired' then clock_timestamp()-interval '1 hour' else null end
                where run_id=%s and stage='comments'""",
            (token, expiry, authorized[3]["run_id"]),
        )
    return token


def add_dispatch(authorized, *, state="accepted", generation=None, token=None):
    token = token or str(uuid4())
    with authorized[1]() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_dispatches
                (owner_token,run_id,generation,stage,target_key,state) values(%s,%s,%s,'comments','post1',%s)""",
            (token, authorized[3]["run_id"], generation or authorized[3]["generation"], state),
        )
    return token


def business_state(authorized):
    store, tx, _, request = authorized
    state = policy_fixture.business_snapshot((None, store, tx), request["run_id"])
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select to_jsonb(d) from social.instagram_ingestion_dispatches d where run_id=%s order by owner_token",
            (request["run_id"],),
        )
        state["dispatches"] = cur.fetchall()
    return state


def test_ordered_immutable_journal_preserves_saved_data_holds_and_all_business_state(authorized):
    with authorized[1]() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set checkpoint=%s::jsonb,
                blocked_reason='cursor_stalled',next_attempt_at=clock_timestamp()+interval '1 hour'
                where run_id=%s and stage='comments'""",
            (
                stable_json({"saved": ["retained-id"], "public_reply_failures": {"parent": {"technical_hold": True}}}),
                authorized[3]["run_id"],
            ),
        )
    before = business_state(authorized)
    assert current_phase(authorized) == "comments"
    drained = drain(authorized)
    assert drained["phase"] == "draining_comments" and drained["previous_phase"] == "comments"
    assert current_phase(authorized) == "draining_comments"
    advanced = media(authorized)
    assert advanced["phase"] == "media" and advanced["previous_phase"] == "draining_comments"
    assert current_phase(authorized) == "media"
    assert business_state(authorized) == before
    with authorized[1]() as conn, conn.cursor() as cur:
        cur.execute(
            """select idempotency_key from social.instagram_ingestion_commands
                where account='bravotv' and idempotency_key in (%s,%s) order by idempotency_key""",
            (authorized[2]["window_id"] + ":drain-comments", authorized[2]["window_id"] + ":media"),
        )
        assert len(cur.fetchall()) == 2


def test_media_cannot_skip_comment_draining(authorized):
    with pytest.raises(Conflict, match="out of order"):
        media(authorized)
    assert current_phase(authorized) == "comments"


@pytest.mark.parametrize("expiry", ["live", "unknown"])
def test_media_refuses_unresolved_live_or_null_expiry_owner(authorized, expiry):
    drain(authorized)
    set_owner(authorized, expiry=expiry)
    before = business_state(authorized)
    with pytest.raises(Conflict, match="live or uncertain"):
        media(authorized)
    assert current_phase(authorized) == "draining_comments"
    assert business_state(authorized) == before


@pytest.mark.parametrize("state", ["sending", "accepted"])
def test_expired_owner_does_not_clear_ambiguous_current_generation_dispatch(authorized, state):
    drain(authorized)
    token = set_owner(authorized, expiry="expired")
    add_dispatch(authorized, state=state, token=token)
    before = business_state(authorized)
    with pytest.raises(Conflict, match="live or uncertain"):
        media(authorized)
    assert business_state(authorized) == before


@pytest.mark.parametrize("kind", ["expired", "reserved", "older_generation", "receipt"])
def test_media_accepts_only_proved_drained_dispatch_cases(authorized, kind):
    drain(authorized)
    token = add_dispatch(
        authorized,
        state=kind if kind in {"expired", "reserved"} else "accepted",
        generation=authorized[3]["generation"] - 1 if kind == "older_generation" else None,
    )
    if kind == "receipt":
        with authorized[1]() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into social.instagram_ingestion_receipts
                    (receipt_id,run_id,stage,target_key,owner_token,owner_generation,observed_at,outcome,canonical_identity)
                    values(%s,%s,'comments','post1',%s,1,clock_timestamp(),'cached_satisfied','{}'::jsonb)""",
                (str(uuid4()), authorized[3]["run_id"], token),
            )
    before = business_state(authorized)
    assert media(authorized)["phase"] == "media"
    assert business_state(authorized) == before


def test_replay_is_exact_before_new_mutable_owners_and_never_reopens_phase(authorized):
    first = drain(authorized)
    second = media(authorized)
    set_owner(authorized, expiry="live")
    add_dispatch(authorized)
    before = business_state(authorized)
    assert drain(authorized) == first
    assert media(authorized) == second
    assert current_phase(authorized) == "media"
    assert business_state(authorized) == before
    with pytest.raises(Conflict, match="another request"):
        drain(authorized, reason="comments_blocked")


@pytest.mark.parametrize("changed", [{"generation": 4}, {"intent_hash": "0" * 64}, {"source_revision": "wrong"}])
def test_mismatched_current_window_request_is_rejected_without_mutations(authorized, changed):
    before = business_state(authorized)
    with pytest.raises(LostOwnership):
        drain(authorized, **changed)
    assert current_phase(authorized) == "comments"
    assert business_state(authorized) == before


def test_replay_still_requires_current_window_env_and_unexpired_binding(authorized, monkeypatch):
    drain(authorized)
    with monkeypatch.context() as patch:
        patch.setenv("TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION", "wrong")
        with pytest.raises(LostOwnership):
            drain(authorized)
    window_fixture.shorten((authorized[0], authorized[1], authorized[3]), authorized[2], -1)
    with pytest.raises(LostOwnership, match="expired"):
        drain(authorized)


def test_legacy_window_cannot_use_phase_transitions(held):
    window = window_fixture.authorize(held)
    with held[1]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        assert phase(cur, window) == "comments"
    with pytest.raises(Conflict, match="useful-progress"):
        transition(
            held[0],
            run_id=held[2]["run_id"],
            account="bravotv",
            generation=window["generation"],
            intent_hash=held[2]["expected_intent_hash"],
            window_id=window["window_id"],
            source_revision=held[2]["source_revision"],
            to_phase="draining_comments",
            reason="comments_stalled",
        )


def test_workflow_and_direct_reservations_fence_old_comments_and_enter_media(authorized, monkeypatch):
    store, tx, _, request = authorized
    workflow = WorkflowStore(tx, execution_placement="local")
    item = workflow.seed(
        {
            **{key: request[key] for key in ("run_id", "account", "generation")},
            "engine": "dbos",
            "workflow_version": "window-test",
        }
    )
    # The held fixture discovers comments/media. Seal its empty upstream
    # manifests so the actual scheduler can choose the runnable comment target.
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_manifests set discovery_complete=true,
                sealed_at=clock_timestamp() where run_id=%s and not discovery_complete""",
            (request["run_id"],),
        )
    old = workflow.decision(item, 0, datetime.now(UTC))
    assert old == {"kind": "target", "stage": "comments", "target_key": "post1"}
    reserve = {key: request[key] for key in ("run_id", "account", "generation")}
    drain(authorized)
    assert workflow.reserve_decision(item, old, ordinal=0) is None
    assert store.reserve(**reserve, stage=Stage.COMMENTS, target_key="post1") is None
    assert workflow.decision(item, 1, datetime.now(UTC))["kind"] == "wait"

    # No owner was created by the rejected stale decision, so the worker's
    # idle-drain transition can advance without settling the ready comment.
    assert media(authorized)["phase"] == "media"
    media_decision = {"kind": "target", "stage": "media", "target_key": "post1"}
    proof_calls = []

    def proved_media(cur, run, candidate, now, *, advance_cursor=False, full_cycle=False):
        proof_calls.append((run["_bounded_window"]["phase"], candidate["execution_id"], advance_cursor, full_cycle))
        return media_decision

    monkeypatch.setattr(WorkflowStore, "_eligible_media", staticmethod(proved_media))
    assert workflow.decision(item, 2, datetime.now(UTC)) == media_decision
    assert proof_calls == [("media", item["execution_id"], True, True)]  # pyright: ignore[reportArgumentType]
    assert workflow.reserve_decision(item, old, ordinal=0) is None
    assert store.reserve(**reserve, stage=Stage.COMMENTS, target_key="post1") is None
    assert store.reserve(**reserve, stage=Stage.MEDIA, target_key="post1") is not None
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select outcome,owner_token,blocked_reason,attempts from social.instagram_ingestion_targets
                where run_id=%s and stage='comments' and target_key='post1'""",
            (request["run_id"],),
        )
        assert cur.fetchone() == ("unresolved", None, None, 0)


def test_media_cooldown_waits_then_resumes_proved_target_without_finishing_phase(authorized, monkeypatch):
    _, tx, _, request = authorized
    workflow = WorkflowStore(tx, execution_placement="local")
    item = workflow.seed(
        {
            **{key: request[key] for key in ("run_id", "account", "generation")},
            "engine": "dbos",
            "workflow_version": "window-test",
        }
    )
    drain(authorized)
    media(authorized)
    now = datetime.now(UTC)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set next_attempt_at=%s
                where run_id=%s and stage='media' and target_key='post1'""",
            (now + timedelta(seconds=30), request["run_id"]),
        )
    monkeypatch.setattr(WorkflowStore, "_eligible_media", staticmethod(lambda *_args, **_kwargs: None))
    waiting = workflow.decision(item, 0, now)
    assert waiting["kind"] == "wait"
    assert waiting["seconds"] == pytest.approx(30, abs=0.1)
    assert current_phase(authorized) == "media"
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set next_attempt_at=%s
                where run_id=%s and stage='media' and target_key='post1'""",
            (now - timedelta(seconds=1), request["run_id"]),
        )
        cur.execute(
            """select outcome,owner_token,blocked_reason,next_attempt_at from social.instagram_ingestion_targets
                where run_id=%s and stage='comments' and target_key='post1'""",
            (request["run_id"],),
        )
        assert cur.fetchone() == ("unresolved", None, None, None)
    proved = {"kind": "target", "stage": "media", "target_key": "post1"}
    monkeypatch.setattr(WorkflowStore, "_eligible_media", staticmethod(lambda *_args, **_kwargs: proved))
    resumed = workflow.decision(item, 1, datetime.now(UTC))
    assert resumed == proved
    assert workflow.reserve_decision(item, resumed, ordinal=1) is not None
