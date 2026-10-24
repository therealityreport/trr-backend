"""Recovery journal fences and preservation, using isolated PostgreSQL."""

from uuid import uuid4

import httpx
import pytest

from tests.ingestion import test_comments as fixtures
from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.comment_recovery import _known_max, _strength, recover_public_target, target_status
from trr_backend.ingestion.comments import CommentActivity
from trr_backend.ingestion.domain import Conflict, Stage, fingerprint

database = fixtures.database


def completed(database, monkeypatch):
    store, tx, args = public.setup(database, bounded=True, seed_missing=True)

    def handler(request):
        return (
            public.bootstrap()
            if request.method == "GET"
            else httpx.Response(200, json=public.page([public.node("p", 0)]))
        )

    fetch, _ = public.source(monkeypatch, handler, expected=2)
    result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    assert result["complete"] and result["checkpoint"]["comment_pages"]["settlement"]["count_gap"] == 1
    return store, tx, args, fetch


def command(tx, args):
    checkpoint = fixtures.query(
        tx,
        "select checkpoint from social.instagram_ingestion_targets where run_id=%s and stage='comments'",
        (args["run_id"],),
    )[0]["checkpoint"]
    return {
        "run_id": args["run_id"],
        "account": "bravotv",
        "generation": 1,
        "target_key": args["target_key"],
        "expected_checkpoint_hash": fingerprint(checkpoint),
        "reason": "retained source gap",
        "source_revision": "fixture",
        "initiated_by": "fixture",
        "idempotency_key": str(uuid4()),
    }


def test_recovery_preserves_progress_journals_and_repeats_idempotently(database, monkeypatch):
    store, tx, args, fetch = completed(database, monkeypatch)
    before = fixtures.query(
        tx, "select count(*) n from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
    )[0]["n"]
    request = command(tx, args)
    result = recover_public_target(store, **request)
    assert result["previous_receipts"] and result["previous_outcome"] == "committed"
    assert recover_public_target(store, **request) == result
    with pytest.raises(Conflict, match="another recovery"):
        recover_public_target(store, **dict(request, reason="different"))
    owner = store.reserve(args["run_id"], "bravotv", generation=1, stage=Stage.COMMENTS, target_key=args["target_key"])
    again = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(
        **dict(args, owner_token=str(owner["owner_token"]))
    )
    assert again["complete"]
    assert (
        fixtures.query(
            tx, "select count(*) n from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
        )[0]["n"]
        > before
    )
    saved = fixtures.query(
        tx, "select comment_id,is_missing from social.instagram_comments where post_id=%s", (args["post_id"],)
    )
    assert {r["comment_id"] for r in saved} == {"p", "old"}
    assert all(not r["is_missing"] for r in saved)
    assert again["checkpoint"]["comment_pages"]["settlement"]["count_gap"] == 1


def test_recovery_refuses_active_owner_and_stale_evidence(database, monkeypatch):
    store, tx, args = public.setup(database, bounded=True)
    request = command(tx, args)
    with pytest.raises(Conflict, match="active or uncertain"):
        recover_public_target(store, **request)
    store, tx, args, _ = completed(database, monkeypatch)
    request = command(tx, args)
    with pytest.raises(Conflict, match="changed since review"):
        recover_public_target(store, **dict(request, expected_checkpoint_hash="0" * 64))
    with pytest.raises(Conflict, match="exact current"):
        recover_public_target(store, **dict(request, generation=2))


def test_two_complete_unchanged_recovery_passes_stop_rescans(database, monkeypatch):
    store, tx, args, fetch = completed(database, monkeypatch)
    for _ in range(2):
        recover_public_target(store, **command(tx, args))
        owner = store.reserve(
            args["run_id"], "bravotv", generation=1, stage=Stage.COMMENTS, target_key=args["target_key"]
        )
        result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(
            **dict(args, owner_token=str(owner["owner_token"]))
        )
        assert result["complete"]
    with pytest.raises(Conflict, match="Two complete recovery passes"):
        recover_public_target(store, **command(tx, args))


def test_ledger_does_not_certify_native_exhaustion_gap():
    result = target_status({"comment_pages": {"observed": {"a": {}}, "settlement": {"count_gap": 1}}}, "committed")
    assert result["reasons"] == ["count_gap"] and not result["candidate_complete"] and not result["final_acceptance"]


def test_unknown_total_never_becomes_native_zero_in_recovery():
    from trr_backend.ingestion.comments import CommentFetchSummary, _settlement

    state = {
        "expected": None,
        "observed": {"p": {"parent": None, "expected_replies": 0}},
        "lanes": {"__top__": {"exhausted": True}},
    }
    baseline = _strength(state)
    assert baseline["expected_total"] is None
    assert _known_max(None, None) is None and _known_max(0, None) == 0
    assert _settlement(state, CommentFetchSummary(full_observation=True)) is None


def test_january_completion_replay_sees_reopened_work(database, monkeypatch):
    from dataclasses import replace
    from datetime import UTC, datetime

    from trr_backend.ingestion.comment_recovery import enqueue_next_recovery
    from trr_backend.ingestion.domain import RunIntent, stable_json

    store, tx, args, _ = completed(database, monkeypatch)
    prior = fixtures.query(tx, "select intent from social.instagram_ingestion_runs where run_id=%s", (args["run_id"],))[
        0
    ]
    intent = replace(
        RunIntent.from_payload(prior["intent"]),
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set intent=%s::jsonb,intent_hash=%s where run_id=%s",
            (stable_json(intent.payload()), intent.digest, args["run_id"]),
        )
    kwargs = {
        "run_id": args["run_id"],
        "account": "bravotv",
        "generation": 1,
        "intent_hash": intent.digest,
        "source_revision": "fixture",
    }
    result = enqueue_next_recovery(store, **kwargs)
    assert result["target_key"] == args["target_key"]
    assert enqueue_next_recovery(store, **kwargs) == {"status": "work_pending"}
    commands = fixtures.query(
        tx,
        """select count(*) n from social.instagram_ingestion_commands
        where run_id=%s and idempotency_key like 'january-recovery:%%'""",
        (args["run_id"],),
    )
    assert commands == [{"n": 1}]
