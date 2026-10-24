"""Productive exhausted-attempt grants against fixture-owned PostgreSQL only."""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.ingestion import test_comments as fixtures
from trr_backend.ingestion.comments import CommentActivity
from trr_backend.ingestion.domain import Conflict, Failure, RunIntent, SourceProfile, Stage, fingerprint, stable_json
from trr_backend.ingestion.productive_comment_budget import extend_productive_budgets

database = fixtures.database


def productive(database, *, pages=True, empty_reply=False, release=True, max_attempts=1):
    store, tx, args = fixtures.setup(database)
    saved = fixtures.query(tx, "select intent from social.instagram_ingestion_runs where run_id=%s", (args["run_id"],))[
        0
    ]
    intent = replace(
        RunIntent.from_payload(saved["intent"]),
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
        source_profile=SourceProfile.for_mode("public"),
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set intent=%s::jsonb,intent_hash=%s where run_id=%s",
            (stable_json(intent.payload()), intent.digest, args["run_id"]),
        )
        cur.execute(
            "update social.instagram_ingestion_targets set max_attempts=%s where run_id=%s and stage='comments'",
            (max_attempts, args["run_id"]),
        )
    owned = {key: value for key, value in args.items() if key != "post_id"}
    if pages:
        partial = deepcopy(fixtures.NATIVE["rest"])
        partial.update(has_more_comments=True, next_max_id="productive-next")
        payloads = [partial]
        if empty_reply:
            reply = deepcopy(fixtures.NATIVE["replies"])
            reply["data"]["xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"][
                "edges"
            ] = []
            payloads.append(reply)
        result = CommentActivity(store, fixtures.source(payloads), clock=lambda: fixtures.OBSERVED).execute(**args)
        assert result["outcome"] == "unresolved"
        checkpoint = result["checkpoint"]
    else:
        store.acquire(**owned, stage=Stage.COMMENTS)
        checkpoint = {}
    if release:
        store.checkpoint_failure(
            **owned,
            stage=Stage.COMMENTS,
            failure=Failure(code="activity_timeout", retry_at=datetime.now(UTC) + timedelta(hours=1)),
            checkpoint=checkpoint,
            requests=0,
        )
    else:
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "update social.instagram_ingestion_targets set owner_expires_at=clock_timestamp()-interval '1 second' "
                "where run_id=%s and stage='comments'",
                (args["run_id"],),
            )
    return (
        store,
        tx,
        args,
        {
            "run_id": args["run_id"],
            "account": args["account"],
            "generation": args["generation"],
            "intent_hash": intent.digest,
            "source_revision": "fixture-revision",
        },
    )


def target(tx, args):
    return fixtures.query(
        tx, "select * from social.instagram_ingestion_targets where run_id=%s and stage='comments'", (args["run_id"],)
    )[0]


@pytest.mark.parametrize("release,empty_reply", [(True, False), (False, False), (True, True)])
def test_productive_grant_preserves_all_progress_backoff_and_journals_once(database, release, empty_reply):
    store, tx, args, binding = productive(database, release=release, empty_reply=empty_reply)
    before = target(tx, args)
    pages = fixtures.query(
        tx,
        "select * from social.instagram_ingestion_comment_pages where run_id=%s order by committed_at",
        (args["run_id"],),
    )
    comments = fixtures.query(
        tx, "select * from social.instagram_comments where post_id=%s order by comment_id", (args["post_id"],)
    )
    evidence = fixtures.query(
        tx, "select * from social.instagram_ingestion_evidence where run_id=%s order by evidence_id", (args["run_id"],)
    )
    if empty_reply:
        assert pages[-1]["parent_comment_id"] == "parent1" and pages[-1]["exhausted"]
    result = extend_productive_budgets(store, **binding)
    assert len(result) == 1
    assert result[0]["attempts"] == 1 and result[0]["old_max_attempts"] == 1 and result[0]["new_max_attempts"] == 13
    assert result[0]["checkpoint_sha256"] == fingerprint(before["checkpoint"])
    after = target(tx, args)
    assert after.pop("max_attempts") == 13
    before.pop("max_attempts")
    assert after == before
    assert extend_productive_budgets(store, **binding) == []
    assert (
        fixtures.query(
            tx,
            "select * from social.instagram_ingestion_comment_pages where run_id=%s order by committed_at",
            (args["run_id"],),
        )
        == pages
    )
    assert (
        fixtures.query(
            tx, "select * from social.instagram_comments where post_id=%s order by comment_id", (args["post_id"],)
        )
        == comments
    )
    assert (
        fixtures.query(
            tx,
            "select * from social.instagram_ingestion_evidence where run_id=%s order by evidence_id",
            (args["run_id"],),
        )
        == evidence
    )
    journal = fixtures.query(
        tx,
        "select result from social.instagram_ingestion_commands where account=%s and idempotency_key=%s",
        (args["account"], result[0]["idempotency_key"]),
    )
    assert len(journal) == 1
    entry = journal[0]["result"]
    assert entry["productive_page_id"] == str(pages[-1]["page_id"])
    assert entry["owner_generation"] == before["owner_generation"]
    assert entry["reason"] == "committed_page_in_exhausted_attempt"
    assert entry["run_id"] == args["run_id"] and entry["generation"] == 1
    assert entry["source_revision"] == "fixture-revision"
    if release:
        assert (
            store.reserve(
                args["run_id"], args["account"], generation=1, stage=Stage.COMMENTS, target_key=args["target_key"]
            )
            is None
        )


@pytest.mark.parametrize(
    "guard", ["active", "uncertain", "blocked", "old_generation", "predates_start", "no_improvement"]
)
def test_unproductive_or_fenced_targets_never_receive_more_budget(database, guard):
    store, tx, args, binding = productive(database, release=guard != "predates_start")
    with tx() as conn, conn.cursor() as cur:
        if guard == "active":
            cur.execute(
                "update social.instagram_ingestion_targets set owner_token=%s,"
                "owner_expires_at=clock_timestamp()+interval '1 hour' "
                "where run_id=%s and stage='comments'",
                (str(uuid4()), args["run_id"]),
            )
        elif guard == "uncertain":
            cur.execute(
                "update social.instagram_ingestion_targets set owner_token=%s,owner_expires_at=null "
                "where run_id=%s and stage='comments'",
                (str(uuid4()), args["run_id"]),
            )
        elif guard == "blocked":
            cur.execute(
                "update social.instagram_ingestion_targets set blocked_reason='source_identity_mismatch' "
                "where run_id=%s and stage='comments'",
                (args["run_id"],),
            )
        elif guard == "old_generation":
            cur.execute(
                "update social.instagram_ingestion_comment_pages set owner_generation=owner_generation-1 "
                "where run_id=%s",
                (args["run_id"],),
            )
        elif guard == "predates_start":
            cur.execute(
                "update social.instagram_ingestion_comment_pages p "
                "set committed_at=t.owner_started_at-interval '1 second' "
                "from social.instagram_ingestion_targets t "
                "where p.run_id=t.run_id and p.target_key=t.target_key and p.run_id=%s",
                (args["run_id"],),
            )
        else:
            cur.execute(
                "update social.instagram_ingestion_targets set checkpoint=checkpoint || "
                "'{\"public_recovery\":{\"no_improvement_passes\":2}}'::jsonb where run_id=%s and stage='comments'",
                (args["run_id"],),
            )
    before = target(tx, args)
    assert extend_productive_budgets(store, **binding) == []
    assert target(tx, args) == before


def test_attempt_without_committed_page_is_not_productive(database):
    store, tx, args, binding = productive(database, pages=False)
    before = target(tx, args)
    assert extend_productive_budgets(store, **binding) == [] and target(tx, args) == before


@pytest.mark.parametrize("stronger_empty_reply", [False, True])
def test_later_attempt_requires_new_ids_or_stronger_lane_proof(database, stronger_empty_reply):
    store, tx, args, binding = productive(database, max_attempts=2)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set next_attempt_at=null where run_id=%s and stage='comments'",
            (args["run_id"],),
        )
    reservation = store.reserve(
        args["run_id"], args["account"], generation=1, stage=Stage.COMMENTS, target_key=args["target_key"]
    )
    args = {**args, "owner_token": str(reservation["owner_token"])}
    if stronger_empty_reply:
        payload = deepcopy(fixtures.NATIVE["replies"])
        payload["data"]["xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"][
            "edges"
        ] = []
    else:
        payload = deepcopy(fixtures.NATIVE["rest"])
        payload.update(has_more_comments=True, next_max_id="productive-next")
    result = CommentActivity(
        store, fixtures.source([payload]), clock=lambda: fixtures.OBSERVED + timedelta(minutes=1)
    ).execute(**args)
    assert result["outcome"] == "unresolved"
    store.checkpoint_failure(
        **{key: value for key, value in args.items() if key != "post_id"},
        stage=Stage.COMMENTS,
        failure=Failure(code="activity_timeout", retry_at=datetime.now(UTC) + timedelta(hours=1)),
        checkpoint=result["checkpoint"],
        requests=0,
    )
    before = target(tx, args)
    grants = extend_productive_budgets(store, **binding)
    if stronger_empty_reply:
        assert len(grants) == 1 and grants[0]["new_max_attempts"] == 14
        entry = fixtures.query(
            tx,
            "select result from social.instagram_ingestion_commands where idempotency_key=%s",
            (grants[0]["idempotency_key"],),
        )[0]["result"]
        assert entry["observed_count"] == entry["previous_observed_count"]
        assert entry["exhausted_count"] > entry["previous_exhausted_count"]
    else:
        assert grants == [] and target(tx, args) == before


@pytest.mark.parametrize("change", [{"generation": 2}, {"intent_hash": "0" * 64}])
def test_stale_frozen_binding_rejected_without_mutation(database, change):
    store, tx, args, binding = productive(database)
    before = target(tx, args)
    with pytest.raises(Conflict, match="exact current unheld public January"):
        extend_productive_budgets(store, **(binding | change))
    assert target(tx, args) == before


def test_non_january_intent_is_not_authorized_even_with_matching_hash(database):
    store, tx, args, binding = productive(database)
    intent = RunIntent.from_payload(
        fixtures.query(tx, "select intent from social.instagram_ingestion_runs where run_id=%s", (args["run_id"],))[0][
            "intent"
        ]
    )
    changed = replace(intent, date_end=datetime(2026, 3, 1, tzinfo=UTC))
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_runs set intent=%s::jsonb,intent_hash=%s where run_id=%s",
            (stable_json(changed.payload()), changed.digest, args["run_id"]),
        )
    with pytest.raises(Conflict, match="exact current unheld public January"):
        extend_productive_budgets(store, **(binding | {"intent_hash": changed.digest}))
