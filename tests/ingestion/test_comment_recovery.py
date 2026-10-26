"""Recovery journal fences and preservation, using isolated PostgreSQL."""

from uuid import uuid4

import httpx
import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_comments as fixtures
from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.comment_recovery import (
    _active_saved_counts,
    _advance_progress,
    _canonical_shortfall_priority,
    _known_max,
    _prioritize_recovery_rows,
    _strength,
    recover_public_target,
    target_status,
    unchanged_passes,
)
from trr_backend.ingestion.comments import CommentActivity
from trr_backend.ingestion.domain import Conflict, Stage, fingerprint

database = fixtures.database


def test_legacy_committed_overshoot_is_not_a_complete_recovery_candidate():
    checkpoint = {
        "comment_pages": {
            "observed": {"saved-native-id": {"parent": None}},
            "settlement": {
                "basis": "count_parity",
                "expected_total": 0,
                "observed_total": 1,
                "count_gap": 0,
            },
        }
    }
    status = target_status(checkpoint, "committed")
    assert status["reasons"] == ["count_overshoot"]
    assert status["candidate_complete"] is False
    assert status["final_acceptance"] is False


def test_recovery_progress_does_not_reset_when_known_scan_subsets_alternate():
    checkpoint = {
        "comment_pages": {"observed": {"a": {}, "b": {}}, "settlement": {"count_gap": 1}},
        "public_recovery": {
            "no_improvement_passes": 1,
            "baseline": {"observed_ids": ["a", "c"], "count_gap": 1, "unsupported": []},
            "progress": {
                "version": 1,
                "observed_ids": ["a", "b", "c"],
                "best_observed_total": 2,
                "supported_ids": ["a", "b", "c"],
            },
        },
    }
    assert unchanged_passes(checkpoint, "committed") == 2
    assert unchanged_passes(checkpoint, "unresolved") == 1


def test_disappearing_unsupported_id_is_not_topology_repair():
    checkpoint = {
        "comment_pages": {"observed": {"a": {}}, "settlement": {"count_gap": 2}},
        "public_recovery": {
            "no_improvement_passes": 1,
            "baseline": {"observed_ids": ["a", "x"], "count_gap": 2, "unsupported": ["x"]},
        },
    }
    assert unchanged_passes(checkpoint, "committed") == 2


def test_progress_frontier_tracks_fresh_coverage_and_preserves_legacy_baseline():
    baseline = {"observed_ids": ["a", "b"], "count_gap": 2, "unsupported": ["b"]}
    recovery = {"no_improvement_passes": 1, "baseline": baseline}
    repaired = {"observed_ids": ["a", "b"], "count_gap": 2, "unsupported": []}
    checkpoint = {
        "comment_pages": {"observed": {"a": {}, "b": {}}, "settlement": {"count_gap": 2}},
        "public_recovery": recovery,
    }
    assert unchanged_passes(checkpoint, "committed") == 0  # b is present and its topology is repaired.
    progress = _advance_progress(recovery, repaired)
    assert recovery["baseline"] == baseline
    assert progress == {"version": 1, "observed_ids": ["a", "b"], "best_observed_total": 2, "supported_ids": ["a", "b"]}
    recovery = {"no_improvement_passes": 1, "baseline": repaired, "progress": progress}
    # A broader single pass is useful proof even when every ID has been seen before.
    recovery["progress"]["observed_ids"].append("c")
    recovery["progress"]["supported_ids"].append("c")
    checkpoint["public_recovery"] = recovery
    checkpoint["comment_pages"]["observed"]["c"] = {}
    assert unchanged_passes(checkpoint, "committed") == 0
    assert _advance_progress(recovery, _strength(checkpoint["comment_pages"]))["best_observed_total"] == 3


def test_canonical_shortfall_priority_is_ordering_only():
    def row(key, expected):
        return {"target_key": key, "checkpoint": {"comment_pages": {"expected": expected}}}

    counts = {"short": 4, "matched": 5, "over": 6}
    assert _canonical_shortfall_priority(row("short", 5), counts) == 0
    assert _canonical_shortfall_priority(row("absent", 5), counts) == 1
    assert _canonical_shortfall_priority(row("matched", None), counts) == 1
    assert _canonical_shortfall_priority(row("matched", 5), counts) == 2
    assert _canonical_shortfall_priority(row("over", 5), counts) == 2


def test_priority_counts_only_fair_shortlist_and_preserves_remaining_candidates():
    rows = [
        {
            "target_key": f"post-{index}",
            "outcome": "committed",
            "checkpoint": {"comment_pages": {"expected": 5, "settlement": {"count_gap": 1}}},
        }
        for index in range(10)
    ]

    class Cursor:
        def execute(self, sql, params):
            self.keys = params[0]

        def fetchall(self):
            return [
                {"shortcode": key, "active_saved_ids": 0 if key == "post-1" else 5}
                for key in self.keys
                if key != "post-2"
            ]

    cur = Cursor()
    ordered = _prioritize_recovery_rows(cur, rows, {row["target_key"]: i for i, row in enumerate(rows)})
    assert cur.keys == [f"post-{i}" for i in range(8)]
    assert [row["target_key"] for row in ordered] == [
        "post-1",
        "post-2",
        "post-0",
        "post-3",
        "post-4",
        "post-5",
        "post-6",
        "post-7",
        "post-8",
        "post-9",
    ]


def test_saved_count_uses_active_rows_and_distinguishes_an_unknown_post(database, monkeypatch):
    _, tx, args, _ = completed(database, monkeypatch)
    with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        counts = _active_saved_counts(cur, [args["target_key"], "unknown-post"])
        assert counts == {args["target_key"]: 2}
        cur.execute(
            "update social.instagram_comments set is_missing=true where post_id=%s and comment_id='old'",
            (args["post_id"],),
        )
        cur.execute(
            "update social.instagram_comments set deleted_at=clock_timestamp() where post_id=%s and comment_id='p'",
            (args["post_id"],),
        )
        assert _active_saved_counts(cur, [args["target_key"], "unknown-post"]) == {args["target_key"]: 0}


def test_first_unresolved_recovery_seeds_frontier_from_retained_observations(database, monkeypatch):
    store, tx, args, _ = completed(database, monkeypatch)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets "
            "set outcome='unresolved', owner_token=null, owner_expires_at=null "
            "where run_id=%s and target_key=%s",
            (args["run_id"], args["target_key"]),
        )
    request = command(tx, args)
    recover_public_target(store, **request)
    current = fixtures.query(
        tx,
        "select checkpoint from social.instagram_ingestion_targets where run_id=%s and target_key=%s",
        (args["run_id"], args["target_key"]),
    )[0]["checkpoint"]
    recovery = current["public_recovery"]
    assert recovery["progress"]["observed_ids"] == recovery["baseline"]["observed_ids"]
    assert recovery["no_improvement_passes"] == 0
    # The same observations in the next complete pass must not appear new.
    assert unchanged_passes(current, "committed") == 1


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
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_targets
            (run_id,stage,target_key,max_attempts,blocked_reason)
            values(%s,'comments','blocked-other',3,'source_payload_invalid')""",
            (args["run_id"],),
        )
        # An unrelated parity target sorts first and still needs an audit.
        # It must not hold this target's audited count-gap recovery behind it.
        cur.execute(
            """insert into social.instagram_ingestion_targets
            (run_id,stage,target_key,outcome,max_attempts,checkpoint,observed_at,committed_at)
            values(%s,'comments','000-parity-audit-pending','committed',3,
                '{"comment_pages":{"settlement":{"count_gap":0}}}'::jsonb,
                clock_timestamp(),clock_timestamp())""",
            (args["run_id"],),
        )
    assert enqueue_next_recovery(store, **kwargs) == {"status": "work_pending"}
    # A separately blocked source does not prevent bounded recovery of a
    # completed gap, and its block is never cleared by that operation.
    result = enqueue_next_recovery(store, **kwargs, blocked_stage=True)
    assert result["target_key"] == args["target_key"] and result["status"] == "retained_audit_completed"
    result = enqueue_next_recovery(store, **kwargs, blocked_stage=True)
    assert result["target_key"] == args["target_key"] and result["status"] == "public_recovery_queued"
    assert enqueue_next_recovery(store, **kwargs, blocked_stage=True) == {"status": "work_pending"}
    commands = fixtures.query(
        tx,
        """select count(*) n from social.instagram_ingestion_commands
        where run_id=%s and idempotency_key like 'january-recovery:%%'""",
        (args["run_id"],),
    )
    assert commands == [{"n": 1}]


def january_binding(tx, args):
    from dataclasses import replace
    from datetime import UTC, datetime

    from trr_backend.ingestion.domain import RunIntent, stable_json

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
    return {
        "run_id": args["run_id"],
        "account": "bravotv",
        "generation": 1,
        "intent_hash": intent.digest,
        "source_revision": "fixture",
    }


def test_parity_target_gets_durable_offline_audit_without_source_recovery(database, monkeypatch):
    from trr_backend.ingestion import retained_comment_recovery as retained
    from trr_backend.ingestion.comment_recovery import enqueue_next_recovery
    from trr_backend.ingestion.domain import stable_json

    store, tx, args, _ = completed(database, monkeypatch)
    kwargs = january_binding(tx, args)
    checkpoint = command(tx, args)["expected_checkpoint_hash"]
    # A stopped fresh-source budget never suppresses a new parser's audit.
    current = fixtures.query(
        tx,
        "select checkpoint from social.instagram_ingestion_targets where run_id=%s and stage='comments'",
        (args["run_id"],),
    )[0]["checkpoint"]
    current["comment_pages"]["settlement"]["count_gap"] = 0
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set checkpoint=%s::jsonb where run_id=%s and stage='comments'",
            (stable_json(current), args["run_id"]),
        )
    monkeypatch.setattr(
        "trr_backend.ingestion.comment_recovery.recover_public_target",
        lambda *args, **kwargs: pytest.fail("parity target attempted fresh source recovery"),
    )
    result = enqueue_next_recovery(store, **kwargs)
    assert result["status"] == "retained_audit_completed" and result["checkpoint_sha256"] != checkpoint
    assert enqueue_next_recovery(store, **kwargs) is None
    monkeypatch.setattr(retained, "parser_implementation_fingerprint", lambda: "f" * 64)
    assert enqueue_next_recovery(store, **kwargs)["status"] == "retained_audit_completed"
    assert enqueue_next_recovery(store, **kwargs) is None
    assert fixtures.query(
        tx,
        """select count(*) as n from social.instagram_ingestion_commands
        where run_id=%s and idempotency_key like 'retained-comment-summary-v1:%%'""",
        (args["run_id"],),
    ) == [{"n": 2}]

    # A later canonical mutation must invalidate the completed retained audit,
    # even though the traversal checkpoint and parser have not changed.
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_comments set last_seen_at=last_seen_at+interval '1 second' where post_id=%s",
            (args["post_id"],),
        )
    assert enqueue_next_recovery(store, **kwargs)["status"] == "retained_audit_completed"
    assert enqueue_next_recovery(store, **kwargs) is None


def test_incomplete_offline_audit_cannot_authorize_fresh_root(database, monkeypatch):
    from trr_backend.ingestion import retained_comment_recovery as retained
    from trr_backend.ingestion.comment_recovery import enqueue_next_recovery

    store, tx, args, _ = completed(database, monkeypatch)
    kwargs = january_binding(tx, args)
    monkeypatch.setattr(
        retained,
        "audit_and_replay_retained",
        lambda *args, **kwargs: {"status": "offline_audit_blocked", "stream_complete": False},
    )
    monkeypatch.setattr(
        "trr_backend.ingestion.comment_recovery.recover_public_target",
        lambda *args, **kwargs: pytest.fail("incomplete offline audit authorized fresh source"),
    )
    with pytest.raises(Conflict, match="offline audit prerequisite"):
        enqueue_next_recovery(store, **kwargs)


def test_exhausted_fresh_pass_budget_still_audits_new_parser(database, monkeypatch):
    from trr_backend.ingestion import retained_comment_recovery as retained
    from trr_backend.ingestion.comment_recovery import enqueue_next_recovery
    from trr_backend.ingestion.domain import stable_json

    store, tx, args, _ = completed(database, monkeypatch)
    kwargs = january_binding(tx, args)
    current = fixtures.query(
        tx,
        "select checkpoint from social.instagram_ingestion_targets where run_id=%s and stage='comments'",
        (args["run_id"],),
    )[0]["checkpoint"]
    current["public_recovery"] = {"no_improvement_passes": 2, "baseline": _strength(current["comment_pages"])}
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set checkpoint=%s::jsonb where run_id=%s and stage='comments'",
            (stable_json(current), args["run_id"]),
        )
    monkeypatch.setattr(
        "trr_backend.ingestion.comment_recovery.recover_public_target",
        lambda *args, **kwargs: pytest.fail("exhausted fresh recovery budget was bypassed"),
    )
    assert enqueue_next_recovery(store, **kwargs)["status"] == "retained_audit_completed"
    assert enqueue_next_recovery(store, **kwargs) is None
    monkeypatch.setattr(retained, "parser_implementation_fingerprint", lambda: "e" * 64)
    assert enqueue_next_recovery(store, **kwargs)["status"] == "retained_audit_completed"
    assert enqueue_next_recovery(store, **kwargs) is None


def test_summary_keys_for_all_settled_targets_are_queried_in_one_batch(database, monkeypatch):
    from psycopg2.extras import RealDictCursor

    from trr_backend.ingestion import comment_recovery as recovery
    from trr_backend.ingestion.comments import COMMENT_PARSER_VERSION
    from trr_backend.ingestion.domain import stable_json
    from trr_backend.ingestion.evidence import Evidence

    store, tx, args, _ = completed(database, monkeypatch)
    kwargs = january_binding(tx, args)
    extra = "extra-" + uuid4().hex
    evidence_id = str(uuid4())
    checkpoint = {
        "comment_pages": {
            "adapter_family": "instagram_public_relay",
            "adapter_version": 1,
            "scan_root_evidence_id": evidence_id,
            "settlement": {"count_gap": 0},
        }
    }
    evidence = Evidence.capture(
        source_identity=f"instagram:comments:{extra}:comments_api",
        captured_at=public.OBSERVED,
        parser_version=COMMENT_PARSER_VERSION,
        payload={"comments": [], "has_more_comments": False},
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set checkpoint=jsonb_set(checkpoint,
            '{comment_pages,settlement,count_gap}','0'::jsonb) where run_id=%s and stage='comments'""",
            (args["run_id"],),
        )
        cur.execute("insert into social.instagram_posts(shortcode,username) values(%s,'bravotv')", (extra,))
        cur.execute(
            """insert into social.instagram_ingestion_targets
            (run_id,stage,target_key,outcome,max_attempts,checkpoint,observed_at,committed_at)
            values(%s,'comments',%s,'committed',12,%s::jsonb,%s,clock_timestamp())""",
            (args["run_id"], extra, stable_json(checkpoint), public.OBSERVED),
        )
        cur.execute(
            """insert into social.instagram_ingestion_evidence
            (evidence_id,run_id,stage,target_key,source_identity,captured_at,expires_at,content_hash,
             parser_version,schema_version,payload) values(%s,%s,'comments',%s,%s,%s,%s,%s,%s,1,%s)""",
            (
                evidence_id,
                args["run_id"],
                extra,
                evidence.source_identity,
                evidence.captured_at,
                evidence.expires_at,
                evidence.content_hash,
                evidence.parser_version,
                evidence.payload_json,
            ),
        )
        cur.execute(
            """insert into social.instagram_ingestion_comment_pages
            (page_id,run_id,target_key,owner_generation,evidence_id,observed_at,content_hash,exhausted,canonical_identity,checkpoint)
            values(%s,%s,%s,1,%s,%s,%s,true,'{}'::jsonb,'{}'::jsonb)""",
            (str(uuid4()), args["run_id"], extra, evidence_id, public.OBSERVED, evidence.content_hash),
        )
    batches = []

    class RecordingCursor(RealDictCursor):
        def execute(self, sql, params=None):
            if "idempotency_key=any" in str(sql):
                batches.append(list(params[2]))
            return super().execute(sql, params)

    monkeypatch.setattr(recovery, "RealDictCursor", RecordingCursor)
    first = recovery.enqueue_next_recovery(store, **kwargs)
    second = recovery.enqueue_next_recovery(store, **kwargs)
    assert first["status"] == second["status"] == "retained_audit_completed"
    assert {first["target_key"], second["target_key"]} == {extra, args["target_key"]}
    assert recovery.enqueue_next_recovery(store, **kwargs) is None
    assert len(batches) == 3 and all(len(batch) == 2 for batch in batches)


def test_completion_extends_productive_budget_before_terminal_audit(database, monkeypatch):
    from tests.ingestion.test_productive_comment_budget import productive, target
    from trr_backend.ingestion.comment_recovery import enqueue_next_recovery

    store, tx, args, binding = productive(database)
    before = target(tx, args)
    result = enqueue_next_recovery(store, **binding, blocked_stage=True)
    assert result["status"] == "productive_budget_extended"
    assert result["targets"][0]["target_key"] == args["target_key"]
    after = target(tx, args)
    assert after["max_attempts"] == before["attempts"] + 12
    assert after["checkpoint"] == before["checkpoint"]
    # The preserved server retry deadline still controls dispatch.
    assert enqueue_next_recovery(store, **binding) == {"status": "work_pending"}
