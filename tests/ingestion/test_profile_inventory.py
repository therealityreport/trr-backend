"""Crash-safe author enumeration from run-bound committed native comment pages."""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_profiles
from trr_backend.ingestion.comments import CommentActivity, CommentFetchSummary
from trr_backend.ingestion.domain import LostOwnership, RunIntent, SourceProfile, Stage
from trr_backend.ingestion.profile_inventory import advance
from trr_backend.ingestion.profiles import PROFILE_WORKFLOW_VERSION

base_database = test_profiles.base_database
pipeline_database = test_profiles.pipeline_database
profiles_db = test_profiles.profiles_db
clear = test_profiles.clear
rows = test_profiles.rows
NOW = datetime(2026, 10, 16, 17, tzinfo=UTC)


@pytest.fixture(autouse=True)
def clear_canonical_posts(clear, profiles_db):
    # Parametrized malformed-proof cases intentionally reuse the same native
    # shortcode. Keep each isolated proof independent of prior canonical rows.
    with profiles_db[1]() as conn, conn.cursor() as cur:
        cur.execute("truncate social.instagram_posts cascade")


def start(database, codes=("First", "Second"), *, version=PROFILE_WORKFLOW_VERSION):
    store, tx = database
    run = store.start(
        RunIntent(
            account="bravotv",
            selected_tasks=(Stage.COMMENTS, Stage.MEDIA),
            source_profile=SourceProfile.for_mode("public"),
        ),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version=version,
        initiated_by="fixture",
    )
    for stage, keys in ((Stage.LISTING, []), (Stage.COMMENTS, codes)):
        store.discover(
            run["run_id"],
            "bravotv",
            generation=1,
            stage=stage,
            target_keys=keys,
            max_attempts=12,
            cursor_state={},
            expected_cursor_state={},
            page_id="fixture",
            seal=True,
        )
    with tx() as conn, conn.cursor() as cur:
        for code in codes:
            cur.execute("insert into social.instagram_posts(shortcode,username) values(%s,'bravotv')", (code,))
    return dict(run, account="bravotv")


def comment(identity, author, username, *, replies=()):
    return {
        "pk": identity,
        "text": identity,
        "user": {"pk": author, "username": username},
        "child_comment_count": len(replies),
        "preview_child_comments": list(replies),
    }


def settle(database, run, code, payloads, *, expected, at=NOW):
    store, _ = database
    token = store.reserve(run["run_id"], "bravotv", generation=1, stage=Stage.COMMENTS, target_key=code)
    post_id = str(rows(database, "select id from social.instagram_posts where shortcode=%s", (code,))[0]["id"])
    clock = iter(at + timedelta(microseconds=n) for n in range(20))

    def source(**callbacks):
        for payload in payloads:
            callbacks["pre_source_request"]()
            callbacks["source_observer"]("comments_api", payload)
        return CommentFetchSummary(expected_count=expected, full_observation=True)

    return CommentActivity(store, source, clock=lambda: next(clock)).execute(
        run_id=run["run_id"],
        account="bravotv",
        generation=1,
        target_key=code,
        owner_token=str(token["owner_token"]),
        post_id=post_id,
    )


def page(comments, *, more=False, expected=2):
    return {
        "comments": comments,
        "comment_count": expected,
        "has_more_comments": more,
        "has_more_headload_comments": False,
        "next_min_id": "after" if more else None,
    }


def step(database, run, *, limit=1, crash=False):
    with database[1]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        current = database[0]._execution_run(cur, run["run_id"], "bravotv")
        if current["generation"] != run["generation"] or current["state"] not in {"queued", "running"}:
            raise LostOwnership("fixture fence")
        result = advance(cur, current, limit=limit)
        if crash:
            raise RuntimeError("simulated driver crash before commit")
        return result


def profiles(database, run):
    return rows(
        database,
        "select target_key,checkpoint,blocked_reason from social.instagram_ingestion_targets "
        "where run_id=%s and stage='media' order by target_key",
        (run["run_id"],),
    )


def finish(database, run):
    for _ in range(30):
        result = step(database, run)
        assert not result.get("blocked"), result
        if result["sealed"]:
            return
    pytest.fail("bounded profile finalizer failed to seal")


def test_bounded_pages_dedup_native_identity_resume_rename_and_atomic_cursor(profiles_db):
    run = start(profiles_db)
    first = [
        page([comment("1", "101", "reader", replies=[comment("2", "102", "replier")])], more=True, expected=3),
        page([comment("3", "103", "third")], expected=3),
    ]
    assert settle(profiles_db, run, "First", first, expected=3)["outcome"] == "committed"
    assert (
        settle(
            profiles_db,
            run,
            "Second",
            [page([comment("4", "101", "renamed")], expected=1)],
            expected=1,
            at=NOW + timedelta(seconds=1),
        )["outcome"]
        == "committed"
    )
    # A rollback cannot leave an inserted target without its durable offset.
    with pytest.raises(RuntimeError):
        step(profiles_db, run, crash=True)
    assert profiles(profiles_db, run) == []
    assert step(profiles_db, run) == {"sealed": False, "blocked": None}
    assert [row["target_key"] for row in profiles(profiles_db, run)] == ["profile:id:101"]
    cursor = rows(
        profiles_db,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
        (run["run_id"],),
    )[0]["cursor_state"]
    assert cursor["commenter_profiles"]["offset"] == 1
    # Resume comes entirely from persisted cursor/pages, including a reply.
    finish(profiles_db, run)
    targets = profiles(profiles_db, run)
    assert [row["target_key"] for row in targets] == ["profile:id:101", "profile:id:102", "profile:id:103"]
    assert targets[0]["checkpoint"]["username"] == "renamed"
    assert targets[0]["checkpoint"]["source_post_key"] == "Second"
    assert all(row["checkpoint"]["source_evidence_id"] for row in targets)
    assert step(profiles_db, run) == {"sealed": True}
    assert profiles(profiles_db, run) == targets  # Never append or reopen after seal.


def test_unavailable_receipt_excludes_unrelated_old_canonical_authors(profiles_db):
    run = start(profiles_db, ("Unavailable",))
    post = rows(profiles_db, "select id from social.instagram_posts where shortcode='Unavailable'")[0]["id"]
    with profiles_db[1]() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at) "
            "values(%s,'old','unrelated','Old','2025-01-01')",
            (str(post),),
        )
    assert (
        settle(profiles_db, run, "Unavailable", [{"comments_disabled": True}], expected=0)["outcome"]
        == "source_unavailable"
    )
    finish(profiles_db, run)
    assert profiles(profiles_db, run) == []


def test_missing_identity_is_explicit_blocked_target_without_fake_profile(profiles_db):
    run = start(profiles_db, ("Unknown",))
    assert (
        settle(profiles_db, run, "Unknown", [page([comment("1", None, "deleted user")], expected=1)], expected=1)[
            "outcome"
        ]
        == "committed"
    )
    finish(profiles_db, run)
    target = profiles(profiles_db, run)[0]
    assert target["target_key"].startswith("profile:unresolved:")
    assert target["blocked_reason"] == "profile_identity_unresolved"
    assert target["checkpoint"]["user_id"] is None and target["checkpoint"]["username"] is None


def test_missing_retained_page_proof_blocks_manifest_and_cancel_fences_finalizer(profiles_db):
    run = start(profiles_db, ("Missing",))
    settle(profiles_db, run, "Missing", [page([comment("1", "101", "reader")], expected=1)], expected=1)
    with profiles_db[1]() as conn, conn.cursor() as cur:
        cur.execute("update social.instagram_ingestion_evidence set payload=null where run_id=%s", (run["run_id"],))
    result = step(profiles_db, run)
    assert result["blocked"] == "profile_inventory_proof_missing" and not result["sealed"]
    assert profiles(profiles_db, run) == []
    profiles_db[0].control(run["run_id"], "bravotv", action="cancel", idempotency_key="stop-inventory")
    with pytest.raises(LostOwnership):
        step(profiles_db, run)


def test_scheduler_finalizes_only_compatible_v2_and_replays_decision_readonly(profiles_db):
    run = start(profiles_db, ("Scheduled",))
    settle(profiles_db, run, "Scheduled", [page([comment("1", "101", "reader")], expected=1)], expected=1)
    partition = profiles_db[0].seed(run)
    for ordinal in range(10):
        result = profiles_db[0].decision(partition, ordinal, NOW)
        before = profiles(profiles_db, run)
        assert profiles_db[0].decision(partition, ordinal, NOW) == result
        assert profiles(profiles_db, run) == before
        if result["kind"] == "target":
            assert result["target_key"] == "profile:id:101"
            break
    else:
        pytest.fail("v2 scheduler did not finalize inventory")
    old = start(profiles_db, (), version="instagram-ingestion-v1")
    partition = profiles_db[0].seed(old)
    assert profiles_db[0].decision(partition, 0, NOW) == {"kind": "wait", "seconds": 5.0}
    assert rows(
        profiles_db,
        "select discovery_complete from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
        (old["run_id"],),
    ) == [{"discovery_complete": False}]


def test_invalid_later_author_rolls_back_all_inserts_and_keeps_manifest_unsealed(profiles_db):
    run = start(profiles_db, ("Invalid",))
    settle(
        profiles_db,
        run,
        "Invalid",
        [page([comment("1", "101", "reader"), comment("2", "not-native-id", "other")], expected=2)],
        expected=2,
    )
    result = step(profiles_db, run, limit=100)
    assert result == {"sealed": False, "blocked": "profile_inventory_author_id_invalid"}
    assert profiles(profiles_db, run) == []


def test_inventory_waits_for_settled_comments_and_never_reopens_already_sealed(profiles_db):
    run = start(profiles_db, ("Pending",))
    assert step(profiles_db, run) == {"sealed": False}
    assert profiles(profiles_db, run) == []
    with profiles_db[1]() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_manifests set discovery_complete=true,sealed_at=now() "
            "where run_id=%s and stage='media'",
            (run["run_id"],),
        )
    assert step(profiles_db, run) == {"sealed": True}
    assert profiles(profiles_db, run) == []


def corrupt(profiles_db, run, location, value):
    with profiles_db[1]() as conn, conn.cursor() as cur:
        if location in {"receipt", "page"}:
            table = "instagram_ingestion_receipts" if location == "receipt" else "instagram_ingestion_comment_pages"
            cur.execute(
                f"update social.{table} set canonical_identity=%s::jsonb where run_id=%s and stage='comments'",
                (json.dumps(value), run["run_id"]),
            )
        else:
            cursor = {"commenter_profiles": value} if location == "inventory" else value
            cur.execute(
                "update social.instagram_ingestion_manifests set cursor_state=%s::jsonb "
                "where run_id=%s and stage='media'",
                (json.dumps(cursor), run["run_id"]),
            )


def assert_malformed_is_durable(profiles_db, run, expected):
    before = rows(
        profiles_db,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
        (run["run_id"],),
    )[0]["cursor_state"]
    assert step(profiles_db, run) == {"sealed": False, "blocked": expected}
    assert profiles(profiles_db, run) == []
    manifest = rows(
        profiles_db,
        "select cursor_state,discovery_complete,sealed_at "
        "from social.instagram_ingestion_manifests where run_id=%s and stage='media'",
        (run["run_id"],),
    )[0]
    state = manifest["cursor_state"]["commenter_profiles"]
    assert not manifest["discovery_complete"] and manifest["sealed_at"] is None
    assert state["state"] == "blocked" and state["error_code"] == expected
    original = before.get("commenter_profiles") if isinstance(before, dict) else None
    if isinstance(original, dict):
        # Diagnostics may change; persisted progress itself may not advance or rewind.
        assert {key: value for key, value in state.items() if key not in {"state", "error_code"}} == {
            key: value for key, value in original.items() if key not in {"state", "error_code"}
        }


@pytest.mark.parametrize("location", ["receipt", "page", "inventory"])
@pytest.mark.parametrize("value", [[], "malformed", 7, None])
def test_nonobject_proofs_or_inventory_state_are_durably_blocked(profiles_db, location, value):
    run = start(profiles_db, ("Malformed",))
    settle(profiles_db, run, "Malformed", [page([comment("1", "101", "reader")], expected=1)], expected=1)
    corrupt(profiles_db, run, location, value)
    code = "profile_inventory_cursor_invalid" if location == "inventory" else "profile_inventory_proof_invalid"
    assert_malformed_is_durable(profiles_db, run, code)


@pytest.mark.parametrize(
    "field,value",
    [
        ("target_after", []),
        ("page_id", []),
        ("page_after", ["not-a-time", "not-a-uuid"]),
        ("offset", "1"),
        ("state", []),
    ],
)
def test_invalid_inventory_cursor_field_shapes_cannot_reach_sql_or_advance(profiles_db, field, value):
    run = start(profiles_db, ("Cursor",))
    settle(profiles_db, run, "Cursor", [page([comment("1", "101", "reader")], expected=1)], expected=1)
    state = {"version": 1, "state": "enumerating", "target_after": "", field: value}
    corrupt(profiles_db, run, "inventory", state)
    assert_malformed_is_durable(profiles_db, run, "profile_inventory_cursor_invalid")


def test_nonobject_manifest_cursor_is_replaced_only_with_fixed_diagnostic(profiles_db):
    run = start(profiles_db, ("Root",))
    settle(profiles_db, run, "Root", [page([comment("1", "101", "reader")], expected=1)], expected=1)
    corrupt(profiles_db, run, "root", [])
    assert_malformed_is_durable(profiles_db, run, "profile_inventory_cursor_invalid")


@pytest.mark.parametrize(
    "location,value,code",
    [
        ("receipt", {"observed_comment_ids": {}}, "profile_inventory_proof_missing"),
        ("page", {"comment_ids": []}, "profile_inventory_proof_invalid"),
    ],
)
def test_nested_comment_id_proof_shapes_are_durably_blocked(profiles_db, location, value, code):
    run = start(profiles_db, ("Nested",))
    settle(profiles_db, run, "Nested", [page([comment("1", "101", "reader")], expected=1)], expected=1)
    corrupt(profiles_db, run, location, value)
    assert_malformed_is_durable(profiles_db, run, code)


@pytest.mark.parametrize("value", ["malformed", {"comment_ids": []}])
def test_final_coverage_query_rejects_changed_page_shape_without_sql_exception(profiles_db, value):
    run = start(profiles_db, ("FinalCoverage",))
    settle(profiles_db, run, "FinalCoverage", [page([comment("1", "101", "reader")], expected=1)], expected=1)
    committed = rows(
        profiles_db,
        "select committed_at,page_id from social.instagram_ingestion_comment_pages where run_id=%s",
        (run["run_id"],),
    )[0]
    corrupt(
        profiles_db,
        run,
        "inventory",
        {
            "version": 1,
            "state": "enumerating",
            "target_after": "",
            "page_after": [committed["committed_at"].isoformat(), str(committed["page_id"])],
        },
    )
    corrupt(profiles_db, run, "page", value)
    assert_malformed_is_durable(profiles_db, run, "profile_inventory_proof_invalid")
