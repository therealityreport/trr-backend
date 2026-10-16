"""Real local PostgreSQL page/receipt/canonical atomicity; no source requests."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from tests.ingestion.test_canonical import database as canonical_database_fixture
from trr_backend.ingestion.canonical import write_post
from trr_backend.ingestion.domain import Failure, LostOwnership, RunIntent, Stage
from trr_backend.ingestion.listing import ROOT_FIELD, ListingActivity, page_target
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.providers import InvalidSourceError

canonical_database = canonical_database_fixture
OBSERVED = datetime(2026, 10, 16, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def database(canonical_database):
    tx = canonical_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute("""create role anon; create role authenticated; create role service_role;
            alter table social.scrape_runs add column source_scope text, add column status text,
                add column initiated_by text, add column config jsonb, add column cancelled_at timestamptz""")
        cur.execute(
            (
                Path(__file__).resolve().parents[2]
                / "supabase/migrations/20261016070000_instagram_ingestion_foundation.sql"
            ).read_text()
        )
    return Store(tx), tx


def sql(database, statement, args=()):
    with database[1]() as conn, conn.cursor() as cur:
        cur.execute(statement, args or None)
        return cur.fetchall() if cur.description else None


def launch(database, *, bounded=False, scope=None):
    store, _ = database
    intent = RunIntent(
        account="fixture_account",
        scope=scope or ("bounded_window" if bounded else "full_history"),
        date_start=datetime(2026, 10, 10, tzinfo=UTC) if bounded else None,
        date_end=datetime(2026, 10, 12, tzinfo=UTC) if bounded else None,
        listing_max_attempts=9,
        comments_max_attempts=7,
    )

    def initialize(conn, run_id):
        with conn.cursor() as cur:
            cur.execute(
                """insert into social.instagram_ingestion_targets
                (run_id,stage,target_key,max_attempts,checkpoint)
                values(%s,'listing','page:root',9,'{"cursor":null}')""",
                (run_id,),
            )

    run = store.start(
        intent,
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="test-v1",
        initiated_by="fixture",
        initialize=initialize,
    )
    return {"run_id": run["run_id"], "account": intent.account, "generation": 1}


def post(code, date):
    return {
        "__typename": "GraphImage",
        "id": "id-" + code,
        "shortcode": code,
        "owner": {"id": "owner1", "username": "fixture_account"},
        "taken_at_timestamp": int(datetime.fromisoformat(date).timestamp()),
        "display_url": "https://cdn.example/image.jpg",
        "edge_media_to_caption": {"edges": []},
        "edge_media_preview_like": {"count": 2},
        "edge_media_to_comment": {"count": 0},
    }


def page(posts, *, more, cursor):
    return {
        "data": {
            ROOT_FIELD: {
                "edges": [{"node": node} for node in posts],
                "page_info": {"has_next_page": more, "end_cursor": cursor},
            }
        }
    }


def execute(database, run, payload, *, cursor=None, writer=write_post, fetch=None):
    store, _ = database
    key = "page:root" if cursor is None else page_target(cursor)
    args = dict(run, stage=Stage.LISTING, target_key=key)
    reserved = store.reserve(**args)
    assert reserved
    args.pop("stage")
    args["owner_token"] = str(reserved["owner_token"])
    if fetch is None:

        def fetch(**callbacks):
            assert callbacks["cursor"] == cursor
            callbacks["pre_source_request"]()
            callbacks["source_observer"](payload, {"cookies": {"secret": "never-retained"}})
            return payload

    return ListingActivity(store, writer=writer, clock=lambda: OBSERVED).execute(**args, fetch=fetch), args


def test_bounded_mixed_and_old_pages_continue_until_explicit_exhaustion(database):
    run = launch(database, bounded=True)
    root = page(
        [
            post("BoundOld", "2026-10-01T00:00:00+00:00"),
            post("BoundStart", "2026-10-10T00:00:00+00:00"),
            post("BoundEnd", "2026-10-12T00:00:00+00:00"),
        ],
        more=True,
        cursor="next1",
    )
    receipt, args = execute(database, run, root)
    assert receipt["canonical_identity"]["post_keys"] == ["BoundStart"]
    assert receipt["observed_at"] == OBSERVED
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]
    assert sql(
        database,
        """select stage,max_attempts from social.instagram_ingestion_targets
        where run_id=%s and target_key='BoundStart' order by stage""",
        (run["run_id"],),
    ) == [("comments", 7), ("media", 3), ("post_details", 3)]
    # Receipt replay returns before transport and keeps the atomically added page.
    duplicate = ListingActivity(database[0]).execute(**args, fetch=lambda **_: pytest.fail("duplicate fetched"))
    assert duplicate["receipt_id"] == receipt["receipt_id"]
    assert sql(
        database,
        """select checkpoint,max_attempts from social.instagram_ingestion_targets
        where run_id=%s and target_key=%s""",
        (run["run_id"], page_target("next1")),
    ) == [({"cursor": "next1"}, 9)]
    # A wholly old page is not proof of chronological exhaustion.
    old = page([post("BoundOlder", "2026-09-01T00:00:00+00:00")], more=True, cursor="next2")
    execute(database, run, old, cursor="next1")
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]
    final = page([post("BoundInside", "2026-10-11T00:00:00+00:00")], more=False, cursor="terminal")
    final_receipt, _ = execute(database, run, final, cursor="next2")
    assert final_receipt["canonical_identity"]["stop_reason"] == "provider_exhausted"
    assert final_receipt["canonical_identity"]["coverage_basis"] == "provider_exhaustion"
    assert final_receipt["canonical_identity"]["provider_exhausted"] is True
    assert sql(
        database, "select shortcode from social.instagram_posts where shortcode like 'Bound%' order by shortcode"
    ) == [("BoundInside",), ("BoundStart",)]
    assert sql(
        database,
        "select bool_and(discovery_complete),count(*) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(True, 4)]
    assert sql(
        database,
        """select count(*),sum(attempts),sum(requests) from social.instagram_ingestion_targets
        where run_id=%s and stage='listing' and outcome='committed'""",
        (run["run_id"],),
    ) == [(3, 3, 3)]
    assert sql(
        database,
        "select count(*) from social.instagram_ingestion_evidence where run_id=%s and payload like %s",
        (run["run_id"], "%never-retained%"),
    ) == [(0,)]


def test_canonical_page_failure_rolls_back_all_discovery_but_retains_evidence(database):
    run = launch(database)
    payload = page([post("RollbackOne", "2026-10-11T00:00:00+00:00")], more=True, cursor="after-rollback")

    def crash(conn, **kwargs):
        write_post(conn, **kwargs)
        raise RuntimeError("crash after canonical writes before receipt")

    store, _ = database
    args = dict(run, stage=Stage.LISTING, target_key="page:root")
    token = str(store.reserve(**args)["owner_token"])
    owned = dict(args, owner_token=token)

    def fetch(**callbacks):
        callbacks["pre_source_request"]()
        callbacks["source_observer"](payload)
        return payload

    with pytest.raises(RuntimeError, match="crash after canonical"):
        ListingActivity(store, writer=crash, clock=lambda: OBSERVED).execute(
            **{k: v for k, v in owned.items() if k != "stage"}, fetch=fetch
        )
    assert sql(database, "select count(*) from social.instagram_posts where shortcode='RollbackOne'") == [(0,)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (run["run_id"],)
    ) == [(0,)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],)
    ) == [(1,)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_evidence where run_id=%s", (run["run_id"],)
    ) == [(1,)]
    assert sql(
        database,
        """select cursor_state,discovery_complete from social.instagram_ingestion_manifests
        where run_id=%s and stage='listing'""",
        (run["run_id"],),
    ) == [({}, False)]
    store.checkpoint_failure(**owned, failure=Failure("WRITER_FAILED"), checkpoint={"cursor": None}, requests=0)
    execute(database, run, payload)
    assert sql(
        database,
        """select count(*) from social.instagram_ingestion_targets
        where run_id=%s and stage='listing'""",
        (run["run_id"],),
    ) == [(2,)]


@pytest.mark.parametrize("more", [True, False])
def test_repeated_cursor_never_commits_or_falsely_seals(database, more):
    run = launch(database)
    execute(database, run, page([], more=True, cursor="repeat"))
    with pytest.raises(InvalidSourceError, match="repeated"):
        execute(
            database,
            run,
            page([post("CursorBad", "2026-10-11T00:00:00+00:00")], more=more, cursor="repeat"),
            cursor="repeat",
        )
    assert sql(
        database,
        """select outcome from social.instagram_ingestion_targets
        where run_id=%s and target_key=%s""",
        (run["run_id"], page_target("repeat")),
    ) == [("unresolved",)]
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]
    assert sql(database, "select count(*) from social.instagram_posts where shortcode='CursorBad'") == [(0,)]


def test_source_callback_failure_after_observation_keeps_evidence_and_target_unresolved(database):
    run = launch(database)
    payload = page([], more=False, cursor=None)

    def failed(**callbacks):
        callbacks["pre_source_request"]()
        callbacks["source_observer"](payload)
        raise RuntimeError("transport parsing failed after evidence")

    with pytest.raises(RuntimeError, match="transport parsing"):
        execute(database, run, payload, fetch=failed)
    assert sql(
        database, "select count(*) from social.instagram_ingestion_evidence where run_id=%s", (run["run_id"],)
    ) == [(1,)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (run["run_id"],)
    ) == [(0,)]


def test_missing_pagination_proof_and_unobserved_response_fail_closed(database):
    run = launch(database)
    malformed = {"data": {ROOT_FIELD: {"edges": [], "page_info": {}}}}
    with pytest.raises(InvalidSourceError, match="pagination"):
        execute(database, run, malformed)
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]
    other = launch(database)
    with pytest.raises(InvalidSourceError, match="not retained"):
        execute(database, other, page([], more=False, cursor=None), fetch=lambda **_: page([], more=False, cursor=None))


@pytest.mark.parametrize("failure", [{"errors": [{"message": "source failed"}]}, {"status": "fail"}])
def test_source_error_cannot_seal_even_with_terminal_page_info(database, failure):
    run = launch(database)
    payload = dict(page([], more=False, cursor=None), **failure)
    with pytest.raises(InvalidSourceError, match="unsuccessful"):
        execute(database, run, payload)
    assert sql(
        database, "select count(*) from social.instagram_ingestion_evidence where run_id=%s", (run["run_id"],)
    ) == [(1,)]
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]


def test_earlier_cursor_loop_is_not_terminal_discovery(database):
    run = launch(database)
    execute(database, run, page([], more=True, cursor="loop-a"))
    execute(database, run, page([], more=True, cursor="loop-b"), cursor="loop-a")
    with pytest.raises(InvalidSourceError, match="already scheduled"):
        execute(database, run, page([], more=False, cursor="loop-a"), cursor="loop-b")
    assert sql(
        database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (run["run_id"],)
    ) == [(2,)]
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]


def older_posts():
    return [post("WindowOld" + str(day), f"2026-10-0{day}T00:00:00+00:00") for day in (9, 8, 7)]


@pytest.mark.parametrize("scope", ["bounded_window", "recent_window"])
@pytest.mark.parametrize("pin_mode", ["missing", "false", "known_pin"])
def test_validated_older_page_covers_window_without_claiming_provider_exhaustion(database, scope, pin_mode):
    run = launch(database, bounded=True, scope=scope)
    posts = older_posts()
    if pin_mode == "false":
        for node in posts:
            node["is_pinned"] = False
    elif pin_mode == "known_pin":
        posts.insert(0, dict(post("WindowPin", "2026-10-11T00:00:00+00:00"), isPinned=True))
    payload = page(posts, more=True, cursor="still-more-at-provider")
    receipt, args = execute(database, run, payload)
    identity = receipt["canonical_identity"]
    assert identity["discovery_complete"] and identity["next_target_key"] is None
    assert identity["stop_reason"] == "date_start_reached"
    assert identity["coverage_basis"] == "bounded_window_heuristic_v1"
    assert identity["provider_exhausted"] is False
    assert identity["post_keys"] == (["WindowPin"] if pin_mode == "known_pin" else [])
    assert len(identity["evidence_hash"]) == 64
    state = sql(
        database,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='listing'",
        (run["run_id"],),
    )[0][0]
    assert state == dict(
        cursor="still-more-at-provider",
        last_page="page:root",
        **{key: identity[key] for key in ("stop_reason", "coverage_basis", "provider_exhausted", "evidence_hash")},
    )
    assert sql(
        database,
        "select bool_and(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(True,)]
    assert sql(
        database,
        "select count(*) from social.instagram_ingestion_targets where run_id=%s and stage='listing'",
        (run["run_id"],),
    ) == [(1,)]
    retained = sql(
        database,
        "select payload from social.instagram_ingestion_evidence where evidence_id=%s",
        (receipt["evidence_id"],),
    )[0][0]
    assert __import__("json").loads(retained)["data"][ROOT_FIELD]["page_info"]["has_next_page"] is True
    duplicate = ListingActivity(database[0]).execute(**args, fetch=lambda **_: pytest.fail("duplicate fetched"))
    assert duplicate["receipt_id"] == receipt["receipt_id"]


@pytest.mark.parametrize(
    "variant",
    ["short", "mixed", "newer_than_end", "out_of_order", "all_pinned", "too_few_unpinned", "malformed", "conflicting"],
)
def test_window_boundary_disqualifiers_preserve_cursor_continuation(database, variant):
    run = launch(database, bounded=True)
    posts = older_posts()
    if variant == "short":
        posts.pop()
    elif variant == "mixed":
        posts.insert(0, post("WindowInside", "2026-10-11T00:00:00+00:00"))
    elif variant == "newer_than_end":
        posts.insert(0, post("WindowAfterEnd", "2026-10-15T00:00:00+00:00"))
    elif variant == "out_of_order":
        posts[1], posts[2] = posts[2], posts[1]
    elif variant == "all_pinned":
        for node in posts:
            node["is_pinned"] = True
    elif variant == "too_few_unpinned":
        posts[0]["is_pinned"] = True
    elif variant == "malformed":
        posts[0]["is_pinned"] = "false"
    else:
        posts[0].update(is_pinned=False, isPinned=True)
    receipt, _ = execute(database, run, page(posts, more=True, cursor="continue-" + variant))
    identity = receipt["canonical_identity"]
    assert identity["discovery_complete"] is False and identity["stop_reason"] is None
    assert identity["coverage_basis"] is None and identity["provider_exhausted"] is False
    assert identity["next_target_key"] == page_target("continue-" + variant)
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]


@pytest.mark.parametrize("bad_timestamp", [None, "not-a-timestamp", True])
def test_window_boundary_requires_whole_page_timestamp_validation(database, bad_timestamp):
    run = launch(database, bounded=True)
    posts = older_posts()
    posts.append(post("WindowBadTimestamp", "2026-10-06T00:00:00+00:00"))
    posts[-1]["taken_at_timestamp"] = bad_timestamp
    with pytest.raises(InvalidSourceError, match="timestamp"):
        execute(database, run, page(posts, more=True, cursor="cannot-seal"))
    assert sql(
        database,
        "select bool_or(discovery_complete) from social.instagram_ingestion_manifests where run_id=%s",
        (run["run_id"],),
    ) == [(False,)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (run["run_id"],)
    ) == [(0,)]


def test_window_boundary_cannot_count_duplicate_old_posts(database):
    run = launch(database, bounded=True)
    node = older_posts()[0]
    with pytest.raises(InvalidSourceError, match="duplicate"):
        execute(database, run, page([node, node, node], more=True, cursor="duplicate-old"))


def test_full_history_and_short_pages_do_not_gain_date_boundary(database):
    full = launch(database)
    receipt, _ = execute(database, full, page(older_posts(), more=True, cursor="full-history-next"))
    assert not receipt["canonical_identity"]["discovery_complete"]
    assert receipt["canonical_identity"]["next_target_key"] == page_target("full-history-next")
    bounded = launch(database, bounded=True)
    execute(database, bounded, page(older_posts()[:2], more=True, cursor="short-next"))
    receipt, _ = execute(
        database, bounded, page(older_posts()[2:], more=True, cursor="short-again"), cursor="short-next"
    )
    assert not receipt["canonical_identity"]["discovery_complete"]  # No cross-page streak.


def test_window_boundary_still_rejects_repeated_cursor(database):
    run = launch(database, bounded=True)
    execute(database, run, page([], more=True, cursor="window-repeat"))
    with pytest.raises(InvalidSourceError, match="repeated"):
        execute(database, run, page(older_posts(), more=True, cursor="window-repeat"), cursor="window-repeat")


def test_window_boundary_commit_rollback_duplicate_and_resume_preserve_proof(database):
    run = launch(database, bounded=True)
    store, _ = database
    posts = [dict(post("WindowRollbackPin", "2026-10-11T00:00:00+00:00"), is_pinned=True), *older_posts()]
    payload = page(posts, more=True, cursor="boundary-provider-cursor")
    owned = dict(run, stage=Stage.LISTING, target_key="page:root")
    owned["owner_token"] = str(store.reserve(**owned)["owner_token"])

    def fetch(**callbacks):
        callbacks["pre_source_request"]()
        callbacks["source_observer"](payload)
        return payload

    def crash(conn, **kwargs):
        write_post(conn, **kwargs)
        raise RuntimeError("boundary commit interrupted")

    with pytest.raises(RuntimeError, match="boundary commit"):
        ListingActivity(store, writer=crash, clock=lambda: OBSERVED).execute(
            **{k: v for k, v in owned.items() if k != "stage"}, fetch=fetch
        )
    assert sql(
        database,
        "select cursor_state,discovery_complete from social.instagram_ingestion_manifests "
        "where run_id=%s and stage='listing'",
        (run["run_id"],),
    ) == [({}, False)]
    assert sql(
        database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (run["run_id"],)
    ) == [(0,)]
    assert sql(database, "select count(*) from social.instagram_posts where shortcode='WindowRollbackPin'") == [(0,)]
    store.checkpoint_failure(**owned, failure=Failure("writer_failed"), checkpoint={"cursor": None}, requests=0)
    receipt, args = execute(database, run, payload)
    proof = receipt["canonical_identity"]
    state = sql(
        database,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='listing'",
        (run["run_id"],),
    )[0][0]
    duplicate = ListingActivity(store).execute(**args, fetch=lambda **_: pytest.fail("duplicate fetched"))
    assert duplicate["canonical_identity"] == proof
    store.control(run["run_id"], run["account"], action="cancel", idempotency_key="boundary-cancel")
    resumed = store.control(run["run_id"], run["account"], action="resume", idempotency_key="boundary-resume")
    assert resumed["generation"] == 3
    assert sql(
        database,
        "select cursor_state from social.instagram_ingestion_manifests where run_id=%s and stage='listing'",
        (run["run_id"],),
    ) == [(state,)]
    assert sql(
        database,
        "select attempts,requests,checkpoint,outcome from social.instagram_ingestion_targets "
        "where run_id=%s and stage='listing'",
        (run["run_id"],),
    ) == [(2, 2, {"cursor": None, "last_error_code": "writer_failed"}, "committed")]
    assert proof["stop_reason"] == "date_start_reached" and proof["provider_exhausted"] is False


def test_cancel_during_boundary_fetch_fences_commit_and_resumes_same_cursor(database):
    run = launch(database, bounded=True)
    store, _ = database
    payload = page(older_posts(), more=True, cursor="fenced-boundary")

    def cancel(**callbacks):
        callbacks["pre_source_request"]()
        callbacks["source_observer"](payload)
        store.control(run["run_id"], run["account"], action="cancel", idempotency_key="during-boundary-cancel")
        return payload

    with pytest.raises(LostOwnership):
        execute(database, run, payload, fetch=cancel)
    assert sql(
        database,
        "select cursor_state,discovery_complete from social.instagram_ingestion_manifests "
        "where run_id=%s and stage='listing'",
        (run["run_id"],),
    ) == [({}, False)]
    resumed = store.control(run["run_id"], run["account"], action="resume", idempotency_key="during-boundary-resume")
    receipt, _ = execute(database, dict(run, generation=resumed["generation"]), payload)
    assert receipt["canonical_identity"]["stop_reason"] == "date_start_reached"
    assert sql(
        database,
        "select attempts,requests,checkpoint from social.instagram_ingestion_targets "
        "where run_id=%s and stage='listing'",
        (run["run_id"],),
    ) == [(2, 2, {"cursor": None})]
