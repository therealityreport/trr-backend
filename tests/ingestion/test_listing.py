"""Real local PostgreSQL page/receipt/canonical atomicity; no source requests."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from tests.ingestion.test_canonical import database as canonical_database_fixture
from trr_backend.ingestion.canonical import write_post
from trr_backend.ingestion.domain import Failure, RunIntent, Stage
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


def launch(database, *, bounded=False):
    store, _ = database
    intent = RunIntent(
        account="fixture_account",
        scope="bounded_window" if bounded else "full_history",
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
    execute(database, run, final, cursor="next2")
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
