"""Native evidence and real isolated PostgreSQL; no configured TRR services."""

import copy
import json
import socket
import subprocess
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.canonical import OlderObservation
from trr_backend.ingestion.comments import (
    COMMENT_PARSER_VERSION,
    CommentActivity,
    CommentFetchSummary,
    CommentSourceError,
    _settlement,
    parse_comment_evidence,
)
from trr_backend.ingestion.domain import LostOwnership, RunIntent, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.persistence import Store

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/ingestion"
NATIVE = json.loads((FIXTURES / "comments_native.json").read_text())
OBSERVED = datetime(2026, 10, 16, 12, tzinfo=UTC)


def evidence(payload, observed=OBSERVED):
    return Evidence.capture(
        source_identity="instagram:comments:post1:comments_api",
        captured_at=observed,
        parser_version=COMMENT_PARSER_VERSION,
        payload=payload,
    )


@pytest.fixture(scope="module")
def database(tmp_path_factory, register_isolated_postgres):
    root = tmp_path_factory.mktemp("comments")
    binaries = Path("/opt/homebrew/opt/postgresql@17/bin")
    if not (binaries / "initdb").exists():
        pytest.skip("isolated PostgreSQL 17 binaries required")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "pg"
    subprocess.run(
        [
            str(binaries / "initdb"),
            "-D",
            str(data),
            "-A",
            "trust",
            "-U",
            "comments_test",
            "--no-locale",
            "--encoding=UTF8",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            str(binaries / "pg_ctl"),
            "-D",
            str(data),
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-p {port} -h 127.0.0.1 -k ''",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )

    @contextmanager
    def transaction():
        conn = psycopg2.connect(f"postgresql://comments_test@127.0.0.1:{port}/postgres")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    with register_isolated_postgres(host="127.0.0.1", port=port, dbname="postgres", user="comments_test"):
        try:
            with transaction() as conn, conn.cursor() as cur:
                cur.execute((FIXTURES / "canonical_schema.sql").read_text())
                cur.execute("""create role anon; create role authenticated; create role service_role;
                    alter table social.scrape_runs add column source_scope text,add column status text,
                    add column initiated_by text,add column config jsonb,add column cancelled_at timestamptz;""")
                cur.execute(
                    (ROOT / "supabase/migrations/20261016070000_instagram_ingestion_foundation.sql").read_text()
                )
                cur.execute(
                    (ROOT / "supabase/migrations/20261016070200_instagram_ingestion_comment_pages.sql").read_text()
                )
            yield Store(transaction), transaction
        finally:
            subprocess.run(
                [str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
            )


def setup(database, *, seed_missing=False):
    store, transaction = database
    run = store.start(
        RunIntent(account="bravotv", selected_tasks=(Stage.COMMENTS,)),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="comments-test",
        initiated_by="fixture",
    )
    shortcode = "post" + uuid4().hex
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=Stage.COMMENTS,
        target_keys=[shortcode],
        max_attempts=12,
        cursor_state={},
        expected_cursor_state={},
        page_id="first",
        seal=True,
    )
    target = store.reserve(run["run_id"], "bravotv", generation=1, stage=Stage.COMMENTS, target_key=shortcode)
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into social.instagram_posts(shortcode,username) values(%s,'bravotv') returning id", (shortcode,)
        )
        post_id = str(cur.fetchone()[0])
        if seed_missing:
            cur.execute(
                """insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at)
                values(%s,'old','old','Old','2026-01-01')""",
                (post_id,),
            )
    args = {
        "run_id": run["run_id"],
        "account": "bravotv",
        "generation": 1,
        "target_key": shortcode,
        "owner_token": str(target["owner_token"]),
        "post_id": post_id,
    }
    return store, transaction, args


def query(transaction, sql, params=()):
    with transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def source(payloads, summary=None):
    def fetch(**kwargs):
        for payload in payloads:
            kwargs["pre_source_request"]()
            kwargs["source_observer"]("comments_api", payload)
        return summary or CommentFetchSummary(expected_count=2, full_observation=True)

    return fetch


def test_rest_and_graphql_are_offline_and_preserve_native_fields():
    for kind in ("rest", "graphql"):
        page = parse_comment_evidence(evidence(NATIVE[kind]), pinned=True)
        assert page.exhausted
        assert page.comments[0].comment_id == "parent1"
        assert page.comments[0].replies[0].parent_comment_id == "parent1"
    page = parse_comment_evidence(evidence(NATIVE["rest"]), pinned=True)
    assert page.comments[0].owner_full_name == "Public Name"
    assert page.comments[0].owner_is_verified is False
    assert page.source_payloads[0]["comment_gif"]["url"] == "https://cdn.example/gif.mp4"
    assert "sessionid" not in page.source_payloads[0]["user"]
    assert "viewer" not in evidence(NATIVE["rest"]).read(pinned=True)
    assert page.observed_at == OBSERVED
    replies = parse_comment_evidence(evidence(NATIVE["replies"]), pinned=True)
    assert replies.comments[0].parent_comment_id == "parent1"


def test_invalid_node_does_not_silently_drop_source():
    with pytest.raises(CommentSourceError, match="identity"):
        parse_comment_evidence(evidence({"comments": [{"text": "bad"}], "has_more_comments": False}), pinned=True)
    with pytest.raises(CommentSourceError, match="Unsupported"):
        parse_comment_evidence(evidence({"text": "rendered HTML"}), pinned=True)


def test_partial_page_is_atomic_and_does_not_mark_missing(database):
    store, tx, args = setup(database, seed_missing=True)
    partial = copy.deepcopy(NATIVE["rest"])
    partial.update(has_more_comments=True, next_max_id="cursor1")
    result = CommentActivity(store, source([partial]), clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "unresolved"
    rows = query(
        tx,
        """select comment_id,is_missing,parent_comment_id,scraped_at from social.instagram_comments
        where post_id=%s order by comment_id""",
        (args["post_id"],),
    )
    assert len(rows) == 3 and not any(row["is_missing"] for row in rows)
    assert rows[1]["scraped_at"] == OBSERVED
    assert rows[2]["parent_comment_id"] is not None
    pages = query(tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],))
    assert len(pages) == 1 and pages[0]["observed_at"] == OBSERVED
    target = query(tx, "select * from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],))[0]
    assert target["outcome"] == "unresolved" and target["checkpoint"] == result["checkpoint"]
    assert target["requests"] == 1
    assert query(tx, "select * from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],)) == []


def test_complete_full_observation_marks_only_missing_post_rows(database):
    store, tx, args = setup(database, seed_missing=True)
    result = CommentActivity(store, source([NATIVE["rest"]]), clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "committed" and result["complete"]
    rows = query(
        tx,
        "select comment_id,is_missing from social.instagram_comments where post_id=%s order by comment_id",
        (args["post_id"],),
    )
    assert rows == [
        {"comment_id": "old", "is_missing": True},
        {"comment_id": "parent1", "is_missing": False},
        {"comment_id": "reply1", "is_missing": False},
    ]


@pytest.mark.parametrize("case", ["unknown", "repeated", "auth", "reply_gap", "headload"])
def test_incomplete_source_never_receives_target_receipt(database, case):
    store, tx, args = setup(database)
    payload = copy.deepcopy(NATIVE["rest"])
    summary = CommentFetchSummary(expected_count=2, full_observation=True)
    payloads = [payload]
    if case == "unknown":
        payload.pop("comment_count")
        summary = CommentFetchSummary(full_observation=True)
    elif case == "repeated":
        payload.update(has_more_comments=True, next_max_id="same")
        payloads = [payload, payload, NATIVE["rest"]]
    elif case == "auth":
        summary = CommentFetchSummary(expected_count=2, full_observation=True, auth_failed=True)
    elif case == "reply_gap":
        payload["comments"][0]["child_comment_count"] = 3
    elif case == "headload":
        payload["has_more_headload_comments"] = True
    result = CommentActivity(store, source(payloads, summary), clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "unresolved"
    assert not query(tx, "select * from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],))


def test_parse_failure_retains_evidence_before_any_canonical_write(database):
    store, tx, args = setup(database)
    with pytest.raises(CommentSourceError):
        CommentActivity(store, source([{"comments": [{}]}]), clock=lambda: OBSERVED).execute(**args)
    assert len(query(tx, "select * from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))) == 1
    assert not query(tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],))
    assert not query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))


def test_page_writer_failure_rolls_back_rows_checkpoint_and_receipt(database):
    store, tx, args = setup(database)
    from trr_backend.ingestion.canonical import write_comments

    def failing(conn, **kwargs):
        write_comments(conn, **kwargs)
        raise RuntimeError("injected after canonical write")

    with pytest.raises(RuntimeError, match="injected"):
        CommentActivity(store, source([NATIVE["rest"]]), writer=failing, clock=lambda: OBSERVED).execute(**args)
    assert not query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))
    assert not query(tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],))
    assert (
        query(tx, "select checkpoint from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],))[0][
            "checkpoint"
        ]
        == {}
    )


def test_cancel_between_capture_and_page_write_fences_late_worker(database):
    store, tx, args = setup(database)
    retained = store.retain_evidence

    def cancel(**kwargs):
        identity = retained(**kwargs)
        store.control(args["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
        return identity

    store.retain_evidence = cancel
    try:
        with pytest.raises(LostOwnership):
            CommentActivity(store, source([NATIVE["rest"]]), clock=lambda: OBSERVED).execute(**args)
    finally:
        store.retain_evidence = retained
    assert not query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))


def test_request_callback_cancels_before_dispatch(database):
    store, tx, args = setup(database)

    def fetch(**kwargs):
        store.control(args["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
        kwargs["pre_source_request"]()
        pytest.fail("request must not dispatch")

    with pytest.raises(LostOwnership):
        CommentActivity(store, fetch).execute(**args)
    assert not query(tx, "select * from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))


def test_explicit_comments_disabled_can_settle_unavailable(database):
    store, tx, args = setup(database)
    result = CommentActivity(store, source([{"comments_disabled": True}]), clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "source_unavailable"


def test_older_observation_is_rejected_without_page_receipt(database):
    store, tx, args = setup(database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at)
            values(%s,'parent1','new','New','2026-10-17')""",
            (args["post_id"],),
        )
    with pytest.raises(OlderObservation):
        CommentActivity(store, source([NATIVE["rest"]]), clock=lambda: OBSERVED).execute(**args)
    assert not query(tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],))


def test_embedded_reply_cursor_blocks_completion_even_when_count_met(database):
    store, tx, args = setup(database)
    payload = copy.deepcopy(NATIVE["graphql"])
    thread = payload["data"]["shortcode_media"]["edge_media_to_parent_comment"]["edges"][0]["node"][
        "edge_threaded_comments"
    ]
    thread["page_info"] = {"has_next_page": True, "end_cursor": "more-replies"}
    result = CommentActivity(store, source([payload]), clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "unresolved"
    assert not query(tx, "select * from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],))


def test_explicit_child_nodes_are_leaf_replies_when_count_omitted():
    payload = copy.deepcopy(NATIVE["replies"])
    connection = payload["data"][
        "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"
    ]
    del connection["edges"][0]["node"]["child_comment_count"]
    page = parse_comment_evidence(evidence(payload), pinned=True)
    assert page.comments[0].parent_comment_id == "parent1"
    assert page.source_payloads[0]["child_comment_count"] == 0


def test_resume_preserves_prior_rows_receipts_and_cursor_topology(database):
    from trr_backend.ingestion.domain import Failure

    store, tx, args = setup(database, seed_missing=True)
    first = copy.deepcopy(NATIVE["rest"])
    first.update(has_more_comments=True, next_max_id="tail1")
    result = CommentActivity(store, source([first]), clock=lambda: OBSERVED).execute(**args)
    owned = {key: value for key, value in args.items() if key != "post_id"}
    owned["stage"] = Stage.COMMENTS
    store.checkpoint_failure(**owned, failure=Failure("TAIL_PENDING"), checkpoint=result["checkpoint"], requests=0)
    target = store.reserve(args["run_id"], "bravotv", generation=1, stage=Stage.COMMENTS, target_key=args["target_key"])
    args["owner_token"] = str(target["owner_token"])
    tail = {"comments": [], "has_more_comments": False, "comment_count": 2}
    result = CommentActivity(store, source([tail]), clock=lambda: OBSERVED + timedelta(hours=1)).execute(
        **args, checkpoint=target["checkpoint"]
    )
    assert result["outcome"] == "committed"
    assert result["checkpoint"]["comment_pages"]["lanes"]["__top__"]["cursors"] == ["tail1"]
    assert result["checkpoint"]["comment_pages"]["scan_started_at"] == OBSERVED.isoformat()
    pages = query(
        tx,
        """select owner_generation from social.instagram_ingestion_comment_pages
        where run_id=%s order by owner_generation""",
        (args["run_id"],),
    )
    assert [row["owner_generation"] for row in pages] == [1, 2]
    assert len(query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))) == 3


def test_complete_scan_protects_newer_missing_candidates_and_other_posts(database):
    store, tx, args = setup(database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into social.instagram_posts(shortcode,username) values(%s,'other') returning id", (uuid4().hex,)
        )
        other_id = str(cur.fetchone()[0])
        cur.execute(
            """insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at) values
            (%s,'newer','newer','Newer','2026-10-17'),(%s,'other','other','Other','2026-01-01')""",
            (args["post_id"], other_id),
        )
    result = CommentActivity(store, source([NATIVE["rest"]]), clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "committed"
    rows = query(
        tx, "select comment_id,is_missing from social.instagram_comments where comment_id in ('newer','other')"
    )
    assert not any(row["is_missing"] for row in rows)


def test_source_network_runs_outside_db_transactions(database):
    store, tx, args = setup(database)
    active = 0

    @contextmanager
    def tracked_transaction():
        nonlocal active
        active += 1
        try:
            with tx() as conn:
                yield conn
        finally:
            active -= 1

    def fetch(**kwargs):
        assert active == 0
        kwargs["pre_source_request"]()
        assert active == 0
        kwargs["source_observer"]("comments_api", NATIVE["rest"])
        assert active == 0
        return CommentFetchSummary(expected_count=2, full_observation=True)

    isolated_store = Store(tracked_transaction)
    assert CommentActivity(isolated_store, fetch, clock=lambda: OBSERVED).execute(**args)["outcome"] == "committed"


def test_wrong_parent_post_is_rejected_before_source_dispatch(database):
    from trr_backend.ingestion.domain import Conflict

    store, tx, args = setup(database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into social.instagram_posts(shortcode,username) values(%s,'other') returning id", (uuid4().hex,)
        )
        args["post_id"] = str(cur.fetchone()[0])

    def fetch(**kwargs):
        pytest.fail("wrong parent must not dispatch source")

    with pytest.raises(Conflict, match="shortcode"):
        CommentActivity(store, fetch).execute(**args)


def test_preview_retained_but_never_promoted_to_full_comment_rows(database):
    store, tx, args = setup(database)

    def fetch(**kwargs):
        kwargs["source_observer"]("graphql_preview", NATIVE["graphql"])
        return CommentFetchSummary(expected_count=2, full_observation=True)

    result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "unresolved"
    assert len(query(tx, "select * from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))) == 1
    assert not query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))


def test_graphql_native_likes_and_utc_timestamp_aliases_survive_replay():
    payload = copy.deepcopy(NATIVE["graphql"])
    node = payload["data"]["shortcode_media"]["edge_media_to_parent_comment"]["edges"][0]["node"]
    node["edge_liked_by"] = {"count": 12}
    node["created_at_utc"] = 1792152000
    page = parse_comment_evidence(evidence(payload), pinned=True)
    assert page.comments[0].likes == 12
    assert page.comments[0].created_at == 1792152000
    assert page.source_payloads[0]["edge_liked_by"] == {"count": 12}
    assert page.source_payloads[0]["created_at_utc"] == 1792152000


def test_replay_rejects_unavailable_parser_version_and_unbound_child_pages():
    with pytest.raises(CommentSourceError, match="version"):
        parse_comment_evidence(evidence(NATIVE["rest"]), pinned=True, parser_version="unknown")
    payload = copy.deepcopy(NATIVE["replies"])
    del payload["parent_comment_id"]
    with pytest.raises(CommentSourceError, match="parent identity"):
        parse_comment_evidence(evidence(payload), pinned=True)


def test_transport_cannot_mutate_activity_checkpoint(database):
    store, tx, args = setup(database)

    def fetch(**kwargs):
        kwargs["checkpoint"]["unexpected"] = "transport-mutation"
        kwargs["source_observer"]("comments_api", NATIVE["rest"])
        return CommentFetchSummary(expected_count=2, full_observation=True)

    result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "committed"
    assert "unexpected" not in result["checkpoint"]


@pytest.mark.parametrize(
    "key,param",
    [
        ("next_max_id", "max_id"),
        ("next_min_id", "min_id"),
        ("cached_comments_cursor", "cached_comments_cursor"),
        ("tao_cursor", "tao_cursor"),
        ("bifilter_token", "bifilter_token"),
    ],
)
def test_native_top_cursor_direction_is_retained(key, param):
    payload = {"comments": [], "has_more_comments": True, key: "tail"}
    page = parse_comment_evidence(evidence(payload), pinned=True)
    assert page.next_cursor == "tail" and page.cursor_param == param and not page.exhausted


def test_native_child_cursor_direction_and_graphql_after_are_retained():
    for key, param in (("next_min_child_cursor", "min_id"), ("next_max_child_cursor", "max_id")):
        page = parse_comment_evidence(
            evidence(
                {
                    "parent_comment_id": "parent1",
                    "child_comments": [],
                    "has_more_tail_child_comments": True,
                    key: "tail",
                }
            ),
            pinned=True,
        )
        assert page.next_cursor == "tail" and page.cursor_param == param
    payload = copy.deepcopy(NATIVE["graphql"])
    connection = payload["data"]["shortcode_media"]["edge_media_to_parent_comment"]
    connection["page_info"] = {"has_next_page": True, "end_cursor": "graph-tail"}
    page = parse_comment_evidence(evidence(payload), pinned=True)
    assert page.next_cursor == "graph-tail" and page.cursor_param == "after"


def test_deliberate_root_restart_resets_manifest_and_preserves_partial_rows_until_complete(database):
    from trr_backend.ingestion.domain import Failure

    store, tx, args = setup(database)
    first = copy.deepcopy(NATIVE["rest"])
    first.update(has_more_comments=True, next_max_id="tail", cursor_payload={"source": "root"})
    result = CommentActivity(store, source([first]), clock=lambda: OBSERVED).execute(**args)
    assert result["checkpoint"]["comment_pages"]["lanes"]["__top__"]["started_from_root"]
    assert result["checkpoint"]["comment_pages"]["lanes"]["__top__"]["cursor_param"] == "max_id"
    owned = {key: value for key, value in args.items() if key != "post_id"}
    owned["stage"] = Stage.COMMENTS
    store.checkpoint_failure(**owned, failure=Failure("TAIL_PENDING"), checkpoint=result["checkpoint"], requests=0)
    reserved = store.reserve(
        args["run_id"], "bravotv", generation=1, stage=Stage.COMMENTS, target_key=args["target_key"]
    )
    args["owner_token"] = str(reserved["owner_token"])
    # A fresh root scan now sees zero comments. Old partial rows remain in the
    # database until this full scan proves they are missing.
    empty_root = {"comments": [], "has_more_comments": False, "comment_count": 0, "cursor_payload": {"source": "root"}}
    result = CommentActivity(
        store,
        source([empty_root], CommentFetchSummary(expected_count=0, full_observation=True)),
        clock=lambda: OBSERVED + timedelta(hours=1),
    ).execute(**args)
    assert result["outcome"] == "committed"
    assert result["checkpoint"]["comment_pages"]["observed"] == {}
    assert result["checkpoint"]["comment_pages"]["scan_started_at"] == (OBSERVED + timedelta(hours=1)).isoformat()
    assert not result["checkpoint"]["comment_pages"]["lanes"]["__top__"]["repeated"]
    rows = query(tx, "select is_missing from social.instagram_comments where post_id=%s", (args["post_id"],))
    assert len(rows) == 2 and all(row["is_missing"] for row in rows)
    assert (
        len(query(tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],))) == 2
    )


def test_transport_activity_and_postgres_resume_after_crash_between_native_pages(database):
    from types import SimpleNamespace

    from trr_backend.ingestion.comment_transport import ScraplingCommentSource
    from trr_backend.ingestion.domain import Failure

    store, tx, args = setup(database)
    calls = []
    closed = []

    class Fetcher:
        def __init__(self, **kwargs):
            self.options = kwargs

        async def fetch_comments_for_shortcode(self, shortcode, **kwargs):
            assert self.options["consume_credentials_only"] is True
            assert kwargs["top_level_cursor"] is None
            self.options["before_source_request"]("comments_api")
            partial = copy.deepcopy(NATIVE["rest"])
            partial.update(has_more_comments=True, next_max_id="durable-tail")
            self.options["source_payload_observer"]("comments_api", partial)
            # Simulate process failure before a legacy fetch-summary checkpoint.
            raise RuntimeError("injected crash after durable native page")

        async def aclose(self):
            closed.append("crashed-fetcher")

    transport = ScraplingCommentSource(
        cookies=[],
        raw_cookies={},
        browser_account_id=None,
        expected_count=2,
        fetcher_factory=Fetcher,
    )
    with pytest.raises(RuntimeError, match="injected crash"):
        CommentActivity(store, transport, clock=lambda: OBSERVED).execute(**args)
    target = query(tx, "select * from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],))[0]
    checkpoint = target["checkpoint"]
    lane = checkpoint["comment_pages"]["lanes"]["__top__"]
    assert lane["cursor_param"] == "max_id" and lane["started_from_root"]
    owned = {key: value for key, value in args.items() if key != "post_id"}
    owned["stage"] = Stage.COMMENTS
    store.checkpoint_failure(**owned, failure=Failure("WORKER_INTERRUPTED"), checkpoint=checkpoint, requests=0)
    reservation = store.reserve(
        args["run_id"], "bravotv", generation=1, stage=Stage.COMMENTS, target_key=args["target_key"]
    )
    args["owner_token"] = str(reservation["owner_token"])

    class ResumedFetcher(Fetcher):
        async def fetch_comments_for_shortcode(self, shortcode, **kwargs):
            calls.append(kwargs)
            self.options["before_source_request"]("comments_api")
            self.options["source_payload_observer"](
                "comments_api",
                {"comments": [], "has_more_comments": False, "comment_count": 2},
            )
            return SimpleNamespace(
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                reported_comment_count=2,
                reply_checkpoints=[],
                top_level_checkpoint=None,
                diagnostic_metadata={},
            )

        async def aclose(self):
            closed.append("resumed-fetcher")

    resumed = ScraplingCommentSource(
        cookies=[],
        raw_cookies={},
        browser_account_id=None,
        expected_count=2,
        fetcher_factory=ResumedFetcher,
    )
    result = CommentActivity(store, resumed, clock=lambda: OBSERVED).execute(**args)
    assert calls[0]["top_level_cursor"] == "durable-tail"
    assert calls[0]["top_level_cursor_param"] == "max_id"
    assert result["outcome"] == "committed" and result["complete"]
    assert not result["checkpoint"]["comment_pages"]["lanes"]["__top__"]["repeated"]
    assert closed == ["crashed-fetcher", "resumed-fetcher"]
    assert len(query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))) == 2
    assert (
        len(query(tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],))) == 2
    )
    target = query(
        tx, "select attempts,requests from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
    )[0]
    assert target == {"attempts": 2, "requests": 2}


def test_missing_reconciliation_protects_comment_observed_between_native_pages(database):
    store, tx, args = setup(database, seed_missing=True)
    midscan = OBSERVED + timedelta(seconds=30)
    final_capture = OBSERVED + timedelta(minutes=2)
    clock = iter((OBSERVED, OBSERVED + timedelta(minutes=1), final_capture))

    def fetch(**kwargs):
        first = copy.deepcopy(NATIVE["rest"])
        first.update(has_more_comments=True, next_max_id="tail", cursor_payload={"source": "root"})
        kwargs["source_observer"]("comments_api", first)
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at)
                values(%s,'concurrent','concurrent','Created during scan',%s)""",
                (args["post_id"], midscan),
            )
        kwargs["source_observer"](
            "comments_api",
            {"comments": [], "has_more_comments": False, "comment_count": 2},
        )
        return CommentFetchSummary(expected_count=2, full_observation=True)

    result = CommentActivity(store, fetch, clock=lambda: next(clock)).execute(**args)
    assert result["outcome"] == "committed"
    rows = query(tx, "select comment_id,is_missing from social.instagram_comments where post_id=%s", (args["post_id"],))
    missing = {row["comment_id"]: row["is_missing"] for row in rows}
    assert missing["concurrent"] is False
    assert missing["old"] is True
    assert result["checkpoint"]["comment_pages"]["scan_started_at"] == OBSERVED.isoformat()
    assert result["receipt"]["observed_at"] == final_capture


@pytest.mark.parametrize(
    "failure",
    [
        {"auth_failed": True},
        {"retryable": True},
        {"fetch_failed": True},
        {"top_level_checkpoint": {"next_top_level_cursor": "pending"}},
    ],
)
def test_disabled_comments_with_invalid_source_summary_stay_unresolved(database, failure):
    store, tx, args = setup(database)
    summary = CommentFetchSummary(expected_count=0, full_observation=True, **failure)
    result = CommentActivity(
        store,
        source([{"comments_disabled": True}], summary),
        clock=lambda: OBSERVED,
    ).execute(**args)
    assert result["outcome"] == "unresolved" and not result["complete"]
    assert not query(tx, "select * from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],))
    assert len(query(tx, "select * from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))) == 1


def test_missing_scan_start_after_legacy_tail_resume_never_marks_rows_missing(database):
    from trr_backend.ingestion.domain import Failure

    store, tx, args = setup(database, seed_missing=True)
    partial = copy.deepcopy(NATIVE["rest"])
    partial.update(has_more_comments=True, next_max_id="tail")
    result = CommentActivity(store, source([partial]), clock=lambda: OBSERVED).execute(**args)
    checkpoint = result["checkpoint"]
    del checkpoint["comment_pages"]["scan_started_at"]
    owned = {key: value for key, value in args.items() if key != "post_id"}
    owned["stage"] = Stage.COMMENTS
    store.checkpoint_failure(**owned, failure=Failure("LEGACY_TAIL"), checkpoint=checkpoint, requests=0)
    target = store.reserve(args["run_id"], "bravotv", generation=1, stage=Stage.COMMENTS, target_key=args["target_key"])
    args["owner_token"] = str(target["owner_token"])
    tail = {"comments": [], "has_more_comments": False, "comment_count": 2}
    result = CommentActivity(store, source([tail]), clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)
    assert result["outcome"] == "unresolved"
    rows = query(tx, "select is_missing from social.instagram_comments where post_id=%s", (args["post_id"],))
    assert len(rows) == 3 and not any(row["is_missing"] for row in rows)
    assert not query(tx, "select * from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],))


def settlement_state():
    return {
        "expected": 3,
        "adapter_family": "instagram_public_relay",
        "adapter_version": 1,
        "observed": {"p": {"parent": None, "expected_replies": 0}},
        "lanes": {"__top__": {"exhausted": True, "started_from_root": True}},
    }


@pytest.mark.parametrize(
    "expected,observed,basis,gap",
    [
        (1, 1, "count_parity", 0),
        (0, 1, "count_parity", 0),
        (3, 1, "public_native_exhaustion", 2),
        (3, 0, "public_native_exhaustion", 3),
    ],
)
def test_settlement_records_expected_and_observed_totals(expected, observed, basis, gap):
    state = settlement_state()
    if not observed:
        state["observed"] = {}
    assert _settlement(state, CommentFetchSummary(expected_count=expected, full_observation=True)) == {
        "basis": basis,
        "expected_total": expected,
        "observed_total": observed,
        "count_gap": gap,
    }


@pytest.mark.parametrize("expected", [None, -1, True, 3.0, "3"])
def test_settlement_requires_known_nonnegative_integer_count(expected):
    state = settlement_state()
    state["expected"] = expected
    assert _settlement(state, CommentFetchSummary(full_observation=True)) is None


@pytest.mark.parametrize(
    "failure",
    [
        {"full_observation": False},
        {"fetch_failed": True},
        {"auth_failed": True},
        {"retryable": True},
        {"top_level_checkpoint": {"after": "tail"}},
        {"reply_checkpoints": ({"after": "tail"},)},
        {"reply_lanes_rejected": ("p",)},
    ],
)
def test_gap_settlement_preserves_source_summary_guards(failure):
    summary = CommentFetchSummary(**({"expected_count": 3, "full_observation": True} | failure))
    assert _settlement(settlement_state(), summary) is None


@pytest.mark.parametrize(
    "mutation",
    [
        "rest",
        "adapter_version",
        "root",
        "top_pending",
        "repeated",
        "child_shortfall",
        "unknown_child",
        "child_pending",
        "unsupported_topology",
        "unknown_child_unproved_root",
    ],
)
def test_gap_settlement_preserves_native_lane_and_topology_guards(mutation):
    state = settlement_state()
    top = state["lanes"]["__top__"]
    if mutation == "rest":
        state.pop("adapter_family")
    elif mutation == "adapter_version":
        state["adapter_version"] = 2
    elif mutation == "root":
        top.pop("started_from_root")
    elif mutation == "top_pending":
        top["exhausted"] = False
    elif mutation == "repeated":
        top["repeated"] = True
    elif mutation == "child_shortfall":
        state["observed"]["p"]["expected_replies"] = 1
    elif mutation == "unknown_child":
        state["observed"]["p"]["expected_replies"] = None
    elif mutation == "child_pending":
        state["lanes"]["p"] = {"exhausted": False}
    elif mutation == "unsupported_topology":
        state["observed"]["p"]["unsupported_reply_topology"] = True
    else:
        state["observed"]["p"]["expected_replies"] = None
        state["lanes"]["p"] = {"exhausted": True, "started_from_root": True}
    assert _settlement(state, CommentFetchSummary(expected_count=3, full_observation=True)) is None


def test_gap_settlement_accepts_unknown_child_only_with_existing_native_root_proof():
    state = settlement_state()
    state["observed"]["p"]["expected_replies"] = None
    state["lanes"]["p"] = {
        "exhausted": True,
        "started_from_root": True,
        "relay_doc_id": "child",
        "relay_friendly_name": "child",
        "sort_order": "popular",
        "page_size": 15,
    }
    assert _settlement(state, CommentFetchSummary(expected_count=3, full_observation=True))["count_gap"] == 2


def test_rest_count_shortfall_stays_unresolved_and_does_not_mark_missing(database):
    store, tx, args = setup(database, seed_missing=True)
    result = CommentActivity(
        store,
        source([NATIVE["rest"]], CommentFetchSummary(expected_count=3, full_observation=True)),
        clock=lambda: OBSERVED,
    ).execute(**args)
    assert result["complete"] is False and "settlement" not in result["checkpoint"]["comment_pages"]
    assert query(
        tx, "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'", (args["post_id"],)
    ) == [{"is_missing": False}]


def test_public_native_count_gap_preserves_unseen_saved_comments_and_media(database):
    store, tx, args = setup(database, seed_missing=True)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_comments
            (post_id,comment_id,parent_comment_id,username,text,is_reply,scraped_at,media_urls,hosted_media_urls,raw_data)
            select post_id,'old-child',id,'retained','Retained reply',true,'2026-01-01',
                '["https://media.example/original.gif"]'::jsonb,'["https://media.example/hosted.gif"]'::jsonb,
                '{"history":"retained"}'::jsonb from social.instagram_comments
            where post_id=%s and comment_id='old'""",
            (args["post_id"],),
        )
    saved_query = (
        "select to_jsonb(c) as saved from social.instagram_comments c where post_id=%s "
        "and comment_id in ('old','old-child') order by comment_id"
    )
    before = query(tx, saved_query, (args["post_id"],))
    payload = copy.deepcopy(NATIVE["rest"])
    payload["cursor_payload"] = {
        "adapter_family": "instagram_public_relay",
        "adapter_version": 1,
        "source": "root",
        "relay_doc_id": "fixture-top-doc",
        "relay_friendly_name": "fixture-top-operation",
        "sort_order": "popular",
        "page_size": 15,
    }
    result = CommentActivity(
        store,
        source([payload], CommentFetchSummary(expected_count=4, full_observation=True)),
        clock=lambda: OBSERVED,
    ).execute(**args)
    assert result["outcome"] == "committed"
    assert result["receipt"]["canonical_identity"]["settlement"] == {
        "basis": "public_native_exhaustion",
        "expected_total": 4,
        "observed_total": 2,
        "count_gap": 2,
    }
    assert query(tx, saved_query, (args["post_id"],)) == before
    observed = query(
        tx,
        "select comment_id,is_missing from social.instagram_comments where post_id=%s "
        "and comment_id in ('parent1','reply1') order by comment_id",
        (args["post_id"],),
    )
    assert observed == [
        {"comment_id": "parent1", "is_missing": False},
        {"comment_id": "reply1", "is_missing": False},
    ]


def seed_resume_pages(database):
    store, tx, args = setup(database)
    old_root = copy.deepcopy(NATIVE["rest"])
    old_root.update(has_more_comments=True, next_max_id="old-tail", cursor_payload={"source": "root"})
    current_root = copy.deepcopy(old_root)
    current_root["next_max_id"] = "current-tail"
    tail = {"comments": [], "has_more_comments": True, "next_max_id": "final-tail", "comment_count": 2}
    activity = CommentActivity(
        store,
        source([old_root, current_root, tail], CommentFetchSummary(expected_count=2, fetch_failed=True)),
        clock=lambda: OBSERVED,
    )
    result = activity.execute(**args)
    pages = query(
        tx,
        "select * from social.instagram_ingestion_comment_pages where run_id=%s order by committed_at,page_id",
        (args["run_id"],),
    )
    # All three pages share a commit time. Only the ordered UUID boundary may
    # distinguish the superseded page from the current root and its tail.
    page_id_base = uuid4().int & ~0xFFFF
    with tx() as conn, conn.cursor() as cur:
        for position, page in enumerate(pages, 1):
            cur.execute(
                "update social.instagram_ingestion_comment_pages set committed_at=%s,page_id=%s where page_id=%s",
                (OBSERVED, str(UUID(int=page_id_base + position)), page["page_id"]),
            )
    owned = {key: value for key, value in args.items() if key != "post_id"}
    owned["stage"] = Stage.COMMENTS
    return activity, tx, owned, result["checkpoint"]["comment_pages"], pages


def test_resume_fetches_only_current_root_evidence_and_one_final_checkpoint(database, monkeypatch):
    import trr_backend.ingestion.comments as comments_module

    activity, _, owned, state, pages = seed_resume_pages(database)
    fetched = []
    checkpoints = []

    class TrackingCursor(RealDictCursor):
        def fetchall(self):
            rows = super().fetchall()
            if self.description and "payload" in {field.name for field in self.description}:
                fetched.extend(rows)
            return rows

        def fetchone(self):
            row = super().fetchone()
            if self.description and [field.name for field in self.description] == ["checkpoint"]:
                checkpoints.append(row)
            return row

    monkeypatch.setattr(comments_module, "RealDictCursor", TrackingCursor)
    nodes, proof = activity._resume_inventory(owned, state)
    assert {node.source_payload["pk"] for node in nodes} == {"parent1", "reply1"}
    assert [str(row["evidence_id"]) for row in fetched] == [str(page["evidence_id"]) for page in pages[1:]]
    expected_fields = {
        "evidence_id",
        "source_identity",
        "captured_at",
        "parser_version",
        "payload",
        "content_hash",
        "schema_version",
    }
    assert all(set(row) == expected_fields for row in fetched)
    assert len(checkpoints) == 1 and checkpoints[0]["checkpoint"] == pages[-1]["checkpoint"]
    assert proof[0] == str(pages[-1]["evidence_id"])


@pytest.mark.parametrize("root_kind", ["missing", "other_target"])
def test_resume_root_must_belong_to_exact_target(database, root_kind):
    activity, _, owned, state, _ = seed_resume_pages(database)
    if root_kind == "other_target":
        _, _, _, other_state, _ = seed_resume_pages(database)
        state["scan_root_evidence_id"] = other_state["scan_root_evidence_id"]
    else:
        state["scan_root_evidence_id"] = str(uuid4())
    with pytest.raises(CommentSourceError, match="Durable scan root lacks committed evidence"):
        activity._resume_inventory(owned, state)


@pytest.mark.parametrize("corruption", ["content_hash", "final_checkpoint", "parent_identity"])
def test_resume_still_rejects_current_scan_corruption(database, corruption):
    activity, tx, owned, state, pages = seed_resume_pages(database)
    if corruption == "content_hash":
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "update social.instagram_ingestion_evidence set content_hash=%s where evidence_id=%s",
                ("0" * 64, pages[1]["evidence_id"]),
            )
        expected = "Evidence content hash mismatch"
    elif corruption == "final_checkpoint":
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "update social.instagram_ingestion_comment_pages set checkpoint='{}'::jsonb where evidence_id=%s",
                (pages[-1]["evidence_id"],),
            )
        expected = "Durable comment inventory lacks matching committed page proof"
    else:
        state["observed"]["reply1"]["parent"] = "wrong-parent"
        expected = "Retained resume inventory changed parent identity"
    with pytest.raises(ValueError, match=expected):
        activity._resume_inventory(owned, state)


def test_resume_without_root_keeps_legacy_full_history_and_scan_cutoff_behavior(database):
    activity, tx, owned, state, pages = seed_resume_pages(database)
    state.pop("scan_root_evidence_id")
    state.pop("scan_started_at")
    checkpoint = copy.deepcopy(pages[-1]["checkpoint"])
    checkpoint["comment_pages"].pop("scan_root_evidence_id")
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_comment_pages set checkpoint=%s::jsonb where evidence_id=%s",
            (json.dumps(checkpoint), pages[-1]["evidence_id"]),
        )
    nodes, proof = activity._resume_inventory(owned, state)
    assert len(nodes) == 2 and proof[0] == str(pages[-1]["evidence_id"])


def test_page_checkpoint_serialized_once_after_custom_writer_mutation(database, monkeypatch):
    import trr_backend.ingestion.comments as comments_module

    store, tx, args = setup(database)
    owned = {key: value for key, value in args.items() if key != "post_id"}
    owned["stage"] = Stage.COMMENTS
    store.acquire(**owned)
    retained = evidence(NATIVE["rest"])
    evidence_id = store.retain_evidence(**owned, evidence=retained)
    checkpoint = {"writer_marker": None}
    calls = []
    original = comments_module.stable_json

    def serialize(value):
        if value is checkpoint:
            calls.append(copy.deepcopy(value))
        return original(value)

    def writer(conn, **kwargs):
        checkpoint["writer_marker"] = "after-writer"
        return {"post_id": args["post_id"], "comment_ids": {}}

    monkeypatch.setattr(comments_module, "stable_json", serialize)
    activity = CommentActivity(store, writer=writer)
    activity._persist_page(
        owned,
        post_id=args["post_id"],
        evidence=retained,
        evidence_id=evidence_id,
        page=parse_comment_evidence(retained, pinned=True),
        expected_checkpoint={},
        checkpoint=checkpoint,
        metadata_writes_enabled=True,
        assignment=None,
    )
    assert calls == [{"writer_marker": "after-writer"}]
    saved = query(
        tx,
        """select p.checkpoint as page_checkpoint,t.checkpoint as target_checkpoint
        from social.instagram_ingestion_comment_pages p join social.instagram_ingestion_targets t
        using(run_id,stage,target_key) where p.evidence_id=%s""",
        (evidence_id,),
    )[0]
    assert saved["page_checkpoint"] == saved["target_checkpoint"] == checkpoint
