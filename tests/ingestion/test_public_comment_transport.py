"""Durable logged-out Relay: real fetcher, explicit mock HTTP, isolated PG."""

import json
import time
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from urllib.parse import parse_qs

import httpx
import pytest

from tests.ingestion import test_comment_resume as resume_tests
from tests.ingestion import test_comments as existing
from trr_backend.ingestion.comment_transport import PublicRelayCommentSource, PublicRelayResume, ScraplingCommentSource
from trr_backend.ingestion.comments import (
    CommentActivity,
    CommentFetchSummary,
    CommentSourceError,
    CursorStalledError,
    _advance,
    _complete,
    parse_comment_evidence,
)
from trr_backend.ingestion.domain import Failure, LostOwnership, RunIntent, SourceProfile, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.comments_scrapling import fetcher as maintained

database = existing.database
deny_unmocked_http = resume_tests.deny_unmocked_http
OBSERVED = existing.OBSERVED
TOP = "xdt_api__v1__media__media_id__comments__connection"
CHILD = "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"


@pytest.mark.parametrize(
    "attachment",
    [
        {"giphy_media_info": {"id": "fixture-gif", "images": {"fixed_height": {"url": "https://media.example/g.gif"}}}},
        {"attachment": {"image_versions2": {"candidates": [{"url": "https://media.example/image.jpg"}]}}},
        {"comment_media": {"video_versions": [{"url": "https://media.example/video.mp4"}]}},
        {"sticker": {"image_url": "https://media.example/sticker.webp"}},
    ],
)
def test_native_media_only_dto_accepts_supported_attachment_urls(attachment):
    native = {**actual_parent_shape("media-parent"), "text": "", **attachment}
    dto = maintained._graphql_comment_to_instagram_comment(native, shortcode="fixture", post_url="fixture")
    assert dto is not None and dto.text == "" and len(dto.media_urls) == 1
    assert dto.media_urls[0].startswith("https://media.example/")


@pytest.mark.parametrize(
    "extra",
    [
        {},
        {"giphy_media_info": {"id": "fixture-gif"}},
        {"giphy_media_info": {"images": {"fixed_height": {"url": "javascript:placeholder"}}}},
        {"attachment": {"url": "https://[malformed"}},
        {"url": "https://media.example/untyped"},
        {"user": {"username": "fixture", "profile_pic_url": "https://media.example/avatar.jpg"}},
    ],
)
def test_empty_native_placeholder_or_author_url_is_not_media_comment(extra):
    native = {**actual_parent_shape("empty"), "text": "", **extra}
    assert maintained._graphql_comment_to_instagram_comment(native, shortcode="fixture", post_url="fixture") is None


@pytest.mark.parametrize("finish_tail", [False, True])
def test_retained_shape_gif_parent_observer_dto_and_fresh_reply_hydration(monkeypatch, finish_tail):
    """Real decoder/fetcher: the retained empty-text GIF must own a fresh child lane."""
    gif_url = "https://media.example/retained-shape.gif"
    native = {
        **actual_parent_shape("gif-parent"),
        "text": "",
        "giphy_media_info": {"id": "fixture-gif", "images": {"fixed_height": {"url": gif_url}}},
    }
    state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
    retained, dtos, calls = [], [], []
    convert = maintained._graphql_comment_to_instagram_comment

    def record_dto(*args, **kwargs):
        dto = convert(*args, **kwargs)
        if dto is not None:
            dtos.append(dto)
        return dto

    monkeypatch.setattr(maintained, "_graphql_comment_to_instagram_comment", record_dto)

    def observe(kind, payload):
        evidence = existing.evidence(payload)
        retained.append(evidence)
        _advance(state, parse_comment_evidence(evidence, pinned=True), scan_started_at=OBSERVED)

    def handler(request):
        if request.method == "GET":
            calls.append("bootstrap")
            return bootstrap()
        _, variables = body(request)
        parent, cursor = variables.get("parent_comment_id"), variables.get("after")
        calls.append((parent, cursor))
        if parent is None:
            return httpx.Response(200, json=page([native]))
        assert parent == "gif-parent"
        gif = next(dto for dto in dtos if dto.comment_id == parent)
        assert gif.text == "" and gif.media_urls == [gif_url] and gif.giphy_media_info == native["giphy_media_info"]
        if cursor is None:
            return httpx.Response(200, json=page([node("reply")], child=True, more=True, after="reply-tail"))
        assert cursor == "reply-tail"
        # Both expected rows are present, but native reply pagination is unfinished.
        assert not _complete(state, CommentFetchSummary(expected_count=2, full_observation=True))
        if not finish_tail:
            raise RuntimeError("fixture interrupted GIF reply tail")
        return httpx.Response(200, json=page([], child=True))

    fetch, _ = source(monkeypatch, handler, expected=2)
    args = {"shortcode": "fixture", "checkpoint": {}, "source_observer": observe, "pre_source_request": lambda: None}
    if finish_tail:
        summary = fetch(**args)
        assert _complete(state, summary)
    else:
        with pytest.raises(RuntimeError, match="fixture interrupted GIF reply tail"):
            fetch(**args)
        assert not _complete(state, CommentFetchSummary(expected_count=2, full_observation=True))
    assert calls == ["bootstrap", (None, None), ("gif-parent", None), ("gif-parent", "reply-tail")]
    parsed_parent = parse_comment_evidence(retained[0], pinned=True).comments[0]
    assert parsed_parent.text == "" and parsed_parent.raw_data["giphy_media_info"] == native["giphy_media_info"]


def node(identity, count=0, **extra):
    return {
        "pk": identity,
        "text": identity,
        "user": {"pk": "100", "username": "fixture"},
        "child_comment_count": count,
        **extra,
    }


def page(nodes, *, more=False, after=None, child=False, count=None):
    connection = {
        "edges": [{"node": item} for item in nodes],
        "page_info": {"has_next_page": more, "end_cursor": after},
    }
    if count is not None:
        connection["count"] = count
    return {"data": {CHILD if child else TOP: connection}}


def source(monkeypatch, handler, *, expected):
    created = []

    def no_browser(*args, **kwargs):
        pytest.fail("Public source constructed a browser")

    monkeypatch.setattr(maintained, "build_stealthy_fetcher", no_browser)

    # Stable constructor: resume tests build a second source in one test. Never
    # wrap the previous mock factory and accidentally pass transport twice.
    httpx_constructor = resume_tests.HTTP_CLIENT

    def client(**kwargs):
        assert kwargs.get("proxy") is None
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        assert "cookies" not in kwargs
        return httpx_constructor(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(maintained.httpx, "AsyncClient", client)
    monkeypatch.setattr(
        maintained,
        "build_comments_async_client",
        lambda **kwargs: pytest.fail("Durable public Relay used an environment-selected transport"),
    )

    def factory(**kwargs):
        assert kwargs["public_only"] is True
        assert kwargs["cookies"] == [] and kwargs["raw_cookies"] == {}
        assert kwargs["browser_account_id"] is None and kwargs["proxy_config"] is None
        fetcher = maintained.InstagramCommentsScraplingFetcher(**kwargs)
        fetcher._api_delay_seconds = fetcher._global_api_delay_seconds = 0
        created.append(fetcher)
        return fetcher

    return PublicRelayCommentSource(expected_count=expected, fetcher_factory=factory), created


def body(request):
    fields = {key: value[0] for key, value in parse_qs(request.content.decode()).items()}
    variables = json.loads(fields["variables"])
    assert fields["__user"] == "0" and fields["av"] == "0"
    assert variables["__relay_internal__pv__PolarisIsLoggedInrelayprovider"] is False
    assert "sessionid" not in request.headers.get("cookie", "") and "ds_user_id" not in request.headers.get(
        "cookie", ""
    )
    return fields, variables


def bootstrap():
    return httpx.Response(
        200, text='"LSD",[],{"token":"ephemeral-fixture-lsd"}', headers={"set-cookie": "csrftoken=anonymous; Path=/"}
    )


def actual_parent_shape(identity, count=None):
    """Synthetic values with the exact retained public parent field shape."""
    return {
        "__typename": "XDTCommentDict",
        "child_comment_count": count,
        "comment_like_count": 0,
        "created_at": 1772766459,
        "giphy_media_info": None,
        "has_translation": None,
        "is_covered": False,
        "is_edited": False,
        "parent_comment_id": None,
        "pk": identity,
        "text": f"synthetic public comment {identity}",
        "user": {"pk": "100", "username": "fixture"},
    }


def setup(database, *, seed_missing=False, bounded=False):
    """Reuse canonical fixtures while freezing a public run at creation."""
    from uuid import uuid4

    store, tx = database
    run = store.start(
        RunIntent(
            account="bravotv",
            selected_tasks=(Stage.COMMENTS,),
            source_profile=SourceProfile.for_mode("public"),
            **(
                {
                    "scope": "recent_window",
                    "date_start": datetime(2026, 9, 1, tzinfo=UTC),
                    "date_end": datetime(2026, 10, 1, tzinfo=UTC),
                }
                if bounded
                else {}
            ),
        ),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="public-comments-test",
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
    with tx() as conn, conn.cursor() as cur:
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
    return (
        store,
        tx,
        {
            "run_id": run["run_id"],
            "account": "bravotv",
            "generation": 1,
            "target_key": shortcode,
            "owner_token": str(target["owner_token"]),
            "post_id": post_id,
        },
    )


@pytest.mark.parametrize("crash_phase,cancel", [("top_tail", False), ("child_tail", True)])
def test_public_real_relay_receipts_resume_parent_first_with_exact_query_and_original_times(
    database, monkeypatch, crash_phase, cancel
):
    store, tx, args = setup(database, seed_missing=True)
    calls = []
    attempt = 0
    first_doc = {}

    def handler(request):
        if request.method == "GET":
            calls.append((attempt, "bootstrap", None))
            return bootstrap()
        fields, variables = body(request)
        parent = variables.get("parent_comment_id")
        lane = parent or "top"
        cursor = variables.get("after")
        calls.append((attempt, lane, cursor))
        first_doc.setdefault(lane, fields["doc_id"])
        assert fields["doc_id"] == first_doc[lane]
        if not parent:
            if cursor is None:
                assert attempt == 0, "durable resume refetched the top root"
                return httpx.Response(200, json=page([node("p1", 2)], more=True, after="top-1"))
            assert cursor == "top-1"
            if attempt == 0 and crash_phase == "top_tail":
                raise RuntimeError("interrupted top tail")
            return httpx.Response(200, json=page([node("p2")]))
        assert parent == "p1"
        if cursor is None:
            assert not (attempt == 1 and crash_phase == "child_tail"), "resumed child root instead of exact after"
            return httpx.Response(200, json=page([node("r1")], child=True, more=True, after="child-1", count=2))
        assert cursor == "child-1"
        if attempt == 0:
            raise RuntimeError("interrupted child tail")
        return httpx.Response(200, json=page([node("r2")], child=True, count=2))

    fetch, _ = source(monkeypatch, handler, expected=4)
    with pytest.raises(RuntimeError, match="interrupted"):
        CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    checkpoint = existing.query(
        tx, "select checkpoint from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
    )[0]["checkpoint"]
    assert checkpoint["comment_pages"]["adapter_family"] == "instagram_public_relay"
    assert (
        existing.query(
            tx,
            "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'",
            (args["post_id"],),
        )[0]["is_missing"]
        is False
    )
    receipts = existing.query(
        tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
    )
    assert len(receipts) == (1 if crash_phase == "top_tail" else 3)
    resume_tests.retry(database, args, cancel=cancel)
    attempt = 1
    fetch, _ = source(monkeypatch, handler, expected=4)
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)
    assert result["complete"] is True
    resumed = [item[1:] for item in calls if item[0] == 1]
    assert resumed == (
        [("bootstrap", None), ("top", "top-1"), ("p1", None), ("p1", "child-1")]
        if crash_phase == "top_tail"
        else [("bootstrap", None), ("p1", "child-1")]
    )
    comments = {
        row["comment_id"]: row
        for row in existing.query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))
    }
    assert comments["p1"]["scraped_at"] == OBSERVED
    assert comments["r1"]["parent_comment_id"] == comments["p1"]["id"]
    assert comments["r1"]["scraped_at"] == (OBSERVED if crash_phase == "child_tail" else OBSERVED + timedelta(hours=1))
    assert comments["old"]["is_missing"] is True
    evidence = existing.query(
        tx, "select payload::text from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],)
    )
    assert all("ephemeral-fixture-lsd" not in row["payload"] and "anonymous" not in row["payload"] for row in evidence)


def test_public_count_met_still_drains_native_top_and_reply_connections(database, monkeypatch):
    store, tx, args = setup(database)
    requests = []

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        requests.append(variables)
        if variables.get("parent_comment_id"):
            return httpx.Response(200, json=page([node("r")], child=True, count=1))
        if variables.get("after"):
            return httpx.Response(200, json=page([]))
        return httpx.Response(200, json=page([node("p", 1)], more=True, after="still-more"))

    fetch, _ = source(monkeypatch, handler, expected=1)
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert result["complete"] is True
    assert len(requests) == 3 and requests[1]["after"] == "still-more" and requests[2]["parent_comment_id"] == "p"


@pytest.mark.parametrize("unknown_count,missing_pagination", [(True, False), (False, True)])
def test_public_unknown_count_or_unproved_pagination_never_settles_or_marks_missing(
    database, monkeypatch, unknown_count, missing_pagination
):
    store, tx, args = setup(database, seed_missing=True)

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        payload = page([node("p")])
        if missing_pagination:
            payload["data"][TOP].pop("page_info")
        return httpx.Response(200, json=payload)

    fetch, _ = source(monkeypatch, handler, expected=None if unknown_count else 1)
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert result["complete"] is False
    assert (
        existing.query(
            tx,
            "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'",
            (args["post_id"],),
        )[0]["is_missing"]
        is False
    )


def test_public_cancel_before_next_request_keeps_only_atomic_page_receipt(database, monkeypatch):
    store, tx, args = setup(database)
    events = []

    def handler(request):
        events.append(request.method)
        if request.method == "GET":
            return bootstrap()
        return httpx.Response(200, json=page([node("p")], more=True, after="tail"))

    fetch, _ = source(monkeypatch, handler, expected=2)
    original_check = store.check_owner

    def owner(**kwargs):
        if len(events) == 2:
            raise LostOwnership("fixture cancellation")
        return original_check(**kwargs)

    monkeypatch.setattr(store, "check_owner", owner)
    with pytest.raises(LostOwnership):
        CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert events == ["GET", "POST"]
    rows = existing.query(
        tx,
        """select p.checkpoint,t.checkpoint as target_checkpoint
        from social.instagram_ingestion_comment_pages p
        join social.instagram_ingestion_targets t using(run_id,stage,target_key) where p.run_id=%s""",
        (args["run_id"],),
    )
    assert len(rows) == 1 and rows[0]["checkpoint"] == rows[0]["target_checkpoint"]


@pytest.mark.parametrize("status", [401, 403, 429])
def test_public_http_blocks_reach_classified_callback_without_retry_or_fallback(monkeypatch, status):
    requests, failures, counts = [], [], []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, headers={"retry-after": "12"})

    fetch, _ = source(monkeypatch, handler, expected=1)

    def rejected(code, delay):
        failures.append((code, delay))
        raise RuntimeError("classified source stop")

    with pytest.raises(RuntimeError, match="classified source stop"):
        fetch(
            shortcode="ABC123",
            checkpoint={},
            source_observer=lambda *args: pytest.fail("block yielded canonical evidence"),
            pre_source_request=lambda: counts.append(True),
            source_failure_observer=rejected,
        )
    assert len(requests) == len(counts) == 1
    assert failures == [("rate_limited", 12.0) if status == 429 else ("public_access_backoff", 12.0)]


@pytest.mark.parametrize("cookie", ["sessionid=forbidden", "ds_user_id=999"])
def test_public_bootstrap_auth_cookie_is_rejected_before_another_send(monkeypatch, cookie):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text='"LSD",[],{"token":"fixture"}', headers={"set-cookie": cookie + "; Path=/"})

    fetch, _ = source(monkeypatch, handler, expected=1)
    with pytest.raises(SourceFailure) as caught:
        fetch(
            shortcode="ABC123",
            checkpoint={},
            source_observer=lambda *args: pytest.fail("auth cookie yielded source"),
            pre_source_request=lambda: None,
        )
    assert len(calls) == 1
    assert caught.value.failure.code == "public_access_backoff"


def test_public_and_authenticated_checkpoints_cannot_cross_families(monkeypatch):
    fetch, _ = source(monkeypatch, lambda request: pytest.fail("family mismatch made a request"), expected=1)
    checkpoint = {
        "comment_pages": {
            "version": 1,
            "adapter_family": "instagram_comments_rest",
            "lanes": {"__top__": {"exhausted": False}},
            "observed": {},
        }
    }
    with pytest.raises(CommentSourceError, match="another adapter"):
        fetch(
            shortcode="ABC123",
            checkpoint=checkpoint,
            source_observer=lambda *args: None,
            pre_source_request=lambda: pytest.fail("counted rejected family"),
        )
    auth = ScraplingCommentSource(cookies=[], raw_cookies={}, browser_account_id=None)
    with pytest.raises(CommentSourceError, match="another adapter"):
        auth(
            shortcode="ABC123",
            checkpoint={"comment_pages": {"adapter_family": "instagram_public_relay"}},
            source_observer=lambda *args: None,
            pre_source_request=lambda: pytest.fail("counted rejected family"),
        )


def test_public_retained_provenance_replays_without_source_and_rejects_wrong_query(database, monkeypatch):
    store, tx, args = setup(database)

    def handler(request):
        return bootstrap() if request.method == "GET" else httpx.Response(200, json=page([node("p")]))

    fetch, _ = source(monkeypatch, handler, expected=1)
    assert CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)["complete"]
    retained = existing.query(
        tx, "select * from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],)
    )[0]
    evidence = Evidence(
        source_identity=retained["source_identity"],
        captured_at=retained["captured_at"],
        parser_version=retained["parser_version"],
        payload_json=retained["payload"],
        content_hash=retained["content_hash"],
        schema_version=retained["schema_version"],
    )
    parsed = parse_comment_evidence(evidence, pinned=True)
    assert parsed.observed_at == OBSERVED and parsed.exhausted
    assert parsed.cursor_provenance["adapter_family"] == "instagram_public_relay"
    state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
    first_payload = evidence.read(pinned=True)
    first_payload["data"][TOP]["page_info"] = {"has_next_page": True, "end_cursor": "tail"}
    first = parse_comment_evidence(existing.evidence(first_payload), pinned=True)
    _advance(state, first, scan_started_at=OBSERVED)
    next_payload = evidence.read(pinned=True)
    next_payload["cursor_payload"].update(source="resume", request_cursor="tail", relay_doc_id="wrong-query")
    second = parse_comment_evidence(existing.evidence(next_payload), pinned=True)
    with pytest.raises(CommentSourceError, match="durable query lane"):
        _advance(state, second, scan_started_at=OBSERVED)


@pytest.mark.parametrize(
    "message,code",
    [
        ("upstream failure", "source_incomplete"),
        ("login_required", "public_access_backoff"),
        ("Please wait", "rate_limited"),
    ],
)
def test_public_decoded_failure_is_retained_before_classified_rejection(database, monkeypatch, message, code):
    store, tx, args = setup(database, seed_missing=True)
    requests = []

    def handler(request):
        requests.append(request)
        return (
            bootstrap()
            if request.method == "GET"
            else httpx.Response(
                200,
                json={"errors": [{"message": message}], "data": {TOP: page([])["data"][TOP]}},
                headers={"retry-after": "37"},
            )
        )

    fetch, _ = source(monkeypatch, handler, expected=0)
    failures = []

    def observe_failure(found, delay):
        failures.append((found, delay))

    def guarded(**kwargs):
        return fetch(**kwargs, source_failure_observer=observe_failure)

    with pytest.raises(CommentSourceError):
        CommentActivity(store, guarded, clock=lambda: OBSERVED).execute(**args)
    assert failures == [(code, 37.0)] and len(requests) == 2
    assert (
        existing.query(tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],))
        == []
    )
    retained = existing.query(
        tx, "select payload from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],)
    )
    assert len(retained) == 1 and json.loads(retained[0]["payload"])["status"] == "fail"
    assert "retry_after" not in json.loads(retained[0]["payload"])
    assert (
        existing.query(
            tx,
            "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'",
            (args["post_id"],),
        )[0]["is_missing"]
        is False
    )


def test_public_repeated_cursor_stays_unresolved_without_deleting_missing(database, monkeypatch):
    store, tx, args = setup(database, seed_missing=True)
    requests = []

    def handler(request):
        requests.append(request.method)
        return (
            bootstrap()
            if request.method == "GET"
            else httpx.Response(200, json=page([node("p")], more=True, after="same"))
        )

    fetch, _ = source(monkeypatch, handler, expected=1)
    with pytest.raises(CursorStalledError) as caught:
        CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert caught.value.failure.code == "cursor_stalled"
    assert requests == ["GET", "POST", "POST"]
    target = existing.query(
        tx,
        "select checkpoint from social.instagram_ingestion_targets where run_id=%s and stage='comments'",
        (args["run_id"],),
    )[0]
    assert target["checkpoint"]["comment_pages"]["lanes"]["__top__"]["repeated"] is True
    assert (
        existing.query(
            tx,
            "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'",
            (args["post_id"],),
        )[0]["is_missing"]
        is False
    )


@pytest.mark.parametrize("lane", ["__top__", "p"])
def test_audited_stalled_lane_restart_retains_pages_and_deduplicates(database, monkeypatch, lane):
    from uuid import uuid4

    from trr_backend.ingestion.lane_restart import restart_lane

    store, tx, args = setup(database, bounded=True)

    def stalled(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        child = variables.get("parent_comment_id") is not None
        if lane == "p" and not child:
            return httpx.Response(200, json=page([node("p", count=2)]))
        return httpx.Response(200, json=page([node("r1" if child else "p")], child=child, more=True, after="same"))

    fetch, _ = source(monkeypatch, stalled, expected=3 if lane == "p" else 1)
    with pytest.raises(CursorStalledError):
        CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    before = existing.query(
        tx,
        "select count(*) as n from social.instagram_ingestion_comment_pages where run_id=%s",
        (args["run_id"],),
    )[0]["n"]
    cancelled = store.control(args["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
    key = str(uuid4())
    command = {
        "run_id": args["run_id"],
        "account": "bravotv",
        "generation": cancelled["generation"],
        "target_key": args["target_key"],
        "lane": lane,
        "reason": "reviewed cursor repair",
        "source_revision": "fixture-repaired-source",
        "initiated_by": "fixture-operator",
        "idempotency_key": key,
    }
    approved = restart_lane(store, **command)
    assert restart_lane(store, **command) == approved
    resumed = store.control(args["run_id"], "bravotv", action="resume", idempotency_key=str(uuid4()))
    target = store.reserve(
        args["run_id"],
        "bravotv",
        generation=resumed["generation"],
        stage=Stage.COMMENTS,
        target_key=args["target_key"],
    )
    calls = []

    def repaired(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        calls.append(variables)
        assert variables.get("after") is None
        if lane == "p":
            assert variables.get("parent_comment_id") == "p"
            return httpx.Response(200, json=page([node("r1"), node("r2")], child=True))
        return httpx.Response(200, json=page([node("p")]))

    fetch, _ = source(monkeypatch, repaired, expected=3 if lane == "p" else 1)
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED + timedelta(minutes=1)).execute(
        **{**args, "generation": resumed["generation"], "owner_token": str(target["owner_token"])}
    )
    assert result["complete"] is True and len(calls) == 1
    rows = existing.query(tx, "select comment_id from social.instagram_comments where post_id=%s", (args["post_id"],))
    assert {row["comment_id"] for row in rows} == ({"p", "r1", "r2"} if lane == "p" else {"p"})
    after = existing.query(
        tx,
        "select count(*) as n from social.instagram_ingestion_comment_pages where run_id=%s",
        (args["run_id"],),
    )[0]["n"]
    assert after == before + 1
    assert result["checkpoint"]["public_lane_restarts"] == {}
    assert result["checkpoint"]["public_lane_restart_history"][0]["previous_lane"]["repeated"] is True


def test_public_constructor_ignores_ambient_pool_and_blocks_nonpublic_operations(monkeypatch):
    from trr_backend.socials.instagram.scraper import InstagramScraper

    monkeypatch.setenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED", "1")
    monkeypatch.setattr(InstagramScraper, "_build_identity_pool", lambda *args: pytest.fail("ambient pool constructed"))
    fetch, created = source(
        monkeypatch,
        lambda request: bootstrap() if request.method == "GET" else httpx.Response(200, json=page([])),
        expected=0,
    )
    assert (
        fetch(
            shortcode="ABC123", checkpoint={}, source_observer=lambda *args: None, pre_source_request=lambda: None
        ).fetch_failed
        is False
    )
    actual = created[0]
    assert (
        actual._fetcher is None and actual._parser._identity_pool is None and not actual._parser._identity_pool_enabled
    )
    for operation in ("browser_warmup", "browser_api", "rendered_comments", "comments_api"):
        with pytest.raises(RuntimeError, match="authenticated or browser"):
            actual._before_source(operation)


@pytest.mark.parametrize("alias", ["childCommentCount", "repliesCount"])
def test_public_native_count_alias_is_retained_without_an_unnecessary_child_probe(database, monkeypatch, alias):
    store, _, args = setup(database)
    operations = []

    def handler(request):
        operations.append(request.method)
        if request.method == "GET":
            return bootstrap()
        parent = node("p")
        parent.pop("child_comment_count")
        parent[alias] = 0
        return httpx.Response(200, json=page([parent]))

    fetch, _ = source(monkeypatch, handler, expected=1)
    assert CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)["complete"] is True
    assert operations == ["GET", "POST"]


def test_durable_public_family_pins_httpx_when_legacy_environment_selects_curl(monkeypatch):
    from trr_backend.socials.instagram.comments_scrapling import async_http_client

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_HTTP_CLIENT", "curl_cffi")
    monkeypatch.setattr(
        async_http_client, "_CurlCffiAsyncClient", lambda **kwargs: pytest.fail("Constructed alternate transport")
    )
    calls = []

    def handler(request):
        calls.append(request.method)
        return bootstrap() if request.method == "GET" else httpx.Response(200, json=page([]))

    fetch, _ = source(monkeypatch, handler, expected=0)
    assert (
        fetch(
            shortcode="ABC123", checkpoint={}, source_observer=lambda *args: None, pre_source_request=lambda: None
        ).fetch_failed
        is False
    )
    assert calls == ["GET", "POST"]


@pytest.mark.parametrize("stage", ["bootstrap", "parent", "child"])
@pytest.mark.parametrize("failure", ["401", "403", "login_redirect", "structured"])
@pytest.mark.parametrize("header", ["37", "date"])
def test_public_retry_after_reaches_each_failure_callback_without_retry(monkeypatch, stage, failure, header):
    from trr_backend.socials.instagram.public_response import parse_public_retry_after

    monkeypatch.setattr(
        maintained, "parse_public_retry_after", lambda value: parse_public_retry_after(value, now=OBSERVED)
    )
    value = format_datetime(OBSERVED + timedelta(seconds=37), usegmt=True) if header == "date" else header
    calls, failures = [], []

    def handler(request):
        current = (
            "bootstrap"
            if request.method == "GET"
            else "child"
            if body(request)[1].get("parent_comment_id")
            else "parent"
        )
        calls.append(current)
        if current != stage:
            return bootstrap() if current == "bootstrap" else httpx.Response(200, json=page([node("p", 1)]))
        headers = {"retry-after": value}
        if failure == "structured":
            return httpx.Response(
                200,
                json={"status": "fail", "message": "login_required"}
                if current == "bootstrap"
                else {"errors": [{"message": "login_required"}]},
                headers=headers,
            )
        if failure == "login_redirect":
            return httpx.Response(302, headers={**headers, "location": "/accounts/login/"})
        return httpx.Response(int(failure), headers=headers)

    fetch, _ = source(monkeypatch, handler, expected=2)

    def rejected(code, delay):
        failures.append((code, delay))
        raise SourceFailure(Failure(code))

    with pytest.raises(SourceFailure):
        fetch(
            shortcode="ABC123",
            checkpoint={},
            source_observer=lambda *args: None,
            pre_source_request=lambda: None,
            source_failure_observer=rejected,
        )
    assert failures == [("public_access_backoff", 37.0)]
    assert calls == ["bootstrap", "parent", "child"][: len(calls)] and calls[-1] == stage


@pytest.fixture(scope="module")
def request_lanes(database):
    _, transaction = database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            (existing.ROOT / "supabase/migrations/20260911121000_instagram_detail_request_lanes.sql").read_text()
        )
    return transaction


@pytest.fixture
def fresh_public_lanes(request_lanes):
    with request_lanes() as conn, conn.cursor() as cur:
        cur.execute("delete from social.instagram_detail_request_lanes")
    return request_lanes


@pytest.mark.parametrize("stage", ["bootstrap", "parent", "child"])
@pytest.mark.parametrize("header", [None, "37", "date"])
def test_public_cookie_violation_opens_real_gate_immediately_without_another_send(
    fresh_public_lanes, monkeypatch, stage, header
):
    from trr_backend.ingestion.composition import _GatedSource
    from trr_backend.ingestion.identity_policy import IdentityDeferred, SourceRequestGate
    from trr_backend.socials.instagram.public_response import parse_public_retry_after

    monkeypatch.setattr(
        maintained, "parse_public_retry_after", lambda value: parse_public_retry_after(value, now=OBSERVED)
    )
    calls = []

    def handler(request):
        current = (
            "bootstrap"
            if request.method == "GET"
            else "child"
            if body(request)[1].get("parent_comment_id")
            else "parent"
        )
        calls.append(current)
        if current == stage:
            headers = {"set-cookie": "sessionid=synthetic-forbidden; Path=/"}
            if header is not None:
                headers["retry-after"] = (
                    format_datetime(OBSERVED + timedelta(seconds=37), usegmt=True) if header == "date" else header
                )
            return httpx.Response(
                200,
                text='"LSD",[],{"token":"fixture"}' if current == "bootstrap" else json.dumps(page([])),
                headers=headers,
            )
        return bootstrap() if current == "bootstrap" else httpx.Response(200, json=page([node("p", 1)]))

    fetch, _ = source(monkeypatch, handler, expected=2)

    def gate():
        return SourceRequestGate(
            cookies={},
            public_only=True,
            transaction=fresh_public_lanes,
            check_cancelled=lambda: None,
            deadline=time.monotonic() + 60,
        )

    lane = gate()
    guarded = _GatedSource(fetch, lane, failure_code="comment_fetch_failed")
    started = datetime.now(UTC)
    with pytest.raises(SourceFailure) as caught:
        guarded(shortcode="ABC123", checkpoint={}, source_observer=lambda *args: None, pre_source_request=lambda: None)
    failure = caught.value.failure
    assert failure.code == "public_access_backoff" and not failure.identity_blocked
    seconds = 900 if header is None else 37
    assert started + timedelta(seconds=seconds) <= failure.retry_at <= datetime.now(UTC) + timedelta(seconds=seconds)
    row = existing.query(fresh_public_lanes, "select * from social.instagram_detail_request_lanes")[0]
    assert row["cooldown_until"] == failure.retry_at and row["failure_generation"] == 1
    assert not row["blocked"] and row["last_error_code"] == "public_access_backoff"
    assert len(row["request_times"]) == len(calls) == ["bootstrap", "parent", "child"].index(stage) + 1
    with pytest.raises(IdentityDeferred, match="identity_cooldown"):
        _GatedSource(fetch, gate(), failure_code="comment_fetch_failed")(
            shortcode="ABC123", checkpoint={}, source_observer=lambda *args: None, pre_source_request=lambda: None
        )
    assert len(calls) == ["bootstrap", "parent", "child"].index(stage) + 1


@pytest.mark.parametrize("strict", [False, True])
def test_retained_26_parent_shape_compares_legacy_fetch_coverage_with_strict_reply_probes(
    database, monkeypatch, strict
):
    import asyncio

    monkeypatch.setenv(maintained._PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV, "0")
    calls, children = [], []

    def handler(request):
        if request.method == "GET":
            calls.append("bootstrap")
            return bootstrap()
        _, variables = body(request)
        parent = variables.get("parent_comment_id")
        if parent:
            children.append(parent)
            calls.append("child")
            replies = [actual_parent_shape("r1")] if parent == "p0" else []
            for reply in replies:
                reply.pop("parent_comment_id")
            return httpx.Response(200, json=page(replies, child=True))
        calls.append("top")
        tail = variables.get("after")
        indexes = range(15, 26) if tail else range(15)
        return httpx.Response(
            200,
            json=page([actual_parent_shape(f"p{i}") for i in indexes], more=not tail, after=None if tail else "tail"),
        )

    fetch, created = source(monkeypatch, handler, expected=26)
    if strict:
        store, _, args = setup(database)
        result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
        state = result["checkpoint"]["comment_pages"]
        assert result["complete"] is True, {"calls": calls, "state": state}
        assert len(state["observed"]) == 27 and all(
            state["observed"][f"p{i}"]["expected_replies"] is None for i in range(26)
        )
        assert (
            state["observed"]["r1"]["expected_replies"] is None and state["observed"]["r1"]["native_reply_leaf"] is True
        )
        assert children == [f"p{i}" for i in range(26)] and len(calls) == 29
    else:

        async def legacy():
            maintained_fetcher = fetch._factory(
                cookies=[], raw_cookies={}, browser_account_id=None, proxy_config=None, public_only=True
            )
            try:
                return await maintained_fetcher.fetch_comments_for_shortcode(
                    "ABC123",
                    max_comments=0,
                    fetch_replies=True,
                    expected_comment_count=26,
                    load_strategy="public_relay",
                )
            finally:
                await maintained_fetcher.aclose()

        result = asyncio.run(legacy())
        assert result.fetch_failed is False
        assert len(result.comments) == 26 and children == [f"p{i}" for i in range(26)] and len(calls) == 29
    assert len(created) == 1


@pytest.mark.parametrize("count", [None, "absent", 0, False, True, -1])
def test_public_unknown_native_counts_probe_but_explicit_zero_skips(database, monkeypatch, count):
    store, tx, args = setup(database)
    children = []

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        if variables.get("parent_comment_id"):
            children.append(variables["parent_comment_id"])
            return httpx.Response(200, json=page([], child=True))
        parent = actual_parent_shape("p", count)
        if count == "absent":
            parent.pop("child_comment_count")
        return httpx.Response(200, json=page([parent]))

    fetch, _ = source(monkeypatch, handler, expected=1)
    if type(count) is bool or (type(count) is int and count < 0):
        with pytest.raises(CommentSourceError, match="nonnegative integer or null"):
            CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
        assert children == []
        assert (
            existing.query(
                tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
            )
            == []
        )
        assert (
            len(
                existing.query(
                    tx, "select * from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],)
                )
            )
            == 1
        )
        return
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    explicit_zero = type(count) is int and count == 0
    assert result["complete"] is True and children == ([] if explicit_zero else ["p"])
    assert result["checkpoint"]["comment_pages"]["observed"]["p"]["expected_replies"] == (0 if explicit_zero else None)


def test_unknown_parent_reply_checkpoint_resumes_only_pending_lane_and_preserves_nulls(database, monkeypatch):
    store, tx, args = setup(database, seed_missing=True)
    attempt, calls = 0, []

    def handler(request):
        if request.method == "GET":
            calls.append((attempt, "bootstrap", None))
            return bootstrap()
        _, variables = body(request)
        parent, cursor = variables.get("parent_comment_id"), variables.get("after")
        calls.append((attempt, parent or "top", cursor))
        if parent is None:
            assert attempt == 0, "refetched exhausted top root"
            return httpx.Response(200, json=page([actual_parent_shape("p1"), actual_parent_shape("p2")]))
        if parent == "p1":
            assert attempt == 0, "refetched an already proved unknown-count parent"
            return httpx.Response(200, json=page([], child=True))
        assert parent == "p2"
        if cursor is None:
            assert attempt == 0
            reply = actual_parent_shape("r1")
            reply.pop("parent_comment_id")
            return httpx.Response(200, json=page([reply], child=True, more=True, after="child-tail"))
        assert cursor == "child-tail"
        if attempt == 0:
            raise RuntimeError("interrupted unknown-count reply tail")
        return httpx.Response(200, json=page([], child=True))

    fetch, _ = source(monkeypatch, handler, expected=2)
    with pytest.raises(RuntimeError, match="interrupted unknown-count"):
        CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    row = resume_tests.retry(database, args, cancel=True)
    before = row["checkpoint"]["comment_pages"]
    assert before["observed"]["p1"]["expected_replies"] is None
    assert before["observed"]["p2"]["expected_replies"] is None
    assert before["lanes"]["p1"]["started_from_root"] is True
    assert before["lanes"]["p2"]["exhausted"] is False
    attempt = 1
    fetch, _ = source(monkeypatch, handler, expected=2)
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)
    assert result["complete"] is True
    assert [item[1:] for item in calls if item[0] == 1] == [("bootstrap", None), ("p2", "child-tail")]
    assert result["checkpoint"]["comment_pages"]["observed"]["p2"]["expected_replies"] is None
    assert (
        existing.query(
            tx,
            "select scraped_at from social.instagram_comments where post_id=%s and comment_id='r1'",
            (args["post_id"],),
        )[0]["scraped_at"]
        == OBSERVED
    )


def test_unknown_reply_exhaustion_requires_root_and_exact_cursor_proof():
    def parsed(payload, *, cursor=None, parent=None):
        return parse_comment_evidence(
            existing.evidence(
                maintained._durable_relay_payload(
                    payload,
                    cursor=cursor,
                    doc_id="child-query" if parent else "top-query",
                    friendly_name="fixture-query",
                    sort_order="popular",
                    page_size=15,
                    parent=parent,
                )
            ),
            pinned=True,
        )

    state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
    _advance(state, parsed(page([actual_parent_shape("p")])), scan_started_at=OBSERVED)
    summary = CommentFetchSummary(expected_count=1, full_observation=True)
    assert not _complete(state, summary)
    _advance(state, parsed(page([], child=True, more=True, after="right-tail"), parent="p"), scan_started_at=OBSERVED)
    with pytest.raises(CommentSourceError, match="durable query lane"):
        _advance(state, parsed(page([], child=True), cursor="wrong-tail", parent="p"), scan_started_at=OBSERVED)
    assert not _complete(state, summary)
    _advance(state, parsed(page([], child=True), cursor="right-tail", parent="p"), scan_started_at=OBSERVED)
    assert _complete(state, summary) and state["observed"]["p"]["expected_replies"] is None
    state["lanes"]["p"].pop("started_from_root")
    assert not _complete(state, summary)
    from types import SimpleNamespace

    resume = PublicRelayResume.from_checkpoint({"comment_pages": state}, [SimpleNamespace(comment_id="p", replies=[])])
    assert resume.replies["p"] == {"pending": True}
    assert not _complete(state, CommentFetchSummary(full_observation=True))


@pytest.mark.parametrize("nested", [False, True])
def test_public_positive_or_nested_reply_topology_never_settles(database, monkeypatch, nested):
    store, tx, args = setup(database, seed_missing=True)

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        if variables.get("parent_comment_id"):
            reply = actual_parent_shape("r", 1 if not nested else 0)
            reply.pop("parent_comment_id")
            if nested:
                reply["edge_threaded_comments"] = {"count": 0, "edges": [], "page_info": {"has_next_page": False}}
            return httpx.Response(200, json=page([reply], child=True))
        return httpx.Response(200, json=page([actual_parent_shape("p")]))

    fetch, _ = source(monkeypatch, handler, expected=1)
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert result["complete"] is False
    assert (
        existing.query(
            tx,
            "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'",
            (args["post_id"],),
        )[0]["is_missing"]
        is False
    )


@pytest.mark.parametrize("positive_alias", ["reply_count", "replies_count", "childCommentCount", "repliesCount"])
def test_public_zero_primary_with_positive_reply_alias_never_settles(database, monkeypatch, positive_alias):
    store, tx, args = setup(database, seed_missing=True)

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        if variables.get("parent_comment_id"):
            reply = actual_parent_shape("r", 0)
            reply.pop("parent_comment_id")
            reply[positive_alias] = 1
            return httpx.Response(200, json=page([reply], child=True))
        return httpx.Response(200, json=page([actual_parent_shape("p")]))

    fetch, _ = source(monkeypatch, handler, expected=1)
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert result["complete"] is False
    observed = result["checkpoint"]["comment_pages"]["observed"]["r"]
    assert observed["expected_replies"] == 0 and observed["native_reply_leaf"] is False
    assert observed["unsupported_reply_topology"] is True
    assert (
        existing.query(
            tx,
            "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'",
            (args["post_id"],),
        )[0]["is_missing"]
        is False
    )


@pytest.mark.parametrize("null_alias", [None, "childCommentCount"])
def test_public_missing_or_null_alias_stays_unknown_through_checkpoint_resume_and_replay(
    database, monkeypatch, null_alias
):
    store, tx, args = setup(database)
    attempt = 0
    captured = []

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        if not variables.get("parent_comment_id"):
            assert attempt == 0, "refetched exhausted top"
            return httpx.Response(200, json=page([actual_parent_shape("p")]))
        if variables.get("after"):
            assert variables["after"] == "tail"
            if attempt == 0:
                raise RuntimeError("interrupted retained unknown reply")
            return httpx.Response(200, json=page([], child=True))
        assert attempt == 0
        reply = actual_parent_shape("r")
        reply.pop("parent_comment_id")
        reply.pop("child_comment_count")
        if null_alias:
            reply[null_alias] = None
        payload = page([reply], child=True, more=True, after="tail")
        captured.append(payload)
        return httpx.Response(200, json=payload)

    fetch, _ = source(monkeypatch, handler, expected=1)
    with pytest.raises(RuntimeError, match="interrupted retained unknown reply"):
        CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    previous = resume_tests.retry(database, args)
    reply_state = previous["checkpoint"]["comment_pages"]["observed"]["r"]
    assert reply_state["expected_replies"] is None and reply_state["native_reply_leaf"] is True
    retained = existing.query(
        tx,
        "select * from social.instagram_ingestion_evidence where run_id=%s order by captured_at,evidence_id",
        (args["run_id"],),
    )
    child = next(row for row in retained if "parent_comment_id" in json.loads(row["payload"]))
    evidence = Evidence(
        source_identity=child["source_identity"],
        captured_at=child["captured_at"],
        parser_version=child["parser_version"],
        payload_json=child["payload"],
        content_hash=child["content_hash"],
        schema_version=child["schema_version"],
    )
    replayed = parse_comment_evidence(evidence, pinned=True)
    assert replayed.source_payloads[0].get("child_comment_count") is None
    assert "child_comment_count" not in replayed.source_payloads[0]
    if null_alias:
        assert replayed.source_payloads[0][null_alias] is None
    attempt = 1
    fetch, _ = source(monkeypatch, handler, expected=1)
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)
    assert result["complete"] is True
    assert result["checkpoint"]["comment_pages"]["observed"]["r"]["expected_replies"] is None
    assert (
        existing.query(
            tx,
            "select scraped_at from social.instagram_comments where post_id=%s and comment_id='r'",
            (args["post_id"],),
        )[0]["scraped_at"]
        == OBSERVED
    )


@pytest.mark.parametrize("count", [None, 0, 2])
def test_native_reply_expectation_survives_dto_and_resume_seed(count):
    from types import SimpleNamespace

    from trr_backend.ingestion.comment_transport import _seed_parents

    native = actual_parent_shape("p", count)
    dto = maintained._graphql_comment_to_instagram_comment(native, shortcode="fixture", post_url="fixture")
    assert dto.reply_count == count and dto.child_comment_count == count
    assert dto.to_dict()["reply_count"] == count
    restored = _seed_parents((SimpleNamespace(source_payload=native),), "fixture")[0]
    assert restored.reply_count == count and restored.child_comment_count == count


@pytest.mark.parametrize("count", [None, 0, 2])
def test_fresh_public_june_request_and_unknown_child_probe(monkeypatch, count):
    calls = []

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        fields, variables = body(request)
        parent = variables.get("parent_comment_id")
        calls.append(parent)
        assert request.url.path == "/graphql/query/"
        if parent is None:
            assert fields["doc_id"] == "25516980651312394"
            assert request.headers["user-agent"] == "Mozilla/5.0"
            assert "csrftoken=anonymous" in request.headers["cookie"]
            return httpx.Response(200, json=page([actual_parent_shape("p", count)]))
        return httpx.Response(200, json=page([], child=True))

    fetch, _ = source(monkeypatch, handler, expected=1)
    fetch(shortcode="fixture", checkpoint={}, source_observer=lambda *args: None, pre_source_request=lambda: None)
    assert calls == ([None] if count == 0 else [None, "p"])


@pytest.mark.parametrize("trusted", [False, True])
@pytest.mark.parametrize("server_delay", [None, 1200, 7200])
@pytest.mark.parametrize("delay_location", ["header", "payload", "both"])
def test_native_child_rejection_isolated_from_parents_and_alternate_empty_trust(
    database, monkeypatch, trusted, server_delay, delay_location
):
    from trr_backend.ingestion.composition import _GatedSource

    store, tx, args = setup(database)
    calls, rejected = [], []
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_CHILD_ALT_DOC_TRUSTED", str(trusted).lower())
    monkeypatch.setattr(
        maintained,
        "_post_child_comments_graphql_doc_attempts",
        lambda: [
            ("PolarisPostChildCommentsQuery", "34884685271179117"),
            ("PolarisPostCommentsChildrenPaginationtQuery", "36239935742272683"),
        ],
    )

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        fields, variables = body(request)
        parent = variables.get("parent_comment_id")
        calls.append((parent, fields["doc_id"], variables.get("after")))
        if not parent:
            if variables.get("after"):
                return httpx.Response(200, json=page([actual_parent_shape("p2", 0)], count=2))
            return httpx.Response(200, json=page([actual_parent_shape("p")], more=True, after="tail", count=2))
        if fields["doc_id"] == "34884685271179117":
            return httpx.Response(
                200,
                json={
                    "errors": [{"code": 1675004, "message": "Rate limit exceeded"}],
                    **({"retry_after": server_delay} if server_delay and delay_location in {"payload", "both"} else {}),
                },
                headers={"retry-after": str(37 if delay_location == "both" else server_delay)}
                if server_delay and delay_location in {"header", "both"}
                else {},
            )
        assert fields["doc_id"] == "36239935742272683" and variables.get("after") is None
        return httpx.Response(200, json=page([], child=True))

    class Gate:
        request_count = 0
        successes = 0

        def before_request(self):
            self.request_count += 1

        def record_success(self):
            self.successes += 1

        def finish_failure(self, code=None, **kwargs):
            rejected.append(code)
            pytest.fail("Native child rejection poisoned the identity gate")

    fetch, _ = source(monkeypatch, handler, expected=2)
    gate = Gate()
    result = CommentActivity(
        store, _GatedSource(fetch, gate, failure_code="comment_fetch_failed"), clock=lambda: OBSERVED
    ).execute(**args)
    assert rejected == [] and gate.successes >= 2
    assert [item[0] for item in calls] == ([None, None, "p"] if server_delay else [None, None, "p", "p"])
    state = result["checkpoint"]["comment_pages"]
    assert state["lanes"]["__top__"]["exhausted"] is True
    assert len(state["observed"]) == 2
    if trusted and not server_delay:
        assert result["complete"] is True
        assert state["lanes"]["p"]["exhausted"] is True
    else:
        assert result["complete"] is False and result["outcome"] == "unresolved"
        assert result["failure"].code == "reply_access_rejected"
        access = state["reply_access"]
        assert access["attempts"] == 1 and access["lanes"] == ["p"]
        assert datetime.fromisoformat(access["next_attempt_at"].replace("Z", "+00:00")) == OBSERVED + timedelta(
            seconds=server_delay or 300
        )
        durable = existing.query(
            tx, "select * from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
        )[0]
        assert durable["checkpoint"]["comment_pages"]["reply_access"] == access
        assert durable["blocked_reason"] is None
        assert durable["checkpoint"]["last_error_code"] == "reply_access_rejected"
    evidence = existing.query(
        tx, "select payload from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],)
    )
    assert any(json.loads(row["payload"]).get("status") == "fail" for row in evidence)


def test_native_child_rejection_circuit_retains_all_pending_lanes(monkeypatch):
    calls, retained = [], []

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        fields, variables = body(request)
        parent = variables.get("parent_comment_id")
        if parent is None:
            return httpx.Response(200, json=page([actual_parent_shape(f"p{i}") for i in range(6)]))
        calls.append((parent, fields["doc_id"]))
        return httpx.Response(200, json={"errors": [{"code": 1675004, "message": "Rate limit exceeded"}]})

    fetch, _ = source(monkeypatch, handler, expected=6)
    summary = fetch(
        shortcode="fixture",
        checkpoint={},
        source_observer=lambda *args: retained.append(args),
        pre_source_request=lambda: None,
        source_failure_observer=lambda *args: pytest.fail("child rejection reached shared gate"),
    )
    assert calls == [("p0", "36239935742272683"), ("p0", "34884685271179117"), ("p1", "36239935742272683")]
    assert summary.reply_lanes_rejected == tuple(f"p{i}" for i in range(6))
    assert summary.fetch_failed is False and summary.reply_access_code == "reply_access_rejected"
    assert len([item for item in retained if item[0] == "graphql_replies"]) == 3


def test_reply_rejection_resume_defers_then_restarts_child_root():
    from types import SimpleNamespace

    now = datetime.now(UTC)
    checkpoint = {
        "comment_pages": {
            "adapter_family": "instagram_public_relay",
            "adapter_version": 1,
            "observed": {"p": {"expected_replies": None}},
            "lanes": {
                "p": {
                    "exhausted": False,
                    "relay_doc_id": "34884685271179117",
                    "relay_friendly_name": "PolarisPostChildCommentsQuery",
                    "sort_order": "popular",
                    "page_size": 12,
                    "cursors": ["old-tail"],
                    "cursor_param": "after",
                }
            },
            "reply_access": {"lanes": ["p"], "next_attempt_at": (now + timedelta(minutes=5)).isoformat()},
        }
    }
    parents = [SimpleNamespace(comment_id="p", replies=[])]
    deferred = PublicRelayResume.from_checkpoint(checkpoint, parents)
    assert deferred.replies == {"p": {"pending": False, "deferred": True}}
    checkpoint["comment_pages"]["reply_access"]["next_attempt_at"] = (now - timedelta(seconds=1)).isoformat()
    ready = PublicRelayResume.from_checkpoint(checkpoint, parents)
    assert ready.replies == {"p": {"pending": True, "deferred": False}}


def test_rejected_reply_checkpoint_releases_reserves_and_resumes_to_completion(database, monkeypatch):
    store, tx, args = setup(database)
    attempt, calls = 0, []

    def handler(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        parent = variables.get("parent_comment_id")
        calls.append((attempt, parent, variables.get("after")))
        if parent is None:
            assert attempt == 0, "Resumed a natively exhausted parent lane"
            return httpx.Response(200, json=page([actual_parent_shape("p")], count=1))
        if attempt == 0:
            return httpx.Response(200, json={"errors": [{"code": 1675004, "message": "Rate limit exceeded"}]})
        reply = actual_parent_shape("r")
        reply.pop("parent_comment_id")
        return httpx.Response(200, json=page([reply], child=True))

    fetch, _ = source(monkeypatch, handler, expected=1)
    first = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert first["outcome"] == "unresolved" and first["failure"].code == "reply_access_rejected"
    durable = existing.query(tx, "select * from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],))[
        0
    ]
    assert durable["owner_token"] is None and durable["checkpoint"]["comment_pages"]["reply_access"]["attempts"] == 1
    reserved = store.reserve(
        args["run_id"], args["account"], generation=1, stage=Stage.COMMENTS, target_key=args["target_key"]
    )
    args["owner_token"] = str(reserved["owner_token"])
    attempt = 1
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED + timedelta(minutes=10)).execute(**args)
    assert result["complete"] is True and result["outcome"] == "committed"
    assert "reply_access" not in result["checkpoint"]["comment_pages"]
    assert [item for item in calls if item[0] == 1] == [(1, "p", None)]
    assert set(result["checkpoint"]["comment_pages"]["observed"]) == {"p", "r"}


@pytest.mark.parametrize(
    "identity,text",
    [
        ("1231675004999", "ordinary reply"),
        ("reply", "Native error 1675004 and Rate limit exceeded are ordinary words here"),
    ],
)
def test_ordinary_child_content_cannot_become_native_request_rejection(monkeypatch, identity, text):
    def handler(request):
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        if not variables.get("parent_comment_id"):
            return httpx.Response(200, json=page([actual_parent_shape("p")]))
        reply = actual_parent_shape(identity)
        reply.pop("parent_comment_id")
        reply["text"] = text
        return httpx.Response(200, json=page([reply], child=True))

    fetch, _ = source(monkeypatch, handler, expected=1)
    summary = fetch(
        shortcode="fixture",
        checkpoint={},
        source_observer=lambda *args: None,
        pre_source_request=lambda: None,
        source_failure_observer=lambda *args: pytest.fail("Ordinary reply poisoned the shared gate"),
    )
    assert summary.reply_lanes_rejected == () and summary.fetch_failed is False
