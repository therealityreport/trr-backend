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
from trr_backend.ingestion.comment_transport import PublicRelayCommentSource, ScraplingCommentSource
from trr_backend.ingestion.comments import CommentActivity, CommentSourceError, _advance, parse_comment_evidence
from trr_backend.ingestion.domain import Failure, LostOwnership, RunIntent, SourceProfile, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.comments_scrapling import fetcher as maintained

database = existing.database
deny_unmocked_http = resume_tests.deny_unmocked_http
OBSERVED = existing.OBSERVED
TOP = "xdt_api__v1__media__media_id__comments__connection"
CHILD = "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"


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


def setup(database, *, seed_missing=False):
    """Reuse canonical fixtures while freezing a public run at creation."""
    from uuid import uuid4

    store, tx = database
    run = store.start(
        RunIntent(account="bravotv", selected_tasks=(Stage.COMMENTS,), source_profile=SourceProfile.for_mode("public")),
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
def test_public_real_relay_receipts_resume_children_first_with_exact_query_and_original_times(
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
        [("bootstrap", None), ("p1", None), ("p1", "child-1"), ("top", "top-1")]
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
    result = CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert result["complete"] is False and requests == ["GET", "POST", "POST"]
    assert result["checkpoint"]["comment_pages"]["lanes"]["__top__"]["repeated"] is True
    assert (
        existing.query(
            tx,
            "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'",
            (args["post_id"],),
        )[0]["is_missing"]
        is False
    )


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
