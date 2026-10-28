"""Real maintained fetcher/decoder with mock HTTP and isolated PostgreSQL only."""

from datetime import timedelta
from uuid import uuid4

import httpx
import pytest

from tests.ingestion import test_comments as existing
from trr_backend.ingestion.comment_transport import ScraplingCommentSource, _summary
from trr_backend.ingestion.comments import (
    CommentActivity,
    CommentSourceError,
    parse_comment_evidence,
)
from trr_backend.ingestion.domain import Failure, Stage
from trr_backend.socials.instagram.comments_scrapling import fetcher as maintained

database = existing.database
OBSERVED = existing.OBSERVED
HTTP_CLIENT = httpx.AsyncClient
COOKIES = {"sessionid": "synthetic-session", "ds_user_id": "100", "csrftoken": "fixture"}


@pytest.fixture(autouse=True)
def deny_unmocked_http(monkeypatch):
    async def denied(*args, **kwargs):
        pytest.fail("Offline regression attempted an unmocked HTTP transport")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", denied)

    def client(*args, **kwargs):
        if not isinstance(kwargs.get("transport"), httpx.MockTransport):
            pytest.fail("Offline regression constructed a client without explicit mock transport")
        return HTTP_CLIENT(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client)


def node(identity, children=0):
    return {"pk": identity, "text": identity, "user": {"username": "fixture"}, "child_comment_count": children}


def transport(monkeypatch, handler, *, expected):
    created = []

    class NoBrowser:
        async def async_fetch(self, *args, **kwargs):
            pytest.fail("Offline regression attempted a browser request")

    monkeypatch.setattr(maintained, "build_stealthy_fetcher", lambda **kwargs: NoBrowser())
    monkeypatch.setattr(
        maintained,
        "build_comments_async_client",
        lambda **kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    async def rebuild(fetcher):
        if fetcher._http_client is not None:
            await fetcher._http_client.aclose()
        fetcher._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)

    monkeypatch.setattr(maintained.InstagramCommentsScraplingFetcher, "_rebuild_http_client_unlocked", rebuild)

    def factory(**kwargs):
        result = maintained.InstagramCommentsScraplingFetcher(**kwargs)
        # Offline test bypasses sleeping/legacy DB pacing, not the real request,
        # decode, observer, parser, pagination, summary or close implementations.
        result._api_delay_seconds = result._global_api_delay_seconds = 0
        created.append(result)
        return result

    return ScraplingCommentSource(
        cookies=[],
        raw_cookies=dict(COOKIES),
        browser_account_id="fixture",
        expected_count=expected,
        fetcher_factory=factory,
    ), created


def retry(database, args, *, cancel=False):
    store, tx = database
    row = existing.query(tx, "select * from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],))[0]
    owned = {key: value for key, value in args.items() if key != "post_id"}
    store.checkpoint_failure(
        **owned, stage=Stage.COMMENTS, failure=Failure("interrupted"), checkpoint=row["checkpoint"], requests=0
    )
    if cancel:
        store.control(args["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
        resumed = store.control(args["run_id"], "bravotv", action="resume", idempotency_key=str(uuid4()))
        args["generation"] = resumed["generation"]
    reservation = store.reserve(
        args["run_id"], "bravotv", generation=args["generation"], stage=Stage.COMMENTS, target_key=args["target_key"]
    )
    args["owner_token"] = str(reservation["owner_token"])
    return row


@pytest.mark.parametrize("top_exhausted,cancel", [(False, False), (True, True)])
def test_real_fetcher_drains_prior_reply_lane_without_root_refetch(database, monkeypatch, top_exhausted, cancel):
    store, tx, args = existing.setup(database)
    calls = []
    attempt = 0
    expected = 3 if top_exhausted else 4

    def handler(request):
        calls.append((attempt, request.url.path, dict(request.url.params)))
        child = "child_comments" in request.url.path
        if attempt == 0:
            if not child:
                return httpx.Response(
                    200,
                    json={
                        "comments": [node("p1", 2)],
                        "comment_count": expected,
                        "has_more_comments": not top_exhausted,
                        **({"next_max_id": "top-tail"} if not top_exhausted else {}),
                    },
                )
            if not request.url.params:
                return httpx.Response(
                    200,
                    json={
                        "child_comments": [node("r1")],
                        "has_more_tail_child_comments": True,
                        "next_min_child_cursor": "reply-tail",
                    },
                )
            raise RuntimeError("crash after committed reply page")
        if child:
            assert request.url.params["min_id"] == "reply-tail"
            return httpx.Response(200, json={"child_comments": [node("r2")], "has_more_tail_child_comments": False})
        assert not top_exhausted and request.url.params["max_id"] == "top-tail"
        return httpx.Response(
            200, json={"comments": [node("p2")], "comment_count": expected, "has_more_comments": False}
        )

    source, created = transport(monkeypatch, handler, expected=expected)
    with pytest.raises(RuntimeError, match="crash"):
        CommentActivity(store, source, clock=lambda: OBSERVED).execute(**args)
    before = retry(database, args, cancel=cancel)
    attempt = 1
    source, _ = transport(monkeypatch, handler, expected=expected)
    result = CommentActivity(store, source, clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)
    assert result["complete"] and result["outcome"] == "committed"
    assert before["checkpoint"]["comment_pages"]["observed"].keys() == {"p1", "r1"}
    after = existing.query(
        tx,
        "select attempts,requests,checkpoint from social.instagram_ingestion_targets where run_id=%s",
        (args["run_id"],),
    )[0]
    assert after["attempts"] == 2 and after["requests"] == len(calls)
    assert {
        entry["comment_id"]: entry["scraped_at"]
        for entry in existing.query(
            tx, "select comment_id,scraped_at from social.instagram_comments where post_id=%s", (args["post_id"],)
        )
    }["p1"] == OBSERVED
    assert existing.query(
        tx, "select scraped_at from social.instagram_comments where post_id=%s and comment_id='r1'", (args["post_id"],)
    ) == [{"scraped_at": OBSERVED}]
    resumed = [call for call in calls if call[0] == 1]
    assert len(resumed) == (1 if top_exhausted else 2)
    assert "child_comments" in resumed[0][1]
    assert all(call[2] for call in resumed)
    assert created[0]._http_client is None
    assert args["generation"] == (3 if cancel else 1)


def test_real_fetcher_top_tail_counts_prior_inventory_instead_of_rendering_gap(database, monkeypatch):
    store, tx, args = existing.setup(database)
    calls = []
    attempt = 0

    def handler(request):
        calls.append((attempt, dict(request.url.params)))
        if attempt == 0:
            if "max_id" in request.url.params:
                raise RuntimeError("crash after root")
            return httpx.Response(
                200,
                json={"comments": [node("p1")], "comment_count": 2, "has_more_comments": True, "next_max_id": "tail"},
            )
        assert request.url.params["max_id"] == "tail"
        return httpx.Response(200, json={"comments": [node("p2")], "comment_count": 2, "has_more_comments": False})

    source, _ = transport(monkeypatch, handler, expected=2)
    with pytest.raises(RuntimeError):
        CommentActivity(store, source, clock=lambda: OBSERVED).execute(**args)
    retry(database, args)
    attempt = 1
    assert CommentActivity(store, source, clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)["complete"]
    assert len([call for call in calls if call[0] == 1]) == 1
    assert (
        len(
            existing.query(
                tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
            )
        )
        == 2
    )


def test_complete_retained_scan_can_finish_after_receipt_failure_without_source(database, monkeypatch):
    store, tx, args = existing.setup(database)
    source, _ = transport(
        monkeypatch,
        lambda request: httpx.Response(
            200, json={"comments": [node("p1")], "comment_count": 1, "has_more_comments": False}
        ),
        expected=1,
    )
    commit = store.commit_target
    monkeypatch.setattr(store, "commit_target", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("receipt sink")))
    with pytest.raises(RuntimeError, match="receipt sink"):
        CommentActivity(store, source, clock=lambda: OBSERVED).execute(**args)
    retry(database, args)
    monkeypatch.setattr(store, "commit_target", commit)
    source, _ = transport(monkeypatch, lambda request: pytest.fail("exhausted scan requested root"), expected=1)
    result = CommentActivity(store, source, clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)
    assert result["complete"] and result["receipt"]["observed_at"] == OBSERVED
    assert existing.query(
        tx, "select requests from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
    ) == [{"requests": 1}]


@pytest.mark.parametrize("child", [False, True])
def test_real_decoder_retains_native_list_body_and_explicit_pagination(monkeypatch, child):
    captured = []
    raw = [node("r1" if child else "p1"), {"has_more_tail_child_comments" if child else "has_more_comments": False}]
    source, created = transport(monkeypatch, lambda request: httpx.Response(200, json=raw), expected=1)
    fetcher = source._factory(
        cookies=[],
        raw_cookies=dict(COOKIES),
        browser_account_id="fixture",
        consume_credentials_only=True,
        source_payload_observer=lambda kind, payload: captured.append(payload),
    )
    import asyncio

    async def decode():
        try:
            return await fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/parent/child_comments/"
                if child
                else "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC123/",
            )
        finally:
            await fetcher.aclose()

    result = asyncio.run(decode())
    assert result["payload"] == raw and len(captured) == 1
    page = parse_comment_evidence(existing.evidence(captured[0]))
    assert page.exhausted and page.parent_comment_id == ("parent" if child else None)
    assert page.comments[0].comment_id == ("r1" if child else "p1")
    assert created[0]._http_client is None


@pytest.mark.parametrize("items", [[], [node("p1")]])
def test_bare_native_list_never_manufactures_exhaustion(items):
    page = parse_comment_evidence(existing.evidence({"source_snapshot_type": "native_rest_list_v1", "items": items}))
    assert not page.exhausted and page.next_cursor is None


def test_real_list_pages_commit_native_top_and_child_proof(database, monkeypatch):
    store, tx, args = existing.setup(database)

    def handler(request):
        child = "child_comments" in request.url.path
        return httpx.Response(
            200,
            json=[
                node("r1") if child else node("p1", 1),
                {"has_more_tail_child_comments" if child else "has_more_comments": False},
            ],
        )

    source, _ = transport(monkeypatch, handler, expected=2)
    assert CommentActivity(store, source, clock=lambda: OBSERVED).execute(**args)["complete"]
    assert (
        len(
            existing.query(
                tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
            )
        )
        == 2
    )


@pytest.mark.parametrize("items,expected", [([], 0), ([node("p1")], 1)])
def test_real_bare_list_stays_unresolved_despite_fetcher_soft_end(database, monkeypatch, items, expected):
    store, tx, args = existing.setup(database)
    source, _ = transport(monkeypatch, lambda request: httpx.Response(200, json=items), expected=expected)
    result = CommentActivity(store, source, clock=lambda: OBSERVED).execute(**args)
    assert result["outcome"] == "unresolved"
    assert not existing.query(
        tx, "select * from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],)
    )


def test_mock_boundary_denies_direct_constructor_and_browser(monkeypatch):
    with pytest.raises(pytest.fail.Exception, match="without explicit mock"):
        httpx.AsyncClient()
    source, _ = transport(monkeypatch, lambda request: pytest.fail("HTTP not expected"), expected=0)
    instance = source._factory(
        cookies=[], raw_cookies=dict(COOKIES), browser_account_id="fixture", consume_credentials_only=True
    )
    import asyncio

    with pytest.raises(pytest.fail.Exception, match="browser request"):
        asyncio.run(instance._fetcher.async_fetch("https://example.invalid"))


def test_native_list_rejects_conflicting_metadata_and_unbound_children():
    with pytest.raises(CommentSourceError, match="conflicts"):
        parse_comment_evidence(
            existing.evidence(
                {
                    "source_snapshot_type": "native_rest_list_v1",
                    "items": [{"has_more_comments": True}, {"has_more_comments": False}],
                }
            )
        )
    with pytest.raises(CommentSourceError):
        parse_comment_evidence(
            existing.evidence(
                {"source_snapshot_type": "native_rest_list_v1", "items": [{"child_comments": [node("r1")]}]}
            )
        )


def test_coverage_target_met_summary_still_requires_native_completion_guards(database):
    store, tx, args = existing.setup(database)
    result = maintained.InstagramCommentsFetchResult(fetch_reason="coverage_target_met", reported_comment_count=1)
    summary = _summary(result, expected=1, from_root=True, capped=False)
    assert not summary.fetch_failed
    partial = {"comments": [node("p1")], "comment_count": 1, "has_more_comments": True, "next_max_id": "pending"}
    assert (
        CommentActivity(store, existing.source([partial], summary), clock=lambda: OBSERVED).execute(**args)["outcome"]
        == "unresolved"
    )
    assert not existing.query(
        tx, "select * from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],)
    )


def test_real_coverage_target_met_does_not_hide_a_repeated_native_reply_cursor(database, monkeypatch):
    store, tx, args = existing.setup(database)

    def handler(request):
        if "child_comments" not in request.url.path:
            return httpx.Response(
                200,
                json={
                    "comments": [node("p1", 1)],
                    "has_more_comments": False,
                    "comment_count": 2,
                },
            )
        return httpx.Response(
            200,
            json={
                "child_comments": [node("r1")],
                "has_more_tail_child_comments": True,
                "next_min_child_cursor": "repeated",
            },
        )

    source, created = transport(monkeypatch, handler, expected=2)
    result = CommentActivity(store, source, clock=lambda: OBSERVED).execute(**args)
    assert created[0]._last_single_session_strategy_metadata["stop_reason"] == "coverage_target_met"
    assert result["outcome"] == "unresolved"
    assert result["checkpoint"]["comment_pages"]["lanes"]["p1"]["repeated"]


def test_deliberate_restart_discards_seed_and_later_resume_never_reads_old_root(database, monkeypatch):
    store, tx, args = existing.setup(database)
    old_root = {
        "data": {
            "shortcode_media": {
                "edge_media_to_parent_comment": {
                    "count": 1,
                    "edges": [{"node": node("old")}],
                    "page_info": {"has_next_page": True, "end_cursor": "graphql-after"},
                }
            }
        }
    }
    CommentActivity(store, existing.source([old_root]), clock=lambda: OBSERVED).execute(**args)
    retry(database, args)
    old_evidence = existing.query(
        tx, "select evidence_id from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
    )[0]["evidence_id"]
    commit = store.commit_target
    monkeypatch.setattr(store, "commit_target", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("receipt sink")))
    source, created = transport(
        monkeypatch,
        lambda request: httpx.Response(
            200,
            json={
                "comments": [node("new")],
                "has_more_comments": False,
                "comment_count": 1,
            },
        ),
        expected=1,
    )
    with pytest.raises(RuntimeError, match="receipt sink"):
        CommentActivity(store, source, clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)
    assert created[0]._last_single_session_strategy_metadata["merged_comments"] == 1
    row = retry(database, args)
    assert set(row["checkpoint"]["comment_pages"]["observed"]) == {"new"}
    with tx() as conn, conn.cursor() as cur:
        # An obsolete scan's unavailable bytes must not be read or seeded.
        cur.execute(
            "update social.instagram_ingestion_evidence set payload=null where evidence_id=%s", (str(old_evidence),)
        )
    monkeypatch.setattr(store, "commit_target", commit)
    source, _ = transport(monkeypatch, lambda request: pytest.fail("completed new root refetched"), expected=1)
    result = CommentActivity(store, source, clock=lambda: OBSERVED + timedelta(hours=2)).execute(**args)
    assert result["complete"]
    assert existing.query(
        tx, "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'", (args["post_id"],)
    ) == [{"is_missing": True}]


@pytest.mark.parametrize(
    "metadata,classification,retry_after",
    [
        ({"status": "fail", "comment_count": 1, "has_more_comments": False}, "source_incomplete", None),
        (
            {"errors": [{"message": "upstream error"}], "comment_count": 1, "has_more_comments": False},
            "source_incomplete",
            None,
        ),
        ({"status": "fail", "message": "login_required", "has_more_comments": False}, "source_auth_failed", None),
        ({"rate_limited": True, "retry_after": 12, "has_more_comments": False}, "rate_limited", 12.0),
        ({"comments": [node("nested")], "status": "fail", "has_more_comments": False}, "source_incomplete", None),
    ],
)
def test_failed_list_metadata_is_retained_and_classified_before_any_canonical_page(
    database, monkeypatch, metadata, classification, retry_after
):
    from trr_backend.ingestion.post_transport import SourceFailure

    store, tx, args = existing.setup(database, seed_missing=True)
    source, _ = transport(monkeypatch, lambda request: httpx.Response(200, json=[node("p1"), metadata]), expected=1)
    failures = []

    def rejected(code, delay):
        failures.append((code, delay))
        raise SourceFailure(Failure(code, identity_blocked=code == "source_auth_failed"))

    def fetch(**callbacks):
        return source(**callbacks, source_failure_observer=rejected)

    with pytest.raises(SourceFailure) as error:
        CommentActivity(store, fetch, clock=lambda: OBSERVED).execute(**args)
    assert error.value.failure.code == classification
    assert error.value.failure.identity_blocked == (classification == "source_auth_failed")
    assert failures == [(classification, retry_after)]
    assert existing.query(
        tx, "select comment_id,is_missing from social.instagram_comments where post_id=%s", (args["post_id"],)
    ) == [{"comment_id": "old", "is_missing": False}]
    assert not existing.query(
        tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
    )
    assert not existing.query(
        tx, "select * from social.instagram_ingestion_receipts where run_id=%s", (args["run_id"],)
    )
    row = existing.query(tx, "select * from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))[0]
    from trr_backend.ingestion.evidence import Evidence

    retained = Evidence(
        source_identity=row["source_identity"],
        captured_at=row["captured_at"],
        parser_version=row["parser_version"],
        payload_json=row["payload"],
        content_hash=row["content_hash"],
        schema_version=row["schema_version"],
    )
    assert retained.read(pinned=True)["status"] == "fail"
    with pytest.raises(CommentSourceError, match="failed request metadata"):
        parse_comment_evidence(retained, pinned=True)


def test_comment_text_and_member_status_are_not_request_failure_metadata(database, monkeypatch):
    store, tx, args = existing.setup(database)
    comment = {**node("p1"), "text": "please wait login_required unauthorized rate limit", "status": "fail"}
    source, _ = transport(
        monkeypatch,
        lambda request: httpx.Response(200, json=[comment, {"comment_count": 1, "has_more_comments": False}]),
        expected=1,
    )
    assert CommentActivity(store, source, clock=lambda: OBSERVED).execute(**args)["complete"]


def test_pending_graphql_reply_cursor_restarts_child_root_even_when_count_met(database, monkeypatch):
    store, tx, args = existing.setup(database)
    captured = {
        "comments": [
            {
                **node("p1", 1),
                "edge_threaded_comments": {
                    "count": 1,
                    "edges": [{"node": node("r1")}],
                    "page_info": {"has_next_page": True, "end_cursor": "graphql-after"},
                },
            }
        ],
        "comment_count": 2,
        "has_more_comments": False,
    }
    first = CommentActivity(store, existing.source([captured]), clock=lambda: OBSERVED).execute(**args)
    assert first["outcome"] == "unresolved"
    retry(database, args)
    requests = []

    def handler(request):
        requests.append(request)
        assert "comments/p1/child_comments/" in request.url.path
        assert not request.url.params
        return httpx.Response(200, json={"child_comments": [node("r1")], "has_more_tail_child_comments": False})

    source, _ = transport(monkeypatch, handler, expected=2)
    result = CommentActivity(store, source, clock=lambda: OBSERVED + timedelta(hours=1)).execute(**args)
    assert result["complete"] and len(requests) == 1
    assert len(existing.query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))) == 2


def strict_fetcher(monkeypatch, handler, **options):
    source, _ = transport(monkeypatch, handler, expected=0)
    return source._factory(
        cookies=options.pop("cookies", []),
        raw_cookies=dict(COOKIES),
        browser_account_id="fixture",
        consume_credentials_only=True,
        **options,
    )


def test_strict_comments_constructor_and_headers_never_build_ambient_identity_pool(monkeypatch):
    from trr_backend.socials.instagram.scraper import InstagramScraper

    monkeypatch.setenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED", "1")
    monkeypatch.setattr(InstagramScraper, "_build_identity_pool", lambda *args: pytest.fail("ambient pool constructed"))
    fetcher = strict_fetcher(monkeypatch, lambda request: pytest.fail("HTTP not expected"))
    assert not fetcher._parser._identity_pool_enabled and fetcher._parser._identity_pool is None
    assert fetcher._parser.get_headers("https://www.instagram.com/p/ABC123/")["x-csrftoken"] == "fixture"
    assert fetcher._raw_cookies == COOKIES and fetcher._parser.cookies == COOKIES


def test_strict_constructor_rejects_divergent_browser_cookie_snapshot(monkeypatch):
    from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch

    with pytest.raises(CredentialIdentityMismatch) as error:
        strict_fetcher(
            monkeypatch,
            lambda request: pytest.fail("HTTP not expected"),
            cookies=[{"name": "sessionid", "value": "other-session"}],
        )
    assert str(error.value) == "instagram_credential_identity_mismatch"
    assert "other-session" not in repr(error.value)


def test_same_identity_response_updates_ancillary_cookies_in_all_states(monkeypatch):
    import asyncio
    from http.cookies import SimpleCookie

    sent = []

    def handler(request):
        cookie = SimpleCookie(request.headers["cookie"])
        assert cookie["sessionid"].value == COOKIES["sessionid"]
        assert cookie["ds_user_id"].value == COOKIES["ds_user_id"]
        assert request.headers["cookie"].count("sessionid=") == 1
        assert request.headers["cookie"].count("ds_user_id=") == 1
        if sent:
            assert cookie["csrftoken"].value == "new-csrf" and request.headers["x-csrftoken"] == "new-csrf"
        sent.append(request)
        return httpx.Response(
            200, json={"comments": [], "has_more_comments": False}, headers={"set-cookie": "csrftoken=new-csrf; Path=/"}
        )

    fetcher = strict_fetcher(monkeypatch, handler)

    async def execute():
        try:
            for _ in range(2):
                await fetcher._fetch_api(
                    "https://www.instagram.com/api/v1/media/1/comments/", referer="https://www.instagram.com/p/ABC123/"
                )
        finally:
            await fetcher.aclose()

    asyncio.run(execute())
    assert len(sent) == 2 and fetcher._raw_cookies["csrftoken"] == "new-csrf"
    assert fetcher._parser.cookies == {**COOKIES, "csrftoken": "new-csrf"}


@pytest.mark.parametrize("name,value", [("sessionid", "other-session"), ("ds_user_id", "999")])
def test_identity_changing_set_cookie_blocks_following_send_without_retry(monkeypatch, name, value):
    import asyncio

    from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch

    sent = []

    def handler(request):
        sent.append(request)
        return httpx.Response(
            200, json={"comments": [], "has_more_comments": False}, headers={"set-cookie": f"{name}={value}; Path=/"}
        )

    fetcher = strict_fetcher(monkeypatch, handler)

    async def execute():
        try:
            for _ in range(2):
                with pytest.raises(CredentialIdentityMismatch):
                    await fetcher._fetch_api(
                        "https://www.instagram.com/api/v1/media/1/comments/",
                        referer="https://www.instagram.com/p/ABC123/",
                    )
        finally:
            await fetcher.aclose()

    asyncio.run(execute())
    assert len(sent) == fetcher._request_count == 1
    assert fetcher._raw_cookies == COOKIES and fetcher._parser.cookies == COOKIES


@pytest.mark.parametrize("site", ["raw", "parser", "parser_jar", "outgoing_jar"])
def test_mutated_identity_is_rejected_before_callback_count_or_send(monkeypatch, site):
    import asyncio

    from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch

    before = []
    fetcher = strict_fetcher(
        monkeypatch,
        lambda request: pytest.fail("tampered identity sent"),
        before_source_request=lambda kind: before.append(kind),
    )

    async def execute():
        try:
            await fetcher._rebuild_http_client()
            if site == "raw":
                fetcher._raw_cookies["sessionid"] = "other-session"
            elif site == "parser":
                fetcher._parser.cookies["sessionid"] = "other-session"
            elif site == "parser_jar":
                fetcher._parser.session.cookies.set("sessionid", "other-session", domain=".instagram.com", path="/")
            else:
                fetcher._http_client.cookies.set("sessionid", "other-session", domain=".instagram.com", path="/")
            with pytest.raises(CredentialIdentityMismatch):
                await fetcher._fetch_api(
                    "https://www.instagram.com/api/v1/media/1/comments/", referer="https://www.instagram.com/p/ABC123/"
                )
        finally:
            await fetcher.aclose()

    asyncio.run(execute())
    assert before == [] and fetcher._request_count == 0


@pytest.mark.parametrize(
    "case",
    ["valid", "conflicting_pair", "api_path_conflict", "empty", "missing_sessionid", "missing_ds_user_id"],
)
def test_browser_evaluated_secondary_fetch_checks_context_identity(monkeypatch, case):
    import asyncio
    import json

    from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch

    fetcher = strict_fetcher(monkeypatch, lambda request: pytest.fail("HTTP not expected"))
    request_url = "https://www.instagram.com/api/v1/media/1/comments/"
    browser_cookies = [{"name": name, "value": value, "path": "/"} for name, value in COOKIES.items()]
    browser_cookies.append({"name": "browser_nonce", "value": "same-identity-update", "path": "/api/"})
    if case == "conflicting_pair":
        browser_cookies = [{"name": "sessionid", "value": "other-session"}, {"name": "ds_user_id", "value": "999"}]
    elif case == "api_path_conflict":
        browser_cookies.append({"name": "sessionid", "value": "other-session", "path": "/api/"})
    elif case == "empty":
        browser_cookies = []
    elif case.startswith("missing_"):
        browser_cookies = [cookie for cookie in browser_cookies if cookie["name"] != case.removeprefix("missing_")]
    evaluations = []
    cookie_queries = []

    class Context:
        async def cookies(self, url):
            cookie_queries.append(url)
            # A root URL would hide the API-path identity conflict.
            assert url == request_url
            return browser_cookies

    class Page:
        context = Context()

        async def evaluate(self, *args):
            assert case == "valid", "browser secondary fetch used missing or changed identity"
            evaluations.append(args)

    class Browser:
        async def async_fetch(self, *args, **kwargs):
            assert {item["name"]: item["value"] for item in kwargs["cookies"]} == COOKIES
            await kwargs["page_action"](Page())
            return httpx.Response(
                200,
                text=json.dumps({"status": 200, "body": "{}", "headers": {}}),
                request=httpx.Request("GET", args[0]),
            )

    fetcher._fetcher = Browser()
    operation = fetcher._fetch_api_with_browser_evaluate(
        request_url,
        referer="https://www.instagram.com/p/ABC123/",
        headers={},
    )
    if case == "valid":
        response = asyncio.run(operation)
        assert response.status_code == 200 and len(evaluations) == 1
        assert fetcher._raw_cookies["browser_nonce"] == "same-identity-update"
    else:
        with pytest.raises(CredentialIdentityMismatch):
            asyncio.run(operation)
        assert evaluations == []
        assert fetcher._raw_cookies == COOKIES
    assert cookie_queries == [request_url]
