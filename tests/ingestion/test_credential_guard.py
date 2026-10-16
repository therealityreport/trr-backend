"""Pinned identity regression cases; actual request preparation, fake transports."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import requests

from tests.ingestion.test_avatar_transport import RecordingSession
from trr_backend.ingestion.media_refresh import ExplicitDetailClient
from trr_backend.socials.instagram import scraper as scraper_module
from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch, PinnedCookieIdentity
from trr_backend.socials.instagram.posts_scrapling import fetcher as posts_module

_REAL_ASYNC_CLIENT = httpx.AsyncClient
COOKIES = {"sessionid": "synthetic-pinned-session", "ds_user_id": "123456", "csrftoken": "old-csrf"}


@pytest.fixture(autouse=True)
def no_unmocked_source_clients(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("unexpected network/client construction")

    monkeypatch.setattr(requests, "Session", unexpected)
    monkeypatch.setattr(requests.sessions.Session, "send", unexpected)
    monkeypatch.setattr(httpx, "Client", unexpected)
    monkeypatch.setattr(httpx, "AsyncClient", unexpected)
    monkeypatch.setattr(posts_module, "build_stealthy_fetcher", lambda: SimpleNamespace(async_fetch=AsyncMock()))


@pytest.mark.parametrize("field", ["sessionid", "ds_user_id"])
def test_guard_rejects_changed_pair_in_full_partial_and_domain_duplicate_jars(field):
    guard = PinnedCookieIdentity(COOKIES)
    changed = COOKIES | {field: "forbidden-private-cookie"}
    for operation in (lambda: guard.validate_snapshot(changed), lambda: guard.merge(COOKIES, {field: changed[field]})):
        with pytest.raises(CredentialIdentityMismatch) as caught:
            operation()
        assert str(caught.value) == "instagram_credential_identity_mismatch"
        assert "private" not in repr(caught.value) + repr(guard)
    for jar in (requests.cookies.RequestsCookieJar(), httpx.Cookies()):
        jar.update(COOKIES)
        jar.set(field, "forbidden-private-cookie", domain=".instagram.com", path="/")
        with pytest.raises(CredentialIdentityMismatch):
            guard.prepare_jar(jar, COOKIES)


def test_guard_accepts_same_pair_and_ancillary_rotation_and_clears_duplicate_csrf():
    guard = PinnedCookieIdentity(COOKIES)
    updated = guard.merge(COOKIES, {"sessionid": COOKIES["sessionid"], "csrftoken": "new-csrf", "mid": "runtime"})
    jar = httpx.Cookies(COOKIES)
    jar.set("csrftoken", "new-csrf", domain=".instagram.com")
    assert guard.prepare_jar(jar, updated) == updated
    assert dict(jar) == updated
    assert len(list(jar.jar)) == len(updated)


@pytest.mark.parametrize("cookies", [{}, {"sessionid": "only-session"}, COOKIES | {"ds_user_id": ""}])
def test_guard_requires_complete_nonblank_supplied_pair(cookies):
    with pytest.raises(CredentialIdentityMismatch):
        PinnedCookieIdentity(cookies)


def strict_scraper(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED", "true")
    monkeypatch.setattr(scraper_module.InstagramScraper, "_build_identity_pool", lambda *_: pytest.fail("ambient pool"))
    monkeypatch.setattr(
        scraper_module, "get_current_instagram_auth_session", lambda: pytest.fail("ambient auth context")
    )
    session = RecordingSession([])
    instance = scraper_module.InstagramScraper(
        cookies=COOKIES.copy(), session=session, strict_supplied_credentials=True
    )
    return instance, session


def test_strict_scraper_never_constructs_or_selects_pool_or_attaches_ambient_auth(monkeypatch):
    instance, session = strict_scraper(monkeypatch)
    try:
        assert not instance._identity_pool_enabled and instance._identity_pool is None
        assert instance._request_cookies() == COOKIES and instance.get_headers()["x-csrftoken"] == "old-csrf"
        assert instance._rotate_active_identity(username="bravotv", reason="fixture") is False
        assert instance._auth_session is None and session.trust_env is False
        assert session.get_adapter("https://www.instagram.com").max_retries.total == 0
        instance.attach_auth_session(SimpleNamespace(cookies={"sessionid": "other", "ds_user_id": "other"}))
        pytest.fail("strict scraper attached divergent identity")
    except CredentialIdentityMismatch:
        assert instance.cookies == COOKIES and instance.browser_account_id is None
    finally:
        session.close()


def test_strict_scraper_allows_csrf_rotation_then_rejects_changed_response_identity_before_next_send(monkeypatch):
    instance, session = strict_scraper(monkeypatch)
    try:
        session.cookies.set("csrftoken", "new-csrf", domain=".instagram.com")
        assert instance.get_headers()["x-csrftoken"] == "new-csrf"
        response = instance._get("https://www.instagram.com/api/v1/test", headers={"Cookie": "sessionid=forbidden"})
        assert response.status_code == 200 and len(session.sent) == 1
        prepared, options = session.sent[0]
        assert COOKIES["sessionid"] in prepared.headers["Cookie"] and "forbidden" not in prepared.headers["Cookie"]
        assert options["allow_redirects"] is False
        session.cookies.set("ds_user_id", "other", domain=".instagram.com")
        with pytest.raises(CredentialIdentityMismatch):
            instance._get("https://www.instagram.com/api/v1/test")
        assert len(session.sent) == 1
    finally:
        session.close()


def test_non_strict_scraper_keeps_legacy_cookie_merge_behavior(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED", "false")
    session = RecordingSession([])
    instance = scraper_module.InstagramScraper(cookies=COOKIES.copy(), session=session, attach_auth_session=False)
    try:
        session.cookies.set("ds_user_id", "legacy-updated")
        assert instance._request_cookies()["ds_user_id"] == "legacy-updated"
        assert session.trust_env is True
    finally:
        session.close()


def posts(*, strict=True):
    return posts_module.InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies=COOKIES.copy(),
        browser_account_id="livestnation",
        consume_credentials_only=strict,
        allow_requests_recovery=False,
    )


def test_posts_discards_divergent_shared_pool_without_consuming_tokens_or_rebuilding(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "true")
    instance = posts()
    instance._rebuild_http_client = AsyncMock(side_effect=AssertionError("foreign pool rebuilt client"))
    monkeypatch.setitem(
        posts_module._POSTS_WARMUP_POOL,
        instance._warmup_pool_key,
        posts_module._WarmupPoolEntry(
            raw_cookies=COOKIES | {"sessionid": "foreign"},
            page_tokens={"lsd": "foreign-runtime"},
            created_at=time.monotonic(),
        ),
    )
    assert asyncio.run(instance._try_apply_warmup_pool_entry()) is False
    assert instance._raw_cookies == COOKIES and instance._page_tokens == {}
    assert instance._warmup_pool_key not in posts_module._POSTS_WARMUP_POOL
    assert instance._warmup_pool_metadata["refresh_reason"] == "identity_mismatch"


@pytest.mark.parametrize("field", ["sessionid", "ds_user_id"])
def test_posts_divergent_warmup_snapshot_fails_before_tokens_or_cookie_mutation(field):
    instance = posts()
    instance._rebuild_http_client = AsyncMock(side_effect=AssertionError("foreign snapshot rebuilt client"))
    with pytest.raises(CredentialIdentityMismatch):
        asyncio.run(
            instance.apply_warmup_snapshot(
                {"cookie_state": COOKIES | {field: "foreign"}, "page_tokens": {"lsd": "foreign-runtime"}}
            )
        )
    assert instance._raw_cookies == COOKIES and instance._page_tokens == {}


def test_posts_same_identity_snapshot_and_pool_allow_runtime_refresh(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "true")
    instance = posts()
    instance._rebuild_http_client = AsyncMock()
    updated = COOKIES | {"csrftoken": "new-csrf"}
    asyncio.run(instance.apply_warmup_snapshot({"cookie_state": updated, "page_tokens": {"lsd": "runtime"}}))
    assert instance._raw_cookies == updated and instance._page_tokens == {"lsd": "runtime"}
    monkeypatch.setitem(
        posts_module._POSTS_WARMUP_POOL,
        instance._warmup_pool_key,
        posts_module._WarmupPoolEntry(
            raw_cookies=updated | {"mid": "runtime"}, page_tokens={"lsd": "pooled-runtime"}, created_at=time.monotonic()
        ),
    )
    assert asyncio.run(instance._try_apply_warmup_pool_entry()) is True
    assert instance._raw_cookies["sessionid"] == COOKIES["sessionid"] and instance._raw_cookies["mid"] == "runtime"


def test_posts_browser_send_rebuilds_cookie_list_from_pinned_state_not_stale_snapshot():
    instance = posts()
    instance._cookies = [{"name": "sessionid", "value": "foreign", "domain": ".instagram.com", "path": "/"}]
    response = SimpleNamespace(
        status_code=200, headers={}, text="<html>fixture</html>", url="https://www.instagram.com/bravotv/"
    )
    instance._fetcher.async_fetch = AsyncMock(return_value=response)
    asyncio.run(instance._fetch_page(response.url, referer=response.url))
    outgoing = instance._fetcher.async_fetch.await_args.kwargs["cookies"]
    assert {cookie["name"]: cookie["value"] for cookie in outgoing} == COOKIES
    assert instance._request_count == 1


@pytest.mark.parametrize("field", ["sessionid", "ds_user_id"])
def test_posts_httpx_set_cookie_cannot_replace_supplied_pair_on_second_attempt(monkeypatch, field):
    instance = posts()
    operations, sent = [], []
    instance._before_source_request = operations.append
    instance._pace_api_requests = AsyncMock()

    def handle(request):
        sent.append(request)
        return httpx.Response(
            200, json={"data": {}}, headers={"set-cookie": f"{field}=foreign; Domain=.instagram.com; Path=/"}
        )

    instance._http_client = _REAL_ASYNC_CLIENT(cookies=COOKIES, transport=httpx.MockTransport(handle), trust_env=False)

    async def run():
        try:
            for _ in range(2):
                with pytest.raises(CredentialIdentityMismatch):
                    await instance._fetch_graphql("https://www.instagram.com/graphql/query", data={}, headers={})
        finally:
            await instance.aclose()

    asyncio.run(run())
    assert len(sent) == instance._request_count == len(operations) == 1
    assert COOKIES["sessionid"] in sent[0].headers["cookie"] and "foreign" not in sent[0].headers["cookie"]
    assert instance._raw_cookies == COOKIES


def test_posts_httpx_allows_same_identity_ancillary_updates_and_uses_exact_prepared_pair():
    instance = posts()
    instance._pace_api_requests = AsyncMock()
    sent = []

    def handle(request):
        sent.append(request)
        return httpx.Response(
            200, json={"data": {}}, headers={"set-cookie": "csrftoken=new-csrf; Domain=.instagram.com; Path=/"}
        )

    instance._http_client = _REAL_ASYNC_CLIENT(cookies=COOKIES, transport=httpx.MockTransport(handle), trust_env=False)
    instance._http_client.headers["Cookie"] = "sessionid=foreign-header"

    async def run():
        try:
            for _ in range(2):
                await instance._fetch_graphql(
                    "https://www.instagram.com/graphql/query", data={}, headers={"Cookie": "foreign"}
                )
        finally:
            await instance.aclose()

    asyncio.run(run())
    assert len(sent) == instance._request_count == 2
    assert sent[1].headers["x-csrftoken"] == "new-csrf"
    assert "new-csrf" in sent[1].headers["cookie"] and "foreign" not in sent[1].headers["cookie"]
    assert COOKIES["sessionid"] in sent[1].headers["cookie"] and COOKIES["ds_user_id"] in sent[1].headers["cookie"]


@pytest.mark.parametrize("referer", [None, "https://www.instagram.com/api/v1/media/66/info/"])
def test_explicit_detail_headers_match_maintained_static_contract(monkeypatch, referer):
    session = RecordingSession([])
    monkeypatch.setenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED", "false")
    maintained = scraper_module.InstagramScraper(cookies=COOKIES.copy(), session=session, attach_auth_session=False)
    explicit = ExplicitDetailClient(
        cookies=COOKIES.copy(), browser_account_id="livestnation", session=RecordingSession([])
    )
    try:
        assert explicit._get_headers(referer) == maintained.get_headers(referer)
    finally:
        maintained.session.close()
        explicit.session.close()


@pytest.mark.parametrize("recovery", [False, True])
def test_posts_warmup_identity_change_cannot_consume_runtime_tokens(recovery, monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "false")
    instance = posts()
    response = SimpleNamespace(
        status_code=200,
        headers={},
        url="https://www.instagram.com/bravotv/",
        text='"LSD",[],{"token":"foreign-runtime"}',
        cookies={"sessionid": "foreign"},
    )
    instance._fetch_page = AsyncMock(return_value=response)
    instance._rebuild_http_client = AsyncMock(side_effect=AssertionError("foreign response rebuilt client"))
    with pytest.raises(CredentialIdentityMismatch):
        if recovery:
            asyncio.run(instance._recover_homepage_redirect(referer=response.url))
        else:
            asyncio.run(instance.warmup("bravotv"))
    assert instance._raw_cookies == COOKIES and instance._page_tokens == {}
