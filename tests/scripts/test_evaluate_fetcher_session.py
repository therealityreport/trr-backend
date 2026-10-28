"""Evaluation guards run without a browser, provider, or external request."""

import asyncio

import httpx
import pytest

from scripts.modal.evaluate_fetcher_session import (
    AsyncTransport,
    Budget,
    EvaluationStop,
    sync_transport,
)


def test_fetcher_session_ignores_hostile_proxy_environment(monkeypatch):
    from curl_cffi import Curl, CurlOpt

    seen = []
    original_setopt = Curl.setopt
    monkeypatch.setenv("HTTPS_PROXY", "http://hostile.invalid:9999")
    monkeypatch.setenv("ALL_PROXY", "http://hostile.invalid:9999")

    def setopt(self, option, value):
        if option == CurlOpt.PROXY:
            seen.append(value)
        return original_setopt(self, option, value)

    def perform(self, *args, **kwargs):
        raise RuntimeError("fixture transport reached")

    monkeypatch.setattr(Curl, "setopt", setopt)
    monkeypatch.setattr(Curl, "perform", perform)
    with sync_transport("fetcher_session", Budget()) as session:
        with pytest.raises(RuntimeError, match="fixture transport reached"):
            session.get("https://www.instagram.com/bravotv/")
    assert seen[-1] == ""


def test_async_fetcher_closes_and_disables_ambient_proxy(monkeypatch):
    monkeypatch.setenv("https_proxy", "http://hostile.invalid:9999")

    from curl_cffi import CurlOpt

    async def check():
        transport = AsyncTransport("fetcher_session", Budget())
        async with transport:
            assert transport.inner.trust_env is False
            assert transport.inner.proxies == {}  # pyright: ignore[reportAttributeAccessIssue]
            assert transport.inner.curl_options[CurlOpt.PROXY] == ""  # pyright: ignore[reportAttributeAccessIssue]
            assert transport.inner.auth is None
        assert transport.client._async_curl_session is None  # pyright: ignore[reportAttributeAccessIssue]

    asyncio.run(check())


@pytest.mark.parametrize("headers,cookies", [({"Authorization": "fixture"}, {}), ({}, {"sessionid": "fixture"})])
def test_budget_rejects_credentials_before_request(headers, cookies):
    budget = Budget()
    with pytest.raises(EvaluationStop):
        budget.before("GET", "https://www.instagram.com/", headers, cookies)
    assert budget.count == 0


def test_budget_stops_on_challenge_and_at_request_limit():
    budget = Budget()
    response = httpx.Response(429, request=httpx.Request("GET", "https://www.instagram.com/"))
    with pytest.raises(EvaluationStop, match="public_response_blocked"):
        budget.after(response, 0)
    budget.count = 60
    with pytest.raises(EvaluationStop, match="request_budget"):
        budget.before("GET", "https://www.instagram.com/", {}, {})
    assert budget.count == 60
