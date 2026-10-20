"""Curl-backed public documents retain the observed session's safety boundary."""

from unittest.mock import Mock

import pytest
import requests
from curl_cffi.requests import Session
from curl_cffi.requests.exceptions import RequestException as CurlRequestException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.post_transport import SourceFailure, _CallbackAbort
from trr_backend.ingestion.public_http import PublicDocumentSession
from trr_backend.ingestion.public_transport import ObservedPublicSession

URL = "https://www.instagram.com/p/fixture/"


@pytest.mark.parametrize("retry_total", [1, None])
def test_document_mount_accepts_zero_retries_only(retry_total):
    with PublicDocumentSession() as session:
        zero = HTTPAdapter(max_retries=0)
        session.mount("https://", zero)
        assert session.adapters["https://"] is zero
        with pytest.raises(ValueError, match="cannot retry implicitly"):
            session.mount("https://", HTTPAdapter(max_retries=Retry(total=retry_total)))
        assert session.adapters["https://"] is zero


def test_curl_error_becomes_redacted_requests_exception_without_retry(monkeypatch):
    request = Mock(side_effect=CurlRequestException("private-response-sentinel"))
    monkeypatch.setattr(Session, "request", request)
    with PublicDocumentSession() as session:
        with pytest.raises(requests.RequestException, match="^Public document request failed$") as caught:
            session.request("GET", URL)
    request.assert_called_once_with("GET", URL)
    assert caught.value.__suppress_context__ is True
    assert "sentinel" not in str(caught.value)


@pytest.mark.parametrize("response_mode", ["public", "sessionid", "ds_user_id", "network_error"])
def test_observed_curl_session_sanitizes_and_charges_each_send_once(monkeypatch, response_mode):
    events, failures, sends = [], [], []
    gate = Mock()
    gate.before_request.side_effect = lambda: events.append("permit") or "permit-1"
    gate.record_success.side_effect = lambda permit: events.append(("success", permit))

    def failure(code, **kwargs):
        events.append(("failure", code, kwargs["permit"]))
        return Failure(code)

    gate.record_failure.side_effect = failure

    def curl_request(session, method, url, **kwargs):
        events.append("send")
        sends.append((method, url, kwargs))
        assert session.trust_env is False
        assert session.allow_redirects is False
        assert session.auth is None and session.proxies == {}
        assert not {key.lower() for key in session.headers} & {"cookie", "authorization", "proxy-authorization"}
        assert dict(session.cookies) == {}
        assert kwargs["auth"] is None and kwargs["proxies"] == {}
        assert kwargs["allow_redirects"] is False
        assert kwargs["cookies"] == {"csrftoken": "anonymous"}
        assert kwargs["headers"] == {"accept": "text/html"}
        if response_mode == "network_error":
            raise CurlRequestException("private-response-sentinel")
        response = requests.Response()
        response.status_code = 200
        response.url = url
        response._content = b"<html><body>public document fixture</body></html>"
        response.headers["content-type"] = "text/html"
        if response_mode != "public":
            # Real curl persists response cookies in its session jar before returning.
            session.cookies.set(response_mode, "unexpected-auth-cookie", domain=".instagram.com")
            response.headers["set-cookie"] = response_mode + "=unexpected-auth-cookie; Path=/"
        return response

    monkeypatch.setattr(Session, "request", curl_request)
    with PublicDocumentSession() as session:
        session.trust_env = True
        session.auth = ("ambient", "credentials")
        session.proxies.update({"https": "http://proxy.invalid"})
        session.headers.update(
            {"Cookie": "ambient-cookie", "Authorization": "ambient-auth", "Proxy-Authorization": "ambient-proxy-auth"}
        )
        session.cookies.set("sessionid", "ambient-cookie", domain=".instagram.com")
        observed = ObservedPublicSession(
            session=session,
            gate=gate,
            deadline=20,
            clock=lambda: 10,
            check_cancelled=lambda: None,
            on_request=lambda: events.append("charged"),
            on_payload=lambda _: pytest.fail("HTML must not become JSON evidence"),
            on_failure=failures.append,
        )
        assert all(adapter.max_retries.total == 0 for adapter in session.adapters.values())

        def send():
            return observed.get(
                URL,
                headers={
                    "Cookie": "request-cookie",
                    "Authorization": "request-auth",
                    "Proxy-Authorization": "request-proxy-auth",
                    "accept": "text/html",
                },
                cookies={"csrftoken": "anonymous"},
                allow_redirects=True,
            )

        if response_mode == "public":
            assert send().status_code == 200
            assert events == ["permit", "charged", "send", ("success", "permit-1")]
            assert failures == []
        else:
            with pytest.raises(_CallbackAbort) as caught:
                send()
            assert isinstance(caught.value.error, SourceFailure)
            code = "network_error" if response_mode == "network_error" else "public_access_backoff"
            assert caught.value.error.failure.code == code
            assert events == ["permit", "charged", "send", ("failure", code, "permit-1")]
            assert len(failures) == 1
            gate.record_success.assert_not_called()
    assert len(sends) == 1
    gate.before_request.assert_called_once_with()


def test_curl_perform_sees_explicit_empty_proxy_despite_proxy_environment(monkeypatch):
    from curl_cffi import Curl, CurlError, CurlOpt

    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.setenv(name, "http://ambient-proxy.invalid:8080")
    options, performed = {}, []
    real_setopt = Curl.setopt

    def record_option(curl, option, value):
        options[option] = value
        return real_setopt(curl, option, value)

    def stop_before_wire(curl, *args, **kwargs):
        performed.append(True)
        # This runs at the real request path's final send boundary. No actual
        # Curl.perform executes, so the test cannot issue a network request.
        assert options[CurlOpt.PROXY] == ""
        assert options[CurlOpt.FOLLOWLOCATION] == 0
        raise CurlError("fixture stopped before wire")

    monkeypatch.setattr(Curl, "setopt", record_option)
    monkeypatch.setattr(Curl, "perform", stop_before_wire)
    with PublicDocumentSession() as session:
        with pytest.raises(requests.RequestException, match="^Public document request failed$"):
            session.get(URL)
    assert performed == [True]  # No implicit curl retry after the injected failure.
