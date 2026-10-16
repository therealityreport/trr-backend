"""Native avatar request contracts; fake HTTP sends only, without source traffic."""

import json

import pytest
import requests

from trr_backend.ingestion.avatar_resolution import AvatarResolutionError
from trr_backend.ingestion.avatar_transport import AvatarProfileSource
from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.constants import PROFILE_INFO_URL
from trr_backend.socials.instagram.request_client import InstagramRequestFailure


class RecordingSession(requests.Session):
    """Exercise real request preparation, replacing only the network send."""

    def __init__(self, events, *, status=200, payload=None, body=None, headers=None):
        super().__init__()
        self.events = events
        self.status = status
        self.body = body if body is not None else json.dumps(payload or {"status": "ok", "data": {"user": {}}})
        self.response_headers = headers or {"content-type": "application/json"}
        self.sent = []
        self.closed = False

    def get(self, url, **kwargs):
        self.events.append("get")
        return super().get(url, **kwargs)

    def send(self, request, **kwargs):
        self.events.append("send")
        self.sent.append((request, kwargs))
        response = requests.Response()
        response.status_code = self.status
        response._content = self.body.encode()
        response.headers.update(self.response_headers)
        response.request = request
        return response

    def close(self):
        self.closed = True
        self.events.append("close")
        super().close()


def test_profile_has_one_counted_send_with_explicit_credentials_and_no_ambient_auth(monkeypatch):
    events = []
    session = RecordingSession(events)
    monkeypatch.setenv("HTTPS_PROXY", "http://ambient-proxy.invalid:8080")
    monkeypatch.setenv("NETRC", "/does/not/exist")
    monkeypatch.setattr(requests.sessions, "get_netrc_auth", lambda *_: pytest.fail("ambient netrc lookup"))
    cookies = {"sessionid": "synthetic-explicit-cookie", "csrftoken": "synthetic-csrf"}
    source = AvatarProfileSource(cookies=cookies, deadline=160, clock=lambda: 100, session=session)
    cookies["sessionid"] = "changed-after-snapshot"
    try:
        assert source(username="BravoTV", pre_source_request=lambda: events.append("count"))["status"] == "ok"
        assert events == ["count", "get", "send"]
        assert len(session.sent) == 1
        request, options = session.sent[0]
        assert request.url == PROFILE_INFO_URL + "?username=bravotv"
        assert request.method == "GET"
        assert request.headers["Cookie"] == "sessionid=synthetic-explicit-cookie; csrftoken=synthetic-csrf"
        assert request.headers["x-csrftoken"] == "synthetic-csrf"
        assert request.headers["referer"] == "https://www.instagram.com/bravotv/"
        assert "Authorization" not in request.headers
        assert session.trust_env is False
        assert options["allow_redirects"] is False
        assert options["proxies"] == {}
        assert options["timeout"] == (5, 20)
        retry = session.get_adapter(PROFILE_INFO_URL).max_retries
        assert retry.total == 0 and retry.read is False
    finally:
        source.close()
    assert session.closed and events[-1] == "close"


@pytest.mark.parametrize("remaining", [1, 0, -1])
def test_expired_deadline_rejects_before_count_or_send(remaining):
    events = []
    session = RecordingSession(events)
    source = AvatarProfileSource(cookies={}, deadline=100 + remaining, clock=lambda: 100, session=session)
    try:
        with pytest.raises(SourceFailure, match="avatar_request_deadline"):
            source(username="bravotv", pre_source_request=lambda: events.append("count"))
        assert events == [] and session.sent == []
    finally:
        source.close()
    assert session.closed


def test_invalid_identity_and_lost_owner_cannot_send():
    events = []
    session = RecordingSession(events)
    source = AvatarProfileSource(cookies={}, deadline=160, clock=lambda: 100, session=session)
    try:
        with pytest.raises(AvatarResolutionError, match="avatar_identity_invalid"):
            source(username="https://unexpected.invalid", pre_source_request=lambda: events.append("count"))
        assert events == []

        def fence():
            events.append("fence")
            raise LostOwnership("cancelled")

        with pytest.raises(LostOwnership, match="cancelled"):
            source(username="bravotv", pre_source_request=fence)
        assert events == ["fence"] and session.sent == []
    finally:
        source.close()


@pytest.mark.parametrize(
    "status,body,headers,code,retryable,retry_after",
    [
        (429, "private-provider-body", {"retry-after": "37"}, "rate_limited", True, 37.0),
        (401, "private-provider-body", {}, "unauthorized", False, None),
        (403, "private-provider-body", {}, "forbidden", False, None),
        (
            302,
            "private-provider-body",
            {"location": "https://www.instagram.com/accounts/login/?token=private-token"},
            "redirect_login",
            False,
            None,
        ),
        (200, "{private-provider-body", {}, "parse_error", True, None),
        (200, "[]", {}, "parse_error", True, None),
        (200, '{"message":"login_required","private":"private-provider-body"}', {}, "login_required", False, None),
    ],
)
def test_provider_failures_are_safe_and_do_not_retry(status, body, headers, code, retryable, retry_after, caplog):
    events = []
    session = RecordingSession(events, status=status, body=body, headers=headers)
    source = AvatarProfileSource(
        cookies={"sessionid": "synthetic-cookie"}, deadline=160, clock=lambda: 100, session=session
    )
    try:
        with pytest.raises(InstagramRequestFailure) as caught:
            source(username="bravotv", pre_source_request=lambda: events.append("count"))
        failure = caught.value
        assert failure.error_code == code and str(failure) == code
        assert failure.status_code == status and failure.retryable is retryable
        assert failure.retry_after_seconds == retry_after
        assert failure.response_text is None
        assert "private" not in str(failure) + repr(failure) + caplog.text
        assert "private-token" not in (failure.redirect_target or "")
        assert events == ["count", "get", "send"] and len(session.sent) == 1
    finally:
        source.close()
    assert session.closed
