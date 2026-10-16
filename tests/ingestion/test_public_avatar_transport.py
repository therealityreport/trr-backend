"""Anonymous profile sessions cannot acquire an authenticated identity."""

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest

from tests.ingestion.test_public_transport import SequenceSession, rejected_response
from tests.ingestion.test_public_transport import no_ambient_clients as no_ambient_clients_fixture
from trr_backend.ingestion.avatar_transport import AvatarProfileSource
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

no_ambient_clients = no_ambient_clients_fixture


def source(session):
    return AvatarProfileSource(cookies={}, deadline=160, clock=lambda: 100, session=session, public_only=True)


def test_public_profiles_keep_only_attempt_local_anonymous_state_across_usernames():
    events = []
    session = SequenceSession(events, [{"cookies": {"csrftoken": "anonymous-csrf"}}, {}])
    instance = source(session)
    try:
        for username in ("bravotv", "writer"):
            session.auth = ("forbidden-user", "forbidden-password")
            session.headers["Authorization"] = "Bearer forbidden-header"
            instance(username=username, pre_source_request=lambda: events.append("count"))
        assert events == ["count", "send", "count", "send"]
        assert "Cookie" not in session.sent[0][0].headers
        assert session.sent[1][0].headers["Cookie"] == "csrftoken=anonymous-csrf"
        assert session.sent[1][0].url.endswith("?username=writer")
        for request, options in session.sent:
            assert "Authorization" not in request.headers
            assert options["allow_redirects"] is False and options["proxies"] == {}
        assert session.trust_env is False and session.auth is None
    finally:
        instance.close()
    assert session.closed


@pytest.mark.parametrize("field", ["sessionid", "ds_user_id", " SESSIONID ", "DS_USER_ID"])
def test_public_profile_rejects_identity_response_and_never_sends_a_second_username(field):
    events = []
    session = SequenceSession(events, [{"cookies": {field: "forbidden-cookie"}}])
    instance = source(session)
    try:
        for username in ("bravotv", "writer"):
            with pytest.raises(InstagramRequestFailure, match="public_access_backoff") as caught:
                instance(username=username, pre_source_request=lambda: events.append("count"))
            assert "forbidden-cookie" not in str(caught.value)
        assert len(session.sent) == events.count("count") == 1
        assert "Cookie" not in session.sent[0][0].headers
    finally:
        instance.close()
    assert session.closed


def test_public_profile_redirect_is_never_followed_or_retried():
    events = []
    session = SequenceSession(events, [{"status": 302, "headers": {"location": "/accounts/login/"}}])
    instance = source(session)
    try:
        with pytest.raises(InstagramRequestFailure, match="redirect_login"):
            instance(username="bravotv", pre_source_request=lambda: events.append("count"))
        assert len(session.sent) == events.count("count") == 1
        assert session.sent[0][1]["allow_redirects"] is False
    finally:
        instance.close()


@pytest.mark.parametrize(
    "kind",
    [
        "unauthorized",
        "forbidden",
        "rate_limited",
        "redirect_login",
        "login_required",
        "challenge_required",
        "public_access_backoff",
    ],
)
def test_public_avatar_failure_preserves_numeric_retry_after(kind):
    events = []
    session = SequenceSession(events, [rejected_response(kind, "37")])
    instance = source(session)
    try:
        with pytest.raises(InstagramRequestFailure, match=kind) as caught:
            instance(username="writer", pre_source_request=lambda: events.append("count"))
        assert caught.value.retry_after_seconds == 37
        assert len(session.sent) == events.count("count") == 1
    finally:
        instance.close()


def test_public_avatar_auth_failure_preserves_http_date_retry_after():
    events = []
    header = format_datetime(datetime.now(UTC) + timedelta(hours=2), usegmt=True)
    session = SequenceSession(events, [rejected_response("unauthorized", header)])
    instance = source(session)
    try:
        with pytest.raises(InstagramRequestFailure, match="unauthorized") as caught:
            instance(username="writer", pre_source_request=lambda: events.append("count"))
        assert caught.value.retry_after_seconds == pytest.approx(7200, abs=2)
    finally:
        instance.close()
