"""Pure public response policy: document structure and server delay semantics."""

import json
import math
import sys
from datetime import UTC, datetime

import pytest
import requests

from trr_backend.socials.instagram.public_response import PublicRequestClient, parse_public_retry_after
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

NOW = datetime(2026, 10, 16, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("37", 37),
        ("37.5", 37.5),
        ("1e3", 1000),
        ("-10", None),
        ("-1e9999", None),
        ("-1e99999999999999999999", None),
        (-1, None),
        ("0", 0),
        ("1e9999", sys.float_info.max),
        ("1e99999999999999999999", sys.float_info.max),
        ("Fri, 16 Oct 2026 13:00:00 GMT", 3600),
        ("Fri, 16 Oct 2026 11:00:00 GMT", 0),
        (None, None),
        ("", None),
        ("not-a-delay", None),
        ("NaN", None),
        ("Infinity", None),
        (float("inf"), None),
        (True, None),
    ],
)
def test_public_retry_after_is_finite_and_preserves_numeric_date_and_overflow(value, expected):
    actual = parse_public_retry_after(value, now=NOW)
    assert actual == expected
    assert actual is None or math.isfinite(actual) and actual >= 0


def response(body, *, status=200, headers=None):
    result = requests.Response()
    result.status_code, result._content = status, body.encode()
    result.headers.update({"content-type": "text/html", **(headers or {})})
    return result


@pytest.mark.parametrize(
    "body",
    [
        "<html><head><title>Challenge recap • Instagram photos</title></head><body>"
        '<a href="/accounts/login/">Log in</a><p>checkpoint challenge login_required</p>'
        '<script>const challenge = "checkpoint"; const accounts = "/accounts/login/";</script></body></html>',
        '<title>Instagram • Instagram</title><script type="application/json">'
        + json.dumps({"caption": {"text": "challenge checkpoint accounts/login login_required"}})
        + "</script>",
    ],
)
def test_incidental_html_words_navigation_and_script_content_are_not_auth_documents(body):
    PublicRequestClient(session=None)._classify_response(response(body))


@pytest.mark.parametrize(
    "body,code",
    [
        ("<title>Login • Instagram</title>", "login_required"),
        ('<form><input name="username"><input name="password"></form>', "login_required"),
        ('<form action="/accounts/login/ajax/"><input name="next"></form>', "redirect_login"),
        ("<title>Security Check • Instagram</title>", "challenge_required"),
        ('<form action="/challenge/fixture"><input name="security_code"></form>', "challenge_required"),
        ('<form action="/checkpoint/"></form>', "checkpoint_required"),
        ('<script type="application/json">{"message":"challenge_required"}</script>', "challenge_required"),
        ('<script type="application/json">{"entry_data":{"LoginAndSignupPage":[]}}</script>', "login_required"),
    ],
)
def test_structured_auth_documents_remain_rejected_with_server_delay(body, code):
    with pytest.raises(InstagramRequestFailure, match=code) as caught:
        PublicRequestClient(session=None)._classify_response(response(body, headers={"retry-after": "37"}))
    assert caught.value.retry_after_seconds == 37


@pytest.mark.parametrize(
    "location,code",
    [
        ("/accounts/login/?next=/safe", "redirect_login"),
        ("/challenge/fixture/", "challenge_required"),
        ("/checkpoint/", "checkpoint_required"),
        ("/safe?next=/accounts/login/", "endpoint_redirect"),
    ],
)
def test_redirects_use_destination_path_not_incidental_query_words(location, code):
    with pytest.raises(InstagramRequestFailure, match=code) as caught:
        PublicRequestClient(session=None)._classify_response(
            response("", status=302, headers={"location": location, "retry-after": "37"})
        )
    assert caught.value.retry_after_seconds == 37
