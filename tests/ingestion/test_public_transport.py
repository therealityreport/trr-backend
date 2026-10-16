"""Maintained public parsers with real request preparation and fake HTTP only."""

import json
import time
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest
import requests

from trr_backend.ingestion.application import validate_detail
from trr_backend.ingestion.domain import Failure, LostOwnership, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.post_transport import SourceFailure, _CallbackAbort
from trr_backend.ingestion.providers import PARSER_VERSION, InvalidSourceError, parse_evidence
from trr_backend.ingestion.public_transport import ObservedPublicSession, PublicDetailSource, PublicListingSource
from trr_backend.socials.instagram import scraper as scraper_module
from trr_backend.socials.instagram.credential_guard import CredentialIdentityMismatch, PublicCookieIdentity


class SequenceSession(requests.Session):
    def __init__(self, events, responses):
        super().__init__()
        self.events, self.responses, self.sent, self.closed = events, list(responses), [], False

    def send(self, request, **kwargs):
        self.events.append("send")
        self.sent.append((request, kwargs))
        assert self.responses, "unexpected additional source send"
        spec = self.responses.pop(0)
        if isinstance(spec, Exception):
            raise spec
        response = requests.Response()
        response.status_code = spec.get("status", 200)
        response._content = spec.get("body", json.dumps(spec.get("payload", {}))).encode()
        response.headers.update(spec.get("headers", {"content-type": "application/json"}))
        response.request = request
        if spec.get("cookies"):
            response.cookies.update(spec["cookies"])
            self.cookies.update(spec["cookies"])
        return response

    def close(self):
        self.closed = True
        self.events.append("close")
        super().close()


class Gate:
    def __init__(self, events):
        self.events, self.request_count = events, 0

    def before_request(self):
        self.events.append("permit")
        self.request_count += 1
        return self.request_count

    def record_success(self, permit):
        self.events.append(("success", permit))

    def record_failure(self, code, *, permit, retry_after_seconds=None):
        self.events.append(("failure", code, permit, retry_after_seconds))
        return Failure(code)

    def finish_success(self):
        self.events.append("finish_success")

    def finish_failure(self, code=None):
        self.events.append(("finish_failure", code))


@pytest.fixture(autouse=True)
def no_ambient_clients(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unexpected ambient client, identity or network operation")

    monkeypatch.setattr(requests, "Session", forbidden)
    monkeypatch.setattr(requests.sessions.Session, "send", forbidden)
    monkeypatch.setattr(httpx, "Client", forbidden)
    monkeypatch.setattr(httpx, "AsyncClient", forbidden)
    monkeypatch.setattr(scraper_module.InstagramScraper, "_build_identity_pool", forbidden)
    monkeypatch.setattr(scraper_module.InstagramScraper, "_attach_current_auth_session", forbidden)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED", "true")
    monkeypatch.setenv("HTTPS_PROXY", "http://forbidden-proxy.invalid")
    monkeypatch.setattr(requests.sessions, "get_netrc_auth", forbidden)


def source_case(source_type, responses, **kwargs):
    events, retained = [], []
    session = SequenceSession(events, responses)
    gate = Gate(events)
    source = source_type(
        gate=gate,
        deadline=time.monotonic() + 120,
        check_cancelled=lambda: events.append("check"),
        session_factory=lambda: session,
        **kwargs,
    )
    return SimpleNamespace(source=source, session=session, gate=gate, events=events, retained=retained)


def listing_call(case, cursor=None):
    return case.source(
        cursor=cursor,
        pre_source_request=lambda: case.events.append("count"),
        source_observer=lambda payload: case.retained.append(payload),
    )


def detail_call(case):
    return case.source(
        check_cancelled=lambda: case.events.append("activity_check"),
        on_request=lambda: case.events.append("count"),
        source_observer=lambda kind, payload: case.retained.append((kind, payload)),
    )


def html(payload):
    return '<script type="application/json" data-sjs>' + json.dumps(payload) + "</script>"


def test_real_public_listing_gates_bootstrap_and_graphql_without_ambient_identity(monkeypatch):
    native = {
        "data": {
            scraper_module.InstagramScraper.PROFILE_POSTS_ROOT_FIELD_NAME: {
                "edges": [],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }
    bootstrap = {
        "body": '<html>"LSD",[],{"token":"attempt-local-runtime"}</html>',
        "headers": {"content-type": "text/html"},
        "cookies": {"csrftoken": "attempt-local-csrf"},
    }
    case = source_case(PublicListingSource, [bootstrap, {"payload": native}], account="bravotv")
    # Keep maintained query selection/parser; remove only wall-clock pacing in this fake-source test.
    monkeypatch.setattr(scraper_module.InstagramRateController, "before_query", lambda *args, **kwargs: None)
    assert listing_call(case, "frozen-public-cursor") == native
    assert case.retained == [native] and case.session.closed
    assert case.gate.request_count == case.events.count("count") == len(case.session.sent) == 2
    for index, (request, options) in enumerate(case.session.sent):
        assert options["allow_redirects"] is False and options["proxies"] == {}
        assert case.events[case.events.index("send") - 1] == "count"
        assert "Authorization" not in request.headers
        assert "sessionid" not in request.headers.get("Cookie", "")
        assert "ds_user_id" not in request.headers.get("Cookie", "")
        assert request.method == ("GET" if index == 0 else "POST")
    post = case.session.sent[1][0]
    assert post.headers["Cookie"] == "csrftoken=attempt-local-csrf"
    form = parse_qs(post.body)
    assert json.loads(form["variables"][0])["after"] == "frozen-public-cursor"
    assert form["av"] == form["__user"] == ["0"]
    assert case.session.trust_env is False and case.session.auth is None
    assert all(adapter.max_retries.total == 0 for adapter in case.session.adapters.values())
    case.source.finish_success()
    assert case.events[-1] == "finish_success"


def test_real_public_permalink_parser_retains_native_media_not_bootstrap_html():
    media = {"pk": "66", "code": "AAAABC", "user": {"pk": "99", "username": "bravotv"}, "comment_count": 3}
    envelope = {"data": {"xdt_api__v1__media__shortcode__web_info": {"items": [media]}}}
    case = source_case(
        PublicDetailSource,
        [{"body": html(envelope), "headers": {"content-type": "text/html"}}],
        shortcode="AAAABC",
    )
    assert detail_call(case) == media
    assert case.retained == [("details", media)]
    assert case.gate.request_count == case.events.count("count") == 1
    assert case.session.closed and "Cookie" not in case.session.sent[0][0].headers
    assert case.session.sent[0][0].url == "https://www.instagram.com/p/AAAABC/"


@pytest.mark.parametrize(
    "spec,code",
    [
        ({"status": 401}, "unauthorized"),
        ({"status": 403}, "forbidden"),
        ({"status": 429, "headers": {"retry-after": "37"}}, "rate_limited"),
        ({"status": 302, "headers": {"location": "/accounts/login/"}}, "redirect_login"),
        ({"payload": {"message": "login_required", "private": "forbidden-payload"}}, "login_required"),
        (requests.ConnectionError("forbidden-provider-message"), "network_error"),
    ],
)
def test_public_listing_failure_escapes_legacy_recovery_after_one_counted_send(monkeypatch, spec, code):
    monkeypatch.setattr(scraper_module.InstagramRateController, "before_query", lambda *args, **kwargs: None)
    case = source_case(PublicListingSource, [spec], account="bravotv")
    with pytest.raises(SourceFailure, match=code) as caught:
        listing_call(case)
    assert "forbidden-payload" not in str(caught.value) and "provider-message" not in str(caught.value)
    assert case.session.closed and len(case.session.sent) == case.events.count("count") == 1
    case.source.finish_failure(caught.value)
    assert case.events[-1] == ("finish_failure", None)


@pytest.mark.parametrize(
    "spec,code",
    [({"body": "not-json"}, "parse_error"), ({"payload": []}, "parse_error"), ({"status": 429}, "rate_limited")],
)
def test_graphql_post_rejection_cannot_replace_observed_session_or_retry(monkeypatch, spec, code):
    monkeypatch.setattr(scraper_module.InstagramRateController, "before_query", lambda *args, **kwargs: None)
    case = source_case(
        PublicListingSource,
        [{"body": "<html>anonymous-bootstrap</html>", "headers": {"content-type": "text/html"}}, spec],
        account="bravotv",
    )
    with pytest.raises(SourceFailure, match=code):
        listing_call(case)
    assert [request.method for request, _ in case.session.sent] == ["GET", "POST"]
    assert case.gate.request_count == case.events.count("count") == 2 and case.session.closed


def test_public_partial_permalink_is_unresolved_without_alternate_routes_or_source_unavailable():
    case = source_case(PublicDetailSource, [{"body": "<html>partial</html>"}], shortcode="AAAABC")
    with pytest.raises(SourceFailure, match="public_detail_unresolved"):
        detail_call(case)
    assert len(case.session.sent) == 1 and case.retained == [] and case.session.closed


@pytest.mark.parametrize("name", ["sessionid", "ds_user_id", "SESSIONID", " ds_User_ID "])
def test_public_response_identity_is_rejected_before_any_followup_send(monkeypatch, name):
    monkeypatch.setattr(scraper_module.InstagramRateController, "before_query", lambda *args, **kwargs: None)
    case = source_case(PublicListingSource, [{"cookies": {name: "forbidden-identity"}}], account="bravotv")
    with pytest.raises(SourceFailure, match="public_access_backoff"):
        listing_call(case)
    assert len(case.session.sent) == 1 and case.session.closed


@pytest.mark.parametrize("reason", ["deadline", "cancelled"])
def test_public_source_fences_before_client_construction(reason):
    def cancelled():
        if reason == "cancelled":
            raise LostOwnership("cancelled")

    source = PublicDetailSource(
        shortcode="ABC",
        gate=Gate([]),
        deadline=time.monotonic() - 1,
        check_cancelled=cancelled,
        session_factory=lambda: pytest.fail("client before owner/deadline"),
    )
    with pytest.raises(SourceFailure if reason == "deadline" else LostOwnership):
        source(
            check_cancelled=cancelled,
            on_request=lambda: pytest.fail("count before owner/deadline"),
            source_observer=lambda *args: pytest.fail("unexpected evidence"),
        )


def test_public_cookie_guard_allows_attempt_local_ancillary_and_rejects_any_supplied_state():
    guard = PublicCookieIdentity({})
    for initial in ({"csrftoken": "prior-attempt"}, {"sessionid": ""}, None):
        with pytest.raises(CredentialIdentityMismatch):
            PublicCookieIdentity(initial)
    jar = requests.cookies.RequestsCookieJar()
    jar.set("csrftoken", "local-csrf", domain=".instagram.com")
    assert guard.prepare_jar(jar, {}) == {"csrftoken": "local-csrf"}
    assert guard.merge({"csrftoken": "local-csrf"}, {"mid": "local-mid"}) == {
        "csrftoken": "local-csrf",
        "mid": "local-mid",
    }


def test_public_session_rechecks_deadline_after_permit_and_before_next_send():
    events = []
    session = SequenceSession(events, [{"payload": {"status": "ok"}}])
    instant = [10]
    wrapped = ObservedPublicSession(
        session=session,
        gate=Gate(events),
        deadline=20,
        clock=lambda: instant[0],
        check_cancelled=lambda: None,
        on_request=lambda: events.append("count"),
        on_payload=lambda _: instant.__setitem__(0, 21),
        on_failure=lambda _: None,
    )
    with pytest.raises(_CallbackAbort) as caught:
        wrapped.get("https://www.instagram.com/bravotv/")
    assert isinstance(caught.value.error, SourceFailure)
    assert str(caught.value.error) == "target_deadline" and events.count("count") == 1
    with pytest.raises(_CallbackAbort):
        wrapped.get("https://www.instagram.com/bravotv/")
    assert len(session.sent) == 1
    wrapped.close()


@pytest.mark.parametrize("stage", ["listing", "details"])
def test_native_public_html_with_incidental_auth_words_and_login_navigation_is_accepted(monkeypatch, stage):
    monkeypatch.setattr(scraper_module.InstagramRateController, "before_query", lambda *args, **kwargs: None)
    native = {
        "pk": "66",
        "code": "AAAABC",
        "user": {"pk": "99", "username": "bravotv"},
        "caption": {"text": "Checkpoint challenge recap /accounts/login/"},
        "like_count": 0,
        "comment_count": 0,
        "media_type": 1,
        "image_versions2": {"candidates": [{"url": "https://scontent.cdninstagram.com/image.png"}]},
    }
    envelope = {"data": {"xdt_api__v1__media__shortcode__web_info": {"items": [native]}}}
    document = (
        '<html><title>Challenge recap • Instagram photos</title><nav><a href="/accounts/login/">Login</a></nav>'
        '<script>const challenge = "checkpoint";</script>' + html(envelope) + "</html>"
    )
    if stage == "listing":
        payload = {
            "data": {
                scraper_module.InstagramScraper.PROFILE_POSTS_ROOT_FIELD_NAME: {
                    "edges": [{"node": native}],
                    "page_info": {"has_next_page": False, "end_cursor": None},
                }
            }
        }
        case = source_case(
            PublicListingSource,
            [{"body": document, "headers": {"content-type": "text/html"}}, {"payload": payload}],
            account="bravotv",
        )
        assert listing_call(case) == payload
        assert case.retained == [payload]
    else:
        case = source_case(
            PublicDetailSource, [{"body": document, "headers": {"content-type": "text/html"}}], shortcode="AAAABC"
        )
        assert detail_call(case) == native
        assert case.retained == [("details", native)]
    evidence = Evidence.capture(
        source_identity="instagram:post:AAAABC",
        captured_at=datetime.now(UTC),
        parser_version=PARSER_VERSION,
        payload=native,
    )
    parsed = parse_evidence(evidence, account="bravotv", stage=Stage.DETAILS, target_key="AAAABC", pinned=True)
    assert validate_detail(parsed)["media_complete"] is True
    partial = Evidence.capture(
        source_identity="instagram:post:AAAABC",
        captured_at=evidence.captured_at,
        parser_version=PARSER_VERSION,
        payload={key: value for key, value in native.items() if key != "like_count"},
    )
    with pytest.raises(InvalidSourceError, match="required source coverage"):
        validate_detail(
            parse_evidence(partial, account="bravotv", stage=Stage.DETAILS, target_key="AAAABC", pinned=True)
        )
    assert case.events.count("count") == len(case.session.sent) == (2 if stage == "listing" else 1)
    assert case.session.closed


def rejected_response(kind, retry_after):
    options = {"headers": {"retry-after": retry_after, "content-type": "application/json"}}
    if kind in ("unauthorized", "forbidden", "rate_limited"):
        options["status"] = {"unauthorized": 401, "forbidden": 403, "rate_limited": 429}[kind]
    elif kind == "redirect_login":
        options.update(status=302)
        options["headers"]["location"] = "/accounts/login/"
    elif kind == "login_required":
        options["payload"] = {"message": "login_required"}
    elif kind == "challenge_required":
        options["body"] = "<title>Security Check • Instagram</title>"
        options["headers"]["content-type"] = "text/html"
    else:
        assert kind == "public_access_backoff"
        options["cookies"] = {"sessionid": "forbidden-identity"}
    return options


@pytest.mark.parametrize("stage", ["listing", "details"])
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
def test_public_failures_forward_numeric_retry_after_to_the_gate(monkeypatch, stage, kind):
    monkeypatch.setattr(scraper_module.InstagramRateController, "before_query", lambda *args, **kwargs: None)
    source_type, identity = (
        (PublicListingSource, {"account": "bravotv"})
        if stage == "listing"
        else (PublicDetailSource, {"shortcode": "AAAABC"})
    )
    case = source_case(source_type, [rejected_response(kind, "37")], **identity)
    with pytest.raises(SourceFailure, match=kind):
        (listing_call if stage == "listing" else detail_call)(case)
    assert ("failure", kind, 1, 37.0) in case.events
    assert len(case.session.sent) == case.events.count("count") == 1 and case.session.closed


@pytest.mark.parametrize("stage", ["listing", "details"])
def test_public_auth_failure_preserves_http_date_retry_after(monkeypatch, stage):
    monkeypatch.setattr(scraper_module.InstagramRateController, "before_query", lambda *args, **kwargs: None)
    header = format_datetime(datetime.now(UTC) + timedelta(hours=2), usegmt=True)
    source_type, identity = (
        (PublicListingSource, {"account": "bravotv"})
        if stage == "listing"
        else (PublicDetailSource, {"shortcode": "AAAABC"})
    )
    case = source_case(source_type, [rejected_response("unauthorized", header)], **identity)
    with pytest.raises(SourceFailure, match="unauthorized"):
        (listing_call if stage == "listing" else detail_call)(case)
    failure = next(item for item in case.events if isinstance(item, tuple) and item[0] == "failure")
    assert failure[3] == pytest.approx(7200, abs=2)


@pytest.mark.parametrize(
    "method,path,category",
    [
        ("GET", "/private-account-sentinel/", "profile_bootstrap"),
        ("POST", "/graphql/query", "graphql"),
        ("GET", "/p/private-shortcode-sentinel/", "permalink"),
    ],
)
def test_public_403_log_is_structured_and_excludes_source_secrets(caplog, method, path, category):
    events, failures = [], []
    session = SequenceSession(
        events,
        [
            {
                "status": 403,
                "body": "private-body-sentinel",
                "headers": {
                    "content-type": "text/html",
                    "retry-after": "37",
                    "set-cookie": "sessionid=private-cookie-sentinel",
                    "location": "https://www.instagram.com/accounts/login/?token=private-redirect-sentinel",
                    "x-private": "private-header-sentinel",
                },
            }
        ],
    )
    wrapped = ObservedPublicSession(
        session=session,
        gate=Gate(events),
        deadline=time.monotonic() + 120,
        check_cancelled=lambda: None,
        on_request=lambda: events.append("count"),
        on_payload=lambda _: pytest.fail("HTML must not be retained"),
        on_failure=failures.append,
    )
    try:
        with pytest.raises(_CallbackAbort) as caught:
            wrapped.request(method, "https://www.instagram.com" + path + "?token=private-query-sentinel")
        assert isinstance(caught.value.error, SourceFailure) and str(caught.value.error) == "forbidden"
        records = [record for record in caplog.records if record.name == "trr_backend.ingestion.public_transport"]
        assert len(records) == 1 and records[0].levelname == "WARNING"
        assert json.loads(records[0].getMessage()) == {
            "event": "instagram_public_request_failed",
            "error_code": "forbidden",
            "status": 403,
            "method": method,
            "endpoint_category": category,
        }
        assert "sentinel" not in caplog.text and "https://" not in caplog.text
        assert ("failure", "forbidden", 1, 37.0) in events
        assert len(failures) == len(session.sent) == events.count("count") == 1
    finally:
        wrapped.close()
    assert session.closed


def test_log_handler_failure_cannot_replace_existing_public_failure_or_accounting(monkeypatch):
    from trr_backend.ingestion import public_transport

    def broken_logger(*args, **kwargs):
        raise RuntimeError("private-logger-error-sentinel")

    monkeypatch.setattr(public_transport.LOGGER, "warning", broken_logger)
    case = source_case(PublicDetailSource, [rejected_response("forbidden", "37")], shortcode="AAAABC")
    with pytest.raises(SourceFailure, match="forbidden"):
        detail_call(case)
    assert ("failure", "forbidden", 1, 37.0) in case.events
    assert len(case.session.sent) == case.events.count("count") == 1 and case.session.closed
