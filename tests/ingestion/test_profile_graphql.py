"""Finite public query, native errors and identity-bound evidence contracts."""

import json
import time
from datetime import UTC, datetime

import pytest
import requests

from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.profile_transport import (
    PROFILE_DOC_ID,
    PROFILE_GRAPHQL_PARSER,
    PROFILE_QUERY,
    PROVIDERS,
    PublicGraphQLProfileSource,
    provider_flags,
)
from trr_backend.ingestion.profiles import ProfileError, parse_profile, project_profile_payload


def bootstrap():
    maps = {"gkxData": {}, "justknobxData": {}}
    for index, (_, (group, key)) in enumerate(PROVIDERS.items()):
        maps[group][key] = {"result": bool(index % 2)}
    return json.dumps(maps)


def provenance():
    return {
        "profile_query_id": PROFILE_DOC_ID,
        "profile_query_name": PROFILE_QUERY,
        "profile_provider_flags": provider_flags(bootstrap()),
    }


def native():
    return {
        "data": {
            "user": {"id": "101", "username": "reader", "follower_count": 26, "biography": "hello", "media_count": None}
        }
    }


def capture(payload, proof=None):
    return Evidence.capture(
        source_identity="instagram:commenter-profile:profile:id:101",
        captured_at=datetime.now(UTC),
        parser_version=PROFILE_GRAPHQL_PARSER,
        payload=project_profile_payload(
            payload, parser_version=PROFILE_GRAPHQL_PARSER, provenance=provenance() if proof is None else proof
        ),
    )


def test_native_status_absence_is_versioned_and_metadata_only():
    payload = native()
    payload["data"]["user"]["timeline"] = {"secret": "discard"}
    evidence = capture(payload)
    observation = parse_profile(evidence, target_key="profile:id:101", expected_id="101", username="reader")
    assert observation.values["followers_count"] == 26
    assert observation.coverage["posts_count"] == "unknown"
    assert observation.provenance == provenance()
    assert "timeline" not in evidence.read()["data"]["user"]
    with pytest.raises(ProfileError, match="identity_mismatch"):
        parse_profile(evidence, target_key="profile:id:101", expected_id="102", username="reader")
    with pytest.raises(ProfileError, match="identity_unresolved"):
        parse_profile(evidence, target_key="profile:id:101", expected_id=None, username="reader")


@pytest.mark.parametrize(
    "mutation",
    [
        {"errors": [{"message": "failure"}]},
        {"errors": {}},
        {"status": "fail"},
        {"login_required": True},
        {"rate_limited": True},
    ],
)
def test_native_failure_never_becomes_profile(mutation):
    with pytest.raises(ProfileError):
        capture(dict(native(), **mutation))


def test_provider_flags_required_and_conflicts_rejected():
    with pytest.raises(ProfileError, match="flags_missing"):
        provider_flags("{}")
    with pytest.raises(ProfileError, match="ambiguous"):
        provider_flags(bootstrap() + '{"gkxData":{"17028":{"result":true}}}')
    with pytest.raises(ProfileError, match="provenance_invalid"):
        capture(native(), {"profile_query_id": "unreviewed"})


class Session(requests.Session):
    def __init__(self, payload=None):
        super().__init__()
        self.calls = []
        self.payload = payload if payload is not None else native()

    def request(self, method, url, **kwargs):  # pyright: ignore[reportIncompatibleMethodOverride]
        self.calls.append((method, url, kwargs))
        response = requests.Response()
        response.status_code = 200
        response.headers["content-type"] = "text/html" if method == "GET" else "application/json"
        response._content = (bootstrap() if method == "GET" else json.dumps(self.payload)).encode()
        response._content_consumed = True
        return response


def test_two_counted_sends_no_fallback_or_ambient_auth(monkeypatch):
    monkeypatch.setenv("INSTAGRAM_PROFILE_PAGE_CONTENT_DOC_ID", "unreviewed")
    session = Session()
    source = PublicGraphQLProfileSource(user_id="101", deadline=time.monotonic() + 60, session=session)
    counted = []
    payload = source(username="reader", pre_source_request=lambda: counted.append(True))
    assert payload == native()
    assert len(session.calls) == len(counted) == 2
    assert [call[0] for call in session.calls] == ["GET", "POST"]
    for _, _, kwargs in session.calls:
        assert kwargs["allow_redirects"] is False and kwargs["auth"] is None and kwargs["proxies"] == {}
    data = session.calls[1][2]["data"]
    assert data["doc_id"] == PROFILE_DOC_ID
    assert data["av"] == data["__user"] == "0"
    variables = json.loads(data["variables"])
    assert variables["id"] == "101"
    for name, value in provider_flags(bootstrap()).items():
        assert variables["__relay_internal__pv__" + name + "relayprovider"] is value
    assert session.trust_env is False
    with pytest.raises(ProfileError, match="boundary"):
        source(username="reader", pre_source_request=lambda: counted.append(True))
    assert len(session.calls) == 2


def test_cancellation_before_second_send_prevents_query():
    session = Session()
    source = PublicGraphQLProfileSource(user_id="101", deadline=time.monotonic() + 60, session=session)

    def before():
        if session.calls:
            raise RuntimeError("cancelled")

    with pytest.raises(RuntimeError, match="cancelled"):
        source(username="reader", pre_source_request=before)
    assert len(session.calls) == 1


def test_network_failure_is_classified_without_retry():
    from trr_backend.socials.instagram.request_client import InstagramRequestFailure

    class Offline(Session):
        def request(self, *args, **kwargs):
            raise requests.ConnectionError("private wire details")

    source = PublicGraphQLProfileSource(user_id="101", deadline=time.monotonic() + 60, session=Offline())
    counted = []
    with pytest.raises(InstagramRequestFailure) as caught:
        source(username="reader", pre_source_request=lambda: counted.append(True))
    assert caught.value.error_code == "network_error"
    assert len(counted) == 1


@pytest.mark.parametrize("during_read", [False, True])
def test_expected_profile_deadline_is_distinct_from_protocol_or_retry_limit(during_read):
    from trr_backend.ingestion.identity_policy import IdentityDeferred

    now = [10.0]

    class DeadlineSession(Session):
        def request(self, *args, **kwargs):
            response = super().request(*args, **kwargs)
            if during_read:
                now[0] = 20.0
            return response

    session = DeadlineSession()
    source = PublicGraphQLProfileSource(
        user_id="101", deadline=20.0 if during_read else 9.0, session=session, clock=lambda: now[0]
    )
    calls = []
    with pytest.raises(IdentityDeferred) as caught:
        source(username="reader", pre_source_request=lambda: calls.append(True))
    assert caught.value.failure.code == "target_deadline"
    assert len(calls) == len(session.calls) == (1 if during_read else 0)
    source.close()


def test_admission_time_cannot_leave_an_expired_physical_send_or_stale_timeout():
    from trr_backend.ingestion.identity_policy import IdentityDeferred

    now = [10.0]
    session = Session()
    source = PublicGraphQLProfileSource(user_id="101", deadline=20, session=session, clock=lambda: now[0])

    def expired():
        now[0] = 19.5

    with pytest.raises(IdentityDeferred) as caught:
        source(username="reader", pre_source_request=expired)
    assert caught.value.failure.code == "target_deadline" and session.calls == [] and source._sends == 0
    source.close()


def test_physical_timeout_uses_budget_remaining_after_control_callback():
    now = [10.0]
    session = Session()
    source = PublicGraphQLProfileSource(user_id="101", deadline=30, session=session, clock=lambda: now[0])

    def admission():
        now[0] += 4

    source(username="reader", pre_source_request=admission)
    assert session.calls[0][2]["timeout"] == (5, 8)
    assert session.calls[1][2]["timeout"] == (4, 6)
    source.close()
