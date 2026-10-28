"""Bounded failure evidence through real fenced avatar activity; fake HTTP only."""

import copy
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg2
import pytest
import requests

from tests.ingestion import test_avatar_resolution as legacy
from tests.ingestion import test_public_avatar_graphql_recovery as public
from trr_backend.ingestion.avatar_resolution import AvatarResolutionError
from trr_backend.ingestion.domain import LostOwnership, fingerprint, stable_json
from trr_backend.ingestion.evidence import Evidence, sanitize_payload
from trr_backend.ingestion.profile_failure_diagnostics import PARSER_VERSION, validate_payload
from trr_backend.ingestion.profile_transport import PROFILE_DOC_ID, PROFILE_GRAPHQL_PARSER, PublicGraphQLProfileSource

SECRET = "private-cookie-comment-signed-url-must-never-retain"
database = legacy.database
avatars = legacy.avatars


def failure_native():
    return {
        "data": {"user": None},
        "errors": [{"message": SECRET, "extensions": {"code": 1357002}, "path": [SECRET]}],
        "cookies": {"sessionid": SECRET},
        "caption": SECRET,
    }


def observe(activity, owned, username, source):
    try:
        return activity.observe(owned=owned, username=username, source=source)
    finally:
        source.close()


def diagnostic(avatars, owned):
    target, rows = legacy.rows(avatars, owned)
    matches = [row for row in rows if row["parser_version"] == PARSER_VERSION]
    assert len(matches) == 1
    evidence = legacy.retained(avatars, matches[0])
    payload = evidence.read(pinned=True)
    assert SECRET not in evidence.payload_json
    for forbidden in ("sessionid", "csrftoken", "message", "Cookie", "Authorization", "https://"):
        assert forbidden not in evidence.payload_json
    return target, evidence, payload


def test_native_errors_retained_before_projection_with_exact_permits_hash_and_original_clock(avatars, monkeypatch):
    from trr_backend.ingestion import profile_failure_diagnostics as facts

    owned, activity, successes, failures = public.setup(avatars)
    username = "n" + uuid4().hex[:10]
    original = datetime.now(UTC) - timedelta(seconds=10)
    monkeypatch.setattr(facts, "utc_now", lambda: original)
    native = failure_native()
    session = public.Session(native)
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    with pytest.raises(AvatarResolutionError) as caught:
        observe(activity, owned, username, source)
    assert caught.value.failure.code == "parse_error"
    target, evidence, payload = diagnostic(avatars, owned)
    assert target["requests"] == 2 and len(session.calls) == 2 and session.closed
    assert successes == [1] and failures == [("parse_error", 2)]
    assert payload["phase"] == "projection" and payload["source"] == "public_graphql"
    assert payload["count"] == 2 and payload["user_id"] == "123" and payload["relay_doc_id"] == PROFILE_DOC_ID
    assert evidence.captured_at == original
    first, last = payload["basis"]
    assert [first["request_ordinal"], last["request_ordinal"]] == [1, 2]
    assert [first["request_number"], last["request_number"]] == [1, 2]
    assert first["observed_at"] == last["observed_at"] == original.isoformat()
    assert last["native_codes"] == [1357002] and last["code"] == "native_errors"
    assert last["shape"]["user_null"] and last["shape"]["errors_count"] == 1
    assert last["status"] == 200 and last["body_complete"] and last["body_observed"]
    assert last["content_hash"] == hashlib.sha256(json.dumps(native).encode()).hexdigest()
    assert last["bytes"] == len(json.dumps(native).encode())
    assert target["outcome"] == "unresolved" and legacy.registry(avatars, username) == []


def test_non_json_decode_keeps_status_shape_and_hash_without_body(avatars):
    owned, activity, _, failures = public.setup(avatars)

    class Session(public.Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            if method == "POST":
                response._content = ("not-json " + SECRET).encode()
            return response

    session = Session({})
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    with pytest.raises(AvatarResolutionError, match="profile_source_capability_missing"):
        observe(activity, owned, "decode_fixture", source)
    target, _, payload = diagnostic(avatars, owned)
    assert payload["phase"] == "decode" and payload["code"] == "profile_source_capability_missing"
    last = payload["basis"][-1]
    assert last["status"] == 200 and last["body_complete"] and not last["shape"]["data_present"]
    assert last["content_hash"] == hashlib.sha256(("not-json " + SECRET).encode()).hexdigest()
    assert failures == [("parse_error", 2)] and target["requests"] == 2


@pytest.mark.parametrize("save_failure", [None, "owner", "database"])
def test_partial_body_failure_keeps_original_error_server_hold_and_closes_response(avatars, monkeypatch, save_failure):
    owned, activity, _, failures = public.setup(avatars)
    closed = []

    class Session(public.Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            if method == "POST":
                response.headers["retry-after"] = "37"

                def chunks(*args, **kwargs):
                    yield b'{"data":'
                    raise requests.ReadTimeout(SECRET)

                response.iter_content = chunks
                response.close = lambda: closed.append(True)
            return response

    session = Session({})
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    original_retain = activity.store.retain_evidence

    def retain(**kwargs):
        if kwargs["evidence"].parser_version == PARSER_VERSION and save_failure:
            if save_failure == "owner":
                raise LostOwnership(SECRET)
            raise psycopg2.OperationalError(SECRET)
        return original_retain(**kwargs)

    monkeypatch.setattr(activity.store, "retain_evidence", retain)
    started = datetime.now(UTC)
    with pytest.raises(AvatarResolutionError) as caught:
        observe(activity, owned, "partial_fixture", source)
    assert caught.value.failure.retry_at is not None
    assert caught.value.failure.code == "network_error" and caught.value.failure.retry_at >= started + timedelta(
        seconds=37
    )
    assert closed == [True] and session.closed and len(session.calls) == 2 and failures == [("network_error", 2)]
    if save_failure is None:
        _, _, payload = diagnostic(avatars, owned)
        last = payload["basis"][-1]
        assert payload["phase"] == "body" and last["status"] == 200
        assert not last["body_complete"] and last["body_observed"] and last["bytes"] == 8
        assert last["content_hash"] == hashlib.sha256(b'{"data":').hexdigest()
    else:
        assert not [r for r in legacy.rows(avatars, owned)[1] if r["parser_version"] == PARSER_VERSION]


def test_stale_owner_keeps_original_fence_and_sends_no_second_request(avatars):
    owned, activity, _, failures = public.setup(avatars)

    def cancel():
        activity.store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))

    session = public.Session({}, after_bootstrap=cancel)
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    with pytest.raises(LostOwnership):
        observe(activity, owned, "cancel_fixture", source)
    assert len(session.calls) == 1 and failures == [] and session.closed
    assert not [r for r in legacy.rows(avatars, owned)[1] if r["parser_version"] == PARSER_VERSION]


def test_lost_permit_records_its_original_generation_without_clearing_newer_hold(avatars):
    owned, activity, _, failures = public.setup(avatars)
    _, tx = avatars

    class Session(public.Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            if method == "POST":
                with tx() as conn, conn.cursor() as cur:
                    cur.execute(
                        """update social.instagram_detail_request_lanes set failure_generation=failure_generation+1,
                        cooldown_until=clock_timestamp()+interval '60seconds', probe_token='replacement-probe',
                        probe_expires_at=clock_timestamp()+interval '60seconds' where identity_key=%s""",
                        (activity.gate.identity_key,),
                    )
            return response

    session = Session(failure_native())
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    with pytest.raises(AvatarResolutionError) as caught:
        observe(activity, owned, "permit_fixture", source)
    assert caught.value.failure.code == "identity_probe_lost"
    _, _, payload = diagnostic(avatars, owned)
    assert (
        payload["basis"][-1]["failure_generation"] == 0 and len(session.calls) == 2 and failures == [("parse_error", 2)]
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select failure_generation,cooldown_until>clock_timestamp()
                from social.instagram_detail_request_lanes where identity_key=%s""",
            (activity.gate.identity_key,),
        )
        assert cur.fetchone() == (1, True)


def test_successful_path_retains_only_original_native_profile_evidence(avatars):
    owned, activity, successes, failures = public.setup(avatars)
    username = "s" + uuid4().hex[:10]
    session = public.Session(public.native(username))
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    capture = observe(activity, owned, username, source)
    target, rows = legacy.rows(avatars, owned)
    assert target["requests"] == 2 and len(session.calls) == 2 and successes == [1, 2] and failures == []
    assert len(rows) == 1 and rows[0]["parser_version"] == PROFILE_GRAPHQL_PARSER
    assert capture.observation.username == username and target["outcome"] == "unresolved"


@pytest.fixture
def valid_diagnostic(avatars):
    owned, activity, _, _ = public.setup(avatars)
    source = PublicGraphQLProfileSource(
        user_id="123", deadline=time.monotonic() + 60, session=public.Session(failure_native())
    )
    with pytest.raises(AvatarResolutionError):
        observe(activity, owned, "schema_fixture", source)
    return diagnostic(avatars, owned)[2]


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown",
        "secret_phase",
        "bool_status",
        "bool_count",
        "bool_code",
        "too_many_facts",
        "too_many_codes",
        "shape_type",
    ],
)
def test_parser_specific_schema_rejects_unknown_secrets_types_and_limits(valid_diagnostic, mutation):
    value = copy.deepcopy(valid_diagnostic)
    if mutation == "unknown":
        value["private_cookie"] = SECRET
    elif mutation == "secret_phase":
        value["phase"] = SECRET
    elif mutation == "bool_status":
        value["basis"][0]["status"] = True
    elif mutation == "bool_count":
        value["count"] = True
    elif mutation == "bool_code":
        value["basis"][1]["native_codes"] = [True]
    elif mutation == "too_many_facts":
        value["basis"].append(value["basis"][0])
    elif mutation == "too_many_codes":
        value["basis"][1]["native_codes"] = list(range(9))
    elif mutation == "shape_type":
        value["basis"][0]["shape"]["data_present"] = "false"
    with pytest.raises(ValueError):
        validate_payload(value)
    # Even a resealed hash cannot bypass the parser-specific validation on read.
    evidence = Evidence(
        "instagram:public-profile-failure:schema_fixture",
        datetime.now(UTC),
        PARSER_VERSION,
        stable_json(value),
        fingerprint(value),
    )
    with pytest.raises(ValueError):
        evidence.read(pinned=True)


def test_diagnostic_roundtrip_and_ordinary_projection_remain_separate(valid_diagnostic):
    evidence = Evidence.capture(
        source_identity="instagram:public-profile-failure:schema_fixture",
        captured_at=datetime.now(UTC),
        parser_version=PARSER_VERSION,
        payload=valid_diagnostic,
    )
    assert evidence.read(pinned=True) == valid_diagnostic
    ordinary = {
        "username": "schema_fixture",
        "request_ordinal": 1,
        "body_complete": True,
        "shape": {"data_present": True},
    }
    assert sanitize_payload(ordinary) == {"username": "schema_fixture"}
    assert Evidence.capture(
        source_identity="ordinary", captured_at=datetime.now(UTC), parser_version="ordinary-v1", payload=ordinary
    ).read(pinned=True) == {"username": "schema_fixture"}


def test_legacy_public_decode_failure_has_one_fact_and_no_extra_request(avatars):
    from tests.ingestion.test_avatar_transport import RecordingSession
    from trr_backend.ingestion.avatar_transport import AvatarProfileSource

    owned, activity, _, failures = public.setup(avatars)
    session = RecordingSession([], body="not-json " + SECRET)
    source = AvatarProfileSource(cookies={}, public_only=True, deadline=time.monotonic() + 60, session=session)
    with pytest.raises(AvatarResolutionError):
        observe(activity, owned, "legacy_fixture", source)
    target, _, payload = diagnostic(avatars, owned)
    assert len(session.sent) == target["requests"] == payload["count"] == 1
    assert len(failures) == 1 and session.closed
    assert payload["source"] == "public_legacy" and payload["relay_doc_id"] is None
    fact = payload["basis"][0]
    assert fact["phase"] == "legacy_profile" and fact["request_number"] == 1
    assert fact["status"] == 200 and fact["body_complete"]
    assert fact["content_hash"] == hashlib.sha256(("not-json " + SECRET).encode()).hexdigest()
