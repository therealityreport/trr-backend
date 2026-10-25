"""The shared avatar observer preserves server waits at every error boundary."""

import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests.ingestion import test_public_avatar_graphql_recovery as recovery
from trr_backend.ingestion.avatar_resolution import AvatarResolutionError, _maximum_server_delay
from trr_backend.ingestion.profile_transport import PROFILE_GRAPHQL_PARSER, PublicGraphQLProfileSource
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

database = recovery.database
avatars = recovery.avatars


@pytest.mark.parametrize(
    "exception,source,expected",
    [
        (None, 37, 37),
        (20, 60, 60),
        (90, 37, 90),
        (None, 0, 0),
        (-3, 37, 37),
        (float("nan"), 37, 37),
        (float("inf"), 37, 37),
        (True, 37, 37),
        (False, None, None),
        (37, float("inf"), 37),
        (-1, float("nan"), None),
        ("37", None, None),
        (10**1000, 37, 10**1000),
    ],
)
def test_maximum_delay_uses_only_gate_valid_numeric_values(exception, source, expected):
    result = _maximum_server_delay(
        SimpleNamespace(retry_after_seconds=exception), SimpleNamespace(retry_after_seconds=source)
    )
    assert result == expected


@pytest.mark.parametrize(
    "exception_delay,source_delay,preexisting",
    [(None, 37, False), (90, 37, False), (20, 60, False), (None, 37, True), (10**1000, 37, False)],
)
def test_transport_error_none_competing_and_preexisting_holds_remain_monotone(
    avatars, exception_delay, source_delay, preexisting
):
    owned, activity, successes, failures = recovery.setup(avatars)
    _, tx = avatars
    hold = datetime.now(UTC) + timedelta(seconds=300)
    error = InstagramRequestFailure(
        "rate_limited", status_code=429, retryable=True, retry_after_seconds=exception_delay
    )

    class Source:
        parser_version = PROFILE_GRAPHQL_PARSER
        request_budget = 2
        user_id = "123"
        retry_after_seconds = source_delay
        closed = False

        def __call__(self, *, username, pre_source_request):
            pre_source_request()
            pre_source_request()
            if preexisting:
                with tx() as conn, conn.cursor() as cur:
                    cur.execute(
                        """update social.instagram_detail_request_lanes set cooldown_until=%s,
                    server_retry_after_until=%s where identity_key=%s""",
                        (hold, hold, activity.gate.identity_key),
                    )
            raise error

        def close(self):
            self.closed = True

    source = Source()
    started = datetime.now(UTC)
    with pytest.raises(AvatarResolutionError) as caught:
        activity.execute(**owned, handles=["hold" + uuid4().hex[:10]], source_factory=lambda _: source)
    assert caught.value.failure.code == "rate_limited" and successes == [1] and failures == [("rate_limited", 2)]
    assert source.closed
    if preexisting:
        assert caught.value.failure.retry_at == hold
    elif exception_delay is not None and exception_delay > 1e100:
        assert caught.value.failure.retry_at == datetime.max.replace(tzinfo=UTC)
    else:
        assert caught.value.failure.retry_at >= started + timedelta(
            seconds=max(value for value in (exception_delay, source_delay) if value is not None)
        )


@pytest.mark.parametrize(
    "case,delay,public_code,health_code,request_count",
    [
        ("native_rate", 37, "rate_limited", "rate_limited", 2),
        ("wrong_id", 37, "profile_identity_mismatch", "parse_error", 2),
        ("null_user", 37, "profile_source_capability_missing", "parse_error", 2),
        ("null_user", None, "profile_source_capability_missing", None, 2),
        ("flags", 37, "profile_provider_flags_missing", "parse_error", 1),
    ],
)
def test_real_public_native_projection_and_retained_parse_keep_header_hold_and_diagnosis(
    avatars,
    case,
    delay,
    public_code,
    health_code,
    request_count,
):
    owned, activity, successes, failures = recovery.setup(avatars)
    username = "native" + uuid4().hex[:10]
    payload = (
        recovery.native(username, identity="999")
        if case == "wrong_id"
        else {"data": {"user": None}, "errors": [{"message": "rate limit exceeded"}]}
        if case == "native_rate"
        else {"data": {"user": None}}
    )

    class Session(recovery.Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            if (method == "POST" or case == "flags") and delay is not None:
                response.headers["retry-after"] = str(delay)
            if case == "flags" and method == "GET":
                response._content = b"{}"
            return response

    session = Session(payload)
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    started = datetime.now(UTC)
    with pytest.raises(AvatarResolutionError) as caught:
        activity.execute(**owned, handles=[username], source_factory=lambda _: source)
    assert caught.value.failure.code == public_code and session.closed and len(session.calls) == request_count
    assert failures == ([(health_code, request_count)] if health_code else [])
    assert recovery.legacy.registry(avatars, username) == []
    if delay is None:
        assert caught.value.failure.retry_at is None
    else:
        assert caught.value.failure.retry_at >= started + timedelta(seconds=delay)


@pytest.mark.parametrize(
    "failure,public_code,health_code",
    [("read", "network_error", "network_error"), ("limit", "profile_response_limit", "parse_error")],
)
def test_observed_header_survives_read_or_body_limit_failure(avatars, failure, public_code, health_code):
    import requests

    owned, activity, successes, failures = recovery.setup(avatars)
    username = "body" + uuid4().hex[:10]

    class Session(recovery.Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            response.headers["retry-after"] = "37"

            def content(*args, **kwargs):
                if failure == "limit":
                    yield b"x" * (2 * 1024 * 1024 + 1)
                else:
                    yield b"partial"
                    raise requests.ReadTimeout("synthetic source body read failure")

            response.iter_content = content
            return response

    session = Session(recovery.native(username))
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    started = datetime.now(UTC)
    with pytest.raises(AvatarResolutionError) as caught:
        activity.execute(**owned, handles=[username], source_factory=lambda _: source)
    assert caught.value.failure.code == public_code
    assert caught.value.failure.retry_at >= started + timedelta(seconds=37)
    assert source.retry_after_seconds == 37 and successes == [] and failures == [(health_code, 1)]
    assert session.closed and len(session.calls) == 1


def test_later_weaker_profile_header_cannot_shorten_source_wait(avatars):
    owned, activity, successes, failures = recovery.setup(avatars)

    class Session(recovery.Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            response.headers["retry-after"] = "90" if method == "GET" else "37"
            return response

    session = Session({"data": {"user": None}, "errors": [{"message": "rate limit exceeded"}]})
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    started = datetime.now(UTC)
    with pytest.raises(AvatarResolutionError) as caught:
        activity.execute(**owned, handles=["weaker" + uuid4().hex[:10]], source_factory=lambda _: source)
    assert caught.value.failure.code == "rate_limited" and source.retry_after_seconds == 90
    assert caught.value.failure.retry_at >= started + timedelta(seconds=90)
    assert successes == [1] and failures == [("rate_limited", 2)] and session.closed


def _lane(avatars, activity):
    from psycopg2.extras import RealDictCursor

    with avatars[1]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select * from social.instagram_detail_request_lanes where identity_key=%s",
            (activity.gate.identity_key,),
        )
        return dict(cur.fetchone())


@pytest.mark.parametrize(
    "status,header,longer", [(429, "37", False), (200, "37", False), (429, "37", True), (200, None, False)]
)
def test_real_received_header_survives_body_deadline_without_health_penalty(avatars, status, header, longer):
    from trr_backend.ingestion.identity_policy import IdentityDeferred

    owned, activity, successes, failures = recovery.setup(avatars)
    username = "deadline" + uuid4().hex[:10]
    now = [10.0]
    hold = datetime.now(UTC) + timedelta(seconds=300)

    class Session(recovery.Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            if method == "POST":
                response.status_code = status
                if header is not None:
                    response.headers["retry-after"] = header

                def content(*args, **kwargs):
                    if longer:
                        with avatars[1]() as conn, conn.cursor() as cur:
                            cur.execute(
                                """update social.instagram_detail_request_lanes set cooldown_until=%s,
                                server_retry_after_until=%s where identity_key=%s""",
                                (hold, hold, activity.gate.identity_key),
                            )
                    now[0] = 21.0
                    yield b"body received after deadline"

                response.iter_content = content
            return response

    session = Session(recovery.native(username))
    source = PublicGraphQLProfileSource(user_id="123", deadline=20, clock=lambda: now[0], session=session)
    started = datetime.now(UTC)
    with pytest.raises(IdentityDeferred) as caught:
        activity.execute(**owned, handles=[username], source_factory=lambda _: source)
    assert caught.value.failure.code == "target_deadline"
    row = _lane(avatars, activity)
    assert row["consecutive_failures"] == 0 and row["last_error_code"] is None
    assert failures == [] and successes == [1] and session.closed and len(session.calls) == 2
    if header is None:
        assert row["server_retry_after_until"] is None and row["cooldown_until"] is None
    elif longer:
        assert row["server_retry_after_until"] == hold and row["cooldown_until"] == hold
    else:
        assert row["server_retry_after_until"] >= started + timedelta(seconds=37)
        assert row["cooldown_until"] >= row["server_retry_after_until"]
        assert source._retry_after_permit.request_number == 2


@pytest.mark.parametrize("failure", ["accounting", "fence", "cleanup", "hold_save"])
def test_header_abort_retains_original_control_error_and_exact_received_permit(avatars, failure, monkeypatch):
    import psycopg2

    from trr_backend.ingestion.domain import LostOwnership

    owned, activity, successes, failures = recovery.setup(avatars)
    username = "control" + uuid4().hex[:10]
    original = (
        LostOwnership("synthetic owner loss")
        if failure == "fence"
        else psycopg2.OperationalError("synthetic local control failure")
    )
    session = recovery.Session(recovery.native(username))
    request = session.request

    def response_with_header(method, url, **kwargs):
        response = request(method, url, **kwargs)
        response.headers["retry-after"] = "37"
        return response

    session.request = response_with_header
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    check_owner = activity.store.check_owner
    accounting = [0]
    fence_lost = [False]
    finish = activity.gate.finish_failure
    settled = []

    def check(**kwargs):
        if fence_lost[0]:
            raise original
        if kwargs.get("record_request"):
            accounting[0] += 1
            if accounting[0] == 2:
                if failure == "fence":
                    fence_lost[0] = True
                raise original
        return check_owner(**kwargs)

    def finish_failure(code=None, **kwargs):
        if code is not None:
            settled.append(kwargs["permit"].request_number)
        if failure == "cleanup" and code is None:
            raise RuntimeError("synthetic first cleanup failure")
        if failure == "hold_save" and code is not None:
            raise RuntimeError("synthetic hold save failure")
        return finish(code, **kwargs)

    monkeypatch.setattr(activity.store, "check_owner", check)
    monkeypatch.setattr(activity.gate, "finish_failure", finish_failure)
    started = datetime.now(UTC)
    with pytest.raises(type(original)) as caught:
        activity.execute(**owned, handles=[username], source_factory=lambda _: source)
    assert caught.value is original and session.closed and len(session.calls) == 1
    assert source._retry_after_permit.request_number == 1
    row = _lane(avatars, activity)
    assert row["consecutive_failures"] == 0 and row["last_error_code"] is None and failures == []
    if failure == "accounting":
        assert settled == [1]  # POST permit 2 was admitted but never sent.
        assert row["server_retry_after_until"] >= started + timedelta(seconds=37)
    else:
        assert row["server_retry_after_until"] is None
