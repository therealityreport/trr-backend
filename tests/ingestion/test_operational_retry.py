"""Operational errors never invent a source wait; explicit source proof survives."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from types import SimpleNamespace

import psycopg2
import pytest

from tests.ingestion.test_media import Response, storage
from tests.ingestion.test_public_network_lanes import _gate, _Lanes
from trr_backend.ingestion import media
from trr_backend.ingestion.avatar_resolution import AvatarResolutionError
from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.identity_policy import IdentityDeferred
from trr_backend.ingestion.profiles import ProfileError
from trr_backend.ingestion.worker import classify_failure
from trr_backend.socials.instagram.detail_transport import DetailDeferred, DetailTransport
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

NOW = datetime(2026, 10, 22, 12, tzinfo=UTC)
SOURCE = "https://scontent.cdninstagram.com/retained.png"


def test_expired_identity_deadline_never_creates_a_future_retry_or_counts():
    lanes = _Lanes()
    gate = _gate(lanes)
    gate.deadline = 0
    with pytest.raises(IdentityDeferred) as failure:
        gate.before_request()
    assert failure.value.failure == Failure("target_deadline")
    assert gate.request_count == 0 and not lanes.rows


@pytest.mark.parametrize("cause", ["missing_lane", "database_unavailable"])
def test_identity_policy_failures_have_no_invented_retry(cause):
    lanes = _Lanes()
    gate = _gate(lanes)
    if cause == "database_unavailable":

        @contextmanager
        def unavailable():
            raise psycopg2.OperationalError("synthetic database outage")
            yield  # pragma: no cover - context manager protocol

        gate.transaction = unavailable
    with pytest.raises(IdentityDeferred) as failure, gate._locked():
        pytest.fail("Unavailable policy admitted a source request")
    expected = "identity_lane_missing" if cause == "missing_lane" else "identity_policy_unavailable"
    assert failure.value.failure == Failure(expected)
    assert gate.request_count == 0


@pytest.mark.parametrize("competing", ["none", "expired", "live"])
def test_lost_probe_waits_only_for_an_actual_live_competing_lease(competing):
    lanes = _Lanes()
    gate = _gate(lanes)
    row = lanes.row(gate.identity_key)
    row.update(probe_token="old", probe_expires_at=lanes.now + timedelta(seconds=35))
    gate._probe_token = "old"
    permit = gate.before_request()
    expiry = lanes.now + timedelta(seconds=20 if competing == "live" else -1)
    row.update(
        probe_token="replacement" if competing != "none" else None,
        probe_expires_at=expiry if competing != "none" else None,
    )
    before = dict(row)
    failure = gate.record_failure("rate_limited", permit=permit)
    assert failure == Failure("identity_probe_lost", retry_at=expiry if competing == "live" else None)
    assert row == before  # A late result cannot alter the replacement lease.


@pytest.mark.parametrize("kind", ["worker", "profile", "avatar", "legacy_deadline"])
def test_operational_failure_defaults_have_no_future_retry(kind):
    if kind == "worker":
        failure = classify_failure(RuntimeError("private transport text"))
    elif kind == "profile":
        failure = ProfileError("profile_identity_unresolved").failure
    elif kind == "avatar":
        failure = AvatarResolutionError("avatar_profile_incomplete").failure
    else:
        transport = SimpleNamespace(deadline=0, check_cancelled=lambda: None)
        with pytest.raises(DetailDeferred) as error:
            DetailTransport.check_deadline(transport)
        failure = classify_failure(error.value)
    assert failure.retry_at is None and not failure.source_unavailable


def test_supplied_server_and_auth_failures_keep_their_exact_deadline_and_block():
    explicit = Failure("challenge_required", retry_at=NOW + timedelta(seconds=120), identity_blocked=True)

    class SuppliedError(Exception):
        failure = explicit

    assert AvatarResolutionError("avatar_profile_incomplete", failure=explicit).failure is explicit
    assert classify_failure(SuppliedError()) is explicit
    error = InstagramRequestFailure("challenge_required", status_code=403, retryable=False)
    error.next_attempt_at = explicit.retry_at
    assert classify_failure(error) == explicit


@pytest.fixture
def fixed_media_clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(media, "datetime", Clock)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("", None),
        ("  ", None),
        ("invalid", None),
        ("-1", None),
        ("1.5", None),
        ("0", NOW),
        ("000", NOW),
        ("120", NOW + timedelta(seconds=120)),
        (" 002 ", NOW + timedelta(seconds=2)),
        (format_datetime(NOW - timedelta(seconds=1), usegmt=True), None),
        (format_datetime(NOW + timedelta(seconds=45), usegmt=True), NOW + timedelta(seconds=45)),
        ("999999999999", datetime.max.replace(tzinfo=UTC)),
        pytest.param("9" * 5000, datetime.max.replace(tzinfo=UTC), id="very-large-server-delay"),
    ],
)
def test_media_retry_dates_require_actual_valid_source_values(fixed_media_clock, header, expected):
    assert media._retry_at(header) == expected


@pytest.mark.parametrize(
    ("status", "header", "expected"),
    [
        (429, None, None),
        (429, "invalid", None),
        (429, "0", NOW),
        (429, "120", NOW + timedelta(seconds=120)),
        (503, "invalid", None),
        (503, "0", NOW),
        (503, format_datetime(NOW - timedelta(seconds=1), usegmt=True), None),
        (503, format_datetime(NOW + timedelta(seconds=45), usegmt=True), NOW + timedelta(seconds=45)),
    ],
)
def test_media_throttle_remains_unresolved_without_hidden_retry(fixed_media_clock, status, header, expected):
    calls = []

    def opener(*args, **kwargs):
        calls.append(1)
        return Response(status=status, headers={"Retry-After": header} if header is not None else {})

    with pytest.raises(media.MediaIncomplete) as error:
        storage(opener=opener)._download(SOURCE, check=lambda: None, request=lambda: None)
    assert error.value.failure == Failure("media_source_throttled", retry_at=expected)
    assert not error.value.failure.source_unavailable and calls == [1]
