from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest

from trr_backend.ingestion import identity_policy
from trr_backend.ingestion.identity_policy import SourceRequestGate


class _Lanes:
    """In-memory stand-in for social.instagram_detail_request_lanes (no database)."""

    def __init__(self):
        self.rows, self.now = {}, datetime(2026, 10, 19, 21, 0, tzinfo=UTC)

    def row(self, key):
        return self.rows.setdefault(
            key,
            {
                "identity_key": key,
                "request_times": [],
                "blocked": False,
                "blocked_fingerprint": None,
                "cooldown_until": None,
                "server_retry_after_until": None,
                "probe_token": None,
                "probe_expires_at": None,
                "consecutive_failures": 0,
                "last_error_code": None,
                "failure_generation": 0,
            },
        )

    @contextmanager
    def transaction(self):
        lanes = self

        class Cursor:
            connection = None

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, params=()):
                text = " ".join(sql.split())
                self._result = None
                if text.startswith("set local"):
                    return
                # The gate folds its lock/statement timeouts into the next statement.
                timeout_prefix = "select set_config('lock_timeout',%s,true),set_config('statement_timeout',%s,true); "
                if text.startswith(timeout_prefix):
                    text, params = text[len(timeout_prefix) :], tuple(params)[2:]
                if text.startswith("insert into social.instagram_detail_request_lanes"):
                    lanes.row(params[0])
                elif text.startswith("with locked_lane as materialized"):
                    found = lanes.rows.get(params[0])
                    self._result = None if found is None else {**found, "current_time": lanes.now}
                elif text.startswith("select * from social.instagram_detail_request_lanes"):
                    self._result = dict(lanes.rows[params[0]]) if params[0] in lanes.rows else None
                elif text.startswith("select clock_timestamp()"):
                    self._result = {"current_time": lanes.now}
                elif "set request_times=" in text:
                    row = lanes.rows[params[6]]
                    row.update(
                        request_times=params[0],
                        blocked=params[1],
                        cooldown_until=params[2],
                        server_retry_after_until=params[3],
                        probe_token=params[4],
                        probe_expires_at=params[5],
                    )
                elif "set blocked=%s,last_error_code=%s" in text:
                    row = lanes.rows[params[8]]
                    row.update(
                        blocked=params[0],
                        last_error_code=params[1],
                        consecutive_failures=params[2],
                        cooldown_until=params[3],
                        server_retry_after_until=params[4],
                        blocked_fingerprint=params[5],
                        probe_token=params[6],
                        probe_expires_at=params[7],
                        failure_generation=row["failure_generation"] + 1,
                    )
                elif "cooldown_until=%s,server_retry_after_until=%s,blocked_fingerprint=%s" in text:
                    row = lanes.rows[params[5]]
                    row["consecutive_failures"], row["last_error_code"] = 0, None
                    row.update(
                        cooldown_until=params[0],
                        server_retry_after_until=params[1],
                        blocked_fingerprint=params[2],
                        probe_token=params[3],
                        probe_expires_at=params[4],
                    )
                elif "set probe_token=null,probe_expires_at=null" in text:
                    row = lanes.rows[params[0]]
                    if row["probe_token"] == params[1]:
                        row.update(probe_token=None, probe_expires_at=None)
                else:  # pragma: no cover - a new statement must be modelled explicitly
                    raise AssertionError(f"unmodelled SQL: {text}")

            def fetchone(self):
                return self._result

        class Conn:
            autocommit = False

            def cursor(self, cursor_factory=None):
                return Cursor()

        yield Conn()


def _gate(lanes, *, lane="modal", public_only=True, cookies=None):
    return SourceRequestGate(
        cookies=cookies or {},
        transaction=lanes.transaction,
        check_cancelled=lambda: None,
        deadline=10**9,
        public_only=public_only,
        network_lane=lane,
        monotonic=lambda: 0.0,
    )


def _fail_once(lanes, code, *, lane="modal", retry_after=None):
    gate = _gate(lanes, lane=lane)
    row = lanes.row(gate.identity_key)
    row["cooldown_until"] = None  # the previous cooldown has passed
    permit = gate.before_request()
    failure = gate.record_failure(code, retry_after_seconds=retry_after, permit=permit)
    return failure, lanes.rows[gate.identity_key]


def test_lane_keys_separate_modal_local_and_the_legacy_shared_lane():
    lanes = _Lanes()
    modal, local, shared = _gate(lanes, lane="modal"), _gate(lanes, lane="local"), _gate(lanes, lane=None)
    assert modal.identity_key == hashlib.sha256(b"public:modal").hexdigest()
    assert local.identity_key == hashlib.sha256(b"public:local").hexdigest()
    assert shared.identity_key == hashlib.sha256(b"public").hexdigest()
    assert len({modal.identity_key, local.identity_key, shared.identity_key}) == 3


@pytest.mark.parametrize(("lane", "public_only"), [("cloud", True), ("modal", False)])
def test_network_lane_requires_a_public_source_and_a_known_location(lane, public_only):
    with pytest.raises(ValueError, match="Network lanes"):
        _gate(_Lanes(), lane=lane, public_only=public_only, cookies={"csrftoken": "x"})


def test_logged_out_rejections_never_synthesize_a_wait():
    lanes = _Lanes()
    for _ in range(5):
        failure, row = _fail_once(lanes, "unauthorized")
        assert failure.code == "public_access_backoff" and failure.retry_at is None
        assert row["server_retry_after_until"] is row["cooldown_until"] is None
    assert row["consecutive_failures"] == 5
    assert _gate(lanes).before_request().request_number == 1


def test_server_retry_after_wins_over_the_escalation_table():
    failure, _ = _fail_once(_Lanes(), "rate_limited", retry_after=120)
    assert failure.retry_at == datetime(2026, 10, 19, 21, 2, tzinfo=UTC)


def test_rate_limit_without_retry_after_has_no_wait():
    lanes = _Lanes()
    failure, _ = _fail_once(lanes, "rate_limited")
    assert failure.retry_at is None
    assert _gate(lanes).before_request().request_number == 1


def test_success_resets_diagnostics_without_a_backoff_ladder():
    lanes = _Lanes()
    for _ in range(3):
        _fail_once(lanes, "unauthorized")
    gate = _gate(lanes)
    lanes.row(gate.identity_key)["cooldown_until"] = None
    permit = gate.before_request()
    gate.finish_success(permit)
    row = lanes.rows[gate.identity_key]
    assert row["consecutive_failures"] == 0
    assert row["last_error_code"] is None
    failure, _ = _fail_once(lanes, "unauthorized")
    assert failure.retry_at is None


def test_legacy_shared_public_lane_has_no_local_wait():
    lanes = _Lanes()
    gate = _gate(lanes, lane=None)
    key = gate.identity_key
    permit = gate.before_request()
    failure = gate.record_failure("unauthorized", permit=permit)
    assert failure.retry_at is None
    lanes.rows[key].update(cooldown_until=None, consecutive_failures=3, last_error_code="public_access_backoff")
    gate = _gate(lanes, lane=None)
    gate.finish_success(gate.before_request())
    assert lanes.rows[key]["consecutive_failures"] == 0
    assert lanes.rows[key]["last_error_code"] is None


def test_lanes_do_not_share_cooldowns():
    lanes = _Lanes()
    _fail_once(lanes, "unauthorized", lane="modal", retry_after=120)
    local = _gate(lanes, lane="local")
    assert local.before_request().request_number == 1
    modal = _gate(lanes, lane="modal")
    with pytest.raises(identity_policy.IdentityDeferred, match="identity_cooldown"):
        modal.before_request()


@pytest.mark.parametrize("lane", [None, "local", "modal"])
def test_public_requests_above_180_remain_accounted_and_obey_server_cooldown(lane):
    lanes = _Lanes()
    gate = _gate(lanes, lane=lane)
    for _ in range(181):
        gate.before_request()
    assert gate.request_count == 181
    assert len(lanes.rows[gate.identity_key]["request_times"]) == 181
    failure = gate.finish_failure("rate_limited", retry_after_seconds=1200)
    assert failure.retry_at == lanes.now + timedelta(seconds=1200)
    deferred = _gate(lanes, lane=lane)
    with pytest.raises(identity_policy.IdentityDeferred, match="identity_cooldown"):
        deferred.before_request()
    assert deferred.request_count == 0
    assert len(lanes.rows[gate.identity_key]["request_times"]) == 181


def test_repeated_non_rate_failures_keep_accounting_without_a_wait():
    lanes = _Lanes()
    for _ in range(3):
        failure, row = _fail_once(lanes, "request_failed", lane="local")
    assert row["consecutive_failures"] == 3
    assert failure.retry_at is None
    listing = _gate(lanes, lane="local")
    assert listing.before_request().request_number == 1
    assert _gate(lanes, lane="modal").before_request().request_number == 1


@pytest.mark.parametrize("finish", [False, True])
def test_content_absence_is_neutral_for_a_network_lane(finish):
    lanes = _Lanes()
    for _ in range(5):
        gate = _gate(lanes, lane="local")
        permit = gate.before_request()
        if finish:
            failure = gate.finish_content_absence("request_failed", permit=permit)
        else:
            failure = gate.record_content_absence("request_failed", permit=permit)
            gate.finish_failure()
        assert failure.code == identity_policy.CONTENT_ABSENT_CODE and failure.retry_at is None
    row = lanes.rows[gate.identity_key]
    assert row["consecutive_failures"] == 0 and row["last_error_code"] is None
    assert row["cooldown_until"] is None and row["failure_generation"] == 0
    # Listing and comments on the same egress remain admitted.
    assert _gate(lanes, lane="local").before_request().request_number == 1


def test_content_absence_releases_probe_without_changing_health_or_server_marker():
    lanes = _Lanes()
    for _ in range(3):
        _fail_once(lanes, "unauthorized", lane="local")
    gate = _gate(lanes, lane="local")
    row = lanes.rows[gate.identity_key]
    expired = lanes.now - timedelta(seconds=1)
    row["cooldown_until"] = row["server_retry_after_until"] = expired
    permit = gate.before_request()
    assert permit.probe_token and row["probe_token"] == permit.probe_token
    gate.finish_content_absence("parse_error", permit=permit)
    assert row["probe_token"] is None and row["probe_expires_at"] is None
    assert row["consecutive_failures"] == 3 and row["last_error_code"] == "public_access_backoff"
    assert row["cooldown_until"] == expired
    # A later rejection without Retry-After introduces no wait.
    probe = _gate(lanes, lane="local")
    failure = probe.record_failure("unauthorized", permit=probe.before_request())
    assert failure.retry_at is None


def test_content_absence_keeps_original_accounting_on_lanes_without_network_scope():
    lanes = _Lanes()
    gate = _gate(lanes, lane=None)
    failure = gate.record_content_absence("request_failed", permit=gate.before_request())
    row = lanes.rows[gate.identity_key]
    assert failure.code == "request_failed"
    assert row["consecutive_failures"] == 1 and row["last_error_code"] == "request_failed"


class _StatusError(Exception):
    def __init__(self, status_code):
        self.status_code = status_code


@pytest.mark.parametrize(
    "error,payload,expected",
    [
        (_StatusError(404), None, True),
        (_StatusError(401), None, False),
        (_StatusError(500), None, False),
        (None, {"status": "ok", "data": {"user": None}}, True),
        (None, {"data": {"user": None}}, True),
        (None, {"data": {"user": None}, "rate_limited": True}, False),
        (None, {"data": {"user": None}, "login_required": True}, False),
        (None, {"data": {"user": None}, "errors": [{"message": "login_required"}]}, False),
        (None, {"data": {"user": None}, "errors": "malformed"}, False),
        (None, {"status": "fail", "data": {"user": None}}, False),
        (None, {"status": "ok", "data": {}}, False),
        (None, {"status": "ok", "data": {"user": {"username": "x"}}}, False),
        (None, None, False),
    ],
)
def test_content_absence_detects_only_404_and_explicit_null_users(error, payload, expected):
    assert identity_policy.is_content_absence(error=error, payload=payload) is expected


def test_404_with_server_delay_keeps_failure_handling():
    error = _StatusError(404)
    error.retry_after_seconds = 7200
    assert not identity_policy.is_content_absence(error=error)


@pytest.mark.parametrize("lane", ["local", "modal", None])
def test_server_delay_applies_to_the_first_non_rate_failure(lane):
    lanes = _Lanes()
    gate = _gate(lanes, lane=lane)
    failure = gate.record_failure("request_failed", retry_after_seconds=7200, permit=gate.before_request())
    assert failure.retry_at == lanes.now + timedelta(seconds=7200)
    with pytest.raises(identity_policy.IdentityDeferred):
        _gate(lanes, lane=lane).before_request()


@pytest.mark.parametrize("delay", [None, -1, float("nan"), float("inf"), True, "120"])
def test_invalid_server_delays_and_legacy_configuration_cannot_create_waits(delay, monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_RATE_PAUSE_MAX_SECONDS", "86400")
    lanes = _Lanes()
    gate = _gate(lanes)
    row = lanes.row(gate.identity_key)
    row["cooldown_until"] = lanes.now + timedelta(days=10)
    permit = gate.before_request()
    assert row["cooldown_until"] is None and permit.probe_token is None
    failure = gate.record_failure("rate_limited", retry_after_seconds=delay, permit=permit)
    assert failure.retry_at is None
    assert row["server_retry_after_until"] is row["cooldown_until"] is None
    assert _gate(lanes).before_request().request_number == 1


@pytest.mark.parametrize("lane", [None, "local", "modal"])
@pytest.mark.parametrize("later_delay", [None, 0, 10])
def test_actual_server_wait_is_monotonic_for_late_failures(lane, later_delay):
    lanes = _Lanes()
    first, late = _gate(lanes, lane=lane), _gate(lanes, lane=lane)
    first_permit, late_permit = first.before_request(), late.before_request()
    deadline = first.record_failure("rate_limited", retry_after_seconds=120, permit=first_permit).retry_at
    result = late.record_failure("request_failed", retry_after_seconds=later_delay, permit=late_permit)
    assert result.retry_at == deadline
    assert lanes.row(first.identity_key)["server_retry_after_until"] == deadline
    assert lanes.row(first.identity_key)["cooldown_until"] == deadline
