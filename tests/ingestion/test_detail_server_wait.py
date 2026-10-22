"""Maintained detail lane contracts in isolated PostgreSQL, without HTTP."""

import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import requests

from tests.ingestion.test_identity_policy import change, row
from tests.ingestion.test_identity_policy import database as database  # noqa: F401
from tests.ingestion.test_identity_policy import lanes as lanes_fixture
from trr_backend.socials.instagram.detail_transport import DetailDeferred, DetailTransport
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

lanes = lanes_fixture


@pytest.fixture
def transport(lanes):
    sessions = []
    subject = uuid4().hex

    def make(*, session_id="synthetic-session", **kwargs):
        session = requests.Session()
        sessions.append(session)
        scraper = SimpleNamespace(
            session=session,
            _request_cookies=lambda: {"ds_user_id": subject, "sessionid": session_id},
        )
        return DetailTransport(
            scraper=scraper,
            transaction=lanes,
            check_cancelled=lambda: None,
            on_request=lambda _: pytest.fail("HTTP is forbidden in lane tests"),
            deadline=time.monotonic() + 30,
            **kwargs,
        )

    yield make
    for session in sessions:
        session.close()


def failure(code="rate_limited", delay=None):
    return InstagramRequestFailure(code, status_code=429, retryable=True, retry_after_seconds=delay)


@pytest.mark.parametrize("code", ["rate_limited", "network_error", "parse_error", "partial_required_fields"])
def test_repeated_detail_failures_never_create_local_waits(lanes, transport, monkeypatch, code):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_DETAILS_COOLDOWN_SECONDS", "86400")
    for _ in range(5):
        current = transport(cooldown_seconds=86400)
        current._reserve()
        error = failure(code)
        current._outcome(error)
        assert error.next_attempt_at is None
    state = row(lanes, current)
    assert state["failure_generation"] == state["consecutive_failures"] == 5
    assert state["server_retry_after_until"] is state["cooldown_until"] is None
    assert len(state["request_times"]) == 5
    transport()._reserve()


def test_detail_ignores_unproven_legacy_cooldown(lanes, transport):
    current = transport()
    current._reserve()
    change(lanes, current, "cooldown_until=clock_timestamp()+interval '1 day'")
    other = transport()
    other._reserve()
    assert other.probe_token is None
    assert row(lanes, other)["cooldown_until"] is None
    assert len(row(lanes, other)["request_times"]) == 2


@pytest.mark.parametrize("delay", [None, -1, float("nan"), float("inf"), True, "120"])
def test_invalid_detail_retry_after_does_not_create_wait(lanes, transport, delay):
    current = transport()
    current._reserve()
    current._outcome(failure(delay=delay))
    assert row(lanes, current)["server_retry_after_until"] is None
    transport()._reserve()


@pytest.mark.parametrize("later_delay", [None, 0, 10])
def test_detail_actual_wait_is_monotonic_and_blocks_before_accounting(lanes, transport, later_delay):
    first, late = transport(), transport()
    first._reserve()
    late._reserve()
    first._outcome(failure(delay=120))
    deadline = row(lanes, first)["server_retry_after_until"]
    late_error = failure("network_error", later_delay)
    late._outcome(late_error)
    assert late_error.next_attempt_at == deadline
    assert row(lanes, first)["cooldown_until"] == deadline
    with pytest.raises(DetailDeferred, match="identity_cooldown") as deferred:
        transport()._reserve()
    assert deferred.value.next_attempt_at == deadline
    assert len(row(lanes, first)["request_times"]) == 2


def test_detail_auth_block_has_no_timer_and_replacement_does_not_bypass_server_wait(lanes, transport):
    old = transport()
    old._reserve()
    old._outcome(failure("unauthorized", 120))
    with pytest.raises(DetailDeferred, match="identity_cooldown"):
        transport(session_id="replacement")._reserve()
    change(lanes, old, "server_retry_after_until=null,cooldown_until=null")
    with pytest.raises(DetailDeferred, match="unauthorized") as blocked:
        transport()._reserve()
    assert blocked.value.next_attempt_at is None
    replacement = transport(session_id="replacement")
    replacement._reserve()
    assert replacement.probe_token
    with pytest.raises(DetailDeferred, match="identity_probe_pending"):
        transport(session_id="replacement")._reserve()
    replacement._outcome(None)
    assert not row(lanes, replacement)["blocked"]
    assert row(lanes, replacement)["blocked_fingerprint"] is None


def test_detail_expired_probe_cannot_change_new_lease_or_server_wait(lanes, transport):
    old = transport()
    old._reserve()
    change(lanes, old, "server_retry_after_until=clock_timestamp()-interval '1 second'")
    old._reserve()
    old_token = old.probe_token
    change(lanes, old, "probe_expires_at=clock_timestamp()-interval '1 second'")
    before = row(lanes, old)
    old._outcome(failure("unauthorized", 120))
    assert row(lanes, old) == before
    replacement = transport()
    replacement._reserve()
    assert replacement.probe_token and replacement.probe_token != old_token
    old._outcome(None)
    assert row(lanes, old)["probe_token"] == replacement.probe_token
    replacement._outcome(None)
    assert row(lanes, replacement)["server_retry_after_until"] is None


def test_late_detail_success_cannot_clear_newer_server_wait(lanes, transport):
    early, failed = transport(), transport()
    early._reserve()
    failed._reserve()
    failed._outcome(failure(delay=120))
    before = row(lanes, failed)
    early._outcome(None)
    assert row(lanes, early) == before


def test_zero_retry_after_is_actual_but_does_not_force_future_wait(lanes, transport):
    current = transport()
    current._reserve()
    current._outcome(failure(delay=0))
    state = row(lanes, current)
    assert state["server_retry_after_until"] == state["cooldown_until"]
    assert state["server_retry_after_until"] <= datetime.now(UTC)
    assert state["server_retry_after_until"] > datetime.now(UTC) - timedelta(seconds=10)
    probe = transport()
    probe._reserve()
    assert probe.probe_token
    probe._outcome(None)
    assert row(lanes, probe)["server_retry_after_until"] is None


def test_migration_is_additive_and_does_not_backfill_legacy_deadlines(lanes):
    from pathlib import Path

    migration = (
        Path(__file__).parents[2] / "supabase/migrations/20261022120000_instagram_server_retry_after.sql"
    ).read_text()
    with lanes() as conn, conn.cursor() as cur:
        cur.execute("""create table social.ig_comment_rate_pace
            (identity_key text primary key,cooldown_until timestamptz);
            insert into social.ig_comment_rate_pace values ('synthetic',clock_timestamp()+interval '1 day')""")
        cur.execute(migration)
        cur.execute(migration)
        cur.execute("select cooldown_until,server_retry_after_until from social.ig_comment_rate_pace")
        legacy, actual = cur.fetchone()
        assert legacy > datetime.now(UTC) and actual is None


def test_source_auth_replacement_waits_for_actual_server_deadline(lanes):
    from tests.ingestion.test_identity_policy import gate
    from trr_backend.ingestion.identity_policy import IdentityDeferred

    subject = uuid4().hex
    old = gate(lanes, subject=subject, session="old")
    old.before_request()
    original = old.finish_failure("unauthorized", retry_after_seconds=120)
    with pytest.raises(IdentityDeferred, match="identity_cooldown") as blocked:
        gate(lanes, subject=subject, session="new").before_request()
    assert blocked.value.next_attempt_at == original.retry_at
    assert row(lanes, old)["blocked"]
    assert len(row(lanes, old)["request_times"]) == 1


@pytest.mark.parametrize("server_header", [None, "120"])
def test_detail_controlled_http_errors_wait_only_with_real_retry_after(lanes, transport, server_header):
    sent = []

    def make():
        current = transport()
        current.scraper._get_headers = lambda _: {}
        current.on_request = lambda first: None

        def send(*args, **kwargs):
            sent.append(args[0])
            return SimpleNamespace(
                status_code=429,
                headers={"Retry-After": server_header} if server_header else {},
                text="{}",
                is_redirect=False,
                json=lambda: {},
            )

        current.scraper.session.get = send
        return current

    for _ in range(1 if server_header else 5):
        current = make()
        with pytest.raises(InstagramRequestFailure, match="rate_limited"):
            current.request("https://www.instagram.com/api/v1/media/123/info/")
    if server_header:
        with pytest.raises(DetailDeferred, match="identity_cooldown"):
            make().request("https://www.instagram.com/api/v1/media/123/info/")
        assert len(sent) == 1
    else:
        assert len(sent) == 5
        assert row(lanes, current)["server_retry_after_until"] is None
    assert len(row(lanes, current)["request_times"]) == len(sent)


def test_detail_rate_capacity_remains_even_without_local_cooldowns(lanes, transport):
    current = transport()
    current._reserve()
    change(lanes, current, "request_times=%s", [datetime.now(UTC)] * 179)
    transport()._reserve()
    with pytest.raises(DetailDeferred, match="identity_budget"):
        transport()._reserve()
    assert len(row(lanes, current)["request_times"]) == 180
    assert row(lanes, current)["server_retry_after_until"] is None
