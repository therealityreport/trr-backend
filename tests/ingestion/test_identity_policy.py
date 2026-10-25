"""Private PostgreSQL policy tests; no credentials, browser or source network."""

import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from tests.ingestion.test_persistence import database as database_fixture
from tests.ingestion.test_persistence import launch, target
from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.identity_policy import IdentityDeferred, SourceRequestGate
from trr_backend.ingestion.persistence import Store

database = database_fixture


@pytest.fixture(scope="module")
def lanes(database):
    _, transaction = database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            (
                Path(__file__).parents[2] / "supabase/migrations/20260911121000_instagram_detail_request_lanes.sql"
            ).read_text()
        )
        cur.execute(
            (
                Path(__file__).parents[2] / "supabase/migrations/20261022120000_instagram_server_retry_after.sql"
            ).read_text()
        )
    return transaction


def gate(transaction, *, subject=None, session="synthetic-session", **kwargs):
    return SourceRequestGate(
        cookies={"ds_user_id": subject or uuid4().hex, "sessionid": session},
        transaction=transaction,
        check_cancelled=kwargs.pop("check_cancelled", lambda: None),
        deadline=kwargs.pop("deadline", time.monotonic() + 300),
        **kwargs,
    )


@pytest.fixture
def one_connection(lanes):
    with lanes() as conn:
        dsn = conn.dsn
    pool = ThreadedConnectionPool(1, 1, dsn=dsn)
    connection = pool.getconn()
    backend_pid = connection.get_backend_pid()
    pool.putconn(connection)

    @contextmanager
    def transaction():
        conn = pool.getconn()
        try:
            with conn:
                yield conn
        finally:
            pool.putconn(conn)

    try:
        yield Store(transaction), transaction, backend_pid
    finally:
        pool.closeall()


def owned_target(store):
    owned = target(store, launch(store))
    owned["owner_token"] = str(store.reserve(**owned)["owner_token"])
    store.acquire(**owned)
    return owned


def test_single_connection_pool_reuses_gate_transaction_for_owner_fence(one_connection):
    store, transaction, _ = one_connection
    owned = owned_target(store)
    # Reproduce the deployed failure without the explicit transaction callback.
    broken = gate(transaction, check_cancelled=lambda: store.check_owner(**owned))
    with pytest.raises(IdentityDeferred, match="identity_policy_unavailable"):
        broken.before_request()
    assert row(transaction, broken) == {}
    fixed = gate(
        transaction,
        check_cancelled=lambda: store.check_owner(**owned),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
    )
    fixed.before_request()
    assert len(row(transaction, fixed)["request_times"]) == 1
    store.check_owner(**owned, record_request=True)
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "select attempts,requests from social.instagram_ingestion_targets where run_id=%s", (owned["run_id"],)
        )
        assert cur.fetchone() == (1, 1)


def test_locked_owner_fence_is_checked_once_after_lane_wait(one_connection):
    store, transaction, _ = one_connection
    owned = owned_target(store)
    calls = []

    def before_wait():
        calls.append("before_wait")
        store.check_owner(**owned)

    def after_wait(connection):
        calls.append("after_wait")
        store.check_owner(**owned, connection=connection)

    lane = gate(transaction, check_cancelled=before_wait, check_cancelled_in_transaction=after_wait)
    permit = lane.before_request()
    assert calls == ["before_wait", "after_wait"]
    assert permit.request_number == lane.request_count == 1
    assert len(row(transaction, lane)["request_times"]) == 1
    # The caller's actual pre-send accounting still runs separately.
    store.check_owner(**owned, record_request=True)
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "select attempts,requests from social.instagram_ingestion_targets where run_id=%s", (owned["run_id"],)
        )
        assert cur.fetchone() == (1, 1)


@pytest.mark.parametrize("expire_at", [2, 3])
def test_locked_owner_fence_rechecks_deadline_and_rolls_back(one_connection, expire_at):
    store, transaction, _ = one_connection
    owned = owned_target(store)
    calls, ticks = [], []

    def clock():
        ticks.append(len(ticks) + 1)
        return 10 if len(ticks) >= expire_at else 0

    def before_wait():
        calls.append("before_wait")
        store.check_owner(**owned)

    def after_wait(connection):
        calls.append("after_wait")
        store.check_owner(**owned, connection=connection)

    lane = gate(
        transaction,
        check_cancelled=before_wait,
        check_cancelled_in_transaction=after_wait,
        deadline=10,
        monotonic=clock,
    )
    with pytest.raises(IdentityDeferred, match="target_deadline"):
        lane.before_request()
    assert calls == ["before_wait", "after_wait"]
    assert len(ticks) == expire_at
    assert lane.request_count == 0 and row(transaction, lane) == {}
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "select attempts,requests from social.instagram_ingestion_targets where run_id=%s", (owned["run_id"],)
        )
        assert cur.fetchone() == (1, 0)


def test_cancellation_while_identity_lock_waits_rolls_back_same_connection_fence(one_connection, lanes):
    store, transaction, backend_pid = one_connection
    owned = owned_target(store)
    fixed = gate(
        transaction,
        check_cancelled=lambda: store.check_owner(**owned),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
    )
    fixed.before_request()
    with ThreadPoolExecutor(max_workers=1) as executor:
        with lanes() as blocker, blocker.cursor() as cur:
            cur.execute(
                "select identity_key from social.instagram_detail_request_lanes where identity_key=%s for update",
                (fixed.identity_key,),
            )
            waiting = executor.submit(fixed.before_request)
            limit = time.monotonic() + 5
            while time.monotonic() < limit:
                with lanes() as observer, observer.cursor() as check:
                    check.execute("select wait_event_type from pg_stat_activity where pid=%s", (backend_pid,))
                    if check.fetchone() == ("Lock",):
                        break
                time.sleep(0.01)
            else:
                pytest.fail("identity gate did not wait on the held row lock")
            # Cancellation never locks the identity row, and must finish while
            # the gate is still waiting; no run->identity lock cycle is possible.
            Store(lanes).control(owned["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
        with pytest.raises(LostOwnership):
            waiting.result(timeout=5)
    assert len(row(transaction, fixed)["request_times"]) == 1
    assert fixed.request_count == 1
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "select attempts,requests,owner_token from social.instagram_ingestion_targets where run_id=%s",
            (owned["run_id"],),
        )
        assert cur.fetchone() == (1, 0, None)


def test_detail_transport_uses_one_connection_for_lane_and_owner(one_connection):
    import requests

    from trr_backend.socials.instagram.detail_transport import DetailTransport

    store, transaction, _ = one_connection
    owned = owned_target(store)
    with requests.Session() as session:

        class Scraper:
            def _request_cookies(self):
                return {"ds_user_id": uuid4().hex, "sessionid": "synthetic-session"}

        scraper = Scraper()
        scraper.session = session
        transport = DetailTransport(
            scraper=scraper,
            transaction=transaction,
            check_cancelled=lambda: store.check_owner(**owned),
            check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, connection=conn),
            on_request=lambda first: pytest.fail("source request was not required"),
            deadline=time.monotonic() + 30,
        )
        transport._reserve()
        assert len(row(transaction, transport)["request_times"]) == 1
        store.control(owned["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))
        with pytest.raises(LostOwnership):
            transport._reserve()
        assert len(row(transaction, transport)["request_times"]) == 1


def row(transaction, lane):
    with transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.instagram_detail_request_lanes where identity_key=%s", (lane.identity_key,))
        return dict(cur.fetchone() or {})


def change(transaction, lane, statement, *values):
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_detail_request_lanes set " + statement + " where identity_key=%s",
            (*values, lane.identity_key),
        )


def cooled_gate(transaction, *, subject=None):
    lane = gate(transaction, subject=subject)
    lane.before_request()
    change(
        transaction,
        lane,
        "server_retry_after_until=clock_timestamp()-interval '1 second',"
        "cooldown_until=clock_timestamp()-interval '1 second'",
    )
    return lane


def test_hashing_matches_maintained_identity_and_never_retains_cookie_values(lanes):
    lane = gate(lanes, subject="synthetic-user-id", session="synthetic-private-session")
    assert lane.identity_key == hashlib.sha256(b"synthetic-user-id").hexdigest()
    assert lane.credential_fingerprint == hashlib.sha256(b"synthetic-private-session").hexdigest()
    assert "synthetic-private-session" not in repr(lane)
    assert "synthetic-user-id" not in repr(vars(lane))
    fallback = SourceRequestGate(
        cookies={"sessionid": "synthetic-private-session"},
        transaction=lanes,
        check_cancelled=lambda: None,
        deadline=time.monotonic() + 30,
    )
    assert fallback.identity_key == lane.credential_fingerprint


@pytest.mark.parametrize("cancel_at", [1, 2, 3])
def test_cancel_before_reservation_and_inside_transaction_never_counts(lanes, cancel_at):
    count = 0

    def cancelled():
        nonlocal count
        count += 1
        if count >= cancel_at:
            raise LostOwnership("fixture cancellation")

    lane = gate(lanes, check_cancelled=cancelled)
    with pytest.raises(LostOwnership):
        lane.before_request()
    assert row(lanes, lane) == {}
    assert lane.request_count == 0


def test_shared_window_allows_exactly_180_and_prunes_expired_times(lanes):
    lane = gate(lanes)
    lane.before_request()
    now = datetime.now(UTC)
    change(lanes, lane, "request_times=%s", [now] * 179)
    lane.before_request()
    competing = gate(lanes, subject="unused")
    competing.identity_key = lane.identity_key
    with pytest.raises(IdentityDeferred) as deferred:
        competing.before_request()
    assert deferred.value.failure.code == "identity_budget"
    assert len(row(lanes, lane)["request_times"]) == 180
    change(lanes, lane, "request_times=%s", [now - timedelta(seconds=661)] * 180)
    lane.before_request()
    assert len(row(lanes, lane)["request_times"]) == 1


def test_simultaneous_reservations_share_one_atomic_rate_budget(lanes):
    subject = uuid4().hex
    first = gate(lanes, subject=subject)
    first.before_request()
    change(lanes, first, "request_times=%s", [datetime.now(UTC)] * 179)
    gates = [gate(lanes, subject=subject) for _ in range(8)]

    def reserve(lane):
        try:
            lane.before_request()
            return "admitted"
        except IdentityDeferred as error:
            return error.error_code

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = list(executor.map(reserve, gates))
    assert outcomes.count("admitted") == 1
    assert outcomes.count("identity_budget") == 7
    assert len(row(lanes, first)["request_times"]) == 180


def test_concurrent_ordinary_failures_count_without_local_wait(lanes):
    subject = uuid4().hex
    gates = [gate(lanes, subject=subject) for _ in range(3)]
    for lane in gates:
        lane.before_request()
    with ThreadPoolExecutor(max_workers=3) as executor:
        failures = list(executor.map(lambda lane: lane.finish_failure("network_error"), gates))
    state = row(lanes, gates[0])
    assert state["failure_generation"] == state["consecutive_failures"] == 3
    assert state["cooldown_until"] is state["server_retry_after_until"] is None
    assert all(failure.retry_at is None for failure in failures)
    gate(lanes, subject=subject).before_request()


def test_auth_block_is_fingerprint_bound_and_explicit_replacement_gets_one_probe(lanes):
    subject = uuid4().hex
    old = gate(lanes, subject=subject, session="old-session")
    old.before_request()
    failure = old.finish_failure("unauthorized")
    assert failure.identity_blocked
    unchanged = gate(lanes, subject=subject, session="old-session")
    with pytest.raises(IdentityDeferred) as deferred:
        unchanged.before_request()
    assert deferred.value.failure.identity_blocked
    replacement = gate(lanes, subject=subject, session="new-session")
    permit = replacement.before_request()
    assert permit.probe_token
    competitor = gate(lanes, subject=subject, session="new-session")
    with pytest.raises(IdentityDeferred, match="identity_probe_pending"):
        competitor.before_request()
    assert replacement.finish_success(permit)
    assert not row(lanes, replacement)["blocked"]
    assert row(lanes, replacement)["blocked_fingerprint"] is None


def test_failed_replacement_blocks_new_fingerprint_instead_of_retrying_it(lanes):
    subject = uuid4().hex
    old = gate(lanes, subject=subject, session="old-session")
    old.before_request()
    old.finish_failure("forbidden")
    replacement = gate(lanes, subject=subject, session="new-session")
    replacement.before_request()
    replacement.finish_failure("unauthorized")
    assert row(lanes, replacement)["blocked_fingerprint"] == replacement.credential_fingerprint
    with pytest.raises(IdentityDeferred) as blocked:
        gate(lanes, subject=subject, session="new-session").before_request()
    assert blocked.value.failure.identity_blocked


def test_owned_probe_allows_multiple_requests_until_explicit_target_finish(lanes):
    subject = uuid4().hex
    lane = cooled_gate(lanes, subject=subject)
    first = lane.before_request()
    assert first.probe_token
    assert lane.record_success(first)
    second = lane.before_request()
    assert second.probe_token == first.probe_token
    assert lane.record_success(second)
    with pytest.raises(IdentityDeferred, match="identity_probe_pending"):
        gate(lanes, subject=subject).before_request()
    assert lane.finish_success(second)
    assert row(lanes, lane)["probe_token"] is None
    assert row(lanes, lane)["cooldown_until"] is None
    gate(lanes, subject=subject).before_request()


def test_cooldown_rejects_before_count_and_rate_limit_retry_after_is_preserved(lanes):
    subject = uuid4().hex
    lane = gate(lanes, subject=subject)
    lane.before_request()
    start = datetime.now(UTC)
    failure = lane.finish_failure("rate_limited", retry_after_seconds=120)
    assert failure.retry_at >= start + timedelta(seconds=120)
    with pytest.raises(IdentityDeferred) as deferred:
        gate(lanes, subject=subject).before_request()
    assert deferred.value.failure.code == "identity_cooldown"
    assert len(row(lanes, lane)["request_times"]) == 1


def test_abandoned_probe_expires_and_old_result_cannot_clear_replacement(lanes):
    subject = uuid4().hex
    old = cooled_gate(lanes, subject=subject)
    old_permit = old.before_request()
    change(lanes, old, "probe_expires_at=clock_timestamp()-interval '1 second'")
    replacement = gate(lanes, subject=subject)
    new_permit = replacement.before_request()
    assert new_permit.probe_token != old_permit.probe_token
    assert old.finish_success(old_permit) is False
    assert row(lanes, old)["probe_token"] == new_permit.probe_token
    generation = row(lanes, old)["failure_generation"]
    assert old.record_failure("forbidden", permit=old_permit).code == "identity_gate_already_finished"
    assert row(lanes, old)["failure_generation"] == generation


def test_expired_probe_failure_has_no_authority_over_current_state(lanes):
    subject = uuid4().hex
    old = cooled_gate(lanes, subject=subject)
    old_permit = old.before_request()
    change(lanes, old, "probe_expires_at=clock_timestamp()-interval '1 second'")
    replacement = gate(lanes, subject=subject)
    new_permit = replacement.before_request()
    failure = old.finish_failure("unauthorized", permit=old_permit)
    state = row(lanes, old)
    assert failure.code == "identity_probe_lost"
    assert not state["blocked"] and state["failure_generation"] == 0
    assert state["probe_token"] == new_permit.probe_token


def test_late_success_does_not_clear_a_newer_failure_generation(lanes):
    subject = uuid4().hex
    early, failed = gate(lanes, subject=subject), gate(lanes, subject=subject)
    permit = early.before_request()
    failed.before_request()
    failed.finish_failure("unauthorized")
    assert early.finish_success(permit) is False
    state = row(lanes, early)
    assert state["blocked"] and state["last_error_code"] == "unauthorized"
    assert state["failure_generation"] == 1


def test_duplicate_failure_is_counted_once_and_cleanup_releases_only_owned_probe(lanes):
    lane = cooled_gate(lanes)
    permit = lane.before_request()
    first = lane.record_failure("rate_limited", permit=permit)
    second = lane.record_failure("rate_limited", permit=permit)
    assert first == second
    assert row(lanes, lane)["failure_generation"] == 1
    assert row(lanes, lane)["probe_token"] == permit.probe_token
    assert lane.finish_failure() == first
    assert row(lanes, lane)["probe_token"] is None
    assert row(lanes, lane)["cooldown_until"] == first.retry_at


def test_deadline_and_cancel_checks_precede_each_count(lanes):
    lane = gate(lanes, deadline=10, monotonic=lambda: 10)
    with pytest.raises(IdentityDeferred, match="target_deadline"):
        lane.before_request()
    assert row(lanes, lane) == {}


def test_sql_lock_is_released_before_source_operation_and_unknown_errors_are_safe(lanes):
    lane = gate(lanes)
    lane.before_request()
    with lanes() as conn, conn.cursor() as cur:
        cur.execute("set local lock_timeout='100ms'")
        cur.execute(
            "select identity_key from social.instagram_detail_request_lanes where identity_key=%s for update",
            (lane.identity_key,),
        )
    failure = lane.finish_failure("https://private.example/?sessionid=secret")
    assert failure.code == "request_failed"
    assert row(lanes, lane)["last_error_code"] == "request_failed"


def test_independent_auth_alias_preserves_legacy_shared_block_vocabulary(lanes):
    lane = gate(lanes)
    lane.before_request()
    failure = lane.finish_failure("source_auth_failed")
    assert failure.code == "unauthorized" and failure.identity_blocked
    assert row(lanes, lane)["blocked_fingerprint"] == lane.credential_fingerprint


def test_existing_detail_transport_and_gate_share_the_same_fence(lanes, monkeypatch):
    import requests

    from trr_backend.socials.instagram import detail_transport
    from trr_backend.socials.instagram.request_client import InstagramRequestFailure

    subject = uuid4().hex
    cookies = {"ds_user_id": subject, "sessionid": "synthetic-session"}

    class Scraper:
        session = requests.Session()

        def _request_cookies(self):
            return cookies

    monkeypatch.setattr(detail_transport.pg, "db_connection", lambda **kwargs: lanes())
    legacy = detail_transport.DetailTransport(
        scraper=Scraper(), check_cancelled=lambda: None, on_request=lambda _: None, deadline=time.monotonic() + 300
    )
    try:
        legacy._reserve()
        shared = gate(lanes, subject=subject)
        permit = shared.before_request()
        assert legacy.identity_key == shared.identity_key
        assert len(row(lanes, shared)["request_times"]) == 2
        legacy._outcome(InstagramRequestFailure("unauthorized", status_code=401, retryable=False))
        assert shared.finish_success(permit) is False
        with pytest.raises(IdentityDeferred) as error:
            gate(lanes, subject=subject).before_request()
        assert error.value.failure.identity_blocked
    finally:
        legacy.scraper.session.close()


def test_huge_retry_after_fails_closed_without_raw_time_overflow(lanes):
    lane = gate(lanes)
    lane.before_request()
    failure = lane.finish_failure("rate_limited", retry_after_seconds=1e300)
    assert failure.retry_at == datetime.max.replace(tzinfo=UTC)


@pytest.fixture
def public_lanes(lanes):
    identity = hashlib.sha256(b"public").hexdigest()

    def clear():
        with lanes() as conn, conn.cursor() as cur:
            cur.execute("delete from social.instagram_detail_request_lanes where identity_key=%s", (identity,))

    clear()
    try:
        yield lanes
    finally:
        clear()


def public_gate(transaction, **kwargs):
    return SourceRequestGate(
        cookies=kwargs.pop("cookies", {}),
        public_only=True,
        transaction=transaction,
        check_cancelled=lambda: None,
        deadline=time.monotonic() + 300,
        **kwargs,
    )


@pytest.mark.parametrize("name", ["sessionid", "ds_user_id", "SESSIONID", " Ds_User_Id "])
@pytest.mark.parametrize("value", ["", "synthetic-auth-value"])
def test_public_mode_rejects_authentication_cookie_names_before_database_access(name, value):
    with pytest.raises(ValueError, match="authentication cookies"):
        public_gate(lambda: pytest.fail("public credential rejection accessed database"), cookies={name: value})


def test_public_mode_keeps_shared_accounting_without_a_public_count_cap(public_lanes):
    first = public_gate(public_lanes)
    second = public_gate(public_lanes, cookies={"csrftoken": "synthetic-public-csrf"})
    compatible = SourceRequestGate(
        cookies={}, transaction=public_lanes, check_cancelled=lambda: None, deadline=time.monotonic() + 300
    )
    expected = hashlib.sha256(b"public").hexdigest()
    assert first.identity_key == second.identity_key == compatible.identity_key == expected
    assert first.credential_fingerprint == second.credential_fingerprint == expected
    assert "synthetic-public-csrf" not in repr(vars(second))
    first.before_request()
    compatible.before_request()
    assert len(row(public_lanes, first)["request_times"]) == 2
    now = datetime.now(UTC)
    change(public_lanes, first, "request_times=%s", [now] * 179)
    second.before_request()
    first.before_request()
    assert len(row(public_lanes, first)["request_times"]) == 181
    # A caller without explicit public-only mode keeps authenticated admission
    # rules even when its no-cookie fallback hashes to the same identity.
    with pytest.raises(IdentityDeferred, match="identity_budget"):
        compatible.before_request()
    assert len(row(public_lanes, first)["request_times"]) == 181
    change(public_lanes, first, "request_times=%s", [now - timedelta(seconds=661)] * 180)
    first.before_request()
    assert len(row(public_lanes, first)["request_times"]) == 1


@pytest.mark.parametrize(
    "code",
    [
        "unauthorized",
        "forbidden",
        "redirect_login",
        "login_required",
        "checkpoint_required",
        "challenge_required",
        "feedback_required",
        "consent_required",
        "sentry_block",
        "source_auth_failed",
        "public_access_backoff",
    ],
)
def test_public_login_wall_has_no_local_wait_or_permanent_auth_block(public_lanes, code):
    lane = public_gate(public_lanes)
    lane.before_request()
    failure = lane.finish_failure(code)
    assert failure.code == "public_access_backoff" and not failure.identity_blocked
    assert failure.retry_at is None
    state = row(public_lanes, lane)
    assert not state["blocked"] and state["blocked_fingerprint"] is None
    assert state["last_error_code"] == "public_access_backoff" and state["failure_generation"] == 1
    public_gate(public_lanes).before_request()
    assert len(row(public_lanes, lane)["request_times"]) == 2


@pytest.mark.parametrize("retry_after", [120, float("nan"), -1, 1e300])
def test_public_backoff_preserves_retry_after_safety_and_overflow_fail_closed(public_lanes, retry_after):
    lane = public_gate(public_lanes)
    lane.before_request()
    started = datetime.now(UTC)
    failure = lane.finish_failure("public_access_backoff", retry_after_seconds=retry_after)
    if retry_after == 1e300:
        assert failure.retry_at == datetime.max.replace(tzinfo=UTC)
    elif retry_after == 120:
        assert started + timedelta(seconds=120) <= failure.retry_at <= datetime.now(UTC) + timedelta(seconds=120)
    else:
        assert failure.retry_at is None
    assert not failure.identity_blocked and not row(public_lanes, lane)["blocked"]


@pytest.mark.parametrize(
    "fingerprint", [None, "synthetic-historical-fingerprint", hashlib.sha256(b"public").hexdigest()]
)
def test_public_mode_never_repairs_an_existing_block_even_when_fingerprint_differs(public_lanes, fingerprint):
    initial = public_gate(public_lanes)
    initial.before_request()
    change(
        public_lanes,
        initial,
        "blocked=true,blocked_fingerprint=%s,last_error_code='unauthorized',failure_generation=9",
        fingerprint,
    )
    before = row(public_lanes, initial)
    current = public_gate(public_lanes)
    with pytest.raises(IdentityDeferred) as error:
        current.before_request()
    assert error.value.failure.identity_blocked and current.request_count == 0
    assert row(public_lanes, current) == before


def test_public_late_success_cannot_clear_a_newer_failure_generation(public_lanes):
    early, failed = public_gate(public_lanes), public_gate(public_lanes)
    permit = early.before_request()
    failed.before_request()
    failure = failed.finish_failure("unauthorized")
    state = row(public_lanes, failed)
    assert not early.finish_success(permit)
    assert row(public_lanes, failed) == state
    assert state["cooldown_until"] == failure.retry_at and state["failure_generation"] == 1


def test_public_late_shorter_retry_after_cannot_advance_existing_server_delay(public_lanes):
    first, late = public_gate(public_lanes), public_gate(public_lanes)
    first.before_request()
    late.before_request()
    original = first.finish_failure("public_access_backoff", retry_after_seconds=600)
    result = late.finish_failure("unauthorized", retry_after_seconds=120)
    assert result.retry_at == original.retry_at and not result.identity_blocked
    state = row(public_lanes, first)
    assert state["cooldown_until"] == original.retry_at and state["failure_generation"] == 2
    assert not state["blocked"] and len(state["request_times"]) == 2


def test_public_expired_probe_cannot_change_replacement_and_duplicate_failure_counts_once(public_lanes):
    initial = public_gate(public_lanes)
    initial.before_request()
    initial.finish_failure("public_access_backoff")
    change(
        public_lanes,
        initial,
        "server_retry_after_until=clock_timestamp()-interval '1 second',"
        "cooldown_until=clock_timestamp()-interval '1 second'",
    )
    old = public_gate(public_lanes)
    old_permit = old.before_request()
    assert old_permit.probe_token
    change(public_lanes, old, "probe_expires_at=clock_timestamp()-interval '1 second'")
    current = public_gate(public_lanes)
    current_permit = current.before_request()
    assert current_permit.probe_token != old_permit.probe_token
    assert old.finish_failure("unauthorized", permit=old_permit).code == "identity_probe_lost"
    assert row(public_lanes, current)["probe_token"] == current_permit.probe_token
    failure = current.record_failure("login_required", permit=current_permit)
    assert current.record_failure("public_access_backoff", permit=current_permit) == failure
    assert row(public_lanes, current)["failure_generation"] == 2
    assert not row(public_lanes, current)["blocked"]
    current.finish_failure()
    assert row(public_lanes, current)["probe_token"] is None
    assert row(public_lanes, current)["cooldown_until"] == failure.retry_at


def test_obsolete_local_pause_settings_cannot_reenable_waits(public_lanes, monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_RATE_PAUSE_MAX_SECONDS", "86400")
    lane = public_gate(public_lanes, cooldown_seconds=86400)
    for _ in range(5):
        permit = lane.before_request()
        assert lane.record_failure("rate_limited", permit=permit).retry_at is None
    state = row(public_lanes, lane)
    assert state["consecutive_failures"] == 5
    assert state["cooldown_until"] is state["server_retry_after_until"] is None


def test_public_rate_pause_server_delay_wins_and_success_resets(public_lanes, monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_RATE_PAUSE_MAX_SECONDS", "60")
    lane = public_gate(public_lanes)
    lane.before_request()
    started = datetime.now(UTC)
    failure = lane.finish_failure("rate_limited", retry_after_seconds=1234)
    assert started + timedelta(seconds=1234) <= failure.retry_at <= datetime.now(UTC) + timedelta(seconds=1234)
    change(
        public_lanes,
        lane,
        "server_retry_after_until=clock_timestamp()-interval '1 second',"
        "cooldown_until=clock_timestamp()-interval '1 second',consecutive_failures=4",
    )
    healthy = public_gate(public_lanes)
    healthy.before_request()
    healthy.finish_success()
    assert row(public_lanes, healthy)["consecutive_failures"] == 0


def test_received_peer_extends_finished_hold_without_health_or_probe_changes(lanes):
    lane = gate(lanes)
    first, second = lane.before_request(), lane.before_request()
    initial = lane.finish_failure("rate_limited", retry_after_seconds=20, permit=first)
    baseline = row(lanes, lane)
    stronger = lane.finish_failure("rate_limited", retry_after_seconds=60, permit=second)
    held = row(lanes, lane)
    assert stronger.retry_at > initial.retry_at + timedelta(seconds=39)
    assert held["server_retry_after_until"] == stronger.retry_at
    assert held["failure_generation"] == baseline["failure_generation"]
    assert held["consecutive_failures"] == baseline["consecutive_failures"]
    assert held["probe_token"] == baseline["probe_token"]
    assert lane.record_success(second) is False
    assert row(lanes, lane)["server_retry_after_until"] == stronger.retry_at
    weaker = lane.finish_failure("rate_limited", retry_after_seconds=1, permit=first)
    assert weaker.retry_at == stronger.retry_at


def test_out_of_order_old_permit_success_cannot_clear_newer_failure(lanes):
    lane = gate(lanes)
    first, second = lane.before_request(), lane.before_request()
    lane.record_failure("rate_limited", retry_after_seconds=60, permit=second)
    baseline = row(lanes, lane)
    assert lane.record_success(first) is False
    after = row(lanes, lane)
    assert after["failure_generation"] == baseline["failure_generation"]
    assert after["server_retry_after_until"] == baseline["server_retry_after_until"]


def test_finished_peer_server_holds_do_not_reduce_longer_policy_or_returned_hold(lanes):
    from dataclasses import replace

    lane = gate(lanes)
    first, second = lane.before_request(), lane.before_request()
    lane.finish_failure("rate_limited", retry_after_seconds=20, permit=first)
    policy_hold = datetime.now(UTC) + timedelta(seconds=180)
    change(lanes, lane, "cooldown_until=%s", policy_hold)
    lane._last_failure = replace(lane._last_failure, retry_at=policy_hold)
    baseline = row(lanes, lane)
    for seconds in (20, 60, 1):
        result = lane.finish_failure("rate_limited", retry_after_seconds=seconds, permit=second)
        current = row(lanes, lane)
        assert result.retry_at == policy_hold
        assert current["cooldown_until"] == policy_hold
        assert current["server_retry_after_until"] < policy_hold
        assert current["failure_generation"] == baseline["failure_generation"]
        assert current["consecutive_failures"] == baseline["consecutive_failures"]
    assert row(lanes, lane)["server_retry_after_until"] >= datetime.now(UTC) + timedelta(seconds=59)
    # A changed shared row must not shorten the already returned target hold.
    change(lanes, lane, "cooldown_until=clock_timestamp()+interval '2 seconds'")
    assert lane.finish_failure("rate_limited", retry_after_seconds=1, permit=second).retry_at == policy_hold
    assert row(lanes, lane)["cooldown_until"] == policy_hold
