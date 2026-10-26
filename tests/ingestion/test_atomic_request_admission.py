"""Final lane/owner/accounting admission on isolated PostgreSQL only."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import psycopg2
import pytest

from tests.ingestion import test_identity_policy as policy
from tests.ingestion.test_identity_policy import gate, owned_target, row
from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.identity_policy import IdentityDeferred
from trr_backend.ingestion.persistence import Store

database = policy.database
lanes = policy.lanes


def request_count(transaction, owned):
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("select requests from social.instagram_ingestion_targets where run_id=%s", (owned["run_id"],))
        return cur.fetchone()[0]


def test_counted_permit_uses_one_transaction_and_one_owner_check(lanes):
    store = Store(lanes)
    owned = owned_target(store)
    transactions, callbacks = [], []

    @contextmanager
    def traced():
        transactions.append("transaction")
        with lanes() as conn:
            yield conn

    def account(conn):
        callbacks.append(conn.get_backend_pid())
        store.check_owner(**owned, record_request=True, connection=conn)

    lane = gate(traced, check_cancelled=lambda: pytest.fail("redundant pre-admission check"))
    first = lane.before_counted_request(account)
    second = lane.before_counted_request(account)
    assert first.request_number == 1 and second.request_number == 2
    assert len(transactions) == len(callbacks) == lane.request_count == 2
    assert request_count(lanes, owned) == len(row(lanes, lane)["request_times"]) == 2


def test_rejected_cooldown_has_no_permit_or_accounting(lanes):
    store = Store(lanes)
    owned = owned_target(store)
    lane = gate(lanes)
    lane.before_counted_request(lambda conn: store.check_owner(**owned, record_request=True, connection=conn))
    with lanes() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_detail_request_lanes set server_retry_after_until=%s where identity_key=%s",
            (datetime.now(UTC) + timedelta(seconds=60), lane.identity_key),
        )
    with pytest.raises(IdentityDeferred, match="identity_cooldown"):
        lane.before_counted_request(lambda conn: pytest.fail("rejected admission counted"))
    assert lane.request_count == request_count(lanes, owned) == 1
    assert len(row(lanes, lane)["request_times"]) == 1


@pytest.mark.parametrize("failure", [LostOwnership, RuntimeError])
def test_atomic_admission_rolls_back_accounting_and_lane(failure, lanes):
    store = Store(lanes)
    owned = owned_target(store)
    lane = gate(lanes)

    def rejected(conn):
        store.check_owner(**owned, record_request=True, connection=conn)
        raise failure("fence rejected")

    with pytest.raises(failure, match="fence rejected"):
        lane.before_counted_request(rejected)
    assert lane.request_count == request_count(lanes, owned) == 0
    assert row(lanes, lane) == {}
    assert not lane._lane_initialized


def test_cooldown_recorded_during_queued_admission_is_not_counted(lanes):
    store = Store(lanes)
    owned = owned_target(store)
    lane = gate(lanes)

    def account(conn):
        store.check_owner(**owned, record_request=True, connection=conn)

    lane.before_counted_request(account)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with lanes() as conn, conn.cursor() as cur:
            cur.execute(
                "select identity_key from social.instagram_detail_request_lanes where identity_key=%s for update",
                (lane.identity_key,),
            )
            waiting = executor.submit(lane.before_counted_request, account)
            with pytest.raises(TimeoutError):
                waiting.result(timeout=0.05)
            cur.execute(
                "update social.instagram_detail_request_lanes set server_retry_after_until=%s where identity_key=%s",
                (datetime.now(UTC) + timedelta(seconds=60), lane.identity_key),
            )
        with pytest.raises(IdentityDeferred, match="identity_cooldown"):
            waiting.result(timeout=5)
    assert lane.request_count == request_count(lanes, owned) == 1
    assert len(row(lanes, lane)["request_times"]) == 1


@pytest.mark.parametrize("stop_kind", ["cancel", "stop", "deadline", "owner_loss"])
def test_scheduler_returns_only_exact_unsent_permit(lanes, stop_kind):
    import asyncio
    import threading
    import time
    from dataclasses import replace

    from trr_backend.socials.instagram.comments_scrapling.reply_scheduler import ReplyRequest, run_reply_lanes

    store = Store(lanes)
    owned = owned_target(store)
    lane = gate(lanes)
    admitted, stop = threading.Event(), threading.Event()

    def account(conn):
        store.check_owner(**owned, record_request=True, connection=conn)

    account.release_in_transaction = lambda conn, permit: store.release_unsent_request(
        **owned, permit=permit, connection=conn
    )

    def branch():
        try:
            yield ReplyRequest("p", "doc", "popular", 12)
        except Exception:
            return

    async def scenario():
        sends, discarded = [], []

        def before(request):
            permit = lane.before_counted_request(account)
            admitted.set()
            if stop_kind == "owner_loss":
                store.control(
                    owned["run_id"], owned["account"], action="cancel", idempotency_key=str(__import__("uuid").uuid4())
                )
            time.sleep(0.03)
            if stop_kind in {"stop", "owner_loss"}:
                stop.set()
            return replace(
                request, request_permit=permit, deadline=time.monotonic() - 1 if stop_kind == "deadline" else None
            )

        def discard(request):
            discarded.append(request.request_permit)
            assert lane.release_unsent(request.request_permit)
            assert not lane.release_unsent(request.request_permit)  # idempotent

        async def request(value):
            lane.mark_sent(value.request_permit)
            sends.append(value)

        task = asyncio.create_task(
            run_reply_lanes(
                {"p": branch()},
                request=request,
                pace=lambda _: asyncio.sleep(0),
                before_dispatch=before,
                discard=discard,
                offload_owner=True,
                stop=stop.is_set,
            )
        )
        if stop_kind == "cancel":
            while not admitted.is_set():
                await asyncio.sleep(0.001)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task
        assert not sends and len(discarded) == 1

    asyncio.run(scenario())
    assert request_count(lanes, owned) == lane.request_count == 0
    assert row(lanes, lane)["request_times"] == []


def test_uncertain_refund_commit_replays_receipt_without_touching_sibling(lanes):
    store = Store(lanes)
    owned = owned_target(store)
    fail = [False]

    @contextmanager
    def uncertain_transaction():
        with lanes() as conn:
            yield conn
        if fail[0]:
            fail[0] = False
            raise psycopg2.OperationalError("lost commit acknowledgment")

    lane = gate(uncertain_transaction)

    def account(conn):
        store.check_owner(**owned, record_request=True, connection=conn)

    account.release_in_transaction = lambda conn, permit: store.release_unsent_request(
        **owned, permit=permit, connection=conn
    )
    sent = lane.before_counted_request(account)
    lane.mark_sent(sent)
    unused = lane.before_counted_request(account)
    fail[0] = True
    with pytest.raises(IdentityDeferred):
        lane.release_unsent(unused)
    assert request_count(lanes, owned) == 1  # the first transaction committed
    assert lane.release_unsent(unused)
    assert lane.request_count == request_count(lanes, owned) == 1
    assert row(lanes, lane)["request_times"] == [sent.admitted_at]
    assert not lane.release_unsent(sent)


@pytest.mark.parametrize("rejection", [None, "generation", "owner", "placement", "cancelled", "deadline"])
def test_combined_production_admission_fences_and_counts_once(lanes, rejection):
    from uuid import uuid4

    from tests.ingestion.test_persistence import parallel_owned_targets

    store = Store(lanes)
    run, targets = parallel_owned_targets(store)
    owned = dict(targets[0])
    if rejection == "generation":
        owned["generation"] += 1
    elif rejection == "owner":
        owned["owner_token"] = str(uuid4())
    elif rejection == "placement":
        store = Store(lanes, execution_placement="local")
    elif rejection == "cancelled":
        store.control(run["run_id"], "bravotv", action="cancel", idempotency_key=str(uuid4()))

    def account(_conn):
        pytest.fail("Redundant accounting callback")

    account.finalize_in_transaction = lambda conn, sql, params: store.finalize_request_admission(
        **owned, connection=conn, lane_update=sql, lane_params=params
    )
    lane = gate(lanes, deadline=10, monotonic=iter([0, 0, 10]).__next__) if rejection == "deadline" else gate(lanes)
    if rejection:
        with pytest.raises(IdentityDeferred if rejection == "deadline" else LostOwnership):
            lane.before_counted_request(account)
        assert lane.request_count == 0 and row(lanes, lane) == {}
    else:
        assert lane.before_counted_request(account).request_number == 1
        assert lane.request_count == len(row(lanes, lane)["request_times"]) == 1
    with lanes() as conn, conn.cursor() as cur:
        cur.execute("select sum(requests) from social.instagram_ingestion_targets where run_id=%s", (run["run_id"],))
        assert cur.fetchone()[0] == (0 if rejection else 1)
