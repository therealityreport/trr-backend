"""Received reply evidence survives source stops, but never an unsafe owner."""

import asyncio
import time
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from tests.ingestion import test_comments as existing
from tests.ingestion import test_parallel_reply_transport as parallel
from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.comments import CommentActivity
from trr_backend.ingestion.composition import _GatedSource
from trr_backend.ingestion.domain import LostOwnership, Stage
from trr_backend.ingestion.identity_policy import SourceRequestGate
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.comments_scrapling import reply_scheduler

database = existing.database
request_lanes = public.request_lanes
fresh_public_lanes = public.fresh_public_lanes
deny_unmocked_http = public.deny_unmocked_http
assert_transport_resources_are_drained = parallel.assert_transport_resources_are_drained


def complete_both_before_owner(monkeypatch, order):
    """Barrier controls task result order; HTTP and owner logic remain real."""
    original = reply_scheduler.run_reply_lanes
    tasks = {}

    async def synchronized(lanes, *, request, **kwargs):
        release = {parent: asyncio.Event() for parent in order}
        received = set()

        async def transport(descriptor):
            response = await request(descriptor)
            assert descriptor.parent_id in release, "a new request escaped the closed dispatch boundary"
            assert descriptor.parent_id not in received, "a continuation was dispatched after both responses arrived"
            tasks[descriptor.parent_id] = asyncio.current_task()
            received.add(descriptor.parent_id)
            if len(received) == len(order):
                release[order[0]].set()
            await release[descriptor.parent_id].wait()
            index = order.index(descriptor.parent_id)
            if index + 1 < len(order):
                release[order[index + 1]].set()
            return response

        return await original(lanes, request=transport, **kwargs)

    monkeypatch.setattr(reply_scheduler, "run_reply_lanes", synchronized)

    def assert_received():
        assert len(tasks) == 2 and all(task.done() for task in tasks.values()), (
            "fixture requires two completed response tasks before the first owner transition"
        )

    return assert_received


def guarded_source(store, tx, args, fetch, assert_received):
    owned = {key: value for key, value in args.items() if key != "post_id"}
    gate = SourceRequestGate(
        cookies={},
        public_only=True,
        network_lane="local",
        transaction=tx,
        check_cancelled=lambda: store.check_owner(**owned, stage=Stage.COMMENTS),
        deadline=time.monotonic() + 60,
    )

    def checked_source(**callbacks):
        observe = callbacks["source_observer"]
        reject = callbacks["source_failure_observer"]

        def received_observer(kind, payload):
            if kind == "graphql_replies":
                assert_received()
            return observe(kind, payload)

        def received_failure(code, delay):
            assert_received()
            return reject(code, delay)

        return fetch(**{**callbacks, "source_observer": received_observer, "source_failure_observer": received_failure})

    return _GatedSource(checked_source, gate, failure_code="comment_fetch_failed"), gate


@pytest.mark.parametrize("order", [("blocked", "success"), ("success", "blocked")])
def test_source_failure_drains_received_success_and_closes_dispatch(fresh_public_lanes, database, monkeypatch, order):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    assert_received = complete_both_before_owner(monkeypatch, order)
    store, tx, args = public.setup(database, seed_missing=True)
    calls = []

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent, cursor = variables.get("parent_comment_id"), variables.get("after")
        calls.append((parent, cursor))
        if parent is None:
            return httpx.Response(
                200,
                json=public.page([public.node("blocked", 1), public.node("success", 2), public.node("unstarted", 1)]),
            )
        if parent == "blocked":
            return httpx.Response(429, headers={"retry-after": "20"})
        assert parent == "success" and cursor is None
        return httpx.Response(
            200, json=public.page([public.node("valid-received")], child=True, more=True, after="must-remain-unsent")
        )

    fetch, _ = public.source(monkeypatch, handler, expected=6)
    source, _ = guarded_source(store, fresh_public_lanes, args, fetch, assert_received)
    with pytest.raises(SourceFailure):
        CommentActivity(store, source, clock=lambda: public.OBSERVED).execute(**args)
    assert sorted(calls[1:]) == [("blocked", None), ("success", None)]
    rows = parallel.saved_inventory(tx, args["post_id"])
    saved = {row["comment_id"]: row for row in rows}
    assert "valid-received" in saved, "already received valid source evidence was discarded on sibling source stop"
    assert saved["valid-received"]["parent_native_id"] == "success"
    state = parallel.checkpoint(tx, args["run_id"])
    assert state["lanes"]["success"]["cursors"] == ["must-remain-unsent"]
    assert state["lanes"]["success"]["exhausted"] is False and not state.get("settlement")
    assert saved["old"]["is_missing"] is False


@pytest.mark.parametrize("order", [("short", "long"), ("long", "short")])
def test_received_retry_after_uses_maximum_server_delay_regardless_of_completion_order(
    fresh_public_lanes, database, monkeypatch, order
):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    assert_received = complete_both_before_owner(monkeypatch, order)
    store, _, args = public.setup(database)
    calls = []

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        calls.append(parent)
        if parent is None:
            return httpx.Response(200, json=public.page([public.node("short", 1), public.node("long", 1)]))
        return httpx.Response(429, headers={"retry-after": "20" if parent == "short" else "60"})

    fetch, _ = public.source(monkeypatch, handler, expected=4)
    source, gate = guarded_source(store, fresh_public_lanes, args, fetch, assert_received)
    started = datetime.now(UTC)
    with pytest.raises(SourceFailure) as caught:
        CommentActivity(store, source, clock=lambda: public.OBSERVED).execute(**args)
    assert sorted(calls[1:]) == ["long", "short"]
    row = existing.query(
        fresh_public_lanes,
        "select * from social.instagram_detail_request_lanes where identity_key=%s",
        (gate.identity_key,),
    )[0]
    assert row["server_retry_after_until"] >= started + timedelta(seconds=60), (
        "the shorter sibling response replaced or prevented recording the received 60-second server hold"
    )
    assert caught.value.failure.retry_at >= started + timedelta(seconds=60)
    assert row["cooldown_until"] == row["server_retry_after_until"]


@pytest.mark.parametrize("failure", ["ownership", "persistence"])
def test_unsafe_owner_failure_never_drains_another_received_commit(database, monkeypatch, failure):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    assert_received = complete_both_before_owner(monkeypatch, ("failed", "other"))
    store, tx, args = public.setup(database)
    commits, calls = [], []
    original = CommentActivity._persist_page

    def persist(self, *positional, **kwargs):
        parent = kwargs["page"].parent_comment_id
        if parent:
            assert_received()
            commits.append(parent)
            if parent == "failed":
                if failure == "ownership":
                    raise LostOwnership("fixture owner is no longer valid")
                raise RuntimeError("fixture persistence failed")
        return original(self, *positional, **kwargs)

    monkeypatch.setattr(CommentActivity, "_persist_page", persist)

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        calls.append(parent)
        if parent is None:
            return httpx.Response(200, json=public.page([public.node("failed", 1), public.node("other", 1)]))
        return httpx.Response(200, json=public.page([public.node(parent + "-reply")], child=True))

    fetch, _ = public.source(monkeypatch, handler, expected=4)
    with pytest.raises(LostOwnership if failure == "ownership" else RuntimeError):
        CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    assert commits == ["failed"] and sorted(calls[1:]) == ["failed", "other"]
    assert {row["comment_id"] for row in parallel.saved_inventory(tx, args["post_id"])} == {"failed", "other"}
    state = parallel.checkpoint(tx, args["run_id"])
    assert "other" not in state["lanes"] and "failed" not in state["lanes"] and not state.get("settlement")
