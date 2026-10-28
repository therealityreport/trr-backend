"""Expected owner time caps retain progress without a source health penalty."""

import asyncio
import time

import httpx
import pytest

from tests.ingestion import test_parallel_reply_transport as parallel
from tests.ingestion import test_public_comment_transport as public
from tests.ingestion.test_parallel_reply_context import Gate
from trr_backend.ingestion.composition import _GatedSource
from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.post_transport import SourceFailure

database = public.database
request_lanes = public.request_lanes
fresh_public_lanes = public.fresh_public_lanes
deny_unmocked_http = public.deny_unmocked_http
assert_transport_resources_are_drained = parallel.assert_transport_resources_are_drained


def test_actual_public_reply_owner_deadline_is_not_source_health_failure(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    gate = Gate()
    gate.deadline = time.monotonic() + 0.15
    received = []

    def finish(code=None, **kwargs):
        gate.failures.append((code, kwargs))
        return Failure(code) if code else None

    gate.finish_failure = finish  # pyright: ignore[reportAttributeAccessIssue]

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        if parent is None:
            return httpx.Response(200, json=public.page([public.actual_parent_shape(p) for p in ("p1", "p2")]))
        received.append(parent)
        await asyncio.sleep(60)
        pytest.fail("reply HTTP task survived its owner deadline")

    fetch, _ = public.source(monkeypatch, handler, expected=None)
    with pytest.raises(SourceFailure) as caught:
        _GatedSource(fetch, gate, failure_code="comment_fetch_failed")(
            shortcode="fixture",
            checkpoint={},
            pre_source_request=lambda: None,
            source_observer=lambda *args: None,
        )
    assert set(received) == {"p1", "p2"}
    assert caught.value.failure.code == "target_deadline"
    assert not [code for code, _ in gate.failures if code is not None]


def test_real_gate_deadline_retains_peer_page_without_health_failure(fresh_public_lanes, database, monkeypatch):
    from trr_backend.ingestion.comments import CommentActivity
    from trr_backend.ingestion.domain import Stage
    from trr_backend.ingestion.identity_policy import SourceRequestGate

    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    store, tx, args = public.setup(database)
    owned = {key: value for key, value in args.items() if key != "post_id"}
    gate = SourceRequestGate(
        cookies={},
        public_only=True,
        network_lane="local",
        transaction=tx,
        check_cancelled=lambda: store.check_owner(**owned, stage=Stage.COMMENTS),
        check_cancelled_in_transaction=lambda conn: store.check_owner(**owned, stage=Stage.COMMENTS, connection=conn),
        deadline=time.monotonic() + 0.5,
    )
    started, cancelled = [], []

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        if parent is None:
            return httpx.Response(200, json=public.page([public.actual_parent_shape(p) for p in ("p1", "p2")]))
        started.append(parent)
        if parent == "p2":
            await asyncio.sleep(0.01)
            return httpx.Response(200, json=public.page([public.node("received-peer")], child=True))
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(parent)
            raise
        pytest.fail("deadline request survived shutdown")

    fetch, _ = public.source(monkeypatch, handler, expected=None)
    with pytest.raises(SourceFailure) as caught:
        CommentActivity(
            store, _GatedSource(fetch, gate, failure_code="comment_fetch_failed"), clock=lambda: public.OBSERVED
        ).execute(**args)
    assert caught.value.failure.code == "target_deadline"
    assert set(started) == {"p1", "p2"} and cancelled == ["p1"]
    row = public.existing.query(
        tx, "select * from social.instagram_detail_request_lanes where identity_key=%s", (gate.identity_key,)
    )[0]
    assert row["failure_generation"] == row["consecutive_failures"] == 0
    assert (
        row["last_error_code"] is row["cooldown_until"] is row["server_retry_after_until"] is row["probe_token"] is None
    )
    saved = public.existing.query(
        tx,
        "select comment_id,parent_comment_external_id from social.instagram_comments where post_id=%s",
        (args["post_id"],),
    )
    assert {item["comment_id"] for item in saved} == {"p1", "p2", "received-peer"}
    assert next(item for item in saved if item["comment_id"] == "received-peer")["parent_comment_external_id"] == "p2"
    cp = public.existing.query(
        tx, "select checkpoint from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
    )[0]["checkpoint"]["comment_pages"]
    assert cp["lanes"]["p2"]["exhausted"] is True and "p1" not in cp["lanes"]


def test_received_rate_limit_wins_over_primary_expected_deadline(monkeypatch):
    from trr_backend.socials.instagram.comments_scrapling import reply_scheduler

    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    original = reply_scheduler.run_reply_lanes

    async def synchronized(lanes, *, request, **kwargs):
        release = {parent: asyncio.Event() for parent in ("p1", "p2")}
        received = set()

        async def transport(descriptor):
            response = await request(descriptor)
            received.add(descriptor.parent_id)
            if len(received) == 2:
                release["p1"].set()
            await release[descriptor.parent_id].wait()
            if descriptor.parent_id == "p1":
                release["p2"].set()
                response.close()
                raise reply_scheduler.ReplyDeadlineExceeded("synthetic expected attempt cap")
            return response

        return await original(lanes, request=transport, **kwargs)

    monkeypatch.setattr(reply_scheduler, "run_reply_lanes", synchronized)
    gate = Gate()

    def finish(code=None, **kwargs):
        gate.failures.append((code, kwargs))
        return Failure(code) if code else None

    gate.finish_failure = finish  # pyright: ignore[reportAttributeAccessIssue]

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        if parent is None:
            return httpx.Response(200, json=public.page([public.actual_parent_shape(p) for p in ("p1", "p2")]))
        if parent == "p1":
            return httpx.Response(200, json=public.page([], child=True))
        return httpx.Response(429, headers={"retry-after": "60"})

    fetch, _ = public.source(monkeypatch, handler, expected=None)
    with pytest.raises(SourceFailure) as caught:
        _GatedSource(fetch, gate, failure_code="comment_fetch_failed")(
            shortcode="fixture",
            checkpoint={},
            pre_source_request=lambda: None,
            source_observer=lambda *args: None,
        )
    assert caught.value.failure.code == "rate_limited"
    assert gate.failures[0][0] == "rate_limited" and gate.failures[0][1]["retry_after_seconds"] == 60
