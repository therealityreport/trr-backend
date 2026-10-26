"""Offline ownership, ordering and cleanup tests for bounded reply transport."""

import asyncio
import hashlib
import threading
import time
from contextvars import ContextVar
from dataclasses import FrozenInstanceError, replace

import pytest

from trr_backend.socials.instagram.comments_scrapling.reply_scheduler import (
    Delay,
    DiskResponseSpool,
    ReplyDeadlineExceeded,
    ReplyRequest,
    SchedulerTelemetry,
    run_reply_lanes,
)


def descriptor(parent, **kwargs):
    return ReplyRequest(parent, "doc", "popular", kwargs.pop("page_size", 12), **kwargs)


async def no_pace(_item):
    return None


class Response:
    def __init__(self, parent):
        self.parent = parent
        self.closed = 0

    def close(self):
        self.closed += 1


def test_descriptor_freezes_snapshots_and_validates_bounds():
    cookies = [["name", "value", "domain", "/"]]
    request = descriptor("a", public_cookie_snapshot=cookies)
    cookies[0][1] = "changed"
    assert request.public_cookie_snapshot == (("name", "value", "domain", "/"),)
    assert "value" not in repr(request)
    with pytest.raises(FrozenInstanceError):
        request.page_size = 50
    for size in (0, 51, True):
        with pytest.raises(ValueError):
            descriptor("a", page_size=size)


@pytest.mark.parametrize("offload_owner", [False, True])
def test_completion_order_owner_advancement_and_outstanding_bound(offload_owner):
    async def scenario():
        started = {key: asyncio.Event() for key in "abcde"}
        release = {key: asyncio.Event() for key in "abcde"}
        outstanding = set()
        peak = 0
        commits = []
        responses = []

        def lane(key):
            response = yield descriptor(key)
            assert response.parent == key
            commits.append(key)
            outstanding.remove(key)  # Slot remains occupied through this commit.
            return key + "-finished"

        async def request(item):
            nonlocal peak
            assert item.parent_id not in outstanding
            outstanding.add(item.parent_id)
            peak = max(peak, len(outstanding))
            started[item.parent_id].set()
            await release[item.parent_id].wait()
            response = Response(item.parent_id)
            responses.append(response)
            return response

        runner = asyncio.create_task(
            run_reply_lanes(
                {key: lane(key) for key in "abcde"},
                request=request,
                pace=no_pace,
                concurrency=2,
                offload_owner=offload_owner,
            )
        )
        await started["a"].wait()
        await started["b"].wait()
        assert not started["c"].is_set()
        release["b"].set()
        await started["c"].wait()
        assert commits == ["b"] and responses[0].closed == 1
        release["c"].set()
        await started["d"].wait()
        release["d"].set()
        await started["e"].wait()
        release["e"].set()
        release["a"].set()
        results = await runner
        assert results == {key: key + "-finished" for key in "abcde"}
        assert peak == 2 and not outstanding
        assert all(response.closed == 1 for response in responses)

    asyncio.run(scenario())


@pytest.mark.parametrize("offload_owner", [False, True])
def test_same_lane_next_request_waits_for_its_response_and_default_is_serial(offload_owner):
    async def scenario():
        trace = []

        def lane(key):
            for cursor in (None, "next"):
                response = yield descriptor(key, cursor=cursor)
                trace.append(("commit", key, cursor))
                assert response.closed == 0
            return key

        async def request(item):
            trace.append(("request", item.parent_id, item.cursor))
            return Response(item.parent_id)

        await run_reply_lanes(
            {key: lane(key) for key in "ab"}, request=request, pace=no_pace, offload_owner=offload_owner
        )
        assert len(trace) == 8
        assert all(trace[i][0] == "request" and trace[i + 1][0] == "commit" for i in range(0, 8, 2))
        assert [key for kind, key, _ in trace if kind == "request"] == ["a", "a", "b", "b"]

    asyncio.run(scenario())


def test_before_dispatch_after_pacing_refreshes_snapshot_and_telemetry():
    async def scenario():
        trace = []
        metrics = SchedulerTelemetry()

        def lane():
            response = yield descriptor("a")
            trace.append("commit")
            return response.parent

        async def pace(_item):
            trace.append("pace")
            await asyncio.sleep(0)

        def before(item):
            trace.append("before")
            return replace(item, public_cookie_snapshot=(("name", "public", "domain", "/"),))

        async def request(item):
            assert item.public_cookie_snapshot[0][1] == "public"
            trace.append("request")
            return Response(item.parent_id)

        result = await run_reply_lanes(
            {"a": lane()}, request=request, pace=pace, before_dispatch=before, telemetry=metrics
        )
        assert result == {"a": "a"} and trace == ["pace", "before", "request", "commit"]
        assert metrics.requests == 1
        assert all(
            value >= 0
            for value in (metrics.network_seconds, metrics.pacing_seconds, metrics.queue_seconds, metrics.owner_seconds)
        )

    asyncio.run(scenario())


def test_deadline_prevents_dispatch_and_delay_returns_to_owner():
    async def scenario():
        def expired():
            try:
                yield descriptor("expired", deadline=5)
            except ReplyDeadlineExceeded:
                return "deadline"

        def waiting():
            value = yield Delay(0)
            assert value is True
            return "waited"

        async def forbidden(_item):
            pytest.fail("expired descriptor reached pacing/transport")

        result = await run_reply_lanes(
            {"expired": expired(), "waiting": waiting()}, request=forbidden, pace=forbidden, clock=lambda: 10
        )
        assert result == {"expired": "deadline", "waiting": "waited"}

    asyncio.run(scenario())


def test_transport_error_is_thrown_to_serial_owner():
    async def scenario():
        failure = OSError("transport")

        def lane():
            try:
                yield descriptor("a")
            except OSError as error:
                assert error is failure
                return "retained-unresolved"

        async def request(_item):
            raise failure

        assert await run_reply_lanes({"a": lane()}, request=request, pace=no_pace) == {"a": "retained-unresolved"}

    asyncio.run(scenario())


@pytest.mark.parametrize("offload_owner", [False, True])
def test_commit_failure_closes_both_consumed_and_waiting_completed_responses(offload_owner):
    async def scenario():
        responses = []
        branches_closed = set()

        def lane(key):
            try:
                yield descriptor(key)
                raise ValueError("commit failure")
            finally:
                branches_closed.add(key)

        async def request(item):
            response = Response(item.parent_id)
            responses.append(response)
            return response

        with pytest.raises(ValueError, match="commit failure"):
            await run_reply_lanes(
                {key: lane(key) for key in "ab"},
                request=request,
                pace=no_pace,
                concurrency=2,
                offload_owner=offload_owner,
            )
        assert len(responses) == 2 and all(response.closed == 1 for response in responses)
        assert branches_closed == {"a", "b"}

    asyncio.run(scenario())


def test_stop_after_commit_closes_waiting_responses_and_stops_new_dispatch():
    async def scenario():
        responses = []
        stopped = False

        def lane(key):
            nonlocal stopped
            yield descriptor(key)
            stopped = True
            return key

        async def request(item):
            response = Response(item.parent_id)
            responses.append(response)
            return response

        result = await run_reply_lanes(
            {key: lane(key) for key in "abc"}, request=request, pace=no_pace, concurrency=2, stop=lambda: stopped
        )
        assert len(result) == 1 and len(responses) == 2
        assert all(response.closed == 1 for response in responses)

    asyncio.run(scenario())


def test_cancellation_drains_shielded_pacing_thread_before_return():
    async def scenario():
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def work():
            started.set()
            assert release.wait(3)
            finished.set()

        async def pace(_item):
            await asyncio.to_thread(work)

        def lane():
            yield descriptor("a")

        async def request(_item):
            pytest.fail("transport launched after cancellation")

        runner = asyncio.create_task(run_reply_lanes({"a": lane()}, request=request, pace=pace))
        while not started.is_set():
            await asyncio.sleep(0)
        runner.cancel()
        await asyncio.sleep(0.01)
        assert not runner.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await runner
        assert finished.is_set()

    asyncio.run(scenario())


def test_cancellation_awaits_transport_and_delay_tasks():
    async def scenario():
        started = asyncio.Event()
        finished = asyncio.Event()

        def waiting():
            yield Delay(60)

        def lane():
            yield descriptor("a")

        async def request(_item):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finished.set()

        runner = asyncio.create_task(
            run_reply_lanes({"a": lane(), "wait": waiting()}, request=request, pace=no_pace, concurrency=2)
        )
        await started.wait()
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner
        assert finished.is_set()
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]

    asyncio.run(scenario())


@pytest.mark.parametrize("offload_owner", [False, True])
def test_stop_drains_received_evidence_without_dispatching_continuations(offload_owner):
    async def scenario():
        stopped = False
        commits = []
        responses = []
        initialized = []

        def lane(key):
            nonlocal stopped
            initialized.append(key)
            yield descriptor(key)
            commits.append(key)
            stopped = True
            yield descriptor(key, cursor="must-not-dispatch")

        async def request(item):
            assert item.cursor is None
            response = Response(item.parent_id)
            responses.append(response)
            return response

        result = await run_reply_lanes(
            {key: lane(key) for key in "abc"},
            request=request,
            pace=no_pace,
            concurrency=2,
            stop=lambda: stopped,
            drain_received_on_stop=True,
            offload_owner=offload_owner,
        )
        assert result == {} and initialized == ["a", "b"]
        assert commits == ["a", "b"] and len(responses) == 2
        assert all(response.closed == 1 for response in responses)

    asyncio.run(scenario())


def test_active_response_deadline_cancels_and_awaits_transport():
    async def scenario():
        finished = asyncio.Event()

        def lane():
            try:
                yield descriptor("a", deadline=asyncio.get_running_loop().time() + 0.01)
            except ReplyDeadlineExceeded:
                return "deadline"

        async def request(_item):
            try:
                await asyncio.sleep(60)
            finally:
                finished.set()

        assert await run_reply_lanes({"a": lane()}, request=request, pace=no_pace) == {"a": "deadline"}
        assert finished.is_set()

    asyncio.run(scenario())


def test_duplicate_parent_lanes_fail_before_any_transport():
    async def scenario():
        def lane():
            yield descriptor("same-parent")

        async def forbidden(_item):
            pytest.fail("duplicate parent dispatched")

        with pytest.raises(ValueError, match="only one reply lane"):
            await run_reply_lanes({"a": lane(), "b": lane()}, request=forbidden, pace=no_pace, concurrency=2)

    asyncio.run(scenario())


def test_disk_spool_keeps_large_body_without_truncation_and_parses_on_owner():
    chunk = b"a" * (64 * 1024)
    expected = hashlib.sha256()
    with DiskResponseSpool.from_chunks(chunk for _ in range(160)) as spool:
        assert spool.size == 10 * 1024 * 1024 > spool.MAX_MEMORY_BYTES
        actual = hashlib.sha256()
        for part in spool.iter_chunks():
            assert len(part) <= 64 * 1024
            actual.update(part)
        for _ in range(160):
            expected.update(chunk)
        assert actual.digest() == expected.digest()
    assert spool.closed
    with DiskResponseSpool.from_chunks([b'{"data":', b"[1,2]}"]) as spool:
        assert spool.json() == {"data": [1, 2]}


def test_spool_handles_short_disk_writes_without_truncation():
    class ShortWriter:
        def __init__(self):
            self.data = bytearray()

        def write(self, chunk):
            part = chunk[:2]
            self.data.extend(part)
            return len(part)

    spool = DiskResponseSpool()
    spool.close()
    writer = ShortWriter()
    spool._file = writer
    spool.write(b"complete-body")
    assert bytes(writer.data) == b"complete-body" and spool.size == len(b"complete-body")


@pytest.mark.parametrize("concurrency", [0, 3, True])
def test_invalid_concurrency_is_rejected(concurrency):
    with pytest.raises(ValueError):
        asyncio.run(run_reply_lanes({}, request=no_pace, pace=no_pace, concurrency=concurrency))


@pytest.mark.parametrize("offload_owner", [False, True])
def test_owner_keeps_one_thread_and_context_across_yields_and_close(offload_owner):
    from trr_backend.ingestion.reply_context import execution_deadline, request_permit
    from trr_backend.socials.instagram.comments_scrapling.reply_http import response_observed_at

    async def scenario():
        loop_thread = threading.get_ident()
        owner_threads = []
        across_yield = ContextVar("reply_owner_across_yield", default="outside")
        deadline, permit, observed = time.monotonic() + 60, object(), object()
        tokens = [execution_deadline.set(deadline), request_permit.set(permit), response_observed_at.set(observed)]
        trace = []

        def lane():
            owner_threads.append(threading.get_ident())
            assert (execution_deadline.get(), request_permit.get(), response_observed_at.get()) == (
                deadline,
                permit,
                observed,
            )
            token = across_yield.set("owner")
            try:
                yield descriptor("a")
                owner_threads.append(threading.get_ident())
                assert across_yield.get() == "owner"
                trace.append("commit")
                yield Delay(60)
            finally:
                owner_threads.append(threading.get_ident())
                assert across_yield.get() == "owner"
                across_yield.reset(token)
                trace.append("closed")

        def before(_item):
            assert threading.get_ident() == loop_thread
            assert across_yield.get() == ("outside" if offload_owner else "owner")

        async def request(_item):
            assert threading.get_ident() == loop_thread
            return Response("a")

        try:
            await run_reply_lanes(
                {"a": lane()},
                request=request,
                pace=no_pace,
                before_dispatch=before,
                stop=lambda: "commit" in trace,
                offload_owner=offload_owner,
            )
            assert trace == ["commit", "closed"]
            assert len(set(owner_threads)) == 1
            assert (owner_threads[0] != loop_thread) == offload_owner
            assert across_yield.get() == "outside"
        finally:
            for variable, token in zip((execution_deadline, request_permit, response_observed_at), tokens, strict=True):
                variable.reset(token)

    asyncio.run(scenario())


def test_offloaded_owner_cancellation_joins_commit_before_all_cleanup_and_never_dispatches_next_cursor():
    async def scenario():
        committing, release, committed = threading.Event(), threading.Event(), threading.Event()
        closed, responses, threads = [], [], []
        owner_states = {key: ContextVar(f"reply_cancel_owner_{key}", default=None) for key in "ab"}

        def lane(key):
            owner_state = owner_states[key]
            token = owner_state.set(key)
            try:
                yield descriptor(key)
                threads.append(threading.get_ident())
                committing.set()
                assert release.wait(2)
                committed.set()
                yield descriptor(key, cursor="must-not-dispatch")
            finally:
                assert owner_state.get() == key
                owner_state.reset(token)
                threads.append(threading.get_ident())
                closed.append(key)

        class CommittedResponse(Response):
            def close(self):
                assert committed.is_set()
                super().close()

        async def request(item):
            assert item.cursor is None
            response = CommittedResponse(item.parent_id)
            responses.append(response)
            return response

        runner = asyncio.create_task(
            run_reply_lanes(
                {key: lane(key) for key in "ab"}, request=request, pace=no_pace, concurrency=2, offload_owner=True
            )
        )
        try:
            async with asyncio.timeout(2):
                while not committing.is_set():
                    await asyncio.sleep(0)
            runner.cancel()
            await asyncio.sleep(0.005)
            runner.cancel()
            await asyncio.sleep(0.005)
            assert not runner.done() and not closed and all(response.closed == 0 for response in responses)
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await runner
        assert committed.is_set() and set(closed) == {"a", "b"}
        assert len(set(threads)) == 1
        assert all(response.closed == 1 for response in responses)
        assert not [thread for thread in threading.enumerate() if thread.name.startswith("instagram-reply-owner")]

    asyncio.run(scenario())


def test_offloaded_owner_offline_timing_allows_network_progress_during_fifty_millisecond_commit():
    async def sample(offload_owner):
        started = time.monotonic()
        events, commits = {}, []
        metrics = SchedulerTelemetry()

        def lane(key):
            response = yield descriptor(key)
            time.sleep(0.05)
            events[f"commit-{key}"] = time.monotonic() - started
            commits.append(response.parent)
            return key

        async def request(item):
            await asyncio.sleep(0.005 if item.parent_id == "a" else 0.025)
            events[f"network-{item.parent_id}"] = time.monotonic() - started
            return Response(item.parent_id)

        result = await run_reply_lanes(
            {key: lane(key) for key in "ab"},
            request=request,
            pace=no_pace,
            concurrency=2,
            telemetry=metrics,
            offload_owner=offload_owner,
        )
        assert metrics.max_outstanding == 2
        return result, commits, {**events, "elapsed": time.monotonic() - started}

    before = asyncio.run(sample(False))
    after = asyncio.run(sample(True))
    assert before[:2] == after[:2] == ({"a": "a", "b": "b"}, ["a", "b"])
    assert before[2]["network-b"] > before[2]["commit-a"]
    assert after[2]["network-b"] < after[2]["commit-a"]
    print(
        {
            label: {key: round(value * 1000, 2) for key, value in sample[2].items()}
            for label, sample in (("inline_ms", before), ("offloaded_ms", after))
        }
    )
