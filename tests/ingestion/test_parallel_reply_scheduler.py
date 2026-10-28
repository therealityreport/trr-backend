"""Offline admission concurrency and owner-context regressions; no source calls."""

import asyncio
import threading
import time
from contextvars import ContextVar
from dataclasses import replace

import pytest

from trr_backend.socials.instagram.comments_scrapling.reply_scheduler import (
    ReplyDeadlineExceeded,
    ReplyDiscardUnresolved,
    ReplyRequest,
    SchedulerTelemetry,
    run_reply_lanes,
)


async def no_pace(_item):
    return None


def test_slow_admission_allows_admitted_sibling_http_progress_with_matched_work():
    async def sample(offload_owner):
        started = time.monotonic()
        events, sent, committed, closed = {}, [], [], []
        first_started = asyncio.Event()
        admission_started = threading.Event()
        metrics = SchedulerTelemetry()
        permits = {key: object() for key in "ab"}
        deadline = started + 3

        def lane(key):
            response = yield ReplyRequest(key, "doc", "popular", 50, cursor=f"saved-{key}", deadline=deadline)
            committed.append(response)
            return response

        async def pace(item):
            if item.parent_id == "b":
                await first_started.wait()

        def before(item):
            if item.parent_id == "b":
                events["admission_b_start"] = time.monotonic() - started
                admission_started.set()
                time.sleep(0.08)
                events["admission_b_end"] = time.monotonic() - started
            return replace(
                item,
                request_permit=permits[item.parent_id],
                public_cookie_snapshot=(("public", item.parent_id, ".instagram.com", "/"),),
            )

        async def request(item):
            assert item.request_permit is permits[item.parent_id]
            assert item.public_cookie_snapshot == (("public", item.parent_id, ".instagram.com", "/"),)
            assert item.deadline == deadline and item.page_size == 50
            assert item.cursor == f"saved-{item.parent_id}"
            sent.append(item.parent_id)
            if item.parent_id == "a":
                first_started.set()
                while not admission_started.is_set():
                    await asyncio.sleep(0)
            await asyncio.sleep(0.02)
            events[f"http_{item.parent_id}_end"] = time.monotonic() - started
            return item.parent_id + "-reply"

        result = await run_reply_lanes(
            {key: lane(key) for key in "ab"},
            request=request,
            pace=pace,
            before_dispatch=before,
            concurrency=2,
            offload_owner=offload_owner,
            telemetry=metrics,
            close_response=closed.append,
        )
        assert result == {"a": "a-reply", "b": "b-reply"}
        assert sent == ["a", "b"] and committed == closed == ["a-reply", "b-reply"]
        assert metrics.requests == 2 and metrics.max_outstanding == 2
        return {**events, "elapsed": time.monotonic() - started}

    before = asyncio.run(sample(False))
    after = asyncio.run(sample(True))
    print(
        {
            label: {key: round(value * 1000, 2) for key, value in values.items()}
            for label, values in (("inline_ms", before), ("owner_executor_ms", after))
        }
    )
    assert before["http_a_end"] > before["admission_b_end"]
    assert after["admission_b_start"] < after["http_a_end"] < after["admission_b_end"]


def test_admission_cancellation_drains_same_owner_context_before_generator_close():
    async def scenario():
        admitting, release, admitted = threading.Event(), threading.Event(), threading.Event()
        state = ContextVar("reply_admission_owner", default="outside")
        owner_threads, closed, discarded = [], [], []
        permit = object()

        def lane():
            token = state.set("owner")
            owner_threads.append(threading.get_ident())
            try:
                yield ReplyRequest("a", "doc", "popular", 12)
                pytest.fail("cancelled admission resumed the generator")
            finally:
                assert admitted.is_set() and state.get() == "admitted" and len(discarded) == 1
                owner_threads.append(threading.get_ident())
                state.reset(token)
                closed.append("a")

        def before(item):
            assert state.get() == "owner"
            owner_threads.append(threading.get_ident())
            admitting.set()
            assert release.wait(2)
            state.set("admitted")
            admitted.set()
            return replace(item, request_permit=permit)

        def discard(item):
            assert state.get() == "admitted" and item.request_permit is permit
            owner_threads.append(threading.get_ident())
            discarded.append(item)

        async def request(_item):
            pytest.fail("transport started after admission cancellation")

        runner = asyncio.create_task(
            run_reply_lanes(
                {"a": lane()},
                request=request,
                pace=no_pace,
                before_dispatch=before,
                discard=discard,
                offload_owner=True,
            )
        )
        try:
            async with asyncio.timeout(2):
                while not admitting.is_set():
                    await asyncio.sleep(0)
            runner.cancel()
            await asyncio.sleep(0.005)
            runner.cancel()
            await asyncio.sleep(0.005)
            assert not runner.done() and not closed and not admitted.is_set()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await runner
        assert closed == ["a"] and len(set(owner_threads)) == 1
        assert owner_threads[0] != threading.get_ident() and state.get() == "outside"
        assert not [thread for thread in threading.enumerate() if thread.name.startswith("instagram-reply-owner")]

    asyncio.run(scenario())


@pytest.mark.parametrize("outcome", ["expired", "stopped", "rejected"])
def test_admission_stop_rejection_or_expiry_never_dispatches(outcome):
    async def scenario():
        now, stopped = [1.0], [False]
        expected_error = RuntimeError("owner rejected admission")
        admitted, discarded, owner_threads = [], [], []
        permit = object()
        metrics = SchedulerTelemetry()

        def lane():
            try:
                yield ReplyRequest("a", "doc", "popular", 12, deadline=2.0)
            except ReplyDeadlineExceeded:
                assert outcome == "expired"
                return "expired"
            except RuntimeError as error:
                assert outcome == "rejected" and error is expected_error
                return "rejected"
            pytest.fail("unsent descriptor resumed as a response")

        def before(item):
            owner_threads.append(threading.get_ident())
            admitted.append(item.parent_id)
            if outcome == "rejected":
                raise expected_error
            now[0] = 3.0 if outcome == "expired" else now[0]
            stopped[0] = outcome == "stopped"
            return replace(item, request_permit=permit)

        def discard(item):
            assert item.request_permit is permit
            owner_threads.append(threading.get_ident())
            discarded.append(item.parent_id)

        async def request(_item):
            pytest.fail("invalid admission reached HTTP")

        result = await run_reply_lanes(
            {"a": lane()},
            request=request,
            pace=no_pace,
            before_dispatch=before,
            discard=discard,
            offload_owner=True,
            stop=lambda: stopped[0],
            clock=lambda: now[0],
            telemetry=metrics,
        )
        assert admitted == ["a"] and metrics.requests == 0 and metrics.max_outstanding == 1
        assert discarded == ([] if outcome == "rejected" else ["a"])
        assert len(set(owner_threads)) == 1 and owner_threads[0] != threading.get_ident()
        assert result == ({} if outcome == "stopped" else {"a": outcome})

    asyncio.run(scenario())


@pytest.mark.parametrize("cancel_before_entry", [False, True])
def test_discard_only_before_transport_coroutine_starts(monkeypatch, cancel_before_entry):
    from trr_backend.socials.instagram.comments_scrapling import reply_scheduler

    async def scenario():
        state = ContextVar("reply_discard_owner", default="outside")
        permit = object()
        discarded, started, closed, threads = [], [], [], []
        metrics = SchedulerTelemetry()
        original_create_task = asyncio.create_task
        runner = None

        def lane():
            token = state.set("owner")
            try:
                yield ReplyRequest("a", "doc", "popular", 12)
            finally:
                state.reset(token)
                closed.append("a")

        def before(item):
            threads.append(threading.get_ident())
            return replace(item, request_permit=permit)

        def discard(item):
            assert state.get() == "owner" and item.request_permit is permit
            threads.append(threading.get_ident())
            discarded.append(item.parent_id)

        async def request(_item):
            started.append("a")
            assert runner is not None
            runner.cancel()
            await asyncio.sleep(60)

        def create_task(coroutine, *args, **kwargs):
            task = original_create_task(coroutine, *args, **kwargs)
            if cancel_before_entry and coroutine.cr_code.co_name == "transported":
                task.cancel()
                assert runner is not None
                runner.cancel()
            return task

        monkeypatch.setattr(reply_scheduler.asyncio, "create_task", create_task)
        runner = original_create_task(
            run_reply_lanes(
                {"a": lane()},
                request=request,
                pace=no_pace,
                before_dispatch=before,
                discard=discard,
                offload_owner=True,
                telemetry=metrics,
            )
        )
        with pytest.raises(asyncio.CancelledError):
            await runner
        assert discarded == (["a"] if cancel_before_entry else [])
        assert started == ([] if cancel_before_entry else ["a"])
        assert metrics.requests == (0 if cancel_before_entry else 1)
        assert closed == ["a"] and len(set(threads)) == 1 and threads[0] != threading.get_ident()

    asyncio.run(scenario())


def test_cancel_pacing_signals_queued_turn_before_join_and_outer_turn_close():
    async def scenario():
        acquired, queued, cancelled, drained = (threading.Event() for _ in range(4))
        trace, sent = [], []
        runner = None

        def acquire(parent):
            if parent == "a":
                acquired.set()
                assert queued.wait(1)
                trace.append("acquired-a")
            else:
                assert acquired.wait(1)
                queued.set()
                assert cancelled.wait(1), "queued turn was not signalled before its shielded pacing join"
                trace.append("drained-b")
                drained.set()

        async def pace(item):
            await asyncio.to_thread(acquire, item.parent_id)
            if item.parent_id == "a":
                # The first turn remains acquired until the fetcher's outer
                # cleanup. Its descriptor has not yet reached admission.
                assert runner is not None
                runner.cancel()

        def cancel_pacing():
            trace.append("cancel-hook")
            cancelled.set()

        def lane(parent):
            try:
                yield ReplyRequest(parent, "doc", "popular", 12)
            finally:
                assert drained.is_set()
                trace.append("closed-" + parent)

        def before(_item):
            pytest.fail("cancelled unadmitted turn reached admission")

        async def request(item):
            sent.append(item.parent_id)

        runner = asyncio.create_task(
            run_reply_lanes(
                {key: lane(key) for key in "ab"},
                request=request,
                pace=pace,
                before_dispatch=before,
                cancel_pacing=cancel_pacing,
                concurrency=2,
                offload_owner=True,
            )
        )
        try:
            with pytest.raises(asyncio.CancelledError):
                await runner
        finally:
            trace.append("outer-turn-close")
            acquired.clear()
        assert sent == [] and trace.count("cancel-hook") == 1
        assert trace.index("cancel-hook") < trace.index("drained-b")
        assert trace.index("drained-b") < trace.index("closed-a") < trace.index("outer-turn-close")
        assert trace.index("drained-b") < trace.index("closed-b") < trace.index("outer-turn-close")

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["transient", "ambiguous", "persistent"])
def test_discard_retains_exact_descriptor_for_one_idempotent_cleanup_retry(failure):
    async def scenario():
        admitting, release = threading.Event(), threading.Event()
        state = ContextVar("reply_refund_retry_owner", default="outside")
        permit = object()
        attempts, refunds, sent, closed, threads = [], [], [], [], []
        charged = [0]
        metrics = SchedulerTelemetry()

        def lane(parent):
            token = state.set("owner") if parent == "a" else None
            try:
                yield ReplyRequest(parent, "doc", "popular", 12)
            finally:
                if token is not None:
                    state.reset(token)
                closed.append(parent)

        def before(item):
            assert item.parent_id == "a", "a sibling was admitted while a refund remained unresolved"
            charged[0] += 1
            admitting.set()
            assert release.wait(1)
            return replace(item, request_permit=permit)

        def discard(item):
            assert state.get() == "owner" and item.request_permit is permit
            threads.append(threading.get_ident())
            attempts.append(item)
            if failure == "persistent" or (failure == "transient" and len(attempts) == 1):
                raise RuntimeError("temporary refund rejection")
            # This fake receipt represents the gate's atomic idempotency record.
            if permit not in refunds:
                charged[0] -= 1
                refunds.append(permit)
            if failure == "ambiguous" and len(attempts) == 1:
                raise RuntimeError("refund committed but acknowledgment was lost")

        async def request(item):
            sent.append(item.parent_id)

        runner = asyncio.create_task(
            run_reply_lanes(
                {key: lane(key) for key in "ab"},
                request=request,
                pace=no_pace,
                before_dispatch=before,
                discard=discard,
                concurrency=2,
                offload_owner=True,
                telemetry=metrics,
            )
        )
        try:
            async with asyncio.timeout(1):
                while not admitting.is_set():
                    await asyncio.sleep(0)
            runner.cancel()
        finally:
            release.set()
        if failure == "persistent":
            with pytest.raises(ReplyDiscardUnresolved, match="refund remains unresolved") as raised:
                await runner
            assert raised.value.descriptors == (attempts[0],)
            assert raised.value.descriptors[0] is attempts[0]
            assert len(raised.value.failures) == 2
            assert isinstance(raised.value.__cause__, asyncio.CancelledError)
        else:
            # A reconciled transient/ambiguous refund preserves cancellation,
            # rather than replacing it with its already recovered first error.
            with pytest.raises(asyncio.CancelledError):
                await runner
        assert len(attempts) == 2 and attempts[0] is attempts[1]
        assert charged == ([1] if failure == "persistent" else [0])
        assert refunds == ([] if failure == "persistent" else [permit])
        assert sent == [] and metrics.requests == 0 and set(closed) == {"a", "b"}
        assert len(set(threads)) == 1 and threads[0] != threading.get_ident()

    asyncio.run(scenario())
