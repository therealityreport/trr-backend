"""Native proof pruning and new-root size fallback without live source access."""

import asyncio
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from tests.ingestion import test_public_comment_transport as public
from tests.socials.instagram.comments_scrapling.test_fetcher_status_only import _build_fetcher, _comment
from trr_backend.socials.instagram.comments_scrapling import fetcher, reply_http, reply_scheduler

EXACT = {
    "exhausted": True,
    "started_from_root": True,
    "relay_doc_id": "native-doc",
    "relay_friendly_name": "native-query",
    "sort_order": "popular",
    "page_size": 12,
}


@pytest.mark.parametrize(
    "count,observed,lane,leaf,reason",
    [
        (None, 0, {}, False, None),
        (None, 1, {}, False, None),
        (0, 0, {}, False, "authoritative_zero"),
        (0, 1, {}, False, None),
        (1, 1, {}, False, None),
        (None, 0, EXACT, False, "exact_exhausted"),
        (None, 0, {"exhausted": True}, False, None),
        (None, 0, {**EXACT, "started_from_root": False}, False, None),
        (None, 0, {}, True, "native_leaf"),
        (2, 1, EXACT, True, None),
        (0, 0, {**EXACT, "pending": True}, True, None),
        (0, 0, {**EXACT, "technical_failure": True}, True, None),
        (None, 0, {**EXACT, "after": "unfinished"}, True, None),
        (0, 0, {"exhausted": False}, True, None),
        (0, 0, {"recovery": True}, True, None),
    ],
)
def test_only_proved_work_is_pruned(count, observed, lane, leaf, reason):
    assert (
        fetcher.reply_work_skip_reason(
            expected_replies=count,
            observed_replies=observed,
            lane=lane,
            native_leaf=leaf,
        )
        == reason
    )


def embedded(identity="parent"):
    return {
        **public.actual_parent_shape(identity, 1),
        "edge_threaded_comments": {
            "count": 1,
            "edges": [{"node": public.node("embedded-child")}],
            "page_info": {"has_next_page": False, "end_cursor": "last-native-edge"},
        },
    }


@pytest.mark.parametrize("mutation", ["unknown", "null", "gap", "duplicate", "missing_id", "unfinished"])
def test_embedded_preview_or_missing_counts_do_not_prove_completion(mutation):
    native = embedded()
    assert fetcher.native_embedded_reply_complete(native)
    thread = native["edge_threaded_comments"]
    if mutation == "unknown":
        thread.pop("count")
    elif mutation == "null":
        thread["count"] = None
    elif mutation == "gap":
        thread["count"] = 2
    elif mutation == "duplicate":
        thread["edges"] *= 2
        thread["count"] = 2
    elif mutation == "missing_id":
        thread["edges"][0]["node"].pop("pk")
    else:
        thread["page_info"]["has_next_page"] = True
    assert not fetcher.native_embedded_reply_complete(native)


def test_prefilter_keeps_unknown_and_positive_gaps_and_prunes_native_proofs(monkeypatch):
    calls, retained, generator_counts = [], [], []
    original_run = reply_scheduler.run_reply_lanes

    async def run(lanes, **kwargs):
        generator_counts.append(len(lanes))
        return await original_run(lanes, **kwargs)

    monkeypatch.setattr(reply_scheduler, "run_reply_lanes", run)
    complete = embedded()
    preview = deepcopy(complete)
    preview["pk"] = "gap"
    preview["child_comment_count"] = 2
    preview["edge_threaded_comments"]["count"] = 2
    preview["edge_threaded_comments"]["edges"][0]["node"]["pk"] = "preview-child"

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        calls.append(parent)
        if parent is None:
            return httpx.Response(
                200,
                json=public.page(
                    [
                        public.node("zero"),
                        complete,
                        preview,
                        public.actual_parent_shape("unknown"),
                    ]
                ),
            )
        assert parent in {"gap", "unknown"}
        return httpx.Response(200, json=public.page([], child=True))

    source, instances = public.source(monkeypatch, handler, expected=None)
    summary = source(
        shortcode="fixture",
        checkpoint={},
        source_observer=lambda *args: retained.append(args),
        pre_source_request=lambda: None,
    )
    assert generator_counts == [2]  # Four parents previously constructed four generators.
    assert calls == [None, "gap", "unknown"]
    assert not summary.fetch_failed
    metadata = instances[0]._reply_scheduler_telemetry
    assert metadata["requests"] == 2
    assert len(retained) == 3  # Root plus the two required child pages.


@pytest.mark.parametrize("lane", ["parent", "child"])
def test_explicit_new_root_size_rejection_retries_safe_size_without_false_page(monkeypatch, lane):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_PAGE_SIZE", "50")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PAGE_SIZE", "50")
    calls, retained = [], []

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        calls.append((parent, variables["first"], variables.get("after")))
        selected = parent is None if lane == "parent" else parent is not None
        if selected and variables["first"] == 50:
            return httpx.Response(400, json={"message": "Unsupported first size"})
        return httpx.Response(
            200,
            json=public.page(
                [public.actual_parent_shape("unknown")] if parent is None else [],
                child=parent is not None,
            ),
        )

    source, _ = public.source(monkeypatch, handler, expected=None)
    summary = source(
        shortcode="fixture",
        checkpoint={},
        source_observer=lambda *args: retained.append(args),
        pre_source_request=lambda: None,
    )
    selected_calls = [item for item in calls if (item[0] is None) == (lane == "parent")]
    assert selected_calls == [(None if lane == "parent" else "unknown", size, None) for size in (50, 12)]
    assert not summary.fetch_failed
    assert len(retained) == 2  # The rejected request is never a page or traversal proof.


@pytest.mark.parametrize("case", ["saved_cursor", "accepted_empty_page", "rate_limit", "known_zero_recovery"])
def test_size_binding_and_recovery_survive_reply_dispatch(monkeypatch, case):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PAGE_SIZE", "50")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_COMMENTS_DECODE_RETRY_LIMIT", "0")
    monkeypatch.setattr(fetcher, "_post_child_comments_graphql_doc_attempts", lambda: [("NativeChild", "doc")])
    instance = _build_fetcher()
    instance._pace_api_requests = AsyncMock(return_value=True)
    parent = _comment("parent")
    parent.reply_count = parent.child_comment_count = 0 if case == "known_zero_recovery" else None
    client = MagicMock()
    client.cookies = httpx.Cookies()
    request = httpx.Request("POST", "https://www.instagram.com/graphql/query/")
    responses = []
    if case == "accepted_empty_page":
        responses.append(
            httpx.Response(200, json=public.page([], child=True, more=True, after="tail"), request=request)
        )
    responses.append(
        httpx.Response(
            429 if case == "rate_limit" else 200 if case == "known_zero_recovery" else 400,
            json=public.page([], child=True) if case == "known_zero_recovery" else {"message": "Rejected"},
            request=request,
        )
    )
    client.post = AsyncMock(side_effect=responses)
    lane = (
        {
            "pending": True,
            "relay_doc_id": "doc",
            "relay_friendly_name": "NativeChild",
            "page_size": 50,
            "after": "saved-tail",
            "seen_cursors": ["saved-tail"],
        }
        if case == "saved_cursor"
        else {"technical_failure": True}
        if case == "known_zero_recovery"
        else {}
    )
    try:
        metadata = asyncio.run(
            instance._fetch_public_relay_child_comments_for_status_only(
                public_client=client,
                shortcode="fixture",
                post_url="https://www.instagram.com/p/fixture/",
                media_id="123",
                comments=[parent],
                graphql_headers={},
                common_body={},
                target_count=None,
                max_comments=0,
                strict_native_exhaustion=True,
                durable_resume={"parent": lane},
            )
        )
        variables = [json.loads(call.kwargs["data"]["variables"]) for call in client.post.await_args_list]
        assert all(item["first"] == 50 for item in variables)
        assert len(variables) == (2 if case == "accepted_empty_page" else 1)
        if case == "saved_cursor":
            assert variables[0]["after"] == "saved-tail"
        if case == "accepted_empty_page":
            assert variables[1]["after"] == "tail"
        if case == "known_zero_recovery":
            assert metadata["completed_parent_ids"] == ["parent"]
        assert "graphql_child_relay_page_size_downgrade" not in instance.runtime_metadata["retry_reason_counts"]
    finally:
        asyncio.run(instance.aclose())
        instance._parser.session.close()


@pytest.mark.parametrize("reject_at", [1, 2, 3])
def test_rejected_admission_never_increments_local_send_count(monkeypatch, reject_at):
    calls, admissions = [], []

    def handler(request):
        calls.append(request.method)
        if request.method == "GET":
            return public.bootstrap()
        return httpx.Response(200, json=public.page([public.actual_parent_shape("unknown")]))

    def admit():
        admissions.append(True)
        if len(admissions) == reject_at:
            raise RuntimeError("fixture rejected admission")

    source, instances = public.source(monkeypatch, handler, expected=None)
    with pytest.raises(RuntimeError, match="fixture rejected admission"):
        source(shortcode="fixture", checkpoint={}, source_observer=lambda *args: None, pre_source_request=admit)
    assert len(calls) == instances[0]._request_count == reject_at - 1


@pytest.mark.parametrize("slow_kind", ["relay_bootstrap", "graphql_comments", "graphql_replies"])
def test_two_workers_share_mixed_send_spacing_after_slow_admission(monkeypatch, slow_kind):
    """Root/bootstrap/reply sends share a clock in both directions across workers."""
    interval = 0.025
    shared_lock = threading.Lock()
    state_lock = threading.Lock()
    state = {"last_send": 0.0, "finished": 0, "admissions": 0}
    starts, turns = [], []
    first_reply_started = threading.Event()

    class FakeTurn:
        def __init__(self, *, key, delay_seconds, deadline, mode):
            self.delay = delay_seconds
            self.cancelled = threading.Event()
            self.acquired = False
            self.finished = False
            turns.append(self)

        def acquire(self):
            while not self.cancelled.is_set():
                if shared_lock.acquire(timeout=0.005):
                    self.acquired = True
                    break
            if not self.acquired:
                raise reply_scheduler.ReplyDeadlineExceeded("fixture canceled turn")
            remaining = state["last_send"] + self.delay - time.monotonic()
            if remaining > 0:
                self.cancelled.wait(remaining)
            if self.cancelled.is_set():
                raise reply_scheduler.ReplyDeadlineExceeded("fixture canceled turn")

        def cancel(self):
            self.cancelled.set()

        def finish(self, *, sent):
            if self.finished:
                return
            self.finished = True
            if self.acquired:
                if sent:
                    # A queued finish task must run after transport's first await.
                    assert len(starts) > state["finished"]
                    state["finished"] += 1
                    state["last_send"] = time.monotonic()
                self.acquired = False
                shared_lock.release()

    module = ModuleType("trr_backend.socials.instagram.comments_scrapling.reply_dispatch_turn")
    module.DispatchTurn = FakeTurn  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setitem(sys.modules, module.__name__, module)

    async def handler(request):
        if request.method == "GET":
            starts.append(("relay_bootstrap", time.monotonic()))
            return public.bootstrap()
        _, variables = public.body(request)
        if variables.get("parent_comment_id") is None:
            starts.append(("graphql_comments", time.monotonic()))
            return httpx.Response(200, json=public.page([public.actual_parent_shape("parent")]))
        starts.append(("graphql_replies", time.monotonic()))
        first_reply_started.set()
        await asyncio.sleep(0.01)
        return httpx.Response(200, json=public.page([], child=True))

    source, instances = public.source(monkeypatch, handler, expected=1)
    original_factory = source._factory

    def factory(**kwargs):
        original_before = kwargs["before_source_request"]

        def before(kind):
            with state_lock:
                state["admissions"] += 1
                slow = kind == slow_kind and not state.get("slow_admission_seen")
                if slow:
                    state["slow_admission_seen"] = True
            if slow:
                time.sleep(interval * 3)
            return original_before(kind)

        before.__dict__.update(original_before.__dict__)
        kwargs["before_source_request"] = before
        instance = original_factory(**kwargs)
        instance._api_delay_seconds = instance._global_api_delay_seconds = interval
        instance._pace_api_requests = AsyncMock(side_effect=AssertionError("Strict source reserved old pacing clock"))
        return instance

    source._factory = factory

    def worker(shortcode):
        if shortcode == "fixtureB":
            assert first_reply_started.wait(timeout=2)
        return source(
            shortcode=shortcode,
            checkpoint={},
            source_observer=lambda *args: None,
            pre_source_request=lambda: None,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker, shortcode) for shortcode in ("fixtureA", "fixtureB")]
        summaries = [future.result(timeout=3) for future in futures]
    assert all(not summary.fetch_failed for summary in summaries)
    assert len(starts) == state["finished"] == state["admissions"] == 6
    assert [kind for kind, _ in starts] == ["relay_bootstrap", "graphql_comments", "graphql_replies"] * 2
    assert all(later[1] - earlier[1] >= interval * 0.9 for earlier, later in zip(starts, starts[1:], strict=False))
    assert all(turn.finished and not turn.acquired for turn in turns)
    assert sum(instance._request_count for instance in instances) == 6
    for instance in instances:
        instance._pace_api_requests.assert_not_awaited()


@pytest.mark.parametrize("failure", ["queued_cancellation", "admission_deadline"])
def test_strict_root_turn_cleanup_never_records_an_unsent_request(monkeypatch, failure):
    acquired = threading.Event()
    cancelled = threading.Event()
    finished = []

    class FakeTurn:
        def __init__(self, **kwargs):
            pass

        def acquire(self):
            acquired.set()
            if failure == "queued_cancellation":
                assert cancelled.wait(timeout=2)
                raise reply_scheduler.ReplyDeadlineExceeded("fixture canceled acquisition")

        def cancel(self):
            cancelled.set()

        def finish(self, *, sent):
            finished.append(sent)

    module = ModuleType("trr_backend.socials.instagram.comments_scrapling.reply_dispatch_turn")
    module.DispatchTurn = FakeTurn  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    instance = _build_fetcher()
    instance._prepare_credentials = MagicMock()
    permit = object()

    def admit(_kind):
        time.sleep(0.025)
        return permit

    instance._before_source = MagicMock(side_effect=admit)
    hooks = SimpleNamespace(deadline=None, mark_sent=MagicMock(), release_unsent=MagicMock())
    instance._before_source_request = hooks  # pyright: ignore[reportAttributeAccessIssue]
    client = MagicMock()
    client.get = AsyncMock()

    async def run():
        task = asyncio.create_task(
            instance._send_strict_public_relay_request(
                client,
                "get",
                "https://www.instagram.com/p/fixture/",
                kind="relay_bootstrap",
                deadline=time.monotonic() + (0.01 if failure == "admission_deadline" else 2),
            )
        )
        if failure == "queued_cancellation":
            assert await asyncio.to_thread(acquired.wait, 1)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(reply_scheduler.ReplyDeadlineExceeded):
                await task
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]

    try:
        asyncio.run(run())
        assert finished == [False]
        assert instance._request_count == 0
        client.get.assert_not_awaited()
        hooks.mark_sent.assert_not_called()
        if failure == "admission_deadline":
            hooks.release_unsent.assert_called_once_with(permit)
        else:
            instance._before_source.assert_not_called()
            hooks.release_unsent.assert_not_called()
    finally:
        asyncio.run(instance.aclose())
        instance._parser.session.close()


@pytest.mark.parametrize("kind", ["relay_bootstrap", "graphql_comments"])
@pytest.mark.parametrize(
    "failure", ["transient", "ambiguous_commit", "persistent", "turn_cleanup", "persistent_with_cleanup_failure"]
)
def test_actual_root_helper_retries_exact_refund_state(monkeypatch, kind, failure):
    turns, closes = [], []

    class FakeTurn:
        def __init__(self, **kwargs):
            self.closed = False
            turns.append(self)

        def acquire(self):
            pass

        def cancel(self):
            pass

        def finish(self, *, sent):
            closes.append(sent)
            if failure == "persistent_with_cleanup_failure" or (failure == "turn_cleanup" and len(closes) == 1):
                raise RuntimeError("fixture turn cleanup failed")
            self.closed = True

    module = ModuleType("trr_backend.socials.instagram.comments_scrapling.reply_dispatch_turn")
    module.DispatchTurn = FakeTurn  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    instance = _build_fetcher()
    instance._prepare_credentials = MagicMock()
    permit = object()
    instance._before_source = MagicMock(return_value=permit)
    journal = {"charged": 1, "attempts": 0}

    def release(received):
        assert received is permit
        journal["attempts"] += 1
        if failure.startswith("persistent") or (failure == "transient" and journal["attempts"] == 1):
            raise RuntimeError("fixture refund unavailable")
        charged = journal["charged"]
        journal["charged"] = 0  # Idempotent receipt: an ambiguous retry can return False.
        if failure == "ambiguous_commit" and journal["attempts"] == 1:
            raise RuntimeError("fixture commit acknowledgment lost")
        return bool(charged)

    hooks = SimpleNamespace(deadline=None, mark_sent=MagicMock(), release_unsent=MagicMock(side_effect=release))
    instance._before_source_request = hooks  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setattr(fetcher, "_deadline_remaining_seconds", lambda _deadline: 0)
    client = MagicMock()
    client.get = client.post = AsyncMock()
    try:
        expected_error = (
            reply_scheduler.ReplyDiscardUnresolved
            if failure.startswith("persistent")
            else (reply_scheduler.ReplyDeadlineExceeded)
        )
        with pytest.raises(expected_error) as raised:
            asyncio.run(
                instance._send_strict_public_relay_request(
                    client,
                    "get" if kind == "relay_bootstrap" else "post",
                    "https://www.instagram.com/graphql/query/",
                    kind=kind,
                    deadline=time.monotonic() + 1,
                )
            )
        assert instance._request_count == journal["charged"] == int(failure.startswith("persistent"))
        assert journal["attempts"] == (1 if failure == "turn_cleanup" else 2)
        assert closes == ([False, False] if failure == "turn_cleanup" else [False])
        hooks.mark_sent.assert_not_called()
        client.get.assert_not_awaited()
        if failure.startswith("persistent"):
            error = raised.value
            assert len(error.failures) == 2 and len(error.descriptors) == 1  # pyright: ignore[reportAttributeAccessIssue]
            assert error.descriptors[0].request_permit is permit and error.descriptors[0].source_kind == kind  # pyright: ignore[reportAttributeAccessIssue]
            assert error.admission_states[0]["permit"] is permit  # pyright: ignore[reportAttributeAccessIssue]
            assert error.admission_states[0]["local_corrected"] is False  # pyright: ignore[reportAttributeAccessIssue]
            if failure == "persistent_with_cleanup_failure":
                assert len(error.cleanup_failures) == 1  # pyright: ignore[reportAttributeAccessIssue]
            assert "https://" not in str(error)
        if failure != "persistent_with_cleanup_failure":
            assert turns[0].closed
    finally:
        asyncio.run(instance.aclose())
        instance._parser.session.close()


def test_none_permits_and_no_release_hook_remain_separate_admissions():
    instance = _build_fetcher()
    instance._before_source_request = SimpleNamespace(release_unsent=None)  # pyright: ignore[reportAttributeAccessIssue]
    instance._request_count = 2
    turns = [MagicMock(), MagicMock()]
    states = [{"permit": None, "local_corrected": False, "turn": turn} for turn in turns]
    try:
        for state in states:
            instance._refund_strict_public_admission(state)
            instance._refund_strict_public_admission(state)
        assert instance._request_count == 0
        for turn in turns:
            turn.finish.assert_called_once_with(sent=False)
    finally:
        asyncio.run(instance.aclose())
        instance._parser.session.close()


def test_actual_child_discard_retries_turn_close_without_repeating_local_refund(monkeypatch):
    closes = []

    class FakeTurn:
        def __init__(self, **kwargs):
            pass

        def acquire(self):
            pass

        def cancel(self):
            pass

        def finish(self, *, sent):
            closes.append(sent)
            if len(closes) == 1:
                raise RuntimeError("fixture close acknowledgment lost")

    module = ModuleType("trr_backend.socials.instagram.comments_scrapling.reply_dispatch_turn")
    module.DispatchTurn = FakeTurn  # pyright: ignore[reportAttributeAccessIssue]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    transport = AsyncMock(side_effect=AssertionError("Unsent descriptor reached transport"))
    monkeypatch.setattr(reply_http, "fetch_reply", transport)
    instance = _build_fetcher()
    instance._public_only = True
    instance._prepare_credentials = MagicMock()
    permit = object()
    hooks = SimpleNamespace(deadline=time.monotonic() + 1, mark_sent=MagicMock(), release_unsent=MagicMock())
    instance._before_source_request = hooks  # pyright: ignore[reportAttributeAccessIssue]

    def admit(_kind):
        hooks.deadline = time.monotonic() - 1
        return permit

    instance._before_source = MagicMock(side_effect=admit)
    parent = _comment("parent")
    parent.reply_count = parent.child_comment_count = None
    client = MagicMock()
    client.cookies = httpx.Cookies()
    try:
        metadata = asyncio.run(
            instance._fetch_public_relay_child_comments_for_status_only(
                public_client=client,
                shortcode="fixture",
                post_url="https://www.instagram.com/p/fixture/",
                media_id="123",
                comments=[parent],
                graphql_headers={},
                common_body={},
                target_count=None,
                max_comments=0,
                strict_native_exhaustion=True,
            )
        )
        assert metadata["native_complete"] is False
        assert instance._request_count == 0
        assert closes == [False, False]
        hooks.release_unsent.assert_called_once_with(permit)
        hooks.mark_sent.assert_not_called()
        transport.assert_not_awaited()
    finally:
        asyncio.run(instance.aclose())
        instance._parser.session.close()
