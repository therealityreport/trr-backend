"""Production wrapper metadata and per-response admission fencing."""

import time
from datetime import UTC, datetime
from types import SimpleNamespace

from tests.ingestion.test_comments import database as database_fixture
from trr_backend.ingestion.comment_transport import PublicRelayCommentSource
from trr_backend.ingestion.composition import _GatedSource
from trr_backend.ingestion.identity_policy import RequestPermit
from trr_backend.ingestion.reply_context import request_permit

database = database_fixture


class Gate:
    def __init__(self):
        self.request_count = 0
        self.deadline = time.monotonic() + 10
        self.successes = []
        self.failures = []

    def before_request(self):
        self.request_count += 1
        return RequestPermit("public", self.request_count - 1, self.request_count, None, "synthetic-owner")

    def record_success(self, permit=None):
        self.successes.append(permit)

    def finish_failure(self, code=None, **kwargs):
        self.failures.append((code, kwargs))


def test_full_public_wrapper_chain_preserves_metadata_and_exact_permits():
    gate = Gate()

    def clock():
        return datetime(2026, 1, 1, tzinfo=UTC)

    def before():
        pass

    before.run_id = "synthetic-run"  # pyright: ignore[reportFunctionMemberAccess]
    before.run_generation = 3  # pyright: ignore[reportFunctionMemberAccess]
    before.owner_generation = 7  # pyright: ignore[reportFunctionMemberAccess]
    before.observation_clock = clock  # pyright: ignore[reportFunctionMemberAccess]
    before.deadline = time.monotonic() + 3  # pyright: ignore[reportFunctionMemberAccess]

    class Fetcher:
        def __init__(self, **kwargs):
            self.before = kwargs["before_source_request"]
            self.observe = kwargs["source_payload_observer"]

        async def fetch_comments_for_shortcode(self, *args, **kwargs):
            for name in ("run_id", "run_generation", "owner_generation", "observation_clock", "deadline"):
                assert getattr(self.before, name) == getattr(before, name)
            first = self.before("graphql_replies")
            second = self.before("graphql_replies")
            assert first.request_number == 1 and second.request_number == 2
            # Complete the older request after admission of the newer policy
            # generation. Its response must still acknowledge the first permit.
            for permit in (second, first):
                token = request_permit.set(permit)
                try:
                    self.observe("graphql_replies", {"data": {}})
                finally:
                    request_permit.reset(token)
            return SimpleNamespace(
                comments=[],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                reported_comment_count=None,
                top_level_checkpoint={},
                reply_checkpoints=[],
            )

        async def aclose(self):
            pass

    source = _GatedSource(PublicRelayCommentSource(fetcher_factory=Fetcher), gate, failure_code="comment_fetch_failed")
    source(shortcode="fixture", checkpoint={}, pre_source_request=before, source_observer=lambda *args: None)
    assert [permit.request_number for permit in gate.successes] == [2, 1]
    assert request_permit.get() is None


def test_gate_deadline_clamps_longer_source_budget():
    gate = Gate()
    captured = []

    def before():
        pass

    before.deadline = time.monotonic() + 300  # pyright: ignore[reportFunctionMemberAccess]

    def source(**callbacks):
        captured.append(callbacks["pre_source_request"].deadline)
        return SimpleNamespace(fetch_failed=False, auth_failed=False)

    _GatedSource(source, gate, failure_code="comment_fetch_failed")(
        pre_source_request=before, source_observer=lambda *args: None
    )
    assert captured == [gate.deadline]


def test_activity_uses_physical_owner_deadline_and_preserves_historical_failure(database):
    from tests.ingestion import test_comments as existing
    from trr_backend.ingestion.comments import CommentActivity

    store, tx, args = existing.setup(database)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set checkpoint=%s::jsonb where run_id=%s",
            ('{"last_error_code":"activity_failed"}', args["run_id"]),
        )
    captured = []

    def source(**callbacks):
        before = callbacks["pre_source_request"]
        assert before.run_id == args["run_id"] and before.run_generation == args["generation"]
        # The evidence clock is historical, while reservation expiry is current.
        remaining = before.deadline - time.monotonic()
        assert 0 < remaining < 600
        captured.append(before.owner_generation)
        return existing.source([existing.NATIVE["rest"]])(**callbacks)

    result = CommentActivity(store, source, clock=lambda: existing.OBSERVED).execute(**args)
    diagnostic = result["checkpoint"]["worker_diagnostics"]
    assert diagnostic["last_failure"] == {
        "code": "activity_failed",
        "exception_class": "UnknownError",
        "category": "unknown",
        "observed_at": None,
        "owner_generation": None,
    }
    assert diagnostic["last_successful_page_at"] == existing.OBSERVED.isoformat()
    assert diagnostic["last_successful_owner_generation"] == captured[0]
    assert diagnostic["committed_pages_since_failure"] == 1
    assert result["checkpoint"]["last_error_code"] == "activity_failed"


def test_parse_failure_retains_own_permit_after_response_context_unwinds():
    import pytest

    from trr_backend.ingestion.comments import CommentSourceError
    from trr_backend.ingestion.domain import Failure
    from trr_backend.ingestion.post_transport import SourceFailure

    gate = Gate()

    def finish(code=None, **kwargs):
        gate.failures.append((code, kwargs))
        return Failure(code)  # pyright: ignore[reportArgumentType]

    gate.finish_failure = finish  # pyright: ignore[reportAttributeAccessIssue]

    def source(**callbacks):
        first = callbacks["pre_source_request"]()
        callbacks["pre_source_request"]()
        token = request_permit.set(first)
        try:
            callbacks["source_observer"]("graphql_replies", {"data": {}})
        finally:
            request_permit.reset(token)

    def malformed(*args):
        raise CommentSourceError("synthetic malformed response")

    with pytest.raises(SourceFailure):
        _GatedSource(source, gate, failure_code="comment_fetch_failed")(
            pre_source_request=lambda: None, source_observer=malformed
        )
    assert gate.failures[0][1]["permit"].request_number == 1
    assert request_permit.get() is None


def test_terminal_read_failure_uses_request_five_while_six_is_outstanding(monkeypatch):
    import asyncio

    import httpx
    import pytest

    from tests.ingestion import test_public_comment_transport as public
    from trr_backend.ingestion.domain import Failure
    from trr_backend.ingestion.post_transport import SourceFailure

    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    gate = Gate()
    calls, cancelled = [], []
    peer_tail_started = None

    def finish(code=None, **kwargs):
        gate.failures.append((code, kwargs))
        return Failure(code)  # pyright: ignore[reportArgumentType]

    gate.finish_failure = finish  # pyright: ignore[reportAttributeAccessIssue]

    async def handler(request):
        nonlocal peer_tail_started
        if peer_tail_started is None:
            peer_tail_started = asyncio.Event()
        if request.method == "GET":
            calls.append("bootstrap")
            return public.bootstrap()
        _, variables = public.body(request)
        parent, cursor = variables.get("parent_comment_id"), variables.get("after")
        calls.append((parent, cursor))
        if parent is None:
            return httpx.Response(200, json=public.page([public.actual_parent_shape(p) for p in ("p1", "p2")]))
        if cursor is None:
            if parent == "p2":
                await asyncio.sleep(0.005)
            return httpx.Response(
                200, json=public.page([public.node(parent + "-r")], child=True, more=True, after=parent + "-tail")
            )
        if parent == "p1":
            await peer_tail_started.wait()
            raise httpx.ReadError("synthetic retained request-five read failure")
        peer_tail_started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append("p2-tail")
            raise
        pytest.fail("peer request survived terminal source failure")

    fetch, _ = public.source(monkeypatch, handler, expected=None)
    with pytest.raises(SourceFailure):
        _GatedSource(fetch, gate, failure_code="comment_fetch_failed")(
            shortcode="fixture",
            checkpoint={},
            pre_source_request=lambda: None,
            source_observer=lambda *args: None,
        )
    assert calls == ["bootstrap", (None, None), ("p1", None), ("p2", None), ("p1", "p1-tail"), ("p2", "p2-tail")]
    assert gate.request_count == 6 and cancelled == ["p2-tail"]
    assert gate.failures[0][1]["permit"].request_number == 5
