"""Public technical reply failures retain proof without starving healthy branches."""

import asyncio

import httpx
import pytest

from tests.ingestion import test_comment_resume as resume
from tests.ingestion import test_comments as existing
from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.comments import CommentActivity

database = existing.database
deny_unmocked_http = resume.deny_unmocked_http


@pytest.mark.parametrize("concurrency", [1, 2])
def test_generic_child_failure_is_durable_and_does_not_starve_peer(database, monkeypatch, concurrency):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", str(concurrency))
    store, tx, args = public.setup(database, seed_missing=True)
    calls = []

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        fields, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        calls.append((parent, fields["doc_id"]))
        if parent is None:
            return httpx.Response(
                200, json=public.page([public.actual_parent_shape("bad"), public.actual_parent_shape("good")])
            )
        if parent == "bad":
            return httpx.Response(
                200,
                json={"status": "fail", "errors": [{"severity": "CRITICAL", "message": "Generic technical failure"}]},
            )
        await asyncio.sleep(0.01)
        return httpx.Response(200, json=public.page([], child=True))

    fetch, instances = public.source(monkeypatch, handler, expected=2)
    result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    assert result["complete"] is False
    checkpoint = result["checkpoint"]
    failures = checkpoint["public_reply_failures"]
    assert set(failures) == {"bad"}
    failure = failures["bad"]
    assert failure["version"] == 1 and failure["code"] == "source_incomplete"
    assert failure["next_attempt_at"] and failure["evidence_id"] and failure["content_hash"]
    assert checkpoint["comment_pages"]["lanes"]["good"]["exhausted"] is True
    assert "bad" not in checkpoint["comment_pages"]["lanes"]
    assert checkpoint["comment_pages"]["observed"]["bad"]["expected_replies"] is None
    assert instances[0]._reply_scheduler_telemetry["committed_pages"] == 1
    receipts = existing.query(
        tx, "select parent_comment_id from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
    )
    assert {row["parent_comment_id"] for row in receipts} == {None, "good"}
    assert not existing.query(
        tx, "select is_missing from social.instagram_comments where post_id=%s and comment_id='old'", (args["post_id"],)
    )[0]["is_missing"]
    assert any(parent == "good" for parent, _ in calls)
    # A restart during the local delay leaves the failed native branch pending,
    # without re-sending it or changing its technical failure receipt.
    resume.retry(database, args)
    calls.clear()
    fetch, _ = public.source(monkeypatch, handler, expected=2)
    again = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    assert again["complete"] is False
    assert not calls
    assert again["checkpoint"]["public_reply_failures"] == failures


def test_document_fallback_clears_only_its_committed_parent_failure(database, monkeypatch):
    store, tx, args = public.setup(database)
    calls = []
    # Use two maintained documents; the generic error on the first must reach
    # the already-supported second document rather than its soft decode retry.
    monkeypatch.setattr(
        public.maintained,
        "_post_child_comments_graphql_doc_attempts",
        lambda: [("Children", "first"), ("ChildrenAlt", "36239935742272683")],
    )

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        fields, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        if parent is None:
            return httpx.Response(
                200, json=public.page([public.actual_parent_shape("bad"), public.actual_parent_shape("fixed")])
            )
        calls.append((parent, fields["doc_id"]))
        if parent == "bad" or fields["doc_id"] == "first":
            return httpx.Response(200, json={"status": "fail", "errors": [{"severity": "CRITICAL"}]})
        return httpx.Response(200, json=public.page([], child=True))

    fetch, _ = public.source(monkeypatch, handler, expected=2)
    result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    assert result["complete"] is False
    assert set(result["checkpoint"]["public_reply_failures"]) == {"bad"}
    assert "reply_access" not in result["checkpoint"]["comment_pages"]
    assert ("fixed", "first") in calls and ("fixed", "36239935742272683") in calls
    assert result["checkpoint"]["comment_pages"]["lanes"]["fixed"]["exhausted"]


def test_backoff_unchanged_failure_hold_and_resume_order():
    from datetime import timedelta
    from types import SimpleNamespace

    from trr_backend.ingestion.comment_transport import PublicRelayResume
    from trr_backend.ingestion.reply_failures import record_failure, resume_failure

    payload = {"parent_comment_id": "bad", "cursor_payload": {"relay_doc_id": "one"}}
    evidence = SimpleNamespace(content_hash="native-hash", captured_at=public.OBSERVED)
    previous = None
    for number, delay in enumerate((60, 120, 240), 1):
        previous = record_failure(
            previous,
            payload=payload,
            evidence=evidence,
            evidence_id="retained",
            owner_generation=9,
            now=public.OBSERVED,
        )
        assert previous["failure_count"] == number
        assert previous["next_attempt_at"] == (public.OBSERVED + timedelta(seconds=delay)).isoformat()
        assert previous["technical_hold"] == (number == 3)
    assert resume_failure(previous, now=public.OBSERVED + timedelta(days=1))["deferred"]
    checkpoint = {
        "public_reply_failures": {"bad": previous},
        "comment_pages": {"observed": {"bad": {"expected_replies": 0}}, "lanes": {}},
    }
    resumed = PublicRelayResume.from_checkpoint(
        checkpoint, [SimpleNamespace(comment_id="bad", replies=[])], now=public.OBSERVED + timedelta(days=1)
    )
    assert resumed.replies["bad"]["deferred"] and not resumed.replies["bad"]["pending"]


def test_gated_source_records_exact_nonterminal_failed_permit(database, monkeypatch):
    from trr_backend.ingestion.composition import _GatedSource

    class Gate:
        request_count = 0

        def __init__(self):
            self.events = []

        def before_request(self):
            self.request_count += 1
            return self.request_count

        def record_success(self, *, permit=None):
            self.events.append(("success", permit))

        def record_failure(self, code, *, permit=None):
            self.events.append((code, permit))

        def finish_failure(self, *args, **kwargs):
            self.events.append(("terminal", args))

        def finish_success(self):
            pass

    store, _, args = public.setup(database)

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        if parent is None:
            return httpx.Response(
                200, json=public.page([public.actual_parent_shape("bad"), public.actual_parent_shape("good")])
            )
        if parent == "bad":
            return httpx.Response(200, json={"status": "fail", "errors": [{"severity": "CRITICAL"}]})
        return httpx.Response(200, json=public.page([], child=True))

    source, _ = public.source(monkeypatch, handler, expected=2)
    gate = Gate()
    wrapped = _GatedSource(source, gate, failure_code="comment_fetch_failed")
    result = CommentActivity(store, wrapped, clock=lambda: public.OBSERVED).execute(**args)
    assert result["complete"] is False
    failed = [permit for code, permit in gate.events if code == "source_incomplete"]
    assert failed and len(failed) == len(set(failed)) and all(permit > 2 for permit in failed)
    assert not any(code == "terminal" for code, _ in gate.events)
    assert not set(failed) & {permit for code, permit in gate.events if code == "success"}
    # Worker unresolved-target cleanup releases the gate without attributing
    # another failure to the most recent healthy sibling permit.
    wrapped.finish_failure()
    assert gate.events[-1] == ("terminal", ())


@pytest.mark.parametrize("change", ["auth", "rate", "retry_after", "reply_access", "root", "parse", "other_adapter"])
def test_only_exact_generic_public_child_failures_are_isolated(change):
    from trr_backend.ingestion.reply_failures import isolated_reply_failure

    payload = {
        "parent_comment_id": "parent",
        "status": "fail",
        "errors": [{"severity": "CRITICAL"}],
        "cursor_payload": {
            "adapter_family": "instagram_public_relay",
            "adapter_version": 1,
            "relay_doc_id": "doc",
            "relay_friendly_name": "Children",
            "sort_order": "popular",
            "page_size": 12,
            "request_cursor": None,
        },
    }
    assert isolated_reply_failure("graphql_replies", payload)
    kind = "graphql_replies"
    if change == "auth":
        payload["message"] = "login_required"
    elif change == "rate":
        payload["message"] = "rate limit exceeded"
    elif change == "retry_after":
        payload["retry_after"] = 180
    elif change == "reply_access":
        payload["errors"] = [{"code": 1675004}]
    elif change == "root":
        kind = "graphql_comments"
    elif change == "parse":
        payload.pop("status")
        payload.pop("errors")
        payload["data"] = {public.CHILD: {"edges": [{"node": "broken"}]}}
    else:
        payload["cursor_payload"]["adapter_family"] = "authenticated"
    assert not isolated_reply_failure(kind, payload)


def test_metadata_checkpoint_uses_live_owner_generation_and_checkpoint_cas(database):
    from trr_backend.ingestion.domain import Conflict, LostOwnership, Stage

    store, tx, args = public.setup(database)
    owned = {key: value for key, value in args.items() if key != "post_id"}
    owned["stage"] = Stage.COMMENTS
    target = store.acquire(**owned)
    activity = CommentActivity(store)
    expected = target["checkpoint"]
    with pytest.raises(LostOwnership):
        activity._persist_reply_failure(
            owned,
            expected_checkpoint=expected,
            checkpoint={"bad": True},
            owner_generation=target["owner_generation"] + 1,
        )
    with pytest.raises(Conflict):
        activity._persist_reply_failure(
            owned,
            expected_checkpoint={"stale": True},
            checkpoint={"bad": True},
            owner_generation=target["owner_generation"],
        )
    actual = {**expected, "public_reply_failures": {"parent": {"version": 1}}}
    activity._persist_reply_failure(
        owned, expected_checkpoint=expected, checkpoint=actual, owner_generation=target["owner_generation"]
    )
    row = existing.query(
        tx,
        "select checkpoint,owner_token,owner_generation from social.instagram_ingestion_targets where run_id=%s",
        (args["run_id"],),
    )[0]
    assert row["checkpoint"] == actual and str(row["owner_token"]) == args["owner_token"]
    assert row["owner_generation"] == target["owner_generation"]
    assert not existing.query(
        tx, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
    )
    assert not existing.query(tx, "select * from social.instagram_comments where post_id=%s", (args["post_id"],))


def test_outstanding_failure_blocks_known_zero_settlement():
    from trr_backend.ingestion.comments import CommentFetchSummary, _settlement

    state = {"expected": 0, "observed": {}, "lanes": {"__top__": {"exhausted": True}}}
    assert _settlement(state, CommentFetchSummary(expected_count=0, full_observation=True))
    assert (
        _settlement(
            state, CommentFetchSummary(expected_count=0, full_observation=True, public_reply_failures=("pending",))
        )
        is None
    )


def test_partial_child_page_failure_still_uses_document_fallback(database, monkeypatch):
    store, _, args = public.setup(database)
    monkeypatch.setattr(
        public.maintained,
        "_post_child_comments_graphql_doc_attempts",
        lambda: [("Children", "first"), ("ChildrenAlt", "36239935742272683")],
    )
    calls = []

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        fields, variables = public.body(request)
        parent, cursor = variables.get("parent_comment_id"), variables.get("after")
        if parent is None:
            return httpx.Response(200, json=public.page([public.actual_parent_shape("parent")]))
        calls.append((fields["doc_id"], cursor))
        if fields["doc_id"] == "first":
            if cursor is None:
                return httpx.Response(
                    200, json=public.page([public.node("reply")], child=True, more=True, after="tail")
                )
            return httpx.Response(200, json={"status": "fail", "errors": [{"severity": "CRITICAL"}]})
        return httpx.Response(200, json=public.page([public.node("reply")], child=True))

    fetch, _ = public.source(monkeypatch, handler, expected=2)
    result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    assert result["complete"] is True
    assert calls == [("first", None), ("first", "tail"), ("36239935742272683", None)]
    assert not result["checkpoint"]["public_reply_failures"]


def test_resumed_healthy_parent_precedes_eligible_failed_branch(database, monkeypatch):
    from datetime import timedelta

    store, _, args = public.setup(database)
    persist = CommentActivity._persist_reply_failure
    calls = []

    def interrupted(self, *positional, **kwargs):
        persist(self, *positional, **kwargs)
        raise RuntimeError("fixture crash after fenced metadata commit")

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        if parent is None:
            return httpx.Response(
                200, json=public.page([public.actual_parent_shape("bad"), public.actual_parent_shape("good")])
            )
        calls.append(parent)
        if parent == "bad":
            return httpx.Response(200, json={"status": "fail", "errors": [{"severity": "CRITICAL"}]})
        return httpx.Response(200, json=public.page([], child=True))

    fetch, _ = public.source(monkeypatch, handler, expected=2)
    monkeypatch.setattr(CommentActivity, "_persist_reply_failure", interrupted)
    with pytest.raises(RuntimeError, match="fixture crash"):
        CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    monkeypatch.setattr(CommentActivity, "_persist_reply_failure", persist)
    resume.retry(database, args)
    calls.clear()
    fetch, _ = public.source(monkeypatch, handler, expected=2)
    result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED + timedelta(minutes=5)).execute(**args)
    assert not result["complete"] and calls[0] == "good" and "bad" in calls[1:]
