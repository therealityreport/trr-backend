"""Real anonymous fetcher/adapters and disposable PostgreSQL, never live HTTP."""

import asyncio
from datetime import timedelta

import httpx
import pytest

from tests.ingestion import test_comment_resume as resume
from tests.ingestion import test_comments as existing
from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.comments import CommentActivity
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.socials.instagram.comments_scrapling import reply_http

database = existing.database
deny_unmocked_http = resume.deny_unmocked_http


def test_rejected_retained_payload_is_not_a_committed_page_metric(database, monkeypatch):
    store, tx, args = public.setup(database)
    child_requests = []

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        if parent is None:
            return httpx.Response(200, json=public.page([public.actual_parent_shape("parent")]))
        child_requests.append(parent)
        return httpx.Response(200, json={"errors": [{"code": 1675004, "message": "Rate limit exceeded"}]})

    fetch, instances = public.source(monkeypatch, handler, expected=2)
    result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    assert result["outcome"] == "unresolved" and child_requests
    metrics = instances[0]._reply_scheduler_telemetry
    assert metrics["committed_pages"] == 0
    assert metrics["reply_access_rejected_responses"] == len(child_requests)
    receipts = existing.query(
        tx, "select checkpoint from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
    )
    assert len(receipts) == 1  # Only the root page committed; rejected replies were retained separately.


@pytest.fixture(autouse=True)
def assert_transport_resources_are_drained(monkeypatch):
    original_run = asyncio.run
    original_spool = reply_http.DiskResponseSpool
    spools = []

    def tracked_spool():
        spool = original_spool()
        spools.append(spool)
        return spool

    def checked_run(coroutine):
        async def checked():
            try:
                return await coroutine
            finally:
                # Assert before asyncio.run's fallback task cancellation, so its
                # cleanup cannot hide a request/pacing task leaked by the source.
                assert not [
                    task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()
                ]

        return original_run(checked())

    monkeypatch.setattr(reply_http, "DiskResponseSpool", tracked_spool)
    monkeypatch.setattr(asyncio, "run", checked_run)
    yield
    assert all(spool.closed for spool in spools)


def saved_inventory(tx, post_id):
    return existing.query(
        tx,
        """select c.comment_id,c.username,c.user_id,c.text,c.likes,c.reply_count,c.is_reply,
          c.parent_comment_external_id,c.reply_depth,c.created_at,c.scraped_at,c.last_seen_at,
          c.is_missing,c.media_urls,c.raw_data,c.source_snapshot_type,p.comment_id as parent_native_id
        from social.instagram_comments c left join social.instagram_comments p on p.id=c.parent_comment_id
        where c.post_id=%s order by c.comment_id""",
        (post_id,),
    )


def checkpoint(tx, run_id):
    return existing.query(tx, "select checkpoint from social.instagram_ingestion_targets where run_id=%s", (run_id,))[
        0
    ]["checkpoint"]["comment_pages"]


def test_parallel_and_serial_exact_inventory_topology_attachments_counts_and_lanes(database, monkeypatch):
    runs = []
    for concurrency in (1, 2):
        monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", str(concurrency))
        store, tx, args = public.setup(database, seed_missing=True)
        active, peak = 0, 0
        calls, completions = [], []

        async def handler(request, calls=calls, completions=completions):
            nonlocal active, peak
            if request.method == "GET":
                return public.bootstrap()
            _, variables = public.body(request)
            parent, cursor = variables.get("parent_comment_id"), variables.get("after")
            calls.append((parent, cursor, variables["first"]))
            if parent is None:
                parents = [public.actual_parent_shape(key) for key in ("p1", "p2")]
                parents += [public.node("p3", 0, reply_count=2), public.node("zero", 0)]
                parents[0]["text"] = ""
                parents[0]["giphy_media_info"] = {
                    "id": "fixture-gif",
                    "images": {"fixed_height": {"url": "https://media.example/parent.gif"}},
                }
                return httpx.Response(200, json=public.page(parents))
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.02 if parent == "p1" else 0)
                if parent == "p2":
                    assert cursor is None
                    payload = public.page([], child=True)
                elif parent == "p1":
                    payload = (
                        public.page([public.node("r1", text="same reply")], child=True, more=True, after="p1-tail")
                        if cursor is None
                        else public.page([], child=True)
                    )
                else:
                    assert parent == "p3"
                    payload = (
                        public.page([public.node("r2", text="same reply")], child=True, more=True, after="p3-tail")
                        if cursor is None
                        else public.page(
                            [public.node("r3", sticker={"image_url": "https://media.example/sticker.webp"})], child=True
                        )
                    )
                completions.append((parent, cursor))
                return httpx.Response(200, json=payload)
            finally:
                active -= 1

        fetch, created = public.source(monkeypatch, handler, expected=7)
        result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
        assert result["complete"] is True and active == 0 and peak == concurrency
        assert all(call[2] == 12 for call in calls)
        assert not any(parent == "zero" for parent, _, _ in calls)
        lanes = checkpoint(tx, args["run_id"])
        assert lanes["observed"]["p1"]["expected_replies"] is None
        assert lanes["observed"]["p2"]["expected_replies"] is None
        assert lanes["observed"]["p3"]["expected_replies"] == 2
        assert lanes["settlement"] == {
            "basis": "count_parity",
            "expected_total": 7,
            "observed_total": 7,
            "count_gap": 0,
        }
        for parent in ("p1", "p2", "p3"):
            assert lanes["lanes"][parent]["exhausted"] is True
            assert lanes["lanes"][parent]["started_from_root"] is True
        inventory = saved_inventory(tx, args["post_id"])
        assert next(row for row in inventory if row["comment_id"] == "r1")["parent_native_id"] == "p1"
        assert next(row for row in inventory if row["comment_id"] == "r3")["media_urls"] == [
            "https://media.example/sticker.webp"
        ]
        assert created[0]._reply_scheduler_telemetry["max_outstanding"] <= concurrency
        assert next(row for row in inventory if row["comment_id"] == "old")["is_missing"] is True
        # The fixture's unobserved legacy row has database-default wall clocks.
        # Compare every field above for native rows and assert its lifecycle
        # separately rather than treating two fixture creation clocks as equal.
        runs.append(
            (
                [row for row in inventory if row["comment_id"] != "old"],
                {k: lanes[k] for k in ("observed", "lanes", "expected", "settlement")},
            )
        )
        if concurrency == 2:
            assert completions[0] == ("p2", None)
    assert runs[0] == runs[1]


def test_parallel_cookie_rotation_is_owner_applied_and_inflight_snapshots_are_isolated(database, monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    store, _, args = public.setup(database)
    cookies, calls = {}, []

    async def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent, cursor = variables.get("parent_comment_id"), variables.get("after")
        if parent is None:
            return httpx.Response(
                200, json=public.page([public.actual_parent_shape("p1"), public.actual_parent_shape("p2")])
            )
        cookies[(parent, cursor)] = request.headers.get("cookie", "")
        calls.append((parent, cursor))
        if parent == "p1" and cursor is None:
            return httpx.Response(
                200,
                json=public.page([], child=True, more=True, after="tail"),
                headers={"set-cookie": "csrftoken=rotated; Path=/"},
            )
        await asyncio.sleep(0.01)
        return httpx.Response(200, json=public.page([], child=True))

    fetch, _ = public.source(monkeypatch, handler, expected=2)
    result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    assert result["complete"] is True
    assert "csrftoken=anonymous" in cookies[("p1", None)]
    assert "csrftoken=anonymous" in cookies[("p2", None)]
    assert "csrftoken=rotated" in cookies[("p1", "tail")]
    assert calls.count(("p1", None)) == calls.count(("p2", None)) == 1


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_persistence_failure_preserves_exact_cursor_and_resumes_without_task_leaks(
    database, monkeypatch, concurrency, phase
):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", str(concurrency))
    store, tx, args = public.setup(database)
    original = CommentActivity._persist_page
    failed = False
    first_calls, resumed_calls = [], []
    attempt = 0
    transport_active = 0

    async def handler(request):
        nonlocal transport_active
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent, cursor = variables.get("parent_comment_id"), variables.get("after")
        (first_calls if attempt == 0 else resumed_calls).append((parent, cursor))
        if parent is None:
            return httpx.Response(200, json=public.page([public.node("p1", 2), public.actual_parent_shape("p2")]))
        transport_active += 1
        try:
            if parent == "p2":
                await asyncio.sleep(0.02)
                return httpx.Response(200, json=public.page([], child=True))
            return httpx.Response(
                200,
                json=public.page([public.node("r1")], child=True, more=True, after="tail")
                if cursor is None
                else public.page([public.node("r2")], child=True),
            )
        finally:
            transport_active -= 1

    def persist(self, *positional, **kwargs):
        nonlocal failed
        first_child = kwargs["page"].parent_comment_id == "p1" and kwargs["page"].next_cursor == "tail"
        if first_child and not failed:
            failed = True
            if phase == "after_commit":
                original(self, *positional, **kwargs)
            raise RuntimeError("fixture persistence failure")
        return original(self, *positional, **kwargs)

    monkeypatch.setattr(CommentActivity, "_persist_page", persist)
    fetch, _ = public.source(monkeypatch, handler, expected=4)
    activity = CommentActivity(store, fetch, clock=lambda: public.OBSERVED)
    with pytest.raises(RuntimeError, match="persistence failure"):
        activity.execute(**args)
    assert transport_active == 0 and ("p1", "tail") not in first_calls
    state = checkpoint(tx, args["run_id"])
    if phase == "before_commit":
        assert "p1" not in state["lanes"]
    else:
        assert state["lanes"]["p1"]["cursors"] == ["tail"]
    resume.retry(database, args)
    attempt = 1
    fetch, _ = public.source(monkeypatch, handler, expected=4)
    result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED + timedelta(hours=1)).execute(**args)
    assert result["complete"] is True and transport_active == 0
    assert not any(parent is None for parent, _ in resumed_calls)
    assert (("p1", None) in resumed_calls) == (phase == "before_commit")
    assert ("p1", "tail") in resumed_calls
    assert {row["comment_id"] for row in saved_inventory(tx, args["post_id"])} == {"p1", "p2", "r1", "r2"}


@pytest.mark.parametrize("failure", ["forbidden_cookie", "cooldown", "malformed", "unproved_pagination"])
def test_parallel_failed_response_never_commits_or_dispatches_its_tail(database, monkeypatch, failure):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
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
                200, json=public.page([public.actual_parent_shape("p1"), public.actual_parent_shape("p2")])
            )
        if parent == "p2":
            await asyncio.sleep(0.01)
            return httpx.Response(200, json=public.page([], child=True))
        if failure == "forbidden_cookie":
            return httpx.Response(
                200, json=public.page([], child=True), headers={"set-cookie": "sessionid=forbidden; Path=/"}
            )
        if failure == "cooldown":
            return httpx.Response(429, headers={"retry-after": "17"})
        if failure == "malformed":
            return httpx.Response(
                200,
                json={
                    "data": {
                        public.CHILD: {
                            "edges": [{"node": "invalid-native-node"}],
                            "page_info": {"has_next_page": False, "end_cursor": None},
                        }
                    }
                },
            )
        return httpx.Response(200, json=public.page([public.node("bad")], child=True, more=True))

    fetch, _ = public.source(monkeypatch, handler, expected=2)

    # Real CommentActivity exercises its own source-failure handler and checkpoint
    # fence. It must never settle or invent zero replies on any of these errors.
    try:
        result = CommentActivity(store, fetch, clock=lambda: public.OBSERVED).execute(**args)
    except (SourceFailure, ValueError):
        result = {"complete": False}
    assert result["complete"] is False
    assert not any(cursor is not None for _, cursor in calls)
    state = checkpoint(tx, args["run_id"])
    assert not state.get("settlement")
    rows = saved_inventory(tx, args["post_id"])
    assert next(row for row in rows if row["comment_id"] == "old")["is_missing"] is False
    if failure == "malformed":
        assert "bad" not in {row["comment_id"] for row in rows}
    elif failure == "unproved_pagination":
        # Valid native content remains saved even though the source failed to
        # provide a usable continuation. Retention never certifies exhaustion.
        assert "bad" in {row["comment_id"] for row in rows}
        assert not state["lanes"]["p1"]["exhausted"]
    else:
        assert "p1" not in state["lanes"]
