from __future__ import annotations

import copy
import json
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from email.utils import format_datetime
from typing import Any

import pytest
import requests

from trr_backend.repositories import reddit_transport as transport


class Response:
    def __init__(self, payload: Any, status: int = 200, headers: dict[str, str] | None = None) -> None:
        self.payload = payload
        self.status_code = status
        self.headers = headers or {}

    def json(self) -> Any:
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class Session:
    def __init__(self, *responses: Response | Exception) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def _call(self, method: str, url: str, **kwargs: Any) -> Response:
        self.calls.append({"method": method, "url": url, **kwargs})
        assert self.responses, "Unexpected HTTP request"
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get(self, url: str, **kwargs: Any) -> Response:
        return self._call("get", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Response:
        return self._call("post", url, **kwargs)


def listing(*children: Any) -> dict[str, Any]:
    return {"kind": "Listing", "data": {"children": list(children)}}


def comment(comment_id: str, parent: str = "t3_post1", **extra: Any) -> dict[str, Any]:
    return {"kind": "t1", "data": {"id": comment_id, "parent_id": parent, "body": "text", **extra}}


def more(*children: str, parent: str = "t3_post1") -> dict[str, Any]:
    return {"kind": "more", "data": {"children": list(children), "parent_id": parent}}


def initial(*children: Any) -> Response:
    return Response([listing({"kind": "t3", "data": {"id": "post1"}}), listing(*children)])


def expanded(*things: Any) -> Response:
    return Response({"json": {"errors": [], "data": {"things": list(things)}}})


@pytest.fixture(autouse=True)
def isolated_client_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_REQUIRE_OAUTH",
        "TRR_JOB_PLANE_MODE",
        "TRR_LONG_JOB_ENFORCE_REMOTE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("REDDIT_PAGE_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("REDDIT_FETCH_MAX_RETRIES", "2")
    monkeypatch.setattr(transport.time, "sleep", lambda _delay: None)


def oauth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDDIT_CLIENT_ID", "test-client")
    monkeypatch.setenv("REDDIT_CLIENT_SECRET", "test-secret")


def test_nested_more_expansion_deduplicates_and_preserves_parent_depth() -> None:
    session = Session(
        initial(comment("a", replies=listing(comment("b", "t1_a"), more("c", "c", parent="t1_a")))),
        expanded(comment("c", "t1_a", replies=listing(more("d", parent="t1_c"))), comment("b", "t1_a")),
        expanded(comment("d", "t1_c")),
    )
    lanes: list[tuple[str, str]] = []

    @contextmanager
    def guard(lane: str):
        lanes.append((lane, threading.current_thread().name))
        yield

    client = transport.RedditHttpClient(session=session, request_guard=guard)
    checkpoints: list[dict[str, Any]] = []
    result = client.get_comments("post1", on_checkpoint=checkpoints.append)
    assert result["complete"] is True
    assert result["source_unavailable"] is False
    assert result["unresolved_ids"] == []
    assert {row["reddit_comment_id"]: (row["parent_comment_id"], row["depth"]) for row in result["comments"]} == {
        "a": (None, 0),
        "b": ("a", 1),
        "c": ("a", 1),
        "d": ("c", 2),
    }
    assert [lane for lane, _thread in lanes] == ["request", "morechildren", "morechildren"]
    assert session.calls[1]["params"]["children"] == "c"
    assert checkpoints[0]["pending_children"] == [{"id": "c", "parent_id": "t1_a", "depth": 1}]
    assert checkpoints[-1]["pending_children"] == []
    assert json.loads(json.dumps(checkpoints[-1])) == checkpoints[-1]


def test_budget_checkpoint_resumes_without_initial_refetch() -> None:
    client = transport.RedditHttpClient(session=Session(initial(comment("a"), more("b"))), max_more_requests=0)
    first = client.get_comments("post1")
    assert first["complete"] is False
    assert first["unresolved_ids"] == ["b"]
    checkpoint = first["evidence"]["checkpoint"]
    preserved = copy.deepcopy(checkpoint)
    resumed = transport.RedditHttpClient(session=Session(expanded(comment("b")))).get_comments("post1", checkpoint)
    assert resumed["complete"] is True
    assert {row["reddit_comment_id"] for row in resumed["comments"]} == {"a", "b"}
    assert checkpoint == preserved


@pytest.mark.parametrize("response", [Response({}, 503), Response({}), expanded({"kind": "t1", "data": {}})])
def test_failed_expansion_preserves_all_pending_children(response: Response) -> None:
    session = Session(initial(comment("a"), more("b", "c")), response, response)
    checkpoints: list[dict[str, Any]] = []
    result = transport.RedditHttpClient(session=session).get_comments("post1", on_checkpoint=checkpoints.append)
    assert result["complete"] is False
    assert result["source_unavailable"] is False
    assert result["unresolved_ids"] == ["b", "c"]
    assert result["evidence"]["errors"]
    assert [child["id"] for child in checkpoints[-1]["pending_children"]] == ["b", "c"]


def test_empty_expansion_is_unresolved_and_is_not_retried_forever() -> None:
    session = Session(initial(more("a")), expanded())
    result = transport.RedditHttpClient(session=session).get_comments("post1")
    assert result["complete"] is False
    assert result["unresolved_ids"] == ["a"]
    assert result["evidence"]["errors"] == [{"reason": "children_not_returned"}]
    assert len(session.calls) == 2


def test_missing_children_remain_pending_while_returned_children_are_saved() -> None:
    result = transport.RedditHttpClient(session=Session(initial(more("a", "b")), expanded(comment("b")))).get_comments(
        "post1"
    )
    assert result["unresolved_ids"] == ["a"]
    assert [row["reddit_comment_id"] for row in result["comments"]] == ["b"]


def test_flat_expansion_order_does_not_change_comment_depth() -> None:
    result = transport.RedditHttpClient(
        session=Session(
            initial(comment("a"), more("b", parent="t1_a")),
            expanded(comment("c", "t1_b"), comment("b", "t1_a")),
        )
    ).get_comments("post1")
    assert {row["reddit_comment_id"]: row["depth"] for row in result["comments"]} == {"a": 0, "b": 1, "c": 2}


def test_expansion_batches_at_most_100_children() -> None:
    children = [f"c{n}" for n in range(101)]
    session = Session(
        initial(more(*children)),
        expanded(*(comment(child) for child in children[:100])),
        expanded(comment(children[100])),
    )
    result = transport.RedditHttpClient(session=session).get_comments("post1")
    assert result["complete"] is True
    assert len(session.calls[1]["params"]["children"].split(",")) == 100
    assert session.calls[2]["params"]["children"] == "c100"


@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        [listing()],
        [listing(), listing()],
        [listing({"kind": "t3", "data": {"id": "other"}}), listing()],
        [listing({"kind": "t3", "data": {"id": "post1"}}), listing(None)],
    ],
)
def test_malformed_initial_response_raises_instead_of_empty_success(payload: Any) -> None:
    with pytest.raises(transport.RedditTransportError):
        transport.RedditHttpClient(session=Session(Response(payload))).get_comments("post1")


def test_valid_empty_initial_listing_is_complete() -> None:
    result = transport.RedditHttpClient(session=Session(initial())).get_comments("post1")
    assert result["complete"] is True
    assert result["comments"] == []
    assert result["evidence"]["checkpoint"]["initial_fetched"] is True


@pytest.mark.parametrize("status", [404, 410])
def test_source_unavailable_requires_explicit_http_evidence(status: int) -> None:
    result = transport.RedditHttpClient(session=Session(Response({}, status))).get_comments("post1")
    assert result["source_unavailable"] is True
    assert result["complete"] is False
    assert result["evidence"]["status"] == status


@pytest.mark.parametrize(
    "checkpoint",
    [
        {},
        {"version": 1, "post_id": "other", "initial_fetched": True},
        {"version": 1, "post_id": "post1", "comments": [], "pending_children": []},
    ],
)
def test_invalid_checkpoint_cannot_become_empty_success(checkpoint: dict[str, Any]) -> None:
    with pytest.raises(transport.RedditTransportError):
        transport.RedditHttpClient(session=Session()).get_comments("post1", checkpoint)


def test_oauth_refreshes_cached_token_once_on_401(monkeypatch: pytest.MonkeyPatch) -> None:
    oauth(monkeypatch)
    session = Session(
        Response({"access_token": "first", "expires_in": 3600}),
        Response({}, 401),
        Response({"access_token": "second", "expires_in": 3600}),
        Response({"ok": True}),
    )
    client = transport.RedditHttpClient(session=session, require_oauth=True)
    assert client.get_json("/test", params={}) == {"ok": True}
    assert session.calls[1]["headers"]["Authorization"] == "Bearer first"
    assert session.calls[3]["headers"]["Authorization"] == "Bearer second"
    assert all(call["url"].startswith("https://oauth.reddit.com") for call in session.calls if call["method"] == "get")


def test_second_401_does_not_refresh_again_or_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    oauth(monkeypatch)
    token = Response({"access_token": "token", "expires_in": 3600})
    session = Session(token, Response({}, 401), token, Response({}, 401))
    with pytest.raises(transport.RedditTransportError, match="401"):
        transport.RedditHttpClient(session=session).get_json("/test", params={})
    assert len(session.calls) == 4


def test_oauth_failure_never_falls_back_anonymously(monkeypatch: pytest.MonkeyPatch) -> None:
    oauth(monkeypatch)
    session = Session(Response({"error": "invalid_client"}, 401))
    with pytest.raises(transport.RedditTransportError):
        transport.RedditHttpClient(session=session).get_json("/test", params={})
    assert [call["method"] for call in session.calls] == ["post"]


@pytest.mark.parametrize(
    "env", [{"TRR_JOB_PLANE_MODE": "remote"}, {"REDDIT_REQUIRE_OAUTH": "true"}, {"TRR_LONG_JOB_ENFORCE_REMOTE": "1"}]
)
def test_remote_or_required_env_rejects_missing_oauth(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    session = Session()
    client = transport.RedditHttpClient(session=session)
    with pytest.raises(transport.RedditTransportError, match="credentials"):
        client.get_json("/test", params={})
    assert client.token_probe()["blocked"] is True
    assert session.calls == []


def test_explicit_oauth_override_and_list_json() -> None:
    client = transport.RedditHttpClient(session=Session(Response([1])), require_oauth=False)
    assert client.get_json("/test", params={}, expected_type=list) == [1]


@pytest.mark.parametrize("payload", [[], None, "string", ValueError("body")])
def test_wrong_json_type_and_invalid_json_are_errors(payload: Any) -> None:
    with pytest.raises(transport.RedditTransportError):
        transport.RedditHttpClient(session=Session(Response(payload))).get_json("/test", params={})


def test_guard_and_observer_cover_oauth_and_each_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    oauth(monkeypatch)
    events: list[str] = []

    @contextmanager
    def guard(lane: str):
        events.append("enter:" + lane)
        yield
        events.append("exit:" + lane)

    client = transport.RedditHttpClient(
        session=Session(Response({"access_token": "token", "expires_in": 3600}), Response({}, 500), Response({})),
        request_guard=guard,
        response_observer=lambda response: events.append(str(response.status_code)),
    )
    client.get_json("/test", params={})
    assert events == [
        "enter:oauth",
        "200",
        "exit:oauth",
        "enter:request",
        "500",
        "exit:request",
        "enter:request",
        "200",
        "exit:request",
    ]


@pytest.mark.parametrize(
    "headers",
    [
        {"Retry-After": "12"},
        {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "12"},
        {"Retry-After": format_datetime(datetime.fromtimestamp(112, UTC))},
    ],
)
def test_server_cooldown_headers_are_respected(monkeypatch: pytest.MonkeyPatch, headers: dict[str, str]) -> None:
    monkeypatch.setattr(transport.time, "time", lambda: 100)
    sleeps: list[float] = []
    monkeypatch.setattr(transport.time, "sleep", sleeps.append)
    session = Session(Response({}, 429, headers), Response({}))
    client = transport.RedditHttpClient(session=session)
    assert client.get_json("/test", params={}) == {}
    assert 12 in sleeps
    assert all(call["timeout"] == 20 for call in session.calls)


def test_network_error_retries_are_finite_and_sanitized() -> None:
    session = Session(requests.Timeout("secret-in-exception"), requests.Timeout("secret-in-exception"))
    with pytest.raises(transport.RedditTransportError) as error:
        transport.RedditHttpClient(session=session).get_json("/test", params={})
    assert "secret" not in str(error.value)
    assert len(session.calls) == 2


def test_checkpoint_callback_error_stops_without_more_requests() -> None:
    session = Session(initial(more("a")))

    def reject(_checkpoint: dict[str, Any]) -> None:
        raise RuntimeError("persistence failed")

    with pytest.raises(RuntimeError, match="persistence failed"):
        transport.RedditHttpClient(session=session).get_comments("post1", on_checkpoint=reject)
    assert len(session.calls) == 1


def test_token_probe_exposes_status_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    oauth(monkeypatch)
    session = Session(Response({"access_token": "test-access-token", "expires_in": 3600}), Response({}))
    client = transport.RedditHttpClient(session=session)
    result = client.token_probe()
    assert result == {
        "configured": True,
        "authenticated": True,
        "reachable": True,
        "cooling_down": False,
        "blocked": False,
    }
    assert "test-access-token" not in json.dumps(result)
    assert "test-secret" not in json.dumps(result)


def test_cooling_down_probe_does_not_make_network_requests() -> None:
    client = transport.RedditHttpClient(session=Session())
    client._cooldown_until = transport.time.time() + 60
    assert client.token_probe()["cooling_down"] is True
    assert client.session.calls == []


def test_unfinished_initial_checkpoint_fetches_instead_of_becoming_complete() -> None:
    checkpoint = {
        "version": 1,
        "post_id": "post1",
        "initial_fetched": False,
        "initial_cursor": "/comments/post1.json",
        "comments": [],
        "pending_children": [],
    }
    session = Session(initial(comment("a")))
    result = transport.RedditHttpClient(session=session).get_comments("post1", checkpoint)
    assert result["complete"] is True
    assert result["comments"][0]["reddit_comment_id"] == "a"
    assert len(session.calls) == 1


def test_failed_initial_fetch_exposes_unfinished_checkpoint() -> None:
    session = Session(Response({}, 500), Response({}, 500))
    checkpoints: list[dict[str, Any]] = []
    with pytest.raises(transport.RedditTransportError) as error:
        transport.RedditHttpClient(session=session).get_comments("post1", on_checkpoint=checkpoints.append)
    assert error.value.checkpoint == checkpoints[-1]
    assert checkpoints[-1]["initial_fetched"] is False
    assert checkpoints[-1]["initial_cursor"] == "/comments/post1.json"


def test_guard_failure_is_not_retried_even_with_transport_error_type() -> None:
    entries: list[str] = []

    @contextmanager
    def guard(lane: str):
        entries.append(lane)
        raise transport.RedditTransportError("claim lost", status=503)
        yield

    session = Session()
    with pytest.raises(transport.RedditTransportError, match="claim lost"):
        transport.RedditHttpClient(session=session, request_guard=guard).get_json("/test", params={})
    assert entries == ["request"]
    assert session.calls == []


def test_malformed_cyclic_expansion_retains_valid_pending_checkpoint() -> None:
    session = Session(initial(comment("a"), more("b")), expanded(comment("b", "t1_b")))
    result = transport.RedditHttpClient(session=session).get_comments("post1")
    assert result["complete"] is False
    assert result["unresolved_ids"] == ["b"]
    assert [row["reddit_comment_id"] for row in result["comments"]] == ["a"]
    assert result["evidence"]["errors"][0]["message"] == "Cyclic Reddit comment parents"


def test_concurrent_oauth_refresh_is_coalesced(monkeypatch: pytest.MonkeyPatch) -> None:
    oauth(monkeypatch)
    session = Session(Response({"access_token": "shared-token", "expires_in": 3600}))
    client = transport.RedditHttpClient(session=session)
    gate = threading.Barrier(9)
    tokens: list[str | None] = []

    def fetch() -> None:
        gate.wait(timeout=5)
        tokens.append(client._get_oauth_token())

    threads = [threading.Thread(target=fetch) for _ in range(8)]
    for thread in threads:
        thread.start()
    gate.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    assert tokens == ["shared-token"] * 8
    assert len(session.calls) == 1


def test_guard_and_checkpoint_run_in_requesting_thread() -> None:
    seen: list[tuple[str, int]] = []

    @contextmanager
    def guard(lane: str):
        seen.append((lane, threading.get_ident()))
        yield

    client = transport.RedditHttpClient(
        session=Session(initial(more("a")), expanded(comment("a"))), request_guard=guard
    )

    def fetch() -> None:
        client.get_comments(
            "post1", on_checkpoint=lambda _checkpoint: seen.append(("checkpoint", threading.get_ident()))
        )

    thread = threading.Thread(target=fetch)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert seen == [
        ("request", thread.ident),
        ("checkpoint", thread.ident),
        ("morechildren", thread.ident),
        ("checkpoint", thread.ident),
    ]


@pytest.mark.parametrize("count", [0, 4])
def test_continuation_without_ids_remains_unresolved_after_checkpoint_resume(count: int) -> None:
    continuation = more(parent="t1_a")
    continuation["data"]["count"] = count
    session = Session(initial(comment("a", replies=listing(continuation))))
    checkpoints: list[dict[str, Any]] = []
    result = transport.RedditHttpClient(session=session).get_comments("post1", on_checkpoint=checkpoints.append)
    assert result["complete"] is False
    assert result["unresolved_ids"] == []
    assert result["evidence"]["pending_continuations_count"] == 1
    assert result["evidence"]["errors"] == [{"reason": "unresolved_continuations", "count": 1}]
    saved = checkpoints[-1]
    assert saved["pending_continuations"][0]["parent_id"] == "t1_a"
    assert saved["pending_continuations"][0]["depth"] == 1
    resumed = transport.RedditHttpClient(session=Session()).get_comments("post1", saved)
    assert resumed["complete"] is False
    assert resumed["evidence"]["checkpoint"]["pending_continuations"] == saved["pending_continuations"]


def test_expansion_continuation_and_later_http_failure_keep_both_queues() -> None:
    session = Session(
        initial(more("a")), expanded(comment("a"), more(parent="t1_a"), more("b")), Response({}, 503), Response({}, 503)
    )
    checkpoints: list[dict[str, Any]] = []
    result = transport.RedditHttpClient(session=session).get_comments("post1", on_checkpoint=checkpoints.append)
    assert result["complete"] is False
    assert result["unresolved_ids"] == ["b"]
    assert result["evidence"]["pending_continuations_count"] == 1
    assert checkpoints[-1]["pending_children"][0]["id"] == "b"
    assert checkpoints[-1]["pending_continuations"][0]["parent_id"] == "t1_a"


def test_legacy_refresh_exports_the_shared_transport_client_and_error() -> None:
    from trr_backend.repositories import reddit_refresh

    assert reddit_refresh.RedditHttpClient is transport.RedditHttpClient
    assert reddit_refresh.RedditRefreshError is transport.RedditTransportError


def test_legacy_submission_wrapper_uses_shared_list_json(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import reddit_refresh

    calls: list[Any] = []

    class Client:
        def get_json(self, path: str, **kwargs: Any) -> list[dict[str, Any]]:
            calls.append((path, kwargs))
            return [
                listing(
                    {
                        "kind": "t3",
                        "data": {
                            "id": "post1",
                            "title": "Thread",
                            "url": "/r/test/comments/post1",
                            "created_utc": 100,
                        },
                    }
                )
            ]

    monkeypatch.setattr(reddit_refresh, "_HTTP_CLIENT", Client())
    row = reddit_refresh._fetch_submission_by_post_id("post1")
    assert row is not None
    assert row["reddit_post_id"] == "post1"
    assert calls == [
        ("/comments/post1.json", {"params": {"limit": 1, "depth": 1, "raw_json": 1}, "expected_type": list})
    ]


def test_legacy_comment_wrapper_rejects_incomplete_tree_and_preserves_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import reddit_refresh

    checkpoint = {"pending_children": [{"id": "a"}]}

    class Client:
        def get_comments(self, post_id: str) -> dict[str, Any]:
            return {
                "comments": [],
                "complete": False,
                "source_unavailable": False,
                "unresolved_ids": ["a"],
                "evidence": {"checkpoint": checkpoint, "errors": [{"status": 429, "retry_after_seconds": 10}]},
            }

    monkeypatch.setattr(reddit_refresh, "_HTTP_CLIENT", Client())
    with pytest.raises(reddit_refresh.RedditRefreshError) as error:
        reddit_refresh._fetch_post_comments_tree("post1")
    assert error.value.checkpoint == checkpoint
    assert error.value.status == 429
    assert error.value.retry_after_seconds == 10


def test_legacy_comment_wrapper_returns_completed_shared_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import reddit_refresh

    class Client:
        def get_comments(self, post_id: str) -> dict[str, Any]:
            return {"complete": True, "comments": [{"reddit_comment_id": "a"}]}

    monkeypatch.setattr(reddit_refresh, "_HTTP_CLIENT", Client())
    assert reddit_refresh._fetch_post_comments_tree("post1") == [{"reddit_comment_id": "a"}]
