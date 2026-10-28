"""Authenticated WWW pages through real parsing and isolated durable storage."""

import json
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from tests.ingestion import test_comments as existing
from tests.ingestion.test_authenticated_relay import document
from trr_backend.ingestion import authenticated_comment_transport as transport
from trr_backend.ingestion.authenticated_relay import AuthenticatedRelayPageClient
from trr_backend.ingestion.authenticated_relay_contract import (
    AUTH_RELAY_FAMILY,
    CHILD_DOCUMENT,
    CHILD_ROOT_DOCUMENT,
    PARENT_DOCUMENT,
    PARENT_ROOT_DOCUMENT,
)
from trr_backend.ingestion.comments import (
    CommentActivity,
    CommentSourceError,
    CursorStalledError,
    _advance,
    _settlement,
    parse_comment_evidence,
)
from trr_backend.ingestion.domain import SourceProfile

database = existing.database
TOP = "xdt_api__v1__media__media_id__comments__connection"
CHILD = "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"


def node(identity, replies=0) -> dict[str, Any]:
    return {"pk": identity, "text": identity, "user": {"username": "fixture"}, "child_comment_count": replies}


def page(nodes, *, after=None, parent=None, initial=True) -> dict[str, Any]:
    doc, name = (
        (CHILD_ROOT_DOCUMENT if initial else CHILD_DOCUMENT)
        if parent
        else (PARENT_ROOT_DOCUMENT if initial else PARENT_DOCUMENT)
    )
    return {
        "data": {
            CHILD if parent else TOP: {
                "edges": [{"node": n} for n in nodes],
                "page_info": {"has_next_page": after is not None, "end_cursor": after},
            }
        },
        **({"parent_comment_id": parent} if parent else {}),
        "cursor_payload": {
            "adapter_family": AUTH_RELAY_FAMILY,
            "adapter_version": 1,
            "source": "root" if initial else "resume",
            "request_cursor": None if initial else "cursor1",
            "relay_doc_id": doc,
            "relay_friendly_name": name,
            "sort_order": "native_default" if parent else "popular",
            "page_size": None if initial else (5 if parent else 10),
            "media_id": "333",
            "relay_logged_in": True,
            "relay_threads_style": False,
        },
    }


def source(monkeypatch, handler, expected=5):
    from trr_backend.socials.instagram import child_reply_probe

    monkeypatch.setattr(transport, "media_id_from_shortcode", lambda _: "333")
    monkeypatch.setattr(child_reply_probe, "media_id_from_shortcode", lambda _: "333")
    clients = []

    def factory(**kw):
        client = httpx.Client(transport=httpx.MockTransport(handler))
        result = AuthenticatedRelayPageClient(client=client, **kw)
        result.close = client.close
        clients.append(client)
        return result

    return transport.AuthenticatedRelayCommentSource(
        raw_cookies={"ds_user_id": "111", "sessionid": "fixture", "csrftoken": "fixture"},
        expected_count=expected,
        client_factory=factory,  # pyright: ignore[reportArgumentType]
    ), clients


def test_profile_opt_in_is_distinct_and_round_trips():
    from dataclasses import asdict

    from trr_backend.ingestion.domain import RunIntent

    relay = SourceProfile.for_authenticated_relay()
    assert relay.comments_family == AUTH_RELAY_FAMILY
    assert relay != SourceProfile.for_mode("authenticated")
    intent = RunIntent(account="bravotv", source_profile=relay)
    assert RunIntent.from_payload(intent.payload()) == intent
    assert asdict(relay)["access_mode"] == "authenticated"


def test_initial_parent_and_child_contract_transitions_are_persistable():
    state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
    for payload in (
        page([node("11", 2)], after="cursor1"),
        page([node("12")], initial=False),
        page([node("21")], parent="11", after="cursor1"),
        page([node("22")], parent="11", initial=False),
    ):
        _advance(
            state, parse_comment_evidence(existing.evidence(payload), pinned=True), scan_started_at=existing.OBSERVED
        )
    assert state["lanes"]["__top__"]["relay_doc_id"] == PARENT_DOCUMENT[0]
    assert state["lanes"]["11"]["relay_doc_id"] == CHILD_DOCUMENT[0]
    assert state["adapter_family"] == AUTH_RELAY_FAMILY
    assert state["observed"]["22"]["parent"] == "11"
    from trr_backend.ingestion.comments import CommentFetchSummary

    settlement = _settlement(state, CommentFetchSummary(expected_count=4, full_observation=True))
    assert settlement is not None
    assert settlement["basis"] == "count_parity"
    assert _settlement(state, CommentFetchSummary(expected_count=5, full_observation=True)) is None


@pytest.mark.parametrize("mutation", ["family", "media", "providers", "cursor", "operation"])
def test_resume_cannot_cross_query_or_identity_boundaries(mutation):
    state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
    _advance(
        state,
        parse_comment_evidence(existing.evidence(page([node("11")], after="cursor1")), pinned=True),
        scan_started_at=existing.OBSERVED,
    )
    payload = page([], initial=False)
    provenance = payload["cursor_payload"]
    if mutation == "family":
        state["adapter_family"] = "instagram_public_relay"
    elif mutation == "media":
        provenance["media_id"] = "444"
    elif mutation == "providers":
        provenance["relay_threads_style"] = True
    elif mutation == "cursor":
        provenance["request_cursor"] = "different"
    else:
        provenance["relay_doc_id"] = CHILD_DOCUMENT[0]
    with pytest.raises(CommentSourceError):
        parsed = parse_comment_evidence(existing.evidence(payload), pinned=True)
        _advance(state, parsed, scan_started_at=existing.OBSERVED)


def test_repeated_authenticated_cursor_stops_after_durable_page(database, monkeypatch):
    store, tx, args = existing.setup(database)
    sends = []

    def handler(request):
        sends.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, text=document())
        return httpx.Response(200, json=page([node("11")], after="cursor1"))

    fetch, clients = source(monkeypatch, handler)
    with pytest.raises(CursorStalledError):
        CommentActivity(store, fetch, clock=lambda: existing.OBSERVED).execute(**args)
    target = existing.query(
        tx, "select checkpoint from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
    )[0]
    assert target["checkpoint"]["comment_pages"]["lanes"]["__top__"]["repeated"]
    assert sends == ["GET", "POST", "POST"] and clients[0].is_closed


def test_real_transport_persists_parents_and_multiple_child_pages(database, monkeypatch):
    store, tx, args = existing.setup(database)
    sends = []

    def handler(request):
        if request.method == "GET":
            sends.append("document")
            return httpx.Response(200, text=document())
        form = parse_qs(request.content.decode())
        doc = form["doc_id"][0]
        sends.append(doc)
        payload = {
            PARENT_ROOT_DOCUMENT[0]: page([node("11", 3)], after="cursor1"),
            PARENT_DOCUMENT[0]: page([node("12")], initial=False),
            CHILD_ROOT_DOCUMENT[0]: page([node("21"), node("22")], after="cursor1", parent="11"),
            CHILD_DOCUMENT[0]: page([node("23")], parent="11", initial=False),
        }[doc]
        return httpx.Response(200, json=payload)

    fetch, clients = source(monkeypatch, handler)
    result = CommentActivity(store, fetch, clock=lambda: existing.OBSERVED).execute(**args)
    assert result["complete"] is True
    rows = existing.query(
        tx, "select id,comment_id,parent_comment_id from social.instagram_comments where post_id=%s", (args["post_id"],)
    )
    assert len(rows) == 5
    parent_id = next(r["id"] for r in rows if r["comment_id"] == "11")
    assert {r["parent_comment_id"] for r in rows if r["comment_id"] in {"21", "22", "23"}} == {parent_id}
    assert sends == ["document", PARENT_ROOT_DOCUMENT[0], PARENT_DOCUMENT[0], CHILD_ROOT_DOCUMENT[0], CHILD_DOCUMENT[0]]
    assert clients[0].is_closed


def test_resume_after_committed_parent_page_keeps_cursor_and_unique_rows(database, monkeypatch):
    from tests.ingestion.test_comment_resume import retry

    store, tx, args = existing.setup(database)
    sends = []
    interrupt = [True]

    def handler(request):
        if request.method == "GET":
            sends.append("document")
            return httpx.Response(200, text=document())
        form = parse_qs(request.content.decode())
        doc = form["doc_id"][0]
        sends.append(doc)
        if doc == PARENT_ROOT_DOCUMENT[0]:
            return httpx.Response(200, json=page([node("11")], after="cursor1"))
        assert doc == PARENT_DOCUMENT[0]
        assert json.loads(form["variables"][0])["after"] == "cursor1"
        if interrupt[0]:
            raise RuntimeError("injected interruption after committed root")
        return httpx.Response(200, json=page([node("12")], initial=False))

    fetch, clients = source(monkeypatch, handler, expected=2)
    with pytest.raises(RuntimeError, match="injected interruption"):
        CommentActivity(store, fetch, clock=lambda: existing.OBSERVED).execute(**args)
    assert clients[0].is_closed
    retry(database, args, cancel=True)
    interrupt[0] = False
    result = CommentActivity(store, fetch, clock=lambda: existing.OBSERVED).execute(**args)
    assert result["complete"] is True
    assert sends.count(PARENT_ROOT_DOCUMENT[0]) == 1
    assert sends.count("document") == 2
    rows = existing.query(tx, "select comment_id from social.instagram_comments where post_id=%s", (args["post_id"],))
    assert {r["comment_id"] for r in rows} == {"11", "12"} and len(rows) == 2
    assert all(c.is_closed for c in clients)


@pytest.mark.parametrize(
    "rejection",
    ["success", "failed", "permission_denied", "incremental", "hasNext", "missing_flag", "missing_cursor", "shape"],
)
def test_rejected_root_retains_failure_without_progress_and_resume_refetches(database, monkeypatch, rejection):
    from tests.ingestion.test_comment_resume import retry
    from trr_backend.ingestion.evidence import Evidence
    from trr_backend.ingestion.post_transport import SourceFailure

    store, tx, args = existing.setup(database)
    sends, rejected = [], [True]

    def handler(request):
        sends.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, text=document())
        payload = page([node("11")])
        if rejected[0]:
            if rejection in {"success", "failed", "permission_denied"}:
                payload[rejection] = rejection != "success"
            elif rejection == "incremental":
                payload.update(incremental=[], hasNext=True)
            elif rejection == "hasNext":
                payload["hasNext"] = False  # Unsupported stream frames, including their final frame.
            elif rejection == "missing_flag":
                payload["data"][TOP]["page_info"].pop("has_next_page")
            elif rejection == "missing_cursor":
                payload["data"][TOP]["page_info"]["has_next_page"] = True
            else:
                payload["data"][TOP] = []
        return httpx.Response(200, json=payload)

    fetch, clients = source(monkeypatch, handler, expected=1)
    activity = CommentActivity(store, fetch, clock=lambda: existing.OBSERVED)
    with pytest.raises(SourceFailure):
        activity.execute(**args)
    assert not existing.query(
        tx, "select comment_id from social.instagram_comments where post_id=%s", (args["post_id"],)
    )
    for table in ("comment_pages", "receipts"):
        assert not existing.query(
            tx, f"select * from social.instagram_ingestion_{table} where run_id=%s", (args["run_id"],)
        )
    target = existing.query(
        tx, "select checkpoint from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
    )[0]
    assert not target["checkpoint"].get("comment_pages")
    row = existing.query(tx, "select * from social.instagram_ingestion_evidence where run_id=%s", (args["run_id"],))[0]
    evidence = Evidence(
        source_identity=row["source_identity"],
        captured_at=row["captured_at"],
        parser_version=row["parser_version"],
        payload_json=row["payload"],
        content_hash=row["content_hash"],
        schema_version=row["schema_version"],
    )
    assert evidence.read(pinned=True)["status"] == "fail"
    with pytest.raises(CommentSourceError, match="failed request metadata"):
        parse_comment_evidence(evidence, pinned=True)

    retry(database, args, cancel=True)
    rejected[0] = False
    result = activity.execute(**args)
    assert result["complete"] is True
    assert sends == ["GET", "POST", "GET", "POST"]
    assert existing.query(
        tx, "select comment_id from social.instagram_comments where post_id=%s", (args["post_id"],)
    ) == [{"comment_id": "11"}]
    assert all(c.is_closed for c in clients)


@pytest.mark.parametrize("family", [None, "instagram_public_relay", "instagram_comments_rest"])
def test_source_rejects_foreign_checkpoint_before_any_request(monkeypatch, family):
    monkeypatch.setattr(transport, "media_id_from_shortcode", lambda _: "333")
    fetch = transport.AuthenticatedRelayCommentSource(
        raw_cookies={},
        client_factory=lambda **kw: pytest.fail("foreign cursor reached source"),  # pyright: ignore[reportArgumentType]
    )
    state = {
        "version": 1,
        "observed": {},
        "lanes": {"__top__": {"cursors": ["cursor1"]}},
        "adapter_family": family,
        "adapter_version": 1,
        "expected": None,
    }
    with pytest.raises(CommentSourceError):
        fetch(
            shortcode="Abc123",
            checkpoint={"comment_pages": state},
            source_observer=lambda *a: None,
            pre_source_request=lambda: pytest.fail("foreign cursor sent"),
        )
