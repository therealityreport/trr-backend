"""Overlapping public reply guards must retain the strongest existing wait."""

from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest

from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.comment_transport import PublicRelayResume
from trr_backend.ingestion.comments import CommentResumeNode
from trr_backend.ingestion.domain import fingerprint

NOW = public.OBSERVED + timedelta(minutes=5)


def checkpoint(mode):
    technical = {
        "version": 1,
        "code": "source_incomplete",
        "technical_hold": mode == "held",
        "failure_count": 3 if mode == "held" else 1,
        "next_attempt_at": (NOW + timedelta(hours=1) if mode == "future" else NOW - timedelta(seconds=1)).isoformat(),
    }
    access = {"lanes": ["bad"]}
    if mode == "server":
        access.update(retry_source="server_retry_after", next_attempt_at=(NOW + timedelta(hours=1)).isoformat())
    return {
        "comment_pages": {
            "adapter_family": "instagram_public_relay",
            "adapter_version": 1,
            "observed": {name: {"expected_replies": None} for name in ("bad", "good")},
            "lanes": {
                "__top__": {
                    "exhausted": True,
                    "started_from_root": True,
                    "relay_doc_id": "top-doc",
                    "relay_friendly_name": "Top",
                    "sort_order": "recent",
                    "page_size": 12,
                }
            },
            "reply_access": access,
        },
        "public_reply_failures": {"bad": technical},
    }


@pytest.mark.parametrize("mode", ["held", "future", "server", "eligible"])
def test_overlap_composes_technical_and_access_deferrals_without_mutation(mode):
    saved = checkpoint(mode)
    before = deepcopy(saved)
    parents = [SimpleNamespace(comment_id=name, replies=[]) for name in ("bad", "good")]
    result = PublicRelayResume.from_checkpoint(saved, parents, now=NOW)
    bad = result.replies["bad"]
    assert bad["technical_failure"] is True
    assert bad["deferred"] is (mode != "eligible")
    assert bad["pending"] is (mode == "eligible")
    assert bad.get("needs_diagnostics", False) is (mode == "eligible")
    assert result.replies["good"] == {"pending": True}
    assert saved == before


@pytest.mark.parametrize("mode", ["held", "future", "server"])
def test_normal_source_sends_only_healthy_parent_when_either_overlap_guard_defers(monkeypatch, mode):
    saved = checkpoint(mode)
    before_saved = deepcopy(saved)
    calls = []

    def handler(request):
        if request.method == "GET":
            calls.append("bootstrap")
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        calls.append(parent)
        assert parent == "good", "Deferred technical parent reached the native request boundary"
        return httpx.Response(200, json=public.page([public.node("healthy-reply", 0)], child=True))

    fetch, _ = public.source(monkeypatch, handler, expected=None)

    def before():
        pass

    before.observation_clock = lambda: NOW
    inventory = tuple(CommentResumeNode(public.actual_parent_shape(name), public.OBSERVED) for name in ("bad", "good"))
    result = fetch(
        shortcode="fixture",
        checkpoint=saved,
        resume_inventory=inventory,
        source_observer=lambda *args: None,
        pre_source_request=before,
    )
    assert calls == ["bootstrap", "good"]
    assert result.public_reply_failures == ("bad",)
    assert saved == before_saved


@pytest.mark.parametrize("mode", ["held", "future", "server", "eligible"])
def test_top_restart_carries_reply_guards_for_newly_discovered_parents(mode):
    saved = checkpoint(mode)
    previous = saved["comment_pages"]["lanes"]["__top__"]
    saved["public_lane_restarts"] = {
        "__top__": {
            "previous_lane_hash": fingerprint(previous),
            "reason": "fresh public traversal",
            "initiated_by": "test",
            "source_revision": "fixture",
        }
    }
    before = deepcopy(saved)
    result = PublicRelayResume.from_checkpoint(saved, [], now=NOW)
    assert result.top == {} and result.from_root is True
    assert result.replies["bad"]["deferred"] is (mode != "eligible")
    assert result.replies["bad"]["pending"] is (mode == "eligible")
    assert not ({"exhausted", "after", "relay_doc_id", "native_reply_leaf"} & result.replies["bad"].keys())
    assert saved == before


@pytest.mark.parametrize("mode", ["held", "future", "server", "eligible"])
def test_top_restart_does_not_send_deferred_reply_requests(monkeypatch, mode):
    saved = checkpoint(mode)
    saved["public_lane_restarts"] = {
        "__top__": {
            "previous_lane_hash": fingerprint(saved["comment_pages"]["lanes"]["__top__"]),
            "reason": "fresh public traversal",
            "initiated_by": "test",
            "source_revision": "fixture",
        }
    }
    before_saved = deepcopy(saved)
    calls = []

    def handler(request):
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        parent = variables.get("parent_comment_id")
        calls.append(parent or "__top__")
        if parent is None:
            return httpx.Response(200, json=public.page([public.node("bad", 1), public.node("good", 1)]))
        return httpx.Response(200, json=public.page([public.node(parent + "-reply", 0)], child=True))

    fetch, _ = public.source(monkeypatch, handler, expected=None)

    def before():
        pass

    before.observation_clock = lambda: NOW
    fetch(
        shortcode="fixture",
        checkpoint=saved,
        resume_inventory=(),
        source_observer=lambda *args: None,
        pre_source_request=before,
    )
    assert "__top__" in calls and "good" in calls
    assert ("bad" in calls) is (mode == "eligible")
    assert saved == before_saved


@pytest.mark.parametrize("guard", ["technical_only", "server_only", "overlapping"])
def test_top_restart_preserves_each_independent_guard_without_seed_parents(guard):
    saved = checkpoint("held")
    if guard == "technical_only":
        saved["comment_pages"].pop("reply_access")
    else:
        saved["comment_pages"]["reply_access"].update(
            retry_source="server_retry_after", next_attempt_at=(NOW + timedelta(hours=1)).isoformat()
        )
        if guard == "server_only":
            saved.pop("public_reply_failures")
    saved["public_lane_restarts"] = {
        "__top__": {
            "previous_lane_hash": fingerprint(saved["comment_pages"]["lanes"]["__top__"]),
            "reason": "fresh public traversal",
            "initiated_by": "test",
            "source_revision": "fixture",
        }
    }
    guard_state = PublicRelayResume.from_checkpoint(saved, [], now=NOW).replies["bad"]
    assert guard_state["deferred"] is True and guard_state["pending"] is False
