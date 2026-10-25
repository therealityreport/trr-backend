"""Overlapping public reply guards must retain the strongest existing wait."""

from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest

from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.comment_transport import PublicRelayResume
from trr_backend.ingestion.comments import CommentResumeNode

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
