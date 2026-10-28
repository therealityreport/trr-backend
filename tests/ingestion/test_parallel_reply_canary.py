"""An exact target and ownership generation permit only one canary attempt."""

import asyncio

import httpx
import pytest

from tests.ingestion import test_public_comment_transport as public

deny_unmocked_http = public.deny_unmocked_http


@pytest.mark.parametrize(
    "target,generation,run,run_generation,expected",
    [
        ("fixture", "7", "run-fixture", "3", 2),
        ("other", "7", "run-fixture", "3", 1),
        ("fixture", "8", "run-fixture", "3", 1),
        ("fixture", "7", "other-run", "3", 1),
        ("fixture", "7", "run-fixture", "4", 1),
    ],
)
def test_canary_is_fenced_to_one_target_and_attempt(monkeypatch, target, generation, run, run_generation, expected):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_TARGET", target)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_GENERATION", generation)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_RUN_ID", run)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_RUN_GENERATION", run_generation)
    active = peak = 0

    async def handler(request):
        nonlocal active, peak
        if request.method == "GET":
            return public.bootstrap()
        _, variables = public.body(request)
        if not variables.get("parent_comment_id"):
            return httpx.Response(200, json=public.page([public.actual_parent_shape(p) for p in ("p1", "p2")]))
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.01)
            return httpx.Response(200, json=public.page([], child=True))
        finally:
            active -= 1

    fetch, _ = public.source(monkeypatch, handler, expected=2)

    def guard():
        pass

    guard.owner_generation = 7  # pyright: ignore[reportFunctionMemberAccess]
    guard.run_id = "run-fixture"  # pyright: ignore[reportFunctionMemberAccess]
    guard.run_generation = 3  # pyright: ignore[reportFunctionMemberAccess]
    summary = fetch(shortcode="fixture", checkpoint={}, source_observer=lambda *args: None, pre_source_request=guard)
    assert summary.fetch_failed is False and peak == expected and active == 0


def test_operator_timing_receipt_contains_only_fixed_numeric_metrics(monkeypatch, tmp_path):
    import json

    from trr_backend.socials.instagram.comments_scrapling.reply_http import write_timing_receipt

    monkeypatch.setenv("TRR_INSTAGRAM_REPLY_TELEMETRY_DIR", str(tmp_path))
    write_timing_receipt(
        "fixture",
        7,
        2,
        {
            "requests": 4,
            "max_outstanding": 2,
            "network_seconds": 1.5,
            "headers": "excluded",
            "text": "excluded",
            "owner_seconds": float("nan"),
        },
    )
    receipts = list(tmp_path.glob("*.json"))
    assert len(receipts) == 1 and not list(tmp_path.glob("*.pending"))
    report = json.loads(receipts[0].read_text())
    assert report["target"] == "fixture" and report["owner_generation"] == 7
    assert report["metrics"] == {"requests": 4, "max_outstanding": 2, "network_seconds": 1.5}
