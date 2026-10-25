"""Predefined matched replay benchmark; opt in with TRR_REPLY_BENCHMARK=1.

Retained January native shapes: unknown empty lanes, populated lanes, overlapping
cursor pages, and attachment-only parents. Fixed latency trace and 250ms global
and per-fetcher pacing are identical in both arms. Real isolated PostgreSQL
retention/canonical writes/receipts are included. No production HTTP or DB.
"""

import asyncio
import json
import os
import statistics
import time
from pathlib import Path

import httpx
import pytest

from tests.ingestion import test_public_comment_transport as native
from trr_backend.ingestion.comments import CommentActivity

database = native.database
deny_unmocked_http = native.deny_unmocked_http

# Freeze the mix and delays before measuring either arm. Units are seconds.
WORKLOAD = (
    ("empty0", 0, 0.72),
    ("empty1", 0, 0.54),
    ("single0", 1, 0.66),
    ("single1", 1, 0.48),
    ("multi0", 3, 0.60),
    ("multi1", 2, 0.78),
    ("gif", 1, 0.54),
    ("empty2", 0, 0.66),
)


@pytest.mark.skipif(os.getenv("TRR_REPLY_BENCHMARK") != "1", reason="explicit paired performance gate")
def test_five_paired_reply_benchmarks(database, monkeypatch, tmp_path):
    pairs = []
    inventories = []
    for pair in range(5):
        timings = {}
        # Alternate order to reduce warm-cache/order bias.
        for concurrency in (1, 2) if pair % 2 == 0 else (2, 1):
            store, tx, args = native.setup(database)
            monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", str(concurrency))
            pace_root = tmp_path / f"pace-{pair}-{concurrency}"
            pace_root.mkdir()
            monkeypatch.setattr(
                native.maintained, "_global_rate_limit_path", lambda key, pace_root=pace_root: str(pace_root / "pace")
            )
            monkeypatch.setattr(
                native.maintained,
                "_global_rate_cooldown_path",
                lambda key, pace_root=pace_root: str(pace_root / "cooldown"),
            )
            calls = []

            async def handler(request, calls=calls):
                if request.method == "GET":
                    return native.bootstrap()
                _, variables = native.body(request)
                parent = variables.get("parent_comment_id")
                if parent is None:
                    rows = [native.actual_parent_shape(key) for key, _, _ in WORKLOAD]
                    rows[6].update(
                        text="",
                        giphy_media_info={
                            "id": "gif",
                            "images": {"fixed_height": {"url": "https://media.example/fixture.gif"}},
                        },
                    )
                    return httpx.Response(200, json=native.page(rows))
                count, latency = next((count, delay) for key, count, delay in WORKLOAD if key == parent)
                cursor = variables.get("after")
                index = int(cursor or 0)
                calls.append((parent, cursor, variables["first"]))
                await asyncio.sleep(latency)
                rows = [] if count == 0 else [native.node(f"{parent}-reply-{index}")]
                if index > 0:
                    rows.append(native.node(f"{parent}-reply-{index - 1}"))
                more = index + 1 < count
                return httpx.Response(
                    200, json=native.page(rows, child=True, more=more, after=str(index + 1) if more else None)
                )

            expected = len(WORKLOAD) + sum(count for _, count, _ in WORKLOAD)
            fetch, created = native.source(monkeypatch, handler, expected=expected)
            base_factory = fetch._factory

            def factory(base_factory=base_factory, **kwargs):
                result = base_factory(**kwargs)
                result._api_delay_seconds = result._global_api_delay_seconds = 0.25
                result._global_rate_limit_mode_configured = "file_lock"
                return result

            fetch._factory = factory
            started = time.monotonic()
            result = CommentActivity(store, fetch, clock=lambda: native.OBSERVED).execute(**args)
            elapsed = time.monotonic() - started
            assert result["complete"] and result["outcome"] == "committed"
            inventory = native.existing.query(
                tx,
                """
                select c.comment_id,p.comment_id parent,c.media_urls,c.is_missing,c.scraped_at
                  from social.instagram_comments c left join social.instagram_comments p
                    on p.id=c.parent_comment_id where c.post_id=%s order by c.comment_id
                """,
                (args["post_id"],),
            )
            proof = {
                "inventory": inventory,
                "lanes": result["checkpoint"]["comment_pages"]["lanes"],
                "expected": result["checkpoint"]["comment_pages"]["expected"],
                "calls": sorted(calls, key=str),
            }
            inventories.append(proof)
            metrics = created[0].runtime_metadata["reply_scheduler"]
            timings[concurrency] = {"elapsed_seconds": elapsed, **metrics}
        pairs.append(timings)
    assert all(proof == inventories[0] for proof in inventories)
    metrics = (
        "elapsed_seconds",
        "network_seconds",
        "pacing_seconds",
        "queue_seconds",
        "completed_queue_seconds",
        "owner_seconds",
        "persistence_seconds",
    )
    summary = {
        str(c): {
            name: {
                "median": statistics.median(p[c][name] for p in pairs),
                "stdev": statistics.stdev(p[c][name] for p in pairs),
            }
            for name in metrics
        }
        for c in (1, 2)
    }
    improvement = 1 - summary["2"]["elapsed_seconds"]["median"] / summary["1"]["elapsed_seconds"]["median"]
    report = {
        "workload": WORKLOAD,
        "pairs": pairs,
        "summary": summary,
        "median_improvement": improvement,
        "equivalent": True,
        "pacing_seconds": 0.25,
        "page_size": 12,
        "passed": improvement >= 0.20,
    }
    output = Path(os.environ.get("TRR_REPLY_BENCHMARK_OUTPUT", str(tmp_path / "benchmark.json")))
    output.write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
    assert improvement >= 0.20
