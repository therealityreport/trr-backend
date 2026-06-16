from __future__ import annotations

from scripts.socials.instagram import smoke_posts_scrapling


def test_instagram_posts_smoke_summary_includes_performance_metadata() -> None:
    summary = smoke_posts_scrapling._build_operator_summary(
        account="bravotv",
        run_id="run-1",
        job_id="job-1",
        result={
            "id": "job-1",
            "run_id": "run-1",
            "status": "completed",
            "metadata": {
                "stage_counters": {"pages": 1, "posts": 33},
                "persist_counters": {"posts_upserted": 33},
                "performance": {
                    "elapsed_ms": 2300,
                    "warmup_duration_ms": 400,
                    "listing_duration_ms": 1200,
                    "persistence_duration_ms": 600,
                    "pages_per_second": 0.833,
                    "posts_per_second": 18.333,
                    "doc_id_attempts": 2,
                    "doc_ids_attempted": ["26859136577041380", "25645538101792896"],
                    "warmup_pool": {"enabled": True, "hit": True, "miss": False},
                    "bytes_total": 12345,
                    "bytes_by_host": {"www.instagram.com": 12345},
                },
                "fetcher_runtime": {
                    "proxy_pacing": {"enabled": True},
                    "proxy_identity": {"provider": "decodo"},
                },
            },
        },
    )

    assert summary["elapsed_ms"] == 2300
    assert summary["warmup_duration_ms"] == 400
    assert summary["listing_duration_ms"] == 1200
    assert summary["persistence_duration_ms"] == 600
    assert summary["pages_per_second"] == 0.833
    assert summary["posts_per_second"] == 18.333
    assert summary["doc_id_attempts"] == 2
    assert summary["warmup_pool"] == {"enabled": True, "hit": True, "miss": False}
    assert summary["bytes_total"] == 12345
