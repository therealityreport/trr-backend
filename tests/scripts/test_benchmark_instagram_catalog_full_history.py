from __future__ import annotations

from scripts.socials import benchmark_instagram_catalog_full_history as cli


def test_collect_catalog_run_metrics_computes_throughput() -> None:
    metrics = cli._collect_catalog_run_metrics(
        [
            {
                "items_found": 120,
                "metadata": {
                    "activity": {"posts_checked": 132, "pages_scanned": 4},
                    "persist_counters": {"posts_upserted": 120},
                    "retrieval_meta": {
                        "posts_checked": 132,
                        "pages_scanned": 4,
                        "retrieval_transport": "authenticated",
                    },
                },
            },
            {
                "items_found": 100,
                "metadata": {
                    "activity": {"posts_checked": 99, "pages_scanned": 3},
                    "persist_counters": {"posts_upserted": 99},
                    "retrieval_meta": {"posts_checked": 99, "pages_scanned": 3, "retrieval_transport": "authenticated"},
                },
            },
        ],
        elapsed_seconds=120.0,
    )

    assert metrics["total_posts_checked"] == 231
    assert metrics["total_posts_saved"] == 219
    assert metrics["pages_scanned"] == 7
    assert metrics["posts_per_minute"] == 115.5
    assert metrics["pages_per_minute"] == 3.5
    assert metrics["transport_used"] == ["authenticated"]


def test_benchmark_instagram_catalog_full_history_polls_until_completion(monkeypatch) -> None:
    class _StubRepo:
        def __init__(self) -> None:
            self.progress_calls = 0
            self.pg = type(
                "PG",
                (),
                {
                    "fetch_all": staticmethod(
                        lambda *_args, **_kwargs: [
                            {
                                "items_found": 165,
                                "metadata": {
                                    "activity": {"posts_checked": 165, "pages_scanned": 5},
                                    "persist_counters": {"posts_upserted": 165},
                                    "retrieval_meta": {
                                        "posts_checked": 165,
                                        "pages_scanned": 5,
                                        "retrieval_transport": "authenticated",
                                    },
                                },
                            }
                        ]
                    )
                },
            )()

        def start_social_account_catalog_backfill(self, *_args, **_kwargs):
            return {"run_id": "run-1", "status": "queued"}

        def get_social_account_catalog_run_progress(self, *_args, **_kwargs):
            self.progress_calls += 1
            if self.progress_calls == 1:
                return {"run_id": "run-1", "run_status": "running", "run_state": "fetching"}
            return {
                "run_id": "run-1",
                "run_status": "completed",
                "run_state": "completed",
                "worker_runtime": {"runner_strategy": "full_history_cursor_breakpoints"},
                "post_progress": {"completed_posts": 165, "total_posts": 16667},
            }

    stub_repo = _StubRepo()
    monotonic_values = iter([0.0, 10.0, 20.0, 20.0])

    monkeypatch.setattr(cli, "_load_social_repo", lambda: stub_repo)
    monkeypatch.setattr(cli.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli.time, "monotonic", lambda: next(monotonic_values))

    payload = cli.benchmark_instagram_catalog_full_history(
        account_handle="bravotv",
        source_scope="bravo",
        poll_seconds=0.01,
        timeout_minutes=5.0,
    )

    assert payload["status"] == "ok"
    assert payload["run_id"] == "run-1"
    assert payload["metrics"]["total_posts_checked"] == 165
    assert payload["metrics"]["pages_scanned"] == 5
    assert payload["metrics"]["transport_used"] == ["authenticated"]
