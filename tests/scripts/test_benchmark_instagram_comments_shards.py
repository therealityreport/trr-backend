from __future__ import annotations

import scripts.socials.instagram.benchmark_comments_shards as cli


def test_fixture_tiny_profile_reports_required_metrics() -> None:
    args = cli._parse_args(["--fixture-profile", "tiny"])

    rc, payload = cli.run_benchmark(args)

    assert rc == 0
    assert payload["status"] == "ok"
    assert payload["mode"] == "fixture"
    assert payload["fixture_profile"] == "tiny"
    assert payload["safety"]["fixture_mode_default"] is True
    assert payload["safety"]["launched_scrape"] is False
    assert payload["totals"]["posts_processed"] == 3
    assert payload["totals"]["top_level_comments"] == 34
    assert payload["totals"]["replies"] == 17
    assert payload["totals"]["flattened_saved"] == 54
    assert payload["totals"]["media_comments"] == 2
    assert payload["totals"]["hidden_reveal_attempts"] == 1
    assert payload["timing"]["per_post_p95_ms"] >= payload["timing"]["per_post_median_ms"]
    assert payload["transport"]["session_count"] == 3


def test_live_mode_refuses_without_explicit_flags() -> None:
    args = cli._parse_args(["--live", "--account", "thetraitorsus"])

    rc, payload = cli.run_benchmark(args)

    assert rc == 2
    assert payload["status"] == "refused"
    assert payload["error"] == "live_mode_requires_explicit_flags"
    assert "--confirm-live" in payload["missing_flags"]
    assert "--active-job-preflight" in payload["missing_flags"]
    assert payload["safety"]["launched_scrape"] is False


def test_live_mode_blocks_when_active_jobs_exist(monkeypatch) -> None:
    args = cli._parse_args(
        [
            "--live",
            "--account",
            "@thetraitorsus",
            "--confirm-live",
            "--active-job-preflight",
        ]
    )
    monkeypatch.setattr(
        cli,
        "_active_live_comments_jobs",
        lambda _account: {"active_comment_jobs": 2, "active_run_ids": ["run-1"]},
    )
    monkeypatch.setattr(cli, "_summarize_live_rows", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError))

    rc, payload = cli.run_benchmark(args)

    assert rc == 2
    assert payload["status"] == "blocked_active_jobs"
    assert payload["account"] == "thetraitorsus"
    assert payload["active_job_preflight"] == {"active_comment_jobs": 2, "active_run_ids": ["run-1"]}
    assert payload["safety"]["launched_scrape"] is False


def test_live_summary_reads_runner_metadata_shape(monkeypatch) -> None:
    class _FakePg:
        @staticmethod
        def fetch_all(_sql, _params):  # noqa: ANN001
            return [
                {
                    "job_id": "job-1",
                    "status": "completed",
                    "items_found": 16,
                    "last_error_code": None,
                    "metadata": {
                        "stage_counters": {"posts": 2, "comments": 14},
                        "persist_counters": {"comments_upserted": 10},
                        "post_latency": {
                            "samples": [
                                {"top_level_comment_count": 6, "observed_comment_count": 8},
                                {"top_level_comment_count": 4, "observed_comment_count": 6},
                            ]
                        },
                        "fetcher_runtime": {
                            "hidden_comments": {"render_attempts": 1, "merged_comments": 3},
                            "retry_reason_counts": {"http_429": 2},
                        },
                    },
                }
            ]

    monkeypatch.setattr(cli, "_load_db_helpers", lambda: (_FakePg, lambda: None))

    payload = cli._summarize_live_rows("thetraitorsus", limit_posts=5)

    assert payload["totals"]["top_level_comments"] == 10
    assert payload["totals"]["replies"] == 4
    assert payload["totals"]["flattened_saved"] == 14
    assert payload["totals"]["hidden_reveal_attempts"] == 1
    assert payload["totals"]["hidden_comments_merged"] == 3
    assert payload["totals"]["transport_retries"] == 2
