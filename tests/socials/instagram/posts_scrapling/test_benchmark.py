from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from scripts.socials.instagram.benchmark_posts_backfill import (
    BenchmarkRequest,
    build_benchmark_payload,
    main,
)
from scripts.socials.instagram.diff_posts_backfill_metadata import build_run_metadata_diff


def test_benchmark_payload_includes_required_comparison_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PER_IP_PACING_ENABLED", "1")

    payload = build_benchmark_payload(
        BenchmarkRequest(account="@TheTraitorsUS", mode="resume", max_pages=500, run_id="run-123"),
        now=datetime(2026, 5, 3, 7, 30, tzinfo=UTC),
    )

    assert payload["account"] == "thetraitorsus"
    assert payload["max_pages"] == 100
    assert payload["metrics"] == {
        "pages_per_second": None,
        "posts_per_second": None,
        "doc_id_attempts_per_page": None,
        "warmup_duration_ms": None,
        "resume_cursor_used": None,
        "detail_fetch_attempts_per_post": None,
    }
    assert payload["request_counts"] == {
        "listing_pages": 0,
        "doc_id_attempts": 0,
        "detail_fetch_attempts": 0,
    }
    assert payload["phase_durations_ms"]["listing"] is None
    assert payload["proxy_pacing"] == {}
    assert payload["warmup_pool"] == {}
    assert payload["bidirectional_probe"] == {}
    assert payload["feature_flags"]["per_ip_pacing_enabled"] is True


def test_benchmark_cli_emits_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--account", "bravotv", "--mode", "listing-only", "--max-pages", "3"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["benchmark"] == "instagram_posts_backfill"
    assert payload["account"] == "bravotv"
    assert payload["mode"] == "listing-only"


def test_benchmark_rejects_missing_account() -> None:
    with pytest.raises(ValueError, match="account is required"):
        build_benchmark_payload(BenchmarkRequest(account="", mode="listing-only", max_pages=3))


def test_run_metadata_diff_reports_request_timing_and_flag_changes() -> None:
    before = {
        "request_counts": {"listing_pages": 3, "doc_id_attempts": 9, "detail_fetch_attempts": 99},
        "metrics": {"pages_per_second": 1.0, "posts_per_second": 33.0, "warmup_duration_ms": 8000},
        "doc_id_used": "doc-old",
        "profile_posts_doc_ids": ["doc-old", "doc-new"],
        "feature_flags": {"per_ip_pacing_enabled": False},
        "phase_durations_ms": {"warmup": 8000, "listing": 3000},
        "warmup_pool": {"hit": False},
        "bidirectional_probe": {"passed": False, "reason": "disabled"},
    }
    after = {
        "request_counts": {"listing_pages": 3, "doc_id_attempts": 3, "detail_fetch_attempts": 0},
        "metrics": {"pages_per_second": 2.0, "posts_per_second": 66.0, "warmup_duration_ms": 1200},
        "doc_id_used": "doc-new",
        "profile_posts_doc_ids": ["doc-new"],
        "feature_flags": {"per_ip_pacing_enabled": True},
        "phase_durations_ms": {"warmup": 1200, "listing": 1500},
        "warmup_pool": {"hit": True},
        "bidirectional_probe": {"passed": True, "reason": "reverse_probe_passed"},
        "field_coverage": {"music_info": {"present": 10, "missing": 2}},
    }

    diff = build_run_metadata_diff(before, after)

    assert diff["request_counts"]["doc_id_attempts"]["delta"] == -6
    assert diff["request_counts"]["detail_fetch_attempts"]["delta"] == -99
    assert diff["timing"]["posts_per_second"]["delta"] == 33
    assert diff["feature_flags"]["changed"] == {
        "per_ip_pacing_enabled": {"before": False, "after": True}
    }
    assert diff["timing"]["phase_durations_ms"]["warmup"]["delta"] == -6800
    assert diff["warmup_pool"]["after"]["hit"] is True
    assert diff["bidirectional_probe"]["after"]["passed"] is True
    assert diff["field_coverage"]["after"]["music_info"]["present"] == 10
