from __future__ import annotations

import json

import pytest

from scripts.socials.benchmark_backfill_runtime_methods import (
    BenchmarkEvidenceError,
    CandidateResult,
    select_default_method,
    write_results_json,
)


def _candidate(method: str, **overrides) -> CandidateResult:
    payload = {
        "method": method,
        "completeness": 0.99,
        "efficiency_score": 0.5,
        "detail_score": 0.5,
        "effectiveness_score": 0.5,
        "browser_use_evidence": {
            "source": "browser_use",
            "artifact": f"{method.lower()}-comparison-screenshot.png",
        },
    }
    payload.update(overrides)
    return CandidateResult(**payload)


def test_select_default_method_rejects_missing_browser_use_evidence() -> None:
    results = [
        _candidate("SCRAPLING"),
        _candidate("CRAWLEE", browser_use_evidence=None),
    ]

    with pytest.raises(BenchmarkEvidenceError, match="Browser Use comparative evidence"):
        select_default_method(results)


def test_select_default_method_requires_completeness_threshold() -> None:
    results = [
        _candidate("SCRAPLING", completeness=0.979),
        _candidate("CRAWLEE"),
    ]

    with pytest.raises(BenchmarkEvidenceError, match="completeness"):
        select_default_method(results)


def test_select_default_method_rejects_failures() -> None:
    results = [
        _candidate("SCRAPLING"),
        _candidate("CRAWLEE", failure_count=1),
    ]

    with pytest.raises(BenchmarkEvidenceError, match="failures"):
        select_default_method(results)


def test_select_default_method_picks_highest_efficiency_detail_effectiveness_tuple() -> None:
    results = [
        _candidate("SCRAPLING", efficiency_score=0.80, detail_score=0.95, effectiveness_score=0.95),
        _candidate("CRAWLEE", efficiency_score=0.90, detail_score=0.60, effectiveness_score=0.70),
    ]

    winner = select_default_method(results)

    assert winner.method == "CRAWLEE"


def test_write_results_json_allows_no_change_report_with_null_winner(tmp_path) -> None:
    output_path = tmp_path / "method-comparison.json"

    payload = write_results_json(
        output_path,
        [
            _candidate("SCRAPLING", browser_use_evidence=None),
            _candidate("CRAWLEE", browser_use_evidence=None),
        ],
        winner=None,
    )

    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["decision_status"]["default_changed"] is False
    assert payload["decision_status"]["winner"] is None
    assert written["decision_status"]["winner"] is None
    assert written["decision_status"]["status"] == "awaiting_browser_use_evidence"


def test_write_results_json_handles_pending_candidate_null_failure_count(tmp_path) -> None:
    output_path = tmp_path / "method-comparison.json"

    payload = write_results_json(
        output_path,
        [
            {
                "method": "SCRAPLING",
                "completeness": None,
                "efficiency_score": None,
                "detail_score": None,
                "effectiveness_score": None,
                "failure_count": None,
                "browser_use_evidence": None,
            },
            {
                "method": "CRAWLEE",
                "completeness": None,
                "efficiency_score": None,
                "detail_score": None,
                "effectiveness_score": None,
                "failure_count": None,
                "browser_use_evidence": None,
            },
        ],
        winner=None,
    )

    assert payload["decision_status"]["winner"] is None
    assert payload["candidates"][0]["failed"] is False
