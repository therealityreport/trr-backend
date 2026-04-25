#!/usr/bin/env python3
"""Compare social backfill runtime benchmark candidates without changing defaults.

This module is intentionally evidence-first. It can serialize benchmark
results and select a candidate default only when Browser Use comparative
evidence is present for the candidates being compared.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MIN_COMPLETENESS = 0.98


class BenchmarkEvidenceError(ValueError):
    """Raised when benchmark evidence is insufficient to select a default."""


@dataclass(frozen=True)
class CandidateResult:
    """Normalized benchmark result for one runtime method."""

    method: str
    completeness: float
    efficiency_score: float
    detail_score: float
    effectiveness_score: float
    failure_count: int = 0
    browser_use_evidence: Mapping[str, Any] | None = None
    metrics: Mapping[str, Any] = field(default_factory=dict)
    notes: str | None = None

    @property
    def failed(self) -> bool:
        return self.failure_count > 0

    @property
    def has_browser_use_evidence(self) -> bool:
        evidence = dict(self.browser_use_evidence or {})
        if not evidence:
            return False
        source = str(evidence.get("source") or evidence.get("tool") or "").strip().lower()
        return source in {"browser_use", "browser-use", "@browser-use"}

    @property
    def selection_score(self) -> tuple[float, float, float, str]:
        return (
            _require_score(self.efficiency_score, self.method, "efficiency_score"),
            _require_score(self.detail_score, self.method, "detail_score"),
            _require_score(self.effectiveness_score, self.method, "effectiveness_score"),
            self.method.lower(),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["failed"] = self.failed
        payload["has_browser_use_evidence"] = self.has_browser_use_evidence
        payload["selection_score"] = {
            "efficiency": self.efficiency_score,
            "detail": self.detail_score,
            "effectiveness": self.effectiveness_score,
        }
        return payload


def _coerce_candidate(result: CandidateResult | Mapping[str, Any]) -> CandidateResult:
    if isinstance(result, CandidateResult):
        return result
    data = dict(result)
    if "failed" in data and "failure_count" not in data:
        data["failure_count"] = 1 if bool(data.pop("failed")) else 0
    allowed = set(CandidateResult.__dataclass_fields__)
    return CandidateResult(**{key: value for key, value in data.items() if key in allowed})


def normalize_results(results: Iterable[CandidateResult | Mapping[str, Any]]) -> list[CandidateResult]:
    return [_coerce_candidate(result) for result in results]


def _require_score(value: Any, method: str, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise BenchmarkEvidenceError(f"{method} is missing numeric {field_name}.") from exc


def select_default_method(results: Iterable[CandidateResult | Mapping[str, Any]]) -> CandidateResult:
    """Return the winning candidate only when evidence is complete enough.

    Selection rules:
    - at least two methods are required for comparison
    - every candidate must include Browser Use evidence
    - every candidate must meet completeness >= 0.98
    - every candidate must have zero failures
    - winner is max efficiency, then detail, then effectiveness
    """

    candidates = normalize_results(results)
    if len(candidates) < 2:
        raise BenchmarkEvidenceError("At least two runtime method results are required.")

    missing_browser_evidence = [candidate.method for candidate in candidates if not candidate.has_browser_use_evidence]
    if missing_browser_evidence:
        raise BenchmarkEvidenceError(
            "Browser Use comparative evidence is required before changing the default method: "
            + ", ".join(sorted(missing_browser_evidence))
        )

    incomplete: list[str] = []
    for candidate in candidates:
        try:
            completeness = float(candidate.completeness)
        except (TypeError, ValueError):
            incomplete.append(candidate.method)
            continue
        if completeness < MIN_COMPLETENESS:
            incomplete.append(candidate.method)
    if incomplete:
        raise BenchmarkEvidenceError(
            f"Runtime method completeness must be >= {MIN_COMPLETENESS:.2f}: "
            + ", ".join(sorted(incomplete))
        )

    failed = [candidate.method for candidate in candidates if candidate.failed]
    if failed:
        raise BenchmarkEvidenceError(
            "Runtime methods with failures cannot be selected as the default: " + ", ".join(sorted(failed))
        )

    return max(candidates, key=lambda candidate: candidate.selection_score)


def build_results_payload(
    results: Iterable[CandidateResult | Mapping[str, Any]],
    *,
    winner: CandidateResult | str | None = None,
    default_changed: bool = False,
    status: str | None = None,
) -> dict[str, Any]:
    candidates = normalize_results(results)
    winner_method = winner.method if isinstance(winner, CandidateResult) else winner
    resolved_status = status or ("winner_selected" if winner_method else "awaiting_browser_use_evidence")
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "decision_status": {
            "default_changed": bool(default_changed),
            "winner": winner_method,
            "status": resolved_status,
            "required_evidence": {
                "browser_use_comparative_evidence": True,
                "minimum_completeness": MIN_COMPLETENESS,
                "no_failures": True,
            },
        },
        "candidates": [candidate.to_dict() for candidate in candidates],
    }


def write_results_json(
    path: str | Path,
    results: Iterable[CandidateResult | Mapping[str, Any]],
    winner: CandidateResult | str | None = None,
) -> dict[str, Any]:
    """Write benchmark results, allowing no-change reports with winner null."""

    payload = build_results_payload(results, winner=winner, default_changed=winner is not None)
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _load_results(path: str | Path) -> list[CandidateResult]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    raw_results = payload if isinstance(payload, list) else payload.get("candidates", [])
    if not isinstance(raw_results, list):
        raise BenchmarkEvidenceError("Input JSON must be a list or contain a candidates list.")
    return normalize_results(raw_results)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare social backfill runtime method benchmark results")
    parser.add_argument("--input", required=True, help="JSON file containing candidate benchmark results")
    parser.add_argument("--output", required=True, help="Path for normalized benchmark report JSON")
    parser.add_argument(
        "--select-default",
        action="store_true",
        help="Require Browser Use evidence and write the selected winner; otherwise winner remains null",
    )
    args = parser.parse_args()

    results = _load_results(args.input)
    winner = select_default_method(results) if args.select_default else None
    payload = write_results_json(args.output, results, winner=winner)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
