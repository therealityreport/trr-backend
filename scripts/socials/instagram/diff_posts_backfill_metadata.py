#!/usr/bin/env python3
"""Compare two Instagram Backfill Posts progress/benchmark payloads."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _metric(payload: Mapping[str, Any], *keys: str) -> Any:
    return _nested(payload, *keys)


def _diff_number(before: Any, after: Any) -> dict[str, Any]:
    before_number = _number(before)
    after_number = _number(after)
    return {
        "before": before,
        "after": after,
        "delta": (after_number - before_number) if before_number is not None and after_number is not None else None,
    }


def build_run_metadata_diff(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    before_flags = _metadata_dict(before.get("feature_flags") or _nested(before, "posts_acceleration_flags", "flags"))
    after_flags = _metadata_dict(after.get("feature_flags") or _nested(after, "posts_acceleration_flags", "flags"))
    return {
        "request_counts": {
            "listing_pages": _diff_number(
                _metric(before, "request_counts", "listing_pages") or _metric(before, "listing_progress", "page_index"),
                _metric(after, "request_counts", "listing_pages") or _metric(after, "listing_progress", "page_index"),
            ),
            "doc_id_attempts": _diff_number(
                _metric(before, "request_counts", "doc_id_attempts"),
                _metric(after, "request_counts", "doc_id_attempts"),
            ),
            "detail_fetch_attempts": _diff_number(
                _metric(before, "request_counts", "detail_fetch_attempts"),
                _metric(after, "request_counts", "detail_fetch_attempts"),
            ),
        },
        "timing": {
            "pages_per_second": _diff_number(
                _metric(before, "metrics", "pages_per_second"),
                _metric(after, "metrics", "pages_per_second"),
            ),
            "posts_per_second": _diff_number(
                _metric(before, "metrics", "posts_per_second"),
                _metric(after, "metrics", "posts_per_second"),
            ),
            "warmup_duration_ms": _diff_number(
                _metric(before, "metrics", "warmup_duration_ms"),
                _metric(after, "metrics", "warmup_duration_ms"),
            ),
            "phase_durations_ms": {
                key: _diff_number(
                    _metric(before, "phase_durations_ms", key),
                    _metric(after, "phase_durations_ms", key),
                )
                for key in sorted(
                    set(_metadata_dict(before.get("phase_durations_ms")))
                    | set(_metadata_dict(after.get("phase_durations_ms")))
                )
            },
        },
        "doc_id": {
            "before": before.get("doc_id_used"),
            "after": after.get("doc_id_used"),
            "attempts_before": before.get("profile_posts_doc_ids"),
            "attempts_after": after.get("profile_posts_doc_ids"),
        },
        "proxy": {
            "before": _metadata_dict(before.get("proxy_pacing")),
            "after": _metadata_dict(after.get("proxy_pacing")),
        },
        "warmup_pool": {
            "before": _metadata_dict(before.get("warmup_pool")),
            "after": _metadata_dict(after.get("warmup_pool")),
        },
        "bidirectional_probe": {
            "before": _metadata_dict(before.get("bidirectional_probe")),
            "after": _metadata_dict(after.get("bidirectional_probe")),
        },
        "feature_flags": {
            "before": before_flags,
            "after": after_flags,
            "changed": {
                key: {"before": before_flags.get(key), "after": after_flags.get(key)}
                for key in sorted(set(before_flags) | set(after_flags))
                if before_flags.get(key) != after_flags.get(key)
            },
        },
        "field_coverage": {
            "before": _metadata_dict(before.get("field_coverage")),
            "after": _metadata_dict(after.get("field_coverage")),
        },
    }


def _load_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    diff = build_run_metadata_diff(_load_json(args.before), _load_json(args.after))
    print(json.dumps(diff, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
