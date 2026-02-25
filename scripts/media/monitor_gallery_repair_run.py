#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="monitor_gallery_repair_run",
        description="Monitor offline gallery repair launchctl runs.",
    )
    parser.add_argument("--label", required=True, help="launchctl label for the run")
    parser.add_argument("--log-path", required=True, help="Log file path")
    parser.add_argument("--json-path", required=True, help="Output JSON artifact path")
    parser.add_argument(
        "--checkpoint-path",
        default=None,
        help="Optional checkpoint sidecar JSON path used while the run is still active.",
    )
    parser.add_argument(
        "--stale-minutes",
        type=int,
        default=240,
        help="Mark running run as stalled when log mtime exceeds this threshold (default: 240)",
    )
    parser.add_argument(
        "--now-epoch",
        type=float,
        default=None,
        help="Override current epoch seconds for deterministic checks/tests.",
    )
    return parser.parse_args(argv)


def _safe_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text == "-":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _launchctl_entry(label: str) -> dict[str, Any]:
    result = subprocess.run(
        ["launchctl", "list"],
        capture_output=True,
        text=True,
        check=False,
    )
    raw = result.stdout or ""
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        if parts[-1] != label:
            continue
        pid = _safe_int(parts[0])
        status = _safe_int(parts[1])
        return {
            "present": True,
            "pid": pid,
            "status": status,
            "running": pid is not None and pid > 0,
            "raw_line": line.strip(),
        }
    return {
        "present": False,
        "pid": None,
        "status": None,
        "running": False,
        "raw_line": None,
    }


def _read_json_summary(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"invalid_json:{exc.__class__.__name__}"
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return None, "invalid_json:missing_summary"
    return summary, None


def _evaluate_state(
    *,
    launchctl: dict[str, Any],
    log_path: Path,
    json_path: Path,
    checkpoint_path: Path | None,
    stale_minutes: int,
    now_epoch: float,
) -> dict[str, Any]:
    summary, summary_error = _read_json_summary(json_path)
    checkpoint_summary = None
    checkpoint_summary_error = None
    checkpoint_exists = False
    if checkpoint_path is not None:
        checkpoint_exists = checkpoint_path.exists()
        checkpoint_summary, checkpoint_summary_error = _read_json_summary(checkpoint_path)
    log_exists = log_path.exists()
    log_mtime_epoch = log_path.stat().st_mtime if log_exists else None
    log_age_minutes = None
    if log_mtime_epoch is not None:
        log_age_minutes = max(0.0, (now_epoch - log_mtime_epoch) / 60.0)

    base = {
        "label": launchctl.get("raw_line"),
        "launchctl": launchctl,
        "log_path": str(log_path),
        "json_path": str(json_path),
        "log_exists": log_exists,
        "log_age_minutes": round(log_age_minutes, 2) if isinstance(log_age_minutes, float) else None,
        "json_exists": json_path.exists(),
        "summary": summary,
        "summary_read_error": summary_error,
        "checkpoint_path": str(checkpoint_path) if checkpoint_path is not None else None,
        "checkpoint_exists": checkpoint_exists,
        "checkpoint_summary": checkpoint_summary,
        "checkpoint_summary_read_error": checkpoint_summary_error,
    }

    if summary is not None:
        error_count = int(summary.get("error", 0) or 0)
        if error_count == 0:
            return {"state": "completed-pass", **base}
        return {"state": "completed-fail", **base}

    if launchctl.get("running"):
        if checkpoint_summary is not None and int(checkpoint_summary.get("error", 0) or 0) > 0:
            return {"state": "running-with-errors", **base}
        stale_threshold = max(1, int(stale_minutes))
        if log_age_minutes is not None and log_age_minutes > stale_threshold:
            return {"state": "stalled", **base}
        return {"state": "healthy/running", **base}

    return {"state": "completed-fail", **base}


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    now_epoch = float(args.now_epoch) if args.now_epoch is not None else time.time()
    launchctl = _launchctl_entry(str(args.label))
    result = _evaluate_state(
        launchctl=launchctl,
        log_path=Path(args.log_path).expanduser(),
        json_path=Path(args.json_path).expanduser(),
        checkpoint_path=Path(args.checkpoint_path).expanduser() if args.checkpoint_path else None,
        stale_minutes=int(args.stale_minutes),
        now_epoch=now_epoch,
    )
    print(json.dumps(result, indent=2))

    state = result["state"]
    if state in {"healthy/running", "completed-pass"}:
        return 0
    if state == "stalled":
        return 2
    if state == "running-with-errors":
        return 3
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
