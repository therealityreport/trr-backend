#!/usr/bin/env python3
# ruff: noqa: E402
"""Run safe deployed Modal smoke probes for non-mutating worker families."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._workspace_runtime_env import apply_workspace_runtime_env
from scripts.modal.verify_modal_readiness import (
    DEFAULT_APP_NAME,
    core_worker_runtime_probe_functions,
    invoke_runtime_probe,
)
from trr_backend.utils.env import load_env


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-name",
        default=str(os.getenv("TRR_MODAL_APP_NAME") or DEFAULT_APP_NAME).strip() or DEFAULT_APP_NAME,
        help=f"Modal app name to probe (default: {DEFAULT_APP_NAME})",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def _load_modal_function_class() -> Any:
    try:
        import modal
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Modal SDK is unavailable") from exc
    function_class = getattr(modal, "Function", None)
    if function_class is None:
        raise RuntimeError("Modal Function helpers are unavailable")
    return function_class


def run_smoke(*, app_name: str) -> dict[str, Any]:
    apply_workspace_runtime_env(repo_root=REPO_ROOT)
    load_env()
    started = time.monotonic()
    function_class = _load_modal_function_class()
    probes: list[dict[str, Any]] = []
    for worker_family, function_name in core_worker_runtime_probe_functions().items():
        probe_started = time.monotonic()
        try:
            function_handle = function_class.from_name(app_name, function_name)
            payload = invoke_runtime_probe(function_handle=function_handle, worker_family=worker_family)
        except Exception as exc:  # noqa: BLE001
            payload = {
                "worker_family": worker_family,
                "healthy": False,
                "reason": "probe_resolution_failed",
                "detail": {
                    "phase": "function_resolution",
                    "exception_class": type(exc).__name__,
                    "message": str(exc)[:240],
                },
            }
        payload.setdefault("function_name", function_name)
        payload["elapsed_seconds"] = round(time.monotonic() - probe_started, 3)
        probes.append(payload)
    return {
        "ok": all(bool(item.get("healthy")) for item in probes),
        "app_name": app_name,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "probes": probes,
    }


def main() -> int:
    args = _parse_args()
    payload = run_smoke(app_name=args.app_name)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        status = "ok" if payload["ok"] else "failed"
        print(f"Modal deployed worker smoke: {status}")
        for item in payload["probes"]:
            worker_family = item.get("worker_family")
            function_name = item.get("function_name")
            reason = item.get("reason")
            healthy = "ok" if item.get("healthy") else "failed"
            print(f"- {worker_family} ({function_name}): {healthy} [{reason}]")
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
