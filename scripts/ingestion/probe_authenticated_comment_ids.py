#!/usr/bin/env python3
"""One-shot Modal diagnostic; defaults to a no-IO plan, never ingestion writes.

An uncertain call is reconciled with --reconcile in the same output directory.
Reservations are conservative and are never refunded after a dispatch attempt.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ingestion.probe_child_reply_access import write_once

CAP = 12


def _require_target():
    if os.environ.get("MODAL_PROFILE") != "admin-56995" or os.environ.get("MODAL_ENVIRONMENT") != "main":
        raise ValueError("Explicit admin-56995 profile and main environment required")
    from scripts.modal.deploy_backend import verify_required_workspace

    verify_required_workspace()


def execute(options):
    _require_target()
    import modal

    from trr_backend.ingestion.fixed_validation import AccountBudget

    output = options.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(output, 0o700)
    result_path = output / "result.json"
    if result_path.exists():
        result = json.loads(result_path.read_text())
        print(json.dumps({"verdict": result["verdict"], "result": str(result_path), "reused": True}))
        return 0 if result["verdict"] == "passed" else 2

    if options.reconcile:
        dispatch = json.loads((output / "dispatch.json").read_text())
        if dispatch.get("environment") != "main" or dispatch.get("app") != "trr-backend-jobs":
            raise ValueError("Dispatch target mismatch")
        call = modal.FunctionCall.from_id(dispatch["call_id"])
        run_id = dispatch["run_id"]
        reserved = CAP
    else:
        if (output / "started.json").exists():
            raise ValueError("One-shot already started; reconcile retained dispatch, never redispatch")
        function = modal.Function.from_name(
            "trr-backend-jobs", "probe_instagram_authenticated_comment_ids", environment_name="main"
        )
        run_id = str(uuid4())
        budget = AccountBudget(options.budget_path, "bravotv")
        with budget.stream():
            write_once(
                output / "started.json",
                {
                    "run_id": run_id,
                    "started_at": datetime.now(UTC).isoformat(),
                    "workspace": "admin-56995",
                    "environment": "main",
                    "app": "trr-backend-jobs",
                    "budget_path": str(budget.path),
                    "max_requests": CAP,
                },
            )
            reserved = 0
            try:
                for _ in range(CAP):
                    budget.consume("authenticated_comment_probe_reserved")
                    reserved += 1
                call = function.spawn()
                write_once(
                    output / "dispatch.json",
                    {
                        "run_id": run_id,
                        "call_id": call.object_id,
                        "environment": "main",
                        "app": "trr-backend-jobs",
                    },
                )
            except (Exception, KeyboardInterrupt):
                write_once(
                    output / "dispatch-uncertain.json",
                    {
                        "run_id": run_id,
                        "reserved_requests": reserved,
                        "status": "uncertain",
                        "redispatch_allowed": False,
                    },
                )
                print(json.dumps({"status": "uncertain", "result": str(output / "dispatch-uncertain.json")}))
                return 2
            return _collect(call, run_id, reserved, result_path)
    return _collect(call, run_id, reserved, result_path)


def _collect(call, run_id, reserved, path):
    try:
        result = call.get(timeout=330)
    except (Exception, KeyboardInterrupt):
        # A call ID has already been retained. Never dispatch again after timeout.
        print(json.dumps({"status": "uncertain", "run_id": run_id, "reconcile_required": True}))
        return 2
    write_once(path, {**result, "run_id": run_id, "reserved_requests": reserved})
    print(
        json.dumps(
            {
                "verdict": result["verdict"],
                "auth_state": result["auth_state"],
                "requests": result["requests"],
                "stop_reason": result["stop_reason"],
                "result": str(path),
            }
        )
    )
    return 0 if result["verdict"] == "passed" else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--budget-path", type=Path, required=True)
    parser.add_argument("--apply-once", action="store_true")
    parser.add_argument("--reconcile", action="store_true")
    options = parser.parse_args(argv)
    if not options.apply_once and not options.reconcile:
        print(
            json.dumps(
                {
                    "status": "plan",
                    "network_sent": False,
                    "max_requests": CAP,
                    "workspace": "admin-56995",
                    "environment": "main",
                    "app": "trr-backend-jobs",
                }
            )
        )
        return 0
    if options.apply_once and options.reconcile:
        parser.error("Choose apply-once or reconcile")
    try:
        return execute(options)
    except Exception as exc:
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
