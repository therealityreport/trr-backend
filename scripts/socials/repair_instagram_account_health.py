#!/usr/bin/env python3
"""Repair one Instagram shared-account catalog health gap and report the result."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._workspace_runtime_env import apply_workspace_runtime_env  # noqa: E402

FOLLOWUP_STAGES = ("media_mirror", "post_classify")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_instagram_account_health",
        description=(
            "Run the Instagram account health flow: gap report, optional missing-post repair, "
            "follow-up drain, and final gap report."
        ),
    )
    parser.add_argument("--account", required=True, help="Instagram account handle, for example thetraitorsus")
    parser.add_argument("--source-scope", default="network", help="Shared source scope, default: network")
    parser.add_argument("--run-id", help="Existing run id to drain instead of launching a missing-post repair.")
    parser.add_argument("--apply", action="store_true", help="Launch repairs and drain follow-up jobs.")
    parser.add_argument("--max-run-seconds", type=int, default=600, help="Max seconds per worker drain stage.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the JSON payload.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _python_command() -> str:
    repo_venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    return str(repo_venv_python) if repo_venv_python.is_file() else sys.executable


def _normalize_account(account: str) -> str:
    return str(account or "").strip().lstrip("@").lower()


def _json_from_stdout(stdout: str) -> dict[str, Any]:
    lines = [line.strip() for line in str(stdout or "").splitlines() if line.strip()]
    for line in reversed(lines):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise ValueError("Command did not emit a JSON object")


def _run_json_command(command: list[str], *, timeout_seconds: int | None = None) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    payload = _json_from_stdout(completed.stdout)
    payload["returncode"] = int(completed.returncode or 0)
    if completed.returncode != 0:
        payload["stderr"] = completed.stderr.strip()
    return payload


def _local_catalog_fill_missing_command(*, account: str, source_scope: str) -> list[str]:
    return [
        _python_command(),
        str(REPO_ROOT / "scripts" / "socials" / "local_catalog_action.py"),
        "--platform",
        "instagram",
        "--account",
        _normalize_account(account),
        "--source-scope",
        str(source_scope or "network").strip().lower(),
        "--action",
        "fill_missing_posts",
    ]


def _worker_drain_command(*, run_id: str, stage: str, max_run_seconds: int) -> list[str]:
    return [
        _python_command(),
        str(REPO_ROOT / "scripts" / "socials" / "worker.py"),
        "--run-id",
        str(run_id).strip(),
        "--stage",
        stage,
        "--platform",
        "instagram",
        "--max-jobs-per-invocation",
        "100",
        "--max-run-seconds",
        str(max(30, int(max_run_seconds or 600))),
    ]


def _gap_analysis(account: str) -> dict[str, Any]:
    from trr_backend.repositories import social_season_analytics as social_repo

    return social_repo.get_social_account_catalog_gap_analysis("instagram", _normalize_account(account))


def _run_id_from_payload(payload: Mapping[str, Any] | dict[str, Any]) -> str | None:
    for key in ("run_id", "catalog_run_id"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    for value in list(payload.get("executed_run_ids") or []):
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return None


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()
    apply_workspace_runtime_env(repo_root=REPO_ROOT)

    before = _gap_analysis(args.account)
    recommended_action = str(before.get("recommended_action") or "").strip().lower()
    steps: list[dict[str, Any]] = [
        {"name": "gap_before", "status": "ok", "recommended_action": recommended_action, "gap_type": before.get("gap_type")}
    ]
    run_id = str(args.run_id or "").strip() or None

    if args.apply and not run_id and recommended_action not in {"", "none", "wait_for_active_run"}:
        repair_payload = _run_json_command(
            _local_catalog_fill_missing_command(account=args.account, source_scope=args.source_scope),
            timeout_seconds=max(900, int(args.max_run_seconds or 600) + 300),
        )
        run_id = _run_id_from_payload(repair_payload)
        steps.append(
            {
                "name": "fill_missing_posts",
                "status": "ok" if repair_payload.get("returncode") == 0 else "failed",
                "run_id": run_id,
                "returncode": repair_payload.get("returncode"),
            }
        )
    elif args.apply and recommended_action == "wait_for_active_run" and not run_id:
        steps.append(
            {
                "name": "fill_missing_posts",
                "status": "blocked",
                "reason": "active_run_present_pass_run_id_to_drain",
            }
        )
    elif not args.apply:
        steps.append({"name": "repair", "status": "planned_only", "would_apply_action": recommended_action})

    if args.apply and run_id:
        for stage in FOLLOWUP_STAGES:
            completed = subprocess.run(
                _worker_drain_command(run_id=run_id, stage=stage, max_run_seconds=args.max_run_seconds),
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
                timeout=max(30, int(args.max_run_seconds or 600)) + 60,
            )
            steps.append(
                {
                    "name": f"drain_{stage}",
                    "status": "ok" if completed.returncode == 0 else "failed",
                    "run_id": run_id,
                    "returncode": int(completed.returncode or 0),
                }
            )

    after = _gap_analysis(args.account)
    payload = {
        "account_handle": _normalize_account(args.account),
        "source_scope": str(args.source_scope or "network").strip().lower(),
        "apply": bool(args.apply),
        "run_id": run_id,
        "gap_before": before,
        "gap_after": after,
        "repaired": bool(args.apply and str(after.get("recommended_action") or "").strip().lower() == "none"),
        "steps": steps,
    }
    print(json.dumps(payload, indent=2 if args.pretty else None, sort_keys=True, default=str))
    return 0 if str(after.get("recommended_action") or "").strip().lower() == "none" else 1


if __name__ == "__main__":
    raise SystemExit(main())
