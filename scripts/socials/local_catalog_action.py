#!/usr/bin/env python3
"""Run shared social catalog actions locally without the admin API."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SUPPORTED_SOURCE_SCOPES = ("network", "news", "creator", "community", "bravo")
SUPPORTED_ACTIONS = ("backfill", "sync_recent", "sync_newer", "fill_missing_posts", "fill_missing_photos")
SUPPORTED_SELECTED_TASKS = ("post_details", "comments", "media")
LOCAL_SCRIPT_LABEL = "local-script:local_catalog_action.py"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="local_catalog_action",
        description="Run a shared social catalog action locally against TRR-Backend.",
    )
    parser.add_argument("--platform", required=True, help="Platform slug, for example twitter")
    parser.add_argument("--account", required=True, help="Social handle, for example bravotv")
    parser.add_argument(
        "--source-scope",
        default="network",
        choices=SUPPORTED_SOURCE_SCOPES,
        help="Shared account source scope",
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=SUPPORTED_ACTIONS,
        help="Catalog action to run locally",
    )
    parser.add_argument(
        "--selected-task",
        dest="selected_tasks",
        action="append",
        choices=SUPPORTED_SELECTED_TASKS,
        default=[],
        help="Backfill task to run. Repeat for post_details, comments, and media.",
    )
    return parser.parse_args(argv if argv is not None else sys.argv[1:])


def apply_workspace_runtime_env(*, repo_root: Path) -> Any:
    from scripts._workspace_runtime_env import apply_workspace_runtime_env as _apply_workspace_runtime_env

    return _apply_workspace_runtime_env(repo_root=repo_root)


def _load_module(name: str) -> Any:
    module = sys.modules.get(name)
    if module is not None:
        return module
    return importlib.import_module(name)


def _inline_worker_id(platform: str) -> str:
    normalized = str(platform or "").strip().lower() or "unknown"
    return f"local-script:catalog:{normalized}"


def _payload_run_ids(payload: dict[str, Any]) -> list[str]:
    ordered = [
        str(payload.get("catalog_run_id") or "").strip(),
        str(payload.get("comments_run_id") or "").strip(),
        str(payload.get("run_id") or "").strip(),
    ]
    seen: set[str] = set()
    run_ids: list[str] = []
    for run_id in ordered:
        if not run_id or run_id in seen:
            continue
        seen.add(run_id)
        run_ids.append(run_id)
    return run_ids


def _execute_run(payload: dict[str, Any], worker_id: str, control_plane: Any) -> int:
    run_ids = _payload_run_ids(payload)
    if not run_ids:
        print("Catalog action did not return a run_id.", file=sys.stderr)
        return 1
    for index, run_id in enumerate(run_ids, start=1):
        control_plane.execute_run_with_inline_worker_registration(
            run_id,
            worker_id=f"{worker_id}:{index}",
        )
    print(json.dumps({"run_id": run_ids[0], "executed_run_ids": run_ids, "status": "completed"}, sort_keys=True))
    return 0


def _start_backfill(
    analytics_repo: Any,
    *,
    platform: str,
    account: str,
    source_scope: str,
    worker_id: str,
    scope: str,
    selected_tasks: list[str] | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
) -> dict[str, Any]:
    normalized_selected_tasks = [str(task).strip() for task in (selected_tasks or []) if str(task).strip()]
    if normalized_selected_tasks:
        return analytics_repo.launch_social_account_catalog_backfill(
            platform=platform,
            account_handle=account,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
            initiated_by=LOCAL_SCRIPT_LABEL,
            inline_worker_id=worker_id,
            allow_local_dev_inline_bypass=True,
            selected_tasks=normalized_selected_tasks,
        )
    return analytics_repo.start_social_account_catalog_backfill(
        platform=platform,
        account_handle=account,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
        initiated_by=LOCAL_SCRIPT_LABEL,
        inline_worker_id=worker_id,
        allow_local_dev_inline_bypass=True,
        catalog_action="backfill",
        catalog_action_scope=scope,
    )


def _dispatch_fill_missing_posts(
    analytics_repo: Any,
    *,
    platform: str,
    account: str,
    source_scope: str,
    worker_id: str,
) -> dict[str, Any]:
    gap_analysis = analytics_repo.get_social_account_catalog_gap_analysis(platform, account)
    recommended_action = str(gap_analysis.get("recommended_action") or "").strip().lower()

    if recommended_action == "sync_newer":
        return analytics_repo.sync_newer_social_account_catalog(
            platform=platform,
            account_handle=account,
            source_scope=source_scope,
            initiated_by=LOCAL_SCRIPT_LABEL,
            inline_worker_id=worker_id,
            allow_local_dev_inline_bypass=True,
        )
    if recommended_action == "backfill_posts":
        return _start_backfill(
            analytics_repo,
            platform=platform,
            account=account,
            source_scope=source_scope,
            worker_id=worker_id,
            scope="full_history",
        )
    if recommended_action == "bounded_window_backfill":
        repair_window_start = str(gap_analysis.get("repair_window_start") or "").strip() or None
        repair_window_end = str(gap_analysis.get("repair_window_end") or "").strip() or None
        if not repair_window_start or not repair_window_end:
            raise ValueError("Gap analysis requested a bounded-window backfill without a repair window.")
        return _start_backfill(
            analytics_repo,
            platform=platform,
            account=account,
            source_scope=source_scope,
            worker_id=worker_id,
            scope="bounded_window",
            date_start=repair_window_start,
            date_end=repair_window_end,
        )
    if recommended_action == "none":
        raise ValueError("No missing posts to fill right now.")
    if recommended_action == "wait_for_active_run":
        raise ValueError("A catalog run is already active for this account. Wait for it to finish before retrying.")
    raise ValueError(f"Unsupported gap-analysis recommendation: {recommended_action or 'unknown'}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv()
    apply_workspace_runtime_env(repo_root=REPO_ROOT)

    analytics_repo = _load_module("trr_backend.repositories.social_season_analytics")
    worker_id = _inline_worker_id(args.platform)

    try:
        if args.action == "backfill":
            payload = _start_backfill(
                analytics_repo,
                platform=args.platform,
                account=args.account,
                source_scope=args.source_scope,
                worker_id=worker_id,
                scope="full_history",
                selected_tasks=list(getattr(args, "selected_tasks", []) or []),
            )
        elif args.action == "sync_recent":
            payload = analytics_repo.sync_recent_social_account_catalog(
                platform=args.platform,
                account_handle=args.account,
                source_scope=args.source_scope,
                initiated_by=LOCAL_SCRIPT_LABEL,
                inline_worker_id=worker_id,
                allow_local_dev_inline_bypass=True,
            )
        elif args.action == "sync_newer":
            payload = analytics_repo.sync_newer_social_account_catalog(
                platform=args.platform,
                account_handle=args.account,
                source_scope=args.source_scope,
                initiated_by=LOCAL_SCRIPT_LABEL,
                inline_worker_id=worker_id,
                allow_local_dev_inline_bypass=True,
            )
        else:
            payload = _dispatch_fill_missing_posts(
                analytics_repo,
                platform=args.platform,
                account=args.account,
                source_scope=args.source_scope,
                worker_id=worker_id,
            )
    except Exception as exc:  # noqa: BLE001
        print(str(exc), file=sys.stderr)
        return 1

    control_plane = _load_module("trr_backend.socials.control_plane")
    return _execute_run(payload, worker_id, control_plane)


if __name__ == "__main__":
    raise SystemExit(main())
