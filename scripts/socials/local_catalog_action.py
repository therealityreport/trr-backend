#!/usr/bin/env python3
"""Run shared social catalog actions locally without the admin API."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SUPPORTED_SOURCE_SCOPES = ("network", "news", "creator", "community", "bravo")
SUPPORTED_ACTIONS = ("backfill", "sync_recent", "sync_newer", "fill_missing_posts")
SUPPORTED_SELECTED_TASKS = ("post_details", "comments", "media")
EXECUTION_OWNERS = ("local-inline", "queue")
QUEUE_ACCEPTED_STATUSES = {"queued", "pending", "running", "submitted"}
LOCAL_SCRIPT_LABEL = "local-script:local_catalog_action.py"
BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION = "RUN BRAVOTV INSTAGRAM BACKFILL"
LOCAL_CATALOG_DB_POOL_DEFAULTS = {
    "TRR_DB_POOL_MAXCONN": "4",
    "TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN": "4",
    "TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN": "4",
    "TRR_SOCIAL_PROGRESS_DB_POOL_MAXCONN": "4",
}


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
    parser.add_argument(
        "--comment-anchor-source-id",
        dest="comment_anchor_source_ids",
        action="append",
        default=[],
        help="Limit comment repair to a specific platform post/source id. Repeat for multiple anchors.",
    )
    parser.add_argument(
        "--date-start",
        help="Optional bounded-window backfill start, for example 2026-01-01T00:00:00Z.",
    )
    parser.add_argument(
        "--date-end",
        help="Optional bounded-window backfill end, for example 2026-12-31T23:59:59Z.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned catalog action without launching or executing any jobs.",
    )
    parser.add_argument(
        "--execution-owner",
        choices=EXECUTION_OWNERS,
        default="local-inline",
        help="Run a local inline worker, or submit work to the healthy Modal-backed queue.",
    )
    parser.add_argument(
        "--confirm-bravotv-instagram-backfill",
        default="",
        help=(
            "Required confirmation phrase for live BravoTV Instagram full-history backfills: "
            f"{BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION}"
        ),
    )
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    try:
        _validate_backfill_window(
            action=args.action,
            date_start=args.date_start,
            date_end=args.date_end,
        )
    except ValueError as exc:
        parser.error(str(exc))
    return args


def _parse_timestamp(value: str, *, option: str) -> datetime:
    normalized = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{option} must be a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{option} must include a timezone")
    return parsed


def _validate_backfill_window(*, action: str, date_start: str | None, date_end: str | None) -> None:
    start = str(date_start or "").strip()
    end = str(date_end or "").strip()
    if not start and not end:
        return
    if str(action or "").strip().lower() != "backfill":
        raise ValueError("--date-start and --date-end are only supported with --action backfill")
    if not start or not end:
        raise ValueError("--date-start and --date-end must be supplied together")
    if _parse_timestamp(end, option="--date-end") < _parse_timestamp(start, option="--date-start"):
        raise ValueError("--date-end must not be earlier than --date-start")


def apply_workspace_runtime_env(*, repo_root: Path) -> Any:
    from scripts._workspace_runtime_env import apply_workspace_runtime_env as _apply_workspace_runtime_env

    return _apply_workspace_runtime_env(repo_root=repo_root)


def apply_local_catalog_db_pool_defaults() -> dict[str, str]:
    applied: dict[str, str] = {}
    for key, value in LOCAL_CATALOG_DB_POOL_DEFAULTS.items():
        if os.environ.get(key):
            continue
        os.environ[key] = value
        applied[key] = value
    return applied


def _load_module(name: str) -> Any:
    module = sys.modules.get(name)
    if module is not None:
        return module
    return importlib.import_module(name)


def _inline_worker_id(platform: str) -> str:
    normalized = str(platform or "").strip().lower() or "unknown"
    return f"local-script:catalog:{normalized}"


def _execution_owner(args: argparse.Namespace) -> str:
    return str(getattr(args, "execution_owner", "local-inline") or "local-inline").strip().lower()


def _execution_launch_kwargs(*, owner: str, worker_id: str) -> dict[str, Any]:
    if owner == "queue":
        return {
            "inline_worker_id": None,
            "allow_local_dev_inline_bypass": False,
            "execution_preference": "auto",
        }
    return {
        "inline_worker_id": worker_id,
        "allow_local_dev_inline_bypass": True,
        "execution_preference": "prefer_local_inline",
    }


def _normalize_handle(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("@")


def _is_bravotv_instagram_backfill(args: argparse.Namespace) -> bool:
    return (
        str(getattr(args, "action", "") or "").strip().lower() == "backfill"
        and str(getattr(args, "platform", "") or "").strip().lower() == "instagram"
        and _normalize_handle(getattr(args, "account", "")) == "bravotv"
    )


def _selected_tasks(args: argparse.Namespace) -> list[str]:
    return [str(task).strip() for task in list(getattr(args, "selected_tasks", []) or []) if str(task).strip()]


def _comment_anchor_source_ids(args: argparse.Namespace) -> list[str]:
    return [
        str(item).strip() for item in list(getattr(args, "comment_anchor_source_ids", []) or []) if str(item).strip()
    ]


def _bravotv_backfill_confirmation_valid(args: argparse.Namespace) -> bool:
    if not _is_bravotv_instagram_backfill(args):
        return True
    supplied = str(getattr(args, "confirm_bravotv_instagram_backfill", "") or "").strip()
    return supplied == BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION


def _dry_run_payload(args: argparse.Namespace) -> dict[str, Any]:
    selected_tasks = _selected_tasks(args)
    execution_owner = _execution_owner(args)
    return {
        "status": "dry_run",
        "would_launch": False,
        "execution_owner": execution_owner,
        "would_execute_inline_worker": execution_owner == "local-inline",
        "platform": str(getattr(args, "platform", "") or "").strip().lower(),
        "account": _normalize_handle(getattr(args, "account", "")),
        "source_scope": str(getattr(args, "source_scope", "") or "").strip().lower(),
        "action": str(getattr(args, "action", "") or "").strip().lower(),
        "selected_tasks": selected_tasks,
        "default_selected_tasks": ["post_details", "comments", "media"] if not selected_tasks else None,
        "comment_anchor_source_ids": _comment_anchor_source_ids(args),
        "date_start": str(getattr(args, "date_start", "") or "").strip() or None,
        "date_end": str(getattr(args, "date_end", "") or "").strip() or None,
        "catalog_action_scope": (
            "bounded_window"
            if str(getattr(args, "date_start", "") or "").strip() and str(getattr(args, "date_end", "") or "").strip()
            else "full_history"
        ),
        "confirmation_required": _is_bravotv_instagram_backfill(args),
        "required_confirmation": (
            BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION if _is_bravotv_instagram_backfill(args) else None
        ),
    }


def _bounded_instagram_dry_run_preview(args: argparse.Namespace) -> dict[str, Any]:
    date_start = str(getattr(args, "date_start", "") or "").strip()
    date_end = str(getattr(args, "date_end", "") or "").strip()
    if not (
        str(getattr(args, "action", "") or "").strip().lower() == "backfill"
        and str(getattr(args, "platform", "") or "").strip().lower() == "instagram"
        and date_start
        and date_end
    ):
        return {}
    try:
        apply_local_catalog_db_pool_defaults()
        load_dotenv()
        apply_workspace_runtime_env(repo_root=REPO_ROOT)
        analytics_repo = _load_module("trr_backend.repositories.social_season_analytics")
        preview = analytics_repo.preview_social_account_catalog_backfill_target(
            str(getattr(args, "platform", "") or "").strip().lower(),
            _normalize_handle(getattr(args, "account", "")),
            date_start=_parse_timestamp(date_start, option="--date-start"),
            date_end=_parse_timestamp(date_end, option="--date-end"),
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "bounded_preview_available": False,
            "bounded_preview_error": f"{type(exc).__name__}: {exc}",
        }
    return {
        "bounded_preview_available": True,
        "bounded_preview_error": None,
        "catalog_total": int(preview.get("catalog_total") or 0),
        "materialized_total": int(preview.get("materialized_total") or 0),
        "completion_target_posts": int(preview.get("completion_target_posts") or 0),
        "completion_target_source": str(preview.get("completion_target_source") or "").strip() or None,
    }


def _blocked_confirmation_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "status": "blocked",
        "reason": "bravotv_instagram_backfill_confirmation_required",
        "platform": str(getattr(args, "platform", "") or "").strip().lower(),
        "account": _normalize_handle(getattr(args, "account", "")),
        "action": str(getattr(args, "action", "") or "").strip().lower(),
        "required_confirmation": BRAVOTV_INSTAGRAM_BACKFILL_CONFIRMATION,
        "dry_run_hint": "Re-run with --dry-run to inspect the plan without launching jobs.",
    }


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


def _execution_result_status(result: Any) -> str:
    if not isinstance(result, Mapping):
        return "unknown"
    return str(result.get("status") or "").strip().lower() or "unknown"


def _cancel_interrupted_runs(
    *,
    analytics_repo: Any,
    platform: str | None,
    account_handle: str,
    run_ids: list[str],
    cancelled_by: str,
) -> dict[str, Any]:
    cleanup: dict[str, Any] = {
        "attempted": False,
        "cancelled_run_ids": [],
        "failed_run_ids": [],
    }
    if not platform or not run_ids:
        return cleanup
    for run_id in run_ids:
        cleanup["attempted"] = True
        try:
            result = analytics_repo.cancel_social_account_catalog_run(
                platform=platform,
                account_handle=account_handle,
                run_id=run_id,
                cancelled_by=cancelled_by,
            )
            cleanup["cancelled_run_ids"].append(str(result.get("run_id") or run_id))
        except Exception as exc:  # noqa: BLE001
            cleanup["failed_run_ids"].append({"run_id": run_id, "error": str(exc)})
    return cleanup


def _execute_run(
    payload: dict[str, Any],
    worker_id: str,
    control_plane: Any,
    *,
    analytics_repo: Any,
    account_handle: str,
) -> int:
    run_ids = _payload_run_ids(payload)
    if not run_ids:
        print(json.dumps({"executed_run_ids": [], **payload}, sort_keys=True))
        return 1
    platform = str(payload.get("platform") or "").strip().lower() or None
    supported_platforms = [platform] if platform else None
    executed_run_ids: list[str] = []
    outcomes: list[dict[str, str]] = []
    try:
        for index, run_id in enumerate(run_ids, start=1):
            executed_run_ids.append(run_id)
            result = control_plane.execute_run_with_inline_worker_registration(
                run_id,
                worker_id=f"{worker_id}:{index}",
                platform=platform,
                supported_platforms=supported_platforms,
            )
            status = _execution_result_status(result)
            outcomes.append({"run_id": run_id, "status": status})
            if status != "completed":
                cleanup = _cancel_interrupted_runs(
                    analytics_repo=analytics_repo,
                    platform=platform,
                    account_handle=account_handle,
                    run_ids=run_ids,
                    cancelled_by=f"{LOCAL_SCRIPT_LABEL}:execution_incomplete",
                )
                print(
                    json.dumps(
                        {
                            **payload,
                            "status": "failed",
                            "reason": "local_run_did_not_complete",
                            "executed_run_ids": executed_run_ids,
                            "run_outcomes": outcomes,
                            "launch_cleanup": cleanup,
                        },
                        sort_keys=True,
                    )
                )
                return 1
    except KeyboardInterrupt:
        cleanup = _cancel_interrupted_runs(
            analytics_repo=analytics_repo,
            platform=platform,
            account_handle=account_handle,
            run_ids=run_ids,
            cancelled_by=f"{LOCAL_SCRIPT_LABEL}:interrupted",
        )
        print(
            json.dumps(
                {
                    "status": "interrupted",
                    "executed_run_ids": executed_run_ids,
                    "run_outcomes": outcomes,
                    "interrupted_cleanup": cleanup,
                },
                sort_keys=True,
            )
        )
        return 130
    except Exception as exc:  # noqa: BLE001
        cleanup = _cancel_interrupted_runs(
            analytics_repo=analytics_repo,
            platform=platform,
            account_handle=account_handle,
            run_ids=run_ids,
            cancelled_by=f"{LOCAL_SCRIPT_LABEL}:execution_error",
        )
        print(
            json.dumps(
                {
                    **payload,
                    "status": "failed",
                    "reason": "local_run_execution_error",
                    "error": str(exc),
                    "executed_run_ids": executed_run_ids,
                    "run_outcomes": outcomes,
                    "launch_cleanup": cleanup,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {"run_id": run_ids[0], "executed_run_ids": run_ids, "run_outcomes": outcomes, "status": "completed"},
            sort_keys=True,
        )
    )
    return 0


def _assert_modal_queue_owner(*, analytics_repo: Any, platform: str, account_handle: str) -> None:
    if not bool(analytics_repo.is_queue_enabled()):
        raise RuntimeError("Queue ownership requires SOCIAL_QUEUE_ENABLED and a Modal-backed worker.")
    analytics_repo.assert_worker_available_when_queue_enabled(
        required_execution_backend="modal",
        platform=platform,
        account_handle=account_handle,
    )


def _queue_submission_result(payload: dict[str, Any], *, execution_owner: str) -> int:
    run_ids = _payload_run_ids(payload)
    launch_status = str(payload.get("status") or "").strip().lower()
    if run_ids and launch_status in QUEUE_ACCEPTED_STATUSES:
        print(
            json.dumps(
                {
                    **payload,
                    "status": "submitted",
                    "submission_status": launch_status,
                    "execution_owner": execution_owner,
                    "submitted_run_ids": run_ids,
                },
                sort_keys=True,
            )
        )
        return 0
    print(
        json.dumps(
            {
                **payload,
                "status": "failed",
                "reason": "queue_launch_not_accepted",
                "execution_owner": execution_owner,
                "submitted_run_ids": run_ids,
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    return 1


def _start_backfill(
    analytics_repo: Any,
    *,
    platform: str,
    account: str,
    source_scope: str,
    worker_id: str,
    execution_owner: str,
    scope: str,
    selected_tasks: list[str] | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    comment_anchor_source_ids: list[str] | None = None,
) -> dict[str, Any]:
    normalized_selected_tasks = [str(task).strip() for task in (selected_tasks or []) if str(task).strip()]
    normalized_anchor_ids = [str(item).strip() for item in (comment_anchor_source_ids or []) if str(item).strip()]
    anchor_config = {platform: normalized_anchor_ids} if normalized_anchor_ids else None
    execution_kwargs = _execution_launch_kwargs(owner=execution_owner, worker_id=worker_id)
    if normalized_selected_tasks:
        return analytics_repo.launch_social_account_catalog_backfill(
            platform=platform,
            account_handle=account,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
            initiated_by=LOCAL_SCRIPT_LABEL,
            **execution_kwargs,
            selected_tasks=normalized_selected_tasks,
            comment_anchor_source_ids=anchor_config,
        )
    return analytics_repo.start_social_account_catalog_backfill(
        platform=platform,
        account_handle=account,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
        initiated_by=LOCAL_SCRIPT_LABEL,
        **execution_kwargs,
        catalog_action="backfill",
        catalog_action_scope=scope,
        comment_anchor_source_ids=anchor_config,
    )


def _dispatch_fill_missing_posts(
    analytics_repo: Any,
    *,
    platform: str,
    account: str,
    source_scope: str,
    worker_id: str,
    execution_owner: str,
) -> dict[str, Any]:
    execution_kwargs = _execution_launch_kwargs(owner=execution_owner, worker_id=worker_id)
    gap_analysis = analytics_repo.get_social_account_catalog_gap_analysis(platform, account)
    recommended_action = str(gap_analysis.get("recommended_action") or "").strip().lower()

    if recommended_action == "sync_newer":
        return analytics_repo.sync_newer_social_account_catalog(
            platform=platform,
            account_handle=account,
            source_scope=source_scope,
            initiated_by=LOCAL_SCRIPT_LABEL,
            **execution_kwargs,
        )
    if recommended_action == "backfill_posts":
        return _start_backfill(
            analytics_repo,
            platform=platform,
            account=account,
            source_scope=source_scope,
            worker_id=worker_id,
            execution_owner=execution_owner,
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
            execution_owner=execution_owner,
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
    if bool(getattr(args, "dry_run", False)):
        print(json.dumps({**_dry_run_payload(args), **_bounded_instagram_dry_run_preview(args)}, sort_keys=True))
        return 0
    if not _bravotv_backfill_confirmation_valid(args):
        print(json.dumps(_blocked_confirmation_payload(args), sort_keys=True), file=sys.stderr)
        return 2
    pool_defaults = apply_local_catalog_db_pool_defaults()
    load_dotenv()
    apply_workspace_runtime_env(repo_root=REPO_ROOT)
    print(
        json.dumps(
            {
                "local_catalog_db_pool_defaults": pool_defaults,
                "local_catalog_db_pool_default_keys": sorted(LOCAL_CATALOG_DB_POOL_DEFAULTS),
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )

    analytics_repo = _load_module("trr_backend.repositories.social_season_analytics")
    worker_id = _inline_worker_id(args.platform)
    execution_owner = _execution_owner(args)
    execution_kwargs = _execution_launch_kwargs(owner=execution_owner, worker_id=worker_id)

    try:
        if execution_owner == "queue":
            _assert_modal_queue_owner(
                analytics_repo=analytics_repo,
                platform=args.platform,
                account_handle=args.account,
            )
        if args.action == "backfill":
            date_start = str(getattr(args, "date_start", "") or "").strip() or None
            date_end = str(getattr(args, "date_end", "") or "").strip() or None
            payload = _start_backfill(
                analytics_repo,
                platform=args.platform,
                account=args.account,
                source_scope=args.source_scope,
                worker_id=worker_id,
                execution_owner=execution_owner,
                scope="bounded_window" if date_start and date_end else "full_history",
                selected_tasks=_selected_tasks(args),
                date_start=date_start,
                date_end=date_end,
                comment_anchor_source_ids=_comment_anchor_source_ids(args),
            )
        elif args.action == "sync_recent":
            payload = analytics_repo.sync_recent_social_account_catalog(
                platform=args.platform,
                account_handle=args.account,
                source_scope=args.source_scope,
                initiated_by=LOCAL_SCRIPT_LABEL,
                **execution_kwargs,
            )
        elif args.action == "sync_newer":
            payload = analytics_repo.sync_newer_social_account_catalog(
                platform=args.platform,
                account_handle=args.account,
                source_scope=args.source_scope,
                initiated_by=LOCAL_SCRIPT_LABEL,
                **execution_kwargs,
            )
        else:
            payload = _dispatch_fill_missing_posts(
                analytics_repo,
                platform=args.platform,
                account=args.account,
                source_scope=args.source_scope,
                worker_id=worker_id,
                execution_owner=execution_owner,
            )
    except Exception as exc:  # noqa: BLE001
        if execution_owner == "queue":
            print(
                json.dumps(
                    {
                        "status": "blocked",
                        "reason": "queue_owner_unavailable",
                        "execution_owner": execution_owner,
                        "error": str(exc),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 1
        print(str(exc), file=sys.stderr)
        return 1

    if execution_owner == "queue":
        return _queue_submission_result(payload, execution_owner=execution_owner)

    control_plane = _load_module("trr_backend.socials.control_plane")
    return _execute_run(
        payload,
        worker_id,
        control_plane,
        analytics_repo=analytics_repo,
        account_handle=args.account,
    )


if __name__ == "__main__":
    raise SystemExit(main())
