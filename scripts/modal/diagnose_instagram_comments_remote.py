#!/usr/bin/env python3
"""Run the deployed Instagram comments lane and print a compact diagnostic."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import modal

from trr_backend.db import pg
from trr_backend.modal_dispatch import modal_app_name, modal_environment_name, modal_social_comments_job_function_name
from trr_backend.repositories import social_season_analytics as social_repo


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", default="thetraitorsus", help="Instagram handle used for browser/account context")
    parser.add_argument("--shortcode", required=True, help="Instagram post shortcode to probe")
    parser.add_argument("--source-scope", default="network", choices=["network", "creator", "community", "news"])
    parser.add_argument("--max-comments", type=int, default=1, help="Maximum top-level comments to request")
    parser.add_argument("--wait-seconds", type=int, default=300, help="Seconds to poll if the probe is claimed async")
    parser.add_argument("--keep-job", action="store_true", help="Keep diagnostic rows after completion")
    return parser.parse_args()


def _safe_runtime_metadata(runtime_metadata: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(runtime_metadata, dict):
        return {}
    allowed_keys = {
        "warmup_cookie_count",
        "warmup_cookie_names",
        "selected_proxy_fingerprint",
        "proxy_session_mode",
        "api_delay_seconds",
        "transport",
        "request_count",
        "retry_reason_counts",
        "lane_diagnostics",
        "challenge_responses",
    }
    return {key: runtime_metadata[key] for key in allowed_keys if key in runtime_metadata}


def _create_probe_job(
    *,
    shortcode: str,
    account_handle: str,
    source_scope: str = "network",
    max_comments: int = 1,
) -> tuple[str, str]:
    probe_token = os.urandom(8).hex()
    run_config: dict[str, Any] = {
        "platform": "instagram",
        "account": account_handle,
        "source_scope": source_scope,
        "mode": "single_post",
        "stage": social_repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        "refresh_policy": "single_post",
        "max_posts": None,
        "max_comments_per_post": max(1, int(max_comments)),
        "comments_enable_media_followups": False,
        "launch_group_id": f"diagnostic:{probe_token}",
        "required_worker_lane": social_repo.INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE,
        "required_execution_backend": "modal",
        "allow_local_dev_inline_bypass": False,
        "ingest_mode": "comments_only",
        "source_id": shortcode,
        "target_source_ids": [shortcode],
    }
    run_row_or_id = social_repo._create_run(  # noqa: SLF001
        None,
        source_scope=source_scope,
        initiated_by="modal_comments_diagnostic",
        config=run_config,
        status="queued",
    )
    if isinstance(run_row_or_id, dict):
        run_id = str(run_row_or_id.get("id") or "").strip()
    else:
        run_id = str(run_row_or_id or "").strip()
    if not run_id:
        raise RuntimeError("Failed to create diagnostic run")
    job_id = social_repo._create_job(  # noqa: SLF001
        None,
        run_id=run_id,
        platform="instagram",
        source_scope=source_scope,
        job_type="comments",
        stage=social_repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE,
        config=run_config,
        initiated_by="modal_comments_diagnostic",
        status="queued",
        priority=105,
    )
    return run_id, job_id


def _load_probe_job(job_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        select
          id::text,
          run_id::text,
          platform,
          job_type,
          status,
          items_found,
          error_message,
          started_at,
          completed_at,
          created_at,
          source_scope,
          metadata,
          config,
          worker_id,
          heartbeat_at,
          last_error_code,
          last_error_class
        from social.scrape_jobs
        where id = %s
        """,
        [job_id],
    )


def _job_status(job_payload: dict[str, object] | None) -> str:
    return str((job_payload or {}).get("status") or "").strip().lower()


def _job_is_terminal(job_payload: dict[str, object] | None) -> bool:
    return _job_status(job_payload) in {"completed", "failed", "cancelled"}


def _wait_for_probe_job(job_id: str, *, wait_seconds: int) -> dict[str, Any] | None:
    deadline = time.monotonic() + max(0, int(wait_seconds or 0))
    last_row = _load_probe_job(job_id)
    while last_row and not _job_is_terminal(last_row) and time.monotonic() < deadline:
        time.sleep(5)
        last_row = _load_probe_job(job_id)
    return last_row


def _cleanup_probe_job(*, run_id: str, job_id: str) -> None:
    try:
        pg.fetch_one(
            """
            delete from social.scrape_jobs
             where id = %s
            returning id::text
            """,
            [job_id],
        )
    except Exception:  # noqa: BLE001
        pass
    try:
        pg.fetch_one(
            """
            delete from social.scrape_runs
             where id = %s
            returning id::text
            """,
            [run_id],
        )
    except Exception:  # noqa: BLE001
        pass


def _invoke_deployed_comments_lane(*, job_id: str) -> dict[str, object]:
    app_name = modal_app_name()
    function_name = modal_social_comments_job_function_name()
    environment_name = modal_environment_name()
    function_handle = modal.Function.from_name(app_name, function_name, environment_name=environment_name or None)
    return dict(function_handle.remote(job_id))


def _fetcher_runtime_from_deployed_result(deployed_result: dict[str, object]) -> dict[str, object] | None:
    job_payload = deployed_result.get("job") if isinstance(deployed_result, dict) else None
    if not isinstance(job_payload, dict):
        return None
    metadata = job_payload.get("metadata")
    if not isinstance(metadata, dict):
        return None
    fetcher_runtime = metadata.get("fetcher_runtime")
    return fetcher_runtime if isinstance(fetcher_runtime, dict) else None


def _fetcher_runtime_from_job_payload(job_payload: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(job_payload, dict):
        return None
    metadata = job_payload.get("metadata")
    if not isinstance(metadata, dict):
        return None
    fetcher_runtime = metadata.get("fetcher_runtime") or metadata.get("runtime_metadata")
    return fetcher_runtime if isinstance(fetcher_runtime, dict) else None


def main() -> int:
    args = parse_args()
    probe_target: dict[str, Any] = {
        "account_handle": args.account,
        "shortcode": args.shortcode,
        "source_scope": args.source_scope,
        "max_comments": args.max_comments,
    }
    payload: dict[str, object] = {
        "dispatch": {
            "app_name": modal_app_name(),
            "function_name": modal_social_comments_job_function_name(),
            "modal_environment": modal_environment_name(),
        },
        "probe_target": probe_target,
    }
    try:
        run_id, job_id = _create_probe_job(
            shortcode=args.shortcode,
            account_handle=args.account,
            source_scope=args.source_scope,
            max_comments=args.max_comments,
        )
        probe_target["run_id"] = run_id
        probe_target["job_id"] = job_id
    except pg.DatabaseServiceUnavailableError as exc:
        payload["error"] = {
            "class": type(exc).__name__,
            "message": str(exc),
            "dispatch": payload["dispatch"],
            "probe_target": payload["probe_target"],
        }
        print(json.dumps(payload, indent=2, default=str))
        return 1
    try:
        deployed_result = _invoke_deployed_comments_lane(job_id=job_id)
        payload["deployed_job"] = deployed_result
        deployed_job = deployed_result.get("job") if isinstance(deployed_result, dict) else None
        latest_job = deployed_job if isinstance(deployed_job, dict) else None
        if latest_job and not _job_is_terminal(latest_job) and args.wait_seconds > 0:
            latest_job = _wait_for_probe_job(job_id, wait_seconds=args.wait_seconds)
            payload["polled_job"] = latest_job
        payload["safe_runtime_metadata"] = _safe_runtime_metadata(
            _fetcher_runtime_from_job_payload(latest_job)
            or _fetcher_runtime_from_deployed_result(deployed_result)
        )
    except Exception as exc:  # noqa: BLE001
        payload["error"] = {
            "class": type(exc).__name__,
            "message": str(exc),
            "dispatch": payload["dispatch"],
            "probe_target": payload["probe_target"],
            "safe_runtime_metadata": payload.get("safe_runtime_metadata", {}),
        }
        return_code = 1
    else:
        return_code = 0
    finally:
        cleanup_job = _load_probe_job(job_id)
        if args.keep_job:
            payload["cleanup"] = {"skipped": True, "reason": "keep_job"}
        elif cleanup_job is None:
            payload["cleanup"] = {"skipped": True, "reason": "job_missing"}
        elif _job_is_terminal(cleanup_job):
            _cleanup_probe_job(run_id=run_id, job_id=job_id)
            payload["cleanup"] = {"skipped": False, "reason": "terminal_probe_rows_deleted"}
        else:
            payload["cleanup"] = {
                "skipped": True,
                "reason": "probe_still_active",
                "status": _job_status(cleanup_job),
                "job_id": job_id,
                "run_id": run_id,
            }

    print(json.dumps(payload, indent=2, default=str))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
