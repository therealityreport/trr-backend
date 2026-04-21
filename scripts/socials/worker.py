#!/usr/bin/env python3
"""Social ingest queue worker (Postgres-backed via social.scrape_jobs)."""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import random
import socket
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime

from trr_backend.repositories.social_sync_orchestrator import tick_sync_orchestrator
from trr_backend.socials.control_plane import (
    _resolve_runtime_version_stamp,
    cancel_claimed_job_before_processing,
    claim_next_queued_jobs,
    ensure_media_mirror_s3_ready,
    execute_run,
    get_worker_auth_capabilities,
    mark_worker_stopped,
    process_claimed_job,
    reconcile_run_summaries,
    recover_stale_running_jobs,
    update_worker_heartbeat,
)
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS
from trr_backend.utils.env import load_env

logger = logging.getLogger("socials.worker")
_UNSET = object()


def _worker_lane_from_env() -> str | None:
    value = str(os.getenv("SOCIAL_WORKER_LANE") or "").strip().lower()
    return value or None


def _worker_script_label() -> str:
    return str(os.getenv("SOCIAL_WORKER_SCRIPT") or "scripts.socials.worker").strip() or "scripts.socials.worker"


class WorkerHeartbeat:
    def __init__(
        self,
        *,
        worker_id: str,
        stage: str | None,
        run_id: str | None,
        supported_platforms: list[str] | None = None,
    ):
        self._worker_id = worker_id
        self._supported_platforms = supported_platforms
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._interval_seconds = max(5.0, float(os.getenv("SOCIAL_WORKER_HEARTBEAT_INTERVAL_SEC") or "15"))
        self._state: dict[str, object | None] = {
            "stage": stage or "any",
            "status": "starting",
            "run_id": run_id,
            "current_job_id": None,
            "metadata": {
                "auth_capabilities": get_worker_auth_capabilities(),
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
                "runtime_version": dict(_resolve_runtime_version_stamp()),
                "worker_lane": _worker_lane_from_env(),
                "worker_script": _worker_script_label(),
            },
        }
        self._last_written_status: str | None = None
        self._last_written_at: float = 0.0

    def set_state(
        self,
        *,
        status: str | None | object = _UNSET,
        stage: str | None | object = _UNSET,
        run_id: str | None | object = _UNSET,
        current_job_id: str | None | object = _UNSET,
        metadata_updates: dict[str, object] | None = None,
    ) -> None:
        with self._lock:
            if status is not _UNSET:
                self._state["status"] = status
            if stage is not _UNSET:
                self._state["stage"] = stage
            if run_id is not _UNSET:
                self._state["run_id"] = run_id
            if current_job_id is not _UNSET:
                self._state["current_job_id"] = current_job_id
            if metadata_updates:
                metadata = dict(self._state.get("metadata") or {})
                metadata.update(metadata_updates)
                self._state["metadata"] = metadata

    def _snapshot(self) -> dict[str, object | None]:
        with self._lock:
            return {
                "status": self._state.get("status"),
                "stage": self._state.get("stage"),
                "run_id": self._state.get("run_id"),
                "current_job_id": self._state.get("current_job_id"),
                "metadata": dict(self._state.get("metadata") or {}),
            }

    def pulse(self) -> None:
        state = self._snapshot()
        current_status = str(state.get("status") or "idle")

        now = time.monotonic()
        # Skip write if status unchanged and interval not elapsed
        if self._last_written_status == current_status and (now - self._last_written_at) < self._interval_seconds:
            return

        try:
            update_worker_heartbeat(
                self._worker_id,
                stage=str(state.get("stage") or "any"),
                status=current_status,
                run_id=(str(state.get("run_id")) if state.get("run_id") else None),
                current_job_id=(str(state.get("current_job_id")) if state.get("current_job_id") else None),
                metadata=dict(state.get("metadata") or {}),
                supported_platforms=self._supported_platforms,
            )
            self._last_written_status = current_status
            self._last_written_at = now
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to update worker heartbeat: worker_id=%s error=%s", self._worker_id, exc)

    def start(self) -> None:
        self.pulse()

        def _loop() -> None:
            while not self._stop.wait(self._interval_seconds):
                self.pulse()

        self._thread = threading.Thread(target=_loop, daemon=True, name=f"{self._worker_id}:heartbeat")
        self._thread.start()

    def stop(self, *, reason: str = "shutdown") -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        metadata = self._snapshot().get("metadata") or {}
        metadata = {**dict(metadata), "stop_reason": reason}
        try:
            mark_worker_stopped(
                self._worker_id,
                stage=str(self._snapshot().get("stage") or "any"),
                metadata=metadata,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to mark worker stopped: worker_id=%s error=%s", self._worker_id, exc)


def _build_worker_id(explicit: str | None) -> str:
    if explicit:
        return explicit
    host = socket.gethostname()
    pid = os.getpid()
    return f"social-worker:{host}:{pid}"


def _resolve_int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return max(minimum, min(maximum, int(default)))
    try:
        parsed = int(raw)
    except ValueError:
        return max(minimum, min(maximum, int(default)))
    return max(minimum, min(maximum, parsed))


def _default_claim_batch_size_for_stage(stage: str | None) -> int:
    # Queue claims mark jobs running up front; batching post claims creates false
    # stale-heartbeat jobs for the later entries while one worker processes the first.
    return 1 if (stage or "").strip().lower() in {"posts", "shared_account_posts"} else 2


def _claim_stage_candidates(stage: str | None) -> tuple[str | None, ...]:
    normalized_stage = (stage or "").strip().lower() or None
    if normalized_stage == "comments":
        # Keep comments draining first, but let otherwise-idle comments workers
        # borrow post shards once the comment queue is empty.
        return ("comments", "posts")
    if normalized_stage == "comments_scrapling":
        return ("comments_scrapling",)
    return (stage,)


def _claim_jobs_for_stage_candidates(
    *,
    worker_id: str,
    stage: str | None,
    platform: str | None,
    limit: int,
) -> list[dict[str, object]]:
    attempted: set[str | None] = set()
    for candidate_stage in _claim_stage_candidates(stage):
        if candidate_stage in attempted:
            continue
        attempted.add(candidate_stage)
        candidate_limit = min(limit, _default_claim_batch_size_for_stage(candidate_stage))
        claimed_jobs = claim_next_queued_jobs(
            worker_id=worker_id,
            stage=candidate_stage,
            platform=platform,
            limit=candidate_limit,
        )
        if claimed_jobs:
            return claimed_jobs
    return []


def _metadata_dict(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_non_negative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _normalize_reason_counts(value: object) -> dict[str, int]:
    payload = value if isinstance(value, dict) else {}
    return {
        str(reason).strip(): _normalize_non_negative_int(count)
        for reason, count in payload.items()
        if str(reason).strip() and _normalize_non_negative_int(count) > 0
    }


def _persist_job_metadata(job_id: str, metadata: dict[str, object]) -> dict[str, object] | None:
    from trr_backend.db import pg

    row = pg.fetch_one(
        """
        update social.scrape_jobs
        set metadata = %s::jsonb
        where id = %s
        returning
          id::text,
          run_id::text as run_id,
          platform,
          job_type,
          status,
          items_found,
          error_message,
          metadata
        """,
        [json.dumps(metadata), job_id],
    )
    return dict(row) if isinstance(row, dict) else None


def _apply_post_persist_truthfulness_diagnostics(job: dict[str, object]) -> tuple[dict[str, object], bool]:
    metadata = _metadata_dict(job.get("metadata"))
    persist_summary = _metadata_dict(metadata.get("posts_scrapling_persist_diagnostics"))
    if not persist_summary:
        return job, False

    updated_metadata = copy.deepcopy(metadata)
    persist_counters = _metadata_dict(updated_metadata.get("persist_counters"))
    stage_counters = _metadata_dict(updated_metadata.get("stage_counters"))
    activity = _metadata_dict(updated_metadata.get("activity"))
    diagnostics = _metadata_dict(updated_metadata.get("diagnostics"))
    posts_upserted = _normalize_non_negative_int(persist_summary.get("posts_upserted"))
    posts_skipped = _normalize_non_negative_int(persist_summary.get("posts_skipped"))
    posts_skipped_by_reason = _normalize_reason_counts(persist_summary.get("posts_skipped_by_reason"))
    posts_checked = max(
        _normalize_non_negative_int(activity.get("posts_checked")),
        _normalize_non_negative_int(stage_counters.get("posts")),
        _normalize_non_negative_int(job.get("items_found")),
    )

    persist_counters["posts_upserted"] = posts_upserted
    persist_counters["posts_skipped"] = posts_skipped
    persist_counters["posts_skipped_by_reason"] = posts_skipped_by_reason
    updated_metadata["persist_counters"] = persist_counters

    silent_drop_detected = (
        str(job.get("status") or "").strip().lower() == "completed"
        and posts_checked > 0
        and posts_upserted == 0
    )
    diagnostics["post_persist_truthfulness"] = {
        "posts_checked": posts_checked,
        "posts_upserted": posts_upserted,
        "posts_skipped": posts_skipped,
        "posts_skipped_by_reason": posts_skipped_by_reason,
        "silent_drop_detected": silent_drop_detected,
    }
    if silent_drop_detected:
        diagnostics["post_persist_truthfulness"]["status_resolution"] = "completed_with_silent_drop_alert"
        diagnostics["post_persist_truthfulness"]["operator_summary"] = (
            "Instagram posts persistence completed with zero saved posts after checking live posts."
        )
    updated_metadata["diagnostics"] = diagnostics

    if silent_drop_detected:
        alerts = [dict(item) for item in list(updated_metadata.get("alerts") or []) if isinstance(item, dict)]
        alert_code = "instagram_posts_persist_zero_saved"
        if not any(str(item.get("code") or "") == alert_code for item in alerts):
            alerts.append(
                {
                    "code": alert_code,
                    "severity": "warning",
                    "message": (
                        f"Instagram posts Scrapling checked {posts_checked} posts but persisted 0 for "
                        f"@{str(updated_metadata.get('account') or '').strip().lstrip('@') or 'unknown'}."
                    ),
                }
            )
        updated_metadata["alerts"] = alerts

    if updated_metadata == metadata:
        return job, silent_drop_detected

    job_id = str(job.get("id") or "").strip()
    if not job_id:
        return {**job, "metadata": updated_metadata}, silent_drop_detected

    try:
        persisted_job = _persist_job_metadata(job_id, updated_metadata)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to persist post-persist diagnostics: worker metadata patch error for job_id=%s error=%s",
            job_id,
            exc,
        )
        return {**job, "metadata": updated_metadata}, silent_drop_detected

    return (persisted_job or {**job, "metadata": updated_metadata}), silent_drop_detected


def _single_target_catalog_progress_for_run(run_id: str, run_result: dict[str, object]) -> dict[str, object] | None:
    from trr_backend.repositories import social_season_analytics as repo

    config = _metadata_dict(run_result.get("config"))
    if (
        str(config.get("pipeline_ingest_mode") or "").strip().lower()
        != repo.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE
    ):
        return None
    platforms = [
        str(item or "").strip().lower()
        for item in list(config.get("platforms") or [])
        if str(item or "").strip()
    ]
    accounts = [
        str(item or "").strip().lstrip("@").lower()
        for item in list(config.get("accounts_override") or [])
        if str(item or "").strip()
    ]
    if len(platforms) != 1 or len(accounts) != 1:
        return None
    return repo.get_social_account_catalog_run_progress(platforms[0], accounts[0], run_id)


def _build_run_completion_metadata(run_id: str, run_result: dict[str, object]) -> dict[str, object]:
    metadata_updates: dict[str, object] = {"run_complete": True, "last_completed_run_id": run_id}
    try:
        progress = _single_target_catalog_progress_for_run(run_id, run_result)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Run completion diagnostics lookup failed: run_id=%s error=%s", run_id, exc)
        return metadata_updates
    if not progress:
        return metadata_updates
    alert_codes = [
        str(item.get("code") or "").strip()
        for item in list(progress.get("alerts") or [])
        if isinstance(item, dict) and str(item.get("code") or "").strip()
    ]
    metadata_updates["run_clean_completion"] = len(alert_codes) == 0
    if alert_codes:
        metadata_updates["run_completion_alerts"] = alert_codes[:5]
    diagnostics = _metadata_dict(progress.get("run_diagnostics"))
    if diagnostics:
        metadata_updates["run_completion_diagnostics"] = {
            "posts_upserted": _normalize_non_negative_int(diagnostics.get("posts_upserted")),
            "posts_skipped": _normalize_non_negative_int(diagnostics.get("posts_skipped")),
            "silent_drop_detected": bool(diagnostics.get("silent_drop_detected")),
        }
    return metadata_updates


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process queued social ingest jobs.")
    parser.add_argument("--worker-id", default=None, help="Explicit worker id")
    parser.add_argument("--interval", type=float, default=3.0, help="Idle sleep interval in seconds")
    parser.add_argument("--once", action="store_true", help="Process at most one job then exit")
    parser.add_argument("--run-id", default=None, help="Execute one specific run id then exit")
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of worker processes (default: 1). Applies to --run-id and queue mode.",
    )
    parser.add_argument(
        "--stage",
        choices=[
            "any",
            "posts",
            "comments",
            "comments_scrapling",
            "media_mirror",
            "comment_media_mirror",
            "shared_account_discovery",
            "shared_account_posts",
            "post_classify",
            "season_materialize",
            "analytics_refresh",
        ],
        default="any",
        help="Optional stage filter when claiming jobs",
    )
    parser.add_argument(
        "--tandem",
        action="store_true",
        help="For --run-id mode, run dedicated posts/comments workers in parallel",
    )
    parser.add_argument(
        "--posts-workers",
        type=int,
        default=1,
        help="Worker count for posts stage in --tandem mode (default: 1)",
    )
    parser.add_argument(
        "--comments-workers",
        type=int,
        default=1,
        help="Worker count for comments stage in --tandem mode (default: 1)",
    )
    parser.add_argument(
        "--platform",
        choices=list(SOCIAL_SUPPORTED_PLATFORMS),
        default=None,
        help="Optional platform filter when claiming jobs",
    )
    return parser.parse_args()


def _requires_media_mirror_s3_preflight(*, stage: str | None, platform: str | None) -> bool:
    normalized_stage = str(stage or "any").strip().lower() or "any"
    normalized_platform = str(platform or "any").strip().lower() or "any"
    return normalized_stage in {"media_mirror", "comment_media_mirror"} and normalized_platform in {
        "any",
        "instagram",
        "tiktok",
        "youtube",
        "twitter",
        "facebook",
        "threads",
    }


def _spawn_child_worker(
    *,
    worker_id: str,
    interval: float,
    stage: str | None = None,
    platform: str | None = None,
    run_id: str | None = None,
    once: bool = False,
) -> subprocess.Popen:
    cmd = [
        sys.executable,
        "-m",
        "scripts.socials.worker",
        "--worker-id",
        worker_id,
        "--parallel",
        "1",
    ]
    if run_id:
        cmd.extend(["--run-id", run_id])
    else:
        cmd.extend(["--interval", str(interval)])
        if once:
            cmd.append("--once")
    if stage:
        cmd.extend(["--stage", stage])
    if platform:
        cmd.extend(["--platform", platform])
    return subprocess.Popen(cmd, cwd=os.getcwd(), env=os.environ.copy())


def _wait_process(proc: subprocess.Popen, timeout: float | None = None) -> int | None:
    try:
        if timeout is None:
            return proc.wait()
        return proc.wait(timeout=timeout)
    except TypeError:
        return proc.wait()
    except subprocess.TimeoutExpired:
        return None


def _poll_process(proc: subprocess.Popen) -> int | None:
    poll = getattr(proc, "poll", None)
    if callable(poll):
        try:
            return poll()
        except Exception:  # noqa: BLE001
            return None
    return None


def _stop_process(proc: subprocess.Popen, *, force: bool) -> None:
    action = getattr(proc, "kill" if force else "terminate", None)
    if callable(action):
        try:
            action()
        except Exception:  # noqa: BLE001
            return


def _wait_for_children(
    children: list[subprocess.Popen],
    *,
    context_label: str,
    terminate_grace_seconds: float = 5.0,
) -> int:
    exit_code = 0
    try:
        for proc in children:
            rc = _wait_process(proc)
            if rc is None:
                continue
            if rc != 0 and exit_code == 0:
                exit_code = rc
                logger.error("%s child exited non-zero: returncode=%s", context_label, rc)
                for sibling in children:
                    if sibling is proc:
                        continue
                    if _poll_process(sibling) is None:
                        _stop_process(sibling, force=False)
    except KeyboardInterrupt:
        exit_code = 130
        logger.warning("%s interrupted; terminating child workers", context_label)
        for proc in children:
            if _poll_process(proc) is None:
                _stop_process(proc, force=False)
    finally:
        deadline = time.monotonic() + max(0.0, terminate_grace_seconds)
        for proc in children:
            if _poll_process(proc) is not None:
                continue
            remaining = max(0.0, deadline - time.monotonic())
            rc = _wait_process(proc, timeout=remaining)
            if rc is None and _poll_process(proc) is None:
                _stop_process(proc, force=True)
                _wait_process(proc, timeout=1.0)
    return exit_code


def main() -> int:
    load_env()
    args = parse_args()
    stage_filter = None if args.stage == "any" else args.stage
    platform_filter = args.platform
    worker_id = _build_worker_id(args.worker_id)
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info("Starting socials worker: worker_id=%s", worker_id)
    if _requires_media_mirror_s3_preflight(stage=stage_filter, platform=platform_filter):
        try:
            ensure_media_mirror_s3_ready()
        except Exception as exc:  # noqa: BLE001
            logger.error("Social media mirror S3 preflight failed: %s", exc)
            return 2
    if not args.run_id and args.parallel > 1:
        logger.info(
            "Starting queue fanout workers: parent_worker_id=%s parallel=%d stage=%s platform=%s once=%s",
            worker_id,
            args.parallel,
            stage_filter or "any",
            platform_filter or "any",
            bool(args.once),
        )
        children: list[subprocess.Popen] = []
        for index in range(max(1, int(args.parallel))):
            child_worker_id = f"{worker_id}:p{index + 1}"
            children.append(
                _spawn_child_worker(
                    worker_id=child_worker_id,
                    interval=args.interval,
                    stage=stage_filter,
                    platform=platform_filter,
                    once=bool(args.once),
                )
            )
        return _wait_for_children(children, context_label="queue fanout")

    if platform_filter:
        worker_supported_platforms = [platform_filter]
    else:
        worker_supported_platforms = list(SOCIAL_SUPPORTED_PLATFORMS)

    heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        stage=stage_filter,
        run_id=args.run_id,
        supported_platforms=worker_supported_platforms,
    )
    heartbeat.start()

    try:
        if args.run_id:
            stale_jobs = recover_stale_running_jobs(
                run_id=args.run_id, stage=stage_filter, platform=platform_filter, limit=50
            )
            if stale_jobs:
                logger.warning(
                    "Recovered %d stale running job(s) before executing run_id=%s",
                    len(stale_jobs),
                    args.run_id,
                )
            if args.tandem:
                posts_workers = max(0, int(args.posts_workers))
                comments_workers = max(0, int(args.comments_workers))
                if posts_workers + comments_workers == 0:
                    logger.error("No workers requested for tandem mode")
                    return 2
                logger.info(
                    "Executing run_id=%s in tandem mode (posts=%d comments=%d)",
                    args.run_id,
                    posts_workers,
                    comments_workers,
                )
                children: list[subprocess.Popen] = []

                def _spawn_group(stage: str, count: int) -> None:
                    for index in range(count):
                        child_worker_id = f"{worker_id}:{stage}:p{index + 1}"
                        children.append(
                            _spawn_child_worker(
                                worker_id=child_worker_id,
                                interval=args.interval,
                                stage=stage,
                                platform=platform_filter,
                                run_id=args.run_id,
                            )
                        )

                _spawn_group("posts", posts_workers)
                _spawn_group("comments", comments_workers)
                return _wait_for_children(children, context_label="tandem run")
            if args.parallel > 1:
                logger.info(
                    "Executing run_id=%s with %d parallel workers",
                    args.run_id,
                    args.parallel,
                )
                children: list[subprocess.Popen] = []
                for index in range(args.parallel):
                    child_worker_id = f"{worker_id}:p{index + 1}"
                    children.append(
                        _spawn_child_worker(
                            worker_id=child_worker_id,
                            interval=args.interval,
                            stage=stage_filter,
                            platform=platform_filter,
                            run_id=args.run_id,
                        )
                    )
                return _wait_for_children(children, context_label="parallel run")
            logger.info(
                "Executing specific run_id=%s stage=%s platform=%s",
                args.run_id,
                stage_filter or "any",
                platform_filter or "any",
            )
            heartbeat.set_state(status="working", run_id=args.run_id, stage=stage_filter or "any")
            run_result = execute_run(args.run_id, worker_id=worker_id, stage=stage_filter, platform=platform_filter)
            heartbeat.set_state(
                status="idle",
                current_job_id=None,
                metadata_updates=_build_run_completion_metadata(args.run_id, dict(run_result or {})),
            )
            return 0

        processed = 0
        claim_batch_size = _resolve_int_env(
            "SOCIAL_JOB_CLAIM_BATCH_SIZE",
            _default_claim_batch_size_for_stage(stage_filter),
            minimum=1,
            maximum=25,
        )
        stale_recovery_interval = _resolve_int_env(
            "SOCIAL_STALE_RECOVERY_INTERVAL_SEC",
            30,
            minimum=5,
            maximum=600,
        )
        run_summary_reconcile_interval = _resolve_int_env(
            "SOCIAL_RUN_SUMMARY_RECONCILE_INTERVAL_SEC",
            60,
            minimum=10,
            maximum=3600,
        )
        sync_orchestrator_interval = _resolve_int_env(
            "SOCIAL_SYNC_SESSION_TICK_INTERVAL_SEC",
            30,
            minimum=5,
            maximum=3600,
        )
        claimed_jobs: list[dict[str, object]] = []
        last_stale_recovery_at = 0.0
        last_summary_reconcile_at = 0.0
        last_sync_orchestrator_at = 0.0
        while True:
            started = datetime.now(tz=UTC)
            now_mono = time.monotonic()
            if now_mono - last_stale_recovery_at >= stale_recovery_interval:
                try:
                    stale_jobs = recover_stale_running_jobs(
                        run_id=None, stage=stage_filter, platform=platform_filter, limit=25
                    )
                    if stale_jobs:
                        logger.warning("Recovered %d stale running job(s) before claim", len(stale_jobs))
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Stale recovery pass failed: %s", exc)
                last_stale_recovery_at = now_mono
            if now_mono - last_summary_reconcile_at >= run_summary_reconcile_interval:
                try:
                    reconcile = reconcile_run_summaries(limit=100)
                    if int(reconcile.get("reconciled_runs") or 0) > 0:
                        logger.debug(
                            "Reconciled %d run summary rows",
                            int(reconcile.get("reconciled_runs") or 0),
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Run summary reconciliation pass failed: %s", exc)
                last_summary_reconcile_at = now_mono
            if now_mono - last_sync_orchestrator_at >= sync_orchestrator_interval:
                try:
                    orchestrated = tick_sync_orchestrator(limit=50)
                    if int(orchestrated.get("evaluated_sessions") or 0) > 0:
                        logger.debug(
                            "Evaluated %d sync session(s)",
                            int(orchestrated.get("evaluated_sessions") or 0),
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Sync-session orchestration tick failed: %s", exc)
                last_sync_orchestrator_at = now_mono

            if not claimed_jobs:
                try:
                    claimed_jobs = _claim_jobs_for_stage_candidates(
                        worker_id=worker_id,
                        stage=stage_filter,
                        platform=platform_filter,
                        limit=claim_batch_size,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Job claim failed: worker_id=%s error=%s", worker_id, exc)
                    heartbeat.set_state(
                        status="idle",
                        current_job_id=None,
                        run_id=None,
                        metadata_updates={"last_claim_error": type(exc).__name__},
                    )
                    if args.once:
                        return 1
                    idle_sleep_seconds = max(0.25, args.interval) + random.uniform(0.0, 0.2)
                    time.sleep(idle_sleep_seconds)
                    continue
            claimed = claimed_jobs.pop(0) if claimed_jobs else None
            if claimed:
                try:
                    cancelled_job = cancel_claimed_job_before_processing(claimed)
                except Exception as exc:  # noqa: BLE001
                    cancelled_job = None
                    logger.warning(
                        "Pre-process cancel check failed: worker_id=%s job_id=%s error=%s",
                        worker_id,
                        claimed.get("id"),
                        exc,
                    )
                    heartbeat.set_state(
                        metadata_updates={"last_cancel_probe_error": type(exc).__name__},
                    )
                if cancelled_job is not None:
                    heartbeat.set_state(status="idle", current_job_id=None, run_id=None)
                    logger.info(
                        "Discarded claimed job=%s run_id=%s because the job or run was already cancelled",
                        cancelled_job.get("id"),
                        cancelled_job.get("run_id"),
                    )
                    if args.once:
                        break
                    continue
                heartbeat.set_state(status="working", stage=stage_filter or "any")
                claimed_config = dict(claimed.get("config") or {})
                heartbeat.set_state(
                    status="working",
                    stage=str(claimed_config.get("stage") or stage_filter or "any"),
                    run_id=str(claimed.get("run_id") or "") or None,
                    current_job_id=str(claimed.get("id") or "") or None,
                )
                try:
                    job = process_claimed_job(claimed, worker_id=worker_id)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "Processing claimed job crashed unexpectedly: worker_id=%s job_id=%s",
                        worker_id,
                        claimed.get("id"),
                    )
                    heartbeat.set_state(
                        status="idle",
                        current_job_id=None,
                        metadata_updates={"last_processing_error": type(exc).__name__},
                    )
                    if args.once:
                        return 1
                    continue
                job, silent_drop_alert = _apply_post_persist_truthfulness_diagnostics(job)
                processed += 1
                heartbeat.set_state(
                    status="working",
                    stage=str(claimed_config.get("stage") or stage_filter or "any"),
                    run_id=str(job.get("run_id") or "") or None,
                    current_job_id=str(job.get("id") or "") or None,
                    metadata_updates={"processed_jobs": processed},
                )
                if silent_drop_alert:
                    logger.warning(
                        "Processed job=%s completed with a post-persist silent-drop alert",
                        job.get("id"),
                    )
                logger.info(
                    "Processed job=%s run_id=%s platform=%s status=%s items=%s elapsed=%.2fs",
                    job.get("id"),
                    job.get("run_id"),
                    job.get("platform"),
                    job.get("status"),
                    job.get("items_found"),
                    (datetime.now(tz=UTC) - started).total_seconds(),
                )
                heartbeat.set_state(status="idle", current_job_id=None)
                if args.once:
                    break
                continue

            heartbeat.set_state(status="idle", current_job_id=None, run_id=None)
            if args.once:
                logger.info("No queued jobs found")
                break
            idle_sleep_seconds = max(0.25, args.interval) + random.uniform(0.0, 0.2)
            time.sleep(idle_sleep_seconds)

        logger.info("Worker exiting: processed=%d", processed)
        return 0
    finally:
        heartbeat.stop(reason="worker_exit")


if __name__ == "__main__":
    raise SystemExit(main())
