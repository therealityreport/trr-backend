#!/usr/bin/env python3
"""Social ingest queue worker (Postgres-backed via social.scrape_jobs)."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime

from trr_backend.repositories.social_season_analytics import (
    ensure_media_mirror_s3_ready,
    execute_run,
    mark_worker_stopped,
    process_next_queued_job,
    recover_stale_running_jobs,
    update_worker_heartbeat,
)
from trr_backend.utils.env import load_env

logger = logging.getLogger("socials.worker")
_UNSET = object()


class WorkerHeartbeat:
    def __init__(self, *, worker_id: str, stage: str | None, run_id: str | None):
        self._worker_id = worker_id
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
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
                "worker_script": "scripts.socials.worker",
            },
        }

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
        try:
            update_worker_heartbeat(
                self._worker_id,
                stage=str(state.get("stage") or "any"),
                status=str(state.get("status") or "idle"),
                run_id=(str(state.get("run_id")) if state.get("run_id") else None),
                current_job_id=(str(state.get("current_job_id")) if state.get("current_job_id") else None),
                metadata=dict(state.get("metadata") or {}),
            )
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process queued social ingest jobs.")
    parser.add_argument("--worker-id", default=None, help="Explicit worker id")
    parser.add_argument("--interval", type=float, default=2.0, help="Idle sleep interval in seconds")
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
        choices=["any", "posts", "comments", "media_mirror"],
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
        choices=["instagram", "tiktok", "youtube", "twitter"],
        default=None,
        help="Optional platform filter when claiming jobs",
    )
    return parser.parse_args()


def _requires_media_mirror_s3_preflight(*, stage: str | None, platform: str | None) -> bool:
    normalized_stage = str(stage or "any").strip().lower() or "any"
    normalized_platform = str(platform or "any").strip().lower() or "any"
    return normalized_stage in {"any", "media_mirror"} and normalized_platform in {
        "any",
        "instagram",
        "tiktok",
        "youtube",
        "twitter",
    }


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
            cmd = [
                sys.executable,
                "-m",
                "scripts.socials.worker",
                "--worker-id",
                child_worker_id,
                "--parallel",
                "1",
                "--interval",
                str(args.interval),
            ]
            if stage_filter:
                cmd.extend(["--stage", stage_filter])
            if platform_filter:
                cmd.extend(["--platform", platform_filter])
            if args.once:
                cmd.append("--once")
            children.append(subprocess.Popen(cmd, cwd=os.getcwd(), env=os.environ.copy()))
        exit_code = 0
        for proc in children:
            rc = proc.wait()
            if rc != 0:
                exit_code = rc
        return exit_code

    heartbeat = WorkerHeartbeat(worker_id=worker_id, stage=stage_filter, run_id=args.run_id)
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
                        cmd = [
                            sys.executable,
                            "-m",
                            "scripts.socials.worker",
                            "--run-id",
                            args.run_id,
                            "--worker-id",
                            child_worker_id,
                            "--stage",
                            stage,
                            "--parallel",
                            "1",
                        ]
                        if platform_filter:
                            cmd.extend(["--platform", platform_filter])
                        children.append(subprocess.Popen(cmd, cwd=os.getcwd(), env=os.environ.copy()))

                _spawn_group("posts", posts_workers)
                _spawn_group("comments", comments_workers)

                exit_code = 0
                for proc in children:
                    rc = proc.wait()
                    if rc != 0:
                        exit_code = rc
                return exit_code
            if args.parallel > 1:
                logger.info(
                    "Executing run_id=%s with %d parallel workers",
                    args.run_id,
                    args.parallel,
                )
                children: list[subprocess.Popen] = []
                for index in range(args.parallel):
                    child_worker_id = f"{worker_id}:p{index + 1}"
                    cmd = [
                        sys.executable,
                        "-m",
                        "scripts.socials.worker",
                        "--run-id",
                        args.run_id,
                        "--worker-id",
                        child_worker_id,
                        "--parallel",
                        "1",
                    ]
                    if stage_filter:
                        cmd.extend(["--stage", stage_filter])
                    if platform_filter:
                        cmd.extend(["--platform", platform_filter])
                    children.append(subprocess.Popen(cmd, cwd=os.getcwd(), env=os.environ.copy()))
                exit_code = 0
                for proc in children:
                    rc = proc.wait()
                    if rc != 0:
                        exit_code = rc
                return exit_code
            logger.info(
                "Executing specific run_id=%s stage=%s platform=%s",
                args.run_id,
                stage_filter or "any",
                platform_filter or "any",
            )
            heartbeat.set_state(status="working", run_id=args.run_id, stage=stage_filter or "any")
            execute_run(args.run_id, worker_id=worker_id, stage=stage_filter, platform=platform_filter)
            heartbeat.set_state(status="idle", current_job_id=None, metadata_updates={"processed_jobs": "run_complete"})
            return 0

        processed = 0
        while True:
            started = datetime.now(tz=UTC)
            stale_jobs = recover_stale_running_jobs(run_id=None, stage=stage_filter, platform=platform_filter, limit=25)
            if stale_jobs:
                logger.warning("Recovered %d stale running job(s) before claim", len(stale_jobs))
            heartbeat.set_state(status="working", stage=stage_filter or "any")
            job = process_next_queued_job(worker_id=worker_id, stage=stage_filter, platform=platform_filter)
            if job:
                processed += 1
                heartbeat.set_state(
                    status="working",
                    stage=str((job.get("config") or {}).get("stage") or stage_filter or "any"),
                    run_id=str(job.get("run_id") or "") or None,
                    current_job_id=str(job.get("id") or "") or None,
                    metadata_updates={"processed_jobs": processed},
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
            time.sleep(max(0.25, args.interval))

        logger.info("Worker exiting: processed=%d", processed)
        return 0
    finally:
        heartbeat.stop(reason="worker_exit")


if __name__ == "__main__":
    raise SystemExit(main())
