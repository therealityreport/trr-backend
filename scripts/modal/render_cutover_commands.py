#!/usr/bin/env python3
"""Render non-mutating rollout commands for the full TRR backend Modal cutover."""

from __future__ import annotations

import argparse
import shlex
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import (  # noqa: E402
    DEFAULT_APP_NAME,
    DEFAULT_APP_REF,
    REQUIRED_MODAL_ENVIRONMENT,
)

TARGET_ENV = {
    "TRR_JOB_PLANE_MODE": "remote",
    "TRR_LONG_JOB_ENFORCE_REMOTE": "1",
    "TRR_REMOTE_EXECUTOR": "modal",
    "TRR_MODAL_ENABLED": "1",
    "TRR_MODAL_APP_NAME": "trr-backend-jobs",
    "TRR_MODAL_API_FUNCTION": "serve_backend_api",
    "TRR_MODAL_API_LABEL": "trr-backend-api",
    "TRR_MODAL_ADMIN_OPERATION_FUNCTION": "run_admin_operation_v2",
    "TRR_MODAL_GOOGLE_NEWS_FUNCTION": "run_google_news_sync",
    "TRR_MODAL_REDDIT_REFRESH_FUNCTION": "run_reddit_refresh",
    "TRR_MODAL_SOCIAL_JOB_FUNCTION": "run_social_job",
    "TRR_MODAL_SOCIAL_POSTS_JOB_FUNCTION": "run_social_posts_job",
    "TRR_MODAL_SOCIAL_MEDIA_JOB_FUNCTION": "run_social_media_job",
    "TRR_MODAL_SOCIAL_COMMENTS_JOB_FUNCTION": "run_social_comments_job",
    "TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_FUNCTION": "run_social_comments_recovery_job",
    "TRR_MODAL_SOCIAL_RECOVERY_FUNCTION": "sweep_social_dispatch_queue",
    "TRR_MODAL_VISION_FUNCTION": "run_admin_vision",
    "TRR_MODAL_CAST_SCREENTIME_FUNCTION": "run_cast_screentime_analysis",
    "TRR_MODAL_RUNTIME_SECRET_NAME": "trr-backend-runtime",
    "TRR_MODAL_SOCIAL_SECRET_NAME": "trr-social-auth",
    "TRR_ADMIN_IMAGE_EXECUTION_BACKEND": "modal",
    "SOCIAL_QUEUE_ENABLED": "true",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--modal-environment",
        choices=(REQUIRED_MODAL_ENVIRONMENT,),
        default=REQUIRED_MODAL_ENVIRONMENT,
        help=f"Pinned Modal environment passed to readiness checks (required: {REQUIRED_MODAL_ENVIRONMENT}).",
    )
    parser.add_argument(
        "--frontend-runtime-target",
        default="TRR-APP runtime configuration",
        help="Label used when describing where TRR_API_URL should be updated.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    verify_args = f" --env {shlex.quote(args.modal_environment)}"

    print("Full backend Modal cutover prep checklist")
    print(f"Repo root: {REPO_ROOT}")
    print(f"Modal environment: {args.modal_environment}")
    print("\nTarget runtime env block:")
    for key, value in TARGET_ENV.items():
        print(f"  {key}={value}")

    deploy_command = shlex.join(
        [
            str(REPO_ROOT / ".venv" / "bin" / "python"),
            str(REPO_ROOT / "scripts" / "modal" / "deploy_backend.py"),
            "--app-ref",
            DEFAULT_APP_REF,
            "--app-name",
            DEFAULT_APP_NAME,
            "--env",
            REQUIRED_MODAL_ENVIRONMENT,
        ]
    )
    print("\nSuggested Modal deploy commands:")
    print("  " + deploy_command)
    print("  " + f"python3.11 scripts/modal/verify_modal_readiness.py{verify_args} --json | jq -r '.api_web_url'")

    print("\nCutover steps:")
    print("  1. Ensure Modal named secrets `trr-backend-runtime` and `trr-social-auth` exist.")
    print("  2. Deploy `trr_backend.modal_jobs` so the API endpoint and all job functions publish together.")
    print(
        "  3. Run `python3.11 scripts/modal/verify_modal_readiness.py` and require "
        "the API web URL plus all eight functions to resolve."
    )
    print(f"  4. Update {args.frontend_runtime_target} so `TRR_API_URL` points at the verified Modal API URL.")
    print("  5. Verify `/health`, admin flows, and social worker-health against the Modal backend URL.")
    print("  6. Confirm Modal Cron owns dispatcher heartbeat and social recovery after cutover.")
    print("  7. Retire the legacy backend runtime only after staging and production Modal smoke checks both pass.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
