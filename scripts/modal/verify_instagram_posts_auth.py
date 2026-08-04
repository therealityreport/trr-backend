#!/usr/bin/env python3
"""Verify the Modal Instagram profile-posts auth path for one account."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import REQUIRED_MODAL_PROFILE  # noqa: E402
from trr_backend.modal_dispatch import get_trr_modal_function_handle  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", default="bravotv", help="Instagram account handle to probe.")
    parser.add_argument("--app-name", default="trr-backend-jobs", help="Modal app name.")
    parser.add_argument(
        "--function-name",
        default="probe_instagram_posts_auth",
        help="Modal function name for the Instagram posts auth probe.",
    )
    parser.add_argument("--json", action="store_true", help="Emit the raw probe payload as JSON.")
    return parser.parse_args(argv if argv is not None else sys.argv[1:])


def verify_instagram_posts_auth(*, account: str, app_name: str, function_name: str) -> dict[str, Any]:
    normalized_account = str(account or "").strip().lstrip("@") or "bravotv"
    fn = get_trr_modal_function_handle(function_name, app_name=app_name)
    payload = fn.remote(account_handle=normalized_account)
    result = dict(payload or {}) if isinstance(payload, dict) else {"raw_result": payload}
    result.setdefault("account_handle", normalized_account)
    return result


def main(argv: list[str] | None = None) -> int:
    # Pin the Modal workspace before verify_instagram_posts_auth lazily imports
    # the Modal SDK, which resolves MODAL_PROFILE at import time.
    os.environ["MODAL_PROFILE"] = REQUIRED_MODAL_PROFILE
    args = parse_args(argv)
    payload = verify_instagram_posts_auth(
        account=args.account,
        app_name=args.app_name,
        function_name=args.function_name,
    )
    ready = bool(payload.get("ready"))
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        status = "ready" if ready else "not ready"
        reason = str(payload.get("reason") or "").strip()
        detail = f" ({reason})" if reason else ""
        print(f"Instagram posts auth for @{payload.get('account_handle')}: {status}{detail}")
        print(f"  posts_seen: {payload.get('posts_seen')}")
        print(f"  request_count: {payload.get('request_count')}")
        print(f"  cookie_fingerprint: {payload.get('cookie_fingerprint')}")
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
