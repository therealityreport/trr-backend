#!/usr/bin/env python3
"""Render the safe Chrome-login cookie retrieval model for a social account."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trr_backend.socials.chrome_cookie_model import (  # noqa: E402
    DEFAULT_CHROME_PROFILE,
    build_social_cookie_chrome_model,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, help="Social platform to log in to, such as instagram or threads.")
    parser.add_argument("--account-handle", default="", help="Optional account handle for operator labeling.")
    parser.add_argument(
        "--chrome-profile",
        default=DEFAULT_CHROME_PROFILE,
        help=f"Chrome profile label routed through @chrome (default: {DEFAULT_CHROME_PROFILE}).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    model = build_social_cookie_chrome_model(
        str(args.platform),
        account_handle=str(args.account_handle or "").strip() or None,
        chrome_profile=str(args.chrome_profile or "").strip() or DEFAULT_CHROME_PROFILE,
    )
    print(json.dumps(model, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
