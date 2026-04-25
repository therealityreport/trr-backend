#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
CANARY_PLATFORMS = ["instagram", "tiktok", "twitter", "facebook"]


def build_canary_commands(account: str) -> list[list[str]]:
    return [
        [
            sys.executable,
            "scripts/socials/local_catalog_action.py",
            "--platform",
            platform,
            "--account",
            account,
            "--source-scope",
            "bravo",
            "--action",
            "backfill",
            "--selected-task",
            "post_details",
        ]
        for platform in CANARY_PLATFORMS
    ]


def run_canaries(account: str) -> int:
    for platform, command in zip(CANARY_PLATFORMS, build_canary_commands(account), strict=True):
        result = subprocess.run(command, cwd=BACKEND_ROOT, check=False)
        payload = {
            "platform": platform,
            "account": account,
            "command": command,
            "returncode": result.returncode,
        }
        print(json.dumps(payload, sort_keys=True))
        if result.returncode != 0:
            return result.returncode
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one-pass social backfill canaries.")
    parser.add_argument("--account", default="thetraitorsus")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.dry_run:
        print(json.dumps(build_canary_commands(args.account), indent=2))
        return 0
    return run_canaries(args.account)


if __name__ == "__main__":
    raise SystemExit(main())
