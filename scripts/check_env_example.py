#!/usr/bin/env python3
"""Validate .env.example style files for contract hygiene."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ASSIGNMENT_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_-]*)\s*=")
VALID_SHELL_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate env example contracts")
    parser.add_argument("--file", required=True, help="Path to .env.example-like file")
    parser.add_argument(
        "--required",
        nargs="*",
        default=[],
        help="Required keys that must be present",
    )
    parser.add_argument(
        "--allow-hyphen",
        nargs="*",
        default=[],
        help="Hyphenated keys allowed temporarily (deprecated aliases)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_file = Path(args.file)
    if not env_file.exists():
        print(f"ERROR: file not found: {env_file}", file=sys.stderr)
        return 1

    allow_hyphen = set(args.allow_hyphen)
    required = set(args.required)
    seen: dict[str, int] = {}
    duplicates: list[tuple[str, int, int]] = []
    bad_keys: list[tuple[str, int]] = []

    for lineno, raw in enumerate(env_file.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = ASSIGNMENT_RE.match(raw)
        if not match:
            continue

        key = match.group(1)
        if key in seen:
            duplicates.append((key, seen[key], lineno))
        else:
            seen[key] = lineno

        if "-" in key and key not in allow_hyphen:
            bad_keys.append((key, lineno))
            continue
        if "-" not in key and not VALID_SHELL_KEY_RE.match(key):
            bad_keys.append((key, lineno))

    missing = sorted(required - set(seen))
    failed = False

    if duplicates:
        failed = True
        print("ERROR: duplicate env keys detected:", file=sys.stderr)
        for key, first, second in duplicates:
            print(f"  - {key}: lines {first} and {second}", file=sys.stderr)

    if bad_keys:
        failed = True
        print("ERROR: invalid env keys detected:", file=sys.stderr)
        for key, lineno in bad_keys:
            print(f"  - {key}: line {lineno}", file=sys.stderr)

    if missing:
        failed = True
        print("ERROR: required env keys are missing:", file=sys.stderr)
        for key in missing:
            print(f"  - {key}", file=sys.stderr)

    if failed:
        return 1

    print(f"OK: {env_file} passed env contract validation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
