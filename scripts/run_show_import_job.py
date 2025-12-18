#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys


def _require_env(name: str) -> str | None:
    value = (os.getenv(name) or "").strip()
    return value or None


def main(argv: list[str]) -> int:
    missing: list[str] = []
    for key in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"):
        if not _require_env(key):
            missing.append(key)

    if missing:
        print("Missing required environment variables:", ", ".join(missing), file=sys.stderr)
        return 2

    if not _require_env("TMDB_API_KEY"):
        print("Warning: TMDB_API_KEY is not set; Stage 2 enrichment will have fewer fields.", file=sys.stderr)

    passthrough = list(argv)
    cmd = [sys.executable, "-m", "scripts.import_shows_from_lists"]

    if "--enrich-show-metadata" not in passthrough:
        cmd.append("--enrich-show-metadata")
    if "--region" not in passthrough:
        cmd.extend(["--region", "US"])

    cmd.extend(passthrough)
    return subprocess.run(cmd, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

