#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]


def _require_env(name: str) -> str | None:
    value = (os.getenv(name) or "").strip()
    return value or None



def _run_importer(args: list[str], *, timeout_seconds: int | None = None) -> int:
    cmd = [sys.executable, "-m", "scripts.import_shows_from_lists", *args]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join([str(REPO_ROOT), env.get("PYTHONPATH", "")]).strip(os.pathsep)
    try:
        return subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            env=env,
            check=False,
            timeout=(timeout_seconds if timeout_seconds and timeout_seconds > 0 else None),
        ).returncode
    except subprocess.TimeoutExpired:
        return 124


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        prog="run_show_import_job.py",
        description="Convenience runner for the show import job.",
    )
    parser.add_argument("--tmdb-list", action="append", default=[], help="TMDb list id or URL (repeatable).")
    parser.add_argument("--imdb-list", action="append", default=[], help="IMDb list URL (repeatable).")

    parser.add_argument("--region", type=str, default="US", help="Region for watch providers (default: US).")
    parser.add_argument("--dry-run", action="store_true", help="No DB writes.")
    parser.add_argument("--concurrency", type=int, default=None, help="Stage 2 concurrency.")
    parser.add_argument("--force-refresh", action="store_true", help="Ignore existing show_meta.fetched_at.")
    parser.add_argument("--imdb-sleep-ms", type=int, default=None, help="Delay between IMDb enrichment requests.")

    parser.add_argument(
        "--imdb-max-enrich",
        type=int,
        default=20,
        help="Stage 2 cap for IMDb pass (default: 20, set 0 for no cap).",
    )
    parser.add_argument(
        "--imdb-retries",
        type=int,
        default=3,
        help="Retries for the IMDb pass on failure/timeout (default: 3).",
    )
    parser.add_argument(
        "--imdb-timeout-seconds",
        type=int,
        default=0,
        help="Optional timeout for the IMDb pass; 0 disables (default: 0).",
    )

    parser.add_argument(
        "--skip-tmdb-external-ids",
        action="store_true",
        help="Skip per-show TMDb external_ids lookups (faster; fewer cross-links).",
    )
    parser.add_argument(
        "--single-pass",
        action="store_true",
        help="Run a single combined import instead of TMDb pass then IMDb pass.",
    )

    return parser.parse_known_args(argv)


def main(argv: list[str]) -> int:
    load_dotenv(REPO_ROOT / ".env")

    args, passthrough = _parse_args(argv)

    # Back-compat fallback: if no explicit list args were provided, pass everything through.
    tmdb_lists = [str(v) for v in (args.tmdb_list or []) if str(v).strip()]
    imdb_lists = [str(v) for v in (args.imdb_list or []) if str(v).strip()]
    if not tmdb_lists and not imdb_lists:
        passthrough = list(argv)
        if "--enrich-show-metadata" not in passthrough:
            passthrough.append("--enrich-show-metadata")
        if "--region" not in passthrough:
            passthrough.extend(["--region", "US"])
        return _run_importer(passthrough)

    if not args.dry_run:
        missing: list[str] = []
        for key in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"):
            if not _require_env(key):
                missing.append(key)
        if missing:
            print("Missing required environment variables:", ", ".join(missing), file=sys.stderr)
            return 2

    if tmdb_lists and not _require_env("TMDB_API_KEY"):
        print("Missing required environment variable: TMDB_API_KEY (required for TMDb list ingestion).", file=sys.stderr)
        return 2

    common: list[str] = []
    if "--enrich-show-metadata" not in passthrough:
        common.append("--enrich-show-metadata")
    if "--region" not in passthrough:
        common.extend(["--region", str(args.region or "US").upper()])
    if args.dry_run and "--dry-run" not in passthrough:
        common.append("--dry-run")
    if args.concurrency is not None and "--concurrency" not in passthrough:
        common.extend(["--concurrency", str(int(args.concurrency))])
    if args.force_refresh and "--force-refresh" not in passthrough:
        common.append("--force-refresh")
    if args.imdb_sleep_ms is not None and "--imdb-sleep-ms" not in passthrough:
        common.extend(["--imdb-sleep-ms", str(int(args.imdb_sleep_ms))])

    # Always allow power-users to forward extra flags (e.g. --annotate-imdb-episodic).
    common.extend(passthrough)

    if args.single_pass or not (tmdb_lists and imdb_lists):
        run_args = list(common)
        for v in tmdb_lists:
            run_args.extend(["--tmdb-list", v])
        for v in imdb_lists:
            run_args.extend(["--imdb-list", v])
        if args.skip_tmdb_external_ids and "--skip-tmdb-external-ids" not in run_args:
            run_args.append("--skip-tmdb-external-ids")
        return _run_importer(run_args)

    # Two-pass mode: TMDb first (reliable), then IMDb (retryable).
    print("==> Pass 1/2: TMDb list import + enrichment", file=sys.stderr)
    tmdb_args = list(common)
    for v in tmdb_lists:
        tmdb_args.extend(["--tmdb-list", v])
    if args.skip_tmdb_external_ids and "--skip-tmdb-external-ids" not in tmdb_args:
        tmdb_args.append("--skip-tmdb-external-ids")
    rc = _run_importer(tmdb_args)
    if rc != 0:
        return rc

    print("==> Pass 2/2: IMDb list import + enrichment", file=sys.stderr)
    imdb_args = list(common)
    for v in imdb_lists:
        imdb_args.extend(["--imdb-list", v])
    if args.imdb_max_enrich and int(args.imdb_max_enrich) > 0 and "--max-enrich" not in imdb_args:
        imdb_args.extend(["--max-enrich", str(int(args.imdb_max_enrich))])

    retries = max(1, int(args.imdb_retries or 1))
    timeout_seconds = int(args.imdb_timeout_seconds or 0)
    for attempt in range(1, retries + 1):
        rc = _run_importer(imdb_args, timeout_seconds=timeout_seconds if timeout_seconds > 0 else None)
        if rc == 0:
            return 0
        if rc == 130:
            return rc
        if attempt < retries:
            sleep_s = min(30, 2**attempt)
            print(f"IMDb pass failed (exit {rc}); retrying in {sleep_s}s (attempt {attempt+1}/{retries})…", file=sys.stderr)
            time.sleep(sleep_s)
            continue
        return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
