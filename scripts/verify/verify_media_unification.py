#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trr_backend.utils.env import load_env  # noqa: E402


def _is_local_db_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    return host in {"localhost", "127.0.0.1"}


def _resolve_sql_path() -> Path:
    return REPO_ROOT / "scripts" / "db" / "verify_media_unification.sql"


def _run_psql(url: str, sql_path: Path) -> bool:
    env = os.environ.copy()
    env["PGCONNECT_TIMEOUT"] = "5"
    cmd = ["psql", url, "-v", "ON_ERROR_STOP=1", "-f", str(sql_path)]
    try:
        subprocess.run(cmd, check=True, env=env, capture_output=False)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _detect_local_db_url() -> str | None:
    try:
        result = subprocess.run(
            ["supabase", "status"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

    match = re.search(r"postgresql://\\S+", result.stdout)
    return match.group(0) if match else None


def _find_db_container() -> str | None:
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    for line in result.stdout.splitlines():
        if line.startswith("supabase_db_"):
            return line.strip()
    return None


def _run_via_docker(sql_path: Path) -> bool:
    container = _find_db_container()
    if not container:
        return False
    try:
        with sql_path.open("rb") as handle:
            subprocess.run(
                [
                    "docker",
                    "exec",
                    "-i",
                    container,
                    "psql",
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-v",
                    "ON_ERROR_STOP=1",
                ],
                check=True,
                stdin=handle,
                capture_output=False,
            )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _run_scoped_grep() -> bool:
    table_names = (
        "show_images|season_images|episode_images|person_images|cast_photos|"
        "v_show_images_served_media|v_person_images_served_media"
    )
    pattern = rf"(?i)\\b(from|join)\\s+core\\.({table_names})\\b"

    cmd = [
        "rg",
        "-n",
        "--pcre2",
        "--type",
        "py",
        "--type",
        "js",
        "--type",
        "ts",
        "--type",
        "sql",
        "-g",
        "!scripts/db/**",
        pattern,
        "trr_backend",
        "scripts",
    ]
    try:
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    except FileNotFoundError as err:
        raise SystemExit("ripgrep (rg) is required for verification.") from err

    if result.returncode == 0:
        print("FAIL: legacy read path references found in runtime code:")
        print(result.stdout.rstrip())
        return False
    if result.returncode not in (1, 2):
        print("FAIL: rg returned an unexpected status.")
        print(result.stdout.rstrip())
        print(result.stderr.rstrip())
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify media unification bridges and coverage using local Supabase.",
    )
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow verification against non-local DB URLs.",
    )
    args = parser.parse_args()

    load_env()
    sql_path = _resolve_sql_path()

    env_url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    if env_url and _is_local_db_url(env_url):
        print(f"Using local SUPABASE_DB_URL: {env_url}")
        if _run_psql(env_url, sql_path):
            return 0 if _run_scoped_grep() else 1

    local_url = _detect_local_db_url()
    if local_url:
        print(f"Using local URL from Supabase status: {local_url}")
        if _run_psql(local_url, sql_path):
            return 0 if _run_scoped_grep() else 1

    print("Falling back to docker exec for verification.")
    if _run_via_docker(sql_path):
        return 0 if _run_scoped_grep() else 1

    if env_url and not args.allow_remote:
        raise SystemExit("Refusing to run against non-local SUPABASE_DB_URL. Use --allow-remote to override.")

    if env_url:
        print(f"Using remote SUPABASE_DB_URL: {env_url}")
        if _run_psql(env_url, sql_path):
            return 0 if _run_scoped_grep() else 1

    raise SystemExit("Failed to run media unification verification locally.")


if __name__ == "__main__":
    raise SystemExit(main())
