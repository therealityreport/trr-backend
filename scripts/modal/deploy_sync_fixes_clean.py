#!/usr/bin/env python3
"""Deploy sync fixes to Modal from an isolated clean worktree.

This helper keeps unrelated local workspace changes out of Modal deploys. It
creates a temporary worktree at HEAD, overlays only the allowed sync-fix paths
from the current checkout, then delegates to scripts/modal/deploy_backend.py.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SYNC_FIX_CONFIG_PATH = REPO_ROOT / "scripts" / "modal" / "sync_fix_deploy_paths.json"


def _run(command: list[str], *, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, check=check, text=True)


def _copy_path(src_root: Path, dst_root: Path, rel_path: str) -> bool:
    src = src_root / rel_path
    dst = dst_root / rel_path
    if not src.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst, symlinks=True)
    else:
        shutil.copy2(src, dst)
    return True


def _normalize_config_path(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lstrip("/")
    if not normalized or normalized.startswith("../") or "/../" in normalized:
        return None
    return normalized


def load_sync_fix_config(config_path: Path = DEFAULT_SYNC_FIX_CONFIG_PATH) -> tuple[tuple[str, ...], tuple[str, ...]]:
    with config_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError(f"{config_path} must contain a JSON object")

    paths = tuple(path for path in (_normalize_config_path(item) for item in payload.get("sync_fix_paths", [])) if path)
    required_dirs = tuple(
        path for path in (_normalize_config_path(item) for item in payload.get("required_local_dirs", [])) if path
    )
    if not paths:
        raise ValueError(f"{config_path} must define at least one sync_fix_paths entry")
    return paths, required_dirs


def parse_args(argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        action="append",
        default=[],
        help="Additional repo-relative path to include in the clean deploy overlay.",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_SYNC_FIX_CONFIG_PATH),
        help="Path to the JSON allowlist config for the clean deploy overlay.",
    )
    parser.add_argument(
        "--keep-worktree",
        action="store_true",
        help="Keep the temporary worktree after the command exits.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the clean worktree and print the delegated Modal deploy command.",
    )
    parser.add_argument(
        "deploy_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to deploy_backend.py. Prefix with -- to separate helper args.",
    )
    return parser.parse_known_args(argv)


def main(argv: list[str] | None = None) -> int:
    args, _unknown = parse_args(argv)
    sync_fix_paths, required_local_dirs = load_sync_fix_config(Path(args.config))
    tmp_root = Path(tempfile.mkdtemp(prefix="trr-modal-sync-fixes-"))
    worktree = tmp_root / "TRR-Backend"

    deploy_args = list(args.deploy_args or [])
    if deploy_args and deploy_args[0] == "--":
        deploy_args = deploy_args[1:]
    if args.dry_run and "--dry-run" not in deploy_args:
        deploy_args.append("--dry-run")

    try:
        _run(["git", "worktree", "add", "--detach", str(worktree), "HEAD"], cwd=REPO_ROOT)

        included = []
        for rel_path in (*sync_fix_paths, *args.path):
            normalized = str(rel_path).strip().lstrip("/")
            if not normalized:
                continue
            if _copy_path(REPO_ROOT, worktree, normalized):
                included.append(normalized)

        for rel_dir in required_local_dirs:
            (worktree / rel_dir).mkdir(parents=True, exist_ok=True)

        status = subprocess.run(
            ["git", "status", "--short", "--", *included],
            cwd=worktree,
            check=True,
            capture_output=True,
            text=True,
        )

        print("Clean Modal sync-fix worktree:", worktree)
        print("Included paths:")
        for rel_path in included:
            print(f"  {rel_path}")
        print("Overlay status:")
        print(status.stdout.rstrip() or "  no overlay changes")

        command = [sys.executable, str(worktree / "scripts" / "modal" / "deploy_backend.py"), *deploy_args]
        print("Delegating:", " ".join(command))
        return _run(command, cwd=worktree, check=False).returncode
    finally:
        if args.keep_worktree:
            print("Kept clean worktree:", worktree)
        else:
            _run(["git", "worktree", "remove", "--force", str(worktree)], cwd=REPO_ROOT, check=False)
            shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
