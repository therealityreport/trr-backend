#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

PIPELINE_STAGES: dict[str, list[tuple[Path, str]]] = {
    "showinfo": [
        (REPO_ROOT / "scripts" / "1-ShowInfo", "showinfo_step1.py"),
    ],
    "castinfo": [
        (REPO_ROOT / "scripts" / "2-CastInfo", "CastInfo_Step1.py"),
        (REPO_ROOT / "scripts" / "2-CastInfo", "CastInfo_Step2.py"),
    ],
    "realitease": [
        (REPO_ROOT / "scripts" / "3-RealiteaseInfo", "RealiteaseInfo_Step1.py"),
        (REPO_ROOT / "scripts" / "3-RealiteaseInfo", "RealiteaseInfo_Step2.py"),
        (REPO_ROOT / "scripts" / "3-RealiteaseInfo", "RealiteaseInfo_Step3.py"),
        (REPO_ROOT / "scripts" / "3-RealiteaseInfo", "RealiteaseInfo_Step4.py"),
    ],
    "wwhl": [
        (REPO_ROOT / "scripts" / "4-WWHLInfo", "WWHLInfo_TMDb_Step1.py"),
        (REPO_ROOT / "scripts" / "4-WWHLInfo", "WWHLInfo_IMDb_Step2.py"),
        (REPO_ROOT / "scripts" / "4-WWHLInfo", "WWHLInfo_Gemini_Step3.py"),
        (REPO_ROOT / "scripts" / "4-WWHLInfo", "WWHLInfo_Checker_Step4.py"),
    ],
    "final": [
        (REPO_ROOT / "scripts" / "5-FinalList", "FinalInfo_Step1.py"),
        (REPO_ROOT / "scripts" / "5-FinalList", "FinalInfo_Step2.py"),
        (REPO_ROOT / "scripts" / "5-FinalList", "FinalInfo_Step3.py"),
    ],
}

PIPELINE_ORDER = ["showinfo", "castinfo", "realitease", "wwhl", "final"]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_pipeline.py",
        description="Run TRR pipeline stages with a single entrypoint.",
    )
    parser.add_argument(
        "--step",
        default="all",
        choices=["all", *PIPELINE_ORDER],
        help="Pipeline stage to run (default: all).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pass --dry-run to underlying scripts (when supported).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Pass --limit N to underlying scripts (when supported).",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter to use (default: current).",
    )
    return parser.parse_args(argv)


def _run_script(python_exe: str, script_dir: Path, script_name: str, passthrough: list[str]) -> None:
    script_path = script_dir / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Missing script: {script_path}")

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT), env.get("PYTHONPATH", "")]
        if env.get("PYTHONPATH")
        else [str(REPO_ROOT)]
    )

    cmd = [python_exe, script_name, *passthrough]
    print(f"\n==> {script_dir.relative_to(REPO_ROOT)}/{script_name}")
    subprocess.run(cmd, cwd=str(script_dir), env=env, check=True)


def main(argv: list[str]) -> int:
    args = _parse_args(argv)

    stages = PIPELINE_ORDER if args.step == "all" else [args.step]
    passthrough: list[str] = []
    if args.dry_run:
        passthrough.append("--dry-run")
    if args.limit is not None:
        passthrough.extend(["--limit", str(args.limit)])

    for stage in stages:
        for script_dir, script_name in PIPELINE_STAGES[stage]:
            _run_script(args.python, script_dir, script_name, passthrough)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
