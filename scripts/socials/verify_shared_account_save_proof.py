#!/usr/bin/env python3
"""Print fetched/upserted/materialized proof for a shared social account run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._workspace_runtime_env import apply_workspace_runtime_env  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, help="Social platform, for example facebook or threads.")
    parser.add_argument("--account", required=True, help="Account handle, for example bravotv.")
    parser.add_argument("--run-id", help="Optional catalog run id to scope proof to one run.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    load_dotenv(REPO_ROOT / ".env", override=False)
    apply_workspace_runtime_env(repo_root=REPO_ROOT)

    from trr_backend.repositories import social_season_analytics as repo

    proof = repo.get_social_account_catalog_save_proof(
        args.platform,
        args.account,
        run_id=args.run_id,
    )
    print(json.dumps(proof, indent=2 if args.pretty else None, sort_keys=True, default=str))
    return 0 if proof.get("verified") else 2


if __name__ == "__main__":
    raise SystemExit(main())
