#!/usr/bin/env python3
"""Plan or apply one parent-reviewed installed January media repair attempt."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import UUID

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from trr_backend.ingestion.media_repair_budget import RepairQualification, extend_media_repair_budget
from trr_backend.ingestion.persistence import Store


def _configuration(env_file):
    if env_file:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=True)
    from scripts.ingestion.local_driver import _project_dsn, validate_configuration
    from trr_backend.db.connection import resolve_database_url_candidate_details

    validate_configuration(os.environ["TRR_INSTAGRAM_DBOS_DATABASE_URL"])
    if not _project_dsn(resolve_database_url_candidate_details()[0]["url"]):
        raise ValueError("TRR project database configuration required")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--account", choices=["bravotv"], required=True)
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--intent-hash", required=True)
    parser.add_argument("--placement", choices=["local"], required=True)
    parser.add_argument("--target-key", required=True)
    parser.add_argument("--checkpoint-sha256", required=True)
    parser.add_argument("--owner-generation", type=int, required=True)
    parser.add_argument("--attempts", type=int, required=True)
    parser.add_argument("--max-attempts", type=int, required=True)
    parser.add_argument("--failure-code", required=True)
    parser.add_argument("--repair-revision", required=True)
    parser.add_argument("--failed-revision", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--review", type=Path, required=True)
    parser.add_argument("--review-sha256", required=True)
    parser.add_argument("--freeze", type=Path, required=True)
    parser.add_argument("--diagnostic", type=Path, required=True)
    parser.add_argument("--diagnostic-sha256", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--initiated-by", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    _configuration(args.env_file)
    # Reserve a private output before any mutation; an existing receipt cannot
    # be overwritten and an output-path failure cannot follow a fresh grant.
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        result = extend_media_repair_budget(
            Store(execution_placement=args.placement),
            intent_hash=args.intent_hash,
            apply=args.apply,
            run_id=str(args.run_id),
            account=args.account,
            generation=args.generation,
            target_key=args.target_key,
            expected_checkpoint_hash=args.checkpoint_sha256,
            expected_owner_generation=args.owner_generation,
            expected_attempts=args.attempts,
            expected_max_attempts=args.max_attempts,
            expected_failure_code=args.failure_code,
            qualification=RepairQualification(
                args.repair_revision,
                str(args.manifest),
                args.manifest_sha256,
                str(args.review),
                args.review_sha256,
                str(args.freeze),
                str(args.diagnostic),
                args.diagnostic_sha256,
            ),
            failed_revision=args.failed_revision,
            reason=args.reason,
            initiated_by=args.initiated_by,
        )
        handle.write(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                "applied": result["applied"],
                "target_key": result["target_key"],
                "idempotency_key": result["idempotency_key"],
                "output": str(args.output),
                "final_acceptance": False,
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        # Database/configuration exceptions can contain connection material.
        print(json.dumps({"error": "media_repair_operator_failed", "category": type(error).__name__}), file=sys.stderr)
        raise SystemExit(2) from None
