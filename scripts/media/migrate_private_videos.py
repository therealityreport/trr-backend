"""Inventory, then explicitly copy/switch private originals. Never delete objects."""

from __future__ import annotations

import argparse
import json
from urllib.parse import urlparse

from trr_backend.db.connection import resolve_database_url_candidate_details
from trr_backend.object_storage import build_s3_client, load_object_storage_config
from trr_backend.services import private_video_migration as migration
from trr_backend.utils.env import load_env
from trr_backend.video_storage import private_video_bucket


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-ref", required=True, choices=["vwxfvzutyufrkhfgoeaa"])
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument(
        "--register", action="store_true", help="Persist the freshly hashed inventory and old references"
    )
    parser.add_argument("--execute-run", help="Copy, verify, and switch a registered run; preserve every source object")
    args = parser.parse_args()
    if args.register and args.execute_run:
        parser.error("Register and execute are separate steps")
    load_env()
    config = load_object_storage_config()
    # Check the actual configured database identity, not just the command-line attestation.
    details = resolve_database_url_candidate_details()
    if not details:
        raise RuntimeError("Database connection is not configured")
    for candidate in details:
        parsed = urlparse(str(candidate["url"]))
        if args.project_ref not in (parsed.hostname or "") and args.project_ref not in (parsed.username or ""):
            raise RuntimeError("Database connection does not identify the bound TRR Supabase project")
    destination = private_video_bucket()
    if args.source_bucket != config.bucket:
        raise RuntimeError("Source bucket must match configured media bucket")
    client = build_s3_client(config)
    if args.execute_run:
        result = migration.execute_run(client, args.execute_run, destination)
    else:
        objects = migration.inventory(client, args.source_bucket, destination)
        result = {
            "source_bucket": args.source_bucket,
            "destination_bucket": destination,
            "objects": len(objects),
            "references": sum(len(o["references"]) for o in objects),
            "bytes": sum(o["size"] for o in objects),
            "source_objects_deleted": 0,
        }
        if args.register:
            result["run_id"] = migration.register(args.source_bucket, destination, objects, provider=config.provider)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
