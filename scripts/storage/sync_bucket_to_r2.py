#!/usr/bin/env python3
"""Copy one AWS S3 bucket into a Cloudflare R2 bucket using the S3-compatible API."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from typing import Any

from scripts.storage._s3_compatible import (
    build_aws_s3_client,
    build_r2_client,
    head_object_or_none,
    iter_bucket_objects,
)


@dataclass
class SyncSummary:
    source_bucket: str
    destination_bucket: str
    examined_objects: int = 0
    copied_objects: int = 0
    skipped_objects: int = 0
    bytes_copied: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--destination-bucket", required=True)
    parser.add_argument("--source-region", default="us-east-1")
    parser.add_argument("--destination-region", default="auto")
    parser.add_argument("--destination-endpoint-url", required=True)
    parser.add_argument("--destination-access-key-id", required=True)
    parser.add_argument("--destination-secret-access-key", required=True)
    parser.add_argument("--destination-session-token", default="")
    parser.add_argument("--prefix", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _copy_one(
    source_client: Any,
    destination_client: Any,
    *,
    source_bucket: str,
    destination_bucket: str,
    key: str,
) -> int:
    response = source_client.get_object(Bucket=source_bucket, Key=key)
    body = response["Body"].read()
    put_kwargs: dict[str, Any] = {
        "Bucket": destination_bucket,
        "Key": key,
        "Body": body,
    }
    for header in (
        "ContentType",
        "CacheControl",
        "ContentDisposition",
        "ContentEncoding",
        "ContentLanguage",
        "Metadata",
    ):
        value = response.get(header)
        if value:
            put_kwargs[header] = value
    destination_client.put_object(**put_kwargs)
    return len(body)


def main() -> int:
    args = _parse_args()
    source_client = build_aws_s3_client(region=args.source_region)
    destination_client = build_r2_client(
        region=args.destination_region,
        endpoint_url=args.destination_endpoint_url,
        access_key_id=args.destination_access_key_id,
        secret_access_key=args.destination_secret_access_key,
        session_token=args.destination_session_token or None,
    )

    summary = SyncSummary(
        source_bucket=args.source_bucket,
        destination_bucket=args.destination_bucket,
    )

    for index, item in enumerate(iter_bucket_objects(source_client, args.source_bucket, prefix=args.prefix), start=1):
        summary.examined_objects += 1
        if args.limit and index > args.limit:
            break

        if args.skip_existing and head_object_or_none(destination_client, args.destination_bucket, item.key):
            summary.skipped_objects += 1
            continue

        if args.dry_run:
            summary.copied_objects += 1
            summary.bytes_copied += item.size
            continue

        copied_bytes = _copy_one(
            source_client,
            destination_client,
            source_bucket=args.source_bucket,
            destination_bucket=args.destination_bucket,
            key=item.key,
        )
        summary.copied_objects += 1
        summary.bytes_copied += copied_bytes

    if args.json:
        print(json.dumps(asdict(summary), indent=2, default=str))
    else:
        print(
            f"synced source={summary.source_bucket} destination={summary.destination_bucket} "
            f"examined={summary.examined_objects} copied={summary.copied_objects} "
            f"skipped={summary.skipped_objects} bytes={summary.bytes_copied}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
