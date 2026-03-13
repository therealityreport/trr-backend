#!/usr/bin/env python3
"""Compare object counts and total bytes between an AWS S3 bucket and a Cloudflare R2 bucket."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass

from scripts.storage._s3_compatible import build_aws_s3_client, build_r2_client, iter_bucket_objects


@dataclass
class BucketSummary:
    bucket: str
    object_count: int = 0
    total_bytes: int = 0


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
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _summarize_bucket(client, bucket: str, *, prefix: str) -> BucketSummary:
    summary = BucketSummary(bucket=bucket)
    for item in iter_bucket_objects(client, bucket, prefix=prefix):
        summary.object_count += 1
        summary.total_bytes += item.size
    return summary


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

    source = _summarize_bucket(source_client, args.source_bucket, prefix=args.prefix)
    destination = _summarize_bucket(destination_client, args.destination_bucket, prefix=args.prefix)
    matched = source.object_count == destination.object_count and source.total_bytes == destination.total_bytes
    payload = {
        "matched": matched,
        "source": asdict(source),
        "destination": asdict(destination),
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(
            f"matched={matched} source_objects={source.object_count} destination_objects={destination.object_count} "
            f"source_bytes={source.total_bytes} destination_bytes={destination.total_bytes}"
        )
    return 0 if matched else 1


if __name__ == "__main__":
    raise SystemExit(main())
