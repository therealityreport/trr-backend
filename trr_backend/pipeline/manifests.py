"""S3 manifest storage for pipeline stages."""

from __future__ import annotations

import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

from trr_backend.pipeline.models import StageManifest

logger = logging.getLogger(__name__)


def get_manifest_bucket() -> str:
    """Get S3 bucket for manifests."""
    bucket = os.environ.get("AWS_S3_BUCKET")
    if not bucket:
        raise RuntimeError("AWS_S3_BUCKET environment variable is required for manifest storage")
    return bucket


def get_manifest_key(run_id: str, stage_name: str) -> str:
    """Generate S3 key for stage manifest."""
    return f"pipeline_runs/{run_id}/{stage_name}/manifest.json"


def write_manifest(manifest: StageManifest, *, skip_s3: bool = False) -> str | None:
    """
    Write manifest to S3, return key or None if skipped.

    Args:
        manifest: The stage manifest to write
        skip_s3: If True, skip S3 upload and return None

    Returns:
        S3 key if written, None if skipped
    """
    if skip_s3:
        logger.debug("Skipping manifest write (--skip-s3)")
        return None

    key = get_manifest_key(manifest.run_id, manifest.stage_name)
    bucket = get_manifest_bucket()

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest.to_dict(), indent=2),
        ContentType="application/json",
    )
    logger.debug(f"Wrote manifest to s3://{bucket}/{key}")
    return key


def read_manifest(run_id: str, stage_name: str) -> StageManifest | None:
    """
    Read manifest from S3.

    Args:
        run_id: The pipeline run ID
        stage_name: The stage name

    Returns:
        StageManifest if found, None otherwise
    """
    key = get_manifest_key(run_id, stage_name)
    bucket = get_manifest_bucket()

    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read())
        return StageManifest(**data)
    except ClientError as e:
        # boto3 raises ClientError - check for NoSuchKey or 404
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
