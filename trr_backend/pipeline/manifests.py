"""S3-compatible manifest storage for pipeline stages."""

from __future__ import annotations

import json
import logging

from botocore.exceptions import ClientError

from trr_backend.object_storage import build_s3_client, load_object_storage_config
from trr_backend.pipeline.models import StageManifest

logger = logging.getLogger(__name__)


def get_manifest_bucket() -> str:
    """Get object-storage bucket for manifests."""
    return load_object_storage_config(require_bucket=True).bucket


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

    s3 = build_s3_client(load_object_storage_config(require_bucket=True))
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest.to_dict(), indent=2),
        ContentType="application/json",
    )
    logger.debug("Wrote manifest to s3://%s/%s", bucket, key)
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

    s3 = build_s3_client(load_object_storage_config(require_bucket=True))
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read())
        return StageManifest(**data)
    except ClientError as e:
        # boto3 raises ClientError - check for NoSuchKey or 404
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
