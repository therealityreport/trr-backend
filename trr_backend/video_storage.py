"""Durable private-original references, independent of migration history."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

from trr_backend.object_storage import load_object_storage_config


def private_video_bucket() -> str:
    bucket = str(os.getenv("OBJECT_STORAGE_VIDEO_BUCKET") or "").strip()
    if not bucket:
        raise RuntimeError("Missing required environment variable: OBJECT_STORAGE_VIDEO_BUCKET")
    if bucket == load_object_storage_config().bucket:
        raise RuntimeError("Private video bucket must differ from the public media bucket")
    return bucket


def video_url_ttl() -> int:
    value = int(os.getenv("OBJECT_STORAGE_VIDEO_URL_TTL_SECONDS") or "3600")
    if not 60 <= value <= 3600:
        raise RuntimeError("OBJECT_STORAGE_VIDEO_URL_TTL_SECONDS must be between 60 and 3600")
    return value


def source_reference(asset: dict[str, Any]) -> tuple[str, str]:
    """Read new explicit references and legacy records without changing ownership."""
    source = asset.get("source_json") or {}
    if not isinstance(source, dict):
        raise ValueError("Video source_json must be an object")
    parsed = urlparse(str(asset.get("source_url") or ""))
    uri_bucket = parsed.netloc if parsed.scheme == "s3" else ""
    uri_key = parsed.path.lstrip("/") if uri_bucket else ""
    bucket = str(source.get("bucket") or uri_bucket or "").strip()
    key = str(source.get("object_key") or uri_key or "").strip()
    if source.get("access") == "private" and not bucket:
        raise ValueError("Private video reference is missing its bucket")
    # Old retained contracts carried only object_key. Keep them readable until backfilled.
    bucket = bucket or load_object_storage_config().bucket
    if not key:
        raise ValueError("Video reference is missing its object key")
    if uri_bucket and (uri_bucket != bucket or uri_key != key):
        raise ValueError("Video URL and durable reference disagree")
    provider = source.get("storage_provider")
    if provider and provider != load_object_storage_config().provider:
        raise ValueError("Video reference provider does not match configured storage")
    return bucket, key


def private_source_json(bucket: str, key: str, **metadata: Any) -> dict[str, Any]:
    return {
        **metadata,
        "storage_reference_version": 1,
        "storage_provider": load_object_storage_config().provider,
        "bucket": bucket,
        "object_key": key,
        "access": "private",
    }
