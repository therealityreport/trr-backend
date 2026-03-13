from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.client import Config


@dataclass(frozen=True)
class BucketObject:
    key: str
    size: int
    etag: str | None
    last_modified: Any | None


def build_aws_s3_client(*, region: str):
    return boto3.client("s3", region_name=region)


def build_r2_client(
    *,
    region: str,
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    session_token: str | None = None,
):
    client_kwargs: dict[str, Any] = {
        "service_name": "s3",
        "region_name": region,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_key_id,
        "aws_secret_access_key": secret_access_key,
        "config": Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    }
    if session_token:
        client_kwargs["aws_session_token"] = session_token
    return boto3.client(**client_kwargs)


def iter_bucket_objects(client: Any, bucket: str, *, prefix: str = "") -> Iterator[BucketObject]:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            yield BucketObject(
                key=str(item["Key"]),
                size=int(item.get("Size") or 0),
                etag=str(item.get("ETag") or "").strip('"') or None,
                last_modified=item.get("LastModified"),
            )


def head_object_or_none(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except Exception:
        return None
