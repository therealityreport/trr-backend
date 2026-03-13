from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.client import Config
from botocore.exceptions import ProfileNotFound


@dataclass(frozen=True)
class ObjectStorageConfig:
    provider: str
    bucket: str
    region: str
    public_base_url: str | None
    prefix: str
    endpoint_url: str | None
    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None
    profile_name: str | None


def _env(name: str) -> str:
    return str(os.getenv(name) or "").strip()


def _first_env(*names: str) -> str:
    for name in names:
        value = _env(name)
        if value:
            return value
    return ""


def _validate_public_base_url(value: str) -> str:
    base = str(value or "").strip()
    if not base:
        raise RuntimeError(
            "Missing required environment variable: OBJECT_STORAGE_PUBLIC_BASE_URL (or AWS_CDN_BASE_URL)"
        )
    if not base.startswith("https://"):
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must start with https://")
    parsed = urlparse(base)
    host = (parsed.netloc or "").strip().lower()
    if not host:
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must include a valid host")
    if host == "s3.amazonaws.com" or host.endswith(".s3.amazonaws.com"):
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must not be a direct S3 endpoint")
    if re.match(r"^s3[.-][a-z0-9-]+\.amazonaws\.com$", host):
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must not be a direct S3 endpoint")
    if re.match(r"^[a-z0-9.-]+\.s3[.-][a-z0-9-]+\.amazonaws\.com$", host):
        raise RuntimeError("OBJECT_STORAGE_PUBLIC_BASE_URL must not be a direct S3 endpoint")
    return base.rstrip("/")


def load_object_storage_config(
    *,
    require_bucket: bool = True,
    require_public_base_url: bool = False,
) -> ObjectStorageConfig:
    bucket = _first_env("OBJECT_STORAGE_BUCKET", "AWS_S3_BUCKET")
    if require_bucket and not bucket:
        raise RuntimeError("Missing required environment variable: OBJECT_STORAGE_BUCKET (or AWS_S3_BUCKET)")

    region = _first_env("OBJECT_STORAGE_REGION", "AWS_REGION", "AWS_DEFAULT_REGION") or "us-east-1"
    public_base_url = _first_env("OBJECT_STORAGE_PUBLIC_BASE_URL", "AWS_CDN_BASE_URL") or None
    if require_public_base_url:
        public_base_url = _validate_public_base_url(public_base_url or "")
    elif public_base_url:
        public_base_url = _validate_public_base_url(public_base_url)

    prefix = _first_env("OBJECT_STORAGE_PREFIX", "AWS_S3_PREFIX").strip().strip("/")
    endpoint_url = _first_env("OBJECT_STORAGE_ENDPOINT_URL", "AWS_ENDPOINT_URL") or None
    provider = (_first_env("OBJECT_STORAGE_PROVIDER") or "").lower()
    if not provider:
        provider = "r2" if endpoint_url and "cloudflarestorage.com" in endpoint_url else "s3"

    access_key_id = _first_env("OBJECT_STORAGE_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID") or None
    secret_access_key = _first_env("OBJECT_STORAGE_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY") or None
    session_token = _first_env("OBJECT_STORAGE_SESSION_TOKEN", "AWS_SESSION_TOKEN") or None
    profile_name = None
    if not (access_key_id and secret_access_key):
        profile_name = _first_env("OBJECT_STORAGE_PROFILE", "AWS_PROFILE", "AWS_DEFAULT_PROFILE") or None

    return ObjectStorageConfig(
        provider=provider,
        bucket=bucket,
        region=region,
        public_base_url=public_base_url,
        prefix=prefix,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        profile_name=profile_name,
    )


def _build_boto3_session(config: ObjectStorageConfig) -> boto3.Session:
    if config.profile_name:
        try:
            return boto3.Session(profile_name=config.profile_name, region_name=config.region)
        except ProfileNotFound:
            if config.access_key_id and config.secret_access_key:
                return boto3.Session(region_name=config.region)
            raise
    return boto3.Session(region_name=config.region)


def build_s3_client(config: ObjectStorageConfig):
    session = _build_boto3_session(config)
    client_kwargs: dict[str, Any] = {"region_name": config.region}
    if config.endpoint_url:
        client_kwargs["endpoint_url"] = config.endpoint_url
        client_kwargs["config"] = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    if config.access_key_id and config.secret_access_key:
        client_kwargs["aws_access_key_id"] = config.access_key_id
        client_kwargs["aws_secret_access_key"] = config.secret_access_key
        if config.session_token:
            client_kwargs["aws_session_token"] = config.session_token
    return session.client("s3", **client_kwargs)
