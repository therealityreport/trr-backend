"""Screenalytics service-to-service auth."""

from __future__ import annotations

import hmac
import os
from functools import lru_cache

from fastapi import HTTPException, Request


@lru_cache
def get_screenalytics_service_token() -> str:
    token = os.getenv("SCREENALYTICS_SERVICE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("SCREENALYTICS_SERVICE_TOKEN environment variable is not set")
    return token


def require_screenalytics_service_token(request: Request) -> None:
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(
            status_code=401,
            detail="Authorization required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=401,
            detail="Invalid authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    expected = get_screenalytics_service_token()
    if not hmac.compare_digest(parts[1], expected):
        raise HTTPException(
            status_code=401,
            detail="Invalid service token",
            headers={"WWW-Authenticate": "Bearer"},
        )
