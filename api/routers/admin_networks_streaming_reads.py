from __future__ import annotations

import logging
import time
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from api.auth import InternalAdminUser
from trr_backend.read_path_diagnostics import log_read_path
from trr_backend.services import networks_streaming_reads as networks_streaming_reads_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows/networks-streaming", tags=["admin-networks-streaming-reads"])


def invalidate_networks_streaming_summary_cache() -> None:
    networks_streaming_reads_service.invalidate_networks_streaming_summary_cache()


def _log_read(route: str, *, query_count: int, payload: dict[str, Any], cache_status: str, started_at: float) -> None:
    log_read_path(
        f"admin-networks-streaming.{route}",
        latency_ms=(time.perf_counter() - started_at) * 1000.0,
        query_count=query_count,
        payload=payload,
        extra={"cache": cache_status},
    )


@router.get("/summary")
def get_networks_streaming_summary(_: InternalAdminUser = cast(InternalAdminUser, None)) -> dict[str, Any]:
    started_at = time.perf_counter()
    payload, query_count, cache_status = networks_streaming_reads_service.get_networks_streaming_summary()
    _log_read(
        "summary",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload


@router.post("/summary/cache/invalidate")
def invalidate_summary_cache(_: InternalAdminUser = cast(InternalAdminUser, None)) -> dict[str, bool]:
    invalidate_networks_streaming_summary_cache()
    logger.info("[admin-networks-streaming-read] route=summary-invalidate-cache")
    return {"success": True}


@router.get("/detail")
def get_networks_streaming_detail(
    entity_type: str | None = Query(default=None),
    entity_key: str | None = Query(default=None),
    entity_slug: str | None = Query(default=None),
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    normalized_type = str(entity_type or "").strip().lower()
    normalized_key = str(entity_key or "").strip()
    normalized_slug = str(entity_slug or "").strip()

    if normalized_type not in {"network", "streaming", "production"}:
        raise HTTPException(status_code=400, detail="entity_type must be network, streaming, or production")
    if not normalized_key and not normalized_slug:
        raise HTTPException(status_code=400, detail="entity_key or entity_slug is required")

    try:
        payload, query_count, cache_status = networks_streaming_reads_service.get_networks_streaming_detail(
            entity_type=normalized_type,
            entity_key=normalized_key or None,
            entity_slug=normalized_slug or None,
        )
    except networks_streaming_reads_service.NetworksStreamingDetailNotFoundError as error:
        return_payload = {"error": "not_found", "suggestions": error.suggestions}
        _log_read(
            "detail-not-found",
            query_count=error.query_count,
            payload=return_payload,
            cache_status="miss",
            started_at=started_at,
        )
        return cast("dict[str, Any]", JSONResponse(return_payload, status_code=404))
    _log_read(
        "detail",
        query_count=query_count,
        payload=payload,
        cache_status=cache_status,
        started_at=started_at,
    )
    return payload
