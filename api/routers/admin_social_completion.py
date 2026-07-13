"""Admin social completion and landing-health read endpoints."""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from api.auth import InternalAdminUser
from trr_backend.db.pg import (
    database_service_unavailable_detail,
    is_database_service_unavailable_error,
)
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import social_completion_summary as completion_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/socials", tags=["admin-social-completion"])


def _normalize_platform(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_account_handle(value: str) -> str:
    return str(value or "").strip().lower().lstrip("@")


def _read_year(value: str | None) -> int:
    fallback_year = datetime.now(tz=UTC).year
    if value is None:
        return fallback_year
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return fallback_year
    if not math.isfinite(parsed) or not parsed.is_integer():
        return fallback_year
    year = int(parsed)
    return year if 2000 <= year <= 2100 else fallback_year


def _database_unavailable_exception(error: Exception, request: Request) -> HTTPException:
    detail = database_service_unavailable_detail(error)
    return problem_http_exception(
        request,
        code=str(detail.get("code") or "DATABASE_SERVICE_UNAVAILABLE"),
        status=503,
        message=str(detail.get("message") or "Database service unavailable."),
        retryable=bool(detail.get("retryable", True)),
        extra={
            "reason": detail.get("reason"),
            "retry_after_ms": detail.get("retry_after_ms"),
        },
    )


def _read_failure_exception(*, request: Request, code: str, message: str) -> HTTPException:
    return problem_http_exception(
        request,
        code=code,
        status=500,
        message=message,
        retryable=True,
    )


@router.get("/profiles/{platform}/{account_handle}/completion-summary", response_model=None)
def get_social_profile_completion_summary(
    request: Request,
    platform: str,
    account_handle: str,
    year: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, object] | JSONResponse:
    normalized_platform = _normalize_platform(platform)
    normalized_handle = _normalize_account_handle(account_handle)
    if normalized_platform != completion_repo.SUPPORTED_COMPLETION_PLATFORM or not normalized_handle:
        return JSONResponse(status_code=400, content={"error": "unsupported_profile"})

    normalized_year = _read_year(year)
    try:
        return completion_repo.get_social_completion_summary(
            platform=normalized_platform,
            account_handle=normalized_handle,
            year=normalized_year,
        )
    except Exception as error:
        if is_database_service_unavailable_error(error):
            raise _database_unavailable_exception(error, request) from error
        logger.exception(
            "[admin-social-completion] completion summary failed platform=%s handle=%s year=%s",
            normalized_platform,
            normalized_handle,
            normalized_year,
        )
        raise _read_failure_exception(
            request=request,
            code="SOCIAL_COMPLETION_SUMMARY_FAILED",
            message="Failed to load social completion summary.",
        ) from error


@router.get("/landing-scrape-job-health")
def get_social_landing_scrape_job_health(
    request: Request,
    _: InternalAdminUser = None,
) -> dict[str, object]:
    try:
        return completion_repo.get_social_landing_scrape_job_health()
    except Exception as error:
        if is_database_service_unavailable_error(error):
            raise _database_unavailable_exception(error, request) from error
        logger.exception("[admin-social-completion] landing scrape-job health failed")
        raise _read_failure_exception(
            request=request,
            code="SOCIAL_LANDING_SCRAPE_JOB_HEALTH_FAILED",
            message="Failed to load social landing scrape-job health.",
        ) from error

