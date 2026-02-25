"""Crawlee runtime helpers for incremental social scraping integration."""

from .auth_preflight import (
    AuthPreflightError,
    AuthPreflightResult,
    build_auth_context,
    check_platform_auth,
)
from .config import (
    CRAWLEE_SUPPORTED_PLATFORMS,
    CREDENTIAL_ACCOUNT_REGISTRY,
    CrawleeRuntimeConfig,
    build_runtime_config,
    is_auth_strict_for_platform,
    should_use_crawlee,
)
from .error_taxonomy import classify_exception
from .request_keys import build_request_key
from .runtime import CrawleeRuntimeError, execute_platform_stage_with_crawlee

__all__ = [
    "AuthPreflightError",
    "AuthPreflightResult",
    "CRAWLEE_SUPPORTED_PLATFORMS",
    "CREDENTIAL_ACCOUNT_REGISTRY",
    "CrawleeRuntimeConfig",
    "CrawleeRuntimeError",
    "build_auth_context",
    "build_request_key",
    "build_runtime_config",
    "is_auth_strict_for_platform",
    "check_platform_auth",
    "classify_exception",
    "execute_platform_stage_with_crawlee",
    "should_use_crawlee",
]
