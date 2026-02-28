"""Platform auth preflight checks for Crawlee-backed jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import CREDENTIAL_ACCOUNT_REGISTRY


@dataclass(frozen=True)
class AuthPreflightResult:
    ok: bool
    platform: str
    auth_mode: str
    auth_source: str | None
    missing: tuple[str, ...]
    account_ref: str | None
    reason: str | None = None


class AuthPreflightError(RuntimeError):
    """Raised when required auth is missing for enabled Crawlee runtime."""

    def __init__(self, result: AuthPreflightResult):
        self.result = result
        self.error_code = "auth"
        self.error_class = self.__class__.__name__
        self.retryable = False
        self.runtime_metadata = {
            "auth_context": build_auth_context(result),
            "crawler_runtime": {
                "blocked_events": 0,
                "requests_total": 0,
                "requests_handled": 0,
                "retries_total": 0,
                "session_rotations": 0,
                "max_proxy_tier_used": 0,
                "crawlee_request_count": 0,
                "crawlee_retry_count": 0,
                "crawlee_session_pool_used": False,
            },
        }
        detail = result.reason or "required auth credentials are missing"
        super().__init__(f"crawlee_auth_preflight_failed:{result.platform}:{detail}")


def _ok(*, platform: str, mode: str, source: str | None) -> AuthPreflightResult:
    return AuthPreflightResult(
        ok=True,
        platform=platform,
        auth_mode=mode,
        auth_source=source,
        missing=(),
        account_ref=CREDENTIAL_ACCOUNT_REGISTRY.get(platform),
    )


def _fail(*, platform: str, mode: str, missing: tuple[str, ...], reason: str) -> AuthPreflightResult:
    return AuthPreflightResult(
        ok=False,
        platform=platform,
        auth_mode=mode,
        auth_source=None,
        missing=missing,
        account_ref=CREDENTIAL_ACCOUNT_REGISTRY.get(platform),
        reason=reason,
    )


def check_platform_auth(
    *,
    platform: str,
    instagram_cookies: dict[str, str] | None = None,
    tiktok_cookies: dict[str, str] | None = None,
    facebook_cookies: dict[str, str] | None = None,
    threads_cookies: dict[str, str] | None = None,
    twitter_cookies: dict[str, str] | None = None,
    twitter_bearer: str | None = None,
    twikit_credentials: dict[str, str] | None = None,
) -> AuthPreflightResult:
    normalized_platform = (platform or "").strip().lower()

    if normalized_platform == "instagram":
        if instagram_cookies:
            return _ok(platform=normalized_platform, mode="cookies", source="SOCIAL_INSTAGRAM_COOKIES_*")
        return _fail(
            platform=normalized_platform,
            mode="cookies",
            missing=("SOCIAL_INSTAGRAM_COOKIES_JSON|SOCIAL_INSTAGRAM_COOKIES_FILE",),
            reason="instagram_cookies_missing",
        )

    if normalized_platform == "tiktok":
        if tiktok_cookies:
            return _ok(platform=normalized_platform, mode="cookies", source="SOCIAL_TIKTOK_COOKIES_*|TIKTOK_COOKIES_*")
        return _fail(
            platform=normalized_platform,
            mode="cookies",
            missing=("SOCIAL_TIKTOK_COOKIES_JSON|SOCIAL_TIKTOK_COOKIES_FILE|TIKTOK_COOKIES_*",),
            reason="tiktok_cookies_missing",
        )

    if normalized_platform == "twitter":
        if twitter_cookies:
            return _ok(
                platform=normalized_platform,
                mode="cookies",
                source="SOCIAL_TWITTER_COOKIES_*|TWITTER_COOKIES_*",
            )
        if twitter_bearer:
            return _ok(
                platform=normalized_platform,
                mode="bearer",
                source="SOCIAL_TWITTER_BEARER_TOKEN|TWITTER_BEARER_TOKEN",
            )
        if twikit_credentials:
            return _ok(platform=normalized_platform, mode="twikit", source="TWIKIT_*")
        return _fail(
            platform=normalized_platform,
            mode="cookies_or_bearer_or_twikit",
            missing=(
                "SOCIAL_TWITTER_COOKIES_JSON|SOCIAL_TWITTER_COOKIES_FILE|TWITTER_COOKIES_*",
                "SOCIAL_TWITTER_BEARER_TOKEN|TWITTER_BEARER_TOKEN",
                "TWIKIT_*",
            ),
            reason="twitter_auth_missing",
        )

    if normalized_platform == "youtube":
        # YouTube scraping currently supports public mode in the existing scraper.
        return _ok(platform=normalized_platform, mode="public", source="none")

    if normalized_platform == "facebook":
        if facebook_cookies:
            return _ok(
                platform=normalized_platform,
                mode="cookies",
                source="SOCIAL_FACEBOOK_COOKIES_JSON|SOCIAL_FACEBOOK_COOKIES_FILE|FACEBOOK_COOKIES_*",
            )
        return _ok(platform=normalized_platform, mode="public", source="none")

    if normalized_platform == "threads":
        if threads_cookies:
            return _ok(
                platform=normalized_platform,
                mode="cookies",
                source="SOCIAL_THREADS_COOKIES_JSON|SOCIAL_THREADS_COOKIES_FILE|THREADS_COOKIES_*",
            )
        return _ok(platform=normalized_platform, mode="public", source="none")

    return _fail(
        platform=normalized_platform,
        mode="unknown",
        missing=(),
        reason="unsupported_platform",
    )


def build_auth_context(result: AuthPreflightResult, *, fallback_to_legacy: bool = False) -> dict[str, Any]:
    return {
        "auth_mode": result.auth_mode,
        "auth_source": result.auth_source,
        "auth_preflight_ok": result.ok,
        "fallback_to_legacy": fallback_to_legacy,
        "account_ref": result.account_ref,
        "missing_hints": list(result.missing),
        "reason": result.reason,
    }
