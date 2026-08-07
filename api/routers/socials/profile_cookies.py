# ruff: noqa: F401, F403, F405
"""Profile-scoped cookie-health and refresh routes."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from fastapi import APIRouter

from ._shared import *
from .profile_reads import *

if TYPE_CHECKING:
    from collections.abc import Callable

    from ._shared import _require_instagram_auth_refresh_confirmation
    from .profile_reads import (
        _cookie_health_auth_probe_metadata,
        _instagram_comments_auth_probe_is_rate_limited,
    )

router = APIRouter()


@router.get("/profiles/{platform}/{account_handle}/cookies/health")
def get_cookie_health_route(
    platform: str,
    account_handle: str,
    force: bool = Query(default=False, description="Bypass validation cache"),
    posts_auth: bool = Query(default=False, description="Include Instagram Modal posts endpoint auth probe"),
    comments_auth: bool = Query(default=False, description="Include Instagram Modal comments endpoint auth probe"),
    user: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import check_platform_cookie_health

    if TYPE_CHECKING:
        # These probes are injected into the launch module's globals at runtime
        # by its provider bridge, so static checkers cannot see them as import
        # symbols.
        probe_modal_instagram_comments_auth_health = cast("Callable[..., dict[str, Any]]", None)
        probe_modal_instagram_posts_auth_health = cast("Callable[..., dict[str, Any]]", None)
    else:
        from trr_backend.socials.pipelines.account_catalog.launch import (
            probe_modal_instagram_comments_auth_health,
            probe_modal_instagram_posts_auth_health,
        )

    health = check_platform_cookie_health(platform, force=force)
    auth_probe_blocked = False
    auth_probe_reason: str | None = None
    local_cookie_fingerprint = str(health.get("cookie_fingerprint") or "").strip() or None
    if posts_auth and str(platform or "").strip().lower() == "instagram":
        posts_probe = _cookie_health_auth_probe_metadata(probe_modal_instagram_posts_auth_health(account_handle))
        posts_cookie_fingerprint = str(posts_probe.get("cookie_fingerprint") or "").strip() or None
        posts_status = (
            str(posts_probe.get("status") or posts_probe.get("result") or ("valid" if posts_probe.get("ready") else ""))
            .strip()
            .lower()
        )
        posts_reason = str(posts_probe.get("reason") or "").strip() or None
        posts_category = (
            "ready"
            if bool(posts_probe.get("ready")) or posts_status == "valid"
            else "transport"
            if posts_status == "transport_blocked"
            else "auth"
            if posts_status == "auth_blocked"
            else "fetch"
            if posts_status == "fetch_blocked"
            else "unknown"
        )
        health = {
            **health,
            "posts_auth_health": {
                "platform": "instagram",
                "account_handle": str(posts_probe.get("account_handle") or account_handle).strip() or account_handle,
                "ready": bool(posts_probe.get("ready")),
                "status": posts_status or None,
                "category": posts_category,
                "reason": posts_reason,
                "execution_backend": str(posts_probe.get("execution_backend") or "modal").strip().lower() or "modal",
                "probe_only": True,
                "probe_source": "cookie_health",
                "repair_action": None,
                "repair_available": False,
                "cookie_fingerprint": posts_cookie_fingerprint,
                "cookie_fingerprint_match": (
                    local_cookie_fingerprint == posts_cookie_fingerprint
                    if local_cookie_fingerprint and posts_cookie_fingerprint
                    else None
                ),
            },
            "posts_auth_probe": posts_probe,
        }
        if posts_category == "auth":
            auth_probe_blocked = True
            auth_probe_reason = posts_reason or "posts_auth_blocked"
    if comments_auth and str(platform or "").strip().lower() == "instagram":
        comments_probe = _cookie_health_auth_probe_metadata(
            probe_modal_instagram_comments_auth_health(account_handle, strict_authenticated=True)
        )
        comments_cookie_fingerprint = str(comments_probe.get("cookie_fingerprint") or "").strip() or None
        comments_status = (
            str(
                comments_probe.get("status")
                or comments_probe.get("result")
                or ("valid" if comments_probe.get("ready") else "")
            )
            .strip()
            .lower()
        )
        comments_reason = str(comments_probe.get("reason") or "").strip() or None
        comments_public_ready = bool(comments_probe.get("public_ready")) or comments_status == "public"
        comments_authenticated_ready = bool(comments_probe.get("authenticated_ready")) or (
            comments_status == "valid" and not bool(comments_probe.get("auth_probe_skipped"))
        )
        comments_rate_limited = _instagram_comments_auth_probe_is_rate_limited(comments_probe)
        comments_category = (
            "ready"
            if comments_authenticated_ready
            else "rate_limited"
            if comments_rate_limited
            else "public"
            if comments_public_ready
            else "transport"
            if comments_status == "transport_blocked"
            else "auth"
            if comments_status == "auth_blocked"
            else "fetch"
            if comments_status == "fetch_blocked"
            else "unknown"
        )
        comments_auth_health = {
            "platform": "instagram",
            "account_handle": str(comments_probe.get("account_handle") or account_handle).strip() or account_handle,
            "shortcode": str(comments_probe.get("shortcode") or "").strip() or None,
            "ready": comments_authenticated_ready,
            "public_ready": comments_public_ready,
            "authenticated_ready": comments_authenticated_ready,
            "auth_probe_skipped": bool(comments_probe.get("auth_probe_skipped")),
            "auth_required_for_hidden_comments": bool(comments_probe.get("auth_required_for_hidden_comments"))
            or not comments_authenticated_ready,
            "comments_auth_blocker": str(comments_probe.get("comments_auth_blocker") or "").strip() or None,
            "operator_action": str(comments_probe.get("operator_action") or "").strip() or None,
            "rate_limited": comments_rate_limited,
            "cooldown_recommended_seconds": comments_probe.get("cooldown_recommended_seconds"),
            "cache_hit": bool(comments_probe.get("cache_hit")),
            "cache_ttl_seconds": comments_probe.get("cache_ttl_seconds"),
            "status": comments_status or None,
            "category": comments_category,
            "reason": comments_reason,
            "execution_backend": str(comments_probe.get("execution_backend") or "modal").strip().lower() or "modal",
            "probe_only": True,
            "probe_source": "cookie_health",
            "repair_action": None,
            "repair_available": False,
        }
        if comments_cookie_fingerprint:
            comments_auth_health["cookie_fingerprint"] = comments_cookie_fingerprint
        if local_cookie_fingerprint and comments_cookie_fingerprint:
            comments_auth_health["cookie_fingerprint_match"] = local_cookie_fingerprint == comments_cookie_fingerprint
        health = {
            **health,
            "comments_auth_health": comments_auth_health,
            "comments_auth_probe": comments_probe,
        }
        if comments_category in {"auth", "public", "rate_limited"}:
            auth_probe_blocked = True
            auth_probe_reason = (
                "comments_auth_rate_limited"
                if comments_category == "rate_limited"
                else comments_reason
                or ("comments_auth_public_only" if comments_category == "public" else "comments_auth_blocked")
            )
    if auth_probe_blocked:
        health = {
            **health,
            "healthy": False,
            "reason": auth_probe_reason,
            "auth_surface_blocked": True,
            "auth_surface_probe_only": True,
        }
    return health


@router.post("/profiles/{platform}/{account_handle}/cookies/refresh")
def post_cookie_refresh_route(
    platform: str,
    account_handle: str,
    payload: CookieRefreshRequest,
    user: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        check_platform_cookie_health,
        refresh_platform_cookies_interactive,
    )

    # Pre-check: is refresh available in this runtime?
    health = check_platform_cookie_health(platform, force=False)
    if not health.get("refresh_supported"):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "COOKIE_REFRESH_NOT_SUPPORTED",
                "message": f"Cookie refresh is not supported for {platform}.",
            },
        )
    if not health.get("refresh_available"):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "COOKIE_REFRESH_REQUIRES_LOCAL",
                "message": (
                    "Cookie refresh requires a local dev environment. A headed browser cannot run on remote workers."
                ),
            },
        )
    _require_instagram_auth_refresh_confirmation(platform, payload.operator_confirmation)

    result = refresh_platform_cookies_interactive(
        platform,
        headless=payload.headless,
        timeout_seconds=payload.timeout_seconds,
        account_handle=account_handle,
        allow_cookie_refresh=bool(payload.allow_cookie_refresh),
    )
    if not result.get("success") and result.get("reason") == "refresh_already_in_progress":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "COOKIE_REFRESH_IN_PROGRESS",
                "message": "A cookie refresh is already in progress for this platform.",
            },
        )
    return result


__all__ = [name for name in globals() if not name.startswith("__")]
