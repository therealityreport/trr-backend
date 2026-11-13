from typing import cast

import pytest
from fastapi import HTTPException

from api.routers.socials import profile_cookies
from api.routers.socials.profile_reads import CookieRefreshRequest
from trr_backend.socials.control_plane import runtime


def test_public_health_skips_every_auth_probe(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "public")

    def forbidden(*args, **kwargs):
        raise AssertionError("Public health attempted to read login info")

    monkeypatch.setattr(runtime, "check_platform_cookie_health", forbidden)
    result = profile_cookies.get_cookie_health_route(
        "instagram", "bravotv", force=True, posts_auth=True, comments_auth=True
    )
    assert result["required"] is False
    assert result["source_kind"] == "public_only"
    assert result["auth_probe_skipped"] is True
    assert result["comments_auth_health"]["authenticated_ready"] is False
    assert result["refresh_available"] is False


def test_public_refresh_rejected_before_loading_login_info(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "public")

    def forbidden(*args, **kwargs):
        raise AssertionError("Public refresh attempted to read login info")

    monkeypatch.setattr(runtime, "check_platform_cookie_health", forbidden)
    with pytest.raises(HTTPException) as error:
        profile_cookies.post_cookie_refresh_route("instagram", "bravotv", CookieRefreshRequest())
    assert error.value.status_code == 403
    detail = cast(object, error.value.detail)
    assert isinstance(detail, dict)
    assert detail["code"] == "INSTAGRAM_PUBLIC_ONLY"


def test_modal_strict_probe_cannot_override_public_only_policy(monkeypatch):
    from trr_backend import modal_jobs
    from trr_backend.socials.pipelines.comments import instagram

    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "public")

    def forbidden(*args, **kwargs):
        raise AssertionError("Strict probe attempted to use login info")

    monkeypatch.setattr(instagram, "_probe_instagram_comments_endpoint_for_launch", forbidden)
    result = modal_jobs.probe_instagram_comments_auth.local("bravotv", "DaTL41Gk4R6", True)
    assert result["auth_probe_skipped"] is True
    assert result["authenticated_ready"] is False
    assert result["comments_auth_blocker"] == "public_only_policy"
