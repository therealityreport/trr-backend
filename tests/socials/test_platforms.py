from __future__ import annotations

from trr_backend.socials.platforms import (
    SOCIAL_PLATFORM_DEFAULT_ORDER,
    SOCIAL_SUPPORTED_PLATFORMS,
    infer_platform_from_url,
    is_supported_platform,
)


def test_supported_platforms_include_reddit() -> None:
    assert "reddit" in SOCIAL_SUPPORTED_PLATFORMS
    assert "reddit" in SOCIAL_PLATFORM_DEFAULT_ORDER
    assert is_supported_platform("reddit") is True


def test_infer_platform_from_url_recognizes_reddit() -> None:
    assert infer_platform_from_url("https://www.reddit.com/r/BravoRealHousewives/comments/abc123/post") == "reddit"
