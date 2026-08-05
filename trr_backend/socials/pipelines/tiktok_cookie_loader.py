"""Configured loader port for canonical TikTok cookies."""

from __future__ import annotations

from typing import Protocol


class TikTokCookieLoader(Protocol):
    """Load TikTok cookies through the configured composition root."""

    def __call__(self) -> dict[str, str]: ...


_tiktok_cookie_loader: TikTokCookieLoader | None = None


def configure_tiktok_cookie_loader(loader: TikTokCookieLoader) -> None:
    global _tiktok_cookie_loader

    _tiktok_cookie_loader = loader


def load_tiktok_cookies() -> dict[str, str]:
    loader = _tiktok_cookie_loader
    if loader is None:
        raise RuntimeError("TikTok cookie loader is not configured")
    return loader()


__all__ = [
    "TikTokCookieLoader",
    "configure_tiktok_cookie_loader",
    "load_tiktok_cookies",
]
