"""Configured loader port for canonical Facebook cookies."""

from __future__ import annotations

from typing import Protocol


class FacebookCookieLoader(Protocol):
    """Load Facebook cookies through the configured composition root."""

    def __call__(self) -> dict[str, str]: ...


_facebook_cookie_loader: FacebookCookieLoader | None = None


def configure_facebook_cookie_loader(loader: FacebookCookieLoader) -> None:
    global _facebook_cookie_loader

    _facebook_cookie_loader = loader


def load_facebook_cookies() -> dict[str, str]:
    loader = _facebook_cookie_loader
    if loader is None:
        raise RuntimeError("Facebook cookie loader is not configured")
    return loader()


__all__ = [
    "FacebookCookieLoader",
    "configure_facebook_cookie_loader",
    "load_facebook_cookies",
]
