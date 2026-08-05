"""Configured loader port for canonical Threads cookies."""

from __future__ import annotations

from typing import Protocol


class ThreadsCookieLoader(Protocol):
    """Load Threads cookies through the configured composition root."""

    def __call__(self) -> dict[str, str]: ...


_threads_cookie_loader: ThreadsCookieLoader | None = None


def configure_threads_cookie_loader(loader: ThreadsCookieLoader) -> None:
    global _threads_cookie_loader

    _threads_cookie_loader = loader


def load_threads_cookies() -> dict[str, str]:
    loader = _threads_cookie_loader
    if loader is None:
        raise RuntimeError("Threads cookie loader is not configured")
    return loader()


__all__ = [
    "ThreadsCookieLoader",
    "configure_threads_cookie_loader",
    "load_threads_cookies",
]
