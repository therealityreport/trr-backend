from __future__ import annotations

import os
from typing import Any


def _split_proxy_values(raw: str) -> list[str]:
    values: list[str] = []
    for chunk in str(raw or "").replace("\n", ",").split(","):
        value = chunk.strip()
        if value:
            values.append(value)
    return values


def _build_decodo_proxy_url() -> str | None:
    username = str(os.getenv("DECODO_USERNAME") or "").strip()
    password = str(os.getenv("DECODO_PASSWORD") or "").strip()
    gateway = str(os.getenv("DECODO_GATEWAY") or "gate.decodo.com:7000").strip()
    if not (username and password and gateway):
        return None
    return f"http://{username}:{password}@{gateway}"


def load_proxy_urls_from_env() -> list[str]:
    proxy_urls = _split_proxy_values(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS") or "")
    if proxy_urls:
        return proxy_urls
    provider = str(os.getenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER") or "").strip().lower()
    if provider in {"", "decodo", "smartproxy"}:
        decodo_url = _build_decodo_proxy_url()
        if decodo_url:
            return [decodo_url]
    return []


def build_proxy_rotator_from_env() -> Any | None:
    proxy_urls = load_proxy_urls_from_env()
    if not proxy_urls:
        return None
    try:
        # P1-4: Use the public `scrapling.fetchers` re-export rather than the
        # internal `scrapling.engines.toolbelt.proxy_rotation` path. Both point
        # to the same class in 0.4.x, but the public surface is the stable one.
        from scrapling.fetchers import ProxyRotator
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling proxy rotation is unavailable. Install scrapling[fetchers].") from exc
    return ProxyRotator(proxy_urls)
