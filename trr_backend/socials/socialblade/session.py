from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trr_backend.socials.socialblade.cookies import normalize_socialblade_cookies


@dataclass(slots=True)
class SocialBladeScraplingSession:
    raw_cookies: dict[str, str]
    cookies: list[dict[str, Any]]


def resolve_socialblade_scrapling_session(raw_payload: Any) -> SocialBladeScraplingSession:
    cookies = normalize_socialblade_cookies(raw_payload)
    raw_cookies: dict[str, str] = {}
    for cookie in cookies:
        name = str(cookie.get("name") or "").strip()
        value = str(cookie.get("value") or "")
        if not name or not value:
            continue
        raw_cookies[name] = value
    return SocialBladeScraplingSession(
        raw_cookies=raw_cookies,
        cookies=cookies,
    )
