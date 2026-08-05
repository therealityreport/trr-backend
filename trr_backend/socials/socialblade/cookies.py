from __future__ import annotations

from typing import Any


def _coerce_cookie_map(raw_payload: Any) -> dict[str, str]:
    cookies: dict[str, str] = {}
    if isinstance(raw_payload, dict):
        nested = raw_payload.get("cookies")
        if isinstance(nested, list):
            return _coerce_cookie_map(nested)

        name = raw_payload.get("name")
        value = raw_payload.get("value")
        if name is not None and value is not None:
            name_str = str(name).strip()
            value_str = str(value)
            return {name_str: value_str} if name_str and value_str else {}

        for key, value in raw_payload.items():
            if value is None:
                continue
            key_str = str(key).strip()
            if not key_str:
                continue
            if isinstance(value, (str, int, float, bool)):
                value_str = str(value)
                if value_str:
                    cookies[key_str] = value_str
        return cookies

    if isinstance(raw_payload, list):
        for item in raw_payload:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            value = str(item.get("value") or "")
            if name and value:
                cookies[name] = value
    return cookies


def normalize_socialblade_cookies(raw_payload: Any) -> list[dict[str, Any]]:
    if isinstance(raw_payload, list):
        normalized: list[dict[str, Any]] = []
        for cookie in raw_payload:
            if not isinstance(cookie, dict):
                continue
            name = str(cookie.get("name") or "").strip()
            value = str(cookie.get("value") or "")
            if not name or not value:
                continue
            rendered = dict(cookie)
            rendered.setdefault("domain", ".socialblade.com")
            rendered.setdefault("path", "/")
            normalized.append(rendered)
        return normalized

    cookie_map = _coerce_cookie_map(raw_payload)
    normalized = []
    for name, value in cookie_map.items():
        normalized.append(
            {
                "name": name,
                "value": value,
                "domain": ".socialblade.com",
                "path": "/",
                "secure": True,
            }
        )
    return normalized
