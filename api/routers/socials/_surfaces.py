"""Route-surface helpers for the admin socials router package."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

RouteRecord = tuple[str, str]


def route_inventory(router: Any) -> list[RouteRecord]:
    records: list[RouteRecord] = []
    for route in getattr(router, "routes", []) or []:
        path = str(getattr(route, "path", "") or "")
        methods = sorted(
            method
            for method in (getattr(route, "methods", None) or [])
            if method not in {"HEAD", "OPTIONS"}
        )
        for method in methods:
            records.append((method, path))
    return sorted(records)


def routes_matching(router: Any, prefixes: Iterable[str]) -> list[RouteRecord]:
    normalized_prefixes = tuple(str(prefix or "").strip() for prefix in prefixes if str(prefix or "").strip())
    return [
        record
        for record in route_inventory(router)
        if any(record[1].startswith(prefix) for prefix in normalized_prefixes)
    ]

