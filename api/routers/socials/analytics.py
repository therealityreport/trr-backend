"""Season analytics and media-mirror route surface."""

from __future__ import annotations

from typing import Any

from ._surfaces import RouteRecord, routes_matching

ROUTE_PREFIXES = (
    "/admin/socials/seasons/",
)


def surface_routes(router: Any) -> list[RouteRecord]:
    return [
        record
        for record in routes_matching(router, ROUTE_PREFIXES)
        if "/analytics" in record[1] or "/tiktok/" in record[1] or "/mirror/" in record[1]
    ]

