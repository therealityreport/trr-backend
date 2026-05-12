"""Season ingest, sync-session, shared-account ingest, and target route surface."""

from __future__ import annotations

from typing import Any

from ._surfaces import RouteRecord, routes_matching

ROUTE_PREFIXES = (
    "/admin/socials/seasons/",
    "/admin/socials/shared/",
)


def surface_routes(router: Any) -> list[RouteRecord]:
    return [
        record
        for record in routes_matching(router, ROUTE_PREFIXES)
        if "/analytics" not in record[1] and "/tiktok/" not in record[1]
    ]
