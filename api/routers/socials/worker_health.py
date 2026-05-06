"""Worker health, queue status, live-status, and job-debug route surface."""

from __future__ import annotations

from typing import Any

from ._surfaces import RouteRecord, routes_matching

ROUTE_PREFIXES = (
    "/admin/socials/ingest/",
    "/admin/socials/live-status",
)


def surface_routes(router: Any) -> list[RouteRecord]:
    return routes_matching(router, ROUTE_PREFIXES)

