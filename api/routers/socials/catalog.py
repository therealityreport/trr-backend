"""Profile-scoped account catalog route surface."""

from __future__ import annotations

from typing import Any

from ._surfaces import RouteRecord, routes_matching

ROUTE_PREFIXES = (
    "/admin/socials/profiles/",
)
REQUIRED_PATH_FRAGMENT = "/catalog/"


def surface_routes(router: Any) -> list[RouteRecord]:
    return [record for record in routes_matching(router, ROUTE_PREFIXES) if REQUIRED_PATH_FRAGMENT in record[1]]

