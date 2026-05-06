"""Legacy direct platform scrape and preview route surface."""

from __future__ import annotations

from typing import Any

from ._surfaces import RouteRecord, routes_matching

ROUTE_PREFIXES = (
    "/admin/socials/instagram/",
    "/admin/socials/tiktok/",
    "/admin/socials/twitter/",
    "/admin/socials/youtube/",
    "/admin/socials/facebook/",
    "/admin/socials/threads/",
    "/admin/socials/landing-",
)


def surface_routes(router: Any) -> list[RouteRecord]:
    return routes_matching(router, ROUTE_PREFIXES)

