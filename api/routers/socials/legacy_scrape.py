# ruff: noqa: F401, F403, F405, I001
"""Legacy direct platform scrape and preview route aggregator."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ._surfaces import RouteRecord, routes_matching
from . import social_landing as _social_landing
from . import instagram_scrape as _instagram_scrape
from . import platform_scrape as _platform_scrape
from .social_landing import *
from .instagram_scrape import *
from .platform_scrape import *

router = APIRouter()
router.routes.extend(_social_landing.router.routes)
router.routes.extend(_instagram_scrape.router.routes)
router.routes.extend(_platform_scrape.router.routes)

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
