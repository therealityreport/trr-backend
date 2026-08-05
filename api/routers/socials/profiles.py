# ruff: noqa: F401, F403, F405, I001
"""Profile, account-profile read, and profile-scoped comments route aggregator."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ._surfaces import RouteRecord, routes_matching
from . import profile_reads as _profile_reads
from . import profile_comments as _profile_comments
from . import profile_cookies as _profile_cookies
from .profile_reads import *
from .profile_comments import *
from .profile_cookies import *

router = APIRouter()
router.routes.extend(_profile_reads.router.routes)
router.routes.extend(_profile_comments.router.routes)
router.routes.extend(_profile_cookies.router.routes)

ROUTE_PREFIXES = ("/admin/socials/profiles/",)

def surface_routes(router: Any) -> list[RouteRecord]:
    return routes_matching(router, ROUTE_PREFIXES)
