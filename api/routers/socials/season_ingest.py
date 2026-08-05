# ruff: noqa: F401, F403, F405, I001
"""Season ingest, sync-session, shared-account ingest, and target route aggregator."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ._surfaces import RouteRecord, routes_matching
from . import season_sync as _season_sync
from . import shared_ingest as _shared_ingest
from . import season_runs as _season_runs
from .season_sync import *
from .shared_ingest import *
from .season_runs import *

router = APIRouter()
router.routes.extend(_season_sync.router.routes)
router.routes.extend(_shared_ingest.router.routes)
router.routes.extend(_season_runs.router.routes)

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
