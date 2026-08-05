# ruff: noqa: F401, F403, F405, I001
"""Profile-scoped account catalog route aggregator."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from ._surfaces import RouteRecord, routes_matching
from . import catalog_reads as _catalog_reads
from . import catalog_operations as _catalog_operations
from . import catalog_backfill as _catalog_backfill
from .catalog_reads import *
from .catalog_operations import *
from .catalog_backfill import *

router = APIRouter()
router.routes.extend(_catalog_reads.router.routes)
router.routes.extend(_catalog_operations.router.routes)
router.routes.extend(_catalog_backfill.router.routes)

ROUTE_PREFIXES = ("/admin/socials/profiles/",)

REQUIRED_PATH_FRAGMENT = "/catalog/"


def surface_routes(router: Any) -> list[RouteRecord]:
    return [record for record in routes_matching(router, ROUTE_PREFIXES) if REQUIRED_PATH_FRAGMENT in record[1]]
