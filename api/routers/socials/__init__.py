# ruff: noqa: F401
"""Registration-only admin-socials router package."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

from fastapi import APIRouter
from fastapi.routing import APIRoute

from . import _shared
from . import analytics as _analytics
from . import catalog as _catalog
from . import legacy_scrape as _legacy_scrape
from . import profiles as _profiles
from . import reddit as _reddit
from . import season_ingest as _season_ingest
from . import worker_health as _worker_health

router = APIRouter(prefix="/admin/socials", tags=["admin-socials"])


def _register_surface(surface: Any) -> None:
    """Materialize a surface's routes on the package router.

    FastAPI 0.140 keeps ``include_router`` relationships lazy.  This router is
    intentionally the concrete compatibility boundary consumed by the route
    inventory, so retain each child route's configuration while registering it
    directly under the shared prefix.
    """
    for route in surface.router.routes:
        if not isinstance(route, APIRoute):
            continue
        router.add_api_route(
            route.path,
            route.endpoint,
            response_model=route.response_model,
            status_code=route.status_code,
            tags=route.tags,
            dependencies=route.dependencies,
            summary=route.summary,
            description=route.description,
            response_description=route.response_description,
            responses=route.responses,
            deprecated=route.deprecated,
            methods=route.methods,
            operation_id=route.operation_id,
            response_model_include=route.response_model_include,
            response_model_exclude=route.response_model_exclude,
            response_model_by_alias=route.response_model_by_alias,
            response_model_exclude_unset=route.response_model_exclude_unset,
            response_model_exclude_defaults=route.response_model_exclude_defaults,
            response_model_exclude_none=route.response_model_exclude_none,
            include_in_schema=route.include_in_schema,
            response_class=route.response_class,
            name=route.name,
            callbacks=route.callbacks,
            openapi_extra=route.openapi_extra,
            generate_unique_id_function=route.generate_unique_id_function,
            strict_content_type=route.strict_content_type,
        )


for _surface in (
    _legacy_scrape,
    _profiles,
    _catalog,
    _season_ingest,
    _worker_health,
    _analytics,
    _reddit,
):
    _register_surface(_surface)

invalidate_week_detail_cache = _shared.invalidate_week_detail_cache
invalidate_week_summary_cache = _shared.invalidate_week_summary_cache

_SURFACES = (
    _analytics,
    _catalog,
    _legacy_scrape,
    _profiles,
    _reddit,
    _season_ingest,
    _worker_health,
)

_COMPATIBILITY_MODULES = tuple(
    dict.fromkeys(
        (
            _shared,
            *_SURFACES,
            *(
                module
                for surface in (_catalog, _legacy_scrape, _profiles, _season_ingest)
                for module in vars(surface).values()
                if isinstance(module, ModuleType) and module.__name__.startswith(f"{__name__}.")
            ),
        )
    )
)
_COMPATIBILITY_OWNERS = {
    name: tuple((module, module.__dict__[name]) for module in _COMPATIBILITY_MODULES if name in module.__dict__)
    for name in {name for module in _COMPATIBILITY_MODULES for name in module.__dict__}
}


class _SocialsRouterModule(ModuleType):
    """Mirror legacy package-level helper patches into their extracted owners."""

    def __setattr__(self, name: str, value: Any) -> None:
        if name != "router":
            for module, _original in _COMPATIBILITY_OWNERS.get(name, ()):
                module.__dict__[name] = value
        super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        super().__delattr__(name)
        for module, original in _COMPATIBILITY_OWNERS.get(name, ()):
            module.__dict__[name] = original


sys.modules[__name__].__class__ = _SocialsRouterModule


def __getattr__(name: str) -> Any:
    """Preserve package-level access to decomposed route helpers."""
    if hasattr(_shared, name):
        return getattr(_shared, name)
    for surface in _SURFACES:
        if hasattr(surface, name):
            return getattr(surface, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


async def get_season_analytics_week_live_health(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_week_live_health_snapshot

    return await _analytics.get_season_analytics_week_live_health(*args, **kwargs)


async def get_season_analytics(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_analytics

    return await _analytics.get_season_analytics(*args, **kwargs)


async def get_season_analytics_week_summary(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_week_detail_summary, get_week_detail_summary_fast

    return await _analytics.get_season_analytics_week_summary(*args, **kwargs)


async def get_season_analytics_week_detail(*args: Any, **kwargs: Any) -> dict[str, Any]:
    from trr_backend.socials.analytics import get_week_detail

    return await _analytics.get_season_analytics_week_detail(*args, **kwargs)
