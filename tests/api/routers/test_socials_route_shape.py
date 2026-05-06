"""Route-shape guard for the admin socials router package."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from fastapi.routing import APIRoute

from api.routers import socials as socials_router
from api.routers.socials import (
    analytics,
    catalog,
    legacy_scrape,
    profiles,
    reddit,
    season_ingest,
    worker_health,
)
from api.routers.socials._surfaces import route_inventory

RouteShapeRecord = dict[str, str]

_ROUTE_SHAPE_FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "admin_socials_route_shape.json"


def _route_shape_sort_key(record: RouteShapeRecord) -> tuple[str, str, str]:
    return record["path"], record["method"], record["name"]


def _admin_socials_route_shape(router: Any) -> list[RouteShapeRecord]:
    records: list[RouteShapeRecord] = []
    for route in getattr(router, "routes", []) or []:
        if not isinstance(route, APIRoute):
            continue
        methods = sorted(
            method
            for method in (route.methods or [])
            if method not in {"HEAD", "OPTIONS"}
        )
        for method in methods:
            records.append(
                {
                    "method": method,
                    "path": route.path,
                    "name": str(route.name),
                }
            )
    return sorted(records, key=_route_shape_sort_key)


def _load_expected_route_shape() -> list[RouteShapeRecord]:
    payload = json.loads(_ROUTE_SHAPE_FIXTURE.read_text(encoding="utf-8"))
    assert isinstance(payload, list), "admin socials route-shape fixture must be a list"
    records: list[RouteShapeRecord] = []
    for record in payload:
        assert isinstance(record, dict), "admin socials route-shape fixture records must be objects"
        records.append(
            {
                "method": str(record["method"]),
                "path": str(record["path"]),
                "name": str(record["name"]),
            }
        )
    return sorted(records, key=_route_shape_sort_key)


def _assert_unique_method_path(records: list[RouteShapeRecord], *, source: str) -> None:
    keys = [(record["method"], record["path"]) for record in records]
    duplicates = sorted(key for key, count in Counter(keys).items() if count > 1)
    assert not duplicates, f"{source} contains duplicate admin socials method/path pairs: {duplicates}"


def _route_shape_drift_message(
    *,
    expected: list[RouteShapeRecord],
    actual: list[RouteShapeRecord],
) -> str:
    expected_by_route = {(record["method"], record["path"]): record for record in expected}
    actual_by_route = {(record["method"], record["path"]): record for record in actual}
    expected_keys = set(expected_by_route)
    actual_keys = set(actual_by_route)
    missing = [expected_by_route[key] for key in sorted(expected_keys - actual_keys)]
    added = [actual_by_route[key] for key in sorted(actual_keys - expected_keys)]
    name_drift = [
        {
            "method": method,
            "path": path,
            "expected_name": expected_by_route[(method, path)]["name"],
            "actual_name": actual_by_route[(method, path)]["name"],
        }
        for method, path in sorted(expected_keys & actual_keys)
        if expected_by_route[(method, path)]["name"] != actual_by_route[(method, path)]["name"]
    ]
    return json.dumps(
        {
            "expected_count": len(expected),
            "actual_count": len(actual),
            "missing_routes": missing,
            "added_routes": added,
            "route_name_drift": name_drift,
        },
        indent=2,
        sort_keys=True,
    )


def test_socials_router_is_package_import_with_stable_router() -> None:
    module_path = Path(socials_router.__file__)
    assert module_path.name == "__init__.py"
    assert module_path.parent.name == "socials"
    assert socials_router.router.prefix == "/admin/socials"


def test_socials_route_shape_matches_golden_snapshot() -> None:
    expected = _load_expected_route_shape()
    actual = _admin_socials_route_shape(socials_router.router)

    _assert_unique_method_path(expected, source="admin socials route-shape fixture")
    _assert_unique_method_path(actual, source="admin socials router")
    assert actual == expected, (
        "Admin socials route shape drifted. Deliberate path, method, or route-name changes "
        "must update tests/fixtures/admin_socials_route_shape.json in the same review.\n"
        f"{_route_shape_drift_message(expected=expected, actual=actual)}"
    )


def test_socials_route_inventory_contains_critical_public_routes() -> None:
    inventory = set(route_inventory(socials_router.router))
    expected = {
        ("POST", "/admin/socials/instagram/scrape"),
        ("GET", "/admin/socials/profiles/{platform}/{account_handle}/summary"),
        ("POST", "/admin/socials/profiles/{platform}/{account_handle}/comments/scrape"),
        ("POST", "/admin/socials/profiles/{platform}/{account_handle}/catalog/backfill"),
        ("GET", "/admin/socials/ingest/worker-health"),
        ("GET", "/admin/socials/ingest/queue-status"),
        ("POST", "/admin/socials/reddit/runs"),
        ("GET", "/admin/socials/seasons/{season_id}/analytics"),
        ("GET", "/admin/socials/seasons/{season_id}/analytics/export.csv"),
    }
    assert expected <= inventory


def test_socials_route_surface_modules_cover_expected_route_groups() -> None:
    router = socials_router.router
    surfaces = {
        "profiles": profiles.surface_routes(router),
        "catalog": catalog.surface_routes(router),
        "season_ingest": season_ingest.surface_routes(router),
        "worker_health": worker_health.surface_routes(router),
        "analytics": analytics.surface_routes(router),
        "reddit": reddit.surface_routes(router),
        "legacy_scrape": legacy_scrape.surface_routes(router),
    }

    assert any(path.endswith("/comments/scrape") for _method, path in surfaces["profiles"])
    assert any("/catalog/backfill" in path for _method, path in surfaces["catalog"])
    assert any("/sync-sessions" in path for _method, path in surfaces["season_ingest"])
    assert any(path.endswith("/ingest/queue-status") for _method, path in surfaces["worker_health"])
    assert any(path.endswith("/analytics/export.csv") for _method, path in surfaces["analytics"])
    assert any(path.endswith("/reddit/runs") for _method, path in surfaces["reddit"])
    assert any(path.endswith("/instagram/scrape") for _method, path in surfaces["legacy_scrape"])
