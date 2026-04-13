from __future__ import annotations

from api.main import app


def test_legacy_screenalytics_v2_paths_are_not_registered() -> None:
    paths = {route.path for route in app.routes}

    assert not any(path.startswith("/api/v1/screenalytics/v2") for path in paths)


def test_openapi_excludes_legacy_screenalytics_v2_paths() -> None:
    schema_paths = set(app.openapi()["paths"].keys())

    assert not any(path.startswith("/api/v1/screenalytics/v2") for path in schema_paths)
