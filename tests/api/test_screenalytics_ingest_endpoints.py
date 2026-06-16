from __future__ import annotations

from api.main import app


def test_legacy_screenalytics_ingest_paths_are_not_registered() -> None:
    paths = {path for route in app.routes if (path := getattr(route, "path", None))}

    assert "/api/v1/screenalytics/episodes/{episode_id}/cast" not in paths
    assert "/api/v1/screenalytics/seasons/{season_id}/cast" not in paths
    assert "/api/v1/screenalytics/people/{person_id}/photos" not in paths


def test_openapi_excludes_legacy_screenalytics_ingest_paths() -> None:
    schema_paths = set(app.openapi()["paths"].keys())

    assert not any(path.startswith("/api/v1/screenalytics/") for path in schema_paths)
