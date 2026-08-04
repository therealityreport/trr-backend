from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_networks_streaming_reads as router_module


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


@pytest.fixture(autouse=True)
def clear_cache():
    router_module.invalidate_networks_streaming_summary_cache()
    yield
    router_module.invalidate_networks_streaming_summary_cache()


def test_summary_returns_contract_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_summary():
        calls["count"] += 1
        return (
            {
                "totals": {
                    "total_available_shows": 18,
                    "total_added_shows": 7,
                },
                "rows": [
                    {
                        "type": "network",
                        "name": "Bravo",
                        "available_show_count": 8,
                        "added_show_count": 3,
                        "hosted_logo_url": "https://cdn.example.com/bravo.png",
                        "hosted_logo_black_url": "https://cdn.example.com/bravo-black.png",
                        "hosted_logo_white_url": "https://cdn.example.com/bravo-white.png",
                        "wikidata_id": "Q123",
                        "wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
                        "tmdb_entity_id": "74",
                        "homepage_url": "https://www.bravotv.com",
                        "resolution_status": "resolved",
                        "resolution_reason": None,
                        "last_attempt_at": "2026-03-26T00:00:00Z",
                        "has_logo": True,
                        "has_bw_variants": True,
                        "has_links": True,
                    }
                ],
                "generated_at": "2026-03-26T00:00:00Z",
            },
            2,
        )

    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_summary",
        fake_summary,
    )

    client = TestClient(app)
    first = client.get("/api/v1/admin/shows/networks-streaming/summary")
    second = client.get("/api/v1/admin/shows/networks-streaming/summary")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["totals"] == {
        "total_available_shows": 18,
        "total_added_shows": 7,
    }
    assert first.json()["rows"][0]["has_bw_variants"] is True
    assert calls["count"] == 1


def test_summary_collapses_concurrent_cold_misses(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_summary():
        calls["count"] += 1
        time.sleep(0.05)
        return (
            {
                "totals": {
                    "total_available_shows": 18,
                    "total_added_shows": 7,
                },
                "rows": [],
                "generated_at": "2026-03-26T00:00:00Z",
            },
            2,
        )

    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_summary",
        fake_summary,
    )

    client = TestClient(app)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(client.get, "/api/v1/admin/shows/networks-streaming/summary")
        second_future = executor.submit(client.get, "/api/v1/admin/shows/networks-streaming/summary")
        first = first_future.result()
        second = second_future.result()

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["totals"] == second.json()["totals"]
    assert calls["count"] == 1


def test_invalidate_summary_cache_clears_cached_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_summary():
        calls["count"] += 1
        return (
            {
                "totals": {
                    "total_available_shows": 0,
                    "total_added_shows": 0,
                },
                "rows": [],
                "generated_at": "2026-03-26T00:00:00Z",
            },
            2,
        )

    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_summary",
        fake_summary,
    )

    client = TestClient(app)
    first = client.get("/api/v1/admin/shows/networks-streaming/summary")
    second = client.get("/api/v1/admin/shows/networks-streaming/summary")
    invalidate = client.post("/api/v1/admin/shows/networks-streaming/summary/cache/invalidate")
    third = client.get("/api/v1/admin/shows/networks-streaming/summary")

    assert first.status_code == 200
    assert second.status_code == 200
    assert invalidate.status_code == 200
    assert third.status_code == 200
    assert calls["count"] == 2


def test_detail_returns_contract_with_family_enrichment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_detail",
        lambda **kwargs: (
            {
                "entity_type": "network",
                "entity_key": "bravo",
                "entity_slug": "bravo",
                "display_name": "Bravo",
                "available_show_count": 10,
                "added_show_count": 5,
                "core": {
                    "entity_id": "74",
                    "hosted_logo_url": "https://cdn.example.com/bravo.png",
                },
                "override": {
                    "id": None,
                    "logo_source_urls_override": [],
                    "source_priority_override": [],
                    "aliases_override": [],
                    "is_active": False,
                },
                "completion": {
                    "resolution_status": "resolved",
                    "resolution_reason": None,
                    "last_attempt_at": None,
                },
                "logo_assets": [],
                "shows": [],
            },
            3,
        ),
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_suggestions",
        lambda **kwargs: ([], 1),
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.brand_families,
        "list_family_suggestions",
        lambda: {"rows": [{"id": "family-1"}]},
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.brand_families,
        "get_family_by_entity",
        lambda **kwargs: {"id": "family-1", "display_name": "NBCUniversal"},
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.brand_families,
        "list_family_links",
        lambda **kwargs: {"rows": [{"id": "link-1"}]},
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.brand_families,
        "list_family_wikipedia_show_links",
        lambda **kwargs: {"rows": [{"id": "wiki-1"}]},
    )

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/shows/networks-streaming/detail",
        params={"entity_type": "network", "entity_key": "bravo"},
    )

    assert response.status_code == 200
    assert response.json()["family"]["display_name"] == "NBCUniversal"
    assert response.json()["shared_links"] == [{"id": "link-1"}]
    assert response.json()["wikipedia_show_urls"] == [{"id": "wiki-1"}]


def test_detail_not_found_returns_suggestions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_detail",
        lambda **kwargs: (None, 1),
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_suggestions",
        lambda **kwargs: (
            [
                {
                    "entity_type": "network",
                    "name": "Bravo",
                    "entity_slug": "bravo",
                    "available_show_count": 10,
                    "added_show_count": 5,
                }
            ],
            1,
        ),
    )

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/shows/networks-streaming/detail",
        params={"entity_type": "network", "entity_slug": "brva"},
    )

    assert response.status_code == 404
    assert response.json()["error"] == "not_found"
    assert response.json()["suggestions"][0]["entity_slug"] == "bravo"


def test_detail_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_detail",
        lambda **kwargs: (
            {
                "entity_type": "network",
                "entity_key": "bravo",
                "entity_slug": "bravo",
                "display_name": "Bravo",
                "available_show_count": 8,
                "added_show_count": 3,
                "core": {"entity_id": "77"},
                "override": {"id": None, "is_active": False},
                "completion": {
                    "resolution_status": "resolved",
                    "resolution_reason": None,
                    "last_attempt_at": None,
                },
                "logo_assets": [],
                "shows": [],
            },
            3,
        ),
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.brand_families,
        "list_family_suggestions",
        lambda: {"rows": [{"id": "family-1"}]},
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.brand_families,
        "get_family_by_entity",
        lambda **kwargs: {"id": "family-1", "name": "Bravo Family"},
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.brand_families,
        "list_family_links",
        lambda **kwargs: {"rows": [{"id": "link-1"}]},
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.brand_families,
        "list_family_wikipedia_show_links",
        lambda **kwargs: {"rows": [{"url": "https://en.wikipedia.org/wiki/Top_Chef"}]},
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/shows/networks-streaming/detail?entity_type=network&entity_slug=bravo")

    assert response.status_code == 200
    assert response.json()["family"]["name"] == "Bravo Family"
    assert response.json()["family_suggestions"] == [{"id": "family-1"}]
    assert response.json()["shared_links"] == [{"id": "link-1"}]
    assert response.json()["wikipedia_show_urls"] == [{"url": "https://en.wikipedia.org/wiki/Top_Chef"}]


def test_detail_returns_not_found_with_suggestions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_detail",
        lambda **kwargs: (None, 1),
    )
    monkeypatch.setattr(
        router_module.networks_streaming_reads_service.repository,
        "get_networks_streaming_suggestions",
        lambda **kwargs: (
            [
                {
                    "entity_type": "network",
                    "name": "Bravo",
                    "entity_slug": "bravo",
                    "available_show_count": 8,
                    "added_show_count": 3,
                }
            ],
            1,
        ),
    )

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/shows/networks-streaming/detail?entity_type=network&entity_slug=missing",
    )

    assert response.status_code == 404
    assert response.json() == {
        "error": "not_found",
        "suggestions": [
            {
                "entity_type": "network",
                "name": "Bravo",
                "entity_slug": "bravo",
                "available_show_count": 8,
                "added_show_count": 3,
            }
        ],
    }


@pytest.mark.parametrize(
    ("params", "detail"),
    [
        (
            {"entity_type": "channel", "entity_key": "bravo"},
            "entity_type must be network, streaming, or production",
        ),
        (
            {"entity_type": "network"},
            "entity_key or entity_slug is required",
        ),
    ],
)
def test_detail_validation_wire_shape_is_unchanged(
    params: dict[str, str],
    detail: str,
) -> None:
    response = TestClient(app).get(
        "/api/v1/admin/shows/networks-streaming/detail",
        params=params,
    )

    assert response.status_code == 400
    assert response.json() == {"detail": detail}
