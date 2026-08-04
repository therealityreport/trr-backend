from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_networks_streaming_reads as v1_router
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.repositories import admin_networks_streaming_reads as repository
from trr_backend.services import networks_streaming_reads as service


def _summary_payload() -> dict[str, object]:
    return {
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
                "last_attempt_at": "2026-07-16T12:00:00Z",
                "has_logo": True,
                "has_bw_variants": True,
                "has_links": True,
            }
        ],
        "generated_at": "2026-07-16T12:00:00Z",
    }


def _detail_payload() -> dict[str, object]:
    return {
        "entity_type": "network",
        "entity_key": "bravo",
        "entity_slug": "bravo",
        "display_name": "Bravo",
        "available_show_count": 10,
        "added_show_count": 5,
        "core": {
            "entity_id": "74",
            "origin_country": "US",
            "display_priority": None,
            "tmdb_logo_path": "/bravo.png",
            "logo_path": None,
            "hosted_logo_key": "network/bravo.png",
            "hosted_logo_url": "https://cdn.example.com/bravo.png",
            "hosted_logo_black_url": "https://cdn.example.com/bravo-black.png",
            "hosted_logo_white_url": "https://cdn.example.com/bravo-white.png",
            "wikidata_id": "Q123",
            "wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
            "wikimedia_logo_file": "Bravo 2024.svg",
            "link_enriched_at": "2026-07-16T12:00:00Z",
            "link_enrichment_source": "wikidata",
            "facebook_id": "bravotv",
            "instagram_id": "bravotv",
            "twitter_id": "BravoTV",
            "tiktok_id": "bravotv",
        },
        "override": {
            "id": "override-1",
            "display_name_override": "Bravo",
            "wikidata_id_override": None,
            "wikipedia_url_override": None,
            "logo_source_urls_override": ["https://images.example.com/bravo.svg"],
            "source_priority_override": ["official", "wikimedia"],
            "aliases_override": ["Bravo TV"],
            "notes": None,
            "is_active": True,
            "updated_by": "signed-admin-uid",
            "updated_at": "2026-07-16T12:00:00Z",
        },
        "completion": {
            "resolution_status": "resolved",
            "resolution_reason": None,
            "last_attempt_at": "2026-07-16T12:00:00Z",
        },
        "logo_assets": [
            {
                "id": "logo-1",
                "source": "official",
                "source_url": "https://images.example.com/bravo.svg",
                "source_rank": 1,
                "hosted_logo_url": "https://cdn.example.com/bravo.png",
                "hosted_logo_content_type": "image/png",
                "base_logo_format": "svg",
                "pixel_width": 1024,
                "pixel_height": 400,
                "mirror_status": "mirrored",
                "failure_reason": None,
                "is_primary": True,
                "updated_at": "2026-07-16T12:00:00Z",
            }
        ],
        "shows": [
            {
                "trr_show_id": "00000000-0000-0000-0000-000000000001",
                "show_name": "Top Chef",
                "canonical_slug": "top-chef",
                "poster_url": "https://cdn.example.com/top-chef.jpg",
            }
        ],
        "family": {
            "id": "family-1",
            "family_key": "nbcuniversal",
            "display_name": "NBCUniversal",
            "owner_wikidata_id": "Q664498",
            "owner_label": "NBCUniversal",
            "is_active": True,
            "notes": None,
            "metadata": {"source": "manual"},
            "created_by": "signed-admin-uid",
            "updated_by": "signed-admin-uid",
            "created_at": "2026-07-15T12:00:00Z",
            "updated_at": "2026-07-16T12:00:00Z",
            "members": [
                {
                    "id": "member-1",
                    "family_id": "family-1",
                    "entity_type": "network",
                    "entity_key": "bravo",
                    "entity_display_name": "Bravo",
                    "source": "manual",
                    "confidence": 1.0,
                    "metadata": {},
                    "created_by": "signed-admin-uid",
                    "updated_by": "signed-admin-uid",
                    "created_at": "2026-07-15T12:00:00Z",
                    "updated_at": "2026-07-16T12:00:00Z",
                }
            ],
        },
        "family_suggestions": [
            {
                "owner_wikidata_id": "Q999",
                "owner_label": "Example Media",
                "entity_count": 2,
                "entities": [
                    {
                        "entity_type": "network",
                        "entity_key": "example-one",
                        "display_name": "Example One",
                        "updated_at": "2026-07-16T12:00:00Z",
                    },
                    {
                        "entity_type": "streaming",
                        "entity_key": "example-plus",
                        "display_name": "Example Plus",
                        "updated_at": "2026-07-16T12:00:00Z",
                    },
                ],
            }
        ],
        "shared_links": [
            {
                "id": "link-1",
                "family_id": "family-1",
                "link_group": "official",
                "link_kind": "homepage",
                "label": "NBCUniversal",
                "url": "https://www.nbcuniversal.com/",
                "url_key": "https://www.nbcuniversal.com/",
                "coverage_type": "family_all_shows",
                "coverage_value": None,
                "source": "manual",
                "priority": 10,
                "auto_apply": True,
                "is_active": True,
                "metadata": {},
                "created_at": "2026-07-15T12:00:00Z",
                "updated_at": "2026-07-16T12:00:00Z",
                "created_by": "signed-admin-uid",
                "updated_by": "signed-admin-uid",
            }
        ],
        "wikipedia_show_urls": [
            {
                "id": "wiki-1",
                "family_id": "family-1",
                "entity_type": "network",
                "entity_key": "bravo",
                "brand_wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
                "show_url": "https://en.wikipedia.org/wiki/Top_Chef",
                "show_url_key": "https://en.wikipedia.org/wiki/top_chef",
                "show_title": "Top Chef",
                "wikidata_id": "Q200076",
                "matched_show_id": "00000000-0000-0000-0000-000000000001",
                "match_method": "wikidata",
                "import_source": "wikipedia",
                "is_applied": True,
                "metadata": {},
                "last_seen_at": "2026-07-16T12:00:00Z",
                "created_at": "2026-07-15T12:00:00Z",
                "updated_at": "2026-07-16T12:00:00Z",
            }
        ],
    }


def _suggestions_payload() -> list[dict[str, object]]:
    return [
        {
            "entity_type": "network",
            "name": "Bravo",
            "entity_slug": "bravo",
            "available_show_count": 10,
            "added_show_count": 5,
        }
    ]


def _patch_detail_repositories(
    monkeypatch: pytest.MonkeyPatch,
    *,
    calls: dict[str, int],
) -> None:
    calls.update(
        {
            "detail": 0,
            "family_suggestions": 0,
            "family": 0,
            "shared_links": 0,
            "wikipedia_show_urls": 0,
        }
    )

    def fake_detail(**_kwargs):
        calls["detail"] += 1
        payload = _detail_payload()
        detail_only = {
            key: value
            for key, value in payload.items()
            if key
            not in {
                "family",
                "family_suggestions",
                "shared_links",
                "wikipedia_show_urls",
            }
        }
        return detail_only, 4

    def fake_family_suggestions():
        calls["family_suggestions"] += 1
        return {"rows": _detail_payload()["family_suggestions"]}

    def fake_family(**_kwargs):
        calls["family"] += 1
        return _detail_payload()["family"]

    def fake_shared_links(**_kwargs):
        calls["shared_links"] += 1
        return {"rows": _detail_payload()["shared_links"]}

    def fake_wikipedia_show_urls(**_kwargs):
        calls["wikipedia_show_urls"] += 1
        return {"rows": _detail_payload()["wikipedia_show_urls"]}

    monkeypatch.setattr(repository, "get_networks_streaming_detail", fake_detail)
    monkeypatch.setattr(
        repository,
        "get_networks_streaming_suggestions",
        lambda **_kwargs: (_suggestions_payload(), 1),
    )
    monkeypatch.setattr(
        service.brand_families,
        "list_family_suggestions",
        fake_family_suggestions,
    )
    monkeypatch.setattr(
        service.brand_families,
        "get_family_by_entity",
        fake_family,
    )
    monkeypatch.setattr(
        service.brand_families,
        "list_family_links",
        fake_shared_links,
    )
    monkeypatch.setattr(
        service.brand_families,
        "list_family_wikipedia_show_links",
        fake_wikipedia_show_urls,
    )


@pytest.fixture(autouse=True)
def override_admin_and_clear_cache():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "trr-app-internal-admin",
        "admin_uid": "signed-admin-uid",
        "role": "internal_admin",
    }
    service.invalidate_networks_streaming_summary_cache()
    v1_router.invalidate_networks_streaming_summary_cache()
    yield
    service.invalidate_networks_streaming_summary_cache()
    v1_router.invalidate_networks_streaming_summary_cache()
    app.dependency_overrides.pop(require_internal_admin, None)


def test_v1_and_v2_summary_share_cache_and_v1_invalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    def fake_summary():
        calls["count"] += 1
        return _summary_payload(), 2

    monkeypatch.setattr(repository, "get_networks_streaming_summary", fake_summary)
    client = TestClient(app)

    first = client.get("/api/v1/admin/shows/networks-streaming/summary")
    second = client.get("/api/v2/admin/networks-streaming/summary")
    invalidated = client.post("/api/v1/admin/shows/networks-streaming/summary/cache/invalidate")
    third = client.get("/api/v2/admin/networks-streaming/summary")

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json() == _summary_payload()
    assert invalidated.status_code == 200
    assert third.status_code == 200
    assert calls["count"] == 2


def test_v2_summary_response_drift_returns_safe_problem_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _summary_payload()
    payload["unexpected"] = True
    monkeypatch.setattr(
        service,
        "get_networks_streaming_summary",
        lambda: (payload, 2, "miss"),
    )

    response = TestClient(app).get("/api/v2/admin/networks-streaming/summary")

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "NETWORKS_STREAMING_SUMMARY_FAILED"
    assert "validation" not in response.text.lower()
    assert "unexpected" not in response.text.lower()


def test_v2_summary_rejects_coercible_response_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _summary_payload()
    payload["totals"] = {
        "total_available_shows": "18",
        "total_added_shows": True,
    }
    monkeypatch.setattr(
        service,
        "get_networks_streaming_summary",
        lambda: (payload, 2, "miss"),
    )

    response = TestClient(app).get("/api/v2/admin/networks-streaming/summary")

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "NETWORKS_STREAMING_SUMMARY_FAILED"
    assert "total_available_shows" not in response.text


def test_v2_summary_database_failure_returns_safe_problem_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable():
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(service, "get_networks_streaming_summary", unavailable)

    response = TestClient(app).get("/api/v2/admin/networks-streaming/summary")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["retryable"] is True
    assert "secret database topology" not in response.text


def test_v2_summary_requires_internal_admin_bearer() -> None:
    app.dependency_overrides.pop(require_internal_admin, None)

    response = TestClient(app).get("/api/v2/admin/networks-streaming/summary")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_v2_summary_openapi_contract_is_strict_and_signed() -> None:
    schema = app.openapi()
    operation = schema["paths"]["/api/v2/admin/networks-streaming/summary"]["get"]

    assert operation["operationId"] == "getAdminNetworksStreamingSummaryV2"
    assert operation["security"] == [{"InternalAdminBearer": []}]
    assert "422" not in operation["responses"]
    assert {"200", "500", "503"}.issubset(operation["responses"])

    summary_schema = schema["components"]["schemas"]["NetworkStreamingSummaryResponseV2"]
    totals_schema = schema["components"]["schemas"]["NetworkStreamingSummaryTotalsV2"]
    row_schema = schema["components"]["schemas"]["NetworkStreamingSummaryRowV2"]
    assert summary_schema["additionalProperties"] is False
    assert totals_schema["additionalProperties"] is False
    assert row_schema["additionalProperties"] is False
    assert row_schema["properties"]["type"]["enum"] == ["network", "streaming", "production"]
    assert row_schema["properties"]["available_show_count"]["minimum"] == 0
    assert row_schema["properties"]["added_show_count"]["minimum"] == 0


def test_v1_then_v2_detail_share_enriched_payload_and_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, int] = {}
    _patch_detail_repositories(monkeypatch, calls=calls)
    client = TestClient(app)

    v1_response = client.get(
        "/api/v1/admin/shows/networks-streaming/detail",
        params={"entity_type": "NETWORK", "entity_key": "  Bravo  "},
    )
    v2_response = client.get(
        "/api/v2/admin/networks-streaming/detail",
        params={"entity_type": "network", "entity_key": "bravo"},
    )

    assert v1_response.status_code == 200
    assert v2_response.status_code == 200
    assert v2_response.json() == v1_response.json()
    assert v2_response.json()["family"]["display_name"] == "NBCUniversal"
    assert v2_response.json()["shared_links"][0]["link_kind"] == "homepage"
    assert calls == {
        "detail": 1,
        "family_suggestions": 1,
        "family": 1,
        "shared_links": 1,
        "wikipedia_show_urls": 1,
    }


def test_v2_then_v1_detail_share_cache_and_v1_invalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, int] = {}
    _patch_detail_repositories(monkeypatch, calls=calls)
    client = TestClient(app)

    first = client.get(
        "/api/v2/admin/networks-streaming/detail",
        params={"entity_type": "network", "entity_slug": "BRAVO"},
    )
    second = client.get(
        "/api/v1/admin/shows/networks-streaming/detail",
        params={"entity_type": "network", "entity_slug": "bravo"},
    )
    invalidated = client.post("/api/v1/admin/shows/networks-streaming/summary/cache/invalidate")
    third = client.get(
        "/api/v2/admin/networks-streaming/detail",
        params={"entity_type": "network", "entity_slug": "bravo"},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert invalidated.status_code == 200
    assert third.status_code == 200
    assert calls == {
        "detail": 2,
        "family_suggestions": 2,
        "family": 2,
        "shared_links": 2,
        "wikipedia_show_urls": 2,
    }


@pytest.mark.parametrize(
    ("params", "error_code"),
    [
        ({"entity_type": "channel", "entity_key": "bravo"}, "INVALID_NETWORKS_STREAMING_ENTITY_TYPE"),
        ({"entity_type": "network"}, "NETWORKS_STREAMING_LOOKUP_REQUIRED"),
        (
            {"entity_type": "network", "entity_key": "x" * 241},
            "INVALID_NETWORKS_STREAMING_ENTITY_KEY",
        ),
        (
            {"entity_type": "network", "entity_slug": "not/a/slug"},
            "INVALID_NETWORKS_STREAMING_ENTITY_SLUG",
        ),
    ],
)
def test_v2_detail_returns_typed_400_instead_of_framework_422(
    params: dict[str, str],
    error_code: str,
) -> None:
    response = TestClient(app).get("/api/v2/admin/networks-streaming/detail", params=params)

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == error_code


def test_v2_detail_not_found_returns_strict_problem_suggestions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        repository,
        "get_networks_streaming_detail",
        lambda **_kwargs: (None, 1),
    )
    monkeypatch.setattr(
        repository,
        "get_networks_streaming_suggestions",
        lambda **_kwargs: (_suggestions_payload(), 1),
    )

    response = TestClient(app).get(
        "/api/v2/admin/networks-streaming/detail",
        params={"entity_type": "network", "entity_slug": "brva"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "NETWORKS_STREAMING_ENTITY_NOT_FOUND"
    assert response.json()["detail"]["retryable"] is False
    assert response.json()["detail"]["suggestions"] == _suggestions_payload()


def test_v2_detail_not_found_suggestion_drift_returns_safe_problem_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    suggestion = _suggestions_payload()[0]
    suggestion["internal_note"] = "must not escape"
    monkeypatch.setattr(
        repository,
        "get_networks_streaming_detail",
        lambda **_kwargs: (None, 1),
    )
    monkeypatch.setattr(
        repository,
        "get_networks_streaming_suggestions",
        lambda **_kwargs: ([suggestion], 1),
    )

    response = TestClient(app).get(
        "/api/v2/admin/networks-streaming/detail",
        params={"entity_type": "network", "entity_slug": "brva"},
    )

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "NETWORKS_STREAMING_DETAIL_FAILED"
    assert "internal_note" not in response.text
    assert "must not escape" not in response.text


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload["core"].__setitem__("unexpected", True),
        lambda payload: payload.__setitem__("available_show_count", "10"),
        lambda payload: payload["logo_assets"][0].__setitem__("mirror_status", "pending"),
    ],
)
def test_v2_detail_response_drift_returns_safe_problem_500(
    monkeypatch: pytest.MonkeyPatch,
    mutate,
) -> None:
    payload = _detail_payload()
    mutate(payload)
    monkeypatch.setattr(
        service,
        "get_networks_streaming_detail",
        lambda **_kwargs: (payload, 4, "miss"),
    )

    response = TestClient(app).get(
        "/api/v2/admin/networks-streaming/detail",
        params={"entity_type": "network", "entity_key": "bravo"},
    )

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "NETWORKS_STREAMING_DETAIL_FAILED"
    assert "unexpected" not in response.text
    assert "pending" not in response.text


def test_v2_detail_database_failure_returns_safe_problem_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(**_kwargs):
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(service, "get_networks_streaming_detail", unavailable)

    response = TestClient(app).get(
        "/api/v2/admin/networks-streaming/detail",
        params={"entity_type": "network", "entity_key": "bravo"},
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert response.json()["detail"]["retryable"] is True
    assert "secret database topology" not in response.text


def test_v2_detail_requires_internal_admin_bearer() -> None:
    app.dependency_overrides.pop(require_internal_admin, None)

    response = TestClient(app).get(
        "/api/v2/admin/networks-streaming/detail",
        params={"entity_type": "network", "entity_key": "bravo"},
    )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_v2_detail_openapi_contract_is_strict_signed_and_typed() -> None:
    schema = app.openapi()
    operation = schema["paths"]["/api/v2/admin/networks-streaming/detail"]["get"]

    assert operation["operationId"] == "getAdminNetworksStreamingDetailV2"
    assert operation["security"] == [{"InternalAdminBearer": []}]
    assert "422" not in operation["responses"]
    assert {"200", "400", "404", "500", "503"}.issubset(operation["responses"])
    parameters = {parameter["name"]: parameter for parameter in operation["parameters"]}
    assert parameters["entity_type"]["schema"]["enum"] == ["network", "streaming", "production"]
    assert parameters["entity_key"]["required"] is False
    assert parameters["entity_slug"]["required"] is False

    detail_schema = schema["components"]["schemas"]["NetworkStreamingDetailResponseV2"]
    core_schema = schema["components"]["schemas"]["NetworkStreamingCoreDetailV2"]
    family_schema = schema["components"]["schemas"]["NetworkStreamingFamilyV2"]
    logo_schema = schema["components"]["schemas"]["NetworkStreamingLogoAssetV2"]
    not_found_schema = schema["components"]["schemas"]["NetworkStreamingDetailNotFoundProblemDetailV2"]
    assert detail_schema["additionalProperties"] is False
    assert core_schema["additionalProperties"] is False
    assert family_schema["additionalProperties"] is False
    assert logo_schema["additionalProperties"] is False
    assert logo_schema["properties"]["mirror_status"]["enum"] == ["mirrored", "skipped", "failed"]
    assert not_found_schema["additionalProperties"] is False
    assert "suggestions" in not_found_schema["required"]
