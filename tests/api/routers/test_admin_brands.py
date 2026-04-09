"""Tests for admin brands shows/franchises endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers import admin_brands
from trr_backend.integrations.free_logo_sources import FreeLogoCandidate


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_admin_brands_endpoints_require_authentication(client: TestClient) -> None:
    endpoints = [
        ("GET", "/api/v1/admin/brands/shows-franchises"),
        ("GET", "/api/v1/admin/brands/franchise-rules"),
        ("GET", "/api/v1/admin/brands/families"),
        ("GET", "/api/v1/admin/brands/families/suggestions"),
        ("GET", "/api/v1/admin/brands/families/by-entity?entity_type=network&entity_key=bravo"),
        ("GET", "/api/v1/admin/brands/families/f1/links"),
        ("GET", "/api/v1/admin/brands/families/f1/wikipedia-show-urls"),
        ("GET", "/api/v1/admin/brands/logos?target_type=publication"),
        (
            "GET",
            "/api/v1/admin/brands/logos/options/sources?target_type=publication&target_key=instagram.com&logo_role=wordmark",
        ),
        (
            "GET",
            "/api/v1/admin/brands/logos/options/modal?target_type=publication&target_key=instagram.com",
        ),
        (
            "GET",
            "/api/v1/admin/brands/logos/options/source-suggestions"
            "?target_type=publication&target_key=instagram.com&logo_role=wordmark&source_provider=logos_fandom",
        ),
        ("POST", "/api/v1/admin/brands/logos/options/discover"),
        ("POST", "/api/v1/admin/brands/logos/options/source-query"),
        ("POST", "/api/v1/admin/brands/logos/options/select"),
        ("POST", "/api/v1/admin/brands/logos/options/assign"),
        ("DELETE", "/api/v1/admin/brands/logos/options/saved/asset-1?target_type=publication&target_key=instagram.com"),
        ("GET", "/api/v1/admin/brands/logo-targets?target_type=network"),
        ("POST", "/api/v1/admin/brands/logos/sync"),
        ("PUT", "/api/v1/admin/brands/franchise-rules/real-housewives"),
        ("POST", "/api/v1/admin/brands/franchise-rules/real-housewives/apply"),
        ("POST", "/api/v1/admin/brands/families"),
        ("PATCH", "/api/v1/admin/brands/families/f1"),
        ("POST", "/api/v1/admin/brands/families/f1/members"),
        ("DELETE", "/api/v1/admin/brands/families/f1/members/m1"),
        ("POST", "/api/v1/admin/brands/families/f1/links"),
        ("PATCH", "/api/v1/admin/brands/families/f1/links/r1"),
        ("POST", "/api/v1/admin/brands/families/f1/links/apply"),
        ("POST", "/api/v1/admin/brands/families/f1/wikipedia-import"),
    ]

    for method, path in endpoints:
        response = client.request(method, path, json={})
        assert response.status_code == 401


def test_get_shows_franchises_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "rows": [{"show_name": "The Traitors", "franchise_key": "traitors"}],
        "count": 1,
        "groups": [{"franchise_key": "traitors", "count": 1}],
    }

    with patch("trr_backend.repositories.brands_franchises.list_shows_franchises", return_value=expected) as mocked:
        response = client.get(
            "/api/v1/admin/brands/shows-franchises?q=traitors&limit=5",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {"q": "traitors", "limit": 5}


def test_get_franchise_rules_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "rules": [{"key": "real-housewives", "name": "Real Housewives"}],
        "suggested_franchises": ["real-housewives"],
    }

    with patch("trr_backend.repositories.brands_franchises.list_franchise_rules", return_value=expected):
        response = client.get(
            "/api/v1/admin/brands/franchise-rules",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected


def test_put_franchise_rule_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    payload = {
        "name": "Real Housewives",
        "primary_url": "https://real-housewives.fandom.com/wiki/Real_Housewives_Wiki",
        "source_rank": 10,
    }
    expected = {"rule": {"key": "real-housewives", **payload}}

    with patch("trr_backend.repositories.brands_franchises.update_franchise_rule", return_value=expected) as mocked:
        with patch(
            "api.routers.admin_brands.fandom_page_directory_repo.enqueue_fandom_page_directory_backfill"
        ) as enqueue_backfill:
            response = client.put(
                "/api/v1/admin/brands/franchise-rules/real-housewives",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["franchise_key"] == "real-housewives"
    assert mocked.call_args.kwargs["payload"] == payload
    assert mocked.call_args.kwargs["actor"] == "service_role:unknown"
    enqueue_backfill.assert_called_once()


def test_post_apply_franchise_rule_uses_safe_defaults(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {"franchise_key": "real-housewives", "dry_run": True, "missing_only": True}

    with patch("trr_backend.repositories.brands_franchises.apply_franchise_rule", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/brands/franchise-rules/real-housewives/apply",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["franchise_key"] == "real-housewives"
    assert mocked.call_args.kwargs["missing_only"] is True
    assert mocked.call_args.kwargs["dry_run"] is True
    assert mocked.call_args.kwargs["actor"] == "service_role:unknown"


def test_put_franchise_rule_maps_keyerror_to_404(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.brands_franchises.update_franchise_rule",
        side_effect=KeyError("Unknown franchise key"),
    ):
        response = client.put(
            "/api/v1/admin/brands/franchise-rules/unknown-key",
            headers={"Authorization": f"Bearer {token}"},
            json={"primary_url": "https://example.com/wiki"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown franchise key"


def test_put_franchise_rule_maps_valueerror_to_400(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.brands_franchises.update_franchise_rule",
        side_effect=ValueError("primary_url is required"),
    ):
        response = client.put(
            "/api/v1/admin/brands/franchise-rules/real-housewives",
            headers={"Authorization": f"Bearer {token}"},
            json={"primary_url": ""},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "primary_url is required"


def test_get_franchise_rules_maps_readiness_runtimeerror_to_503(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "trr_backend.repositories.brands_franchises.list_franchise_rules",
        side_effect=RuntimeError("Brands franchise rules table is unavailable. Run backend migrations."),
    ):
        response = client.get(
            "/api/v1/admin/brands/franchise-rules",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    assert "unavailable" in response.json()["detail"].lower()


def test_get_shows_franchises_maps_unhandled_error_to_500(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.brands_franchises.list_shows_franchises", side_effect=RuntimeError("boom")):
        response = client.get(
            "/api/v1/admin/brands/shows-franchises",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "boom"


def test_get_brand_logos_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "rows": [
            {
                "id": "logo-1",
                "target_type": "publication",
                "target_key": "deadline.com",
                "target_label": "deadline.com",
            }
        ],
        "count": 1,
    }

    with patch("api.routers.admin_brands._list_brand_logos", return_value=expected) as mocked:
        response = client.get(
            "/api/v1/admin/brands/logos?target_type=publication&q=deadline&limit=5&offset=0",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "target_type": "publication",
        "q": "deadline",
        "limit": 5,
        "offset": 0,
        "include_missing": False,
        "target_key": None,
        "logo_role": None,
        "source_provider": None,
        "include_related": False,
        "show_id": None,
    }


def test_get_brand_logo_option_sources_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "target_type": "publication",
        "target_key": "instagram.com",
        "logo_role": "wordmark",
        "sources": [
            {
                "source_provider": "wikimedia_commons",
                "total_count": 2,
                "has_more": False,
                "editable": True,
                "refreshable": True,
                "query_kind": "search_term",
                "default_query_value": "instagram",
                "effective_query_value": "instagram",
                "query_values": ["instagram", "instagram tv"],
                "query_links": ["https://commons.wikimedia.org/example"],
                "logo_role": "wordmark",
            }
        ],
    }
    with patch("api.routers.admin_brands._list_logo_option_sources", return_value=expected) as mocked:
        response = client.get(
            (
                "/api/v1/admin/brands/logos/options/sources"
                "?target_type=publication&target_key=instagram.com&target_label=Instagram&logo_role=wordmark&include_related=true"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "target_type": "publication",
        "target_key": "instagram.com",
        "target_label": "Instagram",
        "logo_role": "wordmark",
        "include_related": True,
    }


def test_get_brand_logo_option_modal_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "target_type": "publication",
        "target_key": "instagram.com",
        "saved_assets": [{"id": "asset-1", "source_provider": "saved"}],
        "featured_assets": {
            "wordmark": {"id": "asset-1", "selected_roles": ["wordmark"]},
            "icon": {"id": "asset-2", "selected_roles": ["icon"]},
        },
        "sources": [{"source_provider": "logos1000", "total_count": 2, "has_more": False}],
    }

    with patch("api.routers.admin_brands._list_combined_logo_modal_state", return_value=expected) as mocked:
        response = client.get(
            (
                "/api/v1/admin/brands/logos/options/modal"
                "?target_type=publication&target_key=instagram.com&target_label=Instagram&include_related=false"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "target_type": "publication",
        "target_key": "instagram.com",
        "target_label": "Instagram",
        "include_related": False,
    }


def test_get_brand_logos_include_related_survives_missing_related_variant_columns(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    base_rows = [
        {
            "id": "logo-1",
            "target_type": "publication",
            "target_key": "imdb.com",
            "target_label": "imdb.com",
            "source_url": "https://commons.wikimedia.org/wiki/Special:FilePath/IMDB_logo.svg",
            "source_page_url": None,
            "source_domain": "commons.wikimedia.org",
            "hosted_logo_url": "https://cdn.example.com/imdb.svg",
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "is_primary": True,
            "mirror_status": "mirrored",
            "failure_reason": None,
            "metadata": {"logo_role": "wordmark"},
            "logo_role": "wordmark",
            "source_provider": "wikimedia_commons",
            "discovered_from": "https://imdb.com/",
            "option_kind": "stored",
            "origin_target_type": "publication",
            "created_at": None,
            "updated_at": None,
        }
    ]
    admin_brands._brand_logo_assets_variant_columns.cache_clear()

    def _fake_fetch_all(_query: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        return base_rows

    with (
        patch("api.routers.admin_brands.pg.fetch_all", side_effect=_fake_fetch_all),
        patch(
            "api.routers.admin_brands._find_related_network_streaming_assets_by_host",
            side_effect=RuntimeError('column "hosted_logo_black_url" does not exist'),
        ),
    ):
        response = client.get(
            (
                "/api/v1/admin/brands/logos"
                "?target_type=publication&target_key=imdb.com&logo_role=wordmark&include_related=true&limit=20"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] >= 1
    assert payload["rows"][0]["target_key"] == "imdb.com"


def test_get_brand_logo_option_sources_include_related_survives_missing_related_variant_columns(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    base_rows = [
        {
            "id": "logo-1",
            "target_type": "publication",
            "target_key": "imdb.com",
            "target_label": "imdb.com",
            "source_url": "https://commons.wikimedia.org/wiki/Special:FilePath/IMDB_logo.svg",
            "source_page_url": None,
            "source_domain": "commons.wikimedia.org",
            "hosted_logo_url": "https://cdn.example.com/imdb.svg",
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "is_primary": True,
            "mirror_status": "mirrored",
            "failure_reason": None,
            "metadata": {"logo_role": "wordmark"},
            "logo_role": "wordmark",
            "source_provider": "wikimedia_commons",
            "discovered_from": "https://imdb.com/",
            "option_kind": "stored",
            "origin_target_type": "publication",
            "created_at": None,
            "updated_at": None,
        }
    ]
    admin_brands._brand_logo_assets_variant_columns.cache_clear()

    def _fake_fetch_all(_query: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        return base_rows

    with (
        patch("api.routers.admin_brands.pg.fetch_all", side_effect=_fake_fetch_all),
        patch(
            "api.routers.admin_brands._find_related_network_streaming_assets_by_host",
            side_effect=RuntimeError('column "hosted_logo_black_url" does not exist'),
        ),
    ):
        response = client.get(
            (
                "/api/v1/admin/brands/logos/options/sources"
                "?target_type=publication&target_key=imdb.com&logo_role=wordmark&include_related=true"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload.get("sources"), list)
    assert any(row.get("source_provider") == "wikimedia_commons" for row in payload["sources"])


def test_post_brand_logo_option_discover_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {"candidates": [], "total_count": 0, "next_offset": 0, "has_more": False}
    payload = {
        "target_type": "publication",
        "target_key": "instagram.com",
        "target_label": "instagram.com",
        "logo_role": "wordmark",
        "offset": 0,
        "limit": 10,
    }
    with patch("api.routers.admin_brands._discover_logo_candidates_by_source", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/brands/logos/options/discover",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )
    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.args[0].target_type == "publication"
    assert mocked.call_args.args[0].target_key == "instagram.com"


def test_previewable_logo_url_accepts_logopedia_revision_latest_urls() -> None:
    assert admin_brands._is_previewable_logo_url(
        "https://static.wikia.nocookie.net/logopedia/images/0/00/Bravo_%282005%29_%28Print%29.svg/revision/latest?cb=20250725204030"
    )
    assert (
        admin_brands._infer_logo_file_type(
            source_url=(
                "https://static.wikia.nocookie.net/logopedia/images/7/78/Bravo_2005_small.png/revision/latest?cb=20250725213343"
            ),
            content_type=None,
        )
        == "png"
    )


def test_discover_logo_candidates_by_source_includes_logopedia_revision_latest_candidates() -> None:
    payload = admin_brands.BrandLogosOptionDiscoverRequest(
        target_type="publication",
        target_key="bravotv.com",
        target_label="Bravo",
        logo_role="wordmark",
        source_provider="logos_fandom",
        query_overrides=["https://logos.fandom.com/wiki/Bravo_(United_States)/Other"],
        offset=0,
        limit=10,
        include_related=True,
    )
    candidate_url = (
        "https://static.wikia.nocookie.net/logopedia/images/0/00/Bravo_%282005%29_%28Print%29.svg"
        "/revision/latest?cb=20250725204030"
    )

    with (
        patch("api.routers.admin_brands._list_brand_logos", return_value={"rows": []}),
        patch(
            "api.routers.admin_brands.collect_free_logo_candidates",
            return_value=[
                FreeLogoCandidate(
                    url=candidate_url,
                    source_provider="logos_fandom",
                    discovered_from="https://logos.fandom.com/wiki/Bravo_(United_States)/Other",
                    context="search",
                )
            ],
        ),
        patch("api.routers.admin_brands.download_image", return_value=(b"<svg></svg>", "image/svg+xml")),
    ):
        result = admin_brands._discover_logo_candidates_by_source(payload)

    assert result["total_count"] == 1
    assert len(result["candidates"]) == 1
    candidate = result["candidates"][0]
    assert candidate["source_url"] == candidate_url
    assert candidate["source_provider"] == "logos_fandom"
    assert candidate["file_type"] == "svg"
    assert candidate["detected_logo_role"] == "wordmark"


def test_seed_logo_targets_from_entity_links_adds_fandom_publication_fallback() -> None:
    rows = [
        {
            "show_id": "show-1",
            "entity_type": "show",
            "entity_id": "entity-1",
            "season_number": None,
            "link_kind": "official",
            "label": "The Real Housewives Wiki",
            "url": "https://real-housewives.fandom.com/wiki/Home_Page",
        }
    ]

    with patch("api.routers.admin_brands.pg.fetch_all", return_value=rows):
        seeded = admin_brands._seed_logo_targets_from_entity_links(show_id="show-1")

    by_key = {(str(row.get("target_type")), str(row.get("target_key"))): row for row in seeded}
    assert ("publication", "real-housewives.fandom.com") in by_key
    assert ("publication", "fandom.com") in by_key

    fandom_row = by_key[("publication", "fandom.com")]
    assert fandom_row["target_label"] == "Fandom"
    assert fandom_row["discovered_from"] == "https://www.fandom.com/"
    assert fandom_row["discovered_from_urls"] == ["https://www.fandom.com/"]
    assert fandom_row["show_ids"] == ["show-1"]
    assert "fandom_fallback" in fandom_row["source_link_kinds"]


def test_detect_logo_role_uses_context_hint_for_fandom_icon_fallback() -> None:
    assert (
        admin_brands._detect_logo_role(
            candidate_url="https://www.fandom.com/brand/images/Logo_transparent_1.png",
            content_type=None,
            width=None,
            height=None,
            context_hint="fandom_brand_icon",
        )
        == "icon"
    )


def test_post_brand_logo_option_source_query_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "target_type": "publication",
        "target_key": "instagram.com",
        "logo_role": "wordmark",
        "source": {
            "source_provider": "logos1000",
            "effective_query_value": "instagram-logo",
            "query_values": ["instagram-logo", "instagram-icon"],
        },
    }
    payload = {
        "target_type": "publication",
        "target_key": "instagram.com",
        "target_label": "Instagram",
        "logo_role": "wordmark",
        "source_provider": "logos1000",
        "query_values": ["instagram-logo", "instagram-icon"],
    }

    with patch("api.routers.admin_brands._save_logo_source_query", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/brands/logos/options/source-query",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.args[0].source_provider == "logos1000"
    assert mocked.call_args.args[0].query_values == ["instagram-logo", "instagram-icon"]


def test_get_brand_logo_option_source_suggestions_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "target_type": "publication",
        "target_key": "imdb.com",
        "target_label": "IMDb",
        "logo_role": "wordmark",
        "source_provider": "logos_fandom",
        "current_query_values": ["IMDb", "IMDb/Special_Logos"],
        "suggestions": [
            {
                "query_value": "IMDb/Original",
                "query_link": "https://logos.fandom.com/wiki/IMDb/Original",
                "reason": "linked_from:IMDb",
                "discovered_from": "https://logos.fandom.com/wiki/IMDb",
            }
        ],
    }

    with patch("api.routers.admin_brands._list_logo_source_suggestions", return_value=expected) as mocked:
        response = client.get(
            (
                "/api/v1/admin/brands/logos/options/source-suggestions"
                "?target_type=publication&target_key=imdb.com&target_label=IMDb&logo_role=wordmark&source_provider=logos_fandom"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "target_type": "publication",
        "target_key": "imdb.com",
        "target_label": "IMDb",
        "logo_role": "wordmark",
        "source_provider": "logos_fandom",
    }


def test_post_brand_logo_option_select_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "selected": {"id": "asset-1", "logo_role": "wordmark"},
        "summary": {"wordmark": {"selected_asset_id": "asset-1"}},
    }
    payload = {
        "target_type": "publication",
        "target_key": "instagram.com",
        "target_label": "instagram.com",
        "logo_role": "wordmark",
        "asset_id": "asset-1",
        "set_featured": False,
    }
    with patch("api.routers.admin_brands._select_logo_option", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/brands/logos/options/select",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )
    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["payload"].target_type == "publication"
    assert mocked.call_args.kwargs["payload"].asset_id == "asset-1"
    assert mocked.call_args.kwargs["payload"].set_featured is False


def test_post_brand_logo_option_assign_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "selected": {"id": "asset-2", "logo_role": "icon"},
        "summary": {"icon": {"selected_asset_id": "asset-2"}},
    }
    payload = {
        "target_type": "publication",
        "target_key": "instagram.com",
        "target_label": "instagram.com",
        "logo_role": "icon",
        "asset_id": "asset-2",
    }
    with patch("api.routers.admin_brands._select_logo_option", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/brands/logos/options/assign",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    assert response.status_code == 200
    assert response.json() == expected
    forwarded_payload = mocked.call_args.kwargs["payload"]
    assert forwarded_payload.target_type == "publication"
    assert forwarded_payload.logo_role == "icon"
    assert forwarded_payload.asset_id == "asset-2"
    assert forwarded_payload.set_featured is True


def test_delete_brand_logo_saved_option_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "deleted_asset_id": "asset-1",
        "saved_assets": [{"id": "asset-2"}],
        "featured_assets": {"wordmark": {"id": "asset-2"}, "icon": None},
    }

    with (
        patch("api.routers.admin_brands.get_supabase_admin_client", return_value=object()),
        patch("api.routers.admin_brands._delete_saved_logo_option", return_value=expected) as mocked,
    ):
        response = client.delete(
            (
                "/api/v1/admin/brands/logos/options/saved/asset-1"
                "?target_type=publication&target_key=instagram.com&target_label=Instagram"
            ),
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["target_type"] == "publication"
    assert mocked.call_args.kwargs["target_key"] == "instagram.com"
    assert mocked.call_args.kwargs["target_label"] == "Instagram"
    assert mocked.call_args.kwargs["asset_id"] == "asset-1"


def test_get_brand_logo_targets_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "rows": [{"target_type": "network", "target_key": "1", "target_label": "Bravo"}],
        "count": 1,
    }

    with patch("api.routers.admin_brands._list_logo_targets", return_value=expected) as mocked:
        response = client.get(
            "/api/v1/admin/brands/logo-targets?target_type=network&q=bra&limit=10",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "target_type": "network",
        "q": "bra",
        "limit": 10,
        "show_id": None,
    }


def test_post_brand_logos_sync_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "scope": "page",
        "page": "news",
        "target_types": ["publication", "social"],
        "targets_scanned": 4,
        "imports_created": 2,
        "imports_updated": 0,
        "skipped": 1,
        "failed": 0,
        "unresolved": 1,
        "targets_with_wordmark": 2,
        "targets_with_icon": 1,
    }

    with patch("api.routers.admin_brands._sync_brand_logos", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/brands/logos/sync",
            headers={"Authorization": f"Bearer {token}"},
            json={"scope": "page", "page": "news", "only_missing": True},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["payload"].scope == "page"
    assert mocked.call_args.kwargs["payload"].page == "news"


def test_brand_family_endpoints_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.brand_families.list_families", return_value={"rows": [], "count": 0}):
        response = client.get(
            "/api/v1/admin/brands/families",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 200
    assert response.json() == {"rows": [], "count": 0}

    with patch(
        "trr_backend.repositories.brand_families.create_family",
        return_value={"id": "f1", "family_key": "nbcu", "display_name": "NBCU Family"},
    ):
        response = client.post(
            "/api/v1/admin/brands/families",
            headers={"Authorization": f"Bearer {token}"},
            json={"display_name": "NBCU Family"},
        )
    assert response.status_code == 200
    assert response.json()["id"] == "f1"

    with (
        patch(
            "trr_backend.repositories.brand_families.get_family_by_entity",
            return_value={"id": "f1", "display_name": "NBCU Family"},
        ),
        patch(
            "trr_backend.repositories.brand_families.list_family_suggestions",
            return_value={"rows": []},
        ),
        patch(
            "trr_backend.repositories.brand_families.list_family_links",
            return_value={"rows": []},
        ),
        patch(
            "trr_backend.repositories.brand_families.list_family_wikipedia_show_links",
            return_value={"rows": []},
        ),
    ):
        response = client.get(
            "/api/v1/admin/brands/families/by-entity?entity_type=network&entity_key=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 200
    assert response.json()["family"]["id"] == "f1"
