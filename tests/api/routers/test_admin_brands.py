from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_list_brand_shows_franchises_returns_effective_rows(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    mock_db = MagicMock()
    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_brands._query_shows",
            return_value=[
                {
                    "id": "show-1",
                    "name": "The Real Housewives of Salt Lake City",
                    "canonical_slug": "the-real-housewives-of-salt-lake-city",
                    "networks": ["bravo"],
                },
                {
                    "id": "show-2",
                    "name": "Below Deck",
                    "canonical_slug": "below-deck",
                    "networks": ["bravo"],
                },
            ],
        ):
            with patch(
                "api.routers.admin_brands._fetch_show_fandom_links",
                return_value={
                    "show-1": [
                        {
                            "url": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                            "status": "approved",
                            "metadata": {},
                            "source": "manual",
                        }
                    ],
                    "show-2": [],
                },
            ):
                with patch(
                    "api.routers.admin_brands._load_effective_rules",
                    return_value={
                        "below-deck": {
                            "key": "below-deck",
                            "name": "Below Deck",
                            "primary_url": "https://below-deck.fandom.com/wiki/Below_Deck_Wiki",
                            "review_allpages_url": None,
                            "match_terms": ["below deck"],
                            "aliases": [],
                            "community_domains": ["below-deck.fandom.com"],
                            "include_allpages_scan": True,
                            "source_rank": 100,
                            "network_terms": [],
                            "is_active": True,
                            "rule_version": 1,
                        }
                    },
                ):
                    response = client.get(
                        "/api/v1/admin/brands/shows-franchises",
                        headers={"Authorization": f"Bearer {token}"},
                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    first = payload["rows"][0]
    second = payload["rows"][1]
    assert first["effective_source"] == "explicit"
    assert second["effective_source"] == "rule_default"
    assert second["effective_fandom_url"] == "https://below-deck.fandom.com/wiki/Below_Deck_Wiki"


def test_apply_brand_franchise_rule_dry_run_respects_missing_only(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    mock_db = MagicMock()
    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_brands._load_effective_rules",
            return_value={
                "below-deck": {
                    "key": "below-deck",
                    "name": "Below Deck",
                    "primary_url": "https://below-deck.fandom.com/wiki/Below_Deck_Wiki",
                    "review_allpages_url": None,
                    "match_terms": ["below deck"],
                    "aliases": [],
                    "community_domains": ["below-deck.fandom.com"],
                    "include_allpages_scan": True,
                    "source_rank": 100,
                    "network_terms": [],
                    "is_active": True,
                    "rule_version": 1,
                }
            },
        ):
            with patch(
                "api.routers.admin_brands._query_shows",
                return_value=[
                    {
                        "id": "show-1",
                        "name": "Below Deck Mediterranean",
                        "canonical_slug": "below-deck-mediterranean",
                        "networks": ["bravo"],
                    },
                    {
                        "id": "show-2",
                        "name": "Below Deck Sailing Yacht",
                        "canonical_slug": "below-deck-sailing-yacht",
                        "networks": ["bravo"],
                    },
                ],
            ):
                with patch(
                    "api.routers.admin_brands._fetch_show_fandom_links",
                    return_value={
                        "show-1": [
                            {
                                "url": "https://below-deck.fandom.com/wiki/Below_Deck_Mediterranean",
                                "status": "approved",
                                "metadata": {},
                                "source": "manual",
                            }
                        ],
                        "show-2": [],
                    },
                ):
                    with patch("api.routers.admin_brands._upsert_link") as upsert_link:
                        response = client.post(
                            "/api/v1/admin/brands/franchise-rules/below-deck/apply",
                            headers={"Authorization": f"Bearer {token}"},
                            json={"missing_only": True, "dry_run": True},
                        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["matched_show_count"] == 2
    assert payload["skipped_explicit"] == 1
    assert payload["applied_show_count"] == 1
    assert payload["links_upserted"] == 0
    upsert_link.assert_not_called()


def test_upsert_brand_franchise_rule_persists_definition_row(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=MagicMock()):
        with patch("api.routers.admin_brands._load_effective_rules", return_value={}):
            with patch(
                "api.routers.admin_brands.pg.execute_returning",
                side_effect=[
                    [],
                    [
                        {
                            "id": "rule-row-1",
                            "updated_at": "2026-02-25T00:00:00Z",
                            "created_at": "2026-02-25T00:00:00Z",
                        }
                    ],
                ],
            ):
                response = client.put(
                    "/api/v1/admin/brands/franchise-rules/survivor",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "name": "Survivor",
                        "primary_url": "https://survivor.fandom.com/wiki/Survivor_Wiki",
                        "match_terms": ["survivor"],
                        "include_allpages_scan": True,
                    },
                )

    assert response.status_code == 200
    payload = response.json()
    assert payload["key"] == "survivor"
    assert payload["primary_url"] == "https://survivor.fandom.com/wiki/Survivor_Wiki"
    assert payload["definition_row_id"] == "rule-row-1"
