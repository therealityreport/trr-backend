from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.integrations.fandom_discovery import FandomCandidatePage


def _make_admin_token(secret: str, subject: str = "admin-1", role: str = "service_role") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": role,
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def test_preview_person_requires_auth() -> None:
    person_id = str(uuid4())
    mock_db = MagicMock()
    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        client = TestClient(app)
        response = client.post(f"/api/v1/admin/person/{person_id}/import-fandom/preview", json={})
    assert response.status_code == 401


def test_preview_person_returns_profile_payload(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")

    mock_db = MagicMock()
    people_resp = MagicMock()
    people_resp.data = [{"id": person_id, "full_name": "Lisa Barlow"}]
    people_resp.error = None
    people_query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    people_query.execute.return_value = people_resp

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_fandom_sync.discover_fandom_candidate_pages",
            return_value=[
                FandomCandidatePage(
                    url="https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                    title="Lisa Barlow",
                    source="search",
                    domain="real-housewives.fandom.com",
                    score=12.0,
                )
            ],
        ):
            with patch(
                "api.routers.admin_fandom_sync.fetch_fandom_person_html",
                return_value=(
                    "<html><h1>Lisa Barlow</h1></html>",
                    "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                ),
            ):
                with patch(
                    "api.routers.admin_fandom_sync.parse_fandom_person_html",
                    return_value=(
                        {
                            "source": "fandom",
                            "source_url": "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                            "page_title": "Lisa Barlow",
                            "summary": "Sample",
                            "dynamic_sections": [{"title": "Biography", "paragraphs": ["Sample"]}],
                        },
                        [],
                    ),
                ):
                    client = TestClient(app)
                    response = client.post(
                        f"/api/v1/admin/person/{person_id}/import-fandom/preview",
                        json={},
                        headers={"Authorization": f"Bearer {token}"},
                    )

    assert response.status_code == 200
    data = response.json()
    assert "candidate_pages" in data
    assert "selected_pages" in data
    assert "profile" in data


def test_preview_person_forbidden_for_non_admin(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret", role="authenticated")

    mock_db = MagicMock()
    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        client = TestClient(app)
        response = client.post(
            f"/api/v1/admin/person/{person_id}/import-fandom/preview",
            json={},
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 403


def test_preview_person_openai_fallback_warning(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")

    mock_db = MagicMock()
    people_resp = MagicMock()
    people_resp.data = [{"id": person_id, "full_name": "Lisa Barlow"}]
    people_resp.error = None
    people_query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    people_query.execute.return_value = people_resp

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_fandom_sync.discover_fandom_candidate_pages",
            return_value=[
                FandomCandidatePage(
                    url="https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                    title="Lisa Barlow",
                    source="search",
                    domain="real-housewives.fandom.com",
                    score=12.0,
                )
            ],
        ):
            with patch(
                "api.routers.admin_fandom_sync.fetch_fandom_person_html",
                return_value=(
                    "<html><h1>Lisa Barlow</h1></html>",
                    "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                ),
            ):
                with patch(
                    "api.routers.admin_fandom_sync.parse_fandom_person_html",
                    return_value=(
                        {
                            "source": "fandom",
                            "source_url": "https://real-housewives.fandom.com/wiki/Lisa_Barlow",
                            "page_title": "Lisa Barlow",
                            "summary": "Sample",
                            "dynamic_sections": [{"title": "Biography", "paragraphs": ["Sample"]}],
                        },
                        [],
                    ),
                ):
                    with patch(
                        "api.routers.admin_fandom_sync.cleanup_fandom_payload_with_openai",
                        return_value=(None, None),
                    ):
                        client = TestClient(app)
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/import-fandom/preview",
                            json={},
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    data = response.json()
    warnings = data.get("warnings") or []
    assert any("OpenAI cleanup unavailable" in warning for warning in warnings)


def test_commit_person_uses_selected_page_urls_deterministically(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")

    selected_url = "https://real-housewives.fandom.com/wiki/Lisa_Barlow"
    other_url = "https://real-housewives.fandom.com/wiki/Heather_Gay"

    mock_db = MagicMock()
    people_resp = MagicMock()
    people_resp.data = [{"id": person_id, "full_name": "Lisa Barlow"}]
    people_resp.error = None
    people_query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    people_query.execute.return_value = people_resp

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_fandom_sync.discover_fandom_candidate_pages",
            return_value=[
                FandomCandidatePage(
                    url=other_url,
                    title="Heather Gay",
                    source="search",
                    domain="real-housewives.fandom.com",
                    score=16.0,
                ),
                FandomCandidatePage(
                    url=selected_url,
                    title="Lisa Barlow",
                    source="search",
                    domain="real-housewives.fandom.com",
                    score=12.0,
                ),
            ],
        ):
            with patch(
                "api.routers.admin_fandom_sync.fetch_fandom_person_html",
                return_value=("<html><h1>Lisa Barlow</h1></html>", selected_url),
            ) as fetch_html_mock:
                with patch(
                    "api.routers.admin_fandom_sync.parse_fandom_person_html",
                    return_value=(
                        {
                            "source": "fandom",
                            "source_url": selected_url,
                            "page_title": "Lisa Barlow",
                            "summary": "Sample",
                        },
                        [],
                    ),
                ):
                    with patch(
                        "api.routers.admin_fandom_sync.upsert_cast_fandom",
                        return_value={"id": str(uuid4()), "person_id": person_id, "source_url": selected_url},
                    ):
                        client = TestClient(app)
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/import-fandom/commit",
                            json={"selected_page_urls": [selected_url]},
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    data = response.json()
    selected_pages = data.get("selected_pages") or []
    assert len(selected_pages) == 1
    assert selected_pages[0]["url"] == selected_url
    fetch_html_mock.assert_called_once_with(selected_url)


def test_preview_person_skips_missing_page_with_warning(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")

    mock_db = MagicMock()
    people_resp = MagicMock()
    people_resp.data = [{"id": person_id, "full_name": "Lisa Barlow"}]
    people_resp.error = None
    people_query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    people_query.execute.return_value = people_resp

    page_url = "https://real-housewives.fandom.com/wiki/Lisa_Barlow"
    missing_html = "<html><title>Page Not Found</title><body>This page does not exist</body></html>"

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_fandom_sync.discover_fandom_candidate_pages",
            return_value=[
                FandomCandidatePage(
                    url=page_url,
                    title="Lisa Barlow",
                    source="search",
                    domain="real-housewives.fandom.com",
                    score=12.0,
                )
            ],
        ):
            with patch(
                "api.routers.admin_fandom_sync.fetch_fandom_person_html",
                return_value=(missing_html, page_url),
            ):
                with patch("api.routers.admin_fandom_sync.parse_fandom_person_html") as parse_mock:
                    client = TestClient(app)
                    response = client.post(
                        f"/api/v1/admin/person/{person_id}/import-fandom/preview",
                        json={},
                        headers={"Authorization": f"Bearer {token}"},
                    )

    assert response.status_code == 200
    data = response.json()
    warnings = data.get("warnings") or []
    assert any("Skipped missing page" in warning for warning in warnings)
    parse_mock.assert_not_called()


def test_preview_season_returns_profile_payload(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    show_id = str(uuid4())
    token = _make_admin_token("test-secret")

    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_fandom_sync._resolve_season_context",
            return_value={
                "show_id": show_id,
                "show_name": "The Real Housewives",
                "season_id": str(uuid4()),
                "season_number": 1,
                "season_title": "Season 1",
            },
        ):
            with patch(
                "api.routers.admin_fandom_sync.discover_fandom_candidate_pages",
                return_value=[
                    FandomCandidatePage(
                        url="https://real-housewives.fandom.com/wiki/Season_1",
                        title="Season 1",
                        source="search",
                        domain="real-housewives.fandom.com",
                        score=9.0,
                    )
                ],
            ):
                with patch(
                    "api.routers.admin_fandom_sync.fetch_fandom_person_html",
                    return_value=("<html><h1>Season 1</h1></html>", "https://real-housewives.fandom.com/wiki/Season_1"),
                ):
                    with patch(
                        "api.routers.admin_fandom_sync.parse_fandom_season_html",
                        return_value={
                            "source": "fandom",
                            "source_url": "https://real-housewives.fandom.com/wiki/Season_1",
                            "page_title": "Season 1",
                            "summary": "Season summary",
                            "dynamic_sections": [{"title": "Biography", "paragraphs": ["Season summary"]}],
                        },
                    ):
                        client = TestClient(app)
                        response = client.post(
                            f"/api/v1/admin/shows/{show_id}/seasons/1/import-fandom/preview",
                            json={},
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    data = response.json()
    assert "candidate_pages" in data
    assert "season_profile" in data
