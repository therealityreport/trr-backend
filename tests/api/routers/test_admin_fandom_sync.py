from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
from fastapi.testclient import TestClient

from api.main import app
from trr_backend.integrations.fandom_discovery import FandomCandidatePage


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


def test_preview_person_requires_auth() -> None:
    person_id = str(uuid4())
    mock_db = MagicMock()
    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        client = TestClient(app)
        response = client.post(f"/api/v1/admin/person/{person_id}/import-fandom/preview", json={})
    assert response.status_code == 401


def test_preview_person_returns_profile_payload(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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


def test_preview_season_returns_profile_payload(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    show_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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


def test_enqueue_page_directory_backfill_accepts_allowlisted_domain(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    mock_db = MagicMock()
    operation = {
        "id": str(uuid4()),
        "status": "pending",
        "attached": False,
        "community_domain": "real-housewives.fandom.com",
        "crawl_url": "https://real-housewives.fandom.com/wiki/Special:AllPages",
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_fandom_sync.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com",),
        ):
            with patch(
                "api.routers.admin_fandom_sync.fandom_page_directory_repo.enqueue_fandom_page_directory_backfill",
                return_value=operation,
            ) as enqueue_mock:
                client = TestClient(app)
                response = client.post(
                    "/api/v1/admin/fandom/page-directory/backfill",
                    json={
                        "community_domain": "real-housewives.fandom.com",
                        "review_allpages_url": "https://real-housewives.fandom.com/wiki/Special:AllPages",
                        "force": True,
                        "reason": "rollout_backfill",
                    },
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    assert response.json() == {
        "operation_id": operation["id"],
        "status": "pending",
        "attached": False,
        "community_domain": "real-housewives.fandom.com",
        "crawl_url": "https://real-housewives.fandom.com/wiki/Special:AllPages",
    }
    enqueue_mock.assert_called_once_with(
        community_domain="real-housewives.fandom.com",
        review_allpages_url="https://real-housewives.fandom.com/wiki/Special:AllPages",
        actor="service_role:unknown",
        reason="rollout_backfill",
        force=True,
    )


def test_enqueue_page_directory_backfill_rejects_non_allowlisted_domain(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_fandom_sync.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com",),
        ):
            with patch(
                "api.routers.admin_fandom_sync.fandom_page_directory_repo.enqueue_fandom_page_directory_backfill"
            ) as enqueue_mock:
                client = TestClient(app)
                response = client.post(
                    "/api/v1/admin/fandom/page-directory/backfill",
                    json={"community_domain": "realitytv-girl.fandom.com"},
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 400
    assert response.json()["detail"] == "Domain is not allowlisted: realitytv-girl.fandom.com"
    enqueue_mock.assert_not_called()


def test_enqueue_page_directory_backfill_returns_attached_operation(monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    mock_db = MagicMock()
    operation = {
        "id": str(uuid4()),
        "status": "running",
        "attached": True,
        "community_domain": "real-housewives.fandom.com",
        "crawl_url": "https://real-housewives.fandom.com/wiki/Special:AllPages",
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_fandom_sync.load_fandom_community_allowlist",
            return_value=("real-housewives.fandom.com",),
        ):
            with patch(
                "api.routers.admin_fandom_sync.fandom_page_directory_repo.enqueue_fandom_page_directory_backfill",
                return_value=operation,
            ):
                client = TestClient(app)
                response = client.post(
                    "/api/v1/admin/fandom/page-directory/backfill",
                    json={"community_domain": "real-housewives.fandom.com"},
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    assert response.json()["attached"] is True
    assert response.json()["operation_id"] == operation["id"]
