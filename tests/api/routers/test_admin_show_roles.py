from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers.admin_show_roles import (
    _SYNC_SOURCE_KID,
    _SYNC_SOURCE_RELATIONSHIP,
    CastMatrixSyncRequest,
    _build_relationship_assignments,
    sync_cast_matrix_for_show,
)


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


def test_cast_matrix_sync_route_returns_service_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_roles._show_exists", return_value=True):
            with patch(
                "api.routers.admin_show_roles.sync_cast_matrix_for_show",
                return_value={
                    "show_id": show_id,
                    "source_urls": {
                        "wikipedia": "https://en.wikipedia.org/wiki/The_Real_Housewives_of_Salt_Lake_City",
                        "fandom": "https://real-housewives.fandom.com/wiki/The_Real_Housewives_of_Salt_Lake_City",
                    },
                    "counts": {
                        "season_role_assignments_upserted": 3,
                        "relationship_role_assignments_upserted": 1,
                        "global_kid_assignments_upserted": 2,
                        "auto_assignments_replaced": 4,
                        "bravo_links_upserted": 1,
                        "bravo_images_imported": 1,
                        "bravo_images_skipped": 0,
                    },
                    "unmatched": {
                        "cast_names": [],
                        "relationship_names": [],
                        "missing_season_evidence": [],
                    },
                },
            ):
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/cast-matrix/sync",
                    headers={"Authorization": f"Bearer {token}"},
                    json={
                        "season_numbers": [1, 2],
                        "include_relationship_roles": True,
                        "include_bravo_links": True,
                        "include_bravo_images": True,
                        "dry_run": False,
                    },
                )

    assert response.status_code == 200
    payload = response.json()
    assert payload["show_id"] == show_id
    assert payload["counts"]["season_role_assignments_upserted"] == 3


def test_sync_cast_matrix_for_show_replaces_auto_sources_and_preserves_manual_sources() -> None:
    show_id = str(uuid4())
    mock_db = MagicMock()
    mock_db.schema.return_value.table.return_value.upsert.return_value.execute.return_value = SimpleNamespace(
        data=[{"id": str(uuid4())}],
        error=None,
    )

    payload = CastMatrixSyncRequest(
        season_numbers=[1],
        include_relationship_roles=True,
        include_bravo_links=False,
        include_bravo_images=False,
        dry_run=False,
    )

    with patch(
        "api.routers.admin_show_roles._show_metadata",
        return_value={"id": show_id, "name": "The Real Housewives of Salt Lake City", "networks": ["bravo"]},
    ):
        with patch(
            "api.routers.admin_show_roles.try_fetch_html",
            side_effect=[("<html>wiki</html>", "https://en.wikipedia.org/wiki/x", None), (None, None, "404")],
        ):
            with patch(
                "api.routers.admin_show_roles.parse_wikipedia_cast_matrix_html",
                return_value={"Lisa Barlow": {1: "Housewife"}},
            ):
                with patch("api.routers.admin_show_roles.parse_fandom_cast_matrix_html", return_value={}):
                    with patch(
                        "api.routers.admin_show_roles.merge_cast_matrices",
                        return_value={"Lisa Barlow": {1: "Housewife"}},
                    ):
                        with patch(
                            "api.routers.admin_show_roles._load_show_cast_people",
                            return_value=[
                                {
                                    "person_id": str(uuid4()),
                                    "person_name": "Lisa Barlow",
                                    "full_name": "Lisa Barlow",
                                    "cast_member_name": "Lisa Barlow",
                                    "fandom_url": None,
                                },
                                {
                                    "person_id": str(uuid4()),
                                    "person_name": "John Barlow",
                                    "full_name": "John Barlow",
                                    "cast_member_name": "John Barlow",
                                    "fandom_url": None,
                                },
                                {
                                    "person_id": str(uuid4()),
                                    "person_name": "Jack Barlow",
                                    "full_name": "Jack Barlow",
                                    "cast_member_name": "Jack Barlow",
                                    "fandom_url": None,
                                },
                            ],
                        ):
                            with patch(
                                "api.routers.admin_show_roles._build_relationship_assignments",
                                return_value=(
                                    [
                                        {
                                            "person_id": "00000000-0000-0000-0000-000000000002",
                                            "season_number": 1,
                                            "role_name": "Husband",
                                            "source": _SYNC_SOURCE_RELATIONSHIP,
                                            "confidence": 0.9,
                                            "metadata": {},
                                        },
                                        {
                                            "person_id": "00000000-0000-0000-0000-000000000003",
                                            "season_number": 0,
                                            "role_name": "Kid",
                                            "source": _SYNC_SOURCE_KID,
                                            "confidence": 0.9,
                                            "metadata": {},
                                        },
                                    ],
                                    [],
                                    [],
                                ),
                            ):
                                with patch(
                                    "api.routers.admin_show_roles._ensure_canonical_roles",
                                    return_value={
                                        "housewife": "role-1",
                                        "husband": "role-2",
                                        "kid": "role-3",
                                    },
                                ):
                                    with patch(
                                        "api.routers.admin_show_roles._resolve_season_ids",
                                        return_value={1: "season-1"},
                                    ):
                                        with patch(
                                            "api.routers.admin_show_roles._count_replaceable_assignments",
                                            return_value=7,
                                        ):
                                            with patch(
                                                "api.routers.admin_show_roles._delete_replaceable_assignments",
                                                return_value=7,
                                            ) as delete_mock:
                                                result = sync_cast_matrix_for_show(
                                                    show_id=show_id,
                                                    payload=payload,
                                                    db=mock_db,
                                                    admin_user={"email": "admin@example.com"},
                                                )

    assert result["counts"]["season_role_assignments_upserted"] == 1
    assert result["counts"]["relationship_role_assignments_upserted"] == 1
    assert result["counts"]["global_kid_assignments_upserted"] == 1
    assert result["counts"]["auto_assignments_replaced"] == 7

    delete_sources = delete_mock.call_args.args[1]
    assert "manual" not in delete_sources
    assert set(delete_sources) == {"cast_matrix_sync", "cast_matrix_relationship_sync", "cast_matrix_kid_sync"}


def test_build_relationship_assignments_fetches_person_fandom_and_wikipedia_pages() -> None:
    cast_people = [
        {
            "person_id": "person-lisa",
            "person_name": "Lisa Barlow",
            "full_name": "Lisa Barlow",
            "cast_member_name": "Lisa Barlow",
            "fandom_url": None,
            "fandom_link_url": None,
            "wikipedia_url": None,
        }
    ]
    by_norm_name = {
        "lisabarlow": "person-lisa",
        "johnbarlow": "person-john",
        "jackbarlow": "person-jack",
    }

    def _mock_fetch(url: str) -> tuple[str | None, str | None, str | None]:
        if url == "https://real-housewives.fandom.com/wiki/Lisa_Barlow":
            return "<html>fandom</html>", url, None
        if url == "https://en.wikipedia.org/wiki/Lisa_Barlow":
            return "<html>wikipedia</html>", url, None
        return None, None, "404"

    with patch("api.routers.admin_show_roles.try_fetch_html", side_effect=_mock_fetch) as fetch_mock:
        with patch(
            "api.routers.admin_show_roles.extract_relationship_data_from_fandom_html",
            return_value={
                "season_partner_roles": [],
                "global_partner_roles": [{"name": "John Barlow", "role": "Husband"}],
                "kid_names": ["Jack Barlow"],
                "missing_season_evidence": [],
            },
        ):
            with patch(
                "api.routers.admin_show_roles.extract_relationship_data_from_wikipedia_html",
                return_value={
                    "season_partner_roles": [],
                    "global_partner_roles": [],
                    "kid_names": [],
                    "missing_season_evidence": [],
                },
            ) as wiki_parser_mock:
                assignments, unmatched, missing = _build_relationship_assignments(
                    show_name="The Real Housewives of Salt Lake City",
                    cast_people=cast_people,
                    by_norm_name=by_norm_name,
                    season_filter=set(),
                )

    fetched_urls = [str(call.args[0]) for call in fetch_mock.call_args_list]
    assert "https://real-housewives.fandom.com/wiki/Lisa_Barlow" in fetched_urls
    assert "https://en.wikipedia.org/wiki/Lisa_Barlow" in fetched_urls
    assert wiki_parser_mock.call_count == 1
    assert unmatched == []
    assert missing == []

    assert len(assignments) == 2
    relationship_assignment = next(row for row in assignments if row["source"] == _SYNC_SOURCE_RELATIONSHIP)
    kid_assignment = next(row for row in assignments if row["source"] == _SYNC_SOURCE_KID)
    assert relationship_assignment["person_id"] == "person-john"
    assert relationship_assignment["season_number"] == 0
    assert relationship_assignment["role_name"] == "Husband"
    assert relationship_assignment["metadata"]["source_url"] == "https://real-housewives.fandom.com/wiki/Lisa_Barlow"
    assert kid_assignment["person_id"] == "person-jack"
    assert kid_assignment["season_number"] == 0
