from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers.admin_show_roles import (
    _SYNC_SOURCE_KID,
    _SYNC_SOURCE_RELATIONSHIP,
    CastMatrixSyncRequest,
    _build_relationship_assignments,
    list_cast_with_roles,
    sync_cast_matrix_for_show,
)


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "admin",
        "email": "admin@example.com",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def _make_internal_admin_token(
    secret: str,
    subject: str = "trr-app-internal-admin",
) -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "iss": "trr-app-internal",
        "aud": "trr-backend-internal-admin",
        "scope": "internal_admin",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_cast_matrix_sync_route_returns_service_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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


def test_list_show_roles_accepts_internal_admin_token_without_supabase_jwt(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    token = _make_internal_admin_token("internal-secret-32-bytes-minimum")
    show_id = str(uuid4())

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch(
            "api.routers.admin_show_roles.pg.fetch_all",
            return_value=[{"id": str(uuid4()), "name": "Housewife", "is_active": True, "sort_order": 0}],
        ):
            response = client.get(
                f"/api/v1/admin/shows/{show_id}/roles",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert response.json()[0]["name"] == "Housewife"


def test_list_cast_role_members_accepts_internal_admin_token_without_supabase_jwt(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    token = _make_internal_admin_token("internal-secret-32-bytes-minimum")
    show_id = str(uuid4())

    base_rows = [
        {
            "show_id": show_id,
            "person_id": "person-heather",
            "person_name": "Heather Gay",
            "total_episodes": 80,
            "archive_episodes": 0,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": ["Housewife"],
            "photo_url": "https://cdn.example/heather.jpg",
        }
    ]
    role_rows = [
        {
            "person_id": "person-heather",
            "role_names": ["Housewife"],
            "assignment_seasons": [1, 4],
        }
    ]

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch("api.routers.admin_show_roles.pg.fetch_all", side_effect=[base_rows, role_rows]):
            response = client.get(
                f"/api/v1/admin/shows/{show_id}/cast-role-members",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    assert response.json()[0]["person_name"] == "Heather Gay"


def test_list_cast_role_members_keeps_people_without_active_role_assignments(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    token = _make_internal_admin_token("internal-secret-32-bytes-minimum")
    show_id = str(uuid4())

    base_rows = [
        {
            "show_id": show_id,
            "person_id": "person-heather",
            "person_name": "Heather Gay",
            "total_episodes": 80,
            "archive_episodes": 0,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": ["Housewife"],
            "photo_url": "https://cdn.example/heather.jpg",
        },
        {
            "show_id": show_id,
            "person_id": "person-andy",
            "person_name": "Andy Cohen",
            "total_episodes": 31,
            "archive_episodes": 0,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": [],
            "photo_url": "https://cdn.example/andy.jpg",
        },
    ]
    role_rows = [
        {
            "person_id": "person-heather",
            "role_names": ["Housewife"],
            "assignment_seasons": [1, 4],
        }
    ]

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch("api.routers.admin_show_roles.pg.fetch_all", side_effect=[base_rows, role_rows]):
            response = client.get(
                f"/api/v1/admin/shows/{show_id}/cast-role-members",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    payload = response.json()
    assert [row["person_name"] for row in payload] == ["Heather Gay", "Andy Cohen"]
    assert payload[0]["roles"] == ["Housewife"]
    assert payload[1]["roles"] == []


def test_list_cast_role_members_falls_back_to_base_roles_when_no_curated_assignments_exist(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    token = _make_internal_admin_token("internal-secret-32-bytes-minimum")
    show_id = str(uuid4())

    base_rows = [
        {
            "show_id": show_id,
            "person_id": "person-heather",
            "person_name": "Heather Gay",
            "total_episodes": 80,
            "archive_episodes": 0,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": ["Housewife"],
            "photo_url": "https://cdn.example/heather.jpg",
        },
        {
            "show_id": show_id,
            "person_id": "person-meredith",
            "person_name": "Meredith Marks",
            "total_episodes": 75,
            "archive_episodes": 0,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": ["Housewife"],
            "photo_url": "https://cdn.example/meredith.jpg",
        },
    ]

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch("api.routers.admin_show_roles.pg.fetch_all", side_effect=[base_rows, []]):
            response = client.get(
                f"/api/v1/admin/shows/{show_id}/cast-role-members",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert response.status_code == 200
    payload = response.json()
    assert [row["person_name"] for row in payload] == ["Heather Gay", "Meredith Marks"]
    assert payload[0]["roles"] == ["Housewife"]


def test_show_role_routes_reject_invalid_internal_admin_token_when_supabase_jwt_unavailable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")

    response = client.get(
        f"/api/v1/admin/shows/{uuid4()}/roles",
        headers={"Authorization": "Bearer not-a-valid-internal-token"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Allowlist admin access required"


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
                                                with patch(
                                                    "api.routers.admin_show_roles._load_existing_bravo_profile_links",
                                                    return_value={},
                                                ):
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

    with patch(
        "api.routers.admin_show_links._validate_person_knowledge_url",
        side_effect=lambda url, kind, expected_name=None: (url, "valid"),
    ):
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


def test_build_relationship_assignments_skips_missing_person_pages() -> None:
    cast_people = [
        {
            "person_id": "person-georgia",
            "person_name": "Georgia Gay",
            "full_name": "Georgia Gay",
            "cast_member_name": "Georgia Gay",
            "fandom_url": "https://real-housewives.fandom.com/wiki/Georgia_Gay",
            "fandom_link_url": None,
            "wikipedia_url": "https://en.wikipedia.org/wiki/Georgia_Gay",
        }
    ]
    by_norm_name = {"georgiagay": "person-georgia"}

    with patch(
        "api.routers.admin_show_links._validate_person_knowledge_url",
        return_value=(None, "invalid"),
    ):
        with patch("api.routers.admin_show_roles.extract_relationship_data_from_fandom_html") as fandom_parser:
            with patch("api.routers.admin_show_roles.extract_relationship_data_from_wikipedia_html") as wiki_parser:
                assignments, unmatched, missing = _build_relationship_assignments(
                    show_name="The Real Housewives of Salt Lake City",
                    cast_people=cast_people,
                    by_norm_name=by_norm_name,
                    season_filter=set(),
                )

    assert assignments == []
    assert unmatched == []
    assert missing == []
    assert fandom_parser.call_count == 0
    assert wiki_parser.call_count == 0


def test_list_cast_with_roles_scopes_total_episodes_to_selected_seasons() -> None:
    show_id = str(uuid4())

    rows = [
        {
            "show_id": show_id,
            "person_id": "person-heather",
            "person_name": "Heather Gay",
            "total_episodes": 80,
            "archive_episodes": 0,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": ["Housewife"],
            "photo_url": "https://cdn.example/heather.jpg",
        }
    ]
    role_rows = [
        {
            "person_id": "person-heather",
            "role_names": ["Housewife"],
            "assignment_seasons": [1, 4],
        }
    ]
    scoped_episode_rows = [{"person_id": "person-heather", "total_episodes": 12}]

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch(
            "api.routers.admin_show_roles.pg.fetch_all",
            side_effect=[rows, role_rows, scoped_episode_rows],
        ):
            payload = list_cast_with_roles(
                UUID(show_id),
                {},
                sort_by="episodes",
                order="desc",
                seasons="4",
                roles=None,
                has_image=None,
                archive_mode="all",
            )

    assert len(payload) == 1
    assert payload[0]["person_id"] == "person-heather"
    assert payload[0]["total_episodes"] == 12
    assert payload[0]["season_numbers"] == [1, 2, 3, 4]


def test_list_cast_with_roles_derives_latest_season_from_role_assignments() -> None:
    show_id = str(uuid4())

    rows = [
        {
            "show_id": show_id,
            "person_id": "person-mary",
            "person_name": "Mary Cosby",
            "total_episodes": 16,
            "archive_episodes": 0,
            "seasons_appeared": 1,
            "season_numbers": [1],
            "latest_season": 1,
            "roles": ["Housewife"],
            "photo_url": "https://cdn.example/mary.jpg",
        }
    ]
    role_rows = [
        {
            "person_id": "person-mary",
            "role_names": ["Housewife", "Friend", "Guest"],
            "assignment_seasons": [0, 1, 3],
        }
    ]

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch("api.routers.admin_show_roles.pg.fetch_all", side_effect=[rows, role_rows]):
            payload = list_cast_with_roles(
                UUID(show_id),
                {},
                sort_by="season",
                order="desc",
                seasons=None,
                roles=None,
                has_image=None,
                archive_mode="all",
            )

    assert len(payload) == 1
    assert payload[0]["person_id"] == "person-mary"
    assert payload[0]["total_episodes"] == 16
    assert payload[0]["latest_season"] == 3
    assert payload[0]["seasons_appeared"] == 2
    assert payload[0]["season_numbers"] == [1, 3]
    assert sorted(payload[0]["roles"]) == ["Friend", "Guest", "Housewife"]


def test_list_cast_with_roles_keeps_unassigned_people_with_empty_roles() -> None:
    show_id = str(uuid4())

    rows = [
        {
            "show_id": show_id,
            "person_id": "person-heather",
            "person_name": "Heather Gay",
            "total_episodes": 80,
            "archive_episodes": 0,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": ["Housewife"],
            "photo_url": "https://cdn.example/heather.jpg",
        },
        {
            "show_id": show_id,
            "person_id": "person-andy",
            "person_name": "Andy Cohen",
            "total_episodes": 31,
            "archive_episodes": 0,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": [],
            "photo_url": "https://cdn.example/andy.jpg",
        },
    ]
    role_rows = [
        {
            "person_id": "person-heather",
            "role_names": ["Housewife"],
            "assignment_seasons": [1, 2, 3, 4],
        }
    ]

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch("api.routers.admin_show_roles.pg.fetch_all", side_effect=[rows, role_rows]):
            payload = list_cast_with_roles(
                UUID(show_id),
                {},
                sort_by="episodes",
                order="desc",
                seasons=None,
                roles=None,
                has_image=None,
                archive_mode="all",
            )

    assert [row["person_name"] for row in payload] == ["Heather Gay", "Andy Cohen"]
    assert payload[0]["roles"] == ["Housewife"]
    assert payload[1]["roles"] == []


def test_list_cast_with_roles_emits_perf_logs_when_enabled() -> None:
    show_id = str(uuid4())

    rows = [
        {
            "show_id": show_id,
            "person_id": "person-lisa",
            "person_name": "Lisa Barlow",
            "total_episodes": 45,
            "archive_episodes": 1,
            "seasons_appeared": 4,
            "season_numbers": [1, 2, 3, 4],
            "latest_season": 4,
            "roles": ["Housewife"],
            "photo_url": "https://cdn.example/lisa.jpg",
        }
    ]
    role_rows = [
        {
            "person_id": "person-lisa",
            "role_names": ["Housewife"],
            "assignment_seasons": [1, 2, 3, 4],
        }
    ]

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch("api.routers.admin_show_roles.CAST_ROLE_MEMBERS_PERF_LOGS_ENABLED", True):
            with patch("api.routers.admin_show_roles.pg.fetch_all", side_effect=[rows, role_rows]):
                with patch("api.routers.admin_show_roles.logger.info") as logger_info:
                    payload = list_cast_with_roles(
                        UUID(show_id),
                        {},
                        sort_by="episodes",
                        order="desc",
                        seasons=None,
                        roles=None,
                        has_image=None,
                        archive_mode="all",
                    )

    assert len(payload) == 1
    assert payload[0]["person_id"] == "person-lisa"
    assert logger_info.call_count == 1
    assert "cast-role-members timings" in str(logger_info.call_args.args[0])


def test_list_cast_with_roles_excludes_zero_episode_members_after_scoping() -> None:
    show_id = str(uuid4())

    rows = [
        {
            "show_id": show_id,
            "person_id": "person-friend",
            "person_name": "Friend Of",
            "total_episodes": 20,
            "archive_episodes": 0,
            "seasons_appeared": 3,
            "season_numbers": [4, 5, 6],
            "latest_season": 6,
            "roles": ["Friend"],
            "photo_url": "https://cdn.example/friend.jpg",
        }
    ]
    role_rows = [
        {
            "person_id": "person-friend",
            "role_names": ["Friend"],
            "assignment_seasons": [6],
        }
    ]
    scoped_episode_rows = [{"person_id": "person-friend", "total_episodes": 0}]

    with patch("api.routers.admin_show_roles._show_exists", return_value=True):
        with patch(
            "api.routers.admin_show_roles.pg.fetch_all",
            side_effect=[rows, role_rows, scoped_episode_rows],
        ):
            payload = list_cast_with_roles(
                UUID(show_id),
                {},
                sort_by="episodes",
                order="desc",
                seasons="6",
                roles=None,
                has_image=None,
                exclude_zero_episode_members=True,
                archive_mode="all",
            )

    assert payload == []
