"""Tests for admin person images refresh endpoint."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers import admin_person_images


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    """Create a valid admin JWT token."""
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def _make_allowlist_user_token(secret: str, email: str, subject: str = "user-1") -> str:
    """Create a valid user JWT token with allowlist-able email."""
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "email": email,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "authenticated",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def _mock_media_link_lookup(mock_db: MagicMock, row: dict | None, *, error: object | None = None) -> None:
    """Mock media_links lookup query for facebank seed endpoint tests."""
    mock_response = MagicMock()
    mock_response.data = [row] if row is not None else []
    mock_response.error = error
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response


@pytest.fixture
def client():
    return TestClient(app)


def test_names_match_requires_first_and_last_name_alignment() -> None:
    assert admin_person_images._names_match("Henry Barlow", "Henry Barlow")
    assert admin_person_images._names_match("Wendy Osefo", "Dr. Wendy Osefo")
    assert not admin_person_images._names_match("Henry Barlow", "Lisa Barlow")
    assert not admin_person_images._names_match("Henry Barlow", "John Barlow")


def test_fandom_profile_match_rejects_mismatched_page_owner() -> None:
    cast_fandom = {
        "full_name": "John Barlow",
        "page_title": "John Barlow",
    }
    assert not admin_person_images._fandom_profile_matches_person_name(
        "John Barlow",
        cast_fandom,
        page_url="https://real-housewives.fandom.com/wiki/Lisa_Barlow",
    )
    assert admin_person_images._fandom_profile_matches_person_name(
        "John Barlow",
        cast_fandom,
        page_url="https://real-housewives.fandom.com/wiki/John_Barlow",
    )


def test_resolve_imdb_focus_filters_uses_show_name_fallback_from_show_id(monkeypatch) -> None:
    mock_db = MagicMock()
    monkeypatch.setattr(admin_person_images, "_get_show_name", lambda db, show_id: "The Traitors")
    monkeypatch.setattr(
        admin_person_images,
        "_load_show_imdb_title_ids",
        lambda db, show_id: {"tt123", "tt456"},
    )

    title_ids, keywords, prioritize_solo = admin_person_images._resolve_imdb_focus_filters(
        mock_db,
        show_id=uuid4(),
        show_name=None,
    )

    assert title_ids == {"tt123", "tt456"}
    assert keywords == ["the traitors", "traitors"]
    assert prioritize_solo is True


def test_resolve_imdb_focus_filters_returns_empty_for_non_target_show(monkeypatch) -> None:
    mock_db = MagicMock()
    monkeypatch.setattr(admin_person_images, "_get_show_name", lambda db, show_id: "Top Chef")

    title_ids, keywords, prioritize_solo = admin_person_images._resolve_imdb_focus_filters(
        mock_db,
        show_id=uuid4(),
        show_name=None,
    )

    assert title_ids == set()
    assert keywords == []
    assert prioritize_solo is False


def test_resolve_imdb_traitors_strict_context_enabled_for_traitors_show(monkeypatch) -> None:
    mock_db = MagicMock()
    show_id = uuid4()
    show_row = {"id": str(show_id), "name": "The Traitors"}
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {"the traitors": show_row}, {}))
    monkeypatch.setattr(admin_person_images, "_find_show_row_by_alias", lambda by_alias, alias: show_row)
    monkeypatch.setattr(
        admin_person_images,
        "_load_show_cast_identity_sets",
        lambda db, resolved_show_id: ({"nmcast01"}, {"Traitors Cast One"}),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_load_show_episode_imdb_ids",
        lambda db, resolved_show_id: {"ttepisode01"},
    )

    context = admin_person_images._resolve_imdb_traitors_strict_context(
        mock_db,
        show_id=None,
        show_name="The Traitors",
        target_person_imdb_id="nm0001086",
        target_person_name="Alan Cumming",
    )

    assert context["strict_mode_enabled"] is True
    assert context["strict_types"] == {"event", "still_frame"}
    assert context["resolved_show_id"] == str(show_id)
    assert context["allowed_cast_imdb_ids"] == {"nmcast01", "nm0001086"}
    assert context["allowed_cast_names"] == {"Traitors Cast One", "Alan Cumming"}
    assert context["allowed_episode_imdb_ids"] == {"ttepisode01"}


def test_resolve_imdb_traitors_strict_context_disabled_for_non_traitors(monkeypatch) -> None:
    mock_db = MagicMock()
    monkeypatch.setattr(
        admin_person_images,
        "_load_show_cast_identity_sets",
        lambda db, resolved_show_id: (_ for _ in ()).throw(AssertionError("should not load cast")),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_load_show_episode_imdb_ids",
        lambda db, resolved_show_id: (_ for _ in ()).throw(AssertionError("should not load episodes")),
    )

    context = admin_person_images._resolve_imdb_traitors_strict_context(
        mock_db,
        show_id=None,
        show_name="Top Chef",
        target_person_imdb_id="nm0001086",
        target_person_name="Alan Cumming",
    )

    assert context["strict_mode_enabled"] is False
    assert context["strict_types"] == set()
    assert context["allowed_episode_imdb_ids"] == set()
    assert context["allowed_cast_imdb_ids"] == {"nm0001086"}
    assert context["allowed_cast_names"] == {"Alan Cumming"}


def test_enrich_cast_photos_with_episode_metadata_falls_back_to_imdb_title_metadata(monkeypatch) -> None:
    photos = [
        {
            "source": "imdb",
            "title_imdb_ids": ["tt35051926"],
            "title_names": ["Reunion Part 3"],
            "metadata": {},
        }
    ]

    mock_db = MagicMock()
    episodes_response = MagicMock()
    episodes_response.error = None
    episodes_response.data = []
    episodes_query = mock_db.schema.return_value.table.return_value.select.return_value.in_.return_value
    episodes_query.execute.return_value = episodes_response

    monkeypatch.setattr(
        admin_person_images,
        "_fetch_imdb_title_fallback_metadata",
        lambda imdb_ids: {
            "tt35051926": {
                "episode_imdb_id": "tt35051926",
                "episode_title": "Reunion Part 3",
                "season_number": 14,
                "episode_number": 20,
                "episode_air_date": "2025-04-15",
                "show_name": "The Real Housewives of Beverly Hills",
                "show_imdb_id": "tt1720601",
                "show_short_code": "RHOBH",
                "imdb_title_type": "TVEpisode",
            }
        },
    )
    monkeypatch.setattr(
        admin_person_images,
        "_build_show_lookup_maps",
        lambda db: (
            {
                "tt1720601": {
                    "id": "show-rhobh-id",
                    "name": "The Real Housewives of Beverly Hills",
                    "imdb_id": "tt1720601",
                }
            },
            {
                "the real housewives of beverly hills": {
                    "id": "show-rhobh-id",
                    "name": "The Real Housewives of Beverly Hills",
                    "imdb_id": "tt1720601",
                }
            },
            {
                "show-rhobh-id": {
                    "id": "show-rhobh-id",
                    "name": "The Real Housewives of Beverly Hills",
                    "imdb_id": "tt1720601",
                }
            },
        ),
    )

    tagged, failed = admin_person_images._enrich_cast_photos_with_episode_metadata(mock_db, photos)

    assert tagged == 1
    assert failed == 0
    enriched = photos[0]
    assert enriched["season"] == 14
    assert enriched["title_names"] == ["Reunion Part 3", "The Real Housewives of Beverly Hills"]
    metadata = enriched["metadata"]
    assert metadata["episode_imdb_id"] == "tt35051926"
    assert metadata["episode_title"] == "Reunion Part 3"
    assert metadata["season_number"] == 14
    assert metadata["episode_number"] == 20
    assert metadata["episode_air_date"] == "2025-04-15"
    assert metadata["show_name"] == "The Real Housewives of Beverly Hills"
    assert metadata["show_id"] == "show-rhobh-id"
    assert metadata["show_imdb_id"] == "tt1720601"
    assert metadata["show_short_code"] == "RHOBH"
    assert metadata["show_context_source"] == "imdb_title_fallback"


def test_enrich_cast_photos_with_episode_metadata_marks_unresolved_imdb_episode_show_as_null(monkeypatch) -> None:
    photos = [
        {
            "source": "imdb",
            "title_imdb_ids": ["tt26755932"],
            "title_names": ["Milo Ventimiglia & Alan Cumming"],
            "metadata": {
                "show_id": "stale-show-id",
                "show_name": "Stale Show",
                "show_imdb_id": "tt0000000",
                "show_short_code": "SS",
            },
        }
    ]

    mock_db = MagicMock()
    episodes_response = MagicMock()
    episodes_response.error = None
    episodes_response.data = []
    episodes_query = mock_db.schema.return_value.table.return_value.select.return_value.in_.return_value
    episodes_query.execute.return_value = episodes_response

    monkeypatch.setattr(
        admin_person_images,
        "_fetch_imdb_title_fallback_metadata",
        lambda imdb_ids: {
            "tt26755932": {
                "episode_imdb_id": "tt26755932",
                "episode_title": "Milo Ventimiglia & Alan Cumming",
                "season_number": 20,
                "episode_number": 37,
                "episode_air_date": "2023-03-15",
                "show_name": "Watch What Happens Live with Andy Cohen",
                "show_imdb_id": "tt0318220",
                "show_short_code": None,
                "imdb_title_type": "TVEpisode",
            }
        },
    )
    monkeypatch.setattr(
        admin_person_images,
        "_build_show_lookup_maps",
        lambda db: ({}, {}, {}),
    )

    tagged, failed = admin_person_images._enrich_cast_photos_with_episode_metadata(mock_db, photos)

    assert tagged == 1
    assert failed == 0
    metadata = photos[0]["metadata"]
    assert metadata["show_context_source"] == "imdb_episode_unresolved"
    assert metadata["show_id"] is None
    assert metadata["show_name"] is None
    assert metadata["show_imdb_id"] is None
    assert metadata["show_short_code"] is None
    assert metadata["imdb_fallback_show_name"] == "Watch What Happens Live with Andy Cohen"
    assert metadata["imdb_fallback_show_imdb_id"] == "tt0318220"


def test_iter_normalized_show_lookup_keys_handles_parenthetical_variants() -> None:
    keys = admin_person_images._iter_normalized_show_lookup_keys("The Traitors (US)")
    assert "the traitors us" in keys
    assert "the traitors" in keys


def test_apply_show_context_to_photos_does_not_overwrite_existing_show_metadata() -> None:
    show_id = uuid4()
    mock_db = MagicMock()
    photos = [
        {"id": "photo-1", "metadata": {"show_id": "legacy-show-id", "show_name": "Legacy Show"}},
        {"id": "photo-2", "metadata": {"show_name": "Legacy Caption Show"}},
    ]

    tagged, failed = admin_person_images._apply_show_context_to_photos(
        mock_db,
        photos,
        show_id=show_id,
        show_name="Current Show",
    )

    assert tagged == 0
    assert failed == 0
    assert photos[0]["metadata"]["show_id"] == "legacy-show-id"
    assert photos[0]["metadata"]["show_name"] == "Legacy Show"
    assert photos[1]["metadata"].get("show_id") is None
    assert photos[1]["metadata"]["show_name"] == "Legacy Caption Show"


def test_apply_show_context_to_photos_only_tag_absent_metadata() -> None:
    show_id = uuid4()
    mock_db = MagicMock()
    photos = [
        {"id": "photo-1", "metadata": {"show_id": "legacy-show-id"}},
        {"id": "photo-2", "metadata": {}},
        {"id": "photo-3", "metadata": None},
        {"id": "photo-4"},
    ]

    tagged, failed = admin_person_images._apply_show_context_to_photos(
        mock_db,
        photos,
        show_id=show_id,
        show_name="Current Show",
    )

    assert tagged == 3
    assert failed == 0
    assert photos[0]["metadata"]["show_id"] == "legacy-show-id"
    assert "show_name" not in photos[0]["metadata"]
    assert photos[1]["metadata"]["show_id"] == str(show_id)
    assert photos[1]["metadata"]["show_name"] == "Current Show"
    assert photos[2]["metadata"]["show_id"] == str(show_id)
    assert photos[2]["metadata"]["show_name"] == "Current Show"
    assert photos[3]["metadata"]["show_id"] == str(show_id)
    assert photos[3]["metadata"]["show_name"] == "Current Show"


def test_apply_show_context_to_photos_skips_unresolved_imdb_episode_rows() -> None:
    show_id = uuid4()
    mock_db = MagicMock()
    photos = [
        {
            "id": "photo-1",
            "source": "imdb",
            "title_imdb_ids": ["tt26755932"],
            "metadata": {
                "show_context_source": "imdb_episode_unresolved",
                "episode_title": "Milo Ventimiglia & Alan Cumming",
            },
        },
        {"id": "photo-2", "source": "tmdb", "metadata": {}},
    ]

    tagged, failed = admin_person_images._apply_show_context_to_photos(
        mock_db,
        photos,
        show_id=show_id,
        show_name="The Traitors",
    )

    assert tagged == 1
    assert failed == 0
    assert photos[0]["metadata"].get("show_id") is None
    assert photos[0]["metadata"].get("show_name") is None
    assert photos[1]["metadata"]["show_id"] == str(show_id)
    assert photos[1]["metadata"]["show_name"] == "The Traitors"
    assert photos[1]["metadata"]["show_context_source"] == "request_context"


def test_apply_show_context_to_photos_infers_unresolved_imdb_episode_rows_from_fallback_show(monkeypatch) -> None:
    show_id = uuid4()
    show_id_str = str(show_id)
    mock_db = MagicMock()
    episodes_response = MagicMock()
    episodes_response.error = None
    episodes_response.data = []
    episodes_query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value
    episodes_query.execute.return_value = episodes_response

    photos = [
        {
            "id": "photo-1",
            "source": "imdb",
            "title_imdb_ids": ["tt99999999"],
            "metadata": {
                "show_context_source": "imdb_episode_unresolved",
                "episode_title": "The Power of the Seer",
                "imdb_fallback_show_name": "The Traitors",
            },
        }
    ]

    monkeypatch.setattr(
        admin_person_images,
        "_build_show_lookup_maps",
        lambda db: (
            {
                "tt1234567": {
                    "id": show_id_str,
                    "name": "The Traitors",
                    "imdb_id": "tt1234567",
                }
            },
            {
                "the traitors": {
                    "id": show_id_str,
                    "name": "The Traitors",
                    "imdb_id": "tt1234567",
                }
            },
            {
                show_id_str: {
                    "id": show_id_str,
                    "name": "The Traitors",
                    "imdb_id": "tt1234567",
                }
            },
        ),
    )

    tagged, failed = admin_person_images._apply_show_context_to_photos(
        mock_db,
        photos,
        show_id=show_id,
        show_name="The Traitors",
    )

    assert tagged == 1
    assert failed == 0
    assert photos[0]["metadata"]["show_id"] == show_id_str
    assert photos[0]["metadata"]["show_name"] == "The Traitors"
    assert photos[0]["metadata"]["show_imdb_id"] == "tt1234567"
    assert photos[0]["metadata"]["show_context_source"] == "request_context_inferred"


def test_apply_show_context_to_photos_infers_unresolved_imdb_episode_rows_from_episode_match(monkeypatch) -> None:
    show_id = uuid4()
    show_id_str = str(show_id)
    mock_db = MagicMock()
    episodes_response = MagicMock()
    episodes_response.error = None
    episodes_response.data = [
        {
            "title": "The Power of the Seer",
            "season_number": 3,
            "episode_number": 10,
        }
    ]
    episodes_query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value
    episodes_query.execute.return_value = episodes_response

    photos = [
        {
            "id": "photo-1",
            "source": "imdb",
            "title_imdb_ids": ["tt99999999"],
            "metadata": {
                "show_context_source": "imdb_episode_unresolved",
                "episode_title": "The Power of the Seer",
                "season_number": 3,
                "episode_number": 10,
            },
        }
    ]

    monkeypatch.setattr(
        admin_person_images,
        "_build_show_lookup_maps",
        lambda db: (
            {
                "tt1234567": {
                    "id": show_id_str,
                    "name": "The Traitors",
                    "imdb_id": "tt1234567",
                }
            },
            {
                "the traitors": {
                    "id": show_id_str,
                    "name": "The Traitors",
                    "imdb_id": "tt1234567",
                }
            },
            {
                show_id_str: {
                    "id": show_id_str,
                    "name": "The Traitors",
                    "imdb_id": "tt1234567",
                }
            },
        ),
    )

    tagged, failed = admin_person_images._apply_show_context_to_photos(
        mock_db,
        photos,
        show_id=show_id,
        show_name="The Traitors",
    )

    assert tagged == 1
    assert failed == 0
    assert photos[0]["metadata"]["show_id"] == show_id_str
    assert photos[0]["metadata"]["show_name"] == "The Traitors"
    assert photos[0]["metadata"]["show_context_source"] == "request_context_inferred"


def test_repair_existing_imdb_cast_photos_rewrites_rows_via_upsert(monkeypatch) -> None:
    mock_db = MagicMock()
    existing_rows = [
        {
            "id": "photo-1",
            "source": "imdb",
            "source_image_id": "rm1103833857",
            "title_imdb_ids": ["tt26755932"],
            "title_names": ["Milo Ventimiglia & Alan Cumming"],
            "metadata": {},
        }
    ]

    monkeypatch.setattr(
        admin_person_images,
        "_load_existing_imdb_cast_photos_for_person",
        lambda db, person_id: existing_rows,
    )
    monkeypatch.setattr(
        admin_person_images,
        "_enrich_cast_photos_with_episode_metadata",
        lambda db, rows: (1, 0),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_apply_show_context_to_photos",
        lambda db, rows, show_id, show_name: (0, 0),
    )

    upsert_calls: list[list[dict[str, object]]] = []

    def _fake_upsert(db, rows, *, dedupe_on):  # type: ignore[no-untyped-def]
        upsert_calls.append(list(rows))
        return [{"id": "photo-1"}]

    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        _fake_upsert,
    )

    repaired, failed = admin_person_images._repair_existing_imdb_cast_photos(
        mock_db,
        "person-1",
        show_id=None,
        show_name="The Traitors",
    )

    assert repaired == 1
    assert failed == 0
    assert len(upsert_calls) == 1


def test_repair_existing_imdb_cast_photos_backfills_image_type_from_mediaviewer_details(monkeypatch) -> None:
    mock_db = MagicMock()
    existing_rows = [
        {
            "id": "photo-1",
            "source": "imdb",
            "source_image_id": "rm_fallback",
            "viewer_id": "rm123",
            "imdb_person_id": "nm0001086",
            "title_imdb_ids": [],
            "people_imdb_ids": [],
            "people_names": [],
            "metadata": {},
        }
    ]

    monkeypatch.setattr(
        admin_person_images,
        "_load_existing_imdb_cast_photos_for_person",
        lambda db, person_id: existing_rows,
    )
    monkeypatch.setattr(admin_person_images, "_load_imdb_viewer_image_types", lambda imdb_person_id, viewer_ids: {})
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaviewer_html",
        lambda imdb_person_id, viewer_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaviewer_details",
        lambda html, viewer_id=None: {
            "caption": "Alan Cumming in The Traitors",
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "title_imdb_ids": ["tt123"],
            "title_names": ["The Traitors"],
            "image_type": "still_frame",
        },
    )
    monkeypatch.setattr(
        admin_person_images,
        "_enrich_cast_photos_with_episode_metadata",
        lambda db, rows: (0, 0),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_apply_show_context_to_photos",
        lambda db, rows, show_id, show_name: (0, 0),
    )

    upserted_rows: list[dict[str, object]] = []

    def _fake_upsert(db, rows, *, dedupe_on):  # type: ignore[no-untyped-def]
        upserted_rows.extend(rows)
        return [{"id": "photo-1"}]

    monkeypatch.setattr("trr_backend.repositories.cast_photos.upsert_cast_photos", _fake_upsert)

    repaired, failed = admin_person_images._repair_existing_imdb_cast_photos(
        mock_db,
        "person-1",
        show_id=None,
        show_name="The Traitors",
    )

    assert repaired == 1
    assert failed == 0
    assert len(upserted_rows) == 1
    metadata = dict(upserted_rows[0].get("metadata") or {})
    assert metadata["imdb_image_type"] == "still_frame"
    assert isinstance(metadata.get("imdb_metadata_refreshed_at"), str)


def test_repair_existing_imdb_cast_photos_skips_complete_rows(monkeypatch) -> None:
    mock_db = MagicMock()
    existing_rows = [
        {
            "id": "photo-1",
            "source": "imdb",
            "source_image_id": "rm_complete",
            "title_imdb_ids": ["tt123"],
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "metadata": {
                "imdb_image_type": "event",
                "show_context_source": "request_context",
                "tags": {
                    "people": [{"imdb_id": "nm0001086", "name": "Alan Cumming"}],
                    "titles": [{"imdb_id": "tt123", "title": "The Traitors"}],
                },
            },
        }
    ]
    monkeypatch.setattr(
        admin_person_images,
        "_load_existing_imdb_cast_photos_for_person",
        lambda db, person_id: existing_rows,
    )
    upsert_mock = MagicMock(return_value=[])
    monkeypatch.setattr("trr_backend.repositories.cast_photos.upsert_cast_photos", upsert_mock)

    repaired, failed = admin_person_images._repair_existing_imdb_cast_photos(
        mock_db,
        "person-1",
        show_id=None,
        show_name="The Traitors",
    )

    assert repaired == 0
    assert failed == 0
    upsert_mock.assert_not_called()


def test_load_existing_imdb_cast_photos_falls_back_when_source_asset_id_missing() -> None:
    mock_db = MagicMock()
    query = mock_db.schema.return_value.table.return_value
    query.select.return_value = query
    query.eq.return_value = query

    fallback_response = MagicMock()
    fallback_response.error = None
    fallback_response.data = [
        {
            "id": "photo-1",
            "person_id": "person-1",
            "source": "imdb",
            "source_image_id": "imdb-image-1",
        }
    ]
    query.execute.side_effect = [Exception('column "source_asset_id" does not exist'), fallback_response]

    rows = admin_person_images._load_existing_imdb_cast_photos_for_person(mock_db, "person-1")

    assert len(rows) == 1
    assert rows[0]["source_asset_id"] is None
    assert query.select.call_count == 2
    assert "source_asset_id" in query.select.call_args_list[0].args[0]
    assert "source_asset_id" not in query.select.call_args_list[1].args[0]


def test_refresh_stream_emits_terminal_error_for_unhandled_exception(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_person_images._get_person_details",
            return_value={"id": person_id, "full_name": "Example Person", "external_ids": {"imdb": "nm123"}},
        ):
            with patch("api.routers.admin_person_images._get_tmdb_id", return_value=None):
                with patch("api.routers.admin_person_images._resolve_refresh_sources", return_value=(["imdb"], False)):
                    with patch(
                        "api.routers.admin_person_images._get_known_source_total",
                        side_effect=RuntimeError("boom"),
                    ):
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                            json={"skip_mirror": True},
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    normalized_payload = response.text.replace("\r\n", "\n")
    assert "event: error" in normalized_payload
    assert '"stage": "stream"' in normalized_payload or '"stage":"stream"' in normalized_payload
    assert "Refresh stream failed" in normalized_payload


def test_reprocess_stream_emits_terminal_error_for_unhandled_exception(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_person_images._get_person_details",
            return_value={"id": person_id, "full_name": "Example Person", "external_ids": {}},
        ):
            with patch(
                "api.routers.admin_person_images._auto_count_cast_photos",
                side_effect=RuntimeError("boom"),
            ):
                response = client.post(
                    f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    normalized_payload = response.text.replace("\r\n", "\n")
    assert "event: error" in normalized_payload
    assert '"stage": "stream"' in normalized_payload or '"stage":"stream"' in normalized_payload
    assert "Reprocess stream failed" in normalized_payload


def test_refresh_stream_emits_resizing_heartbeat_during_long_variant_generation(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")

    person_data = {"id": person_id, "full_name": "Resize Heartbeat Person", "external_ids": {}}
    mock_db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [person_data]
    mock_response.error = None
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response

    def _slow_resize(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        time.sleep(2.4)
        return (1, 1, 0, 1, 1, 0)

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_person_images._refresh_tmdb_profile", return_value=None):
                with patch("api.routers.admin_person_images._refresh_fandom_profile", return_value=None):
                    with patch("api.routers.admin_person_images._resolve_refresh_sources", return_value=([], False)):
                        with patch(
                            "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                            return_value=(0, 0),
                        ):
                            with patch(
                                "api.routers.admin_person_images._resize_person_gallery_images",
                                side_effect=_slow_resize,
                            ):
                                response = client.post(
                                f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                json={
                                    "skip_mirror": True,
                                    "skip_auto_count": True,
                                    "skip_word_detection": True,
                                    "skip_centering": True,
                                },
                                headers={"Authorization": f"Bearer {token}"},
                            )

    assert response.status_code == 200
    payload = response.text.replace("\r\n", "\n")
    blocks = [block for block in payload.split("\n\n") if block.strip()]
    assert any(
        ('"stage": "resizing"' in block or '"stage":"resizing"' in block)
        and ('"heartbeat": true' in block or '"heartbeat":true' in block)
        for block in blocks
    )


def test_refresh_stream_resizing_heartbeat_includes_operation_progress(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")

    person_data = {"id": person_id, "full_name": "Resize Progress Person", "external_ids": {}}
    mock_db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [person_data]
    mock_response.error = None
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response

    def _slow_resize(*_args, **kwargs):  # type: ignore[no-untyped-def]
        progress_cb = kwargs.get("progress_cb")
        if callable(progress_cb):
            progress_cb(3, 10)
        time.sleep(2.4)
        return (4, 3, 1, 2, 2, 0)

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_person_images._refresh_tmdb_profile", return_value=None):
            with patch("api.routers.admin_person_images._refresh_fandom_profile", return_value=None):
                with patch("api.routers.admin_person_images._resolve_refresh_sources", return_value=([], False)):
                    with patch(
                        "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                        return_value=(0, 0),
                    ):
                        with patch(
                            "api.routers.admin_person_images._resize_person_gallery_images",
                            side_effect=_slow_resize,
                        ):
                            response = client.post(
                                f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                json={
                                    "skip_mirror": True,
                                    "skip_auto_count": True,
                                    "skip_word_detection": True,
                                    "skip_centering": True,
                                },
                                headers={"Authorization": f"Bearer {token}"},
                            )

    assert response.status_code == 200
    payload = response.text.replace("\r\n", "\n")
    blocks = [block for block in payload.split("\n\n") if block.strip()]
    saw_progress_heartbeat = False
    for block in blocks:
        lines = [line for line in block.split("\n") if line.startswith("data:")]
        if not lines:
            continue
        data = json.loads(lines[-1][len("data:") :].strip())
        if data.get("stage") != "resizing" or data.get("heartbeat") is not True:
            continue
        if data.get("current") == 3 and data.get("total") == 10:
            saw_progress_heartbeat = True
            break
    assert saw_progress_heartbeat is True


def test_refresh_stream_sync_imdb_progress_includes_diagnostics(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")

    person_data = {
        "id": person_id,
        "full_name": "Alan Cumming",
        "external_ids": {"imdb": "nm0001086"},
    }
    mock_db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [person_data]
    mock_response.error = None
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response

    def _fake_fetch_imdb(*args, **kwargs):  # type: ignore[no-untyped-def]
        diagnostics = kwargs.get("imdb_diagnostics")
        if isinstance(diagnostics, dict):
            diagnostics.update(
                {
                    "imdb_pages_scanned": 4,
                    "imdb_candidates_seen": 120,
                    "imdb_kept": 14,
                    "imdb_filtered_type": 55,
                    "imdb_filtered_people": 41,
                    "imdb_filtered_episode": 8,
                    "imdb_filtered_other": 2,
                }
            )
        return []

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_person_images._refresh_tmdb_profile", return_value=None):
            with patch("api.routers.admin_person_images._refresh_fandom_profile", return_value=None):
                with patch("api.routers.admin_person_images._resolve_refresh_sources", return_value=(["imdb"], False)):
                    with patch("api.routers.admin_person_images._get_known_source_total", return_value=None):
                        with patch(
                            "trr_backend.ingestion.cast_photo_sources.fetch_imdb_cast_photos",
                            side_effect=_fake_fetch_imdb,
                        ):
                            with patch(
                                "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                                return_value=(0, 0),
                            ):
                                response = client.post(
                                    f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                    json={
                                        "sources": ["imdb"],
                                        "skip_mirror": True,
                                        "skip_auto_count": True,
                                        "skip_word_detection": True,
                                        "skip_centering": True,
                                        "skip_resize": True,
                                    },
                                    headers={"Authorization": f"Bearer {token}"},
                                )

    assert response.status_code == 200
    blocks = [block for block in response.text.replace("\r\n", "\n").split("\n\n") if block.strip()]
    sync_imdb_events: list[dict[str, object]] = []
    for block in blocks:
        lines = [line for line in block.split("\n") if line.startswith("data:")]
        if not lines:
            continue
        payload = json.loads(lines[-1][len("data:") :].strip())
        if payload.get("stage") == "sync_imdb":
            sync_imdb_events.append(payload)

    assert sync_imdb_events, "Expected at least one sync_imdb progress event"
    final_sync_imdb = next(
        event for event in sync_imdb_events if str(event.get("message") or "").startswith("Synced IMDb")
    )
    assert final_sync_imdb["imdb_pages_scanned"] == 4
    assert final_sync_imdb["imdb_candidates_seen"] == 120
    assert final_sync_imdb["imdb_kept"] == 14
    assert final_sync_imdb["imdb_filtered_type"] == 55
    assert final_sync_imdb["imdb_filtered_people"] == 41
    assert final_sync_imdb["imdb_filtered_episode"] == 8
    assert final_sync_imdb["imdb_filtered_other"] == 2


def test_refresh_stream_includes_tmdb_profile_failure_fields_in_complete_payload(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret")

    person_data = {"id": person_id, "full_name": None, "external_ids": {"tmdb": "123"}}

    mock_db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [person_data]
    mock_response.error = None
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_person_images._run_tmdb_profile_refresh",
            return_value=("failed", "CAST_TMDB_UPSERT_FAILED", "malformed array literal"),
        ):
            with patch("api.routers.admin_person_images._refresh_fandom_profile", return_value=None):
                response = client.post(
                    f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                    json={
                        "skip_mirror": True,
                        "skip_auto_count": True,
                        "skip_word_detection": True,
                        "skip_centering": True,
                        "skip_resize": True,
                    },
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    normalized_payload = response.text.replace("\r\n", "\n")
    complete_index = normalized_payload.rfind("event: complete")
    assert complete_index >= 0
    data_index = normalized_payload.find("data:", complete_index)
    assert data_index >= 0
    json_start = normalized_payload.find("{", data_index)
    assert json_start >= 0
    json_end = normalized_payload.find("\n\n", json_start)
    if json_end == -1:
        json_end = len(normalized_payload)
    complete_data = json.loads(normalized_payload[json_start:json_end].strip())

    assert complete_data["tmdb_profile_status"] == "failed"
    assert complete_data["tmdb_profile_error_code"] == "CAST_TMDB_UPSERT_FAILED"
    assert complete_data["tmdb_profile_error_detail"] == "malformed array literal"
    assert any("TMDb profile [CAST_TMDB_UPSERT_FAILED]" in item for item in complete_data["errors"])


class TestRefreshPersonImages:
    """Test POST /api/v1/admin/person/{person_id}/refresh-images."""

    def test_returns_401_without_auth(self, client):
        """Unauthenticated requests should return 401."""
        person_id = str(uuid4())

        # Mock the database client to avoid DatabaseConnectionError in CI
        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(f"/api/v1/admin/person/{person_id}/refresh-images")
        assert response.status_code == 401

    def test_returns_404_for_unknown_person(self, client, monkeypatch):
        """Unknown person_id should return 404."""
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        # Mock the database call to return no person
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = []
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/person/{person_id}/refresh-images",
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_refresh_success_returns_summary(self, client, monkeypatch):
        """Successful refresh returns summary with counts."""
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {
            "id": person_id,
            "full_name": "Test Person",
            "external_ids": {"imdb": "nm12345678"},
        }

        # Mock the database
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        # Mock the module-level functions
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id",
                return_value=None,
            ):
                with patch(
                    "trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos",
                    return_value=[
                        {
                            "person_id": person_id,
                            "source": "imdb",
                            "url": "http://test/1.jpg",
                        },
                    ],
                ):
                    with patch(
                        "trr_backend.repositories.cast_photos.upsert_cast_photos",
                        return_value=[{"id": "p1"}],
                    ):
                        with patch(
                            "api.routers.admin_person_images._mirror_person_photos",
                            return_value=(1, 0),
                        ):
                            with patch(
                                "api.routers.admin_person_images._mirror_person_media_assets",
                                return_value=(2, 1),
                            ):
                                with patch(
                                    "api.routers.admin_person_images._prune_person_s3_objects",
                                    return_value=0,
                                ):
                                    with patch(
                                        "api.routers.admin_person_images._resize_person_gallery_images",
                                        return_value=(3, 2, 1, 1, 1, 0),
                                    ):
                                        response = client.post(
                                            f"/api/v1/admin/person/{person_id}/refresh-images",
                                            headers={"Authorization": f"Bearer {token}"},
                                        )

        assert response.status_code == 200
        data = response.json()
        assert data["person_id"] == person_id
        assert data["person_name"] == "Test Person"
        assert data["photos_fetched"] == 1
        assert data["photos_upserted"] == 1
        assert data["photos_mirrored"] == 3
        assert data["photos_failed"] == 1
        assert data["cast_photos_mirrored"] == 1
        assert data["media_assets_mirrored"] == 2
        assert data["text_overlay_unknown"] == 0
        assert data["resize_attempted"] == 3
        assert data["resize_crop_attempted"] == 1
        assert data["tmdb_profile_status"] == "skipped"
        assert data["tmdb_profile_error_code"] == "TMDB_ID_MISSING"
        assert data["tmdb_profile_error_detail"] == "No TMDb person ID was available."
        assert "text_overlay_failure_reasons" in data
        assert "episode_metadata_tagged" in data
        assert "show_context_tagged" in data
        assert "metadata_enrichment_failed" in data

    def test_refresh_tmdb_failure_is_non_terminal_and_sets_status_fields(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {
            "id": person_id,
            "full_name": "TMDb Failure Person",
            "external_ids": {"imdb": "nm0001086", "tmdb": "123"},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images._run_tmdb_profile_refresh",
                return_value=("failed", "CAST_TMDB_UPSERT_FAILED", "malformed array literal"),
            ):
                with patch("trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos", return_value=[]):
                    with patch(
                        "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                        return_value=(0, 0),
                    ):
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images",
                            json={
                                "sources": ["imdb"],
                                "skip_mirror": True,
                                "skip_auto_count": True,
                                "skip_word_detection": True,
                                "skip_centering": True,
                                "skip_resize": True,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

        assert response.status_code == 200
        data = response.json()
        assert data["tmdb_profile_status"] == "failed"
        assert data["tmdb_profile_error_code"] == "CAST_TMDB_UPSERT_FAILED"
        assert data["tmdb_profile_error_detail"] == "malformed array literal"
        assert any("TMDb profile [CAST_TMDB_UPSERT_FAILED]" in item for item in data["errors"])

    def test_refresh_response_includes_imdb_diagnostics(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {
            "id": person_id,
            "full_name": "IMDb Metrics Person",
            "external_ids": {"imdb": "nm0001086"},
        }
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        def _fake_fetch_all_cast_photos(*args, **kwargs):  # type: ignore[no-untyped-def]
            diagnostics = kwargs.get("imdb_diagnostics")
            if isinstance(diagnostics, dict):
                diagnostics.update(
                    {
                        "imdb_pages_scanned": 3,
                        "imdb_candidates_seen": 120,
                        "imdb_kept": 18,
                        "imdb_filtered_type": 12,
                        "imdb_filtered_people": 80,
                        "imdb_filtered_episode": 7,
                        "imdb_filtered_other": 3,
                    }
                )
            return []

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id", return_value=None):
                with patch(
                    "trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos",
                    side_effect=_fake_fetch_all_cast_photos,
                ):
                    with patch(
                        "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                        return_value=(0, 0),
                    ):
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images",
                            json={
                                "sources": ["imdb"],
                                "skip_mirror": True,
                                "skip_auto_count": True,
                                "skip_word_detection": True,
                                "skip_centering": True,
                                "skip_resize": True,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

        assert response.status_code == 200
        data = response.json()
        assert data["imdb_pages_scanned"] == 3
        assert data["imdb_candidates_seen"] == 120
        assert data["imdb_kept"] == 18
        assert data["imdb_filtered_type"] == 12
        assert data["imdb_filtered_people"] == 80
        assert data["imdb_filtered_episode"] == 7
        assert data["imdb_filtered_other"] == 3

    def test_skip_mirror_option(self, client, monkeypatch):
        """skip_mirror=True should skip S3 mirroring."""
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {"id": person_id, "full_name": "Test", "external_ids": {}}

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id",
                return_value=None,
            ):
                with patch(
                    "trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos",
                    return_value=[],
                ):
                    with patch(
                        "trr_backend.repositories.cast_photos.upsert_cast_photos",
                        return_value=[],
                    ):
                        with patch("api.routers.admin_person_images._mirror_person_photos") as mock_mirror:
                            response = client.post(
                                f"/api/v1/admin/person/{person_id}/refresh-images",
                                json={"skip_mirror": True},
                                headers={"Authorization": f"Bearer {token}"},
                            )

        assert response.status_code == 200
        mock_mirror.assert_not_called()

    def test_refresh_runs_existing_imdb_repair_stage(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {
            "id": person_id,
            "full_name": "Repair Person",
            "external_ids": {"imdb": "nm12345678"},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id", return_value=None):
                with patch("trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos", return_value=[]):
                    with patch(
                        "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                        return_value=(2, 0),
                    ) as repair_mock:
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images",
                            json={
                                "skip_mirror": True,
                                "skip_auto_count": True,
                                "skip_word_detection": True,
                                "skip_centering": True,
                                "skip_resize": True,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

        assert response.status_code == 200
        repair_mock.assert_called_once()

    def test_bypasses_show_source_policy_when_disabled(self, client, monkeypatch):
        """enforce_show_source_policy=False should preserve requested sources unchanged."""
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {
            "id": person_id,
            "full_name": "Test Person",
            "external_ids": {"imdb": "nm12345678"},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id",
                return_value=None,
            ):
                with patch("api.routers.admin_person_images._refresh_fandom_profile", return_value=None):
                    with patch(
                        "trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos",
                        return_value=[],
                    ) as mock_fetch_all:
                        with patch(
                            "trr_backend.repositories.cast_photos.upsert_cast_photos",
                            return_value=[],
                        ):
                            with patch(
                                "api.routers.admin_person_images._auto_count_cast_photos",
                                return_value=(0, 0, 0),
                            ):
                                with patch(
                                    "api.routers.admin_person_images._auto_count_media_links",
                                    return_value=(0, 0, 0),
                                ):
                                    with patch(
                                        "api.routers.admin_person_images._detect_text_overlay_cast_photos",
                                        return_value=(0, 0, 0, 0),
                                    ):
                                        with patch(
                                            "api.routers.admin_person_images._detect_text_overlay_media_links",
                                            return_value=(0, 0, 0, 0),
                                        ):
                                            with patch(
                                                "api.routers.admin_person_images._recenter_person_gallery_images",
                                                return_value=(0, 0, 0, 0),
                                            ):
                                                with patch(
                                                    "api.routers.admin_person_images._resize_person_gallery_images",
                                                    return_value=(0, 0, 0, 0, 0, 0),
                                                ):
                                                    with patch(
                                                        "api.routers.admin_person_images._apply_show_source_policy"
                                                    ) as mock_policy:
                                                        response = client.post(
                                                            f"/api/v1/admin/person/{person_id}/refresh-images",
                                                            json={
                                                                "skip_mirror": True,
                                                                "sources": ["imdb", "fandom"],
                                                                "enforce_show_source_policy": False,
                                                            },
                                                            headers={"Authorization": f"Bearer {token}"},
                                                        )

        assert response.status_code == 200
        mock_policy.assert_not_called()
        assert mock_fetch_all.call_count == 1
        assert mock_fetch_all.call_args.kwargs["sources"] == ["imdb", "fandom"]


def test_resize_person_gallery_images_uses_fallback_crop_when_missing(monkeypatch):
    mock_db = MagicMock()

    cast_query = MagicMock()
    cast_query.select.return_value = cast_query
    cast_query.eq.return_value = cast_query
    cast_query.in_.return_value = cast_query
    cast_query.limit.return_value = cast_query
    cast_query.not_ = MagicMock()
    cast_query.not_.is_.return_value = cast_query

    cast_response = MagicMock()
    cast_response.error = None
    cast_response.data = [
        {
            "id": str(uuid4()),
            "source": "imdb",
            "hosted_url": "https://cdn.example.com/photo.jpg",
            "metadata": {},
        }
    ]
    cast_query.execute.return_value = cast_response
    mock_db.schema.return_value.table.return_value = cast_query

    with patch(
        "api.routers.admin_person_images._fetch_person_media_link_rows",
        return_value=[],
    ):
        with patch(
            "api.routers.admin_image_counts.auto_count_cast_photo",
            side_effect=RuntimeError("detector unavailable"),
        ):
            with patch("trr_backend.media.image_variants.generate_cast_photo_variants") as generate_cast_variants:
                result = admin_person_images._resize_person_gallery_images(
                    mock_db,
                    person_id=str(uuid4()),
                    sources=["imdb"],
                    force=True,
                )

    assert result[0] == 1
    assert result[1] == 1
    assert result[2] == 0
    assert result[3] == 1
    assert result[4] == 1
    assert result[5] == 0
    assert generate_cast_variants.call_count == 2
    assert generate_cast_variants.call_args_list[0].kwargs["crop"] is None
    assert generate_cast_variants.call_args_list[1].kwargs["crop"]["strategy"] == "resize_center_fallback_v1"

    def test_stream_emits_resizing_stage_and_complete_counters(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {"id": person_id, "full_name": None, "external_ids": {}}

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id",
                return_value=None,
            ):
                with patch("api.routers.admin_person_images._refresh_tmdb_profile", return_value=None):
                    with patch("api.routers.admin_person_images._refresh_fandom_profile", return_value=None):
                        with patch(
                            "api.routers.admin_person_images._resize_person_gallery_images",
                            return_value=(4, 3, 1, 2, 2, 0),
                        ):
                            with patch(
                                "trr_backend.clients.screenalytics.is_screenalytics_configured",
                                return_value=False,
                            ):
                                with patch(
                                    "trr_backend.vision.text_overlay.is_text_overlay_detection_configured",
                                    return_value=False,
                                ):
                                    response = client.post(
                                        f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                        json={"skip_mirror": True},
                                        headers={"Authorization": f"Bearer {token}"},
                                    )

        assert response.status_code == 200
        payload = response.text
        assert "event: progress" in payload
        assert '"stage": "resizing"' in payload or '"stage":"resizing"' in payload

        normalized_payload = payload.replace("\r\n", "\n")
        assert "event: complete" in normalized_payload
        complete_index = normalized_payload.rfind("event: complete")
        assert complete_index >= 0
        data_index = normalized_payload.find("data:", complete_index)
        assert data_index >= 0
        json_start = normalized_payload.find("{", data_index)
        assert json_start >= 0
        json_end = normalized_payload.find("\n\n", json_start)
        if json_end == -1:
            json_end = len(normalized_payload)
        complete_data = json.loads(normalized_payload[json_start:json_end].strip())
        assert complete_data["resize_attempted"] == 4
        assert complete_data["resize_succeeded"] == 3
        assert complete_data["resize_crop_attempted"] == 2
        assert complete_data["text_overlay_configured"] is False
        assert complete_data["text_overlay_candidates"] == 0
        assert complete_data["text_overlay_skipped_reason"] == "not_configured"
        assert complete_data["live_counts"] == {
            "synced": complete_data["photos_upserted"],
            "mirrored": complete_data["photos_mirrored"],
            "counted": complete_data["auto_counts_succeeded"],
            "cropped": complete_data["centering_succeeded"],
            "id_text": complete_data["text_overlay_succeeded"],
            "resized": complete_data["resize_succeeded"],
        }

    def test_stream_honors_skip_flags_for_ingest_only_mode(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {
            "id": person_id,
            "full_name": "Skip Flags Person",
            "external_ids": {"imdb": "nm7654321"},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_person_images._refresh_tmdb_profile", return_value=None):
                with patch("api.routers.admin_person_images._refresh_fandom_profile", return_value=None):
                    with patch(
                        "api.routers.admin_person_images._enrich_cast_photos_with_episode_metadata",
                        return_value=(0, 0),
                    ):
                        with patch(
                            "api.routers.admin_person_images._apply_show_context_to_photos",
                            return_value=(0, 0),
                        ):
                            with patch(
                                "api.routers.admin_person_images._get_known_source_total",
                                return_value=None,
                            ):
                                with patch(
                                    "trr_backend.ingestion.cast_photo_sources.fetch_imdb_cast_photos",
                                    return_value=[
                                        {
                                            "person_id": person_id,
                                            "source": "imdb",
                                            "url": "https://images.example.com/imdb.jpg",
                                            "source_image_id": "imdb-1",
                                        }
                                    ],
                                ):
                                    with patch(
                                        "trr_backend.repositories.cast_photos.upsert_cast_photos",
                                        return_value=[{"id": "cast-photo-1"}],
                                    ):
                                        with patch(
                                            "api.routers.admin_person_images._resize_person_gallery_images"
                                        ) as resize_mock:
                                            with patch(
                                                "trr_backend.clients.screenalytics.is_screenalytics_configured"
                                            ) as screen_cfg_mock:
                                                with patch(
                                                    "trr_backend.vision.text_overlay.is_text_overlay_detection_configured"
                                                ) as text_cfg_mock:
                                                    response = client.post(
                                                        f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                                        json={
                                                            "sources": ["imdb"],
                                                            "skip_mirror": True,
                                                            "skip_auto_count": True,
                                                            "skip_word_detection": True,
                                                            "skip_centering": True,
                                                            "skip_resize": True,
                                                        },
                                                        headers={"Authorization": f"Bearer {token}"},
                                                    )

        assert response.status_code == 200
        normalized_payload = response.text.replace("\r\n", "\n")
        complete_index = normalized_payload.rfind("event: complete")
        assert complete_index >= 0
        data_index = normalized_payload.find("data:", complete_index)
        assert data_index >= 0
        json_start = normalized_payload.find("{", data_index)
        assert json_start >= 0
        json_end = normalized_payload.find("\n\n", json_start)
        if json_end == -1:
            json_end = len(normalized_payload)
        complete_data = json.loads(normalized_payload[json_start:json_end].strip())

        assert complete_data["photos_fetched"] == 1
        assert complete_data["photos_upserted"] == 1
        assert complete_data["auto_counts_attempted"] == 0
        assert complete_data["text_overlay_attempted"] == 0
        assert complete_data["centering_attempted"] == 0
        assert complete_data["resize_attempted"] == 0
        assert complete_data["live_counts"]["counted"] == 0
        assert complete_data["live_counts"]["cropped"] == 0
        assert complete_data["live_counts"]["id_text"] == 0
        assert complete_data["live_counts"]["resized"] == 0

        resize_mock.assert_not_called()
        screen_cfg_mock.assert_not_called()
        text_cfg_mock.assert_not_called()

    def test_stream_skips_imdb_when_source_already_fully_mirrored(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {
            "id": person_id,
            "full_name": "Test Person",
            "external_ids": {"imdb": "nm1234567"},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_person_images._refresh_tmdb_profile", return_value=None):
                with patch("api.routers.admin_person_images._refresh_fandom_profile", return_value=None):
                    with patch("api.routers.admin_person_images._get_known_source_total", return_value=3):
                        with patch("api.routers.admin_person_images._count_mirrored_cast_photos", return_value=3):
                            with patch(
                                "trr_backend.ingestion.cast_photo_sources.fetch_imdb_cast_photos",
                                return_value=[],
                            ) as imdb_fetch_mock:
                                response = client.post(
                                    f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                    json={"sources": ["imdb"], "skip_mirror": True, "force_mirror": False},
                                    headers={"Authorization": f"Bearer {token}"},
                                )

        assert response.status_code == 200
        payload = response.text
        assert "already_mirrored" in payload
        assert '"sources_skipped": 1' in payload or '"sources_skipped":1' in payload
        imdb_fetch_mock.assert_not_called()

    def test_reprocess_stream_includes_text_overlay_skip_reason_fields(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret")

        person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images._auto_count_cast_photos",
                return_value=(0, 0, 0),
            ):
                with patch(
                    "api.routers.admin_person_images._auto_count_media_links",
                    return_value=(0, 0, 0),
                ):
                    with patch(
                        "trr_backend.vision.text_overlay.is_text_overlay_detection_configured",
                        return_value=False,
                    ):
                        with patch(
                            "api.routers.admin_person_images._recenter_person_gallery_images",
                            return_value=(0, 0, 0, 0),
                        ):
                            with patch(
                                "api.routers.admin_person_images._resize_person_gallery_images",
                                return_value=(4, 3, 1, 2, 2, 0),
                            ):
                                response = client.post(
                                    f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                                    headers={"Authorization": f"Bearer {token}"},
                                )

        assert response.status_code == 200
        normalized_payload = response.text.replace("\r\n", "\n")
        complete_index = normalized_payload.rfind("event: complete")
        assert complete_index >= 0
        data_index = normalized_payload.find("data:", complete_index)
        assert data_index >= 0
        json_start = normalized_payload.find("{", data_index)
        assert json_start >= 0
        json_end = normalized_payload.find("\n\n", json_start)
        if json_end == -1:
            json_end = len(normalized_payload)
        complete_data = json.loads(normalized_payload[json_start:json_end].strip())

        assert complete_data["text_overlay_configured"] is False
        assert complete_data["text_overlay_candidates"] == 0
        assert complete_data["text_overlay_skipped_reason"] == "not_configured"
        assert complete_data["live_counts"] == {
            "synced": 0,
            "mirrored": 0,
            "counted": complete_data["auto_counts_succeeded"],
            "cropped": complete_data["centering_succeeded"],
            "id_text": complete_data["text_overlay_succeeded"],
            "resized": complete_data["resize_succeeded"],
        }
        assert complete_data["resize_attempted"] == 4
        assert complete_data["resize_succeeded"] == 3
        assert complete_data["resize_crop_attempted"] == 2


class TestUpdateFacebankSeed:
    """Test PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed."""

    def test_allows_allowlist_user(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": person_id,
                "entity_type": "person",
                "kind": "gallery",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images.update_media_link_facebank_seed",
                return_value={"id": link_id, "facebank_seed": True},
            ):
                response = client.patch(
                    f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                    json={"facebank_seed": True},
                    headers={"Authorization": f"Bearer {token}"},
                )

        assert response.status_code == 200
        data = response.json()
        assert data["link_id"] == link_id
        assert data["person_id"] == person_id
        assert data["facebank_seed"] is True

    def test_rejects_service_role_without_internal_secret_header(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 403
        assert "Allowlist admin access required" in response.json()["detail"]

    def test_rejects_service_role_with_invalid_internal_secret_header(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-TRR-Internal-Admin-Secret": "wrong-secret",
                },
            )

        assert response.status_code == 403
        assert "Allowlist admin access required" in response.json()["detail"]

    def test_allows_service_role_with_valid_internal_secret_header(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": person_id,
                "entity_type": "person",
                "kind": "gallery",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images.update_media_link_facebank_seed",
                return_value={"id": link_id, "facebank_seed": True},
            ):
                response = client.patch(
                    f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                    json={"facebank_seed": True},
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-TRR-Internal-Admin-Secret": "internal-secret",
                    },
                )

        assert response.status_code == 200
        data = response.json()
        assert data["link_id"] == link_id
        assert data["person_id"] == person_id
        assert data["facebank_seed"] is True

    def test_returns_404_when_media_link_not_found(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(mock_db, None)

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 404
        assert "Media link not found" in response.json()["detail"]

    def test_returns_409_when_media_link_is_not_person_gallery(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": person_id,
                "entity_type": "show",
                "kind": "poster",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 409
        assert "not a person gallery image" in response.json()["detail"]

    def test_returns_409_when_media_link_person_mismatch(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        other_person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": other_person_id,
                "entity_type": "person",
                "kind": "gallery",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.patch(
                f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                json={"facebank_seed": True},
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 409
        assert "does not belong to this person" in response.json()["detail"]

    def test_returns_502_when_update_media_link_fails(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret", "admin@example.com")

        mock_db = MagicMock()
        _mock_media_link_lookup(
            mock_db,
            {
                "id": link_id,
                "entity_id": person_id,
                "entity_type": "person",
                "kind": "gallery",
                "facebank_seed": False,
            },
        )

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images.update_media_link_facebank_seed",
                side_effect=RuntimeError("db write failed"),
            ):
                response = client.patch(
                    f"/api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed",
                    json={"facebank_seed": True},
                    headers={"Authorization": f"Bearer {token}"},
                )

        assert response.status_code == 502
        assert "Database error updating facebank_seed" in response.json()["detail"]


def test_pick_autocount_url_prefers_hosted_when_available() -> None:
    row = {
        "source": "fandom",
        "image_url": "https://real-housewives.fandom.com/wiki/Special:FilePath/Bad.png",
        "thumb_url": "https://static.wikia.nocookie.net/real-housewives/images/1/1a/Good.png",
        "hosted_url": "https://cdn.example.com/x.png",
    }
    assert admin_person_images._pick_autocount_url(row) == row["hosted_url"]


def test_pick_autocount_urls_normalizes_stale_wikia_revision_path() -> None:
    row = {
        "source": "fandom",
        "thumb_url": "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg/revision/latest",
    }
    urls = admin_person_images._pick_autocount_urls(row)
    assert urls[0] == "https://static.wikia.nocookie.net/real-housewives/images/0/08/angie_k_s3.jpeg"


def test_pick_autocount_url_prefers_tmdb_image() -> None:
    row = {
        "source": "tmdb",
        "image_url": "https://image.tmdb.org/t/p/original/x.png",
        "hosted_url": "https://cdn.example.com/x.png",
    }
    assert admin_person_images._pick_autocount_url(row) == row["image_url"]


def test_pick_autocount_url_falls_back_to_hosted() -> None:
    row = {
        "source": "fandom",
        "hosted_url": "https://cdn.example.com/x.png",
    }
    assert admin_person_images._pick_autocount_url(row) == row["hosted_url"]
