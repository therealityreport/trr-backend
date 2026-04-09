"""Tests for admin person images refresh endpoint."""

from __future__ import annotations

import json
import re
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi import Response
from fastapi.testclient import TestClient

from api.main import app
from api.routers import admin_nbcumv, admin_person_images
from trr_backend.media.getty_replacement import ResolvedPublicReplacement

_REAL_IMPORT_NBCUMV_PERSON_MEDIA = admin_person_images._import_nbcumv_person_media
_REAL_IMPORT_BRAVOTV_PERSON_MEDIA = admin_person_images._import_bravotv_person_media


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    """Create a valid admin JWT token."""
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
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
        "nbf": int(now.timestamp()),
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


@pytest.fixture(autouse=True)
def stub_nbcumv_person_import(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        admin_person_images,
        "_import_nbcumv_person_media",
        lambda *args, **kwargs: {
            "fetched": 0,
            "imported": 0,
            "skipped": 0,
            "failed": 0,
            "gallery_links_created": 0,
            "asset_ids": [],
            "errors": [],
        },
    )


@pytest.fixture(autouse=True)
def stub_bravotv_person_import(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        admin_person_images,
        "_import_bravotv_person_media",
        lambda *args, **kwargs: {
            "fetched": 0,
            "imported": 0,
            "skipped": 0,
            "failed": 0,
            "gallery_links_created": 0,
            "asset_ids": [],
            "errors": [],
        },
    )


@pytest.fixture(autouse=True)
def stub_getty_grouped_events(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.getty.search_grouped_events",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda *args, **kwargs: {},
    )


@pytest.fixture(autouse=True)
def stub_nbcumv_direct_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.nbcumv.discover_person_show_titles",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "trr_backend.integrations.nbcumv.search_person_images",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "trr_backend.integrations.nbcumv.search_person_show_catalog",
        lambda *args, **kwargs: [],
    )


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


def test_resolve_refresh_sources_canonicalizes_getty_alias_to_nbcumv() -> None:
    request = admin_person_images.RefreshImagesRequest(sources=["imdb", "getty"])
    sources, fandom_skipped = admin_person_images._resolve_refresh_sources(MagicMock(), request)
    assert fandom_skipped is False
    assert sources == ["imdb", "nbcumv"]


def test_resolve_refresh_sources_preserves_bravotv_source() -> None:
    request = admin_person_images.RefreshImagesRequest(sources=["bravotv", "imdb"])
    sources, fandom_skipped = admin_person_images._resolve_refresh_sources(MagicMock(), request)
    assert fandom_skipped is False
    assert sources == ["bravotv", "imdb"]


def test_normalize_operational_refresh_sources_forces_getty_pipeline_from_requested_alias() -> None:
    request = admin_person_images.RefreshImagesRequest(sources=["getty"])
    normalized = admin_person_images._normalize_operational_refresh_sources(["getty"], request)
    assert normalized == ["nbcumv"]


def test_normalize_operational_refresh_sources_forces_getty_pipeline_from_prefetched_payload() -> None:
    request = admin_person_images.RefreshImagesRequest(
        sources=["imdb"],
        getty_prefetched_assets=[{"editorial_id": "123"}],
    )
    normalized = admin_person_images._normalize_operational_refresh_sources(["imdb"], request)
    assert normalized == ["imdb", "nbcumv"]


def test_normalize_source_progress_key_maps_getty_alias_to_shared_bucket() -> None:
    assert admin_person_images._normalize_source_progress_key("getty") == "getty_nbcumv"
    assert admin_person_images._normalize_source_progress_key("nbcumv") == "getty_nbcumv"


def test_refresh_request_accepts_expanded_limit_per_source() -> None:
    request = admin_person_images.RefreshImagesRequest(limit_per_source=1000)
    assert request.limit_per_source == 1000


def test_refresh_request_accepts_getty_source_alias() -> None:
    request = admin_person_images.RefreshImagesRequest(sources=["getty"])
    assert request.sources == ["getty"]


def test_reprocess_request_accepts_getty_source() -> None:
    request = admin_person_images.ReprocessImagesRequest(sources=["getty"])
    assert request.sources == ["getty"]


def test_reprocess_request_accepts_bravotv_source() -> None:
    request = admin_person_images.ReprocessImagesRequest(sources=["bravotv"])
    assert request.sources == ["bravotv"]


def test_should_run_imdb_metadata_repair_for_sources_is_source_aware() -> None:
    assert admin_person_images._should_run_imdb_metadata_repair_for_sources(["imdb"]) is True
    assert admin_person_images._should_run_imdb_metadata_repair_for_sources(["getty", "nbcumv"]) is False
    assert admin_person_images._should_run_imdb_metadata_repair_for_sources(["bravotv"]) is False


def test_import_bravotv_person_media_skips_low_quality_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.bravotv import get_images_pipeline, run_service

    good_row = {
        "file_url": "https://cdn.example.com/good.jpg",
        "file_name": "good.jpg",
        "media_uuid": "media-good",
        "gallery_path": "/gallery/good",
        "gallery_title": "Good Gallery",
        "gallery_show_name": "Watch What Happens Live with Andy Cohen",
        "gallery_people_names": ["Brandi Glanville"],
        "field_caption": "Brandi Glanville poses backstage.",
        "gallery_position": 0,
        "season_number": 19,
    }
    low_row = {
        "file_url": "https://cdn.example.com/low.jpg",
        "file_name": "low.jpg",
        "media_uuid": "media-low",
        "gallery_path": "/gallery/low",
        "gallery_title": "Low Gallery",
        "gallery_show_name": "Watch What Happens Live with Andy Cohen",
        "gallery_people_names": ["Brandi Glanville"],
        "field_caption": "Brandi Glanville waves backstage.",
        "gallery_position": 1,
        "season_number": 19,
    }

    monkeypatch.setattr(get_images_pipeline, "_collect_bravo_person", lambda *args, **kwargs: [good_row, low_row])

    def _fake_download(source_url: str) -> dict[str, object]:
        if source_url.endswith("good.jpg"):
            return {
                "data": b"good-bytes",
                "content_type": "image/jpeg",
                "width": 1600,
                "height": 900,
                "size_bytes": 250_000,
            }
        return {
            "data": b"low-bytes",
            "content_type": "image/jpeg",
            "width": 640,
            "height": 360,
            "size_bytes": 20_000,
        }

    monkeypatch.setattr(admin_person_images, "_download_bravotv_source_image", _fake_download)
    monkeypatch.setattr(
        get_images_pipeline,
        "_upload_bytes",
        lambda data, *, content_type=None: {
            "hosted_url": "https://cdn.example.com/hosted.jpg",
            "hosted_key": "shared/key.jpg",
            "hosted_sha256": "sha256",
            "hosted_content_type": content_type or "image/jpeg",
            "hosted_bytes": len(data),
            "hosted_etag": "etag",
            "hosted_at": "2026-03-21T00:00:00+00:00",
        },
    )

    def _fake_import(
        *,
        supplemental_catalog: dict[str, object],
        **kwargs: object,
    ) -> tuple[dict[str, object], list[dict[str, str]]]:
        bravo_rows = supplemental_catalog.get("bravo")
        assert isinstance(bravo_rows, list)
        assert len(bravo_rows) == 1
        assert bravo_rows[0]["source_image_id"] == "media-good"
        return (
            {"supplemental_assets_upserted": 1, "supplemental_links_created": 2},
            [{"media_asset_id": "asset-1"}],
        )

    monkeypatch.setattr(run_service, "_import_supplemental_catalog", _fake_import)

    result = _REAL_IMPORT_BRAVOTV_PERSON_MEDIA(
        MagicMock(),
        person_id="person-1",
        person_name="Brandi Glanville",
        show_name="Watch What Happens Live with Andy Cohen",
        limit=10,
    )

    assert result["fetched"] == 2
    assert result["imported"] == 1
    assert result["skipped"] == 1
    assert result["failed"] == 0
    assert result["gallery_links_created"] == 2


def test_import_bravotv_person_media_skips_castmate_caption_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.bravotv import get_images_pipeline, run_service

    castmate_row = {
        "file_url": "https://cdn.example.com/kyle.jpg",
        "file_name": "kyle.jpg",
        "media_uuid": "media-kyle",
        "gallery_path": "/the-real-housewives-of-beverly-hills/photos/meet-the-rhobh-season-3-cast",
        "gallery_item_id": "9783321",
        "source_page_url": (
            "https://www.bravotv.com/the-real-housewives-of-beverly-hills/photos/meet-the-rhobh-season-3-cast#9783321"
        ),
        "gallery_title": "Meet the RHOBH Season 3 Cast",
        "gallery_show_name": "The Real Housewives of Beverly Hills",
        "gallery_people_names": ["Brandi Glanville", "Kyle Richards", "Kim Richards"],
        "field_caption": "Kyle continues tho struggle with her sister, Kim.",
        "field_media_image_alt": "Kyle and Kim",
        "gallery_position": 0,
        "season_number": 3,
    }

    monkeypatch.setattr(get_images_pipeline, "_collect_bravo_person", lambda *args, **kwargs: [castmate_row])
    monkeypatch.setattr(
        admin_person_images,
        "_download_bravotv_source_image",
        lambda _source_url: {
            "data": b"good-bytes",
            "content_type": "image/jpeg",
            "width": 1600,
            "height": 900,
            "size_bytes": 250_000,
        },
    )
    monkeypatch.setattr(
        get_images_pipeline,
        "_upload_bytes",
        lambda data, *, content_type=None: {
            "hosted_url": "https://cdn.example.com/hosted.jpg",
            "hosted_key": "shared/key.jpg",
            "hosted_sha256": "sha256",
            "hosted_content_type": content_type or "image/jpeg",
            "hosted_bytes": len(data),
            "hosted_etag": "etag",
            "hosted_at": "2026-03-21T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(
        run_service,
        "_import_supplemental_catalog",
        lambda **kwargs: pytest.fail("castmate rows should not be imported for the wrong person"),
    )

    result = _REAL_IMPORT_BRAVOTV_PERSON_MEDIA(
        MagicMock(),
        person_id="person-1",
        person_name="Brandi Glanville",
        show_name="The Real Housewives of Beverly Hills",
        limit=10,
    )

    assert result["fetched"] == 1
    assert result["imported"] == 0
    assert result["failed"] == 0
    assert result["attribution_skipped"] == 1
    assert result["skipped"] == 1


def test_import_bravotv_person_media_routes_episode_gallery_without_person_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.bravotv import get_images_pipeline, run_service

    episode_row = {
        "file_url": "https://cdn.example.com/portia.jpg",
        "file_name": "portia.jpg",
        "media_uuid": "media-portia",
        "gallery_path": "/the-real-housewives-of-beverly-hills/photos/portias-drama-filled-birthday-party",
        "gallery_item_id": "9799496",
        "source_page_url": (
            "https://www.bravotv.com/the-real-housewives-of-beverly-hills/photos/portias-drama-filled-birthday-party#9799496"
        ),
        "gallery_title": "Portia's Drama Filled Birthday Party",
        "gallery_show_name": "The Real Housewives of Beverly Hills",
        "gallery_people_names": ["Brandi Glanville", "Kyle Richards", "Adrienne Maloof"],
        "field_caption": "Kyle and Adrienne head out to find the proper gift for young Portia.",
        "field_media_image_alt": "Kyle and Adrienne shop for Portia's party.",
        "gallery_position": 0,
        "season_number": 3,
        "gallery_page_title": "Portia's Drama Filled Birthday Party",
    }

    monkeypatch.setattr(get_images_pipeline, "_collect_bravo_person", lambda *args, **kwargs: [episode_row])
    monkeypatch.setattr(
        admin_person_images,
        "_download_bravotv_source_image",
        lambda _source_url: {
            "data": b"good-bytes",
            "content_type": "image/jpeg",
            "width": 1600,
            "height": 900,
            "size_bytes": 250_000,
        },
    )
    monkeypatch.setattr(
        get_images_pipeline,
        "_upload_bytes",
        lambda data, *, content_type=None: {
            "hosted_url": "https://cdn.example.com/hosted.jpg",
            "hosted_key": "shared/key.jpg",
            "hosted_sha256": "sha256",
            "hosted_content_type": content_type or "image/jpeg",
            "hosted_bytes": len(data),
            "hosted_etag": "etag",
            "hosted_at": "2026-03-21T00:00:00+00:00",
        },
    )

    def _fake_import(
        *,
        supplemental_catalog: dict[str, object],
        **kwargs: object,
    ) -> tuple[dict[str, object], list[dict[str, str]]]:
        bravo_rows = supplemental_catalog.get("bravo")
        assert isinstance(bravo_rows, list)
        assert len(bravo_rows) == 1
        row = bravo_rows[0]
        assert row["link_person"] is False
        assert row["link_show"] is False
        assert row["link_season"] is True
        assert row["link_episode"] is True
        assert row["metadata"]["bravotv_gallery_classification"] == "episode_or_event_gallery"
        return (
            {"supplemental_assets_upserted": 1, "supplemental_links_created": 1},
            [{"media_asset_id": "asset-episode"}],
        )

    monkeypatch.setattr(run_service, "_import_supplemental_catalog", _fake_import)

    result = _REAL_IMPORT_BRAVOTV_PERSON_MEDIA(
        MagicMock(),
        person_id="person-1",
        person_name="Brandi Glanville",
        show_name="The Real Housewives of Beverly Hills",
        limit=10,
    )

    assert result["fetched"] == 1
    assert result["imported"] == 1
    assert result["failed"] == 0
    assert result["episode_routed"] == 1
    assert result["attribution_skipped"] == 0


def test_import_bravotv_person_media_skips_denylisted_gallery(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.bravotv import get_images_pipeline, run_service

    skipped_row = {
        "file_url": "https://cdn.example.com/home.jpg",
        "file_name": "home.jpg",
        "media_uuid": "media-home",
        "gallery_path": "/the-real-housewives-of-beverly-hills/photos/tour-brandi-glanvilles-home-and-closet",
        "source_page_url": (
            "https://www.bravotv.com/the-real-housewives-of-beverly-hills/photos/tour-brandi-glanvilles-home-and-closet"
        ),
        "gallery_title": "Tour Brandi Glanville's Home and Closet",
        "gallery_show_name": "The Real Housewives of Beverly Hills",
        "gallery_people_names": ["Brandi Glanville"],
        "field_caption": "Brandi opens the doors to her home.",
        "field_media_image_alt": "Brandi at home",
        "gallery_position": 0,
        "season_number": 3,
    }

    monkeypatch.setattr(get_images_pipeline, "_collect_bravo_person", lambda *args, **kwargs: [skipped_row])
    monkeypatch.setattr(
        admin_person_images,
        "_download_bravotv_source_image",
        lambda _source_url: {
            "data": b"good-bytes",
            "content_type": "image/jpeg",
            "width": 1600,
            "height": 900,
            "size_bytes": 250_000,
        },
    )
    monkeypatch.setattr(
        get_images_pipeline,
        "_upload_bytes",
        lambda *args, **kwargs: pytest.fail("denylisted Bravo gallery should not upload"),
    )
    monkeypatch.setattr(
        run_service,
        "_import_supplemental_catalog",
        lambda **kwargs: pytest.fail("denylisted Bravo gallery should not import"),
    )

    result = _REAL_IMPORT_BRAVOTV_PERSON_MEDIA(
        MagicMock(),
        person_id="person-1",
        person_name="Brandi Glanville",
        show_name="The Real Housewives of Beverly Hills",
        limit=10,
    )

    assert result["fetched"] == 1
    assert result["imported"] == 0
    assert result["failed"] == 0
    assert result["skip_gallery_count"] == 1
    assert result["skipped"] == 1


def test_auto_count_runtime_batch_size_caps_large_tagging_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_AUTO_COUNT_BATCH_SIZE_CAP", raising=False)

    assert admin_person_images._auto_count_runtime_batch_size(48) == 8


def test_auto_count_runtime_batch_size_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_AUTO_COUNT_BATCH_SIZE_CAP", "5")

    assert admin_person_images._auto_count_runtime_batch_size(48) == 5
    assert admin_person_images._auto_count_runtime_batch_size(3) == 3


def test_resolve_gallery_bucket_metadata_includes_event_subcategories() -> None:
    metadata = admin_person_images._resolve_gallery_bucket_metadata(
        asset={
            "event_name": "Bravo Launch Party Premiere Night",
            "title": "Bravo Launch Party Premiere Night",
            "caption": "Bravo cast attends the launch party premiere night.",
        },
        resolved_asset_show=None,
        show_lookup_by_alias={},
    )

    assert metadata["bucket_type"] == "event"
    assert metadata["event_subcategory_keys"] == [
        "premieres_red_carpet_screenings",
        "brand_launch_opening_social",
        "reality_tv_bravo_franchise",
    ]
    assert metadata["event_primary_subcategory_key"] == "premieres_red_carpet_screenings"


def test_resolve_gallery_bucket_metadata_prefers_explicit_getty_event_over_show_match() -> None:
    metadata = admin_person_images._resolve_gallery_bucket_metadata(
        asset={
            "event_name": 'Bravo\'s "The Real Housewives of Beverly Hills" Season 5 Premiere Party',
            "event_id": "rhobh-season-5-premiere",
            "event_url_slug": "rhobh-season-5-premiere",
            "title": 'Bravo\'s "The Real Housewives of Beverly Hills" Season 5 Premiere Party',
            "caption": "Brandi Glanville attends the RHOBH season 5 premiere party.",
            "grouped_image_count": 18,
        },
        resolved_asset_show={"id": "show-rhobh", "name": "The Real Housewives of Beverly Hills"},
        show_lookup_by_alias={
            "the real housewives of beverly hills": {
                "id": "show-rhobh",
                "name": "The Real Housewives of Beverly Hills",
            }
        },
    )

    assert metadata["bucket_type"] == "event"
    assert metadata["bucket_key"] == "bravo-s-the-real-housewives-of-beverly-hills-season-5-premiere-party"
    assert metadata["resolved_show_id"] is None
    assert metadata["resolved_show_name"] is None


def test_import_nbcumv_person_media_uses_wwhl_date_range_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    captured_grouped_searches: list[dict[str, object]] = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [])

    def _fake_grouped_search(phrase: str, **kwargs):
        captured_grouped_searches.append(
            {
                "phrase": phrase,
                "query_params": kwargs.get("query_params"),
                "source_query_scope": kwargs.get("source_query_scope"),
            }
        )
        return []

    monkeypatch.setattr(getty_integration, "search_grouped_events", _fake_grouped_search)
    monkeypatch.setattr(
        admin_person_images,
        "_load_person_wwhl_episode_air_dates_from_credits",
        lambda db, person_id: ["2022-06-27"],
    )
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: {"id": "show-wwhl", "title": "Watch What Happens Live with Andy Cohen"},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda *args, **kwargs: {"ok": True},
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=None,
        show_name="Watch What Happens Live with Andy Cohen",
        limit=10,
    )

    assert result["getty_wwhl_events"] == []
    assert captured_grouped_searches[2] == {
        "phrase": "Watch What Happens Live",
        "query_params": {
            "sort": "newest",
            "numberofpeople": "one,two",
            "begindate": "2022-06-25",
            "enddate": "2022-06-29",
            "recency": "daterange",
        },
        "source_query_scope": "wwhl_date_range",
    }


def test_import_nbcumv_person_media_persists_getty_unmatched_urls_and_imports_only_overlaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    person_id = str(uuid4())
    captured_snapshot: dict[str, object] = {}
    imported_items: list[object] = []
    imported_getty_rows: list[dict[str, object]] = []
    captured_searches: list[dict[str, object]] = []
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value.data
    ) = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)

    def _fake_import_single_item(*, db, item, assign_people, people_index):
        imported_items.append(item)
        return {
            "asset_id": str(uuid4()),
            "created_person_ids": [person_id],
            "created_show_ids": [],
            "already_imported": False,
        }

    monkeypatch.setattr(admin_nbcumv, "_import_single_item", _fake_import_single_item)
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: (
            captured_searches.append(
                {
                    "phrase": args[0] if args else None,
                    "query_params": kwargs.get("query_params"),
                }
            )
            or [
                {
                    "detail_url": "https://www.gettyimages.com/detail/news-photo/match/1",
                    "editorial_id": "1",
                    "object_name": "MATCH.JPG",
                    "title": "Matched Getty Asset",
                    "event_name": "BravoCon 2025 - Panels",
                    "caption": "BravoCon -- Pictured: Lisa Barlow -- (Photo by: Bravo / Contributor)",
                    "date_created": "November 16, 2025",
                    "original_image_url": "https://media.gettyimages.com/match-original.jpg",
                    "preview_image_url": "https://media.gettyimages.com/match-comp.jpg",
                    "keyword_texts": ["Lisa Barlow", "Season 6", "BravoCon"],
                    "details": {
                        "credit_display": "Bravo / Contributor",
                        "collection_display": "NBCUniversal",
                    },
                    "people": [{"text": "Lisa Barlow"}],
                },
                {
                    "detail_url": "https://www.gettyimages.com/detail/news-photo/unmatched/2",
                    "editorial_id": "2",
                    "object_name": "UNMATCHED.JPG",
                    "title": "Unmatched Getty Asset",
                    "event_name": "DIRECTV Plot Twist Featuring Bravo",
                    "original_image_url": "https://media.gettyimages.com/unmatched-original.jpg",
                    "preview_image_url": "https://media.gettyimages.com/unmatched-comp.jpg",
                    "thumb_url": "https://media.gettyimages.com/unmatched-thumb.jpg",
                    "people": [{"text": "Lisa Barlow"}],
                },
            ]
        ),
    )

    def _fake_fetch_image_by_identity(*, filename=None, lbx_id=None):
        if filename == "MATCH.JPG":
            return {
                "lbx_id": "70761513",
                "lbx_filename": "MATCH.JPG",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/match.jpg",
                "showIds": ["show-1"],
                "lbx_showTitle": "The Real Housewives of Salt Lake City",
            }
        return None

    monkeypatch.setattr(nbcumv_integration, "fetch_image_by_identity", _fake_fetch_image_by_identity)
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )

    def _fake_persist_snapshot(db, *, person_id, payload, status="success", error=None):
        captured_snapshot["person_id"] = person_id
        captured_snapshot["payload"] = payload
        captured_snapshot["status"] = status
        return {"person_id": person_id, "source_id": "getty", "variant": "person_gallery_nbcumv_crosswalk"}

    monkeypatch.setattr(admin_person_images, "_persist_person_getty_snapshot", _fake_persist_snapshot)

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        mock_db,
        person_id=person_id,
        person_name="Lisa Barlow",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert result["fetched"] == 2
    assert result["imported"] == 1
    assert result["failed"] == 0
    assert result["getty_candidates_total"] == 2
    assert result["getty_matched_total"] == 1
    assert result["getty_unmatched_total"] == 1
    assert result["shared_nbcumv_total"] == 1
    assert result["shared_nbcumv_imported"] == 1
    assert result["nbcumv_only_total"] == 0
    assert result["nbcumv_only_imported"] == 0
    assert result["getty_only_imported"] == 1
    assert result["getty_snapshot_saved"] is True
    assert len(imported_items) == 1
    assert imported_items[0].lbx_filename == "MATCH.JPG"
    assert imported_items[0].gallery_bucket["source_resolution"] == "nbcumv_preferred_shared"
    assert imported_items[0].getty_asset is not None
    assert imported_items[0].getty_asset["editorial_id"] == "1"
    assert imported_items[0].getty_asset["keyword_texts"] == ["Lisa Barlow", "Season 6", "BravoCon"]
    assert imported_items[0].getty_asset["details"]["credit_display"] == "Bravo / Contributor"
    assert len(imported_getty_rows) == 1
    assert imported_getty_rows[0]["source"] == "getty"
    assert imported_getty_rows[0]["source_image_id"] == "2"
    assert imported_getty_rows[0]["url"] == "https://media.gettyimages.com/unmatched-original.jpg?s=2048x2048&w=gi"
    assert imported_getty_rows[0]["original_url"] == "https://media.gettyimages.com/unmatched-original.jpg"
    assert imported_getty_rows[0]["source_page_url"] == "https://www.gettyimages.com/detail/news-photo/unmatched/2"
    assert imported_getty_rows[0]["metadata"]["getty_only_fallback"] is True
    assert imported_getty_rows[0]["metadata"]["getty"]["editorial_id"] == "2"
    assert (
        imported_getty_rows[0]["metadata"]["getty_original_image_url"]
        == "https://media.gettyimages.com/unmatched-original.jpg"
    )
    assert (
        imported_getty_rows[0]["metadata"]["getty_preview_image_url"]
        == "https://media.gettyimages.com/unmatched-comp.jpg"
    )
    assert imported_getty_rows[0]["metadata"]["bucket_type"] == "event"
    assert imported_getty_rows[0]["metadata"]["bucket_label"] == "DIRECTV Plot Twist Featuring Bravo"
    assert imported_items[0].gallery_bucket["bucket_type"] == "bravocon"
    assert captured_searches == [{"phrase": "Lisa Barlow", "query_params": None}]

    payload = captured_snapshot["payload"]
    assert isinstance(payload, dict)
    assert payload["candidate_count"] == 2
    assert payload["raw_candidate_count"] == 2
    assert payload["matched_count"] == 1
    assert payload["unmatched_count"] == 1
    assert payload["matched"][0]["object_name"] == "MATCH.JPG"
    assert payload["unmatched"][0]["detail_url"] == "https://www.gettyimages.com/detail/news-photo/unmatched/2"
    assert payload["unmatched"][0]["reason"] == "no_nbcumv_match"


def test_import_nbcumv_person_media_uses_show_index_crosswalk_when_filename_search_misses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    person_id = str(uuid4())
    imported_items: list[object] = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)

    def _fake_import_single_item(*, db, item, assign_people, people_index):
        imported_items.append(item)
        return {
            "asset_id": str(uuid4()),
            "created_person_ids": [person_id],
            "created_show_ids": [],
            "already_imported": False,
        }

    monkeypatch.setattr(admin_nbcumv, "_import_single_item", _fake_import_single_item)
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/rhoslc/1",
                "editorial_id": "2254325635",
                "object_name": "NUP_209430_00480.jpg",
                "title": "The Real Housewives of Salt Lake City - Season 6",
                "event_name": 'UT: BRAVO\'S "The Real Housewives of Salt Lake City" - Season 6',
                "caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "Reunion" -- Pictured: Lisa Barlow',
            }
        ],
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: (
            {
                "id": "show-rhoslc",
                "title": "The Real Housewives of Salt Lake City",
            }
            if "salt lake city" in str(title).lower()
            else None
        ),
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "build_show_image_index",
        lambda show_id: {
            "nup_209430_00480.jpg": {
                "lbx_id": "70075355",
                "lbx_filename": "NUP_209430_00480.jpg",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/rhoslc.jpg",
                "showIds": [show_id],
                "lbx_showTitle": "Real Housewives of Salt Lake City, The",
            }
        },
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "fetch_image_by_identity",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=person_id,
        person_name="Lisa Barlow",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert result["fetched"] == 1
    assert result["imported"] == 1
    assert result["getty_matched_total"] == 1
    assert result["getty_unmatched_total"] == 0
    assert len(imported_items) == 1
    assert imported_items[0].lbx_id == "70075355"
    assert imported_items[0].gallery_bucket["bucket_type"] == "event"
    assert imported_items[0].gallery_bucket["resolved_show_name"] is None


def test_import_nbcumv_person_media_repairs_existing_shared_getty_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    person_id = str(uuid4())
    original_url = (
        "https://media.gettyimages.com/id/1435767826/photo/legends-ball-2022-bravocon.jpg"
        "?s=2048x2048&w=gi&k=20&c=WvPQ9UDMOcuYz0FjoJhESs1VlsuQd41CmLoHCRVCRDU="
    )
    preview_url = (
        "https://media.gettyimages.com/id/1435767826/photo/legends-ball-2022-bravocon.jpg"
        "?p=1&s=594x594&w=gi&k=20&c=qm3GOG53fvQgAxq82lriZGbdZ_rzQZWtiq59vsjszbs="
    )
    cast_updates: list[dict[str, object]] = []
    asset_updates: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.action = "select"
            self.filters: dict[str, object] = {}
            self.payload: dict[str, object] | None = None

        def select(self, _columns: str):
            self.action = "select"
            return self

        def update(self, payload: dict[str, object]):
            self.action = "update"
            self.payload = dict(payload)
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def execute(self):
            if self.table_name == "media_links" and self.action == "select":
                if self.filters.get("media_assets.source") == "nbcumv":
                    return _Response([{"id": "existing-nbcumv-link"}])
                return _Response([{"media_asset_id": "asset-1"}])
            if self.table_name == "cast_photos" and self.action == "select":
                return _Response(
                    [
                        {
                            "id": "cast-row-1",
                            "source_image_id": "1435767826",
                            "url": preview_url,
                            "image_url": None,
                            "image_url_canonical": None,
                            "thumb_url": None,
                            "source_page_url": "https://www.gettyimages.com/detail/news-photo/old/1435767826",
                            "width": 594,
                            "height": 594,
                            "hosted_url": original_url,
                            "hosted_key": None,
                        }
                    ]
                )
            if self.table_name == "cast_photos" and self.action == "update":
                cast_updates.append(dict(self.payload or {}))
                return _Response([{"id": self.filters.get("id")}])
            if self.table_name == "media_assets" and self.action == "select":
                return _Response(
                    [
                        {
                            "id": "asset-1",
                            "source_url": preview_url,
                            "width": 594,
                            "height": 594,
                            "metadata": {},
                        }
                    ]
                )
            if self.table_name == "media_assets" and self.action == "update":
                asset_updates.append(dict(self.payload or {}))
                return _Response([{"id": self.filters.get("id")}])
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_persist_person_getty_snapshot", lambda *args, **kwargs: {})
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(getty_integration, "search_grouped_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/1435767826",
                "editorial_id": "1435767826",
                "object_name": "NUP_1435767826.JPG",
                "preview_image_url": preview_url,
                "original_image_url": original_url,
                "assetDimensions": {"width": 2048, "height": 2048},
                "event_name": "Legends Ball 2022 - BravoCon",
                "caption": "Brandi Glanville attends BravoCon.",
                "people": [{"text": "Brandi Glanville"}],
            }
        ],
    )
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id: {})
    monkeypatch.setattr(
        nbcumv_integration,
        "fetch_image_by_identity",
        lambda **kwargs: {
            "lbx_id": "70761513",
            "lbx_filename": "NUP_1435767826.JPG",
            "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/match.jpg",
            "showIds": ["show-1"],
            "lbx_showTitle": "The Real Housewives of Beverly Hills",
        },
    )
    monkeypatch.setattr(
        admin_nbcumv,
        "_import_single_item",
        lambda **kwargs: {
            "asset_id": "nbcumv-asset-1",
            "created_person_ids": [],
            "created_show_ids": [],
            "already_imported": True,
            "metadata_upgraded": True,
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=person_id,
        person_name="Brandi Glanville",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert result["shared_nbcumv_total"] == 1
    assert result["shared_nbcumv_existing"] == 1
    assert result["upgraded_existing"] == 1
    assert result["getty_repair_row_ids"] == ["cast-row-1"]
    assert result["getty_repair_media_asset_ids"] == ["asset-1"]
    assert cast_updates
    assert cast_updates[0]["url"] == original_url
    assert cast_updates[0]["image_url"] == original_url
    assert cast_updates[0]["image_url_canonical"] == original_url.split("?", 1)[0]
    assert cast_updates[0]["hosted_url"] is None
    assert cast_updates[0]["hosted_key"] is None
    assert asset_updates
    assert asset_updates[0]["source_url"] == original_url
    assert asset_updates[0]["hosted_url"] is None
    assert asset_updates[0]["ingest_status"] == "pending"
    assert asset_updates[0]["metadata"]["getty_original_image_url"] == original_url


def test_import_nbcumv_person_media_resets_stale_hosted_getty_asset_when_mirrored_from_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    person_id = str(uuid4())
    original_url = (
        "https://media.gettyimages.com/id/1435767826/photo/legends-ball-2022-bravocon.jpg"
        "?s=2048x2048&w=gi&k=20&c=WvPQ9UDMOcuYz0FjoJhESs1VlsuQd41CmLoHCRVCRDU="
    )
    preview_url = (
        "https://media.gettyimages.com/id/1435767826/photo/legends-ball-2022-bravocon.jpg"
        "?p=1&s=594x594&w=gi&k=20&c=qm3GOG53fvQgAxq82lriZGbdZ_rzQZWtiq59vsjszbs="
    )
    asset_updates: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.action = "select"
            self.filters: dict[str, object] = {}
            self.payload: dict[str, object] | None = None

        def select(self, _columns: str):
            self.action = "select"
            return self

        def update(self, payload: dict[str, object]):
            self.action = "update"
            self.payload = dict(payload)
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def execute(self):
            if self.table_name == "media_links" and self.action == "select":
                if self.filters.get("media_assets.source") == "nbcumv":
                    return _Response([{"id": "existing-nbcumv-link"}])
                return _Response([{"media_asset_id": "asset-1"}])
            if self.table_name == "cast_photos" and self.action == "select":
                return _Response(
                    [
                        {
                            "id": "cast-row-1",
                            "source_image_id": "1435767826",
                            "url": original_url,
                            "image_url": original_url,
                            "image_url_canonical": original_url.split("?", 1)[0],
                            "thumb_url": None,
                            "source_page_url": "https://www.gettyimages.com/detail/news-photo/1435767826",
                            "width": 2048,
                            "height": 2048,
                            "hosted_url": "https://cdn.example.com/media/old-preview.jpg",
                            "hosted_key": "media/old-preview.jpg",
                            "metadata": {"getty_original_image_url": original_url},
                        }
                    ]
                )
            if self.table_name == "media_assets" and self.action == "select":
                return _Response(
                    [
                        {
                            "id": "asset-1",
                            "source_url": original_url,
                            "hosted_url": "https://cdn.example.com/media/old-preview.jpg",
                            "hosted_key": "media/old-preview.jpg",
                            "width": 2048,
                            "height": 2048,
                            "metadata": {"mirrored_from": preview_url},
                        }
                    ]
                )
            if self.table_name == "media_assets" and self.action == "update":
                asset_updates.append(dict(self.payload or {}))
                return _Response([{"id": self.filters.get("id")}])
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_persist_person_getty_snapshot", lambda *args, **kwargs: {})
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(getty_integration, "search_grouped_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/1435767826",
                "editorial_id": "1435767826",
                "object_name": "NUP_1435767826.JPG",
                "preview_image_url": preview_url,
                "original_image_url": original_url,
                "assetDimensions": {"width": 2048, "height": 2048},
                "event_name": "Legends Ball 2022 - BravoCon",
                "caption": "Brandi Glanville attends BravoCon.",
                "people": [{"text": "Brandi Glanville"}],
            }
        ],
    )
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id: {})
    monkeypatch.setattr(
        nbcumv_integration,
        "fetch_image_by_identity",
        lambda **kwargs: {
            "lbx_id": "70761513",
            "lbx_filename": "NUP_1435767826.JPG",
            "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/match.jpg",
            "showIds": ["show-1"],
            "lbx_showTitle": "The Real Housewives of Beverly Hills",
        },
    )
    monkeypatch.setattr(
        admin_nbcumv,
        "_import_single_item",
        lambda **kwargs: {
            "asset_id": "nbcumv-asset-1",
            "created_person_ids": [],
            "created_show_ids": [],
            "already_imported": True,
            "metadata_upgraded": True,
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=person_id,
        person_name="Brandi Glanville",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert result["shared_nbcumv_total"] == 1
    assert result["getty_repair_media_asset_ids"] == ["asset-1"]
    assert asset_updates
    assert any(update.get("hosted_url") is None for update in asset_updates)
    assert any(update.get("hosted_key") is None for update in asset_updates)
    assert any(update.get("ingest_status") == "pending" for update in asset_updates)


def test_sync_cast_gallery_rows_to_media_assets_resets_stale_getty_hosted_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_url = "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=2048x2048&w=gi"
    preview_url = "https://media.gettyimages.com/id/1435767826/photo/example.jpg?p=1&s=594x594&w=gi"
    asset_updates: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.action = "select"
            self.payload: dict[str, object] | None = None

        def select(self, _columns: str):
            self.action = "select"
            return self

        def update(self, payload: dict[str, object]):
            self.action = "update"
            self.payload = dict(payload)
            return self

        def in_(self, _key: str, _values: list[object]):
            return self

        def eq(self, _key: str, _value: object):
            return self

        def execute(self):
            if self.table_name == "media_assets" and self.action == "select":
                return _Response(
                    [
                        {
                            "id": "asset-1",
                            "source": "getty",
                            "source_url": original_url,
                            "hosted_url": "https://cdn.example.com/media/old-preview.jpg",
                            "hosted_key": "media/old-preview.jpg",
                            "metadata": {"mirrored_from": preview_url},
                        }
                    ]
                )
            if self.table_name == "media_assets" and self.action == "update":
                asset_updates.append(dict(self.payload or {}))
                return _Response([{"id": "asset-1"}])
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(
        "trr_backend.repositories.media_assets.transform_cast_photos_to_media",
        lambda rows: (
            [
                {
                    "id": "asset-1",
                    "source": "getty",
                    "source_url": original_url,
                    "width": 2048,
                    "height": 2048,
                    "metadata": {"getty_original_image_url": original_url},
                }
            ],
            [
                {
                    "id": "link-1",
                    "entity_type": "person",
                    "entity_id": "person-1",
                    "media_asset_id": "asset-1",
                    "kind": "gallery",
                    "context": {},
                    "is_primary": False,
                }
            ],
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.media_assets.reconcile_media_asset_id_conflicts",
        lambda db, assets, links: (assets, links),
    )
    monkeypatch.setattr("trr_backend.repositories.media_assets.upsert_media_assets", lambda db, assets: list(assets))
    monkeypatch.setattr("trr_backend.repositories.media_assets.upsert_media_links", lambda db, links: list(links))

    admin_person_images._sync_cast_gallery_rows_to_media_assets(
        _Db(),
        [
            {
                "id": "cast-row-1",
                "person_id": "person-1",
                "source": "getty",
                "source_image_id": "1435767826",
                "image_url": original_url,
                "image_url_canonical": original_url.split("?", 1)[0],
                "metadata": {"getty_original_image_url": original_url},
            }
        ],
    )

    assert asset_updates == [
        {
            "source_url": original_url,
            "sha256": None,
            "hosted_bucket": None,
            "hosted_key": None,
            "hosted_url": None,
            "hosted_sha256": None,
            "hosted_content_type": None,
            "hosted_bytes": None,
            "hosted_etag": None,
            "hosted_at": None,
            "ingest_status": "pending",
            "ingest_last_error": None,
            "ingest_retry_count": 0,
            "ingest_failed_at": None,
            "ingest_completed_at": None,
            "ingest_next_retry_at": None,
            "width": 2048,
            "height": 2048,
            "metadata": {"getty_original_image_url": original_url},
        }
    ]


def test_import_nbcumv_person_media_aborts_when_both_direct_getty_searches_return_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(
        admin_person_images,
        "_load_person_credit_show_catalog",
        lambda db, person_id: [{"networks": ["Bravo"], "streaming_providers": []}],
    )
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [])
    grouped_event_calls = {"count": 0}

    def _fake_grouped_events(*args, **kwargs):
        grouped_event_calls["count"] += 1
        return []

    monkeypatch.setattr(getty_integration, "search_grouped_events", _fake_grouped_events)
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=str(uuid4()),
        person_name="Mary Cosby",
        show_id=None,
        show_name=None,
        limit=25,
    )

    assert result["getty_candidates_total"] == 0
    assert result["getty_matched_total"] == 0
    assert result["getty_unmatched_total"] == 0
    assert result["getty_only_imported"] == 0
    assert result["getty_search_attempted"] is True
    assert result["getty_initial_search_zero_abort"] is True
    assert result["getty_initial_search_queries"] == ["Mary Cosby Bravo", "Mary Cosby"]
    assert result["getty_initial_search_counts"] == {"Mary Cosby Bravo": 0, "Mary Cosby": 0}
    assert result["getty_zero_result_reason"] is None
    assert result["errors"] == []
    assert "stopped refresh early" in str(result["summary_message"]).lower()
    assert grouped_event_calls["count"] == 0


def test_import_nbcumv_person_media_marks_direct_getty_searches_as_warning_when_getty_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    progress_events: list[dict[str, object]] = []

    def _fake_search_editorial_assets(*args, diagnostics_out=None, **kwargs):
        if isinstance(diagnostics_out, dict):
            diagnostics_out.update(
                {
                    "status": "unavailable",
                    "failure_stage": "search",
                    "unavailable_reason": "challenge_page",
                    "http_status": 200,
                    "page_classification": "challenge_page",
                }
            )
        return []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(
        admin_person_images,
        "_load_person_credit_show_catalog",
        lambda db, person_id: [{"networks": ["Bravo"], "streaming_providers": []}],
    )
    monkeypatch.setattr(getty_integration, "search_editorial_assets", _fake_search_editorial_assets)
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=None,
        show_name=None,
        limit=25,
        getty_progress_cb=progress_events.append,
    )

    primary_progress = next(
        event for event in reversed(progress_events) if event.get("subtask_id") == "primary_person_search"
    )
    fallback_progress = next(
        event for event in reversed(progress_events) if event.get("subtask_id") == "fallback_person_search"
    )

    assert result["getty_initial_search_zero_abort"] is True
    assert result["getty_access_mode"] == "live_modal_unavailable"
    assert result["getty_unavailable_reason"] == "challenge_page"
    assert primary_progress["subtask_status"] == "warning"
    assert fallback_progress["subtask_status"] == "warning"
    assert "Getty unavailable during direct person search" in str(primary_progress["message"])
    assert "challenge page" in str(primary_progress["message"])
    assert "HTTP 200" in str(primary_progress["message"])
    assert "Getty unavailable during direct person search" in str(result["summary_message"])


def test_import_nbcumv_person_media_falls_back_to_direct_nbcumv_caption_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_items: list[object] = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: {"id": "show-rhoslc", "title": "The Real Housewives of Salt Lake City"},
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "search_person_show_catalog",
        lambda person_name, show_id, limit=100, session=None: [
            {
                "lbx_id": "70075355",
                "lbx_filename": "DIRECT_MATCH.JPG",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/direct-match.jpg",
                "showIds": ["show-rhoslc"],
                "lbx_showTitle": "The Real Housewives of Salt Lake City",
                "lbx_headline": "The Real Housewives of Salt Lake City - After Show",
                "lbx_caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "After Show" -- Pictured: Mary Cosby',
            }
        ],
    )
    monkeypatch.setattr(nbcumv_integration, "search_images", lambda filters, session=None: [])

    def _fake_import_single_item(*, db, item, assign_people, people_index):
        imported_items.append(item)
        return {
            "asset_id": str(uuid4()),
            "created_person_ids": [item.person_ids[0]] if item.person_ids else [],
            "created_show_ids": [],
            "already_imported": False,
        }

    monkeypatch.setattr(admin_nbcumv, "_import_single_item", _fake_import_single_item)

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=str(uuid4()),
        person_name="Mary Cosby",
        show_id=None,
        show_name="The Real Housewives of Salt Lake City",
        limit=10,
    )

    assert result["getty_candidates_total"] == 0
    assert result["getty_matched_total"] == 0
    assert result["getty_unmatched_total"] == 0
    assert result["shared_nbcumv_total"] == 0
    assert result["shared_nbcumv_imported"] == 0
    assert result["nbcumv_only_total"] == 1
    assert result["nbcumv_only_imported"] == 1
    assert result["getty_only_imported"] == 0
    assert result["fetched"] == 1
    assert result["imported"] == 1
    assert len(imported_items) == 1
    assert imported_items[0].lbx_filename == "DIRECT_MATCH.JPG"
    assert imported_items[0].gallery_bucket["bucket_type"] == "event"
    assert imported_items[0].gallery_bucket["resolved_show_name"] is None
    assert imported_items[0].gallery_bucket["source_resolution"] == "nbcumv_only"
    assert "direct caption search" in str(result["summary_message"]).lower()


def test_import_nbcumv_person_media_times_out_stuck_asset_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_filenames: list[str] = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: {"id": "show-rhoslc", "title": "The Real Housewives of Salt Lake City"},
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "search_person_show_catalog",
        lambda person_name, show_id, limit=100, session=None: [
            {
                "lbx_id": "70075355",
                "lbx_filename": "TIMEOUT.JPG",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/timeout.jpg",
                "showIds": ["show-rhoslc"],
                "lbx_showTitle": "The Real Housewives of Salt Lake City",
                "lbx_headline": "The Real Housewives of Salt Lake City - Timeout",
                "lbx_caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "Timeout" -- Pictured: Mary Cosby',
            },
            {
                "lbx_id": "70075356",
                "lbx_filename": "RECOVERED.JPG",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/recovered.jpg",
                "showIds": ["show-rhoslc"],
                "lbx_showTitle": "The Real Housewives of Salt Lake City",
                "lbx_headline": "The Real Housewives of Salt Lake City - Recovered",
                "lbx_caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "Recovered" -- Pictured: Mary Cosby',
            },
        ],
    )
    monkeypatch.setattr(nbcumv_integration, "search_images", lambda filters, session=None: [])
    monkeypatch.setattr(admin_person_images, "_resolve_nbcumv_import_item_timeout_seconds", lambda: 0.01)

    def _fake_import_single_item(*, db, item, assign_people, people_index):
        if item.lbx_filename == "TIMEOUT.JPG":
            time.sleep(0.05)
            return {
                "asset_id": str(uuid4()),
                "created_person_ids": [str(item.person_ids[0])] if item.person_ids else [],
                "created_show_ids": [],
                "already_imported": False,
            }
        imported_filenames.append(item.lbx_filename)
        return {
            "asset_id": str(uuid4()),
            "created_person_ids": [str(item.person_ids[0])] if item.person_ids else [],
            "created_show_ids": [],
            "already_imported": False,
        }

    monkeypatch.setattr(admin_nbcumv, "_import_single_item", _fake_import_single_item)

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=str(uuid4()),
        person_name="Mary Cosby",
        show_id=None,
        show_name="The Real Housewives of Salt Lake City",
        limit=10,
    )

    assert result["cancelled"] is False
    assert result["failed"] == 2
    assert result["imported"] == 0
    assert result["nbcumv_only_total"] == 2
    assert result["nbcumv_only_imported"] == 0
    assert imported_filenames == []
    assert any("timed out" in str(error).lower() for error in result["errors"])


def test_import_nbcumv_person_media_honors_cancel_between_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_filenames: list[str] = []
    cancel_checks = {"count": 0}

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: {"id": "show-rhoslc", "title": "The Real Housewives of Salt Lake City"},
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "search_person_show_catalog",
        lambda person_name, show_id, limit=100, session=None: [
            {
                "lbx_id": "70075355",
                "lbx_filename": "FIRST.JPG",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/first.jpg",
                "showIds": ["show-rhoslc"],
                "lbx_showTitle": "The Real Housewives of Salt Lake City",
                "lbx_headline": "The Real Housewives of Salt Lake City - First",
                "lbx_caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "First" -- Pictured: Lisa Barlow',
            },
            {
                "lbx_id": "70075356",
                "lbx_filename": "SECOND.JPG",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/second.jpg",
                "showIds": ["show-rhoslc"],
                "lbx_showTitle": "The Real Housewives of Salt Lake City",
                "lbx_headline": "The Real Housewives of Salt Lake City - Second",
                "lbx_caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "Second" -- Pictured: Lisa Barlow',
            },
        ],
    )
    monkeypatch.setattr(nbcumv_integration, "search_images", lambda filters, session=None: [])

    def _fake_import_single_item(*, db, item, assign_people, people_index):
        imported_filenames.append(item.lbx_filename)
        return {
            "asset_id": str(uuid4()),
            "created_person_ids": [str(item.person_ids[0])] if item.person_ids else [],
            "created_show_ids": [],
            "already_imported": False,
        }

    def _cancel_requested() -> bool:
        cancel_checks["count"] += 1
        return cancel_checks["count"] >= 2

    monkeypatch.setattr(admin_nbcumv, "_import_single_item", _fake_import_single_item)

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=str(uuid4()),
        person_name="Lisa Barlow",
        show_id=None,
        show_name="The Real Housewives of Salt Lake City",
        limit=10,
        cancel_requested_cb=_cancel_requested,
    )

    assert result["cancelled"] is True
    assert result["failed"] == 0
    assert result["imported"] == 1
    assert result["nbcumv_only_total"] == 2
    assert result["nbcumv_only_imported"] == 1
    assert imported_filenames == ["FIRST.JPG"]
    assert "cancellation requested" in str(result["summary_message"]).lower()


def test_import_nbcumv_person_media_uses_credited_shows_for_direct_nbcumv_search_without_request_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_items: list[object] = []
    searched_show_ids: list[str | None] = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        admin_person_images,
        "_load_person_credit_show_names",
        lambda db, person_id: [
            "The Real Housewives of Salt Lake City",
            "Watch What Happens Live with Andy Cohen",
        ],
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: (
            {"id": "show-rhoslc", "title": "The Real Housewives of Salt Lake City"}
            if "salt lake city" in str(title).lower()
            else {"id": "show-wwhl", "title": "Watch What Happens Live with Andy Cohen"}
            if "watch what happens live" in str(title).lower()
            else None
        ),
    )
    monkeypatch.setattr(nbcumv_integration, "discover_person_show_titles", lambda *args, **kwargs: [])

    def _fake_search_person_show_catalog(person_name, *, show_id, limit=100, session=None):
        searched_show_ids.append(show_id)
        if show_id == "show-rhoslc":
            return [
                {
                    "lbx_id": "70075355",
                    "lbx_filename": "DIRECT_MATCH.JPG",
                    "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/direct-match.jpg",
                    "showIds": ["show-rhoslc"],
                    "lbx_showTitle": "The Real Housewives of Salt Lake City",
                    "lbx_headline": "The Real Housewives of Salt Lake City - After Show",
                    "lbx_caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "After Show" -- Pictured: Lisa Barlow',
                }
            ]
        return []

    monkeypatch.setattr(nbcumv_integration, "search_person_show_catalog", _fake_search_person_show_catalog)
    monkeypatch.setattr(
        nbcumv_integration,
        "search_person_images",
        lambda person_name, *, show_id=None, limit=100, session=None: searched_show_ids.append(show_id) or [],
    )
    monkeypatch.setattr(nbcumv_integration, "search_images", lambda filters, session=None: [])

    def _fake_import_single_item(*, db, item, assign_people, people_index):
        imported_items.append(item)
        return {
            "asset_id": str(uuid4()),
            "created_person_ids": [item.person_ids[0]] if item.person_ids else [],
            "created_show_ids": [],
            "already_imported": False,
        }

    monkeypatch.setattr(admin_nbcumv, "_import_single_item", _fake_import_single_item)

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=str(uuid4()),
        person_name="Lisa Barlow",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert searched_show_ids == ["show-rhoslc", "show-wwhl", None]
    assert result["getty_candidates_total"] == 0
    assert result["getty_matched_total"] == 0
    assert result["getty_unmatched_total"] == 0
    assert result["shared_nbcumv_total"] == 0
    assert result["shared_nbcumv_imported"] == 0
    assert result["nbcumv_only_total"] == 1
    assert result["nbcumv_only_imported"] == 1
    assert result["getty_only_imported"] == 0
    assert result["fetched"] == 1
    assert result["imported"] == 1
    assert len(imported_items) == 1
    assert imported_items[0].lbx_filename == "DIRECT_MATCH.JPG"
    assert imported_items[0].gallery_bucket["bucket_type"] == "event"
    assert imported_items[0].gallery_bucket["resolved_show_name"] is None
    assert imported_items[0].gallery_bucket["source_resolution"] == "nbcumv_only"
    assert "direct caption search" in str(result["summary_message"]).lower()


def test_import_nbcumv_person_media_supplements_getty_matches_with_all_nbcumv_caption_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_items: list[object] = []
    searched_show_ids: list[str | None] = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "editorial_id": "2246511440",
                "object_name": "NUP_209171_01723.JPG",
                "title": "BravoCon 2025",
                "event_name": 'NV: Bravo\'s "BravoCon 2025" - Day 3',
                "caption": "BravoCon -- Pictured: Lisa Barlow",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/bravocon/2246511440",
            }
        ],
    )
    monkeypatch.setattr(getty_integration, "search_grouped_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(admin_person_images, "_load_person_credit_show_names", lambda db, person_id: [])
    monkeypatch.setattr(nbcumv_integration, "discover_person_show_titles", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: {"id": "show-bravocon", "title": "BravoCon 2025"} if "bravocon" in str(title).lower() else None,
    )
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id, session=None: {})
    monkeypatch.setattr(
        nbcumv_integration,
        "fetch_image_by_identity",
        lambda *, filename=None, lbx_id=None, show_id=None, session=None: (
            {
                "lbx_id": "70080001",
                "lbx_filename": "NUP_209171_01723.JPG",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/getty-match.jpg",
                "showIds": ["show-bravocon"],
                "lbx_showTitle": "BravoCon 2025",
                "lbx_caption": "BRAVOCON -- Pictured: Lisa Barlow",
            }
            if filename == "NUP_209171_01723.JPG"
            else None
        ),
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "search_person_show_catalog",
        lambda person_name, *, show_id, limit=100, session=None: searched_show_ids.append(show_id) or [],
    )

    def _fake_search_person_images(person_name, *, show_id=None, limit=100, session=None):
        searched_show_ids.append(show_id)
        if show_id:
            return []
        return [
            {
                "lbx_id": "70090001",
                "lbx_filename": "DIRECT_EVENT_MATCH.JPG",
                "location": "https://lightbox-thumbnails.s3.us-west-2.amazonaws.com/direct-event.jpg",
                "showIds": ["show-random-event"],
                "lbx_showTitle": "People's Choice Awards",
                "lbx_headline": "People's Choice Awards 2025",
                "grouped_image_count": 4,
                "lbx_caption": "PEOPLE'S CHOICE AWARDS -- Pictured: Lisa Barlow",
            }
        ]

    monkeypatch.setattr(nbcumv_integration, "search_person_images", _fake_search_person_images)
    monkeypatch.setattr(nbcumv_integration, "search_images", lambda filters, session=None: [])

    def _fake_import_single_item(*, db, item, assign_people, people_index):
        imported_items.append(item)
        return {
            "asset_id": str(uuid4()),
            "created_person_ids": [item.person_ids[0]] if item.person_ids else [],
            "created_show_ids": [],
            "already_imported": False,
        }

    monkeypatch.setattr(admin_nbcumv, "_import_single_item", _fake_import_single_item)

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        MagicMock(),
        person_id=str(uuid4()),
        person_name="Lisa Barlow",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert searched_show_ids == [None]
    assert result["getty_candidates_total"] == 1
    assert result["getty_matched_total"] == 1
    assert result["shared_nbcumv_total"] == 1
    assert result["shared_nbcumv_imported"] == 1
    assert result["nbcumv_only_total"] == 1
    assert result["nbcumv_only_imported"] == 1
    assert result["fetched"] == 2
    assert result["imported"] == 2
    assert len(imported_items) == 2
    assert {item.lbx_filename for item in imported_items} == {
        "NUP_209171_01723.JPG",
        "DIRECT_EVENT_MATCH.JPG",
    }
    direct_item = next(item for item in imported_items if item.lbx_filename == "DIRECT_EVENT_MATCH.JPG")
    shared_item = next(item for item in imported_items if item.lbx_filename == "NUP_209171_01723.JPG")
    assert direct_item.gallery_bucket["bucket_type"] == "event"
    assert direct_item.gallery_bucket["bucket_label"] == "People's Choice Awards 2025"
    assert direct_item.gallery_bucket["grouped_image_count"] == 4
    assert direct_item.gallery_bucket["source_resolution"] == "nbcumv_only"
    assert shared_item.gallery_bucket["source_resolution"] == "nbcumv_preferred_shared"
    assert "shared via nbcumv" in str(result["summary_message"]).lower()
    assert "nbcumv-only" in str(result["summary_message"]).lower()


def test_import_nbcumv_person_media_imports_getty_fallback_when_nbcumv_is_unauthorized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_getty_rows: list[dict[str, object]] = []
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value.data
    ) = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/bravocon/2246511440",
                "editorial_id": "2246511440",
                "object_name": "NUP_209171_01723.JPG",
                "title": "BravoCon 2025",
                "event_name": 'NV: Bravo\'s "BravoCon 2025" - Day 3',
                "caption": "BravoCon -- Pictured: Lisa Barlow and Meredith Marks",
                "preview_image_url": "https://media.gettyimages.com/id/2246511440/photo/sample.jpg",
                "downloadableCompUrl": (
                    "https://media.gettyimages.com/id/2246511440/photo/"
                    "watch-what-happens-live-with-andy-cohen-season-20.jpg?p=1&s=594x594&w=gi&k=small"
                ),
                "galleryHighResCompUrl": (
                    "https://media.gettyimages.com/id/2246511440/photo/"
                    "watch-what-happens-live-with-andy-cohen-season-20.jpg?p=1&w=gi&k=large"
                ),
                "thumb_url": "https://media.gettyimages.com/id/2246511440/photo/sample-thumb.jpg",
                "keyword_texts": ["Lisa Barlow", "Two People", "BravoCon"],
                "people": [{"text": "Lisa Barlow"}],
                "details": {
                    "object_name_display": "NUP_209171_01723.JPG",
                    "credit_display": "Bravo / Contributor",
                    "max_file_size": "3000 x 2000 px (10.00 x 6.67 in) - 300 dpi - 2 MB",
                },
            }
        ],
    )
    monkeypatch.setattr(getty_integration, "search_grouped_events", lambda *args, **kwargs: [])

    def _raise_unauthorized(*args, **kwargs):
        raise RuntimeError(
            "NBCUMV GraphQL request failed: 401 Client Error: Unauthorized for url: "
            "https://example.appsync-api.us-west-2.amazonaws.com/graphql"
        )

    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", _raise_unauthorized)
    monkeypatch.setattr(nbcumv_integration, "fetch_image_by_identity", _raise_unauthorized)
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", _raise_unauthorized)
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        mock_db,
        person_id=str(uuid4()),
        person_name="Lisa Barlow",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert result["fetched"] == 1
    assert result["imported"] == 0
    assert result["failed"] == 0
    assert result["getty_matched_total"] == 0
    assert result["getty_unmatched_total"] == 1
    assert result["getty_only_imported"] == 1
    assert any("NBCUMV unavailable" in str(error) for error in result["errors"])
    assert "nbcumv unavailable" in str(result["summary_message"]).lower()
    assert len(imported_getty_rows) == 1
    assert imported_getty_rows[0]["source"] == "getty"
    assert imported_getty_rows[0]["url"] == (
        "https://media.gettyimages.com/id/2246511440/photo/"
        "watch-what-happens-live-with-andy-cohen-season-20.jpg?p=1&w=gi&k=large&s=2048x2048"
    )
    assert imported_getty_rows[0]["metadata"]["crosswalk_reason"] == "nbcumv_unavailable"
    assert imported_getty_rows[0]["metadata"]["source_resolution"] == "getty_watermark_fallback"
    assert imported_getty_rows[0]["metadata"]["getty_original_image_url"] == (
        "https://media.gettyimages.com/id/2246511440/photo/"
        "watch-what-happens-live-with-andy-cohen-season-20.jpg?p=1&w=gi&k=large"
    )
    assert (
        imported_getty_rows[0]["metadata"]["google_reverse_image_search_url"]
        == "https://www.google.com/searchbyimage?image_url=https%3A%2F%2Fmedia.gettyimages.com%2Fid%2F2246511440%2Fphoto%2Fsample.jpg"
    )


def test_import_nbcumv_person_media_auto_replaces_bravocon_getty_asset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_getty_rows: list[dict[str, object]] = []
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value.data
    ) = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/bravocon/1",
                "editorial_id": "9001",
                "object_name": "BRAVOCON.JPG",
                "title": "BravoCon Press Room",
                "event_name": "BravoCon 2025 - Press Room",
                "preview_image_url": "https://media.gettyimages.com/bravocon-comp.jpg",
                "thumb_url": "https://media.gettyimages.com/bravocon-thumb.jpg",
                "assetDimensions": {"width": 1600, "height": 900},
                "people": [{"text": "Lisa Barlow"}],
            }
        ],
    )
    monkeypatch.setattr(getty_integration, "search_grouped_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)
    monkeypatch.setattr(nbcumv_integration, "fetch_image_by_identity", lambda **kwargs: None)
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id: {})
    monkeypatch.setattr(
        admin_person_images,
        "resolve_best_public_replacement",
        lambda *args, **kwargs: ResolvedPublicReplacement(
            page_url="https://www.bravotv.com/bravocon/gallery",
            source_domain="bravotv.com",
            image_url="https://www.bravotv.com/sites/bravo/files/bravocon-01.jpg",
            width=1825,
            height=1217,
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        mock_db,
        person_id=str(uuid4()),
        person_name="Lisa Barlow",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert result["getty_only_imported"] == 1
    assert len(imported_getty_rows) == 1
    row = imported_getty_rows[0]
    metadata = row["metadata"]
    assert row["url"] == "https://www.bravotv.com/sites/bravo/files/bravocon-01.jpg"
    assert metadata["source_resolution"] == "auto_picdetective_bravo"
    assert metadata["source_domain"] == "bravotv.com"
    assert metadata["source_page_url"] == "https://www.bravotv.com/bravocon/gallery"
    assert metadata["original_source_url"] == "https://media.gettyimages.com/bravocon-comp.jpg"
    assert metadata["original_source"] == "getty"
    assert metadata["getty_only_fallback"] is False
    assert result["matched_via_image_search"] == 1
    assert (
        metadata["google_reverse_image_search_url"]
        == "https://www.google.com/searchbyimage?image_url=https%3A%2F%2Fmedia.gettyimages.com%2Fbravocon-comp.jpg"
    )


def test_import_nbcumv_person_media_skips_public_replacement_for_prefetched_getty_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported_getty_rows: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.action = "select"
            self.filters: dict[str, object] = {}

        def select(self, _columns: str):
            self.action = "select"
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def execute(self):
            if self.table_name == "media_links" and self.action == "select":
                if self.filters.get("media_assets.source") == "nbcumv":
                    return _Response([{"id": "existing-nbcumv-link"}])
                return _Response([])
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(
        "trr_backend.integrations.nbcumv.fetch_image_by_identity",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("identity lookup should be skipped")),
    )
    monkeypatch.setattr(
        admin_person_images,
        "resolve_best_public_replacement",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("public replacement should be skipped")),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=None,
        show_name=None,
        limit=10,
        getty_prefetched_assets=[
            {
                "editorial_id": "1435767826",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/1435767826",
                "object_name": "NUP_1435767826.JPG",
                "preview_image_url": "https://media.gettyimages.com/brandi-preview.jpg",
                "original_image_url": "https://media.gettyimages.com/brandi-original.jpg",
                "thumb_url": "https://media.gettyimages.com/brandi-thumb.jpg",
                "event_name": "BravoCon",
                "caption": "Brandi Glanville attends BravoCon.",
                "source_query_scope": "bravo",
                "assetDimensions": {"width": 2048, "height": 2048},
            }
        ],
        getty_prefetched_events=[],
    )

    assert result["getty_prefetched"] is True
    assert result["getty_only_imported"] == 1
    assert result["existing_nbcumv_prefetched_enrichment_mode"] is True
    assert imported_getty_rows
    metadata = imported_getty_rows[0]["metadata"]
    assert metadata["source_resolution"] == "getty_watermark_fallback"
    assert "google_reverse_image_search_url" not in metadata


def test_import_nbcumv_person_media_discovery_mode_defers_weak_prefetched_getty_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported_getty_rows: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.action = "select"
            self.filters: dict[str, object] = {}

        def select(self, _columns: str):
            self.action = "select"
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def execute(self):
            if self.table_name == "media_links" and self.action == "select":
                if self.filters.get("media_assets.source") == "nbcumv":
                    return _Response([{"id": "existing-nbcumv-link"}])
                return _Response([])
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=None,
        show_name=None,
        limit=10,
        getty_prefetch_mode="discovery",
        getty_deferred_enrichment=True,
        getty_prefetched_assets=[
            {
                "editorial_id": "1435767826",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/1435767826",
                "object_name": "NUP_1435767826.JPG",
                "preview_image_url": "https://media.gettyimages.com/brandi-preview.jpg?s=1024x1024&w=gi",
                "original_image_url": "https://media.gettyimages.com/brandi-original.jpg?s=2048x2048&w=gi",
                "thumb_url": "https://media.gettyimages.com/brandi-thumb.jpg?s=300x300&w=gi",
                "event_name": "BravoCon",
                "caption": "Brandi Glanville attends BravoCon.",
                "source_query_scope": "bravo",
                "assetDimensions": {"width": 2048, "height": 2048},
            },
            {
                "editorial_id": "1435767827",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/1435767827",
                "object_name": "NUP_1435767827.JPG",
                "preview_image_url": "https://media.gettyimages.com/brandi-preview-small.jpg?s=300x300&w=gi",
                "original_image_url": "https://media.gettyimages.com/brandi-preview-small.jpg?s=300x300&w=gi",
                "thumb_url": "https://media.gettyimages.com/brandi-thumb-small.jpg?s=300x300&w=gi",
                "event_name": "BravoCon",
                "caption": "Brandi Glanville attends BravoCon.",
                "source_query_scope": "broad",
                "assetDimensions": {"width": 300, "height": 300},
            },
        ],
        getty_prefetched_events=[],
    )

    assert result["getty_prefetched"] is True
    assert result["getty_only_imported"] == 1
    assert result["getty_enrichment_pending"] == 2
    assert result["getty_deferred_editorial_ids"] == ["1435767826", "1435767827"]
    assert [row["source_image_id"] for row in imported_getty_rows] == ["1435767826"]


def test_import_nbcumv_person_media_getty_only_prefetch_ignores_requested_show_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported_getty_rows: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.filters: dict[str, object] = {}
            self.not_ = self._NotQuery(self)

        def select(self, _columns: str):
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        class _NotQuery:
            def __init__(self, parent: _Query):
                self.parent = parent

            def eq(self, key: str, value: object):
                self.parent.filters[f"neq:{key}"] = value
                return self.parent

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def execute(self):
            if self.table_name == "media_links" and self.filters.get("media_assets.source") == "nbcumv":
                return _Response([])
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=uuid4(),
        show_name="The Real Housewives of Beverly Hills",
        limit=10,
        getty_prefetched_assets=[
            {
                "editorial_id": "9990001",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/9990001",
                "object_name": "NUP_9990001.JPG",
                "preview_image_url": "https://media.gettyimages.com/9990001-preview.jpg?s=1024x1024&w=gi",
                "original_image_url": "https://media.gettyimages.com/9990001-original.jpg?s=2048x2048&w=gi",
                "thumb_url": "https://media.gettyimages.com/9990001-thumb.jpg?s=300x300&w=gi",
                "caption": "Brandi Glanville appears on Watch What Happens Live with Andy Cohen.",
                "source_query_scope": "broad",
                "assetDimensions": {"width": 2048, "height": 2048},
            }
        ],
        getty_prefetched_events=[],
        allow_nbcumv_only_supplement=False,
    )

    assert result["getty_only_direct_import_mode"] is True
    assert result["getty_candidates_total"] == 1
    assert result["getty_only_imported"] == 1
    assert result["getty_to_import_total"] == 1
    assert [row["source_image_id"] for row in imported_getty_rows] == ["9990001"]


def test_import_nbcumv_person_media_getty_only_defers_weak_discovery_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported_getty_rows: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.filters: dict[str, object] = {}
            self.not_ = self._NotQuery(self)

        def select(self, _columns: str):
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        class _NotQuery:
            def __init__(self, parent: _Query):
                self.parent = parent

            def eq(self, key: str, value: object):
                self.parent.filters[f"neq:{key}"] = value
                return self.parent

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def execute(self):
            if self.table_name == "media_links" and self.filters.get("media_assets.source") == "nbcumv":
                return _Response([])
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=uuid4(),
        show_name="The Real Housewives of Beverly Hills",
        limit=10,
        getty_prefetch_mode="discovery",
        getty_deferred_enrichment=True,
        getty_prefetched_assets=[
            {
                "editorial_id": "1246182583",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/1246182583",
                "preview_image_url": "https://media.gettyimages.com/id/1246182583/photo/brandi.jpg?s=612x612&w=gi",
                "original_image_url": "https://media.gettyimages.com/id/1246182583/photo/brandi.jpg?s=612x612&w=gi",
                "thumb_url": "https://media.gettyimages.com/id/1246182583/photo/brandi.jpg?s=300x300&w=gi",
                "caption": "Brandi Glanville on Watch What Happens Live.",
                "source_query_scope": "broad",
                "assetDimensions": {"width": 612, "height": 612},
            }
        ],
        getty_prefetched_events=[],
        allow_nbcumv_only_supplement=False,
    )

    assert result["getty_only_direct_import_mode"] is True
    assert result["getty_only_imported"] == 0
    assert result["getty_to_import_total"] == 0
    assert result["getty_deferred_resolution_total"] == 1
    assert result["getty_deferred_editorial_ids"] == ["1246182583"]
    assert imported_getty_rows == []
    assert result["summary_message"] == (
        "Deferred 1 Getty assets for full-detail enrichment before import; "
        "discovery previews are not imported as final Getty rows."
    )


def test_import_nbcumv_person_media_getty_only_skips_existing_shared_counterparts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported_getty_rows: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.filters: dict[str, object] = {}
            self.not_ = self._NotQuery(self)

        def select(self, _columns: str):
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        class _NotQuery:
            def __init__(self, parent: _Query):
                self.parent = parent

            def eq(self, key: str, value: object):
                self.parent.filters[f"neq:{key}"] = value
                return self.parent

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def update(self, _payload: dict[str, object]):
            return self

        def execute(self):
            if self.table_name == "cast_photos" and self.filters.get("neq:source") == "getty":
                return _Response([{"file_name": "NUP_1435767826.JPG"}])
            if self.table_name == "media_links" and self.filters.get("media_assets.source") == "nbcumv":
                return _Response([])
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=None,
        show_name=None,
        limit=10,
        getty_prefetched_assets=[
            {
                "editorial_id": "1435767826",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/1435767826",
                "object_name": "NUP_1435767826.JPG",
                "preview_image_url": "https://media.gettyimages.com/brandi-preview.jpg?s=1024x1024&w=gi",
                "original_image_url": "https://media.gettyimages.com/brandi-original.jpg?s=2048x2048&w=gi",
                "thumb_url": "https://media.gettyimages.com/brandi-thumb.jpg?s=300x300&w=gi",
                "caption": "Brandi Glanville attends BravoCon.",
                "source_query_scope": "bravo",
                "assetDimensions": {"width": 2048, "height": 2048},
            }
        ],
        getty_prefetched_events=[],
        allow_nbcumv_only_supplement=False,
    )

    assert result["getty_only_direct_import_mode"] is True
    assert result["getty_existing_shared_total"] == 1
    assert result["getty_skipped_existing_total"] == 1
    assert result["getty_to_import_total"] == 0
    assert result["getty_only_imported"] == 0
    assert imported_getty_rows == []


def test_import_nbcumv_person_media_getty_only_batches_large_upserts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported_batches: list[list[dict[str, object]]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.filters: dict[str, object] = {}
            self.not_ = self._NotQuery(self)

        def select(self, _columns: str):
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        class _NotQuery:
            def __init__(self, parent: _Query):
                self.parent = parent

            def eq(self, key: str, value: object):
                self.parent.filters[f"neq:{key}"] = value
                return self.parent

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def execute(self):
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setenv("TRR_GETTY_ONLY_UPSERT_BATCH_SIZE", "2")
    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_batches.append(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    assets = [
        {
            "editorial_id": f"batch-{index}",
            "detail_url": f"https://www.gettyimages.com/detail/news-photo/batch-{index}",
            "preview_image_url": f"https://media.gettyimages.com/id/batch-{index}/photo/brandi.jpg?s=1024x1024&w=gi",
            "original_image_url": f"https://media.gettyimages.com/id/batch-{index}/photo/brandi.jpg?s=2048x2048&w=gi",
            "thumb_url": f"https://media.gettyimages.com/id/batch-{index}/photo/brandi.jpg?s=300x300&w=gi",
            "caption": f"Brandi Glanville asset {index}",
            "source_query_scope": "broad",
            "assetDimensions": {"width": 2048, "height": 2048},
        }
        for index in range(5)
    ]

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=None,
        show_name=None,
        limit=10,
        getty_prefetch_mode="discovery",
        getty_deferred_enrichment=False,
        getty_prefetched_assets=assets,
        getty_prefetched_events=[],
        allow_nbcumv_only_supplement=False,
    )

    assert [len(batch) for batch in imported_batches] == [2, 2, 1]
    assert result["getty_to_import_total"] == 5
    assert result["getty_only_imported"] == 5


def test_import_nbcumv_person_media_getty_only_keeps_distinct_editorial_ids_with_shared_object_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imported_getty_rows: list[dict[str, object]] = []

    class _Response:
        def __init__(self, data):
            self.data = data
            self.error = None

    class _Query:
        def __init__(self, table_name: str):
            self.table_name = table_name
            self.filters: dict[str, object] = {}
            self.not_ = self._NotQuery(self)

        def select(self, _columns: str):
            return self

        def eq(self, key: str, value: object):
            self.filters[key] = value
            return self

        class _NotQuery:
            def __init__(self, parent: _Query):
                self.parent = parent

            def eq(self, key: str, value: object):
                self.parent.filters[f"neq:{key}"] = value
                return self.parent

        def in_(self, key: str, values: list[object]):
            self.filters[key] = list(values)
            return self

        def limit(self, _value: int):
            return self

        def execute(self):
            return _Response([])

    class _Schema:
        def table(self, table_name: str):
            return _Query(table_name)

    class _Db:
        def schema(self, _schema_name: str):
            return _Schema()

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    shared_object_name = "BRANDI_REPEAT.JPG"
    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        _Db(),
        person_id=str(uuid4()),
        person_name="Brandi Glanville",
        show_id=None,
        show_name=None,
        limit=10,
        getty_prefetched_assets=[
            {
                "editorial_id": "repeat-1",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/repeat-1",
                "object_name": shared_object_name,
                "preview_image_url": "https://media.gettyimages.com/id/repeat-1/photo/brandi.jpg?s=1024x1024&w=gi",
                "original_image_url": "https://media.gettyimages.com/id/repeat-1/photo/brandi.jpg?s=2048x2048&w=gi",
                "thumb_url": "https://media.gettyimages.com/id/repeat-1/photo/brandi.jpg?s=300x300&w=gi",
                "caption": "Brandi Glanville asset 1",
                "source_query_scope": "broad",
                "assetDimensions": {"width": 2048, "height": 2048},
            },
            {
                "editorial_id": "repeat-2",
                "detail_url": "https://www.gettyimages.com/detail/news-photo/repeat-2",
                "object_name": shared_object_name,
                "preview_image_url": "https://media.gettyimages.com/id/repeat-2/photo/brandi.jpg?s=1024x1024&w=gi",
                "original_image_url": "https://media.gettyimages.com/id/repeat-2/photo/brandi.jpg?s=2048x2048&w=gi",
                "thumb_url": "https://media.gettyimages.com/id/repeat-2/photo/brandi.jpg?s=300x300&w=gi",
                "caption": "Brandi Glanville asset 2",
                "source_query_scope": "broad",
                "assetDimensions": {"width": 2048, "height": 2048},
            },
        ],
        getty_prefetched_events=[],
        allow_nbcumv_only_supplement=False,
    )

    assert result["getty_only_direct_import_mode"] is True
    assert result["getty_usable_total"] == 2
    assert result["getty_to_import_total"] == 2
    assert result["getty_only_imported"] == 2
    assert [row["source_image_id"] for row in imported_getty_rows] == ["repeat-1", "repeat-2"]


def test_import_nbcumv_person_media_uses_date_scoped_nup_fallback_before_getty_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_items: list[dict[str, object]] = []
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value.data
    ) = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/rhoslc/1",
                "editorial_id": "1",
                "object_name": "NUP_204746_08001.JPG",
                "title": "The Real Housewives of Salt Lake City",
                "event_name": 'UT: BRAVO\'S "The Real Housewives of Salt Lake City"',
                "caption": "Lisa Barlow attends filming.",
                "date_created": "2025-01-22",
                "preview_image_url": "https://media.gettyimages.com/rhoslc.jpg",
            }
        ],
    )
    monkeypatch.setattr(getty_integration, "search_grouped_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: {"id": "show-rhoslc", "title": "The Real Housewives of Salt Lake City"},
    )
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id: {})
    monkeypatch.setattr(nbcumv_integration, "fetch_image_by_identity", lambda **kwargs: None)

    def _search_images(filters, **kwargs):
        if getattr(filters, "created_start", None) == "2025-01-22":
            return [
                {
                    "lbx_id": "70770001",
                    "lbx_filename": "NUP_204746_08001.JPG",
                    "location": "https://lightbox-thumbnails.test/NUP_204746_08001.JPG",
                    "lbx_showTitle": "The Real Housewives of Salt Lake City",
                    "showIds": ["show-rhoslc"],
                }
            ]
        return []

    monkeypatch.setattr(nbcumv_integration, "search_images", _search_images)
    monkeypatch.setattr(
        admin_nbcumv,
        "_import_single_item",
        lambda **kwargs: (
            imported_items.append(kwargs["item"].model_dump())
            or {
                "asset_id": "asset-1",
                "already_imported": False,
            }
        ),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {"person_id": person_id},
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        mock_db,
        person_id=str(uuid4()),
        person_name="Lisa Barlow",
        show_id=None,
        show_name="The Real Housewives of Salt Lake City",
        limit=10,
    )

    assert result["imported"] == 1
    assert result["getty_only_imported"] == 0
    assert result["getty_matched_total"] == 1
    assert imported_items[0]["lbx_filename"] == "NUP_204746_08001.JPG"
    assert imported_items[0]["lbx_id"] == "70770001"


def test_import_nbcumv_person_media_filters_getty_fallback_rows_to_requested_show(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_getty_rows: list[dict[str, object]] = []
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value.data
    ) = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_nbcumv, "_import_single_item", lambda **kwargs: {"already_imported": False})
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/rhoslc/1",
                "editorial_id": "1",
                "object_name": "RHOSLC_ONLY.JPG",
                "title": "The Real Housewives of Salt Lake City - Season 6",
                "event_name": 'UT: BRAVO\'S "The Real Housewives of Salt Lake City" - Season 6',
                "caption": 'THE REAL HOUSEWIVES OF SALT LAKE CITY -- "Reunion" -- Pictured: Mary Cosby',
                "preview_image_url": "https://media.gettyimages.com/rhoslc.jpg",
            },
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/wwhl/2",
                "editorial_id": "2",
                "object_name": "WWHL_ONLY.JPG",
                "title": "Watch What Happens Live With Andy Cohen - Season 23",
                "event_name": 'NY: Bravo\'s "Watch What Happens Live with Andy Cohen" - Season 23',
                "caption": "WATCH WHAT HAPPENS LIVE WITH ANDY COHEN -- Pictured: Mary Cosby",
                "preview_image_url": "https://media.gettyimages.com/wwhl.jpg",
            },
        ],
    )
    monkeypatch.setattr(
        nbcumv_integration,
        "resolve_show_by_title",
        lambda title: (
            {"id": "show-rhoslc", "title": "The Real Housewives of Salt Lake City"}
            if "salt lake city" in str(title).lower()
            else {"id": "show-wwhl", "title": "Watch What Happens Live with Andy Cohen"}
            if "watch what happens live" in str(title).lower()
            else None
        ),
    )
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id: {})
    monkeypatch.setattr(nbcumv_integration, "fetch_image_by_identity", lambda **kwargs: None)
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {
            "person_id": person_id,
            "source_id": "getty",
            "variant": "person_gallery_nbcumv_crosswalk",
        },
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        mock_db,
        person_id=str(uuid4()),
        person_name="Mary Cosby",
        show_id=None,
        show_name="The Real Housewives of Salt Lake City",
        limit=10,
    )

    assert result["getty_candidates_total"] == 1
    assert result["getty_unmatched_total"] == 1
    assert result["getty_only_imported"] == 1
    assert len(imported_getty_rows) == 1
    assert imported_getty_rows[0]["source_image_id"] == "1"
    assert imported_getty_rows[0]["metadata"]["bucket_type"] == "event"
    assert (
        imported_getty_rows[0]["metadata"]["bucket_label"]
        == 'UT: BRAVO\'S "The Real Housewives of Salt Lake City" - Season 6'
    )
    assert imported_getty_rows[0]["metadata"].get("show_name") is None


def test_import_nbcumv_person_media_buckets_wwhl_and_bravocon(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_getty_rows: list[dict[str, object]] = []
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value.data
    ) = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_nbcumv, "_import_single_item", lambda **kwargs: {"already_imported": False})
    monkeypatch.setattr(
        getty_integration,
        "search_editorial_assets",
        lambda *args, **kwargs: [
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/wwhl/1",
                "editorial_id": "1",
                "object_name": "WWHL_ONLY.JPG",
                "event_name": 'NY: Bravo\'s "Watch What Happens Live with Andy Cohen" - Season 22',
                "preview_image_url": "https://media.gettyimages.com/wwhl.jpg",
            },
            {
                "detail_url": "https://www.gettyimages.com/detail/news-photo/bravocon/2",
                "editorial_id": "2",
                "object_name": "BRAVOCON_ONLY.JPG",
                "event_name": 'NV: Bravo\'s "BravoCon 2025" - Day 3 Panels',
                "preview_image_url": "https://media.gettyimages.com/bravocon.jpg",
            },
        ],
    )
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id: {})
    monkeypatch.setattr(nbcumv_integration, "fetch_image_by_identity", lambda **kwargs: None)
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [{"id": str(uuid4()), "source_image_id": str(row.get("source_image_id") or "")} for row in rows]
        ),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {"person_id": person_id},
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        mock_db,
        person_id=str(uuid4()),
        person_name="Mary Cosby",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert result["getty_only_imported"] == 2
    assert [row["metadata"]["bucket_type"] for row in imported_getty_rows] == ["wwhl", "bravocon"]


def test_import_nbcumv_person_media_imports_broad_grouped_events_as_event_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    imported_getty_rows: list[dict[str, object]] = []
    hosted_updates: list[tuple[str, dict[str, object]]] = []
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value.data
    ) = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_nbcumv, "_import_single_item", lambda **kwargs: {"already_imported": False})
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        getty_integration,
        "search_grouped_events",
        lambda phrase, **kwargs: (
            [
                {
                    "source_query_scope": "broad",
                    "event_name": "NY: Amazon Home For The Holidays",
                    "event_url": "https://www.gettyimages.com/editorial-images/entertainment/event/amazon-home-for-the-holidays/1",
                    "event_id": "amazon-home",
                    "event_url_slug": "amazon-home-for-the-holidays",
                    "grouped_image_count": 4,
                    "matched_asset": {
                        "detail_url": "https://www.gettyimages.com/detail/news-photo/amazon-home/2246035169",
                        "editorial_id": "2246035169",
                        "object_name": "AMAZON_HOME.JPG",
                        "title": "Amazon Home For The Holidays",
                        "caption": (
                            "Jessel Taank, Ciara Miller, Amanda Batula and Lisa Barlow attend "
                            "Amazon Home For The Holidays."
                        ),
                        "preview_image_url": "https://media.gettyimages.com/id/2246035169/photo/sample.jpg",
                        "thumb_url": "https://media.gettyimages.com/id/2246035169/photo/sample-thumb.jpg",
                        "details": {
                            "credit_display": "Getty Images / Contributor",
                            "max_file_size": "4000 x 2667 px",
                        },
                        "keyword_texts": ["Lisa Barlow", "Four People", "Event"],
                        "people": [{"text": "Lisa Barlow"}],
                        "people_count": 4,
                    },
                    "asset_samples": [],
                    "event_asset_count_scanned": 1,
                }
            ]
            if kwargs.get("source_query_scope") == "broad"
            else []
        ),
    )
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id: {})
    monkeypatch.setattr(nbcumv_integration, "fetch_image_by_identity", lambda **kwargs: None)
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.upsert_cast_photos",
        lambda db, rows, dedupe_on="source_image_id": (
            imported_getty_rows.extend(list(rows))
            or [
                {
                    "id": str(uuid4()),
                    "source": "getty",
                    "source_image_id": str(row.get("source_image_id") or ""),
                    "metadata": row.get("metadata") or {},
                }
                for row in rows
            ]
        ),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.cast_photos.update_cast_photo_hosted_fields",
        lambda db, photo_id, patch: hosted_updates.append((photo_id, patch)) or {},
    )
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {"person_id": person_id, "payload": payload},
    )

    result = _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        mock_db,
        person_id=str(uuid4()),
        person_name="Lisa Barlow",
        show_id=None,
        show_name=None,
        limit=10,
    )

    assert result["getty_only_imported"] == 1
    assert result["getty_candidates_total"] == 1
    assert result["getty_broad_events"][0]["bucket_type"] == "event"
    assert result["getty_broad_events"][0]["grouped_image_count"] == 4
    assert result["getty_broad_events"][0]["resolution"] == "getty_watermark_fallback"
    assert imported_getty_rows[0]["metadata"]["bucket_type"] == "event"
    assert imported_getty_rows[0]["metadata"]["people_count"] == 4
    assert imported_getty_rows[0]["metadata"]["source_resolution"] == "getty_watermark_fallback"
    assert len(result["getty_only_row_ids"]) == 1
    assert hosted_updates == []


def test_import_nbcumv_person_media_requests_broad_events_with_minimum_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    captured_calls: list[dict[str, object]] = []
    mock_db = MagicMock()
    (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value.data
    ) = []

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [])

    def _fake_search_grouped_events(phrase: str, **kwargs):
        captured_calls.append({"phrase": phrase, **kwargs})
        return []

    monkeypatch.setattr(getty_integration, "search_grouped_events", _fake_search_grouped_events)
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)
    monkeypatch.setattr(nbcumv_integration, "build_show_image_index", lambda show_id: {})
    monkeypatch.setattr(
        admin_person_images,
        "_persist_person_getty_snapshot",
        lambda db, *, person_id, payload, status="success", error=None: {"person_id": person_id, "payload": payload},
    )

    _REAL_IMPORT_NBCUMV_PERSON_MEDIA(
        mock_db,
        person_id=str(uuid4()),
        person_name="Lisa Barlow",
        show_id=None,
        show_name=None,
        limit=10,
    )

    broad_call = next(call for call in captured_calls if call.get("source_query_scope") == "broad")
    assert broad_call["minimum_grouped_image_count"] == 2
    assert broad_call["person_match_required"] is True


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


def test_build_detection_boxes_omits_identity_when_assignment_not_allowed() -> None:
    result = SimpleNamespace(
        people_count=1,
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.92,
                kind="face",
                person_id=str(uuid4()),
                person_name="Alan Cumming",
                label="Alan Cumming",
                match_similarity=0.95,
                match_status="matched",
                match_reason="matched",
                match_candidates=[{"person_id": str(uuid4()), "person_name": "Alan Cumming", "similarity": 0.95}],
            )
        ],
    )
    face_boxes, diagnostics = admin_person_images._build_detection_boxes(result, allow_identity_assignment=False)
    assert diagnostics["auto_faces_detected"] == 1
    assert len(face_boxes) == 1
    assert face_boxes[0]["label_source"] == "generic"
    assert "person_id" not in face_boxes[0]
    assert "person_name" not in face_boxes[0]
    assert "match_similarity" not in face_boxes[0]
    assert "match_reason" not in face_boxes[0]
    assert "match_candidates" not in face_boxes[0]


def test_build_detection_boxes_includes_match_reason_and_candidates_when_allowed() -> None:
    result = SimpleNamespace(
        people_count=1,
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.92,
                kind="face",
                person_id=str(uuid4()),
                person_name="Alan Cumming",
                label="Alan Cumming",
                match_similarity=0.81,
                match_status="below_threshold",
                match_reason="below_threshold",
                match_candidates=[
                    {"person_id": str(uuid4()), "person_name": "Susan Lucci", "similarity": 0.81},
                    {"person_id": str(uuid4()), "similarity": 0.66},
                ],
            )
        ],
    )

    face_boxes, _ = admin_person_images._build_detection_boxes(result, allow_identity_assignment=True)
    assert len(face_boxes) == 1
    assert face_boxes[0]["match_reason"] == "below_threshold"
    assert isinstance(face_boxes[0]["match_candidates"], list)
    assert len(face_boxes[0]["match_candidates"]) == 2


def test_build_detection_boxes_backfills_person_name_from_tagged_people_ids() -> None:
    alan_id = "11111111-1111-1111-1111-111111111111"
    result = SimpleNamespace(
        people_count=1,
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.92,
                kind="face",
                person_id=alan_id,
                match_similarity=0.81,
                match_status="matched",
                match_reason="matched",
                match_candidates=[{"person_id": alan_id, "similarity": 0.81}],
            )
        ],
    )

    face_boxes, _ = admin_person_images._build_detection_boxes(
        result,
        allow_identity_assignment=True,
        tagged_people_ids=[alan_id],
        tagged_people_names=["Alan Cumming"],
    )
    assert len(face_boxes) == 1
    assert face_boxes[0]["person_name"] == "Alan Cumming"
    assert face_boxes[0]["label"] == "Alan Cumming"
    assert face_boxes[0]["match_candidates"][0]["person_name"] == "Alan Cumming"


def test_build_detection_boxes_applies_best_effort_tag_assignment_when_tags_fewer_than_boxes() -> None:
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.92,
                kind="face",
            ),
            SimpleNamespace(
                x1=0.6,
                y1=0.2,
                x2=0.8,
                y2=0.5,
                confidence=0.91,
                kind="face",
            ),
        ],
    )
    face_boxes, _ = admin_person_images._build_detection_boxes(
        result,
        allow_identity_assignment=True,
        tagged_people_names=["Alan Cumming"],
    )
    assert len(face_boxes) == 2
    assert face_boxes[0]["person_name"] == "Alan Cumming"
    assert face_boxes[0]["label_source"] == "best_effort_tag_map"
    assert face_boxes[0]["match_status"] == "matched"
    assert face_boxes[0]["match_reason"] == "best_effort_tag_map"
    assert "person_name" not in face_boxes[1]


def test_build_detection_boxes_promotes_single_face_deterministic_assignment_to_matched() -> None:
    owner_id = "11111111-1111-1111-1111-111111111111"
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.2,
                y1=0.15,
                x2=0.45,
                y2=0.6,
                confidence=0.84,
                kind="face",
                match_status="unassigned",
            )
        ],
    )
    face_boxes, _ = admin_person_images._build_detection_boxes(
        result,
        allow_identity_assignment=True,
        tagged_people_ids=[owner_id],
        owner_person_id=owner_id,
        owner_person_name="Alan Cumming",
    )
    assert len(face_boxes) == 1
    assert face_boxes[0]["person_id"] == owner_id
    assert face_boxes[0]["person_name"] == "Alan Cumming"
    assert face_boxes[0]["label_source"] == "owner_fallback_map"
    assert face_boxes[0]["match_status"] == "matched"
    assert face_boxes[0]["match_reason"] == "owner_fallback_map"


def test_build_detection_boxes_applies_similarity_lead_override_before_hybrid_fallback() -> None:
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                x1=0.1,
                y1=0.2,
                x2=0.3,
                y2=0.5,
                confidence=0.91,
                kind="face",
                match_status="below_threshold",
                match_reason="below_threshold",
                match_similarity=0.76,
                match_candidates=[
                    {
                        "person_id": "11111111-1111-1111-1111-111111111111",
                        "person_name": "Alan Cumming",
                        "similarity": 0.76,
                    }
                ],
            ),
            SimpleNamespace(
                x1=0.6,
                y1=0.2,
                x2=0.8,
                y2=0.5,
                confidence=0.88,
                kind="face",
                match_status="below_threshold",
                match_reason="below_threshold",
                match_similarity=0.07,
                match_candidates=[
                    {
                        "person_id": "11111111-1111-1111-1111-111111111111",
                        "person_name": "Alan Cumming",
                        "similarity": 0.07,
                    }
                ],
            ),
        ],
    )

    face_boxes, _ = admin_person_images._build_detection_boxes(
        result,
        allow_identity_assignment=True,
        tagged_people_ids=[
            "11111111-1111-1111-1111-111111111111",
            "22222222-2222-2222-2222-222222222222",
        ],
        tagged_people_names=["Alan Cumming", "Milo Ventimiglia"],
    )

    assert len(face_boxes) == 2
    by_x = sorted(face_boxes, key=lambda box: box.get("x", 0.0))
    lead_face = by_x[0]
    fallback_face = by_x[1]
    assert lead_face["person_name"] == "Alan Cumming"
    assert lead_face["label_source"] == "lead_override"
    assert lead_face["match_reason"] == "cross_face_lead_override"
    assert fallback_face["person_name"] == "Milo Ventimiglia"
    assert fallback_face["label_source"] == "deterministic_tag_map"


def test_is_trr_show_eligible_accepts_mapped_fallback_show_name(monkeypatch) -> None:
    mock_db = MagicMock()
    show_id = str(uuid4())
    show_row = {"id": show_id, "name": "Watch What Happens Live with Andy Cohen"}
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))
    monkeypatch.setattr(admin_person_images, "_find_show_row_by_alias", lambda by_alias, value: show_row)

    shows_execute = (
        mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value.execute
    )
    shows_response = MagicMock()
    shows_response.data = [{"id": show_id}]
    shows_execute.return_value = shows_response

    assert (
        admin_person_images._is_trr_show_eligible(
            mock_db,
            metadata={"imdb_fallback_show_name": "Watch What Happens Live with Andy Cohen"},
        )
        is True
    )


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


def test_enrich_cast_photos_with_episode_metadata_uses_wwhl_credit_episode_ids_when_fallback_missing(
    monkeypatch,
) -> None:
    photos = [
        {
            "source": "imdb",
            "title_imdb_ids": ["tt26755932"],
            "title_names": ["Milo Ventimiglia & Alan Cumming"],
            "metadata": {},
        }
    ]

    mock_db = MagicMock()
    episodes_response = MagicMock()
    episodes_response.error = None
    episodes_response.data = []
    episodes_query = mock_db.schema.return_value.table.return_value.select.return_value.in_.return_value
    episodes_query.execute.return_value = episodes_response

    monkeypatch.setattr(admin_person_images, "_fetch_imdb_title_fallback_metadata", lambda imdb_ids: {})
    monkeypatch.setattr(admin_person_images, "_build_show_lookup_maps", lambda db: ({}, {}, {}))

    tagged, failed = admin_person_images._enrich_cast_photos_with_episode_metadata(
        mock_db,
        photos,
        person_wwhl_episode_imdb_ids={"tt26755932"},
    )

    assert tagged == 1
    assert failed == 0
    metadata = photos[0]["metadata"]
    assert metadata["show_context_source"] == "imdb_episode_unresolved"
    assert metadata["show_name"] is None
    assert metadata["show_id"] is None
    assert metadata["imdb_fallback_show_name"] == "Watch What Happens Live with Andy Cohen"
    assert metadata["imdb_fallback_show_imdb_id"] == "tt2057880"
    assert metadata["episode_imdb_id"] == "tt26755932"


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


def test_apply_show_context_to_photos_overrides_mismatched_request_context_when_episode_evidence_matches(
    monkeypatch,
) -> None:
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
            "title_imdb_ids": ["tt26755932"],
            "metadata": {
                "show_context_source": "request_context",
                "show_id": "wrong-show-id",
                "show_name": "Wrong Show",
                "show_imdb_id": "tt0000001",
                "imdb_title_type": "TVEpisode",
                "imdb_image_type": "still_frame",
                "imdb_fallback_show_name": "The Traitors",
                "imdb_fallback_show_imdb_id": "tt1234567",
                "episode_title": "The Power of the Seer",
            },
        }
    ]

    monkeypatch.setattr(
        admin_person_images,
        "_build_show_lookup_maps",
        lambda db: (
            {
                "tt1234567": {"id": show_id_str, "name": "The Traitors", "imdb_id": "tt1234567"},
                "tt0000001": {"id": "wrong-show-id", "name": "Wrong Show", "imdb_id": "tt0000001"},
            },
            {
                "the traitors": {"id": show_id_str, "name": "The Traitors", "imdb_id": "tt1234567"},
                "wrong show": {"id": "wrong-show-id", "name": "Wrong Show", "imdb_id": "tt0000001"},
            },
            {
                show_id_str: {"id": show_id_str, "name": "The Traitors", "imdb_id": "tt1234567"},
                "wrong-show-id": {"id": "wrong-show-id", "name": "Wrong Show", "imdb_id": "tt0000001"},
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
        lambda db, rows, **kwargs: (1, 0),
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
            "metadata": {"imdb_title_type": "TVEpisode"},
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
            "imdb_title_id": "tt123",
            "imdb_title_url": "https://www.imdb.com/title/tt123/",
            "image_type": "still_frame",
        },
    )
    monkeypatch.setattr(
        admin_person_images,
        "_enrich_cast_photos_with_episode_metadata",
        lambda db, rows, **kwargs: (0, 0),
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
    assert metadata["imdb_title_id"] == "tt123"
    assert metadata["imdb_title_url"] == "https://www.imdb.com/title/tt123/"
    assert metadata["imdb_credit_media_type"] == "TV Episode"
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
                "imdb_title_type": "Movie",
                "imdb_title_id": "tt123",
                "imdb_title_url": "https://www.imdb.com/title/tt123/",
                "imdb_credit_media_type": "Movie",
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


def test_needs_imdb_metadata_refresh_when_episode_like_row_missing_fallback_show() -> None:
    row = {
        "source": "imdb",
        "title_imdb_ids": ["tt26755932"],
        "people_imdb_ids": ["nm0001086"],
        "context_type": "Episode Still",
        "metadata": {
            "imdb_image_type": "still_frame",
            "imdb_title_type": "TVEpisode",
            "show_context_source": "request_context",
            "tags": {
                "people": [{"imdb_id": "nm0001086", "name": "Alan Cumming"}],
                "titles": [{"imdb_id": "tt26755932", "title": "Milo Ventimiglia & Alan Cumming"}],
            },
        },
    }

    assert admin_person_images._needs_imdb_metadata_refresh(row) is True


def test_needs_imdb_metadata_refresh_when_request_context_inferred_lacks_corroboration() -> None:
    row = {
        "source": "imdb",
        "title_imdb_ids": ["tt99999999"],
        "people_imdb_ids": ["nm0001086"],
        "context_type": "Still Frame",
        "metadata": {
            "imdb_image_type": "still_frame",
            "show_context_source": "request_context_inferred",
            "show_id": "legacy-show-id",
            "show_name": "The Traitors",
            "tags": {
                "people": [{"imdb_id": "nm0001086", "name": "Alan Cumming"}],
                "titles": [{"imdb_id": "tt99999999", "title": "Some Episode"}],
            },
        },
    }

    assert admin_person_images._needs_imdb_metadata_refresh(row) is True


def test_needs_imdb_metadata_refresh_for_episode_evidence_with_mistagged_request_context() -> None:
    row = {
        "source": "imdb",
        "title_imdb_ids": ["tt26755932"],
        "people_imdb_ids": ["nm0001086"],
        "context_type": "Still Frame",
        "metadata": {
            "show_context_source": "request_context",
            "show_name": "Wrong Show",
            "imdb_title_type": "TVEpisode",
            "imdb_image_type": "still_frame",
            "episode_title": "The Power of the Seer",
            "tags": {
                "people": [{"imdb_id": "nm0001086", "name": "Alan Cumming"}],
                "titles": [{"imdb_id": "tt26755932", "title": "The Power of the Seer"}],
            },
        },
    }

    assert admin_person_images._needs_imdb_metadata_refresh(row) is True


def test_repair_existing_imdb_cast_photos_rejects_stale_request_context_show(monkeypatch) -> None:
    mock_db = MagicMock()
    existing_rows = [
        {
            "id": "photo-1",
            "source": "imdb",
            "source_image_id": "rm_stale",
            "title_imdb_ids": ["tt26755932"],
            "title_names": ["Milo Ventimiglia & Alan Cumming"],
            "people_imdb_ids": ["nm0001086", "nm0000232"],
            "people_names": ["Alan Cumming", "Milo Ventimiglia"],
            "metadata": {
                "show_context_source": "request_context_inferred",
                "show_id": "show-traitors",
                "show_name": "The Traitors",
                "show_imdb_id": "tt1234567",
                "show_short_code": "TT",
                "imdb_image_type": "event",
                "imdb_fallback_show_name": "Watch What Happens Live with Andy Cohen",
                "imdb_fallback_show_imdb_id": "tt0318220",
                "tags": {
                    "people": [
                        {"imdb_id": "nm0001086", "name": "Alan Cumming"},
                        {"imdb_id": "nm0000232", "name": "Milo Ventimiglia"},
                    ],
                    "titles": [{"imdb_id": "tt26755932", "title": "Milo Ventimiglia & Alan Cumming"}],
                },
            },
        }
    ]

    monkeypatch.setattr(
        admin_person_images,
        "_load_existing_imdb_cast_photos_for_person",
        lambda db, person_id: existing_rows,
    )
    monkeypatch.setattr(admin_person_images, "_load_imdb_viewer_image_types", lambda imdb_person_id, viewer_ids: {})
    monkeypatch.setattr(
        admin_person_images,
        "_enrich_cast_photos_with_episode_metadata",
        lambda db, rows, **kwargs: (0, 0),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_apply_show_context_to_photos",
        lambda db, rows, show_id, show_name: (0, 0),
    )
    monkeypatch.setattr(
        admin_person_images,
        "_build_show_lookup_maps",
        lambda db: (
            {
                "tt1234567": {"id": "show-traitors", "name": "The Traitors", "imdb_id": "tt1234567"},
                "tt0318220": {
                    "id": "show-wwhl",
                    "name": "Watch What Happens Live with Andy Cohen",
                    "imdb_id": "tt0318220",
                },
            },
            {
                "the traitors": {"id": "show-traitors", "name": "The Traitors", "imdb_id": "tt1234567"},
                "watch what happens live with andy cohen": {
                    "id": "show-wwhl",
                    "name": "Watch What Happens Live with Andy Cohen",
                    "imdb_id": "tt0318220",
                },
            },
            {
                "show-traitors": {"id": "show-traitors", "name": "The Traitors", "imdb_id": "tt1234567"},
                "show-wwhl": {
                    "id": "show-wwhl",
                    "name": "Watch What Happens Live with Andy Cohen",
                    "imdb_id": "tt0318220",
                },
            },
        ),
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
        show_name=None,
    )

    assert repaired == 1
    assert failed == 0
    assert len(upserted_rows) == 1
    metadata = dict(upserted_rows[0].get("metadata") or {})
    assert metadata.get("show_id") is None
    assert metadata.get("show_name") is None
    assert metadata.get("show_imdb_id") is None
    assert metadata.get("show_short_code") is None
    assert metadata.get("show_context_source") == "request_context_rejected"
    assert metadata.get("show_context_repair_reason") == "missing_corroboration"


def test_evaluate_imdb_request_context_staleness_trusts_episode_fallback_on_show_mismatch() -> None:
    row = {
        "source": "imdb",
        "context_type": "Episode Still",
        "metadata": {
            "show_context_source": "request_context_inferred",
            "show_id": "show-wrong",
            "show_name": "Wrong Show",
            "show_imdb_id": "tt0000001",
            "imdb_title_type": "TVEpisode",
            "imdb_fallback_show_name": "The Traitors",
            "imdb_fallback_show_imdb_id": "tt1234567",
            "episode_title": "The Power of the Seer",
            "imdb_image_type": "still_frame",
        },
    }
    show_lookup_by_imdb_id = {
        "tt0000001": {"id": "show-wrong", "name": "Wrong Show", "imdb_id": "tt0000001"},
        "tt1234567": {"id": "show-traitors", "name": "The Traitors", "imdb_id": "tt1234567"},
    }
    show_lookup_by_alias = {
        "wrong show": {"id": "show-wrong", "name": "Wrong Show", "imdb_id": "tt0000001"},
        "the traitors": {"id": "show-traitors", "name": "The Traitors", "imdb_id": "tt1234567"},
    }
    show_lookup_by_id = {
        "show-wrong": {"id": "show-wrong", "name": "Wrong Show", "imdb_id": "tt0000001"},
        "show-traitors": {"id": "show-traitors", "name": "The Traitors", "imdb_id": "tt1234567"},
    }

    stale, reason = admin_person_images._evaluate_imdb_request_context_staleness(
        row,
        show_lookup_by_imdb_id=show_lookup_by_imdb_id,
        show_lookup_by_alias=show_lookup_by_alias,
        show_lookup_by_id=show_lookup_by_id,
    )

    assert stale is False
    assert reason is None


def test_repair_existing_imdb_cast_photos_does_not_downgrade_authoritative_rows(monkeypatch) -> None:
    mock_db = MagicMock()
    existing_rows = [
        {
            "id": "photo-1",
            "source": "imdb",
            "source_image_id": "rm_episode_table",
            "title_imdb_ids": ["tt26755932"],
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "metadata": {
                "show_context_source": "episode_table",
                "show_id": "show-traitors",
                "show_name": "The Traitors",
                "show_imdb_id": "tt1234567",
                "imdb_image_type": "still_frame",
                "imdb_title_type": "TVEpisode",
                "imdb_title_id": "tt26755932",
                "imdb_title_url": "https://www.imdb.com/title/tt26755932/",
                "imdb_credit_media_type": "TV Episode",
                "episode_imdb_id": "tt26755932",
                "episode_title": "The Power of the Seer",
                "tags": {
                    "people": [{"imdb_id": "nm0001086", "name": "Alan Cumming"}],
                    "titles": [{"imdb_id": "tt26755932", "title": "The Power of the Seer"}],
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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


def test_refresh_stream_emits_terminal_error_when_operation_kickoff_fails(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_person_images._get_person_details",
            return_value={"id": person_id, "full_name": "Example Person", "external_ids": {"imdb": "nm123"}},
        ):
            with patch("api.routers.admin_person_images._get_tmdb_id", return_value=None):
                with patch("api.routers.admin_person_images._resolve_refresh_sources", return_value=(["imdb"], False)):
                    with patch(
                        "api.routers.admin_person_images.start_operation_for_stream",
                        side_effect=RuntimeError("kickoff failed"),
                    ):
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                            json={"skip_mirror": True},
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    normalized_payload = response.text.replace("\r\n", "\n")
    assert "event: error" in normalized_payload
    assert '"stage": "startup"' in normalized_payload or '"stage":"startup"' in normalized_payload
    assert '"error_code": "STREAM_OPERATION_START_FAILED"' in normalized_payload or '"error_code":"STREAM_OPERATION_START_FAILED"' in normalized_payload
    assert "kickoff failed" in normalized_payload


def test_reprocess_stream_emits_terminal_error_for_unhandled_exception(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
                    json={"prefer_fast_pass": False},
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    normalized_payload = response.text.replace("\r\n", "\n")
    assert "event: error" in normalized_payload
    assert '"stage": "stream"' in normalized_payload or '"stage":"stream"' in normalized_payload
    assert "Reprocess stream failed" in normalized_payload


def test_reprocess_stream_force_tagging_recount_true_passes_force_recount(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
            return_value=(2, 2, 0),
        ) as cast_mock:
            with patch(
                "api.routers.admin_person_images._auto_count_media_links",
                return_value=(1, 1, 0),
            ) as media_mock:
                response = client.post(
                    f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                    json={
                        "run_metadata": False,
                        "run_count": True,
                        "run_tagging": True,
                        "force_tagging_recount": True,
                        "run_id_text": False,
                        "run_crop": False,
                        "run_resize": False,
                        "prefer_fast_pass": False,
                    },
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    assert cast_mock.call_count == 1
    assert media_mock.call_count == 1
    assert cast_mock.call_args.kwargs["force_recount"] is True
    assert media_mock.call_args.kwargs["force_recount"] is True


def test_reprocess_stream_tagging_is_always_full_fix_even_when_flag_false(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
            return_value=(2, 2, 0),
        ) as cast_mock:
            with patch(
                "api.routers.admin_person_images._auto_count_media_links",
                return_value=(1, 1, 0),
            ) as media_mock:
                response = client.post(
                    f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                    json={
                        "run_metadata": False,
                        "run_count": True,
                        "run_tagging": True,
                        "force_tagging_recount": False,
                        "run_id_text": False,
                        "run_crop": False,
                        "run_resize": False,
                        "prefer_fast_pass": False,
                    },
                    headers={"Authorization": f"Bearer {token}"},
                )

    assert response.status_code == 200
    assert cast_mock.call_count == 1
    assert media_mock.call_count == 1
    assert cast_mock.call_args.kwargs["force_recount"] is True
    assert media_mock.call_args.kwargs["force_recount"] is True


def test_reprocess_stream_passes_owner_reference_pool_to_tagging_calls(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}
    mock_db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [person_data]
    mock_response.error = None
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response
    expected_refs = [
        {
            "url": "https://example.com/ref-1.jpg",
            "media_asset_id": "asset-1",
            "link_id": "link-1",
            "rank": 1,
            "reasons": ["seeded", "solo"],
        }
    ]

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_person_images.build_owner_tagging_reference_profile",
            return_value={"used": expected_refs},
        ) as profile_mock:
            with patch(
                "api.routers.admin_person_images.sync_owner_tagging_reference_usage",
                side_effect=lambda _db, _person_id, *, used_references: used_references,
            ):
                with patch(
                    "api.routers.admin_person_images._auto_count_cast_photos",
                    return_value=(2, 2, 0),
                ) as cast_mock:
                    with patch(
                        "api.routers.admin_person_images._auto_count_media_links",
                        return_value=(1, 1, 0),
                    ) as media_mock:
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                            json={
                                "run_metadata": False,
                                "run_count": True,
                                "run_tagging": True,
                                "run_id_text": False,
                                "run_crop": False,
                                "run_resize": False,
                                "show_name": "The Traitors",
                                "prefer_fast_pass": False,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    assert profile_mock.call_count == 1
    assert cast_mock.call_args.kwargs["owner_reference_images"] == expected_refs
    assert media_mock.call_args.kwargs["owner_reference_images"] == expected_refs
    assert callable(cast_mock.call_args.kwargs["owner_reference_sync_cb"])
    assert callable(media_mock.call_args.kwargs["owner_reference_sync_cb"])


def test_reprocess_stream_forwards_scoped_targets_to_stage_helpers(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}
    mock_db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [person_data]
    mock_response.error = None
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response

    target_cast_photo_ids = [str(uuid4()), str(uuid4())]
    target_media_link_ids = [str(uuid4())]

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch(
            "api.routers.admin_person_images._auto_count_cast_photos",
            return_value=(2, 2, 0),
        ) as cast_mock:
            with patch(
                "api.routers.admin_person_images._auto_count_media_links",
                return_value=(1, 1, 0),
            ) as media_mock:
                with patch(
                    "api.routers.admin_person_images._recenter_person_gallery_images",
                    return_value=(1, 1, 0, 0),
                ) as recenter_mock:
                    with patch(
                        "api.routers.admin_person_images._resize_person_gallery_images",
                        return_value=(1, 1, 0, 1, 1, 0),
                    ) as resize_mock:
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                            json={
                                "run_metadata": False,
                                "run_count": True,
                                "run_id_text": False,
                                "run_crop": True,
                                "run_resize": True,
                                "target_cast_photo_ids": target_cast_photo_ids,
                                "target_media_link_ids": target_media_link_ids,
                                "prefer_fast_pass": False,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    assert cast_mock.call_count == 1
    assert media_mock.call_count == 1
    assert recenter_mock.call_count == 1
    assert resize_mock.call_count == 1
    assert cast_mock.call_args.kwargs["photo_ids"] == target_cast_photo_ids
    assert media_mock.call_args.kwargs["media_link_ids"] == target_media_link_ids
    assert recenter_mock.call_args.kwargs["photo_ids"] == target_cast_photo_ids
    assert recenter_mock.call_args.kwargs["media_link_ids"] == target_media_link_ids
    assert resize_mock.call_args.kwargs["photo_ids"] == target_cast_photo_ids
    assert resize_mock.call_args.kwargs["media_link_ids"] == target_media_link_ids


def test_reprocess_stream_skips_scoped_stages_when_scope_targets_are_empty(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}
    mock_db = MagicMock()
    mock_response = MagicMock()
    mock_response.data = [person_data]
    mock_response.error = None
    query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
    query.execute.return_value = mock_response

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_person_images._auto_count_cast_photos") as cast_mock:
            with patch("api.routers.admin_person_images._auto_count_media_links") as media_mock:
                with patch("api.routers.admin_person_images._recenter_person_gallery_images") as recenter_mock:
                    with patch("api.routers.admin_person_images._resize_person_gallery_images") as resize_mock:
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                            json={
                                "run_metadata": False,
                                "run_count": True,
                                "run_id_text": False,
                                "run_crop": True,
                                "run_resize": True,
                                "target_cast_photo_ids": [],
                                "target_media_link_ids": [],
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

    assert response.status_code == 200
    assert cast_mock.call_count == 0
    assert media_mock.call_count == 0
    assert recenter_mock.call_count == 0
    assert resize_mock.call_count == 0
    assert "Skipping tagging stage (no scoped targets)." in response.text
    assert "Skipping centering/cropping stage (no scoped targets)." in response.text
    assert "Skipping resize stage (no scoped targets)." in response.text


def test_refresh_stream_emits_resizing_heartbeat_during_long_variant_generation(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
    assert '"operation_id"' in payload
    event_seq_matches = [int(match) for match in re.findall(r'"event_seq"\s*:\s*(\d+)', payload)]
    assert event_seq_matches
    assert event_seq_matches == sorted(event_seq_matches)
    assert len(event_seq_matches) == len(set(event_seq_matches))
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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


def test_refresh_stream_metadata_repair_uses_fixing_imdb_details_label(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    person_data = {
        "id": person_id,
        "full_name": "Metadata Repair Label Person",
        "external_ids": {"imdb": "nm0001086"},
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
                with patch("api.routers.admin_person_images._resolve_refresh_sources", return_value=(["imdb"], False)):
                    with patch(
                        "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                        return_value=(0, 0),
                    ):
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
    assert "Fixing IMDb Details" in response.text


def test_mirror_person_media_assets_reports_progress_callbacks() -> None:
    mock_db = MagicMock()
    progress_updates: list[tuple[int, int]] = []

    with patch(
        "api.routers.admin_person_images._fetch_person_media_link_rows",
        return_value=[
            {
                "id": str(uuid4()),
                "media_asset_id": str(uuid4()),
                "source": "getty",
                "source_url": "https://media.gettyimages.com/id/example-1.jpg",
                "hosted_url": None,
                "metadata": {},
                "ingest_status": "pending",
                "ingest_last_error": None,
            },
            {
                "id": str(uuid4()),
                "media_asset_id": str(uuid4()),
                "source": "getty",
                "source_url": "https://media.gettyimages.com/id/example-2.jpg",
                "hosted_url": None,
                "metadata": {},
                "ingest_status": "pending",
                "ingest_last_error": None,
            },
        ],
    ):
        with patch("trr_backend.media.s3_mirror.mirror_media_asset_row", return_value=None):
            with patch("trr_backend.repositories.media_assets.update_ingest_status"):
                mirrored, failed = admin_person_images._mirror_person_media_assets(
                    mock_db,
                    person_id=str(uuid4()),
                    progress_cb=lambda done, total: progress_updates.append((done, total)),
                )

    assert mirrored == 0
    assert failed == 0
    assert progress_updates == [(1, 2), (2, 2)]


def test_refresh_stream_includes_tmdb_profile_failure_fields_in_complete_payload(client, monkeypatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    person_id = str(uuid4())
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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

    def test_refresh_with_show_context_includes_nbcumv_counts(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        show_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Lisa Barlow",
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
                        "api.routers.admin_person_images._import_nbcumv_person_media",
                        return_value={
                            "fetched": 3,
                            "imported": 2,
                            "skipped": 1,
                            "failed": 0,
                            "gallery_links_created": 4,
                            "asset_ids": ["asset-1", "asset-2"],
                            "errors": [],
                        },
                    ):
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images",
                            json={
                                "show_id": show_id,
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
        assert data["nbcumv_photos_fetched"] == 3
        assert data["nbcumv_assets_imported"] == 2
        assert data["nbcumv_assets_skipped"] == 1
        assert data["nbcumv_gallery_links_created"] == 4
        assert data["photos_fetched"] == 3
        assert data["photos_upserted"] == 2

    def test_refresh_aborts_after_double_zero_getty_direct_searches(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Brandi Glanville",
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
                        "api.routers.admin_person_images._import_nbcumv_person_media",
                        return_value={
                            "fetched": 0,
                            "imported": 0,
                            "skipped": 0,
                            "failed": 0,
                            "gallery_links_created": 0,
                            "asset_ids": [],
                            "errors": [],
                            "summary_message": (
                                "Stopped refresh early: both direct Getty person searches returned zero results. "
                                "Grouped Getty, NBCUMV, and BravoTV stages were not run."
                            ),
                            "getty_search_attempted": True,
                            "getty_primary_candidates_total": 0,
                            "getty_fallback_candidates_total": 0,
                            "getty_initial_search_zero_abort": True,
                            "getty_initial_search_queries": ["Brandi Glanville Bravo", "Brandi Glanville"],
                            "getty_initial_search_counts": {"Brandi Glanville Bravo": 0, "Brandi Glanville": 0},
                        },
                    ):
                        with patch(
                            "api.routers.admin_person_images._import_bravotv_person_media",
                        ) as bravotv_import_mock:
                            response = client.post(
                                f"/api/v1/admin/person/{person_id}/refresh-images",
                                json={
                                    "sources": ["nbcumv", "bravotv"],
                                    "skip_mirror": True,
                                    "skip_auto_count": False,
                                    "skip_word_detection": False,
                                    "skip_centering": False,
                                    "skip_resize": False,
                                },
                                headers={"Authorization": f"Bearer {token}"},
                            )

        assert response.status_code == 200
        bravotv_import_mock.assert_not_called()
        data = response.json()
        assert data["getty_initial_search_zero_abort"] is True
        assert data["getty_initial_search_queries"] == ["Brandi Glanville Bravo", "Brandi Glanville"]
        assert data["getty_initial_search_counts"] == {"Brandi Glanville Bravo": 0, "Brandi Glanville": 0}
        assert data["bravotv_photos_fetched"] == 0
        assert any(part["part"] == "getty_initial_search_zero_abort" for part in data["failed_parts"])

    def test_refresh_stream_starts_new_operation_without_attach(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [{"id": person_id, "full_name": "Brandi Glanville", "external_ids": {}}]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images.start_operation_for_stream",
                return_value={"id": str(uuid4())},
            ) as start_operation:
                with patch("api.routers.admin_person_images.operation_stream_response", return_value=Response("ok")):
                    response = client.post(
                        f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                        json={"sources": ["nbcumv"], "skip_mirror": True},
                        headers={"Authorization": f"Bearer {token}"},
                    )

        assert response.status_code == 200
        assert start_operation.call_args.kwargs["allow_attach"] is False

    def test_stream_emits_nbcumv_progress_updates(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Lisa Barlow",
            "external_ids": {"imdb": "nm12345678"},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        def _fake_nbcumv_import(*args, progress_cb=None, **kwargs):
            if progress_cb:
                progress_cb(0, 4, "Searching Getty for 'Lisa Barlow' on Getty (Bravo)...")
                progress_cb(2, 4, "Matching Getty asset 2/4: NUP_123.JPG")
                progress_cb(4, 4, "Imported NBCUMV 2/2: NUP_123.JPG")
            time.sleep(0.05)
            return {
                "fetched": 2,
                "imported": 2,
                "skipped": 0,
                "failed": 0,
                "shared_nbcumv_imported": 2,
                "nbcumv_only_imported": 0,
                "getty_only_imported": 1,
                "gallery_links_created": 2,
                "asset_ids": ["asset-1", "asset-2"],
                "errors": [],
            }

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id", return_value=None):
                with patch("trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos", return_value=[]):
                    with patch(
                        "api.routers.admin_person_images._import_nbcumv_person_media",
                        side_effect=_fake_nbcumv_import,
                    ):
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                            json={
                                "sources": ["nbcumv"],
                                "skip_mirror": True,
                                "skip_auto_count": True,
                                "skip_word_detection": True,
                                "skip_centering": True,
                                "skip_resize": True,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

        assert response.status_code == 200
        payload = response.text.replace("\r\n", "\n")
        assert "Searching Getty for 'Lisa Barlow' on Getty (Bravo)..." in payload
        assert "Matching Getty asset 2/4: NUP_123.JPG" in payload
        assert (
            "Summary: 2 shared via NBCUMV, 0 NBCUMV-only, 1 Getty-only, 0 covered existing, 0 skipped, 0 failed."
        ) in payload

    def test_stream_uses_nbcumv_stage_totals_when_getty_candidates_are_zero(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Mary Cosby",
            "external_ids": {"imdb": "nm12345678"},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        def _fake_nbcumv_import(*args, progress_cb=None, **kwargs):
            if progress_cb:
                progress_cb(
                    0,
                    0,
                    "No Getty candidates found. Searching NBCUMV directly for "
                    "'Mary Cosby' in 'The Real Housewives of Salt Lake City'...",
                )
                progress_cb(0, 3, "Found 3 NBCUMV caption matches in 'The Real Housewives of Salt Lake City'.")
                progress_cb(3, 3, "Queued NBCUMV direct match 3/3: DIRECT_MATCH_3.JPG")
            return {
                "fetched": 3,
                "imported": 3,
                "skipped": 0,
                "failed": 0,
                "getty_candidates_total": 0,
                "getty_only_imported": 0,
                "gallery_links_created": 3,
                "asset_ids": ["asset-1", "asset-2", "asset-3"],
                "errors": [],
                "summary_message": "NBCUMV direct caption search queued 3 matches.",
            }

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id", return_value=None):
                with patch("trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos", return_value=[]):
                    with patch(
                        "api.routers.admin_person_images._import_nbcumv_person_media",
                        side_effect=_fake_nbcumv_import,
                    ):
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                            json={
                                "sources": ["nbcumv"],
                                "show_name": "The Real Housewives of Salt Lake City",
                                "skip_mirror": True,
                                "skip_auto_count": True,
                                "skip_word_detection": True,
                                "skip_centering": True,
                                "skip_resize": True,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

        assert response.status_code == 200
        payload = response.text.replace("\r\n", "\n")
        assert "Searching NBCUMV directly for 'Mary Cosby'" in payload
        assert "Found 3 NBCUMV caption matches" in payload

    def test_refresh_stream_stops_when_nbcumv_import_reports_cancellation(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Brandi Glanville",
            "external_ids": {"imdb": "nm12345678"},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        def _fake_nbcumv_import(*args, progress_cb=None, **kwargs):
            if progress_cb:
                progress_cb(41, 96, "Importing NBCUMV 42/96: NUP_188900_0696.JPG")
            return {
                "cancelled": True,
                "fetched": 41,
                "imported": 0,
                "skipped": 0,
                "failed": 0,
                "gallery_links_created": 0,
                "asset_ids": [],
                "errors": [],
                "summary_message": "Cancellation requested after importing 0 NBCUMV assets.",
            }

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id", return_value=None):
                with patch("trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos", return_value=[]):
                    with patch(
                        "api.routers.admin_person_images._import_nbcumv_person_media",
                        side_effect=_fake_nbcumv_import,
                    ):
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                            json={
                                "sources": ["nbcumv"],
                                "skip_mirror": True,
                                "skip_auto_count": True,
                                "skip_word_detection": True,
                                "skip_centering": True,
                                "skip_resize": True,
                            },
                            headers={
                                "Authorization": f"Bearer {token}",
                                "x-trr-internal-raw-stream": "1",
                                "x-trr-admin-operation-id": str(uuid4()),
                            },
                        )

        assert response.status_code == 200
        payload = response.text.replace("\r\n", "\n")
        assert "Cancellation requested after importing 0 NBCUMV assets." in payload
        assert "event: complete" not in payload

    def test_ordered_getty_progress_snapshot_preserves_subtask_order_and_breakdown(self):
        snapshot = admin_person_images._ordered_getty_progress_snapshot(
            {
                "status": "completed",
                "phase": "completed",
                "auth_mode": "chrome_profile_cookies",
                "subtasks": {
                    "mirror_imported_assets": {
                        "id": "mirror_imported_assets",
                        "label": "Host Imported Assets",
                        "status": "completed",
                        "current": 3,
                        "total": 3,
                        "message": "Mirrored/hosted Getty and NBCUMV imports.",
                    },
                    "primary_person_search": {
                        "id": "primary_person_search",
                        "label": "Primary Person Search",
                        "status": "completed",
                        "query": "Brandi Glanville Bravo",
                        "query_url": "https://www.gettyimages.com/search/2/image?family=editorial&phrase=Brandi%20Glanville%20Bravo&sort=newest",
                        "site_image_total": 877,
                        "site_event_total": 39,
                        "site_video_total": 0,
                        "candidates_found": 12,
                        "usable_after_dedupe_total": 10,
                        "overlap_count": 2,
                        "current": 12,
                        "total": 12,
                        "message": "Found 12 direct Getty candidates.",
                    },
                },
                "breakdown": {
                    "raw_getty_candidates": 12,
                    "matched_via_nbcumv": 2,
                    "getty_only_imported": 1,
                },
            }
        )

        assert snapshot is not None
        assert snapshot["status"] == "completed"
        assert snapshot["phase"] == "completed"
        assert snapshot["auth_mode"] == "chrome_profile_cookies"
        assert snapshot["subtasks"][0]["id"] == "primary_person_search"
        assert (
            snapshot["subtasks"][0]["query_url"]
            == "https://www.gettyimages.com/search/2/image?family=editorial&phrase=Brandi%20Glanville%20Bravo&sort=newest"
        )
        assert snapshot["subtasks"][0]["site_image_total"] == 877
        assert snapshot["subtasks"][0]["usable_after_dedupe_total"] == 10
        assert snapshot["subtasks"][-1]["id"] == "mirror_imported_assets"
        assert snapshot["breakdown"]["raw_getty_candidates"] == 12
        assert snapshot["breakdown"]["matched_via_nbcumv"] == 2
        assert snapshot["breakdown"]["getty_only_imported"] == 1

    def test_refresh_tmdb_failure_is_non_terminal_and_sets_status_fields(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
                                "sources": ["imdb", "tmdb"],
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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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

    def test_refresh_skips_existing_imdb_repair_when_imdb_not_selected(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Getty Only Person",
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
                                "sources": ["nbcumv"],
                                "skip_mirror": True,
                                "skip_auto_count": True,
                                "skip_word_detection": True,
                                "skip_centering": True,
                                "skip_resize": True,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

        assert response.status_code == 200
        repair_mock.assert_not_called()

    def test_bypasses_show_source_policy_when_disabled(self, client, monkeypatch):
        """enforce_show_source_policy=False should preserve requested sources unchanged."""
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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


def test_resize_person_gallery_images_times_out_stuck_variant_job(monkeypatch):
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
    monkeypatch.setenv("TRR_RESIZE_VARIANT_JOB_TIMEOUT_S", "0.01")

    with patch(
        "api.routers.admin_person_images._fetch_person_media_link_rows",
        return_value=[],
    ):
        with patch(
            "api.routers.admin_image_counts.auto_count_cast_photo",
            side_effect=RuntimeError("detector unavailable"),
        ):
            with patch(
                "trr_backend.media.image_variants.generate_cast_photo_variants",
                side_effect=lambda *args, **kwargs: time.sleep(0.05),
            ) as generate_cast_variants:
                result = admin_person_images._resize_person_gallery_images(
                    mock_db,
                    person_id=str(uuid4()),
                    sources=["imdb"],
                    force=True,
                )

    assert result == (1, 0, 1, 1, 0, 1)
    assert generate_cast_variants.call_count == 2


def test_mirror_person_media_assets_skips_previously_failed_rows_without_force() -> None:
    mock_db = MagicMock()
    asset_id = str(uuid4())

    with patch(
        "api.routers.admin_person_images._fetch_person_media_link_rows",
        return_value=[
            {
                "id": str(uuid4()),
                "media_asset_id": asset_id,
                "source": "fandom",
                "source_url": "https://static.wikia.nocookie.net/example.jpg",
                "hosted_url": None,
                "metadata": {},
                "ingest_status": "failed",
                "ingest_last_error": "404 Client Error",
            }
        ],
    ):
        with patch("trr_backend.media.s3_mirror.mirror_media_asset_row") as mirror_mock:
            with patch("trr_backend.repositories.media_assets.update_ingest_status") as update_status_mock:
                mirrored, failed = admin_person_images._mirror_person_media_assets(mock_db, person_id=str(uuid4()))

    assert mirrored == 0
    assert failed == 0
    mirror_mock.assert_not_called()
    update_status_mock.assert_not_called()


def test_mirror_person_media_assets_recovers_from_duplicate_sha_conflict() -> None:
    mock_db = MagicMock()
    asset_id = str(uuid4())
    hosted_fallback_calls: list[dict[str, object]] = []

    with patch(
        "api.routers.admin_person_images._fetch_person_media_link_rows",
        return_value=[
            {
                "id": str(uuid4()),
                "media_asset_id": asset_id,
                "source": "getty",
                "source_url": "https://media.gettyimages.com/id/example.jpg",
                "hosted_url": None,
                "hosted_key": None,
                "hosted_sha256": None,
                "metadata": {},
                "ingest_status": "pending",
                "ingest_last_error": None,
            }
        ],
    ):
        with patch(
            "trr_backend.media.s3_mirror.mirror_media_asset_row",
            return_value={
                "sha256": "same-bytes",
                "hosted_bucket": "bucket",
                "hosted_key": "images/shared/example.jpg",
                "hosted_url": "https://cdn.example.com/images/shared/example.jpg",
                "hosted_bytes": 1234,
                "hosted_content_type": "image/jpeg",
                "hosted_etag": "etag-1",
                "hosted_at": "2026-03-17T03:00:00+00:00",
                "width": 1200,
                "height": 800,
                "metadata": {"mirrored_from": "https://media.gettyimages.com/id/example.jpg"},
            },
        ):
            with patch(
                "trr_backend.repositories.media_assets.update_asset_with_mirror_result",
                side_effect=RuntimeError(
                    "Supabase error updating mirror result: duplicate key value violates "
                    'unique constraint "media_assets_source_hosted_sha_uq"'
                ),
            ):
                with patch(
                    "trr_backend.repositories.media_assets.update_asset_with_hosted_fields",
                    side_effect=lambda db, asset_id, **kwargs: (
                        hosted_fallback_calls.append({"asset_id": asset_id, **kwargs}) or {}
                    ),
                ):
                    mirrored, failed = admin_person_images._mirror_person_media_assets(
                        mock_db,
                        person_id=str(uuid4()),
                    )

    assert mirrored == 1
    assert failed == 0
    assert hosted_fallback_calls == [
        {
            "asset_id": asset_id,
            "hosted_bucket": "bucket",
            "hosted_key": "images/shared/example.jpg",
            "hosted_url": "https://cdn.example.com/images/shared/example.jpg",
            "hosted_bytes": 1234,
            "hosted_content_type": "image/jpeg",
            "hosted_etag": "etag-1",
            "width": 1200,
            "height": 800,
            "completed_at": "2026-03-17T03:00:00+00:00",
            "metadata": {"mirrored_from": "https://media.gettyimages.com/id/example.jpg"},
        }
    ]

    def test_stream_emits_resizing_stage_and_complete_counters(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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

    def test_stream_complete_payload_includes_symmetric_source_progress(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Mary Cosby",
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
                    with patch(
                        "api.routers.admin_person_images._enrich_cast_photos_with_episode_metadata",
                        return_value=(0, 0),
                    ):
                        with patch(
                            "api.routers.admin_person_images._apply_show_context_to_photos",
                            return_value=(0, 0),
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
                                    response = client.post(
                                        f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                        json={
                                            "sources": ["imdb", "nbcumv"],
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
        json_start = normalized_payload.find("{", data_index)
        json_end = normalized_payload.find("\n\n", json_start)
        if json_end == -1:
            json_end = len(normalized_payload)
        complete_data = json.loads(normalized_payload[json_start:json_end].strip())

        source_progress = complete_data["source_progress"]
        assert source_progress["imdb"]["scraped_current"] == 1
        assert source_progress["imdb"]["saved_current"] == 1
        assert source_progress["imdb"]["status"] == "completed"
        assert source_progress["getty_nbcumv"]["status"] == "completed"
        assert source_progress["getty_nbcumv"]["discovered_total"] == 0
        assert source_progress["getty_nbcumv"]["saved_current"] == 0

    def test_stream_uses_imports_only_hosting_summary_and_warning_source_status(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Brandi Glanville",
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
                    with patch(
                        "api.routers.admin_person_images._enrich_cast_photos_with_episode_metadata",
                        return_value=(0, 0),
                    ):
                        with patch(
                            "api.routers.admin_person_images._apply_show_context_to_photos",
                            return_value=(0, 0),
                        ):
                            with patch(
                                "trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos",
                                return_value=[],
                            ):
                                with patch(
                                    "api.routers.admin_person_images._import_nbcumv_person_media",
                                    return_value={
                                        "fetched": 96,
                                        "imported": 35,
                                        "skipped": 95,
                                        "failed": 1,
                                        "covered_existing": 95,
                                        "nbcumv_only_imported": 35,
                                        "getty_only_imported": 0,
                                        "shared_nbcumv_imported": 0,
                                        "asset_ids": ["asset-1", "asset-2"],
                                        "getty_only_row_ids": [],
                                        "errors": ["1 failed"],
                                        "summary_message": "NBCUMV direct caption search queued 96 matches.",
                                    },
                                ):
                                    with patch(
                                        "api.routers.admin_person_images._mirror_person_media_assets",
                                        return_value=(2, 1),
                                    ) as mirror_media_mock:
                                        with patch(
                                            "api.routers.admin_person_images._mirror_person_photos",
                                            return_value=(0, 0),
                                        ) as mirror_photo_mock:
                                            response = client.post(
                                                f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                                json={
                                                    "sources": ["nbcumv"],
                                                    "skip_auto_count": True,
                                                    "skip_word_detection": True,
                                                    "skip_centering": True,
                                                    "skip_resize": True,
                                                },
                                                headers={"Authorization": f"Bearer {token}"},
                                            )

        assert response.status_code == 200
        mirror_photo_mock.assert_not_called()
        mirror_media_mock.assert_called_once()
        assert mirror_media_mock.call_args.kwargs["asset_ids"] == ["asset-1", "asset-2"]

        normalized_payload = response.text.replace("\r\n", "\n")
        complete_index = normalized_payload.rfind("event: complete")
        assert complete_index >= 0
        data_index = normalized_payload.find("data:", complete_index)
        json_start = normalized_payload.find("{", data_index)
        json_end = normalized_payload.find("\n\n", json_start)
        if json_end == -1:
            json_end = len(normalized_payload)
        complete_data = json.loads(normalized_payload[json_start:json_end].strip())

        assert complete_data["hosting_hosted_total"] == 2
        assert complete_data["hosting_failed_total"] == 1
        assert complete_data["source_progress"]["getty_nbcumv"]["status"] == "warning"

    def test_stream_force_mirrors_existing_getty_only_rows_and_linked_media_assets(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {
            "id": person_id,
            "full_name": "Getty Existing Person",
            "external_ids": {},
        }

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("trr_backend.repositories.cast_tmdb.get_cast_tmdb_by_person_id", return_value=None):
                with patch(
                    "trr_backend.ingestion.cast_photo_sources.fetch_all_cast_photos",
                    return_value=[],
                ):
                    with patch(
                        "api.routers.admin_person_images._import_nbcumv_person_media",
                        return_value={
                            "fetched": 1,
                            "imported": 0,
                            "skipped": 1,
                            "failed": 0,
                            "covered_existing": 1,
                            "nbcumv_only_imported": 0,
                            "getty_only_imported": 0,
                            "shared_nbcumv_imported": 0,
                            "asset_ids": [],
                            "getty_only_row_ids": ["photo-1"],
                            "getty_only_media_asset_ids": ["asset-1"],
                            "getty_repair_row_ids": ["photo-2"],
                            "getty_repair_media_asset_ids": ["asset-2"],
                            "errors": [],
                            "summary_message": "Getty-only fallback rows already existed and were normalized.",
                        },
                    ):
                        with patch(
                            "api.routers.admin_person_images._mirror_person_media_assets",
                            return_value=(1, 0),
                        ) as mirror_media_mock:
                            with patch(
                                "api.routers.admin_person_images._mirror_person_photos",
                                return_value=(1, 0),
                            ) as mirror_photo_mock:
                                response = client.post(
                                    f"/api/v1/admin/person/{person_id}/refresh-images/stream",
                                    json={
                                        "sources": ["nbcumv"],
                                        "skip_auto_count": True,
                                        "skip_word_detection": True,
                                        "skip_centering": True,
                                        "skip_resize": True,
                                    },
                                    headers={"Authorization": f"Bearer {token}"},
                                )

        assert response.status_code == 200
        mirror_photo_mock.assert_called_once()
        assert mirror_photo_mock.call_args.kwargs["photo_ids"] == ["photo-1", "photo-2"]
        assert mirror_photo_mock.call_args.kwargs["force"] is True
        mirror_media_mock.assert_called_once()
        assert mirror_media_mock.call_args.kwargs["asset_ids"] == ["asset-1", "asset-2"]
        assert mirror_media_mock.call_args.kwargs["force"] is True

    def test_stream_skips_imdb_when_source_already_fully_mirrored(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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

    def test_reprocess_stream_runs_metadata_repair_when_enabled(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {"imdb": "nm0001086"}}

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images._load_person_wwhl_episode_imdb_ids_from_credits",
                return_value=set(),
            ):
                with patch(
                    "api.routers.admin_person_images._resolve_imdb_traitors_strict_context",
                    return_value={
                        "strict_mode_enabled": False,
                        "strict_types": set(),
                        "target_person_imdb_id": "nm0001086",
                        "target_person_name": "Test Person",
                        "allowed_cast_imdb_ids": set(),
                        "allowed_cast_names": set(),
                        "allowed_episode_imdb_ids": set(),
                    },
                ):
                    with patch(
                        "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                        return_value=(7, 0),
                    ) as repair_mock:
                        response = client.post(
                            f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                            json={
                                "run_metadata": True,
                                "run_count": False,
                                "run_id_text": False,
                                "run_crop": False,
                                "run_resize": False,
                            },
                            headers={"Authorization": f"Bearer {token}"},
                        )

        assert response.status_code == 200
        repair_mock.assert_called_once()

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

        assert complete_data["metadata_repair_attempted"] == 1
        assert complete_data["existing_imdb_rows_repaired"] == 7
        assert complete_data["metadata_enrichment_failed"] == 0

    def test_reprocess_stream_skips_metadata_repair_when_imdb_is_not_selected(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {"imdb": "nm0001086"}}

        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images._repair_existing_imdb_cast_photos",
                return_value=(7, 0),
            ) as repair_mock:
                response = client.post(
                    f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                    json={
                        "run_metadata": True,
                        "run_count": False,
                        "run_id_text": False,
                        "run_crop": False,
                        "run_resize": False,
                        "sources": ["getty", "nbcumv"],
                    },
                    headers={"Authorization": f"Bearer {token}"},
                )

        assert response.status_code == 200
        repair_mock.assert_not_called()
        assert "Skipping IMDb Details (IMDb source not selected)." in response.text

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

        assert complete_data["metadata_repair_attempted"] == 0

    def test_reprocess_stream_retries_auto_count_failures_and_reports_failed_parts(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
                side_effect=[(10, 7, 3), (3, 2, 1)],
            ) as cast_retry_mock:
                with patch(
                    "api.routers.admin_person_images._auto_count_media_links",
                    side_effect=[(4, 3, 1), (1, 1, 0)],
                ) as media_retry_mock:
                    response = client.post(
                        f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                        json={
                            "run_metadata": False,
                            "run_count": True,
                            "run_id_text": False,
                            "run_crop": False,
                            "run_resize": False,
                        },
                        headers={"Authorization": f"Bearer {token}"},
                    )

        assert response.status_code == 200
        assert cast_retry_mock.call_count == 2
        assert media_retry_mock.call_count == 2

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

        assert complete_data["retry_attempts"]["auto_count"] == 2
        assert complete_data["auto_counts_failed"] == 1
        failed_parts = complete_data.get("failed_parts") or []
        assert any(
            isinstance(part, dict) and part.get("part") == "people_count_face_crops" and int(part.get("failed", 0)) == 1
            for part in failed_parts
        )

    def test_reprocess_stream_honors_run_tagging_alias_when_run_count_disabled(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
                return_value=(2, 2, 0),
            ) as cast_mock:
                with patch(
                    "api.routers.admin_person_images._auto_count_media_links",
                    return_value=(1, 1, 0),
                ) as media_mock:
                    response = client.post(
                        f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                        json={
                            "run_metadata": False,
                            "run_count": False,
                            "run_tagging": True,
                            "run_id_text": False,
                            "run_crop": False,
                            "run_resize": False,
                        },
                        headers={"Authorization": f"Bearer {token}"},
                    )

        assert response.status_code == 200
        assert cast_mock.call_count == 1
        assert media_mock.call_count == 1

    def test_reprocess_stream_run_tagging_takes_precedence_over_run_count(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_person_images._auto_count_cast_photos") as cast_mock:
                with patch("api.routers.admin_person_images._auto_count_media_links") as media_mock:
                    response = client.post(
                        f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                        json={
                            "run_metadata": False,
                            "run_count": True,
                            "run_tagging": False,
                            "run_id_text": False,
                            "run_crop": False,
                            "run_resize": False,
                        },
                        headers={"Authorization": f"Bearer {token}"},
                    )

        assert response.status_code == 200
        assert cast_mock.call_count == 0
        assert media_mock.call_count == 0
        assert "Skipping tagging stage." in response.text

    def test_reprocess_stream_forwards_scoped_targets_to_stage_helpers(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        target_cast_photo_ids = [str(uuid4()), str(uuid4())]
        target_media_link_ids = [str(uuid4())]

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch(
                "api.routers.admin_person_images._auto_count_cast_photos",
                return_value=(2, 2, 0),
            ) as cast_mock:
                with patch(
                    "api.routers.admin_person_images._auto_count_media_links",
                    return_value=(1, 1, 0),
                ) as media_mock:
                    with patch(
                        "api.routers.admin_person_images._recenter_person_gallery_images",
                        return_value=(1, 1, 0, 0),
                    ) as recenter_mock:
                        with patch(
                            "api.routers.admin_person_images._resize_person_gallery_images",
                            return_value=(1, 1, 0, 1, 1, 0),
                        ) as resize_mock:
                            response = client.post(
                                f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                                json={
                                    "run_metadata": False,
                                    "run_count": True,
                                    "run_id_text": False,
                                    "run_crop": True,
                                    "run_resize": True,
                                    "target_cast_photo_ids": target_cast_photo_ids,
                                    "target_media_link_ids": target_media_link_ids,
                                    "prefer_fast_pass": False,
                                },
                                headers={"Authorization": f"Bearer {token}"},
                            )

        assert response.status_code == 200
        assert cast_mock.call_count == 1
        assert media_mock.call_count == 1
        assert recenter_mock.call_count == 1
        assert resize_mock.call_count == 1

        assert cast_mock.call_args.kwargs["photo_ids"] == target_cast_photo_ids
        assert media_mock.call_args.kwargs["media_link_ids"] == target_media_link_ids
        assert recenter_mock.call_args.kwargs["photo_ids"] == target_cast_photo_ids
        assert recenter_mock.call_args.kwargs["media_link_ids"] == target_media_link_ids
        assert resize_mock.call_args.kwargs["photo_ids"] == target_cast_photo_ids
        assert resize_mock.call_args.kwargs["media_link_ids"] == target_media_link_ids

    def test_reprocess_stream_skips_scoped_stages_when_scope_targets_are_empty(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_person_images._auto_count_cast_photos") as cast_mock:
                with patch("api.routers.admin_person_images._auto_count_media_links") as media_mock:
                    with patch("api.routers.admin_person_images._recenter_person_gallery_images") as recenter_mock:
                        with patch("api.routers.admin_person_images._resize_person_gallery_images") as resize_mock:
                            response = client.post(
                                f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                                json={
                                    "run_metadata": False,
                                    "run_count": True,
                                    "run_id_text": False,
                                    "run_crop": True,
                                    "run_resize": True,
                                    "target_cast_photo_ids": [],
                                    "target_media_link_ids": [],
                                },
                                headers={"Authorization": f"Bearer {token}"},
                            )

        assert response.status_code == 200
        assert cast_mock.call_count == 0
        assert media_mock.call_count == 0
        assert recenter_mock.call_count == 0
        assert resize_mock.call_count == 0
        assert "Skipping tagging stage (no scoped targets)." in response.text
        assert "Skipping centering/cropping stage (no scoped targets)." in response.text
        assert "Skipping resize stage (no scoped targets)." in response.text

    def test_reprocess_stream_accepts_getty_source_filter(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            response = client.post(
                f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                json={
                    "run_metadata": False,
                    "run_count": False,
                    "run_tagging": False,
                    "run_id_text": False,
                    "run_crop": False,
                    "run_resize": False,
                    "sources": ["getty"],
                },
                headers={"Authorization": f"Bearer {token}"},
            )

        assert response.status_code == 200

    def test_reprocess_stream_stops_when_admin_operation_cancel_is_requested(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        person_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

        person_data = {"id": person_id, "full_name": "Test Person", "external_ids": {}}
        mock_db = MagicMock()
        mock_response = MagicMock()
        mock_response.data = [person_data]
        mock_response.error = None
        query = mock_db.schema.return_value.table.return_value.select.return_value.eq.return_value.limit.return_value
        query.execute.return_value = mock_response

        resize_mock = MagicMock(return_value=(1, 1, 0, 1, 1, 0))

        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
            with patch("api.routers.admin_person_images.admin_operations.is_cancel_requested", return_value=True):
                with patch("api.routers.admin_person_images._resize_person_gallery_images", resize_mock):
                    response = client.post(
                        f"/api/v1/admin/person/{person_id}/reprocess-images/stream",
                        json={
                            "run_metadata": False,
                            "run_count": False,
                            "run_tagging": False,
                            "run_id_text": False,
                            "run_crop": False,
                            "run_resize": True,
                        },
                        headers={
                            "Authorization": f"Bearer {token}",
                            "x-trr-internal-raw-stream": "1",
                            "x-trr-admin-operation-id": str(uuid4()),
                        },
                    )

        assert response.status_code == 200
        assert "Cancellation requested. Stopping worker..." in response.text
        assert "event: complete" not in response.text
        resize_mock.assert_not_called()


class TestUpdateFacebankSeed:
    """Test PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed."""

    def test_allows_allowlist_user_and_syncs_reference_candidate(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret-32-bytes-minimum-abcdef", "admin@example.com")

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
                with patch("api.routers.admin_person_images.face_references.sync_face_reference_image") as sync_mock:
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
        sync_mock.assert_called_once_with(link_id=link_id, enabled=True)

    def test_allows_allowlist_user(self, client, monkeypatch):
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret-32-bytes-minimum-abcdef", "admin@example.com")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret-32-bytes-minimum-abcdef", "admin@example.com")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret-32-bytes-minimum-abcdef", "admin@example.com")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        other_person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret-32-bytes-minimum-abcdef", "admin@example.com")

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
        monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
        monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@example.com")
        person_id = str(uuid4())
        link_id = str(uuid4())
        token = _make_allowlist_user_token("test-secret-32-bytes-minimum-abcdef", "admin@example.com")

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


def test_owner_face_crop_payload_accepts_moderate_similarity() -> None:
    """Faces matched at 65-80% similarity should generate owner crop payloads."""
    face_boxes = [
        {
            "x": 0.1,
            "y": 0.2,
            "width": 0.2,
            "height": 0.3,
            "confidence": 0.916,
            "person_id": "11111111-1111-1111-1111-111111111111",
            "match_status": "matched",
            "match_similarity": 0.765,
            "match_reason": "matched",
        },
        {
            "x": 0.6,
            "y": 0.2,
            "width": 0.2,
            "height": 0.3,
            "confidence": 0.893,
            "match_status": "below_threshold",
            "match_similarity": 0.078,
            "match_reason": "below_threshold",
        },
    ]
    result = admin_person_images._owner_face_crop_payload(
        face_boxes,
        owner_person_id="11111111-1111-1111-1111-111111111111",
    )
    assert result is not None, "76.5% similarity should pass crop threshold"
    assert result["mode"] == "auto"
    assert result["strategy"] == "owner_face_box_v1"
    assert result["x"] < 50  # Face 1 is at x=0.1, center ~0.2 -> ~20%


def test_owner_face_crop_payload_accepts_cross_face_lead_override() -> None:
    """Cross-face lead override matches at ~55% similarity should generate crops."""
    face_boxes = [
        {
            "x": 0.5,
            "y": 0.2,
            "width": 0.2,
            "height": 0.3,
            "confidence": 0.757,
            "person_id": "11111111-1111-1111-1111-111111111111",
            "match_status": "matched",
            "match_similarity": 0.55,
            "match_reason": "cross_face_lead_override",
        },
    ]
    result = admin_person_images._owner_face_crop_payload(
        face_boxes,
        owner_person_id="11111111-1111-1111-1111-111111111111",
    )
    assert result is not None, "55% similarity cross_face_lead_override should pass"
    assert result["strategy"] == "owner_face_box_v1"


def test_owner_face_crop_payload_uses_square_crop_bbox() -> None:
    """When square_crop_bbox is available, use it for crop center computation."""
    face_boxes = [
        {
            "x": 0.4,
            "y": 0.3,
            "width": 0.1,
            "height": 0.15,
            "confidence": 0.90,
            "person_id": "owner-id",
            "match_status": "matched",
            "match_similarity": 0.80,
            "match_reason": "matched",
            "square_crop_bbox": [0.3, 0.2, 0.6, 0.5],  # center = (0.45, 0.35)
        },
    ]
    result = admin_person_images._owner_face_crop_payload(
        face_boxes,
        owner_person_id="owner-id",
    )
    assert result is not None
    # With square_crop_bbox [0.3, 0.2, 0.6, 0.5]:
    # cx = (0.3 + 0.6) / 2 = 0.45 -> x = 45.0
    # scb_height = 0.5 - 0.2 = 0.3
    # cy = 0.2 + (0.3 * 0.45) = 0.335 -> y = 33.5
    assert result["strategy"] == "owner_face_box_v1"
    assert result["x"] == 45.0, f"Expected x=45.0 (scb center), got {result['x']}"
    assert result["y"] == 33.5, f"Expected y=33.5 (scb weighted center), got {result['y']}"


def test_similarity_lead_does_not_overwrite_already_matched_same_person() -> None:
    """If a box is already matched for the same person, don't overwrite with lead_override."""
    boxes = [
        {
            "index": 1,
            "x": 0.1,
            "y": 0.2,
            "width": 0.2,
            "height": 0.3,
            "confidence": 0.91,
            "person_id": "11111111-1111-1111-1111-111111111111",
            "person_name": "Alan Cumming",
            "label": "Alan Cumming",
            "label_source": "identity_match",
            "match_status": "matched",
            "match_reason": "matched",
            "match_similarity": 0.765,
            "match_candidates": [
                {
                    "person_id": "11111111-1111-1111-1111-111111111111",
                    "person_name": "Alan Cumming",
                    "similarity": 0.765,
                },
            ],
        },
        {
            "index": 2,
            "x": 0.6,
            "y": 0.2,
            "width": 0.2,
            "height": 0.3,
            "confidence": 0.88,
            "label_source": "generic",
            "match_status": "below_threshold",
            "match_reason": "below_threshold",
            "match_similarity": 0.078,
            "match_candidates": [
                {
                    "person_id": "11111111-1111-1111-1111-111111111111",
                    "person_name": "Alan Cumming",
                    "similarity": 0.078,
                },
            ],
        },
    ]
    admin_person_images._apply_similarity_lead_assignments(
        boxes,
        tagged_people_ids=["11111111-1111-1111-1111-111111111111"],
        tagged_people_names=["Alan Cumming"],
    )
    # Face 1 should keep original match_reason, NOT be overwritten to cross_face_lead_override
    assert boxes[0]["match_reason"] == "matched", (
        f"Expected 'matched' but got '{boxes[0]['match_reason']}' — "
        "already-matched faces should not be overwritten by lead_override"
    )
    assert boxes[0]["label_source"] == "identity_match"


def test_should_recenter_auto_crop_accepts_owner_face_box_v1() -> None:
    """owner_face_box_v1 crops should be recognized as stable (skip recentering)."""
    crop = {
        "x": 35.0,
        "y": 42.0,
        "zoom": 1.35,
        "mode": "auto",
        "strategy": "owner_face_box_v1",
    }
    assert admin_person_images._should_recenter_auto_crop(crop) is False, (
        "owner_face_box_v1 should be recognized as a stable auto crop"
    )
