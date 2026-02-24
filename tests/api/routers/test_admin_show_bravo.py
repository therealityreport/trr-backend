"""Tests for admin show Bravo import endpoints."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api.main import app
from api.routers.admin_show_bravo import (
    _dedupe_items,
    _merge_external_ids_fill_missing,
    _persist_pending_links_from_bravo_sync,
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


def _read_sse_events(raw_payload: str, event_type: str) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    normalized = raw_payload.replace("\r\n", "\n")
    for chunk in normalized.split("\n\n"):
        if not chunk.strip():
            continue
        lines = [line for line in chunk.split("\n") if line.strip()]
        current_event = "message"
        data_lines: list[str] = []
        for line in lines:
            if line.startswith("event:"):
                current_event = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].strip())
        if current_event != event_type:
            continue
        if not data_lines:
            events.append({})
            continue
        payload_text = "\n".join(data_lines)
        events.append(json.loads(payload_text))
    return events


def test_preview_bravo_import_returns_expected_shape(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                    return_value={
                        "show": {"title": "The Valley", "description": "desc"},
                        "people": [{"canonical_url": "https://www.bravotv.com/people/janet-caperna"}],
                        "videos": [{"title": "The Valley Persian Style", "clip_url": "https://www.bravotv.com/v/1"}],
                        "news": [{"headline": "A headline", "article_url": "https://www.bravotv.com/n/1"}],
                        "image_candidates": [{"url": "https://www.bravotv.com/i/1.jpg"}],
                        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
                    },
                ):
                    response = client.post(
                        f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"show_url": "https://www.bravotv.com/the-valley"},
                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["show"]["title"] == "The Valley"
    assert len(payload["people"]) == 1
    assert len(payload["videos"]) == 1
    assert len(payload["news"]) == 1
    assert len(payload["image_candidates"]) == 1


def test_preview_bravo_import_includes_fandom_probe_fields(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()
    fandom_url = "https://real-housewives.fandom.com/wiki/Andy_Cohen"

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo._build_show_cast_index",
                    return_value=[{"person_id": str(uuid4()), "person_name": "Andy Cohen"}],
                ):
                    with patch(
                        "api.routers.admin_show_bravo._load_fandom_probe_domains",
                        return_value=["real-housewives.fandom.com"],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._probe_fandom_person_url_candidates",
                            return_value=[
                                {
                                    "candidate_url": fandom_url,
                                    "url": fandom_url,
                                    "status": "ok",
                                    "person": {"canonical_url": fandom_url, "name": "Andy Cohen"},
                                }
                            ],
                        ):
                            with patch(
                                "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                                return_value={
                                    "show": {"title": "The Real Housewives of Salt Lake City"},
                                    "people": [],
                                    "videos": [],
                                    "news": [],
                                    "image_candidates": [],
                                    "discovered_person_urls": [],
                                    "person_candidate_results": [],
                                },
                            ):
                                response = client.post(
                                    f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={
                                        "show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city",
                                        "cast_only": True,
                                    },
                                )

    assert response.status_code == 200
    payload = response.json()
    assert payload["fandom_domains_used"] == ["real-housewives.fandom.com"]
    assert payload["fandom_candidates_tested"] == 1
    assert payload["fandom_candidates_valid"] == 1
    assert payload["fandom_candidates_missing"] == 0
    assert payload["fandom_candidates_errors"] == 0
    assert payload["fandom_candidate_results"][0]["url"] == fandom_url


def test_preview_bravo_import_includes_cast_candidate_urls(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo._build_show_cast_index",
                    return_value=[{"person_id": str(uuid4()), "person_name": "Brooks Marks"}],
                ):
                    with patch(
                        "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                        return_value={
                            "show": {"title": "The Real Housewives of Salt Lake City"},
                            "people": [],
                            "videos": [],
                            "news": [],
                            "image_candidates": [],
                            "discovered_person_urls": [],
                        },
                    ) as parse_mock:
                        response = client.post(
                            f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                            headers={"Authorization": f"Bearer {token}"},
                            json={
                                "show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city",
                                "person_url_candidates": ["https://www.bravotv.com/people/andy-cohen"],
                            },
                        )

    assert response.status_code == 200
    called_candidates = parse_mock.call_args.kwargs.get("person_url_candidates")
    assert called_candidates is not None
    assert "https://www.bravotv.com/people/andy-cohen" in called_candidates
    assert "https://www.bravotv.com/people/brooks-marks" in called_candidates


def test_preview_bravo_import_skips_existing_and_na_bravo_profiles(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    lisa_id = str(uuid4())
    andy_id = str(uuid4())
    john_id = str(uuid4())
    mock_db = MagicMock()

    cast_rows = [
        {"person_id": lisa_id, "person_name": "Lisa Barlow"},
        {"person_id": andy_id, "person_name": "Andy Cohen"},
        {"person_id": john_id, "person_name": "John Barlow"},
    ]

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo._build_show_cast_index", return_value=cast_rows):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={
                            lisa_id: {"has_non_rejected": True, "has_na": False},
                            john_id: {"has_non_rejected": False, "has_na": True},
                        },
                    ):
                        with patch(
                            "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                            return_value={
                                "show": {"title": "The Real Housewives of Salt Lake City"},
                                "people": [],
                                "videos": [],
                                "news": [],
                                "image_candidates": [],
                                "discovered_person_urls": [],
                                "person_candidate_results": [],
                            },
                        ) as parse_mock:
                            response = client.post(
                                f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                                headers={"Authorization": f"Bearer {token}"},
                                json={
                                    "show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city",
                                    "person_url_candidates": [
                                        "https://www.bravotv.com/people/lisa-barlow",
                                        "https://www.bravotv.com/people/andy-cohen",
                                        "https://www.bravotv.com/people/john-barlow",
                                    ],
                                },
                            )

    assert response.status_code == 200
    called_candidates = parse_mock.call_args.kwargs.get("person_url_candidates")
    assert called_candidates == ["https://www.bravotv.com/people/andy-cohen"]


def test_preview_bravo_import_cast_only_probes_all_cast_profiles_even_if_existing_or_na(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    lisa_id = str(uuid4())
    andy_id = str(uuid4())
    john_id = str(uuid4())
    mock_db = MagicMock()

    cast_rows = [
        {"person_id": lisa_id, "person_name": "Lisa Barlow"},
        {"person_id": andy_id, "person_name": "Andy Cohen"},
        {"person_id": john_id, "person_name": "John Barlow"},
    ]

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo._build_show_cast_index", return_value=cast_rows):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={
                            lisa_id: {"has_non_rejected": True, "has_na": False},
                            john_id: {"has_non_rejected": False, "has_na": True},
                        },
                    ):
                        with patch(
                            "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                            return_value={
                                "show": {"title": "The Real Housewives of Salt Lake City"},
                                "people": [],
                                "videos": [],
                                "news": [],
                                "image_candidates": [],
                                "discovered_person_urls": [],
                                "person_candidate_results": [],
                            },
                        ) as parse_mock:
                            response = client.post(
                                f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                                headers={"Authorization": f"Bearer {token}"},
                                json={
                                    "show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city",
                                    "cast_only": True,
                                },
                            )

    assert response.status_code == 200
    called_candidates = parse_mock.call_args.kwargs.get("person_url_candidates")
    assert called_candidates is not None
    assert set(called_candidates) == {
        "https://www.bravotv.com/people/lisa-barlow",
        "https://www.bravotv.com/people/andy-cohen",
        "https://www.bravotv.com/people/john-barlow",
    }


def test_preview_bravo_import_returns_probe_counters_from_candidate_results(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                    return_value={
                        "show": {"title": "The Real Housewives of Salt Lake City"},
                        "people": [],
                        "videos": [],
                        "news": [],
                        "image_candidates": [],
                        "discovered_person_urls": [],
                        "person_candidate_results": [
                            {"url": "https://www.bravotv.com/people/andy-cohen", "status": "ok"},
                            {"url": "https://www.bravotv.com/people/john-barlow", "status": "missing"},
                            {
                                "url": "https://www.bravotv.com/people/heather-gay",
                                "status": "error",
                                "error": "request_failed",
                            },
                        ],
                    },
                ):
                    response = client.post(
                        f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city"},
                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["bravo_candidates_tested"] == 3
    assert payload["bravo_candidates_valid"] == 1
    assert payload["bravo_candidates_missing"] == 1
    assert payload["bravo_candidates_errors"] == 1
    assert len(payload["person_candidate_results"]) == 3


def test_preview_bravo_import_stream_emits_start_progress_and_complete(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    andy_url = "https://www.bravotv.com/people/andy-cohen"
    john_url = "https://www.bravotv.com/people/john-barlow"
    heather_url = "https://www.bravotv.com/people/heather-gay"
    andy_fandom_url = "https://real-housewives.fandom.com/wiki/Andy_Cohen"
    john_fandom_url = "https://real-housewives.fandom.com/wiki/John_Barlow"
    heather_fandom_url = "https://real-housewives.fandom.com/wiki/Heather_Gay"
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo._build_show_cast_index",
                    return_value=[
                        {"person_id": str(uuid4()), "person_name": "Andy Cohen"},
                        {"person_id": str(uuid4()), "person_name": "John Barlow"},
                        {"person_id": str(uuid4()), "person_name": "Heather Gay"},
                    ],
                ):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={},
                    ):
                        with patch(
                            "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                            return_value={
                                "show": {"title": "The Real Housewives of Salt Lake City"},
                                "videos": [],
                                "news": [],
                                "image_candidates": [],
                                "discovered_person_urls": [],
                            },
                        ):
                            with patch(
                                "api.routers.admin_show_bravo._probe_single_bravo_candidate",
                                side_effect=lambda candidate_url, **_: (
                                    {
                                        "candidate_url": andy_url,
                                        "url": andy_url,
                                        "status": "ok",
                                        "person": {
                                            "canonical_url": andy_url,
                                            "name": "Andy Cohen",
                                        },
                                    }
                                    if candidate_url == andy_url
                                    else {
                                        "candidate_url": john_url,
                                        "url": john_url,
                                        "status": "missing",
                                    }
                                    if candidate_url == john_url
                                    else {
                                        "candidate_url": heather_url,
                                        "url": heather_url,
                                        "status": "error",
                                        "error": "request_failed",
                                    }
                                ),
                            ):
                                with patch(
                                    "api.routers.admin_show_bravo._probe_single_fandom_candidate",
                                    side_effect=lambda candidate_url, **_: (
                                        {
                                            "candidate_url": andy_fandom_url,
                                            "url": andy_fandom_url,
                                            "status": "ok",
                                            "person": {
                                                "canonical_url": andy_fandom_url,
                                                "name": "Andy Cohen",
                                            },
                                        }
                                        if candidate_url == andy_fandom_url
                                        else {
                                            "candidate_url": john_fandom_url,
                                            "url": john_fandom_url,
                                            "status": "missing",
                                        }
                                        if candidate_url == john_fandom_url
                                        else {
                                            "candidate_url": heather_fandom_url,
                                            "url": heather_fandom_url,
                                            "status": "error",
                                            "error": "request_failed",
                                        }
                                    ),
                                ):
                                    response = client.post(
                                        f"/api/v1/admin/shows/{show_id}/import-bravo/preview/stream",
                                        headers={"Authorization": f"Bearer {token}"},
                                        json={
                                            "show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city",
                                            "cast_only": True,
                                            "include_people": True,
                                            "include_videos": False,
                                            "include_news": False,
                                        },
                                    )

    assert response.status_code == 200
    start_events = _read_sse_events(response.text, "start")
    progress_events = _read_sse_events(response.text, "progress")
    complete_events = _read_sse_events(response.text, "complete")

    assert len(start_events) == 1
    assert start_events[0]["total"] == 3
    assert len(progress_events) == 12
    assert sum(1 for event in progress_events if event.get("status") == "in_progress") == 6
    bravo_terminal_events = [
        event
        for event in progress_events
        if event.get("source") == "bravo" and str(event.get("status") or "").lower() in {"ok", "missing", "error"}
    ]
    fandom_terminal_events = [
        event
        for event in progress_events
        if event.get("source") == "fandom" and str(event.get("status") or "").lower() in {"ok", "missing", "error"}
    ]
    assert len(bravo_terminal_events) == 3
    assert {event["status"] for event in bravo_terminal_events} == {"ok", "missing", "error"}
    assert len(fandom_terminal_events) == 3
    assert {event["status"] for event in fandom_terminal_events} == {"ok", "missing", "error"}
    for event in bravo_terminal_events:
        assert isinstance(event.get("candidate_index"), int)
        assert isinstance(event.get("elapsed_ms"), int)
    assert bravo_terminal_events[-1]["bravo_candidates_tested"] == 3
    assert bravo_terminal_events[-1]["bravo_candidates_valid"] == 1
    assert bravo_terminal_events[-1]["bravo_candidates_missing"] == 1
    assert bravo_terminal_events[-1]["bravo_candidates_errors"] == 1
    assert len(complete_events) == 1
    assert complete_events[0]["bravo_candidates_tested"] == 3
    assert complete_events[0]["bravo_candidates_valid"] == 1
    assert complete_events[0]["bravo_candidates_missing"] == 1
    assert complete_events[0]["bravo_candidates_errors"] == 1
    assert complete_events[0]["fandom_candidates_tested"] == 3
    assert complete_events[0]["fandom_candidates_valid"] == 1
    assert complete_events[0]["fandom_candidates_missing"] == 1
    assert complete_events[0]["fandom_candidates_errors"] == 1
    assert complete_events[0]["show_url"] == "https://www.bravotv.com/the-real-housewives-of-salt-lake-city"
    assert complete_events[0]["cast_only"] is True


def test_preview_bravo_import_stream_cast_only_probes_all_cast_profiles_even_if_existing_or_na(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    lisa_id = str(uuid4())
    andy_id = str(uuid4())
    john_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo._build_show_cast_index",
                    return_value=[
                        {"person_id": lisa_id, "person_name": "Lisa Barlow"},
                        {"person_id": andy_id, "person_name": "Andy Cohen"},
                        {"person_id": john_id, "person_name": "John Barlow"},
                    ],
                ):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={
                            lisa_id: {"has_non_rejected": True, "has_na": False},
                            john_id: {"has_non_rejected": False, "has_na": True},
                        },
                    ):
                        with patch(
                            "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                            return_value={
                                "show": {"title": "The Real Housewives of Salt Lake City"},
                                "videos": [],
                                "news": [],
                                "image_candidates": [],
                                "discovered_person_urls": [],
                            },
                        ) as parse_mock:
                            with patch(
                                "api.routers.admin_show_bravo.probe_bravo_person_url_candidates",
                                return_value=iter([]),
                            ):
                                response = client.post(
                                    f"/api/v1/admin/shows/{show_id}/import-bravo/preview/stream",
                                    headers={"Authorization": f"Bearer {token}"},
                                    json={
                                        "show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city",
                                        "cast_only": True,
                                        "include_videos": False,
                                        "include_news": False,
                                    },
                                )

    assert response.status_code == 200
    called_candidates = parse_mock.call_args.kwargs.get("person_url_candidates")
    assert called_candidates is not None
    assert set(called_candidates) == {
        "https://www.bravotv.com/people/lisa-barlow",
        "https://www.bravotv.com/people/andy-cohen",
        "https://www.bravotv.com/people/john-barlow",
    }


def test_preview_bravo_import_stream_emits_error_event_on_fatal_failure(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo._build_show_cast_index", return_value=[]):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={},
                    ):
                        with patch(
                            "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                            side_effect=RuntimeError("boom"),
                        ):
                            response = client.post(
                                f"/api/v1/admin/shows/{show_id}/import-bravo/preview/stream",
                                headers={"Authorization": f"Bearer {token}"},
                                json={
                                    "show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city",
                                    "cast_only": True,
                                    "include_people": True,
                                    "include_videos": False,
                                    "include_news": False,
                                },
                            )

    assert response.status_code == 200
    error_events = _read_sse_events(response.text, "error")
    assert len(error_events) == 1
    assert error_events[0]["error"] == "Bravo preview stream failed"
    assert error_events[0]["status"] == 500
    assert "boom" in str(error_events[0]["detail"])


def test_preview_bravo_import_sets_max_people_to_candidate_count_over_default(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()
    cast_rows = [{"person_id": str(uuid4()), "person_name": f"Cast Member {index}"} for index in range(1, 56)]

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo._build_show_cast_index", return_value=cast_rows):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={},
                    ):
                        with patch(
                            "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                            return_value={
                                "show": {"title": "The Real Housewives of Salt Lake City"},
                                "people": [],
                                "videos": [],
                                "news": [],
                                "image_candidates": [],
                                "discovered_person_urls": [],
                                "person_candidate_results": [],
                            },
                        ) as parse_mock:
                            response = client.post(
                                f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                                headers={"Authorization": f"Bearer {token}"},
                                json={"show_url": "https://www.bravotv.com/the-real-housewives-of-salt-lake-city"},
                            )

    assert response.status_code == 200
    called_candidates = parse_mock.call_args.kwargs.get("person_url_candidates") or []
    assert len(called_candidates) == 55
    assert parse_mock.call_args.kwargs.get("max_people") == 55


def test_commit_bravo_import_returns_snapshot_metadata(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [{"title": "The Valley Persian Style", "clip_url": "https://www.bravotv.com/v/1"}],
        "news": [{"headline": "A headline", "article_url": "https://www.bravotv.com/n/1"}],
        "people": [
            {
                "canonical_url": "https://www.bravotv.com/people/janet-caperna",
                "name": "Janet Caperna",
                "bio": "Bio",
                "hero_image_url": "https://www.bravotv.com/p/1.jpg",
                "social_links": {"instagram": "janetcaperna"},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                    with patch(
                        "api.routers.admin_show_bravo._build_show_cast_index",
                        return_value=[{"person_id": person_id, "person_name": "Janet Caperna"}],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._upsert_show_snapshot",
                            return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                        ):
                            with patch("api.routers.admin_show_bravo._persist_show_description"):
                                with patch(
                                    "api.routers.admin_show_bravo._upsert_person_snapshot",
                                    return_value={"person_id": person_id, "source_id": "bravo", "variant": "default"},
                                ):
                                    with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                        with patch(
                                            "api.routers.admin_show_bravo._import_bravo_person_image",
                                            return_value={"imported": 1, "skipped": 0, "errors": []},
                                        ):
                                            with patch(
                                                "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync",
                                                return_value=0,
                                            ):
                                                with patch(
                                                    "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync",
                                                    return_value={
                                                        "role_suggestions": 0,
                                                        "role_assignments": 0,
                                                        "announcement_people": 0,
                                                    },
                                                ):
                                                    response = client.post(
                                                        f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                        headers={"Authorization": f"Bearer {token}"},
                                                        json={"show_url": "https://www.bravotv.com/the-valley"},
                                                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["show_snapshot"]["source_id"] == "bravo"
    assert len(payload["person_snapshots"]) == 1
    assert payload["counts"]["people_updated"] == 1


def test_commit_bravo_import_promotes_hosted_profile_media_for_people(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()
    hosted_profile_url = "https://cdn.example.com/cast/profile.jpg"

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [],
        "news": [],
        "people": [
            {
                "canonical_url": "https://www.bravotv.com/people/janet-caperna",
                "name": "Janet Caperna",
                "bio": "Bio",
                "hero_image_url": "https://www.bravotv.com/p/1.jpg",
                "social_links": {"instagram": "janetcaperna"},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                    with patch(
                        "api.routers.admin_show_bravo._build_show_cast_index",
                        return_value=[{"person_id": person_id, "person_name": "Janet Caperna"}],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._upsert_show_snapshot",
                            return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                        ):
                            with patch("api.routers.admin_show_bravo._persist_show_description"):
                                with patch(
                                    "api.routers.admin_show_bravo._upsert_person_snapshot",
                                    return_value={"person_id": person_id, "source_id": "bravo", "variant": "default"},
                                ):
                                    with patch("api.routers.admin_show_bravo._persist_person_profile") as persist_mock:
                                        with patch(
                                            "api.routers.admin_show_bravo._import_bravo_person_image",
                                            return_value={
                                                "imported": 1,
                                                "skipped": 0,
                                                "errors": [],
                                                "asset_ids": ["asset-1"],
                                                "primary_hosted_url": hosted_profile_url,
                                            },
                                        ):
                                            with patch(
                                                "api.routers.admin_show_bravo._promote_bravo_profile_media_link"
                                            ) as promote_mock:
                                                with patch(
                                                    "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync",
                                                    return_value=0,
                                                ):
                                                    with patch(
                                                        "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync",
                                                        return_value={
                                                            "role_suggestions": 0,
                                                            "role_assignments": 0,
                                                            "announcement_people": 0,
                                                        },
                                                    ):
                                                        response = client.post(
                                                            f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                            headers={"Authorization": f"Bearer {token}"},
                                                            json={"show_url": "https://www.bravotv.com/the-valley"},
                                                        )

    assert response.status_code == 200
    promote_mock.assert_called_once()
    promote_kwargs = promote_mock.call_args.kwargs
    assert promote_kwargs["person_id"] == person_id
    assert promote_kwargs["media_asset_id"] == "asset-1"
    assert promote_kwargs["season_number"] is None
    assert any(call.kwargs.get("hero_image_url") == hosted_profile_url for call in persist_mock.call_args_list)


def test_commit_bravo_import_cast_only_skips_show_side_effects(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [{"title": "Clip", "clip_url": "https://www.bravotv.com/v/1"}],
        "news": [{"headline": "Headline", "article_url": "https://www.bravotv.com/n/1"}],
        "people": [],
        "image_candidates": [],
        "discovered_person_urls": [],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo._build_show_cast_index", return_value=[]):
                    with patch(
                        "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                        return_value=bundle,
                    ) as parse_mock:
                        with patch(
                            "api.routers.admin_show_bravo._upsert_show_snapshot",
                            return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                        ):
                            with patch("api.routers.admin_show_bravo._persist_show_description") as persist_show_mock:
                                with patch(
                                    "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync"
                                ) as pending_links_mock:
                                    with patch(
                                        "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync"
                                    ) as role_suggestions_mock:
                                        response = client.post(
                                            f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                            headers={"Authorization": f"Bearer {token}"},
                                            json={
                                                "show_url": "https://www.bravotv.com/the-valley",
                                                "cast_only": True,
                                                "person_url_candidates": ["https://www.bravotv.com/people/andy-cohen"],
                                            },
                                        )

    assert response.status_code == 200
    assert parse_mock.call_args.kwargs.get("include_videos") is False
    assert parse_mock.call_args.kwargs.get("include_news") is False
    assert parse_mock.call_args.kwargs.get("include_person_related_content") is False
    assert parse_mock.call_args.kwargs.get("hydrate_person_related_dates") is False
    assert parse_mock.call_args.kwargs.get("person_url_candidates") == ["https://www.bravotv.com/people/andy-cohen"]
    persist_show_mock.assert_not_called()
    pending_links_mock.assert_not_called()
    role_suggestions_mock.assert_not_called()


def test_commit_bravo_import_cast_only_reuses_preview_result_without_reprobe(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    andy_id = str(uuid4())
    mock_db = MagicMock()
    andy_url = "https://www.bravotv.com/people/andy-cohen"
    john_url = "https://www.bravotv.com/people/john-barlow"

    cast_rows = [
        {"person_id": andy_id, "person_name": "Andy Cohen"},
        {"person_id": str(uuid4()), "person_name": "John Barlow"},
    ]
    preview_result = {
        "show": {"canonical_url": "https://www.bravotv.com/the-valley", "title": "The Valley"},
        "show_url": "https://www.bravotv.com/the-valley",
        "cast_only": True,
        "season_filter": None,
        "people": [
            {
                "canonical_url": andy_url,
                "name": "Andy Cohen",
                "bio": "Bio",
                "hero_image_url": None,
                "social_links": {},
                "videos": [],
                "news": [],
            }
        ],
        "videos": [],
        "news": [],
        "image_candidates": [],
        "discovered_person_urls": [andy_url],
        "person_candidate_results": [{"url": andy_url, "status": "ok"}, {"url": john_url, "status": "ok"}],
        "cast_candidate_urls_tested": [andy_url, john_url],
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo._build_show_cast_index", return_value=cast_rows):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={},
                    ):
                        with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle") as parse_mock:
                            with patch(
                                "api.routers.admin_show_bravo._upsert_show_snapshot",
                                return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                            ):
                                with patch(
                                    "api.routers.admin_show_bravo._persist_show_description"
                                ) as persist_show_mock:
                                    with patch(
                                        "api.routers.admin_show_bravo._upsert_person_snapshot",
                                        return_value={
                                            "person_id": andy_id,
                                            "source_id": "bravo",
                                            "variant": "default",
                                        },
                                    ):
                                        with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                            with patch(
                                                "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync"
                                            ) as pending_links_mock:
                                                with patch(
                                                    "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync"
                                                ) as role_suggestions_mock:
                                                    response = client.post(
                                                        f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                        headers={"Authorization": f"Bearer {token}"},
                                                        json={
                                                            "show_url": "https://www.bravotv.com/the-valley",
                                                            "cast_only": True,
                                                            "preview_result": preview_result,
                                                        },
                                                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["counts"]["people_updated"] == 1
    parse_mock.assert_not_called()
    persist_show_mock.assert_not_called()
    pending_links_mock.assert_not_called()
    role_suggestions_mock.assert_not_called()


def test_commit_bravo_import_cast_only_reports_fandom_counts_from_preview_result(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    andy_id = str(uuid4())
    john_id = str(uuid4())
    mock_db = MagicMock()
    andy_url = "https://www.bravotv.com/people/andy-cohen"
    john_url = "https://www.bravotv.com/people/john-barlow"
    andy_fandom_url = "https://real-housewives.fandom.com/wiki/Andy_Cohen"
    john_fandom_url = "https://real-housewives.fandom.com/wiki/John_Barlow"

    cast_rows = [
        {"person_id": andy_id, "person_name": "Andy Cohen"},
        {"person_id": john_id, "person_name": "John Barlow"},
    ]
    preview_result = {
        "show": {"canonical_url": "https://www.bravotv.com/the-valley", "title": "The Valley"},
        "show_url": "https://www.bravotv.com/the-valley",
        "cast_only": True,
        "season_filter": None,
        "people": [],
        "videos": [],
        "news": [],
        "image_candidates": [],
        "discovered_person_urls": [andy_url, john_url],
        "person_candidate_results": [{"url": andy_url, "status": "ok"}, {"url": john_url, "status": "ok"}],
        "cast_candidate_urls_tested": [andy_url, john_url],
        "fandom_domains_used": ["real-housewives.fandom.com"],
        "fandom_candidate_urls_tested": [andy_fandom_url, john_fandom_url],
        "fandom_candidate_results": [
            {"url": andy_fandom_url, "status": "ok", "person": {"name": "Andy Cohen"}},
            {"url": john_fandom_url, "status": "missing"},
        ],
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo._build_show_cast_index", return_value=cast_rows):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={},
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._load_fandom_link_state_by_person_id",
                            return_value={},
                        ):
                            with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle") as parse_mock:
                                with patch(
                                    "api.routers.admin_show_bravo._persist_valid_fandom_profile_links",
                                    return_value=1,
                                ):
                                    with patch(
                                        "api.routers.admin_show_bravo._persist_missing_fandom_profile_markers",
                                        return_value=1,
                                    ):
                                        with patch(
                                            "api.routers.admin_show_bravo.upsert_cast_fandom",
                                            return_value=None,
                                        ):
                                            with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                                with patch("api.routers.admin_show_bravo._upsert_show_snapshot"):
                                                    with patch(
                                                        "api.routers.admin_show_bravo._persist_show_description"
                                                    ):
                                                        with patch(
                                                            "api.routers.admin_show_bravo._upsert_person_snapshot"
                                                        ):
                                                            with patch(
                                                                "api.routers.admin_show_bravo._import_fandom_person_image"
                                                            ):
                                                                with patch(
                                                                    "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync"
                                                                ):
                                                                    with patch(
                                                                        "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync"
                                                                    ):
                                                                        response = client.post(
                                                                            f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                                            headers={
                                                                                "Authorization": f"Bearer {token}"
                                                                            },
                                                                            json={
                                                                                "show_url": "https://www.bravotv.com/the-valley",
                                                                                "cast_only": True,
                                                                                "preview_result": preview_result,
                                                                            },
                                                                        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["counts"]["fandom_candidates_tested"] == 2
    assert payload["counts"]["fandom_candidates_valid"] == 1
    assert payload["counts"]["fandom_candidates_missing"] == 1
    assert payload["counts"]["fandom_links_upserted"] == 1
    assert payload["counts"]["fandom_na_marked"] == 1
    parse_mock.assert_not_called()


def test_commit_bravo_import_cast_only_rejects_stale_preview_result(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()
    andy_url = "https://www.bravotv.com/people/andy-cohen"

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo._build_show_cast_index",
                    return_value=[
                        {"person_id": str(uuid4()), "person_name": "Andy Cohen"},
                        {"person_id": str(uuid4()), "person_name": "John Barlow"},
                    ],
                ):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={},
                    ):
                        with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle") as parse_mock:
                            response = client.post(
                                f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                headers={"Authorization": f"Bearer {token}"},
                                json={
                                    "show_url": "https://www.bravotv.com/the-valley",
                                    "cast_only": True,
                                    "preview_result": {
                                        "show_url": "https://www.bravotv.com/the-valley",
                                        "cast_only": True,
                                        "season_filter": None,
                                        "cast_candidate_urls_tested": [andy_url],
                                        "people": [],
                                        "person_candidate_results": [],
                                    },
                                },
                            )

    assert response.status_code == 409
    assert "Preview stale. Re-run preview." in response.text
    parse_mock.assert_not_called()


def test_commit_bravo_import_cast_only_probes_all_cast_profiles_even_if_existing_or_na(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    lisa_id = str(uuid4())
    andy_id = str(uuid4())
    john_id = str(uuid4())
    mock_db = MagicMock()

    cast_rows = [
        {"person_id": lisa_id, "person_name": "Lisa Barlow"},
        {"person_id": andy_id, "person_name": "Andy Cohen"},
        {"person_id": john_id, "person_name": "John Barlow"},
    ]

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [],
        "news": [],
        "people": [],
        "image_candidates": [],
        "discovered_person_urls": [],
        "person_candidate_results": [],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo._build_show_cast_index", return_value=cast_rows):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={
                            lisa_id: {"has_non_rejected": True, "has_na": False},
                            john_id: {"has_non_rejected": False, "has_na": True},
                        },
                    ):
                        with patch(
                            "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                            return_value=bundle,
                        ) as parse_mock:
                            with patch(
                                "api.routers.admin_show_bravo._upsert_show_snapshot",
                                return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                            ):
                                with patch(
                                    "api.routers.admin_show_bravo._persist_show_description"
                                ) as persist_show_mock:
                                    with patch(
                                        "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync"
                                    ) as pending_links_mock:
                                        with patch(
                                            "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync"
                                        ) as role_suggestions_mock:
                                            response = client.post(
                                                f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                headers={"Authorization": f"Bearer {token}"},
                                                json={
                                                    "show_url": "https://www.bravotv.com/the-valley",
                                                    "cast_only": True,
                                                },
                                            )

    assert response.status_code == 200
    called_candidates = parse_mock.call_args.kwargs.get("person_url_candidates")
    assert called_candidates is not None
    assert set(called_candidates) == {
        "https://www.bravotv.com/people/lisa-barlow",
        "https://www.bravotv.com/people/andy-cohen",
        "https://www.bravotv.com/people/john-barlow",
    }
    persist_show_mock.assert_not_called()
    pending_links_mock.assert_not_called()
    role_suggestions_mock.assert_not_called()


def test_commit_bravo_import_marks_missing_candidates_as_na(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    andy_id = str(uuid4())
    mock_db = MagicMock()
    andy_url = "https://www.bravotv.com/people/andy-cohen"
    missing_url = "https://www.bravotv.com/people/john-barlow"

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [],
        "news": [],
        "people": [
            {
                "canonical_url": andy_url,
                "name": "Andy Cohen",
                "bio": "Bio",
                "hero_image_url": None,
                "social_links": {},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": [andy_url],
        "person_candidate_results": [
            {"url": andy_url, "status": "ok"},
            {"url": missing_url, "status": "missing"},
        ],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo._build_show_cast_index",
                    return_value=[
                        {"person_id": andy_id, "person_name": "Andy Cohen"},
                        {"person_id": str(uuid4()), "person_name": "John Barlow"},
                    ],
                ):
                    with patch(
                        "api.routers.admin_show_bravo._load_bravo_profile_link_state_by_person_id",
                        return_value={},
                    ):
                        with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                            with patch(
                                "api.routers.admin_show_bravo._upsert_show_snapshot",
                                return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                            ):
                                with patch("api.routers.admin_show_bravo._persist_show_description"):
                                    with patch(
                                        "api.routers.admin_show_bravo._upsert_person_snapshot",
                                        return_value={
                                            "person_id": andy_id,
                                            "source_id": "bravo",
                                            "variant": "default",
                                        },
                                    ):
                                        with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                            with patch(
                                                "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync"
                                            ) as pending_links_mock:
                                                with patch(
                                                    "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync"
                                                ) as role_suggestions_mock:
                                                    with patch(
                                                        "api.routers.admin_show_links._upsert_link"
                                                    ) as upsert_link_mock:
                                                        response = client.post(
                                                            f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                            headers={"Authorization": f"Bearer {token}"},
                                                            json={
                                                                "show_url": "https://www.bravotv.com/the-valley",
                                                                "cast_only": True,
                                                            },
                                                        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["counts"]["bravo_candidates_tested"] == 2
    assert payload["counts"]["bravo_candidates_valid"] == 1
    assert payload["counts"]["bravo_candidates_missing"] == 1
    assert payload["counts"]["bravo_na_marked"] == 1
    assert upsert_link_mock.call_count == 1
    metadata = upsert_link_mock.call_args.kwargs["metadata"]
    assert metadata["bravo_probe_state"] == "na"
    assert metadata["bravo_probe_reason"] == "missing"
    assert upsert_link_mock.call_args.kwargs["status"] == "rejected"
    pending_links_mock.assert_not_called()
    role_suggestions_mock.assert_not_called()


def test_persist_pending_links_from_bravo_sync_respects_discovered_status_and_confidence() -> None:
    mock_db = MagicMock()
    show_id = str(uuid4())

    with patch(
        "api.routers.admin_show_links._discover_show_links",
        return_value=[
            {
                "entity_type": "show",
                "entity_id": show_id,
                "link_group": "official",
                "link_kind": "official_page",
                "url": "https://www.bravotv.com/the-valley",
                "label": "Official",
            },
            {
                "entity_type": "show",
                "entity_id": show_id,
                "link_group": "cast_announcements",
                "link_kind": "cast_announcement",
                "url": "https://www.bravotv.com/the-daily-dish/cast-news",
                "label": "Cast news",
            },
        ],
    ):
        with patch("api.routers.admin_show_links._discover_season_links", return_value=[]):
            with patch(
                "api.routers.admin_show_links._discover_people_links",
                return_value=[
                    {
                        "entity_type": "person",
                        "entity_id": str(uuid4()),
                        "link_group": "knowledge",
                        "link_kind": "imdb",
                        "url": "https://www.imdb.com/name/nm1234567/",
                        "label": "Heather Gay IMDb",
                        "status": "approved",
                        "confidence": 0.99,
                    },
                    {
                        "entity_type": "person",
                        "entity_id": str(uuid4()),
                        "link_group": "knowledge",
                        "link_kind": "tmdb",
                        "url": "https://www.themoviedb.org/person/12345",
                        "label": "Pending-like TMDb",
                    },
                ],
            ):
                with patch("api.routers.admin_show_links._upsert_link") as upsert_link:
                    upserted = _persist_pending_links_from_bravo_sync(
                        mock_db,
                        show_id=show_id,
                        actor="admin@example.com",
                    )

    assert upserted == 3
    statuses = [call.kwargs["status"] for call in upsert_link.call_args_list]
    confidences = [call.kwargs["confidence"] for call in upsert_link.call_args_list]
    assert statuses == ["pending", "pending", "approved"]
    assert confidences == [0.65, 0.75, 0.99]
    assert all(
        not (
            call.kwargs["entity_type"] == "person"
            and call.kwargs["link_kind"]
            in {"imdb", "tmdb", "wikidata", "wikipedia", "fandom", "wikia", "bravo_profile"}
            and call.kwargs["status"] != "approved"
        )
        for call in upsert_link.call_args_list
    )


def test_commit_bravo_import_persists_season_overview_for_season_scoped_sync(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    season_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Season-specific Bravo copy",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [],
        "news": [],
        "people": [
            {
                "canonical_url": "https://www.bravotv.com/people/janet-caperna",
                "name": "Janet Caperna",
                "bio": "Bio",
                "hero_image_url": None,
                "social_links": {},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                    with patch(
                        "api.routers.admin_show_bravo._build_show_cast_index",
                        return_value=[{"person_id": person_id, "person_name": "Janet Caperna"}],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._resolve_season_id",
                            return_value=season_id,
                        ):
                            with patch(
                                "api.routers.admin_show_bravo._upsert_show_snapshot",
                                return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                            ):
                                with patch(
                                    "api.routers.admin_show_bravo._upsert_person_snapshot",
                                    return_value={"person_id": person_id, "source_id": "bravo", "variant": "default"},
                                ):
                                    with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                        with patch(
                                            "api.routers.admin_show_bravo._persist_show_description"
                                        ) as persist_show_description_mock:
                                            with patch(
                                                "api.routers.admin_show_bravo._persist_season_overview"
                                            ) as persist_season_overview_mock:
                                                with patch(
                                                    "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync",
                                                    return_value=0,
                                                ):
                                                    with patch(
                                                        "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync",
                                                        return_value={
                                                            "role_suggestions": 0,
                                                            "role_assignments": 0,
                                                            "announcement_people": 0,
                                                        },
                                                    ):
                                                        response = client.post(
                                                            f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                            headers={"Authorization": f"Bearer {token}"},
                                                            json={
                                                                "show_url": "https://www.bravotv.com/the-valley",
                                                                "season_number": 1,
                                                            },
                                                        )

    assert response.status_code == 200
    persist_season_overview_mock.assert_called_once()
    persist_show_description_mock.assert_not_called()


def test_commit_bravo_import_uses_selected_show_image_kinds(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [],
        "news": [],
        "people": [
            {
                "canonical_url": "https://www.bravotv.com/people/janet-caperna",
                "name": "Janet Caperna",
                "bio": "Bio",
                "hero_image_url": "https://www.bravotv.com/p/1.jpg",
                "social_links": {},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                    with patch(
                        "api.routers.admin_show_bravo._build_show_cast_index",
                        return_value=[{"person_id": person_id, "person_name": "Janet Caperna"}],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._upsert_show_snapshot",
                            return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                        ):
                            with patch("api.routers.admin_show_bravo._persist_show_description"):
                                with patch(
                                    "api.routers.admin_show_bravo._upsert_person_snapshot",
                                    return_value={"person_id": person_id, "source_id": "bravo", "variant": "default"},
                                ):
                                    with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                        with patch(
                                            "api.routers.admin_show_bravo._import_bravo_person_image",
                                            return_value={"imported": 0, "skipped": 0, "errors": []},
                                        ):
                                            with patch(
                                                "api.routers.admin_scrape.import_images",
                                                return_value=SimpleNamespace(
                                                    imported=2,
                                                    skipped_duplicates=0,
                                                    errors=[],
                                                ),
                                            ) as import_images_mock:
                                                with patch(
                                                    "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync",
                                                    return_value=0,
                                                ):
                                                    with patch(
                                                        "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync",
                                                        return_value={
                                                            "role_suggestions": 0,
                                                            "role_assignments": 0,
                                                            "announcement_people": 0,
                                                        },
                                                    ):
                                                        response = client.post(
                                                            f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                            headers={"Authorization": f"Bearer {token}"},
                                                            json={
                                                                "show_url": "https://www.bravotv.com/the-valley",
                                                                "selected_show_images": [
                                                                    {
                                                                        "url": "https://www.bravotv.com/i/logo.png",
                                                                        "kind": "logo",
                                                                    },
                                                                    {
                                                                        "url": "https://www.bravotv.com/i/poster.jpg",
                                                                        "kind": "poster",
                                                                    },
                                                                ],
                                                            },
                                                        )

    assert response.status_code == 200
    assert import_images_mock.called
    import_request = import_images_mock.call_args.args[0]
    kinds = [image.kind for image in import_request.images]
    assert kinds == ["logo", "poster"]


def test_commit_bravo_import_invokes_cast_matrix_sync_when_enabled(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [],
        "news": [],
        "people": [
            {
                "canonical_url": "https://www.bravotv.com/people/janet-caperna",
                "name": "Janet Caperna",
                "bio": "Bio",
                "hero_image_url": None,
                "social_links": {},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                    with patch(
                        "api.routers.admin_show_bravo._build_show_cast_index",
                        return_value=[{"person_id": person_id, "person_name": "Janet Caperna"}],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._upsert_show_snapshot",
                            return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                        ):
                            with patch("api.routers.admin_show_bravo._persist_show_description"):
                                with patch(
                                    "api.routers.admin_show_bravo._upsert_person_snapshot",
                                    return_value={"person_id": person_id, "source_id": "bravo", "variant": "default"},
                                ):
                                    with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                        with patch(
                                            "api.routers.admin_show_bravo._persist_pending_links_from_bravo_sync",
                                            return_value=0,
                                        ):
                                            with patch(
                                                "api.routers.admin_show_bravo._persist_cast_role_suggestions_from_bravo_sync",
                                                return_value={
                                                    "role_suggestions": 0,
                                                    "role_assignments": 0,
                                                    "announcement_people": 0,
                                                },
                                            ):
                                                with patch(
                                                    "api.routers.admin_show_roles.sync_cast_matrix_for_show",
                                                    return_value={
                                                        "counts": {
                                                            "season_role_assignments_upserted": 2,
                                                            "relationship_role_assignments_upserted": 1,
                                                            "global_kid_assignments_upserted": 1,
                                                            "bravo_links_upserted": 1,
                                                        }
                                                    },
                                                ) as sync_mock:
                                                    response = client.post(
                                                        f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                        headers={"Authorization": f"Bearer {token}"},
                                                        json={"show_url": "https://www.bravotv.com/the-valley"},
                                                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["counts"]["cast_matrix_season_roles"] == 2
    assert payload["counts"]["cast_matrix_relationship_roles"] == 1
    assert payload["counts"]["cast_matrix_kid_roles"] == 1
    assert payload["counts"]["cast_matrix_bravo_links"] == 1
    assert sync_mock.called


def test_preview_bravo_import_filters_videos_by_season(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                    return_value={
                        "show": {"title": "Summer House"},
                        "people": [],
                        "videos": [
                            {"title": "S10 clip", "clip_url": "https://www.bravotv.com/v/10", "season_number": 10},
                            {"title": "S9 clip", "clip_url": "https://www.bravotv.com/v/9", "season_number": 9},
                        ],
                        "news": [],
                        "image_candidates": [],
                        "discovered_person_urls": [],
                    },
                ):
                    response = client.post(
                        f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"show_url": "https://www.bravotv.com/summer-house", "season_number": 10},
                    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["videos"]) == 1
    assert payload["videos"][0]["season_number"] == 10


def test_preview_bravo_import_requires_synced_seasons_episodes_and_cast(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch(
                "api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo",
                side_effect=HTTPException(
                    status_code=409,
                    detail="Sync seasons, episodes, and cast before Bravo import (missing: episodes, cast).",
                ),
            ):
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"show_url": "https://www.bravotv.com/summer-house"},
                )

    assert response.status_code == 409
    assert "Sync seasons, episodes, and cast before Bravo import" in response.json().get("detail", "")


def test_dedupe_items_merges_person_tags_for_duplicate_article() -> None:
    items = [
        {
            "article_url": "https://www.bravotv.com/the-daily-dish/story",
            "headline": "Story",
            "person_tags": [{"person_id": "p1", "person_name": "Person One", "person_url": None}],
        },
        {
            "article_url": "https://www.bravotv.com/the-daily-dish/story",
            "headline": "Story",
            "person_tags": [{"person_id": "p2", "person_name": "Person Two", "person_url": None}],
        },
    ]

    merged = _dedupe_items(items, "article_url", merge_person_tags=True)

    assert len(merged) == 1
    merged_tags = merged[0]["person_tags"]
    assert isinstance(merged_tags, list)
    assert {tag.get("person_id") for tag in merged_tags} == {"p1", "p2"}


def test_external_ids_merge_fill_missing_only() -> None:
    existing = {
        "instagram": "already-set",
        "twitter": "",
    }
    incoming = {
        "instagram": "should-not-overwrite",
        "twitter": "fresh-handle",
        "tiktok": "new-account",
    }

    merged = _merge_external_ids_fill_missing(existing, incoming)

    assert merged["instagram"] == "already-set"
    assert merged["instagram_id"] == "already-set"
    assert merged["twitter"] == "fresh-handle"
    assert merged["twitter_id"] == "fresh-handle"
    assert merged["tiktok"] == "new-account"
    assert merged["tiktok_id"] == "new-account"
    assert merged["tiktok_url"] == "https://www.tiktok.com/@new-account"


def test_external_ids_merge_normalizes_social_urls_to_ids_and_urls() -> None:
    merged = _merge_external_ids_fill_missing(
        existing={},
        incoming={
            "instagram": "https://www.instagram.com/janetcaperna/",
            "twitter": "https://x.com/janetcaperna",
            "youtube": "https://www.youtube.com/@janetcaperna",
        },
    )

    assert merged["instagram"] == "janetcaperna"
    assert merged["instagram_id"] == "janetcaperna"
    assert merged["instagram_url"] == "https://www.instagram.com/janetcaperna"
    assert merged["twitter"] == "janetcaperna"
    assert merged["twitter_id"] == "janetcaperna"
    assert merged["twitter_url"] == "https://x.com/janetcaperna"
    assert merged["youtube"] == "@janetcaperna"
    assert merged["youtube_id"] == "@janetcaperna"
    assert merged["youtube_url"] == "https://www.youtube.com/@janetcaperna"


def test_external_ids_merge_skips_generic_youtube_placeholders() -> None:
    merged = _merge_external_ids_fill_missing(
        existing={},
        incoming={
            "youtube": "https://www.youtube.com/user/",
        },
    )

    assert "youtube" not in merged
    assert "youtube_id" not in merged
    assert "youtube_url" not in merged
