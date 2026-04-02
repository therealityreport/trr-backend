from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api.main import app
from api.routers.admin_scrape import ImportImageItem


def test_import_image_item_accepts_new_kinds() -> None:
    # Pydantic should accept expanded ImageKind literals.
    item = ImportImageItem(
        candidate_id="abc",
        url="https://example.com/x.jpg",
        kind="promo",
        caption=None,
        person_ids=None,
    )
    assert item.kind == "promo"


def test_import_image_item_rejects_unknown_kind() -> None:
    with pytest.raises(ValidationError):
        ImportImageItem(
            candidate_id="abc",
            url="https://example.com/x.jpg",
            kind="not-a-kind",
        )


def test_import_image_item_accepts_logo_target_fields() -> None:
    item = ImportImageItem(
        candidate_id="logo-1",
        url="https://example.com/logo.png",
        kind="logo",
        logo_target_type="publication",
        logo_target_key="deadline.com",
        logo_target_label="Deadline",
        logo_set_primary=True,
    )
    assert item.kind == "logo"
    assert item.logo_target_type == "publication"
    assert item.logo_target_key == "deadline.com"
    assert item.logo_target_label == "Deadline"
    assert item.logo_set_primary is True


def test_import_image_item_rejects_unknown_logo_target_type() -> None:
    with pytest.raises(ValidationError):
        ImportImageItem(
            candidate_id="logo-2",
            url="https://example.com/logo.png",
            kind="logo",
            logo_target_type="invalid-target",  # type: ignore[arg-type]
            logo_target_key="foo",
            logo_target_label="Foo",
        )


def test_import_image_item_requires_logo_target_type_for_logo_images() -> None:
    with pytest.raises(ValidationError):
        ImportImageItem(
            candidate_id="logo-3",
            url="https://example.com/logo.png",
            kind="logo",
        )


def test_import_image_item_rejects_logo_metadata_for_non_logo_images() -> None:
    with pytest.raises(ValidationError):
        ImportImageItem(
            candidate_id="poster-1",
            url="https://example.com/poster.jpg",
            kind="poster",
            logo_target_type="show",
        )


def test_import_image_item_rejects_person_ids_for_logo_images() -> None:
    with pytest.raises(ValidationError):
        ImportImageItem(
            candidate_id="logo-4",
            url="https://example.com/logo.png",
            kind="logo",
            logo_target_type="publication",
            person_ids=[uuid4()],
        )


def test_import_image_item_dedupes_person_ids_and_trims_optional_text() -> None:
    person_id = uuid4()
    item = ImportImageItem(
        candidate_id="cast-1",
        url="https://example.com/cast.jpg",
        kind="cast",
        caption="  Caption  ",
        context_section="  gallery  ",
        context_type="  hero  ",
        asset_name="  cast-shot  ",
        person_ids=[person_id, person_id],
    )

    assert item.caption == "Caption"
    assert item.context_section == "gallery"
    assert item.context_type == "hero"
    assert item.asset_name == "cast-shot"
    assert item.person_ids == [person_id]


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


def test_import_images_stream_includes_operation_contract_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_DB_URL", "postgresql://postgres:postgres@localhost:54322/postgres")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    operation_id = str(uuid4())
    seen_after: list[int] = []

    def _fake_stream_events(_operation_id: str, *, after_seq: int = 0, limit: int = 500):
        seen_after.append(after_seq)
        if after_seq > 0:
            return []
        return [
            {
                "operation_id": operation_id,
                "event_seq": 1,
                "event_type": "operation",
                "event_payload": {
                    "operation_id": operation_id,
                    "status": "running",
                    "attached": False,
                    "request_id": None,
                },
            },
            {
                "operation_id": operation_id,
                "event_seq": 2,
                "event_type": "complete",
                "event_payload": {
                    "imported": 0,
                    "skipped_duplicates": 0,
                    "errors": [],
                    "assets": [],
                },
            },
        ]

    with patch("api.main._validate_startup_config"), patch("api.main._prewarm_database_pool"), TestClient(app) as client:
        with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=MagicMock()):
            with patch("api.routers.admin_scrape.start_operation_for_stream", return_value={"id": operation_id}):
                with patch(
                    "trr_backend.repositories.admin_operations.get_operation",
                    side_effect=[
                        {"id": operation_id, "status": "running"},
                        {"id": operation_id, "status": "completed"},
                    ],
                ):
                    with patch(
                        "trr_backend.repositories.admin_operations.stream_events_after_seq",
                        side_effect=_fake_stream_events,
                    ):
                        response = client.post(
                            "/api/v1/admin/scrape/import/stream",
                            headers={"Authorization": f"Bearer {token}"},
                            json={
                                "entity_type": "show",
                                "show_id": str(uuid4()),
                                "source_url": "https://example.com/source",
                                "images": [
                                    {
                                        "candidate_id": "candidate-1",
                                        "url": "https://example.com/image.jpg",
                                        "kind": "other",
                                    }
                                ],
                            },
                        )

    assert response.status_code == 200
    assert "event: operation" in response.text
    assert "event: complete" in response.text
    assert '"operation_id"' in response.text
    event_seq_matches = [int(match) for match in re.findall(r'"event_seq"\s*:\s*(\d+)', response.text)]
    assert event_seq_matches == [1, 2]
    assert seen_after and seen_after[0] == 0
