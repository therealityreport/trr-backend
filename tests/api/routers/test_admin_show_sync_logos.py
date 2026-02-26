from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from api.routers import admin_show_sync


class _FakeResponse:
    def __init__(self, data=None, error=None):
        self.data = data if data is not None else []
        self.error = error


class _FakeQuery:
    def __init__(self, db: _FakeDb, table: str):
        self._db = db
        self._table = table
        self._operation: str | None = None

    def select(self, _fields: str):
        self._operation = "select"
        return self

    def update(self, payload):
        self._operation = "update"
        self._db.updates.append((self._table, payload))
        return self

    def eq(self, _field: str, _value):
        return self

    def limit(self, _n: int):
        return self

    def execute(self):
        return self._db.pop(self._table, self._operation or "select")


class _FakeDb:
    def __init__(self, responses):
        self._responses = {key: list(value) for key, value in responses.items()}
        self.updates: list[tuple[str, dict]] = []

    def schema(self, _schema_name: str):
        return self

    def table(self, table_name: str):
        return _FakeQuery(self, table_name)

    def pop(self, table: str, operation: str):
        key = (table, operation)
        queue = self._responses.get(key, [])
        if queue:
            return queue.pop(0)
        return _FakeResponse(data=[])


def test_set_show_logo_primary_media_asset_clears_show_image_primary() -> None:
    db = _FakeDb(
        {
            ("media_links", "select"): [_FakeResponse(data=[{"id": "link-1"}])],
            ("media_links", "update"): [
                _FakeResponse(data=[{"id": "link-1"}]),
                _FakeResponse(data=[{"id": "link-1"}]),
            ],
            ("shows", "update"): [_FakeResponse(data=[{"id": "show-1"}])],
        }
    )

    admin_show_sync._set_show_logo_primary_media_asset(
        db,
        show_id="show-1",
        media_asset_id="asset-1",
    )

    media_link_updates = [payload for table, payload in db.updates if table == "media_links"]
    assert len(media_link_updates) == 2
    assert media_link_updates[0]["is_primary"] is False
    assert media_link_updates[1]["is_primary"] is True

    show_updates = [payload for table, payload in db.updates if table == "shows"]
    assert len(show_updates) == 1
    assert show_updates[0]["primary_logo_image_id"] is None


def test_set_show_logo_primary_show_image_clears_media_link_primaries() -> None:
    db = _FakeDb(
        {
            ("show_images", "select"): [
                _FakeResponse(data=[{"id": "img-1", "kind": "logo", "image_type": None}]),
            ],
            ("media_links", "update"): [_FakeResponse(data=[{"id": "link-1"}])],
            ("shows", "update"): [_FakeResponse(data=[{"id": "show-1"}])],
        }
    )

    admin_show_sync._set_show_logo_primary_show_image(
        db,
        show_id="show-1",
        show_image_id="img-1",
    )

    media_link_updates = [payload for table, payload in db.updates if table == "media_links"]
    assert len(media_link_updates) == 1
    assert media_link_updates[0]["is_primary"] is False

    show_updates = [payload for table, payload in db.updates if table == "shows"]
    assert len(show_updates) == 1
    assert show_updates[0]["primary_logo_image_id"] == "img-1"


def test_set_show_logo_primary_show_image_rejects_non_logo_rows() -> None:
    db = _FakeDb(
        {
            ("show_images", "select"): [
                _FakeResponse(data=[{"id": "img-1", "kind": "poster", "image_type": "poster"}]),
            ],
        }
    )

    with pytest.raises(HTTPException) as exc_info:
        admin_show_sync._set_show_logo_primary_show_image(
            db,
            show_id="show-1",
            show_image_id="img-1",
        )

    assert exc_info.value.status_code == 400
    assert "show_images logo row" in str(exc_info.value.detail)


def test_set_logo_primary_for_show_dispatches_to_media_asset_handler() -> None:
    show_id = uuid4()
    db = MagicMock()
    payload = admin_show_sync.LogoSetPrimaryRequest(
        target_type="show",
        show_id=show_id,
        media_asset_id="asset-123",
    )

    with patch.object(admin_show_sync, "_set_show_logo_primary_media_asset") as set_media:
        response = admin_show_sync.set_logo_primary(payload=payload, db=db, _=MagicMock())

    set_media.assert_called_once_with(db, show_id=str(show_id), media_asset_id="asset-123")
    assert response.target_type == "show"
    assert response.asset_id == "asset-123"
    assert response.set_primary is True


def test_set_logo_primary_for_show_dispatches_to_show_image_handler() -> None:
    show_id = uuid4()
    show_image_id = uuid4()
    db = MagicMock()
    payload = admin_show_sync.LogoSetPrimaryRequest(
        target_type="show",
        show_id=show_id,
        show_image_id=show_image_id,
    )

    with patch.object(admin_show_sync, "_set_show_logo_primary_show_image") as set_show_image:
        response = admin_show_sync.set_logo_primary(payload=payload, db=db, _=MagicMock())

    set_show_image.assert_called_once_with(
        db,
        show_id=str(show_id),
        show_image_id=str(show_image_id),
    )
    assert response.target_type == "show"
    assert response.asset_id == str(show_image_id)
    assert response.set_primary is True


def test_import_show_logo_bytes_normalizes_existing_show_logo_to_png() -> None:
    db = MagicMock()
    show_row = {"id": "show-1", "imdb_id": "tt1234567", "canonical_slug": "rhoslc"}
    existing_asset = {
        "id": "asset-1",
        "hosted_url": "https://cdn.example.com/images/shows/tt1234567/logo/manual/logo.jpg",
        "hosted_key": "images/shows/tt1234567/logo/manual/logo.jpg",
        "hosted_content_type": "image/jpeg",
        "metadata": {},
    }

    with (
        patch.object(
            admin_show_sync,
            "ensure_logo_png_bytes",
            return_value=(b"png-data", "image/png", ".png"),
        ),
        patch.object(
            admin_show_sync,
            "find_asset_by_sha256",
            return_value=existing_asset,
        ),
        patch.object(
            admin_show_sync,
            "get_s3_client",
            return_value=MagicMock(),
        ),
        patch.object(
            admin_show_sync,
            "build_show_image_s3_key",
            return_value="images/shows/tt1234567/logo/manual-logo-import/hash.png",
        ),
        patch.object(
            admin_show_sync,
            "get_s3_bucket",
            return_value="bucket",
        ),
        patch.object(
            admin_show_sync,
            "upload_bytes_to_s3",
            return_value=("etag", 42),
        ) as upload_bytes,
        patch.object(
            admin_show_sync,
            "build_hosted_url",
            return_value="https://cdn.example.com/images/shows/tt1234567/logo/manual-logo-import/hash.png",
        ),
        patch.object(
            admin_show_sync,
            "update_asset_with_mirror_result",
        ) as update_asset,
        patch.object(
            admin_show_sync,
            "_ensure_show_logo_asset_variants",
        ) as ensure_variants,
        patch.object(
            admin_show_sync,
            "create_media_link_for_entity",
        ),
        patch.object(
            admin_show_sync,
            "_set_show_logo_primary_media_asset",
        ),
    ):
        response = admin_show_sync._import_show_logo_bytes(
            db,
            show_row=show_row,
            image_bytes=b"raw-logo-bytes",
            content_type="image/svg+xml",
            source_url="https://example.com/logo.svg",
            set_primary=True,
        )

    assert response.status == "skipped"
    assert response.asset_id == "asset-1"
    assert response.hosted_logo_url and response.hosted_logo_url.endswith(".png")

    upload_call = upload_bytes.call_args.kwargs
    assert upload_call["data"] == b"png-data"
    assert upload_call["content_type"] == "image/png"

    update_call = update_asset.call_args.kwargs
    assert update_call["hosted_content_type"] == "image/png"
    assert update_call["hosted_key"].endswith(".png")
    ensure_variants.assert_called_once()
