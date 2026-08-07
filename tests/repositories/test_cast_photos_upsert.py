from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, cast
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from trr_backend.db.session import DbSession
from trr_backend.models.cast_photos import CastPhotoUpsert
from trr_backend.repositories.cast_photos import (
    CastPhotoRepositoryError,
    _handle_pgrst204_with_retry,
    upsert_cast_photos,
)


class _FakeResponse:
    def __init__(self, *, data=None, error=None):  # noqa: ANN001
        self.data = data or []
        self.error = error


class _FakePGRST204Error(Exception):
    def __init__(self) -> None:
        super().__init__("Could not find the 'cast_photos' table in the schema cache")
        self.code = "PGRST204"


class _FakeClient:
    def __init__(self, *, fail_count: int = 0, response: _FakeResponse | None = None) -> None:
        self._fail_count = fail_count
        self._attempts = 0
        self._response = response or _FakeResponse(data=[{"id": "test-id"}])
        self._rpc_calls: list[tuple[str, dict[str, Any]]] = []
        self._update_calls: list[tuple[str, dict[str, object]]] = []
        self._mode = "rpc"
        self._pending_table = ""
        self._pending_update: dict[str, object] | None = None
        self._pending_eq: tuple[str, object] | None = None

    def schema(self, _name: str):  # noqa: ANN001
        return self

    def rpc(self, name: str, params: dict[str, object]):
        self._rpc_calls.append((name, params))
        self._mode = "rpc"
        return self

    def table(self, table_name: str):
        self._pending_table = table_name
        return self

    def update(self, payload: dict[str, object]):
        self._pending_update = payload
        self._mode = "update"
        return self

    def eq(self, column: str, value: object):
        self._pending_eq = (column, value)
        return self

    def execute(self) -> _FakeResponse:
        if self._mode == "update":
            row_id = self._pending_eq[1] if self._pending_eq else None
            payload = self._pending_update or {}
            self._update_calls.append((str(row_id), dict(payload)))
            self._mode = "rpc"
            self._pending_update = None
            self._pending_eq = None
            return _FakeResponse(data=[{"id": row_id, **payload}])
        self._attempts += 1
        if self._attempts <= self._fail_count:
            raise _FakePGRST204Error()
        return self._response


def _sample_row() -> CastPhotoUpsert:
    return CastPhotoUpsert(
        person_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        imdb_person_id="nm11883948",
        source_image_id="MV5BTEST@",
        url="https://m.media-amazon.com/images/M/MV5BTEST@._V1_.jpg",
        url_path="/images/M/MV5BTEST@._V1_.jpg",
        width=640,
    )


def test_handle_pgrst204_with_retry_returns_false_for_non_pgrst204() -> None:
    err = RuntimeError("Some other error")
    assert _handle_pgrst204_with_retry(err, attempt=0, context="test") is False


@patch("trr_backend.repositories.cast_photos.reload_postgrest_schema")
@patch("trr_backend.repositories.cast_photos.time.sleep")
def test_upsert_cast_photos_retries_on_pgrst204(
    mock_sleep: MagicMock,
    mock_reload: MagicMock,
) -> None:
    client = _FakeClient(fail_count=1)

    upsert_cast_photos(cast(DbSession, client), [_sample_row()])

    assert client._attempts == 2
    mock_reload.assert_called_once()
    mock_sleep.assert_called_once()


def test_upsert_cast_photos_uses_rpc_function() -> None:
    client = _FakeClient()

    upsert_cast_photos(cast(DbSession, client), [_sample_row()])

    assert client._rpc_calls
    assert client._rpc_calls[0][0] == "upsert_cast_photos_by_identity"


def test_upsert_cast_photos_uses_canonical_rpc() -> None:
    client = _FakeClient()
    row = _sample_row()
    row = CastPhotoUpsert(
        person_id=row.person_id,
        imdb_person_id=row.imdb_person_id,
        source_image_id=row.source_image_id,
        url=row.url,
        url_path=row.url_path,
        width=row.width,
        image_url_canonical="https://static.wikia.nocookie.net/real-housewives/images/abc.jpg",
        source="fandom",
    )

    upsert_cast_photos(cast(DbSession, client), [row], dedupe_on="image_url_canonical")

    assert client._rpc_calls
    assert client._rpc_calls[0][0] == "upsert_cast_photos_by_canonical"
    payload = client._rpc_calls[0][1]["rows"][0]
    assert payload["image_url_canonical"] == "https://static.wikia.nocookie.net/real-housewives/images/abc.jpg"


def test_upsert_cast_photos_requires_source_image_id() -> None:
    client = _FakeClient()
    bad_row = CastPhotoUpsert(
        person_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        imdb_person_id="nm11883948",
        source_image_id="",
        url="https://m.media-amazon.com/images/M/MV5BTEST@._V1_.jpg",
        url_path="/images/M/MV5BTEST@._V1_.jpg",
    )

    with pytest.raises(CastPhotoRepositoryError):
        upsert_cast_photos(cast(DbSession, client), [bad_row])


def test_upsert_cast_photos_requires_canonical_url() -> None:
    client = _FakeClient()
    bad_row = CastPhotoUpsert(
        person_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        imdb_person_id="nm11883948",
        source="fandom",
        source_image_id="",
        url="",
        url_path="",
    )

    with pytest.raises(CastPhotoRepositoryError):
        upsert_cast_photos(cast(DbSession, client), [bad_row], dedupe_on="image_url_canonical")


def test_upsert_cast_photos_normalizes_canonical_payload_from_image_url() -> None:
    client = _FakeClient()
    row = CastPhotoUpsert(
        person_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        imdb_person_id="nm11883948",
        source="fandom",
        source_image_id="fandom-gallery-abc123",
        url="https://static.wikia.nocookie.net/Real-Housewives/images/AbC.jpg?cb=123",
        url_path="/Real-Housewives/images/AbC.jpg?cb=123",
        image_url="https://static.wikia.nocookie.net/Real-Housewives/images/AbC.jpg?cb=123",
        image_url_canonical="https://static.wikia.nocookie.net/Real-Housewives/images/AbC.jpg?cb=123",
    )

    upsert_cast_photos(cast(DbSession, client), [row], dedupe_on="image_url_canonical")

    payload = client._rpc_calls[0][1]["rows"][0]
    assert payload["image_url_canonical"] == "https://static.wikia.nocookie.net/real-housewives/images/abc.jpg"


def test_upsert_cast_photos_serializes_nested_metadata() -> None:
    class _TestEnum(Enum):
        SAMPLE = "sample"

    client = _FakeClient()
    nested_key = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    row = CastPhotoUpsert(
        person_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        imdb_person_id="nm11883948",
        source_image_id="MV5BTEST@",
        url="https://m.media-amazon.com/images/M/MV5BTEST@._V1_.jpg",
        url_path="/images/M/MV5BTEST@._V1_.jpg",
        width=640,
        metadata=cast(
            "dict[str, Any]",
            {
                nested_key: {
                    "date_only": date(2024, 1, 2),
                    "date_time": datetime(2024, 1, 2, 3, 4, 5),
                    "values": [Decimal("12.34"), {"enum": _TestEnum.SAMPLE}],
                },
                "list": [date(2024, 1, 3), datetime(2024, 1, 3, 4, 5, 6)],
            },
        ),
    )

    upsert_cast_photos(cast(DbSession, client), [row])

    payload = client._rpc_calls[0][1]["rows"][0]
    metadata = payload["metadata"]
    key = str(nested_key)
    assert key in metadata
    assert metadata[key]["date_only"] == "2024-01-02"
    assert metadata[key]["date_time"] == "2024-01-02T03:04:05"
    assert metadata[key]["values"][0] == 12.34
    assert metadata[key]["values"][1]["enum"] == "sample"
    assert metadata["list"][0] == "2024-01-03"
    assert metadata["list"][1] == "2024-01-03T04:05:06"


def test_upsert_cast_photos_merges_metadata_for_existing_identity_rows() -> None:
    client = _FakeClient(
        response=_FakeResponse(
            data=[
                {
                    "id": "row-1",
                    "person_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "source": "imdb",
                    "source_image_id": "MV5BTEST@",
                    "metadata": {"legacy": "keep"},
                }
            ]
        )
    )
    row = CastPhotoUpsert(
        person_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        imdb_person_id="nm11883948",
        source="imdb",
        source_image_id="MV5BTEST@",
        url="https://m.media-amazon.com/images/M/MV5BTEST@._V1_.jpg",
        url_path="/images/M/MV5BTEST@._V1_.jpg",
        width=640,
        metadata={"source_variant": "imdb_person_gallery"},
    )

    upserted = upsert_cast_photos(cast(DbSession, client), [row], dedupe_on="source_image_id")

    assert client._update_calls
    row_id, payload = client._update_calls[0]
    assert row_id == "row-1"
    assert payload["metadata"] == {"legacy": "keep", "source_variant": "imdb_person_gallery"}
    assert upserted[0]["metadata"] == {"legacy": "keep", "source_variant": "imdb_person_gallery"}
