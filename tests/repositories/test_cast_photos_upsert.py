from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

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
        self._rpc_calls: list[tuple[str, dict[str, object]]] = []

    def schema(self, _name: str):  # noqa: ANN001
        return self

    def rpc(self, name: str, params: dict[str, object]):
        self._rpc_calls.append((name, params))
        return self

    def execute(self) -> _FakeResponse:
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

    upsert_cast_photos(client, [_sample_row()])

    assert client._attempts == 2
    mock_reload.assert_called_once()
    mock_sleep.assert_called_once()


def test_upsert_cast_photos_uses_rpc_function() -> None:
    client = _FakeClient()

    upsert_cast_photos(client, [_sample_row()])

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

    upsert_cast_photos(client, [row], dedupe_on="image_url_canonical")

    assert client._rpc_calls
    assert client._rpc_calls[0][0] == "upsert_cast_photos_by_canonical"


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
        upsert_cast_photos(client, [bad_row])


def test_upsert_cast_photos_requires_canonical_url() -> None:
    client = _FakeClient()
    bad_row = CastPhotoUpsert(
        person_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        imdb_person_id="nm11883948",
        source="fandom",
        source_image_id="",
        url="https://static.wikia.nocookie.net/real-housewives/images/abc.jpg",
        url_path="/real-housewives/images/abc.jpg",
    )

    with pytest.raises(CastPhotoRepositoryError):
        upsert_cast_photos(client, [bad_row], dedupe_on="image_url_canonical")
