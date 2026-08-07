from __future__ import annotations

from typing import Any, cast

from trr_backend.integrations.tmdb_person import (
    TMDbExternalIds,
    TMDbPersonDetails,
    TMDbPersonFull,
    fetch_tmdb_person_details,
)


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def get(self, _url: str, timeout: int = 30) -> _FakeResponse:
        _ = timeout
        return self._response


def test_fetch_tmdb_person_details_normalizes_json_string_aliases() -> None:
    session = _FakeSession(
        _FakeResponse(
            200,
            {
                "id": 123,
                "name": "Alan Cumming",
                "also_known_as": '[" Alan Cumming ", "", null, "A. Cumming"]',
            },
        )
    )

    details = fetch_tmdb_person_details(123, session=cast(Any, session), retries=1)

    assert details is not None
    assert details.tmdb_id == 123
    assert details.also_known_as == ["Alan Cumming", "A. Cumming"]


def test_to_cast_tmdb_row_always_emits_list_aliases() -> None:
    details = TMDbPersonDetails(
        tmdb_id=123,
        name="Alan Cumming",
        also_known_as='[" Alan Cumming ", "", "A. Cumming"]',  # type: ignore[arg-type]
    )
    person = TMDbPersonFull(details=details, external_ids=TMDbExternalIds(tmdb_id=123))

    row = person.to_cast_tmdb_row("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

    assert row["person_id"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert row["tmdb_id"] == 123
    assert row["also_known_as"] == ["Alan Cumming", "A. Cumming"]
    assert isinstance(row["also_known_as"], list)
