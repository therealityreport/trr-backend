from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_person_external_ids as router_module


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def test_sync_person_external_ids_returns_contract_and_invalidates_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sync_calls: list[tuple[str, list[dict[str, object]]]] = []
    invalidated: list[str] = []

    def fake_sync(person_id: str, inputs: list[dict[str, object]]) -> list[dict[str, object]]:
        sync_calls.append((person_id, inputs))
        return [
            {
                "id": 1,
                "source_id": "tmdb",
                "external_id": "1686599",
                "is_primary": True,
                "valid_from": None,
                "valid_to": None,
                "observed_at": "2026-06-08T12:00:00+00:00",
                "created_at": "2026-06-08T12:00:00+00:00",
                "updated_at": "2026-06-08T12:00:00+00:00",
            }
        ]

    monkeypatch.setattr(router_module.external_ids_repo, "sync_person_external_ids", fake_sync)
    monkeypatch.setattr(
        router_module,
        "invalidate_person_read_cache",
        lambda *, person_id=None: invalidated.append(person_id or ""),
    )

    client = TestClient(app)
    response = client.put(
        "/api/v1/admin/people/person-1/external-ids",
        json={
            "external_ids": [
                {
                    "source_id": "tmdb",
                    "external_id": "1686599",
                    "valid_from": None,
                    "valid_to": None,
                }
            ]
        },
    )

    assert response.status_code == 200
    assert sync_calls == [
        (
            "person-1",
            [
                {
                    "source_id": "tmdb",
                    "external_id": "1686599",
                    "valid_from": None,
                    "valid_to": None,
                    "is_primary": True,
                }
            ],
        )
    ]
    assert invalidated == ["person-1"]
    assert response.json() == {
        "external_ids": [
            {
                "id": 1,
                "source_id": "tmdb",
                "external_id": "1686599",
                "is_primary": True,
                "valid_from": None,
                "valid_to": None,
                "observed_at": "2026-06-08T12:00:00+00:00",
                "created_at": "2026-06-08T12:00:00+00:00",
                "updated_at": "2026-06-08T12:00:00+00:00",
            }
        ]
    }


def test_sync_person_external_ids_maps_repository_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_conflict(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
        raise router_module.external_ids_repo.PersonExternalIdConflictError(
            "That external ID is already assigned to another person."
        )

    monkeypatch.setattr(router_module.external_ids_repo, "sync_person_external_ids", raise_conflict)

    client = TestClient(app)
    response = client.put(
        "/api/v1/admin/people/person-1/external-ids",
        json={"external_ids": [{"source_id": "imdb", "external_id": "nm1234567"}]},
    )

    assert response.status_code == 409
    assert response.json() == {"detail": "That external ID is already assigned to another person."}


def test_sync_person_external_ids_maps_missing_person(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_missing(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
        raise router_module.external_ids_repo.PersonExternalIdNotFoundError("Person not found")

    monkeypatch.setattr(router_module.external_ids_repo, "sync_person_external_ids", raise_missing)

    client = TestClient(app)
    response = client.put(
        "/api/v1/admin/people/person-1/external-ids",
        json={"external_ids": []},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Person not found"}
