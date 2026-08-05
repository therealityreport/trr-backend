from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2 import admin_show_person_writes

SHOW_ID = "11111111-1111-1111-1111-111111111111"
PERSON_ID = "22222222-2222-2222-2222-222222222222"


@dataclass
class FakeRepository:
    calls: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def update_show(self, show_id: str, patch: dict[str, Any]):
        self.calls.append(("update_show", (show_id, patch)))
        if show_id.endswith("3333"):
            return None, 1
        return ({"id": show_id, "name": patch.get("name") or "Test Show"}, 1)

    def update_person_canonical_profile_source_order(self, person_id: str, source_order: list[str]):
        self.calls.append(("update_person_canonical_profile_source_order", (person_id, source_order)))
        if len(source_order) != 4:
            raise ValueError("source_order_must_include_all_sources")
        return ({"id": person_id, "external_ids": {"canonical_profile_source_order": source_order}}, 1)

    def list_effective_person_social_handles(self, person_ids: list[str]):
        self.calls.append(("list_effective_person_social_handles", (person_ids,)))
        return (
            [
                {
                    "person_id": person_id,
                    "facebook_handle": None,
                    "instagram_handle": "handle",
                    "tiktok_handle": None,
                    "twitter_handle": None,
                    "youtube_handle": None,
                }
                for person_id in person_ids
            ],
            1,
        )


@pytest.fixture
def fake_repository(monkeypatch: pytest.MonkeyPatch) -> FakeRepository:
    fake = FakeRepository()
    monkeypatch.setattr(admin_show_person_writes, "repository", fake)
    return fake


@pytest.fixture
def app() -> FastAPI:
    test_app = FastAPI()
    test_app.include_router(admin_show_person_writes.router, prefix="/api/v2")
    test_app.dependency_overrides[require_internal_admin] = lambda: {"role": "internal_admin"}
    return test_app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def test_show_person_write_routes_require_the_new_v2_contracts(
    client: TestClient,
    fake_repository: FakeRepository,
) -> None:
    show_response = client.patch(
        f"/api/v2/admin/shows/{SHOW_ID}",
        json={"name": "Updated Show", "external_ids": {"instagram": "updated"}},
    )
    person_response = client.patch(
        f"/api/v2/admin/people/{PERSON_ID}/canonical-profile-source-order",
        json={"source_order": ["imdb", "tmdb", "fandom", "manual"]},
    )
    effective_response = client.post(
        "/api/v2/admin/people/effective-social-handles",
        json={"person_ids": [PERSON_ID]},
    )

    assert show_response.status_code == 200
    assert show_response.json()["show"]["name"] == "Updated Show"
    assert person_response.status_code == 200
    assert person_response.json()["person"]["external_ids"]["canonical_profile_source_order"] == [
        "imdb",
        "tmdb",
        "fandom",
        "manual",
    ]
    assert effective_response.status_code == 200
    assert effective_response.json()["handles"] == [
        {
            "person_id": PERSON_ID,
            "facebook_handle": None,
            "instagram_handle": "handle",
            "tiktok_handle": None,
            "twitter_handle": None,
            "youtube_handle": None,
        }
    ]
    assert fake_repository.calls == [
        ("update_show", (SHOW_ID, {"name": "Updated Show", "external_ids": {"instagram": "updated"}})),
        (
            "update_person_canonical_profile_source_order",
            (PERSON_ID, ["imdb", "tmdb", "fandom", "manual"]),
        ),
        ("list_effective_person_social_handles", ([PERSON_ID],)),
    ]


@pytest.mark.parametrize(
    ("method", "path", "payload", "code"),
    [
        ("patch", "/api/v2/admin/shows/not-a-uuid", {"name": "Test"}, "INVALID_SHOW_ID"),
        ("patch", f"/api/v2/admin/shows/{SHOW_ID}", ["not-an-object"], "INVALID_REQUEST_BODY"),
        (
            "patch",
            f"/api/v2/admin/people/{PERSON_ID}/canonical-profile-source-order",
            {"source_order": ["imdb"]},
            "source_order_must_include_all_sources",
        ),
        (
            "post",
            "/api/v2/admin/people/effective-social-handles",
            {"person_ids": ["bad"]},
            "INVALID_REQUEST_BODY",
        ),
    ],
)
def test_invalid_write_inputs_use_stable_400_problems_not_fastapi_422(
    client: TestClient,
    fake_repository: FakeRepository,
    method: str,
    path: str,
    payload: Any,
    code: str,
) -> None:
    response = getattr(client, method)(path, json=payload, headers={"x-request-id": "invalid-write"})

    assert response.status_code == 400
    assert response.json()["detail"]["code"] in {code, "INVALID_SOURCE_ORDER"}
    assert response.json()["detail"]["request_id"] == "invalid-write"
    assert "422" not in response.text
    if code == "source_order_must_include_all_sources":
        assert fake_repository.calls == [("update_person_canonical_profile_source_order", (PERSON_ID, ["imdb"]))]
    else:
        assert fake_repository.calls == []


def test_missing_show_uses_a_typed_404(client: TestClient, fake_repository: FakeRepository) -> None:
    response = client.patch(
        "/api/v2/admin/shows/33333333-3333-3333-3333-333333333333",
        json={"name": "Missing"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "SHOW_NOT_FOUND"
    assert fake_repository.calls == [("update_show", ("33333333-3333-3333-3333-333333333333", {"name": "Missing"}))]
