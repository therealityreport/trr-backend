from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from api.auth import require_admin
from api.main import app
from api.routers import admin_cast as router_module


def _compact_sql(sql: str) -> str:
    return " ".join(sql.split())


@pytest.fixture(autouse=True)
def override_admin() -> Iterator[None]:
    app.dependency_overrides[require_admin] = lambda: {
        "id": "admin:test",
        "role": "admin",
        "email": "admin@example.com",
    }
    yield
    app.dependency_overrides.pop(require_admin, None)


def test_cast_summary_includes_available_photo_url_with_person_card_photo_ranking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured_sql.append(sql)
        assert params == [["show-1"]]
        return [
            {
                "show_id": "show-1",
                "person_id": "person-1",
                "full_name": "Heather Gay",
                "photo_url": "https://cdn.example/heather.jpg",
            }
        ]

    monkeypatch.setattr(router_module.pg, "fetch_all", fake_fetch_all)

    client = TestClient(app)
    response = client.post("/api/v1/admin/shows/cast-summary", json={"show_ids": ["show-1"]})

    assert response.status_code == 200
    assert response.json() == {
        "shows": [
            {
                "show_id": "show-1",
                "cast_members": [
                    {
                        "person_id": "person-1",
                        "full_name": "Heather Gay",
                        "photo_url": "https://cdn.example/heather.jpg",
                    }
                ],
            }
        ]
    }
    sql = _compact_sql(captured_sql[0])
    assert "COALESCE(photo.thumb_url, photo.display_url, photo.hosted_url, photo.url) AS photo_url" in sql
    assert "FROM core.v_cast_photos AS cp" in sql
    assert (
        "CASE WHEN lower(COALESCE(cp.context_section, '')) = 'bravo_profile' THEN 0 "
        "WHEN lower(COALESCE(cp.context_section, '')) IN "
        "( 'official season announcement', 'official_season_announcement' ) THEN 1 ELSE 2 END, "
        "cp.gallery_index ASC NULLS LAST"
    ) in sql
    assert "core.cast_photos" not in sql


def test_cast_summary_keeps_member_when_photo_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        return [
            {
                "show_id": "show-1",
                "person_id": "person-1",
                "full_name": "Lisa Barlow",
                "photo_url": None,
            },
            {
                "show_id": "show-1",
                "person_id": "person-2",
                "full_name": "Meredith Marks",
            },
        ]

    monkeypatch.setattr(router_module.pg, "fetch_all", fake_fetch_all)

    client = TestClient(app)
    response = client.post("/api/v1/admin/shows/cast-summary", json={"show_ids": ["show-1"]})

    assert response.status_code == 200
    assert response.json()["shows"][0]["cast_members"] == [
        {
            "person_id": "person-1",
            "full_name": "Lisa Barlow",
            "photo_url": None,
        },
        {
            "person_id": "person-2",
            "full_name": "Meredith Marks",
            "photo_url": None,
        },
    ]
