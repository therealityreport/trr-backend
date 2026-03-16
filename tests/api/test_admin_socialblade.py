from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_admin
from api.main import app


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_admin] = lambda: {"id": "service_role:test", "role": "service_role"}
    yield
    app.dependency_overrides.pop(require_admin, None)


def test_single_refresh_passes_force(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.socials.socialblade.service as service_module

    captured: dict[str, object] = {}

    def fake_refresh_and_persist_socialblade(**kwargs):
        captured.update(kwargs)
        return {
            "username": kwargs["handle"],
            "scraped_at": "2026-03-16T12:00:00Z",
            "freshness_status": "fresh",
            "refresh_status": "refreshed",
        }

    monkeypatch.setattr(service_module, "refresh_and_persist_socialblade", fake_refresh_and_persist_socialblade)

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/people/person-1/socialblade/refresh",
        json={"handle": "lisabarlow14", "force": True},
    )

    assert response.status_code == 200
    assert response.json()["refresh_status"] == "refreshed"
    assert captured["person_id"] == "person-1"
    assert captured["handle"] == "lisabarlow14"
    assert captured["source"] == "person_page"
    assert captured["force"] is True


def test_batch_refresh_dedupes_and_skips_fresh_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.modal_dispatch as dispatch_module
    import trr_backend.socials.socialblade.service as service_module

    monkeypatch.setattr(service_module, "socialblade_auto_refresh_enabled", lambda: True)

    def fake_queue_refresh_decision(*, person_id: str, handle: str, force: bool = False):
        if handle == "freshalready":
            return (
                "skipped",
                {
                    "scraped_at": "2026-03-16T12:00:00Z",
                    "freshness_status": "fresh",
                },
                "fresh_within_24h",
            )
        return ("accepted", None, None)

    monkeypatch.setattr(service_module, "queue_refresh_decision", fake_queue_refresh_decision)
    monkeypatch.setattr(
        dispatch_module,
        "dispatch_socialblade_scrape",
        lambda **kwargs: {"dispatched": True, "call_id": f"call-{kwargs['handle']}"},
    )

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/people/socialblade/refresh-batch",
        json={
            "source": "cast_comparison",
            "items": [
                {"personId": "person-1", "handle": "lisabarlow14"},
                {"personId": "person-1", "handle": "lisabarlow14"},
                {"personId": "person-2", "handle": "freshalready"},
            ],
        },
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["accepted"] == [
        {"personId": "person-1", "handle": "lisabarlow14", "callId": "call-lisabarlow14"}
    ]
    assert payload["skipped"] == [
        {"personId": "person-1", "handle": "lisabarlow14", "reason": "duplicate_request"},
        {
            "personId": "person-2",
            "handle": "freshalready",
            "reason": "fresh_within_24h",
            "scrapedAt": "2026-03-16T12:00:00Z",
            "freshnessStatus": "fresh",
        },
    ]
    assert payload["errors"] == []


def test_batch_refresh_respects_season_run_kill_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.socials.socialblade.service as service_module

    monkeypatch.setattr(service_module, "socialblade_auto_refresh_enabled", lambda: False)

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/people/socialblade/refresh-batch",
        json={
            "source": "season_run",
            "items": [{"personId": "person-1", "handle": "lisabarlow14"}],
        },
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["accepted"] == []
    assert payload["errors"] == []
    assert payload["skipped"] == [
        {"personId": "person-1", "handle": "lisabarlow14", "reason": "auto_refresh_disabled"}
    ]
