from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "internal-admin:test",
        "role": "internal_admin",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def test_single_refresh_passes_force(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.routers.admin_socialblade as router_module
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
    monkeypatch.setattr(
        router_module,
        "_scrape_socialblade_person_page",
        lambda handle: {"username": handle, "scraped_at": "2026-03-16T12:00:00Z"},
    )

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/people/person-1/socialblade/refresh",
        json={"handle": "lisabarlow14", "force": True},
    )

    assert response.status_code == 200
    assert response.json()["refresh_status"] == "refreshed"
    assert captured["person_id"] == "person-1"
    assert captured["handle"] == "lisabarlow14"
    assert captured["platform"] == "instagram"
    assert captured["source"] == "person_page"
    assert captured["force"] is True


def test_single_refresh_runs_sync_pipeline_in_threadpool(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.routers.admin_socialblade as router_module
    import trr_backend.socials.socialblade.service as service_module

    captured: dict[str, object] = {}

    def fake_refresh_and_persist_socialblade(**kwargs):
        captured["kwargs"] = kwargs
        return {
            "username": kwargs["handle"],
            "scraped_at": "2026-04-07T12:00:00Z",
            "refresh_status": "refreshed",
        }

    async def fake_run_in_threadpool(func, /, *args, **kwargs):
        captured["threadpool_func"] = func
        captured["threadpool_args"] = args
        captured["threadpool_kwargs"] = kwargs
        return func(*args, **kwargs)

    monkeypatch.setattr(service_module, "refresh_and_persist_socialblade", fake_refresh_and_persist_socialblade)
    monkeypatch.setattr(router_module, "run_in_threadpool", fake_run_in_threadpool)

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/people/person-1/socialblade/refresh",
        json={"handle": "heathergay", "force": True},
    )

    assert response.status_code == 200
    assert response.json()["refresh_status"] == "refreshed"
    assert captured["threadpool_func"] is fake_refresh_and_persist_socialblade
    assert captured["threadpool_args"] == ()
    assert captured["threadpool_kwargs"] == {
        "person_id": "person-1",
        "handle": "heathergay",
        "platform": "instagram",
        "scraper": router_module._scrape_socialblade_person_page,
        "source": "person_page",
        "force": True,
    }


def test_single_refresh_returns_structured_error_when_local_scrape_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import api.routers.admin_socialblade as router_module
    import trr_backend.socials.socialblade.service as service_module

    def fake_refresh_and_persist_socialblade(**kwargs):
        return kwargs["scraper"](kwargs["handle"])

    monkeypatch.setattr(service_module, "refresh_and_persist_socialblade", fake_refresh_and_persist_socialblade)
    monkeypatch.setattr(
        router_module,
        "_scrape_socialblade_person_page",
        lambda handle: (_ for _ in ()).throw(
            RuntimeError("SocialBlade scrape failed: visible browser session could not complete challenge")
        ),
    )

    client = TestClient(app, raise_server_exceptions=False)
    response = client.post(
        "/api/v1/admin/people/person-1/socialblade/refresh",
        json={"handle": "lisabarlow14", "force": True},
    )

    assert response.status_code == 502
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == {
        "detail": "SocialBlade scrape failed: visible browser session could not complete challenge"
    }


def test_person_page_scrape_uses_visible_browser_retry_without_headless_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import api.routers.admin_socialblade as router_module
    import trr_backend.job_plane as job_plane_module
    import trr_backend.socials.socialblade.auth as auth_module
    import trr_backend.socials.socialblade.scraper as scraper_module

    captured: dict[str, object] = {}

    monkeypatch.setattr(job_plane_module, "is_modal_remote_executor_enabled", lambda: False)
    monkeypatch.setattr(
        auth_module,
        "refresh_socialblade_cookies",
        lambda reason=None, allow_headless_fallback=True: {"cf_clearance": "token"},
    )
    monkeypatch.setattr(auth_module, "load_socialblade_cookies_from_sources", lambda: {"cf_clearance": "token"})

    def fake_scrape_socialblade(
        handle: str,
        cookies,
        *,
        platform: str,
        allow_login_fallback: bool,
        allow_visible_browser_retry: bool,
    ):
        captured.update(
            {
                "handle": handle,
                "cookies": cookies,
                "platform": platform,
                "allow_login_fallback": allow_login_fallback,
                "allow_visible_browser_retry": allow_visible_browser_retry,
            }
        )
        return {"username": handle, "scraped_at": "2026-03-16T12:00:00Z"}

    monkeypatch.setattr(scraper_module, "scrape_socialblade", fake_scrape_socialblade)

    payload = router_module._scrape_socialblade_person_page("heathergay")

    assert payload["username"] == "heathergay"
    assert captured == {
        "handle": "heathergay",
        "cookies": {"cf_clearance": "token"},
        "platform": "instagram",
        "allow_login_fallback": False,
        "allow_visible_browser_retry": True,
    }


def test_batch_refresh_dedupes_and_skips_fresh_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.routers.admin_socialblade as router_module
    import trr_backend.socials.socialblade.service as service_module

    monkeypatch.setattr(service_module, "socialblade_auto_refresh_enabled", lambda: True)

    def fake_queue_refresh_decision(*, person_id: str, handle: str, force: bool = False, platform: str = "instagram"):
        assert platform == "instagram"
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
        service_module,
        "refresh_and_persist_socialblade",
        lambda **kwargs: {
            "username": kwargs["handle"],
            "scraped_at": "2026-03-18T04:10:25Z",
            "refresh_status": "refreshed",
        },
    )
    monkeypatch.setattr(
        router_module,
        "_scrape_socialblade_person_page",
        lambda handle: {"username": handle, "scraped_at": "2026-03-18T04:10:25Z"},
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
        {
            "personId": "person-1",
            "handle": "lisabarlow14",
            "refreshStatus": "refreshed",
            "scrapedAt": "2026-03-18T04:10:25Z",
        }
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


def test_account_socialblade_read_route_uses_platform_handle_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.repositories.socialblade_growth as repository_module

    captured: dict[str, object] = {}

    def fake_get_growth_data(person_id, handle: str, *, platform: str = "instagram"):
        captured.update({"person_id": person_id, "handle": handle, "platform": platform})
        return {
          "username": "bravotv",
          "account_handle": "bravotv",
          "platform": "facebook",
          "scraped_at": "2026-04-08T12:00:00Z",
        }

    monkeypatch.setattr(repository_module, "get_growth_data", fake_get_growth_data)

    client = TestClient(app)
    response = client.get("/api/v1/admin/socials/profiles/Facebook/@BravoTV/socialblade")

    assert response.status_code == 200
    assert response.json()["platform"] == "facebook"
    assert captured == {
        "person_id": None,
        "handle": "bravotv",
        "platform": "facebook",
    }


def test_account_socialblade_refresh_route_uses_platform_specific_scraper(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.routers.socials as router_module
    import trr_backend.socials.socialblade.auth as auth_module
    import trr_backend.socials.socialblade.scraper as scraper_module
    import trr_backend.socials.socialblade.service as service_module

    captured: dict[str, object] = {}

    monkeypatch.setattr(auth_module, "refresh_socialblade_cookies", lambda *args, **kwargs: {"cf_clearance": "token"})
    monkeypatch.setattr(auth_module, "load_socialblade_cookies_from_sources", lambda: {"cf_clearance": "token"})

    def fake_scrape_socialblade(
        handle: str,
        cookies,
        *,
        platform: str,
        allow_login_fallback: bool,
        allow_visible_browser_retry: bool,
    ):
        captured["scraper"] = {
            "handle": handle,
            "cookies": cookies,
            "platform": platform,
            "allow_login_fallback": allow_login_fallback,
            "allow_visible_browser_retry": allow_visible_browser_retry,
        }
        return {
            "username": handle,
            "account_handle": handle,
            "platform": platform,
            "scraped_at": "2026-04-08T12:00:00Z",
        }

    def fake_refresh_and_persist_socialblade(**kwargs):
        captured["refresh_kwargs"] = kwargs
        return kwargs["scraper"](kwargs["handle"])

    async def fake_run_in_threadpool(func, /, *args, **kwargs):
        captured["threadpool_func"] = func
        captured["threadpool_args"] = args
        captured["threadpool_kwargs"] = kwargs
        return func(*args, **kwargs)

    monkeypatch.setattr(scraper_module, "scrape_socialblade", fake_scrape_socialblade)
    monkeypatch.setattr(service_module, "refresh_and_persist_socialblade", fake_refresh_and_persist_socialblade)
    monkeypatch.setattr(router_module, "run_in_threadpool", fake_run_in_threadpool)

    client = TestClient(app)
    response = client.post(
        "/api/v1/admin/socials/profiles/youtube/@Bravo/socialblade/refresh",
        json={"force": True},
    )

    assert response.status_code == 200
    assert response.json()["platform"] == "youtube"
    assert captured["threadpool_func"] is fake_refresh_and_persist_socialblade
    assert captured["threadpool_kwargs"] == {
        "person_id": None,
        "platform": "youtube",
        "handle": "bravo",
        "scraper": captured["refresh_kwargs"]["scraper"],
        "source": "account_page",
        "force": True,
    }
    assert captured["scraper"] == {
        "handle": "bravo",
        "cookies": {"cf_clearance": "token"},
        "platform": "youtube",
        "allow_login_fallback": False,
        "allow_visible_browser_retry": False,
    }


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
    assert payload["skipped"] == [{"personId": "person-1", "handle": "lisabarlow14", "reason": "auto_refresh_disabled"}]
