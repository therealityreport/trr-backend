from __future__ import annotations

import httpx
import pytest

from trr_backend.socials.socialblade import business_api


def test_socialblade_business_api_disabled_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIALBLADE_API_ENABLED", "true")
    monkeypatch.delenv("SOCIALBLADE_API_CLIENT_ID", raising=False)
    monkeypatch.delenv("SOCIALBLADE_API_TOKEN", raising=False)

    assert business_api.scrape_socialblade_business_api_if_configured("bravotv") is None


def test_socialblade_business_api_maps_statistics_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setenv("SOCIALBLADE_API_ENABLED", "true")
    monkeypatch.setenv("SOCIALBLADE_API_CLIENT_ID", "client-id")
    monkeypatch.setenv("SOCIALBLADE_API_TOKEN", "token")
    monkeypatch.setenv("SOCIALBLADE_API_BASE_URL", "https://matrix.sbapis.com/b")
    original_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["clientid"] = request.headers.get("clientid")
        captured["token"] = request.headers.get("token")
        return httpx.Response(
            200,
            json={
                "data": {
                    "username": "bravotv",
                    "statistics": {
                        "total": {
                            "followers": 1000,
                            "following": 25,
                            "media": 40,
                            "engagement_rate": 1.25,
                            "average_likes": 50,
                            "average_comments": 5,
                        },
                        "daily": [
                            {"date": "2026-06-28", "followers": 990},
                            {"date": "2026-06-29", "followers": 1000},
                        ],
                    },
                    "ranks": {"grade": "B", "followers": 123},
                }
            },
        )

    monkeypatch.setattr(
        business_api.httpx,
        "Client",
        lambda **_kwargs: original_client(transport=httpx.MockTransport(handler)),
    )

    payload = business_api.scrape_socialblade_business_api("bravotv", platform="instagram")

    assert captured == {
        "url": "https://matrix.sbapis.com/b/instagram/statistics?query=bravotv",
        "clientid": "client-id",
        "token": "token",
    }
    assert payload["history_source"] == "business_api"
    assert payload["profile_stats"]["followers"] == 1000
    assert payload["daily_total_followers_chart"]["total_data_points"] == 2
