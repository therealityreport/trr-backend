from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from api.routers import socials as socials_router


def test_scrape_tiktok_returns_safe_diagnostics_subset(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeScraper:
        def __init__(self, *, cookies=None):  # noqa: ANN003
            captured["cookies"] = cookies
            self.last_retrieval_meta = {
                "retrieval_mode": "browser_intercept",
                "http_client": "curl_cffi",
                "fallback_chain": ["browser_intercept", "ytdlp"],
                "stop_reason": "path_degraded",
                "error_code": "tiktok_posts_path_degraded",
                "risk_state": "critical",
                "operator_summary": "TikTok posts path degraded; use browser intercept.",
                "operator_action": "Retry with browser intercept.",
                "triage_bucket": "manual_review",
                "profile_enrichment_status": "partial",
                "ignored_field": "should_not_escape",
            }

        def scrape(self, config):  # noqa: ANN001
            captured["config"] = config
            return []

    def _fake_preflight(*, platform: str, surface: str, loader):  # noqa: ANN001
        captured["preflight"] = {"platform": platform, "surface": surface, "loader": loader}
        return {"sessionid": "cookie"}

    monkeypatch.setattr(socials_router, "_load_social_auth_or_503", _fake_preflight)
    monkeypatch.setattr("trr_backend.socials.tiktok.scraper.TikTokScraper", _FakeScraper)

    payload = asyncio.run(
        socials_router.scrape_tiktok(
            socials_router.TikTokScrapeRequest(
                username="bravotv",
                hashtags=["RHOSLC"],
                date_start=datetime(2025, 1, 1, tzinfo=UTC),
                date_end=datetime(2025, 1, 2, tzinfo=UTC),
            ),
            {"email": "admin@example.com"},
        )
    )

    assert captured["preflight"]["platform"] == "tiktok"
    assert captured["preflight"]["surface"] == "scrape"
    assert captured["cookies"] == {"sessionid": "cookie"}
    assert payload.diagnostics["risk_state"] == "critical"
    assert payload.diagnostics["operator_summary"].startswith("TikTok posts path degraded")
    assert "ignored_field" not in payload.diagnostics
    assert set(payload.diagnostics) <= {
        "retrieval_mode",
        "http_client",
        "fallback_chain",
        "stop_reason",
        "error_code",
        "risk_state",
        "operator_summary",
        "operator_action",
        "triage_bucket",
        "profile_enrichment_status",
    }
