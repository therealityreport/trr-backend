from __future__ import annotations

import pytest

from scripts.socials.twitter import scrape as cli


def test_cli_uses_load_env_and_passes_cookie_map_to_twikit_loader(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    load_env_called = {"value": False}
    twikit_loader_cookies: dict | None = None
    scraper_init: dict[str, object] = {}

    class _FakeScraper:
        def __init__(self, *, cookies: dict, bearer_token: str | None, twikit_credentials: dict | None):
            scraper_init["cookies"] = cookies
            scraper_init["bearer_token"] = bearer_token
            scraper_init["twikit_credentials"] = twikit_credentials
            self.last_quote_fetch_reason = None
            self.last_quote_fetch_meta = {"attempts": [], "source_used": None, "failure_reason": None}

        def fetch_public_tweet_summary(self, tweet_id: str, delay: float = 0.0) -> dict:
            del delay
            return {
                "tweet_id": tweet_id,
                "username": "viewer",
                "display_name": "Viewer",
                "url": f"https://x.com/viewer/status/{tweet_id}",
                "text": "Root text",
            }

        def fetch_tweet_quotes(self, tweet_id: str, delay: float = 2.0, max_pages: int = 5):  # noqa: ARG002
            del tweet_id, delay, max_pages
            return []

    def _fake_load_env():
        load_env_called["value"] = True
        return None

    def _fake_load_twitter_auth() -> tuple[dict[str, str], str | None]:
        return {"auth_token": "token-1", "ct0": "ct0-1"}, None

    def _fake_load_twikit_credentials(cookies: dict | None = None) -> dict[str, str]:
        nonlocal twikit_loader_cookies
        twikit_loader_cookies = dict(cookies or {})
        return {"auth_token": "token-1", "ct0": "ct0-1"}

    monkeypatch.setattr(cli, "load_env", _fake_load_env)
    monkeypatch.setattr(cli, "TwitterScraper", _FakeScraper)
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twitter_auth",
        _fake_load_twitter_auth,
    )
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        _fake_load_twikit_credentials,
    )
    monkeypatch.setattr(cli.sys, "argv", ["scrape.py", "--quotes", "--tweet", "123"])

    cli.main()
    captured = capsys.readouterr()

    assert load_env_called["value"] is True
    assert twikit_loader_cookies == {"auth_token": "token-1", "ct0": "ct0-1"}
    assert scraper_init["twikit_credentials"] == {"auth_token": "token-1", "ct0": "ct0-1"}
    assert "Root Tweet Context" in captured.out


def test_cli_quotes_mode_forwards_max_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_max_pages: dict[str, int | None] = {"value": None}

    class _FakeScraper:
        def __init__(self, *, cookies: dict, bearer_token: str | None, twikit_credentials: dict | None):  # noqa: ARG002
            self.last_quote_fetch_reason = "http_404"
            self.last_quote_fetch_meta = {"attempts": [], "source_used": None, "failure_reason": "http_404"}

        def fetch_public_tweet_summary(self, tweet_id: str, delay: float = 0.0) -> dict:
            del delay
            return {
                "tweet_id": tweet_id,
                "username": "viewer",
                "display_name": "Viewer",
                "url": f"https://x.com/viewer/status/{tweet_id}",
                "text": "Root text",
            }

        def fetch_tweet_quotes(self, tweet_id: str, delay: float = 2.0, max_pages: int = 5):  # noqa: ARG002
            captured_max_pages["value"] = max_pages
            return []

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "TwitterScraper", _FakeScraper)
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twitter_auth",
        lambda: ({}, None),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(cli.sys, "argv", ["scrape.py", "--quotes", "--tweet", "123", "--max-pages", "7"])

    cli.main()

    assert captured_max_pages["value"] == 7


def test_cli_dedicated_mode_fails_when_root_tweet_summary_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeScraper:
        def __init__(self, *, cookies: dict, bearer_token: str | None, twikit_credentials: dict | None):  # noqa: ARG002
            self.last_quote_fetch_reason = None
            self.last_quote_fetch_meta = {"attempts": [], "source_used": None, "failure_reason": None}

        def fetch_public_tweet_summary(self, tweet_id: str, delay: float = 0.0):  # noqa: ARG002
            return None

    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "TwitterScraper", _FakeScraper)
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twitter_auth",
        lambda: ({}, None),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_twikit_credentials",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(cli.sys, "argv", ["scrape.py", "--quotes", "--tweet", "123"])

    with pytest.raises(SystemExit):
        cli.main()
