from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.socials import refresh_cookies as cli


def _handlers(
    *,
    platform: str = "facebook",
    load=lambda: {"c_user": "fresh-user", "xs": "fresh-xs"},  # noqa: B008
    load_from_sources=lambda: {"c_user": "stored-user", "xs": "stored-xs"},  # noqa: B008
    validate=lambda cookies: (bool(cookies.get("c_user") and cookies.get("xs")), None),  # noqa: B008
    refresh=lambda _reason=None: {"c_user": "refreshed-user", "xs": "refreshed-xs"},  # noqa: B008
) -> cli.PlatformHandlers:
    return cli.PlatformHandlers(
        platform=platform,
        load=load,
        load_from_sources=load_from_sources,
        validate=validate,
        refresh=refresh,
        cookie_file=lambda: Path(f"/tmp/{platform}-cookies.json"),
        headless_env=f"SOCIAL_{platform.upper()}_COOKIE_REFRESH_HEADLESS",
    )


def test_run_platform_force_refresh_uses_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    refresh_reasons: list[str | None] = []

    handlers = _handlers(refresh=lambda reason=None: refresh_reasons.append(reason) or {"c_user": "fresh", "xs": "ok"})

    rc, result = cli.run_platform(handlers, force=True, validate_only=False, headed=False)

    assert rc == 0
    assert result["action"] == "force_refresh"
    assert result["validated"] is True
    assert refresh_reasons == ["forced_by_cli"]


def test_run_platform_validate_only_bypasses_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    refresh_called = {"value": False}
    handlers = _handlers(
        load=lambda: {"c_user": "auto", "xs": "auto"},
        load_from_sources=lambda: {"c_user": "stored", "xs": "stored"},
        refresh=lambda _reason=None: refresh_called.__setitem__("value", True) or {},
    )

    rc, result = cli.run_platform(handlers, force=False, validate_only=True, headed=False)

    assert rc == 0
    assert result["action"] == "validate_only"
    assert result["cookie_count"] == 2
    assert refresh_called["value"] is False


def test_main_prints_json_for_each_platform(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(
        cli,
        "PLATFORM_HANDLERS",
        {
            "instagram": _handlers(platform="instagram", load=lambda: {"sessionid": "fresh"}),
            "facebook": _handlers(platform="facebook", load=lambda: {"c_user": "fresh", "xs": "fresh"}),
        },
    )
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "platform": "all",
                "force": False,
                "validate_only": False,
                "headed": False,
                "validation_mode": "graphql_profile",
                "confirm_instagram_refresh": "",
            },
        )(),
    )

    rc = cli.main()
    captured = capsys.readouterr().out.strip().splitlines()

    assert rc == 1
    payloads = [json.loads(line) for line in captured]
    assert [payload["platform"] for payload in payloads] == ["instagram", "facebook"]
    assert payloads[1]["validated"] is True


def test_main_blocks_forced_instagram_refresh_without_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(
        cli,
        "PLATFORM_HANDLERS",
        {"instagram": _handlers(platform="instagram", refresh=lambda _reason=None: pytest.fail("unsafe refresh"))},
    )
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "platform": "instagram",
                "force": True,
                "validate_only": False,
                "headed": False,
                "validation_mode": "comments_endpoint",
                "confirm_instagram_refresh": "",
            },
        )(),
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 2
    assert payload["reason"] == "instagram_refresh_confirmation_required"
    assert payload["required_confirmation"] == cli.INSTAGRAM_REFRESH_CONFIRMATION


def test_instagram_cli_uses_canonical_cookie_loader(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import scripts.socials.instagram.scrape as instagram_cli

    scraper_init: dict[str, object] = {}
    load_env_called = {"value": False}

    class _FakeScraper:
        def __init__(
            self,
            *,
            cookies: dict,
            http_client_name: str | None = None,
            proxy_urls: list[str] | None = None,
            curl_cffi_impersonate: str | None = None,
        ):
            scraper_init["cookies"] = cookies
            scraper_init["http_client_name"] = http_client_name
            scraper_init["proxy_urls"] = proxy_urls
            scraper_init["curl_cffi_impersonate"] = curl_cffi_impersonate
            self.last_retrieval_meta = {"http_client": http_client_name or "requests", "auth_mode": "with_cookies"}

        def scrape(self, config):  # noqa: ANN001
            assert config.username == "bravotv"
            return []

    monkeypatch.setattr(instagram_cli, "load_env", lambda: load_env_called.__setitem__("value", True) or None)
    monkeypatch.setattr(instagram_cli, "InstagramScraper", _FakeScraper)
    monkeypatch.setattr(
        "trr_backend.repositories.social_season_analytics._load_instagram_cookies",
        lambda: {"sessionid": "fresh-session"},
    )
    monkeypatch.setattr(
        instagram_cli.sys,
        "argv",
        ["scrape.py", "--username", "bravotv", "--hashtags", "RHOSLC", "--start", "2025-01-01", "--end", "2025-01-02"],
    )

    instagram_cli.main()
    _ = capsys.readouterr()

    assert load_env_called["value"] is True
    assert scraper_init["cookies"] == {"sessionid": "fresh-session"}


def test_tiktok_cli_uses_canonical_cookie_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.socials.tiktok.scrape as tiktok_cli

    scraper_init: dict[str, object] = {}
    load_env_called = {"value": False}

    class _FakeScraper:
        def __init__(
            self,
            *,
            cookies: dict,
            http_client_name: str | None = None,
            proxy_urls: list[str] | None = None,
            curl_cffi_impersonate: str | None = None,
        ):
            scraper_init["cookies"] = cookies
            scraper_init["http_client_name"] = http_client_name
            scraper_init["proxy_urls"] = proxy_urls
            scraper_init["curl_cffi_impersonate"] = curl_cffi_impersonate
            self.last_retrieval_meta = {"http_client": http_client_name or "requests", "auth_mode": "with_cookies"}

        def scrape(self, config):  # noqa: ANN001
            assert config.username == "bravotv"
            return []

    monkeypatch.setattr(tiktok_cli, "load_env", lambda: load_env_called.__setitem__("value", True) or None)
    monkeypatch.setattr(tiktok_cli, "TikTokScraper", _FakeScraper)
    monkeypatch.setattr(
        "trr_backend.socials.control_plane._load_tiktok_cookies",
        lambda: {"sessionid": "fresh-session", "sid_tt": "fresh-sid"},
    )
    monkeypatch.setattr(
        tiktok_cli.sys,
        "argv",
        ["scrape.py", "--username", "bravotv", "--hashtags", "RHOSLC", "--start", "2025-01-01", "--end", "2025-01-02"],
    )

    tiktok_cli.main()

    assert load_env_called["value"] is True
    assert scraper_init["cookies"] == {"sessionid": "fresh-session", "sid_tt": "fresh-sid"}
    assert scraper_init["http_client_name"] is None
    assert scraper_init["proxy_urls"] is None
    assert scraper_init["http_client_name"] is None
    assert scraper_init["proxy_urls"] is None


def test_tiktok_cli_parses_http_client_proxy_and_diagnostics_args() -> None:
    import scripts.socials.tiktok.scrape as tiktok_cli

    args = tiktok_cli._parse_args(  # noqa: SLF001
        [
            "--username",
            "bravotv",
            "--hashtags",
            "RHOSLC",
            "--start",
            "2025-01-01",
            "--end",
            "2025-01-02",
            "--http-client",
            "curl_cffi",
            "--proxy-url",
            "http://proxy-user:proxy-pass@proxy-host:8080",
            "--diagnostics-json",
            "/tmp/tiktok-diag.json",
        ]
    )

    assert args.http_client == "curl_cffi"
    assert args.proxy_url == "http://proxy-user:proxy-pass@proxy-host:8080"
    assert args.diagnostics_json == "/tmp/tiktok-diag.json"


def test_tiktok_cli_passes_transport_options_and_writes_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import scripts.socials.tiktok.scrape as tiktok_cli

    diagnostics_path = tmp_path / "tiktok-diag.json"
    scraper_init: dict[str, object] = {}

    class _FakeScraper:
        def __init__(
            self,
            *,
            cookies: dict,
            http_client_name: str | None = None,
            proxy_urls: list[str] | None = None,
            curl_cffi_impersonate: str | None = None,
        ):
            scraper_init["cookies"] = cookies
            scraper_init["http_client_name"] = http_client_name
            scraper_init["proxy_urls"] = proxy_urls
            scraper_init["curl_cffi_impersonate"] = curl_cffi_impersonate
            self.last_retrieval_meta = {
                "http_client": http_client_name,
                "curl_cffi_impersonate": curl_cffi_impersonate,
                "proxy_enabled": bool(proxy_urls),
                "proxy_label": "proxy-host",
                "auth_mode": "without_cookies",
                "endpoint_responses": {
                    "fetch_posts": {
                        "endpoint": "fetch_posts",
                        "failure_reason": "non_json_response",
                    }
                },
            }

        def scrape(self, config):  # noqa: ANN001
            assert config.username == "bravotv"
            return []

    monkeypatch.setattr(tiktok_cli, "load_env", lambda: None)
    monkeypatch.setattr(tiktok_cli, "TikTokScraper", _FakeScraper)
    monkeypatch.setattr(
        "trr_backend.socials.control_plane._load_tiktok_cookies",
        lambda: {},
    )

    code = tiktok_cli.main(
        [
            "--username",
            "bravotv",
            "--hashtags",
            "RHOSLC",
            "--start",
            "2025-01-01",
            "--end",
            "2025-01-02",
            "--http-client",
            "curl_cffi",
            "--proxy-url",
            "http://proxy-user:proxy-pass@proxy-host:8080",
            "--diagnostics-json",
            str(diagnostics_path),
        ]
    )

    assert code == 0
    assert scraper_init["cookies"] == {}
    assert scraper_init["http_client_name"] == "curl_cffi"
    assert scraper_init["proxy_urls"] == ["http://proxy-user:proxy-pass@proxy-host:8080"]
    assert diagnostics_path.exists() is True
    assert json.loads(diagnostics_path.read_text(encoding="utf-8"))["http_client"] == "curl_cffi"
