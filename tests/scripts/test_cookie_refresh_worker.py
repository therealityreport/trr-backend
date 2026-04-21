from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts.socials import cookie_refresh_worker as cli


def _iso_days_ago(days: int) -> str:
    return (datetime.now(tz=UTC) - timedelta(days=days)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def test_run_worker_skips_when_cookies_are_fresh_and_healthy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli.social_repo, "_instagram_cookie_refresh_target_path", lambda: Path("/tmp/instagram-cookies.json"))
    monkeypatch.setattr(
        cli.instagram_cookie_refresh,
        "read_instagram_cookie_file_metadata",
        lambda _path: {"_cookie_refreshed_at": _iso_days_ago(1)},
    )
    monkeypatch.setattr(
        cli.social_repo,
        "get_instagram_auth_repair_signal",
        lambda failure_lookback_hours=24: {
            "needs_repair": False,
            "reason_codes": [],
            "cookie_validation": {"valid": True, "reason": None, "detail": None},
            "latest_failure": None,
        },
    )

    payload = cli.run_worker(max_cookie_age_days=7, failure_lookback_hours=24)

    assert payload["ok"] is True
    assert payload["action"] == "skip"
    assert payload["needs_repair"] is False


def test_run_worker_repairs_when_recent_unauthorized_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli.social_repo, "_instagram_cookie_refresh_target_path", lambda: Path("/tmp/instagram-cookies.json"))
    monkeypatch.setattr(
        cli.instagram_cookie_refresh,
        "read_instagram_cookie_file_metadata",
        lambda _path: {"_cookie_refreshed_at": _iso_days_ago(1)},
    )
    monkeypatch.setattr(
        cli.social_repo,
        "get_instagram_auth_repair_signal",
        lambda failure_lookback_hours=24: {
            "needs_repair": True,
            "reason_codes": ["recent_instagram_graphql_unauthorized"],
            "cookie_validation": {"valid": True, "reason": None, "detail": None},
            "latest_failure": {"job_id": "job-1"},
        },
    )
    monkeypatch.setattr(
        cli.repair_instagram_auth,
        "run_repair",
        lambda **_kwargs: {"ok": True, "failure_reason": None, "steps": [{"name": "refresh", "status": "ok"}]},
    )

    payload = cli.run_worker(max_cookie_age_days=7, failure_lookback_hours=24)

    assert payload["ok"] is True
    assert payload["action"] == "repair"
    assert payload["trigger"]["reason_codes"] == ["recent_instagram_graphql_unauthorized"]


def test_run_worker_repairs_when_cookie_age_is_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli.social_repo, "_instagram_cookie_refresh_target_path", lambda: Path("/tmp/instagram-cookies.json"))
    monkeypatch.setattr(
        cli.instagram_cookie_refresh,
        "read_instagram_cookie_file_metadata",
        lambda _path: {"_cookie_refreshed_at": _iso_days_ago(30)},
    )
    monkeypatch.setattr(
        cli.social_repo,
        "get_instagram_auth_repair_signal",
        lambda failure_lookback_hours=24: {
            "needs_repair": False,
            "reason_codes": [],
            "cookie_validation": {"valid": True, "reason": None, "detail": None},
            "latest_failure": None,
        },
    )
    monkeypatch.setattr(
        cli.repair_instagram_auth,
        "run_repair",
        lambda **_kwargs: {"ok": True, "failure_reason": None, "steps": [{"name": "refresh", "status": "ok"}]},
    )

    payload = cli.run_worker(max_cookie_age_days=7, failure_lookback_hours=24)

    assert payload["ok"] is True
    assert payload["action"] == "repair"
    assert "cookie_age_exceeded" in payload["trigger"]["reason_codes"]


def test_run_worker_returns_failed_payload_when_repair_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli.social_repo, "_instagram_cookie_refresh_target_path", lambda: Path("/tmp/instagram-cookies.json"))
    monkeypatch.setattr(
        cli.instagram_cookie_refresh,
        "read_instagram_cookie_file_metadata",
        lambda _path: {"_cookie_refreshed_at": _iso_days_ago(30)},
    )
    monkeypatch.setattr(
        cli.social_repo,
        "get_instagram_auth_repair_signal",
        lambda failure_lookback_hours=24: {
            "needs_repair": False,
            "reason_codes": [],
            "cookie_validation": {"valid": False, "reason": "checkpoint_required", "detail": None},
            "latest_failure": None,
        },
    )
    monkeypatch.setattr(
        cli.repair_instagram_auth,
        "run_repair",
        lambda **_kwargs: {"ok": False, "failure_reason": "remote_probe_failed", "steps": []},
    )

    payload = cli.run_worker(max_cookie_age_days=7, failure_lookback_hours=24)

    assert payload["ok"] is False
    assert payload["action"] == "repair"
    assert payload["failure_reason"] == "remote_probe_failed"


def test_main_emits_json_summary(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "max_cookie_age_days": 7,
                "failure_lookback_hours": 24,
                "check_only": False,
                "source_env": Path("/tmp/.env"),
                "modal_environment": "main",
                "json": True,
            },
        )(),
    )
    monkeypatch.setattr(
        cli,
        "run_worker",
        lambda **_kwargs: {"ok": True, "action": "skip", "needs_repair": False},
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["ok"] is True
    assert payload["action"] == "skip"
