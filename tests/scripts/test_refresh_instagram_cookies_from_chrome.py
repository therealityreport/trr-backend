from __future__ import annotations

import builtins
import json
from pathlib import Path

import pytest

from scripts.modal import refresh_instagram_cookies_from_chrome as cli


def test_validate_chrome_only_never_writes_cookies(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "chrome_profile": "codex@thereality.report",
                "validate_chrome_only": True,
                "sync_local": False,
                "push_to_modal": False,
                "deploy": False,
                "verify_remote": False,
                "source_env": Path("/tmp/source.env"),
                "skip_live_validation": True,
                "validation_username": "bravotv",
                "json": True,
                "confirm_instagram_refresh": "",
            },
        )(),
    )
    monkeypatch.setattr(cli, "_find_chrome_profile", lambda _profile: Path("/tmp/Profile 13"))
    monkeypatch.setattr(
        cli,
        "_extract_cookies",
        lambda _profile_path: {"sessionid": "session", "csrftoken": "csrf", "ds_user_id": "123"},
    )

    def _write_cookies(_cookies: dict[str, str]) -> list[str]:
        raise AssertionError("_write_cookies must not run during Chrome validation")

    monkeypatch.setattr(cli, "_write_cookies", _write_cookies)
    monkeypatch.setattr(
        cli,
        "_write_browser_session",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("_write_browser_session must not run during Chrome validation")
        ),
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["ok"] is True
    assert payload["mode"] == "validate_chrome_only"
    assert payload["validation_username"] == "bravotv"
    assert payload["cookie_schema_valid"] is True
    assert payload["cookie_fingerprint"]
    assert payload["cookie_fingerprint_algorithm"] == "sha256:16"
    assert payload["live_validation_valid"] is None
    assert payload["wrote_cookie_files"] is False
    assert payload["pushed_to_modal"] is False
    assert payload["deployed_modal"] is False
    assert payload["verified_remote"] is False
    assert payload["writes"] == {"requested": False, "performed": False, "files": []}
    assert payload["steps"][-1] == {
        "name": "write_cookies",
        "status": "skipped",
        "reason": "sync_local_not_requested",
    }


@pytest.mark.parametrize(
    ("validate_chrome_only", "sync_local", "push_to_modal", "confirmation"),
    [
        (True, False, False, ""),
        (False, True, False, cli.INSTAGRAM_REFRESH_CONFIRMATION),
        (False, False, True, cli.INSTAGRAM_REFRESH_CONFIRMATION),
    ],
)
def test_missing_cookie_extraction_dependency_returns_actionable_redacted_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    validate_chrome_only: bool,
    sync_local: bool,
    push_to_modal: bool,
    confirmation: str,
) -> None:
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "chrome_profile": "Codex",
                "validate_chrome_only": validate_chrome_only,
                "sync_local": sync_local,
                "push_to_modal": push_to_modal,
                "deploy": False,
                "verify_remote": False,
                "source_env": Path("/tmp/source.env"),
                "skip_live_validation": True,
                "validation_username": "bravotv",
                "json": True,
                "confirm_instagram_refresh": confirmation,
            },
        )(),
    )
    monkeypatch.setattr(cli, "_find_chrome_profile", lambda _profile: Path("/tmp/Profile 13"))
    real_import = builtins.__import__

    def _missing_pycookiecheat(name: str, *args: object, **kwargs: object) -> object:
        if name == "pycookiecheat":
            raise ImportError("No module named pycookiecheat")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _missing_pycookiecheat)

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)
    serialized = json.dumps(payload, sort_keys=True)

    assert rc == 1
    assert payload["ok"] is False
    assert payload["failure_reason"] == cli.COOKIE_EXTRACTION_DEPENDENCY_FAILURE_REASON
    assert payload["next_action"] == cli.COOKIE_EXTRACTION_DEPENDENCY_INSTALL_COMMAND
    assert payload["wrote_cookie_files"] is False
    assert payload["pushed_to_modal"] is False
    assert payload["writes"]["performed"] is False
    assert payload["modal_push"]["performed"] is False
    assert payload["setup_error"] == {
        "category": "dependency_setup",
        "package": "pycookiecheat",
        "message": cli.COOKIE_EXTRACTION_DEPENDENCY_MESSAGE,
        "install_command": cli.COOKIE_EXTRACTION_DEPENDENCY_INSTALL_COMMAND,
    }
    assert payload["steps"][-1] == {
        "name": "extract_cookies",
        "status": "failed",
        "reason": cli.COOKIE_EXTRACTION_DEPENDENCY_FAILURE_REASON,
        "setup_error": payload["setup_error"],
    }
    assert "sessionid" not in serialized
    assert "csrftoken" not in serialized
    assert "ds_user_id" not in serialized
    assert "No module named" not in serialized


def test_live_validation_uses_profile_posts_graphql(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_validate(cookies: dict[str, str], *, validation_username: str, timeout_seconds: int) -> tuple[bool, str | None]:
        captured["cookies"] = dict(cookies)
        captured["validation_username"] = validation_username
        captured["timeout_seconds"] = timeout_seconds
        return False, "checkpoint_required"

    monkeypatch.setattr(
        "trr_backend.socials.instagram.cookie_refresh._validate_saved_cookies_via_graphql",
        _fake_validate,
    )

    valid, reason = cli._validate_cookies_live(  # noqa: SLF001
        {"sessionid": "session", "csrftoken": "csrf", "ds_user_id": "123"},
        validation_username="@BravoTV",
    )

    assert valid is False
    assert reason == "checkpoint_required"
    assert captured == {
        "cookies": {"sessionid": "session", "csrftoken": "csrf", "ds_user_id": "123"},
        "validation_username": "BravoTV",
        "timeout_seconds": 45,
    }


def test_sync_local_writes_cookie_files_and_browser_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_env = tmp_path / ".env"
    source_env.write_text("SOCIAL_AUTH_INSTAGRAM_USERNAME=codexhuli\n", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "chrome_profile": "codex@thereality.report",
                "validate_chrome_only": False,
                "sync_local": True,
                "push_to_modal": False,
                "deploy": False,
                "verify_remote": False,
                "source_env": source_env,
                "skip_live_validation": False,
                "validation_username": "bravotv",
                "json": True,
                "confirm_instagram_refresh": "I UNDERSTAND INSTAGRAM AUTH RISK",
            },
        )(),
    )
    monkeypatch.setattr(cli, "_find_chrome_profile", lambda _profile: Path("/tmp/Profile 13"))
    monkeypatch.setattr(
        cli,
        "_extract_cookies",
        lambda _profile_path: {"sessionid": "session", "csrftoken": "csrf", "ds_user_id": "123"},
    )
    monkeypatch.setattr(cli, "_validate_cookies_live", lambda *_args, **_kwargs: (True, "valid"))
    monkeypatch.setattr(cli, "_write_cookies", lambda _cookies: ["/tmp/data/instagram_cookies.json"])
    monkeypatch.setattr(
        cli,
        "_write_browser_session",
        lambda _cookies, *, account_handle: [
            f"/tmp/sessions/{account_handle}.cookies.json",
            f"/tmp/sessions/{account_handle}.storage-state.json",
        ],
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["ok"] is True
    assert payload["browser_session_accounts"] == ["bravotv", "codexhuli"]
    assert payload["writes"]["files"] == [
        "/tmp/data/instagram_cookies.json",
        "/tmp/sessions/bravotv.cookies.json",
        "/tmp/sessions/bravotv.storage-state.json",
        "/tmp/sessions/codexhuli.cookies.json",
        "/tmp/sessions/codexhuli.storage-state.json",
    ]


def test_modal_source_env_embeds_validated_cookies_and_removes_file_pointer(tmp_path: Path) -> None:
    source_env = tmp_path / ".env"
    source_env.write_text(
        "\n".join(
            [
                "TRR_DB_URL=postgresql://example",
                "SOCIAL_INSTAGRAM_COOKIES_FILE=data/instagram_cookies.json",
                "SOCIAL_INSTAGRAM_COOKIES_JSON={\"sessionid\":\"stale\"}",
                "SOCIAL_AUTH_INSTAGRAM_USERNAME=codexhuli",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    temp_env = cli._modal_source_env_with_cookies(  # noqa: SLF001
        {"sessionid": "fresh", "csrftoken": "csrf", "ds_user_id": "123"},
        source_env=source_env,
    )
    try:
        rendered = temp_env.read_text(encoding="utf-8")
    finally:
        temp_env.unlink(missing_ok=True)

    assert "SOCIAL_INSTAGRAM_COOKIES_FILE" not in rendered
    assert "INSTAGRAM_COOKIES_FILE" not in rendered
    assert "stale" not in rendered
    assert "fresh" in rendered
    assert "TRR_DB_URL" in rendered


def test_push_to_modal_uses_generated_cookie_source_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source_env = tmp_path / ".env"
    source_env.write_text("TRR_DB_URL=postgresql://example\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def _fake_run(command, **_kwargs):
        generated_source = Path(command[command.index("--source-env") + 1])
        captured["source_env"] = generated_source
        captured["exists_during_run"] = generated_source.is_file()
        captured["contents"] = generated_source.read_text(encoding="utf-8")
        return None

    monkeypatch.setattr(cli.subprocess, "run", _fake_run)

    ok, reason = cli._push_to_modal(  # noqa: SLF001
        source_env,
        cookies={"sessionid": "fresh", "csrftoken": "csrf", "ds_user_id": "123"},
    )

    assert ok is True
    assert reason == "secrets pushed successfully"
    assert captured["exists_during_run"] is True
    assert "fresh" in str(captured["contents"])
    assert not Path(captured["source_env"]).exists()


def test_push_to_modal_summary_includes_pushed_cookie_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_env = tmp_path / ".env"
    source_env.write_text("TRR_DB_URL=postgresql://example\n", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "chrome_profile": "codex@thereality.report",
                "validate_chrome_only": False,
                "sync_local": False,
                "push_to_modal": True,
                "deploy": False,
                "verify_remote": False,
                "source_env": source_env,
                "skip_live_validation": False,
                "validation_username": "bravotv",
                "json": True,
                "confirm_instagram_refresh": "I UNDERSTAND INSTAGRAM AUTH RISK",
            },
        )(),
    )
    monkeypatch.setattr(cli, "_find_chrome_profile", lambda _profile: Path("/tmp/Profile 13"))
    monkeypatch.setattr(
        cli,
        "_extract_cookies",
        lambda _profile_path: {"sessionid": "session", "csrftoken": "csrf", "ds_user_id": "123"},
    )
    monkeypatch.setattr(cli, "_validate_cookies_live", lambda *_args, **_kwargs: (True, "valid"))
    monkeypatch.setattr(cli, "_push_to_modal", lambda _source_env, *, cookies: (True, "pushed"))

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["pushed_cookie_fingerprint"] == payload["cookie_fingerprint"]
    assert payload["modal_push"]["cookie_fingerprint"] == payload["cookie_fingerprint"]
    assert payload["steps"][-1]["cookie_fingerprint"] == payload["cookie_fingerprint"]
