from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.modal import refresh_instagram_cookies_from_chrome as chrome_refresh


def test_chrome_cookie_refresh_push_to_modal_pins_admin_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(chrome_refresh.subprocess, "run", fake_run)
    monkeypatch.setattr(chrome_refresh, "_python_command", lambda: "python")

    ok, message = chrome_refresh._push_to_modal(Path("/tmp/source.env"))

    assert ok is True
    assert message == "secrets pushed successfully"
    assert calls[0]["env"]["MODAL_PROFILE"] == "admin-56995"


def test_chrome_cookie_refresh_deploy_uses_pinned_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(chrome_refresh.subprocess, "run", fake_run)
    monkeypatch.setattr(chrome_refresh, "_python_command", lambda: "python")

    ok, message = chrome_refresh._deploy_modal()

    assert ok is True
    assert message == "modal app deployed"
    command = calls[0]["command"]
    assert command[:2] == ["python", "-m"]
    assert "modal" in command
    assert "deploy" in command
    assert "trr_backend.modal_jobs" in command
    assert command[-2:] == ["--env", "main"]
    assert "deploy_backend.py" not in " ".join(command)
    assert calls[0]["env"]["MODAL_PROFILE"] == "admin-56995"


def test_interactive_login_push_to_modal_uses_pinned_profile() -> None:
    source = (chrome_refresh.REPO_ROOT / "scripts" / "socials" / "instagram" / "interactive_login.py").read_text()

    assert "pinned_modal_env" in source
    assert "env=pinned_modal_env()" in source
