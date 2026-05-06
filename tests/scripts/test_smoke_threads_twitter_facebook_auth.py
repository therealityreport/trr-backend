from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.socials import smoke_threads_twitter_facebook_auth as smoke


def test_help_mentions_all_remote_auth_smoke_keys(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        smoke.main(["--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "--dry-run" in output
    assert "--run" in output
    assert "remote-auth-twitter" in output
    assert "remote-auth-facebook" in output
    assert "remote-auth-threads" in output


def test_dry_run_prints_remote_auth_commands_without_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fail_run(*_args, **_kwargs):
        pytest.fail("dry-run should not execute subprocesses")

    monkeypatch.setattr(smoke.subprocess, "run", _fail_run)

    exit_code = smoke.main(["--dry-run"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Threads/Twitter/Facebook remote-auth smoke checks (DRY RUN)" in output
    assert "python scripts/modal/verify_modal_readiness.py --probe-remote-auth twitter --json" in output
    assert "python scripts/modal/verify_modal_readiness.py --probe-remote-auth facebook --json" in output
    assert "python scripts/modal/verify_modal_readiness.py --probe-remote-auth threads --json" in output


def test_run_executes_selected_command_with_current_interpreter(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(argv, *, cwd, check):
        calls.append({"argv": tuple(argv), "cwd": cwd, "check": check})
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(smoke.subprocess, "run", _fake_run)

    exit_code = smoke.main(["--run", "--only", "remote-auth-threads"])

    assert exit_code == 0
    assert len(calls) == 1
    assert calls[0]["cwd"] == smoke.BACKEND_ROOT
    assert calls[0]["check"] is False
    argv = calls[0]["argv"]
    assert isinstance(argv, tuple)
    assert argv[1:] == (
        "scripts/modal/verify_modal_readiness.py",
        "--probe-remote-auth",
        "threads",
        "--json",
    )
    assert "Running remote-auth-threads" in capsys.readouterr().out
