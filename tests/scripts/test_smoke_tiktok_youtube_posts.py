from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.socials import smoke_tiktok_youtube_posts as smoke


def test_help_mentions_dry_run_and_run_modes(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        smoke.main(["--help"])

    assert exc_info.value.code == 0
    output = capsys.readouterr().out
    assert "--dry-run" in output
    assert "--run" in output
    assert "remote-auth-tiktok" in output
    assert "twitter-posts" in output
    assert "youtube-posts" in output


def test_dry_run_prints_commands_without_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fail_run(*_args, **_kwargs):
        pytest.fail("dry-run should not execute subprocesses")

    monkeypatch.setattr(smoke.subprocess, "run", _fail_run)

    exit_code = smoke.main(
        [
            "--dry-run",
            "--youtube-start",
            "2026-04-01",
            "--youtube-end",
            "2026-05-05",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "TikTok/Twitter/YouTube posts smoke checks (DRY RUN)" in output
    assert "python scripts/modal/verify_modal_readiness.py --probe-remote-auth tiktok --json" in output
    assert "python -m scripts.socials.tiktok.smoke_posts_scrapling --account bravotv --max-pages 1" in output
    assert (
        "python -m scripts.socials.twitter.scrape --query from:BravoTV "
        "--start 2026-04-01 --end 2026-05-05 --max-pages 2"
    ) in output
    assert (
        "python -m scripts.socials.youtube.scrape --channel bravo --keywords Bravo "
        "--start 2026-04-01 --end 2026-05-05 --max-results 5 --max-pages 2 --no-ytdlp-supplement"
    ) in output


def test_run_executes_selected_command_with_current_interpreter(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(argv, *, cwd, check):
        calls.append({"argv": tuple(argv), "cwd": cwd, "check": check})
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(smoke.subprocess, "run", _fake_run)

    exit_code = smoke.main(
        [
            "--run",
            "--only",
            "twitter-posts",
            "--youtube-start",
            "2026-04-01",
            "--youtube-end",
            "2026-05-05",
        ]
    )

    assert exit_code == 0
    assert len(calls) == 1
    assert calls[0]["cwd"] == smoke.BACKEND_ROOT
    assert calls[0]["check"] is False
    argv = calls[0]["argv"]
    assert isinstance(argv, tuple)
    assert argv[1:] == (
        "-m",
        "scripts.socials.twitter.scrape",
        "--query",
        "from:BravoTV",
        "--start",
        "2026-04-01",
        "--end",
        "2026-05-05",
        "--max-pages",
        "2",
    )
    assert "Running twitter-posts" in capsys.readouterr().out
