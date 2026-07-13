from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from scripts.socials import prepare_bravotv_2026_post_backfills as prep


def test_build_prepared_commands_defaults_to_three_separate_platforms() -> None:
    args = SimpleNamespace(
        platform=None,
        date_start="2026-01-01T00:00:00Z",
        date_end="2026-12-31T23:59:59Z",
        tiktok_account="bravotv",
        twitter_account="bravotv",
        youtube_account="bravo",
    )

    commands = prep.build_prepared_commands(args)

    assert [command.platform for command in commands] == ["tiktok", "twitter", "youtube"]
    assert [command.account for command in commands] == ["bravotv", "bravotv", "bravo"]
    for command in commands:
        assert command.run_argv[1:] == (
            "scripts/socials/local_catalog_action.py",
            "--platform",
            command.platform,
            "--account",
            command.account,
            "--source-scope",
            "network",
            "--action",
            "backfill",
            "--execution-owner",
            "queue",
            "--date-start",
            "2026-01-01T00:00:00Z",
            "--date-end",
            "2026-12-31T23:59:59Z",
        )


def test_dry_run_prints_commands_without_execution(monkeypatch, capsys) -> None:
    def _fail_run(*_args, **_kwargs):
        raise AssertionError("dry-run should not execute subprocesses")

    monkeypatch.setattr(prep.subprocess, "run", _fail_run)

    exit_code = prep.main(["--dry-run", "--platform", "twitter"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "BravoTV 2026 posts backfill commands" in output
    assert "1. twitter @bravotv" in output
    assert "--date-start 2026-01-01T00:00:00Z --date-end 2026-12-31T23:59:59Z" in output
    assert "tiktok @" not in output


def test_json_dry_run_outputs_machine_readable_json(capsys) -> None:
    assert prep.main(["--dry-run", "--platform", "twitter", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["commands"][0]["platform"] == "twitter"
    assert payload["commands"][0]["account"] == "bravotv"


def test_run_executes_selected_platform(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fake_run(argv, *, cwd, check):
        calls.append({"argv": tuple(argv), "cwd": cwd, "check": check})
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(prep.subprocess, "run", _fake_run)

    assert prep.main(["--run", "--platform", "youtube"]) == 0
    assert len(calls) == 1
    assert calls[0]["cwd"] == prep.BACKEND_ROOT
    assert calls[0]["check"] is False
    assert calls[0]["argv"][1:7] == (
        "scripts/socials/local_catalog_action.py",
        "--platform",
        "youtube",
        "--account",
        "bravo",
        "--source-scope",
    )


def test_prepared_commands_do_not_limit_catalog_post_discovery_to_details() -> None:
    args = SimpleNamespace(
        platform=["twitter"],
        date_start="2026-01-01T00:00:00Z",
        date_end="2026-12-31T23:59:59Z",
        tiktok_account="bravotv",
        twitter_account="bravotv",
        youtube_account="bravo",
    )

    command = prep.build_prepared_commands(args)[0]

    assert "--selected-task" not in command.run_argv
    assert command.run_argv[command.run_argv.index("--execution-owner") + 1] == "queue"


@pytest.mark.parametrize(
    "args",
    [
        ["--date-start", "invalid"],
        ["--date-start", "2026-01-02T00:00:00Z", "--date-end", "2026-01-01T00:00:00Z"],
    ],
)
def test_prepared_commands_reject_invalid_windows(args: list[str]) -> None:
    with pytest.raises(SystemExit):
        prep.main(args)
