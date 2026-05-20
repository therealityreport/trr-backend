from __future__ import annotations

import sys
from types import SimpleNamespace

from scripts.socials import run_social_backfill_canaries as cli


def test_build_canary_commands_includes_one_command_per_required_platform() -> None:
    commands = cli.build_canary_commands("thetraitorsus")

    assert commands == [
        [
            sys.executable,
            "scripts/socials/local_catalog_action.py",
            "--platform",
            platform,
            "--account",
            "thetraitorsus",
            "--source-scope",
            "network",
            "--action",
            "backfill",
            "--selected-task",
            "post_details",
        ]
        for platform in cli.CANARY_PLATFORMS
    ]
    assert cli.CANARY_PLATFORMS == ["instagram", "tiktok", "twitter", "facebook"]


def test_run_canaries_stops_on_first_failure(monkeypatch, capsys) -> None:
    calls: list[list[str]] = []

    def fake_run(command, *, cwd, check):
        calls.append(command)
        assert cwd == cli.BACKEND_ROOT
        assert check is False
        return SimpleNamespace(returncode=1 if len(calls) == 2 else 0)

    monkeypatch.setattr(cli.subprocess, "run", fake_run)

    assert cli.run_canaries("thetraitorsus") == 1
    assert calls == cli.build_canary_commands("thetraitorsus")[:2]

    output = capsys.readouterr().out
    assert '"platform": "instagram"' in output
    assert '"platform": "tiktok"' in output
    assert '"returncode": 1' in output
    assert '"platform": "twitter"' not in output
