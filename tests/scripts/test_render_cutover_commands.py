from __future__ import annotations

import sys

import pytest

from scripts.modal import render_cutover_commands as cli


def test_render_cutover_rejects_non_main_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["render_cutover_commands.py", "--modal-environment", "staging"])

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    assert exc_info.value.code == 2


def test_render_cutover_emits_immutable_deploy_target(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "argv", ["render_cutover_commands.py"])

    assert cli.main() == 0

    output = capsys.readouterr().out
    assert "Modal environment: main" in output
    assert "scripts/modal/deploy_backend.py --app-ref trr_backend.modal_jobs" in output
    assert "--app-name trr-backend-jobs --env main" in output
    assert " -m modal deploy " not in output
