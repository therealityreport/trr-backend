"""Explicit operator invocation only; no configured services or source clients."""

import json
import stat
from uuid import uuid4

import pytest

from scripts.ingestion import extend_january_media_repair as cli
from trr_backend.ingestion.media_repair_budget import RepairQualification


def arguments(tmp_path):
    values = {
        "run-id": str(uuid4()),
        "account": "bravotv",
        "generation": "1",
        "intent-hash": "a" * 64,
        "placement": "local",
        "target-key": "DUDfixture",
        "checkpoint-sha256": "b" * 64,
        "owner-generation": "3",
        "attempts": "3",
        "max-attempts": "3",
        "failure-code": "avatar_refresh_asset_unresolved",
        "repair-revision": "fixture-reviewed-repair",
        "failed-revision": "fixture-old",
        "manifest": str(tmp_path / "manifest.json"),
        "manifest-sha256": "c" * 64,
        "review": str(tmp_path / "review.json"),
        "review-sha256": "d" * 64,
        "freeze": str(tmp_path / "freeze.json"),
        "diagnostic": str(tmp_path / "diagnostic.json"),
        "diagnostic-sha256": "e" * 64,
        "reason": "Reviewed saved-owner repair",
        "initiated-by": "fixture-parent",
        "output": str(tmp_path / "receipt.json"),
    }
    return [value for key, item in values.items() for value in ("--" + key, item)]


@pytest.mark.parametrize("apply", [False, True])
def test_cli_plan_default_and_explicit_apply_pass_exact_qualification_inputs(tmp_path, monkeypatch, capsys, apply):
    seen = []
    monkeypatch.setattr(cli, "_configuration", lambda _: seen.append("validated TRR config"))
    monkeypatch.setattr(cli, "Store", lambda **kwargs: kwargs)

    def command(store, **kwargs):
        assert store == {"execution_placement": "local"}
        assert kwargs["apply"] is apply
        assert isinstance(kwargs["qualification"], RepairQualification)
        assert kwargs["qualification"].diagnostic_sha256 == "e" * 64
        assert kwargs["expected_attempts"] == kwargs["expected_max_attempts"] == 3
        seen.append("command")
        return {"applied": apply, "target_key": kwargs["target_key"], "idempotency_key": "fixture-key"}

    monkeypatch.setattr(cli, "extend_media_repair_budget", command)
    argv = arguments(tmp_path) + (["--apply"] if apply else [])
    assert cli.main(argv) == 0
    assert seen == ["validated TRR config", "command"]
    path = tmp_path / "receipt.json"
    assert json.loads(path.read_text())["applied"] is apply
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert json.loads(capsys.readouterr().out)["final_acceptance"] is False


@pytest.mark.parametrize("missing", ["--diagnostic-sha256", "--owner-generation", "--review-sha256", "--failure-code"])
def test_apply_rejects_incomplete_review_state_before_configuration(tmp_path, monkeypatch, missing):
    monkeypatch.setattr(cli, "_configuration", lambda _: pytest.fail("No configuration before argument validation"))
    argv = arguments(tmp_path)
    index = argv.index(missing)
    del argv[index : index + 2]
    with pytest.raises(SystemExit) as caught:
        cli.main(argv + ["--apply"])
    assert caught.value.code == 2
    assert not (tmp_path / "receipt.json").exists()


def test_existing_output_cannot_be_overwritten_or_follow_a_fresh_grant(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_configuration", lambda _: None)
    monkeypatch.setattr(
        cli,
        "extend_media_repair_budget",
        lambda *args, **kwargs: pytest.fail("Cannot apply before exclusive output reservation"),
    )
    path = tmp_path / "receipt.json"
    path.write_text("preserved existing proof")
    with pytest.raises(FileExistsError):
        cli.main(arguments(tmp_path) + ["--apply"])
    assert path.read_text() == "preserved existing proof"
