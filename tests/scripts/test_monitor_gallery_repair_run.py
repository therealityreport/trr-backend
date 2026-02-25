from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

mod = importlib.import_module("scripts.media.monitor_gallery_repair_run")


def test_evaluate_state_healthy_running(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text("[start]\n", encoding="utf-8")
    json_path = tmp_path / "run.json"
    now_epoch = log_path.stat().st_mtime + (60 * 5)

    result = mod._evaluate_state(
        launchctl={"present": True, "pid": 123, "status": 0, "running": True, "raw_line": "123 0 label"},
        log_path=log_path,
        json_path=json_path,
        checkpoint_path=None,
        stale_minutes=240,
        now_epoch=now_epoch,
    )

    assert result["state"] == "healthy/running"
    assert result["json_exists"] is False


def test_evaluate_state_stalled_when_log_is_stale(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text("[start]\n", encoding="utf-8")
    json_path = tmp_path / "run.json"
    now_epoch = log_path.stat().st_mtime + (60 * 300)

    result = mod._evaluate_state(
        launchctl={"present": True, "pid": 999, "status": 0, "running": True, "raw_line": "999 0 label"},
        log_path=log_path,
        json_path=json_path,
        checkpoint_path=None,
        stale_minutes=240,
        now_epoch=now_epoch,
    )

    assert result["state"] == "stalled"


def test_evaluate_state_completed_pass_from_summary(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text("[start]\n[exit] 0\n", encoding="utf-8")
    json_path = tmp_path / "run.json"
    json_path.write_text(
        json.dumps(
            {
                "summary": {
                    "scanned": 10,
                    "ok": 4,
                    "repaired": 2,
                    "broken_unreachable": 4,
                    "error": 0,
                    "apply": True,
                }
            }
        ),
        encoding="utf-8",
    )

    result = mod._evaluate_state(
        launchctl={"present": True, "pid": None, "status": 0, "running": False, "raw_line": "- 0 label"},
        log_path=log_path,
        json_path=json_path,
        checkpoint_path=None,
        stale_minutes=240,
        now_epoch=log_path.stat().st_mtime + 60,
    )

    assert result["state"] == "completed-pass"


def test_evaluate_state_completed_fail_from_summary_error(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text("[start]\n[exit] 1\n", encoding="utf-8")
    json_path = tmp_path / "run.json"
    json_path.write_text(
        json.dumps(
            {
                "summary": {
                    "scanned": 10,
                    "ok": 0,
                    "repaired": 0,
                    "broken_unreachable": 9,
                    "error": 1,
                    "apply": True,
                }
            }
        ),
        encoding="utf-8",
    )

    result = mod._evaluate_state(
        launchctl={"present": True, "pid": None, "status": 1, "running": False, "raw_line": "- 1 label"},
        log_path=log_path,
        json_path=json_path,
        checkpoint_path=None,
        stale_minutes=240,
        now_epoch=log_path.stat().st_mtime + 60,
    )

    assert result["state"] == "completed-fail"


def test_main_exit_code_completed_pass(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    json_path = tmp_path / "run.json"
    log_path.write_text("[start]\n[exit] 0\n", encoding="utf-8")
    json_path.write_text(
        json.dumps({"summary": {"error": 0, "apply": True, "scanned": 1}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        mod,
        "_launchctl_entry",
        lambda _label: {"present": True, "pid": None, "status": 0, "running": False, "raw_line": "- 0 label"},
    )

    rc = mod.main(
        [
            "--label",
            "test.label",
            "--log-path",
            str(log_path),
            "--json-path",
            str(json_path),
            "--checkpoint-path",
            str(tmp_path / "run.checkpoint.json"),
            "--now-epoch",
            str(log_path.stat().st_mtime + 60),
        ]
    )
    assert rc == 0


def test_evaluate_state_running_with_errors_from_checkpoint(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text("[start]\n", encoding="utf-8")
    json_path = tmp_path / "run.json"
    checkpoint_path = tmp_path / "run.checkpoint.json"
    checkpoint_path.write_text(
        json.dumps({"summary": {"ok": 1, "repaired": 2, "broken_unreachable": 3, "error": 4}}),
        encoding="utf-8",
    )
    now_epoch = log_path.stat().st_mtime + (60 * 5)

    result = mod._evaluate_state(
        launchctl={"present": True, "pid": 321, "status": 0, "running": True, "raw_line": "321 0 label"},
        log_path=log_path,
        json_path=json_path,
        checkpoint_path=checkpoint_path,
        stale_minutes=240,
        now_epoch=now_epoch,
    )

    assert result["state"] == "running-with-errors"
    assert result["checkpoint_exists"] is True
    assert result["checkpoint_summary"]["error"] == 4


def test_main_exit_code_running_with_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    log_path.write_text("[start]\n", encoding="utf-8")
    json_path = tmp_path / "run.json"
    checkpoint_path = tmp_path / "run.checkpoint.json"
    checkpoint_path.write_text(json.dumps({"summary": {"error": 2}}), encoding="utf-8")
    monkeypatch.setattr(
        mod,
        "_launchctl_entry",
        lambda _label: {"present": True, "pid": 111, "status": 0, "running": True, "raw_line": "111 0 label"},
    )

    rc = mod.main(
        [
            "--label",
            "test.label",
            "--log-path",
            str(log_path),
            "--json-path",
            str(json_path),
            "--checkpoint-path",
            str(checkpoint_path),
            "--now-epoch",
            str(log_path.stat().st_mtime + 60),
        ]
    )
    assert rc == 3
