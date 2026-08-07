from __future__ import annotations

import argparse
import json
from types import SimpleNamespace
from typing import Any, cast

from scripts.socials import queue_snapshot as cli


def _args(**overrides: Any) -> argparse.Namespace:
    values = {
        "run_id": "11111111-1111-1111-1111-111111111111",
        "platform": "instagram",
        "stage": "media_mirror",
        "account": "@BravoTV",
        "stale_after_seconds": 900,
        "json": True,
    }
    values.update(overrides)
    return cast(argparse.Namespace, SimpleNamespace(**values))


def test_build_snapshot_normalizes_account_and_delegates(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cli,
        "fetch_status_snapshot",
        lambda **kwargs: calls.append(kwargs) or {"stale_running_count": 1, "status_counts": []},
    )

    payload = cli.build_snapshot(_args())

    assert payload["ok"] is True
    assert payload["account"] == "bravotv"
    assert payload["snapshot"]["stale_running_count"] == 1
    assert calls == [
        {
            "run_id": "11111111-1111-1111-1111-111111111111",
            "stage": "media_mirror",
            "account": "bravotv",
            "stale_after_seconds": 900,
        }
    ]


def test_main_emits_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli.pg, "close_pool", lambda: None)
    monkeypatch.setattr(
        cli,
        "build_snapshot",
        lambda _args: {"ok": True, "stage": "media_mirror", "snapshot": {"stale_running_count": 0}},
    )

    assert cli.main(["--run-id", "run-1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "ok": True,
        "stage": "media_mirror",
        "snapshot": {"stale_running_count": 0},
    }
