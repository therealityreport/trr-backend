from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from scripts.modal import modal_ops_mcp


def _rendered_json(payload: object) -> str:
    return "\n".join(
        (
            "# test",
            "status: ok",
            "",
            "## stdout",
            json.dumps(payload),
            "",
            "## stderr",
            "(empty)",
        )
    )


def test_deployment_history_honors_requested_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = iter(
        (
            _rendered_json([{"name": "trr-backend-jobs", "app_id": "ap-test"}]),
            _rendered_json(
                [
                    {"version": "v3"},
                    {"version": "v2"},
                    {"version": "v1"},
                ]
            ),
        )
    )
    monkeypatch.setattr(modal_ops_mcp, "_run", lambda *_args, **_kwargs: next(responses))

    result = modal_ops_mcp.tool_deployment_history(limit=2)

    assert '"limit": 2' in result
    assert '"version": "v3"' in result
    assert '"version": "v2"' in result
    assert '"version": "v1"' not in result


def test_tail_logs_rejects_invalid_since_and_accepts_supported_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(
        modal_ops_mcp,
        "_run",
        lambda command, **_kwargs: commands.append(command) or "ok",
    )

    modal_ops_mcp.tool_tail_logs(since="--json")
    modal_ops_mcp.tool_tail_logs(since="2026-03-01T05:00:00Z")

    assert commands[0][commands[0].index("--since") + 1] == "24h"
    assert commands[1][commands[1].index("--since") + 1] == "2026-03-01T05:00:00Z"


def test_active_job_statuses_uses_default_when_canonical_constant_is_unavailable() -> None:
    fake_social_core = SimpleNamespace(
        SOCIAL_CONTROL_POOL_NAME="social_control",
    )

    assert modal_ops_mcp._active_job_statuses(fake_social_core) == (
        "queued",
        "running",
        "retrying",
        "cancelling",
    )
