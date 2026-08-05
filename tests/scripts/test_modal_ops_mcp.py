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


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("30m", "30m"),
        ("2026-08-05T14:00:00Z", "2026-08-05T14:00:00Z"),
        ("--help", "24h"),
        ("1h --search anything", "24h"),
    ],
)
def test_modal_log_since_rejects_cli_options(value: str, expected: str) -> None:
    assert modal_ops_mcp._validated_modal_logs_since(value) == expected


def test_active_job_statuses_uses_compatible_fallback() -> None:
    assert modal_ops_mcp._active_job_statuses(SimpleNamespace()) == ("queued", "running", "retrying", "cancelling")
