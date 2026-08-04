from __future__ import annotations

import json

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
