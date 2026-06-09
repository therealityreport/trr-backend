from __future__ import annotations

import json
from types import SimpleNamespace

from scripts.modal import verify_instagram_posts_auth as cli


def test_verify_instagram_posts_auth_calls_modal_function(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Function:
        @staticmethod
        def from_name(app_name: str, function_name: str):
            captured["app_name"] = app_name
            captured["function_name"] = function_name

            class _Remote:
                @staticmethod
                def remote(*, account_handle: str):
                    captured["account_handle"] = account_handle
                    return {"ready": True, "posts_seen": 33, "cookie_fingerprint": "abc123"}

            return _Remote()

    monkeypatch.setitem(__import__("sys").modules, "modal", SimpleNamespace(Function=_Function))

    payload = cli.verify_instagram_posts_auth(
        account="@bravotv",
        app_name="trr-backend-jobs",
        function_name="probe_instagram_posts_auth",
    )

    assert payload["ready"] is True
    assert captured == {
        "app_name": "trr-backend-jobs",
        "function_name": "probe_instagram_posts_auth",
        "account_handle": "bravotv",
    }


def test_main_json_exits_nonzero_when_not_ready(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "verify_instagram_posts_auth",
        lambda **_kwargs: {"ready": False, "account_handle": "bravotv", "reason": "checkpoint_required"},
    )

    rc = cli.main(["--account", "bravotv", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["ready"] is False
    assert payload["reason"] == "checkpoint_required"
