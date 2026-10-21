from __future__ import annotations

import json

import pytest

from scripts.ingestion.probe_child_reply_access import main, require_anonymous_failures, write_once


def _failed_arm(arm):
    return {
        "arm": arm,
        "status": "completed",
        "original_run_unchanged": True,
        "subjects": [
            {
                "authenticated": False,
                "requests": 4,
                "positive_reply_proof": False,
                "stop_reason": None,
                "events": [
                    {"kind": "bootstrap", "http_status": 200},
                    {"kind": "graphql_34884685271179117", "http_status": 200, "native_codes": ["1675004"]},
                    {"kind": "graphql_36239935742272683", "http_status": 200, "connection_found": True},
                    {"kind": "rest", "http_status": 200, "connection_found": True},
                ],
            }
        ],
    }


def test_login_is_held_until_all_anonymous_arms_finish_without_replies(tmp_path):
    for arm in ("mac", "decodo"):
        write_once(tmp_path / f"{arm}.result.json", _failed_arm(arm))
    with pytest.raises(ValueError, match="incomplete"):
        require_anonymous_failures(tmp_path)
    write_once(tmp_path / "modal.result.json", _failed_arm("modal"))
    require_anonymous_failures(tmp_path)


@pytest.mark.parametrize(
    "change",
    [
        {"status": "uncertain"},
        {"original_run_unchanged": False},
        {"subjects": []},
        {"subjects": [{"authenticated": False, "requests": 3, "positive_reply_proof": True}]},
        {"subjects": [{"authenticated": False, "requests": 0, "positive_reply_proof": False}]},
    ],
)
def test_uncertain_or_positive_anonymous_evidence_never_enables_login(tmp_path, change):
    for arm in ("mac", "decodo", "modal"):
        write_once(tmp_path / f"{arm}.result.json", {**_failed_arm(arm), **(change if arm == "modal" else {})})
    with pytest.raises(ValueError, match="eligibility"):
        require_anonymous_failures(tmp_path)


def test_one_shot_marker_is_not_overwritten(tmp_path):
    path = tmp_path / "started.json"
    write_once(path, {"comparison": "first"})
    with pytest.raises(FileExistsError):
        write_once(path, {"comparison": "second"})
    assert json.loads(path.read_text()) == {"comparison": "first"}


def test_default_invocation_does_not_execute(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("scripts.ingestion.probe_child_reply_access.execute", lambda _: pytest.fail("No live calls"))
    assert main(["--arm", "mac", "--output", str(tmp_path), "--budget-path", str(tmp_path / "ledger")]) == 0
    assert json.loads(capsys.readouterr().out)["network_sent"] is False
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "reason",
    [
        "network_error",
        "bootstrap_failed",
        "response_size_limit",
        "reply_parent_mismatch",
        "server_retry_after",
        "endpoint_redirect",
    ],
)
def test_inconclusive_anonymous_stop_never_enables_login(tmp_path, reason):
    for arm in ("mac", "decodo", "modal"):
        result = _failed_arm(arm)
        result["subjects"][0].update(stop_reason=reason, requests=1, events=[{"kind": "bootstrap"}])
        write_once(tmp_path / f"{arm}.result.json", result)
    with pytest.raises(ValueError, match="eligibility"):
        require_anonymous_failures(tmp_path)


def test_missing_graphql_context_never_enables_login(tmp_path):
    for arm in ("mac", "decodo", "modal"):
        result = _failed_arm(arm)
        result["subjects"][0]["graphql_unavailable"] = "bootstrap_context_missing"
        write_once(tmp_path / f"{arm}.result.json", result)
    with pytest.raises(ValueError, match="eligibility"):
        require_anonymous_failures(tmp_path)


@pytest.mark.parametrize("status,reason", [(403, "source_auth_failed"), (429, "http_rate_limited")])
def test_explicit_child_access_denial_is_conclusive(tmp_path, status, reason):
    for arm in ("mac", "decodo", "modal"):
        result = _failed_arm(arm)
        result["subjects"][0].update(
            stop_reason=reason,
            events=[
                {"kind": "bootstrap", "http_status": 200},
                {"kind": "graphql_34884685271179117", "http_status": status},
            ],
        )
        write_once(tmp_path / f"{arm}.result.json", result)
    require_anonymous_failures(tmp_path)


def test_modal_requires_explicit_profile_before_any_io(tmp_path, monkeypatch):
    from argparse import Namespace

    from scripts.ingestion.probe_child_reply_access import execute

    monkeypatch.setenv("MODAL_PROFILE", "wrong-profile")
    with pytest.raises(ValueError, match="Explicit admin-56995"):
        execute(Namespace(arm="modal", output=tmp_path))
    assert not list(tmp_path.iterdir())
