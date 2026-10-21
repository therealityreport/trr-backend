import json
from types import SimpleNamespace

import pytest

from scripts.ingestion import probe_authenticated_comment_ids as driver
from trr_backend.ingestion.fixed_validation import AccountBudget
from trr_backend.socials.instagram.authenticated_comment_probe import empty_report


def test_plan_has_no_io(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(driver, "execute", lambda _: pytest.fail("No execution"))
    assert driver.main(["--output", str(tmp_path / "out"), "--budget-path", str(tmp_path / "ledger")]) == 0
    assert json.loads(capsys.readouterr().out)["network_sent"] is False
    assert not list(tmp_path.iterdir())


def fake_modal(monkeypatch, tmp_path, *, fail_get=False, fail_spawn=False):
    import modal

    calls = []
    result = empty_report()
    result.update(auth_state="unavailable", stop_reason="credential_snapshot_unavailable")

    def get(timeout):
        calls.append("get")
        if fail_get and calls.count("get") == 1:
            raise TimeoutError("SECRET")
        return result

    call = SimpleNamespace(object_id="fc-test", get=get)

    def spawn():
        import sqlite3

        with sqlite3.connect(tmp_path / "ledger") as db:
            assert db.execute("select count(*) from requests").fetchone()[0] == 12
        calls.append("spawn")
        if fail_spawn:
            raise TimeoutError("SECRET")
        return call

    def lookup(app, name, environment_name):
        assert (app, name, environment_name) == (
            "trr-backend-jobs",
            "probe_instagram_authenticated_comment_ids",
            "main",
        )
        return SimpleNamespace(spawn=spawn)

    monkeypatch.setattr(driver, "_require_target", lambda: None)
    monkeypatch.setattr(modal.Function, "from_name", lookup)
    monkeypatch.setattr(modal.FunctionCall, "from_id", lambda identity: call if identity == "fc-test" else None)
    return calls


def args(tmp_path):
    return ["--output", str(tmp_path / "out"), "--budget-path", str(tmp_path / "ledger")]


def test_reserves_before_spawn_and_persists_safe_result(tmp_path, monkeypatch):
    calls = fake_modal(monkeypatch, tmp_path)
    assert driver.main(args(tmp_path) + ["--apply-once"]) == 2
    result = json.loads((tmp_path / "out/result.json").read_text())
    assert result["reserved_requests"] == 12 and result["requests"] == 0
    assert calls == ["spawn", "get"]
    assert driver.main(args(tmp_path) + ["--apply-once"]) == 2
    assert calls == ["spawn", "get"]
    assert (tmp_path / "out/result.json").stat().st_mode & 0o777 == 0o600
    budget = AccountBudget(tmp_path / "ledger", "bravotv")
    with budget._db() as db:
        assert db.execute("select count(*) from tranches where finished_at is null").fetchone()[0] == 0


def test_uncertain_call_reconciles_without_second_spawn(tmp_path, monkeypatch, capsys):
    calls = fake_modal(monkeypatch, tmp_path, fail_get=True)
    assert driver.main(args(tmp_path) + ["--apply-once"]) == 2
    assert not (tmp_path / "out/result.json").exists()
    assert driver.main(args(tmp_path) + ["--apply-once"]) == 2
    assert driver.main(args(tmp_path) + ["--reconcile"]) == 2
    assert calls == ["spawn", "get", "get"]
    assert "SECRET" not in capsys.readouterr().out


def test_uncertain_spawn_never_redispatched(tmp_path, monkeypatch):
    calls = fake_modal(monkeypatch, tmp_path, fail_spawn=True)
    assert driver.main(args(tmp_path) + ["--apply-once"]) == 2
    assert driver.main(args(tmp_path) + ["--apply-once"]) == 2
    assert calls == ["spawn"]
    assert (tmp_path / "out/dispatch-uncertain.json").is_file()
