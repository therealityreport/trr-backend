from __future__ import annotations

import pytest

import scripts.db.run_fk_index_observer as observer_mod


def _base_argv(tmp_path, iterations: int = 1) -> list[str]:
    return [
        "run_fk_index_observer.py",
        "loop",
        "--wave",
        "wave-1",
        "--inventory",
        str(tmp_path / "inventory.yml"),
        "--output-dir",
        str(tmp_path / "snapshots"),
        "--interval-sec",
        "1",
        "--iterations",
        str(iterations),
    ]


def _patch_time_sleep(monkeypatch) -> None:
    monkeypatch.setattr(observer_mod.time, "sleep", lambda _: None)


def test_loop_retries_on_transient_failure(tmp_path, monkeypatch) -> None:
    calls: list[int] = []

    def _fake_snapshot(**kwargs):  # noqa: ANN001, ANN003
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return tmp_path / "snapshots" / "ok"

    monkeypatch.setattr(observer_mod, "snapshot", _fake_snapshot)
    _patch_time_sleep(monkeypatch)
    monkeypatch.setattr("sys.argv", _base_argv(tmp_path, iterations=1))

    rc = observer_mod.main()

    assert rc == 0
    assert len(calls) == 2


def test_loop_aborts_after_three_consecutive_failures(tmp_path, monkeypatch) -> None:
    calls: list[int] = []

    def _fake_snapshot(**kwargs):  # noqa: ANN001, ANN003
        calls.append(1)
        raise RuntimeError(f"boom-{len(calls)}")

    monkeypatch.setattr(observer_mod, "snapshot", _fake_snapshot)
    _patch_time_sleep(monkeypatch)
    # iterations=0 means "run forever" (no cap). Retry guard must abort after 3.
    monkeypatch.setattr("sys.argv", _base_argv(tmp_path, iterations=0))

    with pytest.raises(RuntimeError):
        observer_mod.main()

    assert len(calls) == 3


def test_loop_resets_failure_counter_on_success(tmp_path, monkeypatch) -> None:
    # Sequence: fail, fail, succeed, fail, fail, succeed -> 2 successful iterations
    outcomes = ["fail", "fail", "ok", "fail", "fail", "ok"]
    calls: list[str] = []

    def _fake_snapshot(**kwargs):  # noqa: ANN001, ANN003
        outcome = outcomes[len(calls)]
        calls.append(outcome)
        if outcome == "fail":
            raise RuntimeError(f"boom-{len(calls)}")
        return tmp_path / "snapshots" / "ok"

    monkeypatch.setattr(observer_mod, "snapshot", _fake_snapshot)
    _patch_time_sleep(monkeypatch)
    monkeypatch.setattr("sys.argv", _base_argv(tmp_path, iterations=2))

    rc = observer_mod.main()

    assert rc == 0
    assert len(calls) == 6
    assert calls == outcomes


def test_loop_does_not_catch_keyboard_interrupt(tmp_path, monkeypatch) -> None:
    calls: list[int] = []

    def _fake_snapshot(**kwargs):  # noqa: ANN001, ANN003
        calls.append(1)
        raise KeyboardInterrupt()

    monkeypatch.setattr(observer_mod, "snapshot", _fake_snapshot)
    _patch_time_sleep(monkeypatch)
    monkeypatch.setattr("sys.argv", _base_argv(tmp_path, iterations=0))

    with pytest.raises(KeyboardInterrupt):
        observer_mod.main()

    assert len(calls) == 1
