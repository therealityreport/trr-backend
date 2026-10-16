"""Real local Postgres tests for the application boundary, not engine proofs."""

import importlib.util
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

_source = Path(__file__).resolve().parents[2] / "scripts/ingestion_engine_eval/store.py"
_spec = importlib.util.spec_from_file_location("eval_store", _source)
store = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(store)


@pytest.fixture(scope="module", autouse=True)
def local_postgres(tmp_path_factory):
    root = tmp_path_factory.mktemp("engine-adapter")
    binaries = Path("/opt/homebrew/opt/postgresql@17/bin")
    if not (binaries / "initdb").exists():
        pytest.skip("local Postgres 17 binaries required; engine harness supports --pg-bin")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "pg"
    subprocess.run(
        [str(binaries / "initdb"), "-D", str(data), "-A", "trust", "-U", "eval", "--no-locale", "--encoding=UTF8"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            str(binaries / "pg_ctl"),
            "-D",
            str(data),
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-p {port} -h 127.0.0.1 -k ''",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    patch = pytest.MonkeyPatch()
    patch.setenv("EVAL_DATABASE_URL", f"postgresql://eval@127.0.0.1:{port}/postgres")
    patch.delenv("EVAL_INJECT", raising=False)
    store.initialize()
    try:
        yield
    finally:
        patch.undo()
        subprocess.run(
            [str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
        )


def test_concurrent_duplicate_dispatch_and_receipt():
    store.create_run("concurrent", 1)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.perform("concurrent", 0, 1, ""), range(16)))
    assert set(results) == {"committed"}
    assert store.snapshot("concurrent") == {"targets": 1, "dispatches": 1, "receipts": 1, "attempts": 16}


@pytest.mark.parametrize("mutation", ["owner_epoch=2", "cancelled=true"])
def test_late_completion_cannot_commit_after_fence(monkeypatch, mutation):
    run_id = "late-" + mutation
    store.create_run(run_id, 1)

    def change_owner_after_dispatch(run_id, stage, requested):
        if stage == "after_dispatch":
            with store.connection() as conn:
                conn.execute(f"UPDATE eval_runs SET {mutation} WHERE run_id=%s", (run_id,))

    monkeypatch.setattr(store, "checkpoint", change_owner_after_dispatch)
    assert store.perform(run_id, 0, 1, "") == "fenced"
    assert store.snapshot(run_id)["dispatches"] == 1
    assert store.snapshot(run_id)["receipts"] == 0


def test_cancelled_run_rejects_dispatch():
    store.create_run("cancelled", 1)
    with store.connection() as conn:
        conn.execute("UPDATE eval_runs SET cancelled=true WHERE run_id='cancelled'")
    assert store.perform("cancelled", 0, 1, "") == "fenced"
    assert store.snapshot("cancelled")["dispatches"] == 0
