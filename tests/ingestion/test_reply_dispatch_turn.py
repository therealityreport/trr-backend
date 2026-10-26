"""Bounded real cross-process clocks; isolated PostgreSQL and local files only."""

import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import pytest

from tests.ingestion import test_identity_policy as policy
from trr_backend.socials.instagram.comments_scrapling import fetcher
from trr_backend.socials.instagram.comments_scrapling.reply_dispatch_turn import DispatchTurn
from trr_backend.socials.instagram.comments_scrapling.reply_scheduler import ReplyDeadlineExceeded

database = policy.database


def turn_worker(path, dsn, ready, start, results, admission):
    from trr_backend.db import pg

    fetcher._global_rate_limit_path = lambda _key: path
    fetcher._global_rate_cooldown_path = lambda _key: path + ".cooldown"

    @contextmanager
    def connection(**_kwargs):
        conn = psycopg2.connect(dsn)
        assert conn.info.host == "127.0.0.1"
        conn.autocommit = True
        try:
            yield conn
        finally:
            conn.close()

    pg.db_read_connection = connection
    turn = DispatchTurn(
        key="shared-fixture", delay_seconds=0.05, deadline=time.monotonic() + 5, mode="advisory" if dsn else "file_lock"
    )
    ready.put(True)
    assert start.wait(5)
    try:
        turn.acquire()
        if turn.connection is not None:
            assert turn.connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_IDLE
        time.sleep(admission)  # owner admission, deliberately longer than pacing
        results.put(time.monotonic())  # actual transport entry
        turn.finish(sent=True)
    finally:
        turn.finish(sent=False)


@pytest.mark.parametrize("mode", ["file_lock", "advisory"])
def test_independent_workers_do_not_catch_up_after_slow_admission(database, tmp_path, mode):
    _, transaction = database
    dsn = None
    if mode == "advisory":
        with transaction() as conn, conn.cursor() as cur:
            cur.execute("""create table if not exists social.ig_comment_rate_pace(
                rate_key text primary key,last_start timestamptz not null,
                cooldown_until timestamptz,server_retry_after_until timestamptz)""")
            dsn = conn.dsn
    context = multiprocessing.get_context("spawn")
    ready, start, results = context.Queue(), context.Event(), context.Queue()
    processes = [
        context.Process(target=turn_worker, args=(str(tmp_path / "clock"), dsn, ready, start, results, delay))
        for delay in (0.15, 0)
    ]
    try:
        for process in processes:
            process.start()
        for _ in processes:
            assert ready.get(timeout=10)
        start.set()
        times = sorted(results.get(timeout=10) for _ in processes)
        assert times[1] - times[0] >= 0.045
        for process in processes:
            process.join(timeout=5)
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)


def test_cancel_drains_server_cooldown_wait_without_waiting_for_its_deadline(monkeypatch, tmp_path):
    clock, cooldown = str(tmp_path / "clock"), str(tmp_path / "cooldown")
    monkeypatch.setattr(fetcher, "_global_rate_limit_path", lambda _key: clock)
    monkeypatch.setattr(fetcher, "_global_rate_cooldown_path", lambda _key: cooldown)
    Path(cooldown).write_text(str(time.time() + 60))
    turn = DispatchTurn(key="fixture", delay_seconds=0.05, deadline=time.monotonic() + 30, mode="file_lock")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(turn.acquire)
        time.sleep(0.04)
        cancelled = time.monotonic()
        turn.cancel()
        with pytest.raises(ReplyDeadlineExceeded):
            future.result(timeout=0.25)
    assert time.monotonic() - cancelled < 0.25
    assert not turn.process_locked and not turn.locked
