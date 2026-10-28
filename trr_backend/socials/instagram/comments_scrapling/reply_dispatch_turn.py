"""Shared reply send turn, retained through admission until HTTP starts.

No transaction spans a wait or HTTP. The existing global key and clock remain
authoritative. A completed start advances that clock before another turn opens;
an unused turn never reserves a future slot or accumulates catch-up credit.
"""

from __future__ import annotations

import fcntl
import importlib
import os
import threading
import time

from psycopg2.extras import RealDictCursor

from .reply_scheduler import ReplyDeadlineExceeded

_PROCESS_TURN_LOCK = threading.Lock()  # session_control has one checkout per process.


class DispatchTurn:
    def __init__(self, *, key, delay_seconds, deadline, mode):
        self.key, self.delay, self.deadline, self.mode = key, max(0.0, delay_seconds), deadline, mode
        self.cancelled = threading.Event()
        self.handle = self.connection = self.context = None
        self.locked = False
        self.finished = False
        self.process_locked = False

    def cancel(self):
        self.cancelled.set()

    def _check(self):
        if self.cancelled.is_set() or self.deadline is not None and time.monotonic() >= self.deadline:
            raise ReplyDeadlineExceeded("Reply dispatch turn expired or cancelled")

    def _wait(self, seconds):
        until = time.monotonic() + max(0, seconds)
        while time.monotonic() < until:
            self._check()
            self.cancelled.wait(min(0.02, until - time.monotonic()))
        self._check()

    def acquire(self):
        fetcher = importlib.import_module(".fetcher", __package__)

        self._check()
        if self.delay <= 0:
            return
        try:
            while not _PROCESS_TURN_LOCK.acquire(blocking=False):
                self._wait(0.02)
            self.process_locked = True
            if self.mode == "advisory":
                from trr_backend.db import pg

                # Read checkout is autocommit; the session lock, not a SQL
                # transaction, owns the interval through admission/start.
                context = pg.db_read_connection(label="instagram-reply-dispatch-turn", pool_name="session_control")
                self.connection = context.__enter__()
                self.context = context
                if not self.connection.autocommit:
                    raise ValueError("Dispatch turn requires autocommit")
                keys = fetcher._advisory_lock_keys_for(self.key)
                while True:
                    self._check()
                    with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """with ownership as materialized (
                                select pg_try_advisory_lock(%s,%s) as locked
                            ) select ownership.locked,p.rate_key,
                                greatest(0,extract(epoch from
                                (greatest(p.last_start+make_interval(secs=>%s),
                                 p.server_retry_after_until,clock_timestamp())-clock_timestamp())))::float8 as wait
                            from ownership left join lateral (
                                select rate_key,last_start,server_retry_after_until
                                from social.ig_comment_rate_pace
                                where rate_key=%s and ownership.locked for share
                            ) p on true""",
                            (*keys, self.delay, self.key),
                        )
                        row = cur.fetchone()
                        if row is None:
                            raise RuntimeError("Dispatch turn admission query returned no row")
                        if row["locked"]:
                            self.locked = True
                            break
                    self._wait(0.02)
                if row["rate_key"] is None:
                    # Establish the row before any first send, so later combined
                    # lock/read statements can refresh an updated row under FOR
                    # SHARE even if their MVCC snapshot predates its commit.
                    with self.connection.cursor() as cur:
                        cur.execute(
                            """insert into social.ig_comment_rate_pace(rate_key,last_start)
                            values(%s,clock_timestamp()-make_interval(secs=>%s)) on conflict do nothing""",
                            (self.key, self.delay),
                        )
                    row = None
                wait = row["wait"] if row else None
                while True:
                    if wait is not None:
                        if not wait:
                            break
                        self._wait(wait)
                    with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """select greatest(0,extract(epoch from
                            (greatest(last_start+make_interval(secs=>%s),
                                      server_retry_after_until,clock_timestamp())-clock_timestamp())))::float8 as wait
                            from social.ig_comment_rate_pace where rate_key=%s""",
                            (self.delay, self.key),
                        )
                        row = cur.fetchone()
                    wait = row["wait"] if row else 0
            else:
                self.handle = open(fetcher._global_rate_limit_path(self.key), "a+", encoding="utf-8")
                while True:
                    self._check()
                    try:
                        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        self.locked = True
                        break
                    except BlockingIOError:
                        self._wait(0.02)
                previous = fetcher._read_monotonic_timestamp(self.handle)
                self._wait(previous + self.delay - time.monotonic())
            # Server waits can arrive while this turn queues. Check again after
            # acquisition; production's final identity admission fences it too.
            while True:
                with open(fetcher._global_rate_cooldown_path(self.key), "a+", encoding="utf-8") as cooldown:
                    while True:
                        self._check()
                        try:
                            fcntl.flock(cooldown.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            self._wait(0.02)
                    try:
                        wait = fetcher._read_server_retry_deadline(cooldown) - time.time()
                    finally:
                        fcntl.flock(cooldown.fileno(), fcntl.LOCK_UN)
                if wait <= 0:
                    break
                self._wait(wait)
            self._check()
        except BaseException:
            self.finish(sent=False)
            raise

    def finish(self, sent=False):
        fetcher = importlib.import_module(".fetcher", __package__)

        if self.finished:
            return
        self.finished = True
        try:
            if self.connection is not None and self.locked and sent:
                with self.connection.cursor() as cur:
                    cur.execute(
                        """insert into social.ig_comment_rate_pace as p(rate_key,last_start)
                        values(%s,clock_timestamp()) on conflict(rate_key) do update
                        set last_start=greatest(p.last_start,excluded.last_start)""",
                        (self.key,),
                    )
            elif self.handle is not None and self.locked and sent:
                self.handle.seek(0)
                self.handle.truncate()
                self.handle.write(f"{time.monotonic():.6f}")
                self.handle.flush()
                os.fsync(self.handle.fileno())
        finally:
            try:
                if self.connection is not None and self.locked:
                    with self.connection.cursor() as cur:
                        cur.execute("select pg_advisory_unlock(%s,%s)", fetcher._advisory_lock_keys_for(self.key))
                if self.handle is not None:
                    if self.locked:
                        fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                    self.handle.close()
            finally:
                try:
                    if self.context is not None:
                        self.context.__exit__(None, None, None)
                finally:
                    self.locked = False
                    if self.process_locked:
                        self.process_locked = False
                        _PROCESS_TURN_LOCK.release()
