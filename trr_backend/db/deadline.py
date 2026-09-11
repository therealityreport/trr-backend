"""Attempt-local deadlines, including cancellation of exclusively owned DB checkouts."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Event, Lock
from typing import Any

logger = logging.getLogger(__name__)


class DeadlineExceeded(TimeoutError):  # noqa: N818
    pass


class Deadline:
    def __init__(self, seconds: float) -> None:
        self.expires_at = time.monotonic() + seconds
        self.cancelled = Event()
        self._lock = Lock()
        self._connections: dict[int, Any] = {}

    def remaining(self) -> float:
        remaining = self.expires_at - time.monotonic()
        if self.cancelled.is_set() or remaining <= 0:
            raise DeadlineExceeded("Catalog finalization deadline exceeded")
        return remaining

    def register(self, conn: Any) -> None:
        with self._lock:
            self.remaining()
            self._connections[id(conn)] = conn

    def unregister(self, conn: Any) -> None:
        # Cancellation and removal are serialized: a returned connection can never
        # be cancelled after another attempt has borrowed it.
        with self._lock:
            self._connections.pop(id(conn), None)

    def cancel(self) -> None:
        self.cancelled.set()
        with self._lock:
            for conn in self._connections.values():
                try:
                    conn.cancel()
                except Exception:
                    logger.warning("[catalog-launch] owned query cancellation failed", exc_info=True)


_current: ContextVar[Deadline | None] = ContextVar("database_work_deadline", default=None)


def current_deadline() -> Deadline | None:
    return _current.get()


def check_deadline() -> None:
    deadline = current_deadline()
    if deadline is not None:
        deadline.remaining()


def bounded_timeout(seconds: float) -> float:
    deadline = current_deadline()
    return min(seconds, deadline.remaining()) if deadline is not None else seconds


@contextmanager
def deadline_scope(deadline: Deadline):
    token = _current.set(deadline)
    try:
        check_deadline()
        yield
    finally:
        _current.reset(token)
