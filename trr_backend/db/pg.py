"""Lightweight Postgres helpers for direct SQL access."""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from trr_backend.db.connection import resolve_database_url


def _sslmode_for_url(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host in {"localhost", "127.0.0.1"}:
        return "disable"
    return None


@contextmanager
def db_connection():
    url = resolve_database_url()
    sslmode = _sslmode_for_url(url)
    if sslmode:
        conn = psycopg2.connect(url, sslmode=sslmode)
    else:
        conn = psycopg2.connect(url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(query: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or [])
            rows = cur.fetchall()
            return [dict(row) for row in rows]


def fetch_one(query: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or [])
            row = cur.fetchone()
            return dict(row) if row else None


def execute_returning(
    query: str,
    params: Iterable[Any] | None = None,
) -> list[dict[str, Any]]:
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or [])
            rows = cur.fetchall()
            return [dict(row) for row in rows]


def execute_values_returning(
    query: str,
    rows: list[tuple[Any, ...]],
) -> list[dict[str, Any]]:
    if not rows:
        return []
    with db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            execute_values(cur, query, rows)
            result = cur.fetchall()
            return [dict(row) for row in result]
