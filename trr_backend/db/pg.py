"""Lightweight Postgres helpers for direct SQL access."""

from __future__ import annotations

from collections.abc import Iterable
from contextlib import contextmanager
from typing import TYPE_CHECKING
from typing import Any
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from trr_backend.db.connection import resolve_database_url

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection
    from psycopg2.extensions import cursor as PgCursor


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


@contextmanager
def db_cursor(*, conn: PgConnection | None = None):
    """Yield a RealDict cursor, optionally reusing an existing connection."""
    if conn is not None:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        return

    with db_connection() as managed_conn:
        with managed_conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur


def fetch_all_with_cursor(
    cur: PgCursor,
    query: str,
    params: Iterable[Any] | None = None,
) -> list[dict[str, Any]]:
    cur.execute(query, params or [])
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def fetch_one_with_cursor(
    cur: PgCursor,
    query: str,
    params: Iterable[Any] | None = None,
) -> dict[str, Any] | None:
    cur.execute(query, params or [])
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_all(query: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
    with db_cursor() as cur:
        return fetch_all_with_cursor(cur, query, params)


def fetch_one(query: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
    with db_cursor() as cur:
        return fetch_one_with_cursor(cur, query, params)


def execute_returning(
    query: str,
    params: Iterable[Any] | None = None,
) -> list[dict[str, Any]]:
    with db_cursor() as cur:
        cur.execute(query, params or [])
        rows = cur.fetchall()
        return [dict(row) for row in rows]


def execute_values_returning(
    query: str,
    rows: list[tuple[Any, ...]],
    *,
    conn: PgConnection | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    with db_cursor(conn=conn) as cur:
        execute_values(cur, query, rows)
        result = cur.fetchall()
        return [dict(row) for row in result]


def execute_values_no_return(
    query: str,
    rows: list[tuple[Any, ...]],
    *,
    conn: PgConnection | None = None,
) -> None:
    if not rows:
        return
    with db_cursor(conn=conn) as cur:
        execute_values(cur, query, rows)
