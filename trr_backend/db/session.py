"""Minimal PostgREST-like DB session using direct SQL."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

from psycopg2.extras import Json, RealDictCursor, execute_values

from trr_backend.db.pg import db_connection, db_read_connection

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class DbError:
    message: str
    code: str | None = None
    details: str | None = None
    hint: str | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message


@dataclass
class DbResponse:
    data: Any
    error: DbError | None = None
    count: int | None = None


class DbSession:
    """Lightweight DB session with a Supabase-like query interface."""

    def schema(self, name: str) -> DbSchema:
        return DbSchema(self, _validate_identifier(name))


def _validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


def _validate_identifier_list(names: str) -> str:
    validated = [_validate_identifier(name.strip()) for name in names.split(",") if name.strip()]
    if not validated or ",".join(validated) != names.replace(" ", ""):
        raise ValueError(f"Invalid SQL identifier list: {names}")
    return ",".join(validated)


def validate_mapping_keys(payload: dict[str, Any]) -> dict[str, Any]:
    for key in payload:
        _validate_identifier(key)
    return payload


def _parse_or_expression(expression: str) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    for raw_term in expression.split(","):
        term = raw_term.strip()
        if not term:
            raise ValueError("Invalid OR expression: empty term")

        parts = term.split(".")
        if len(parts) < 3:
            raise ValueError(f"Invalid OR expression term: {term}")

        column = _validate_identifier(parts[0])
        negate = False
        operator_index = 1
        if parts[1] == "not":
            negate = True
            operator_index = 2
        if len(parts) <= operator_index:
            raise ValueError(f"Invalid OR expression term: {term}")

        operator = parts[operator_index]
        raw_value = ".".join(parts[operator_index + 1 :])
        if not raw_value:
            raise ValueError(f"Invalid OR expression term: {term}")

        clause: str
        term_params: list[Any] = []

        if operator == "is":
            normalized = raw_value.lower()
            if normalized != "null":
                raise ValueError(f"Unsupported OR expression value for is: {raw_value}")
            clause = f"{column} IS {'NOT ' if negate else ''}NULL"
        elif operator in {"eq", "gt", "gte", "lt", "lte", "ilike"}:
            operator_sql = {
                "eq": "=",
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
                "ilike": "ILIKE",
            }[operator]
            if raw_value == "now()":
                clause = f"{column} {operator_sql} NOW()"
            else:
                clause = f"{column} {operator_sql} %s"
                term_params.append(raw_value)
            if negate:
                clause = f"NOT ({clause})"
        else:
            raise ValueError(f"Unsupported OR expression operator: {operator}")

        clauses.append(clause)
        params.extend(term_params)

    return "(" + " OR ".join(clauses) + ")", params


class DbSchema:
    def __init__(self, session: DbSession, name: str):
        self._session = session
        self._name = name

    def table(self, name: str) -> DbQuery:
        return DbQuery(self._session, self._name, _validate_identifier(name))

    def rpc(self, function_name: str, params: dict[str, Any] | None = None) -> DbRpc:
        safe_params = validate_mapping_keys(params or {})
        return DbRpc(self._session, self._name, _validate_identifier(function_name), safe_params)


class DbQuery:
    def __init__(self, session: DbSession, schema: str, table: str):
        self._session = session
        self._schema = schema
        self._table = table
        self._op: str | None = None
        self._columns: str = "*"
        self._count: str | None = None
        self._filters: list[tuple[str, list[Any]]] = []
        self._order: list[tuple[str, bool, bool | None]] = []
        self._limit: int | None = None
        self._offset: int | None = None
        self._single: bool = False
        self._payload: Any = None
        self._on_conflict: str | None = None
        self._ignore_duplicates: bool = False
        self._default_to_null: bool = True
        self._negate_next: bool = False

    @property
    def not_(self) -> DbQuery:
        self._negate_next = True
        return self

    def select(self, columns: str = "*", count: str | None = None) -> DbQuery:
        self._op = "select"
        self._columns = columns
        self._count = count
        return self

    def insert(self, payload: dict[str, Any] | list[dict[str, Any]]) -> DbQuery:
        self._op = "insert"
        self._payload = payload
        return self

    def update(self, payload: dict[str, Any]) -> DbQuery:
        self._op = "update"
        self._payload = payload
        return self

    def delete(self) -> DbQuery:
        self._op = "delete"
        return self

    def upsert(
        self,
        payload: dict[str, Any] | list[dict[str, Any]],
        *,
        on_conflict: str | None = None,
        ignore_duplicates: bool = False,
        default_to_null: bool = True,
    ) -> DbQuery:
        self._op = "upsert"
        self._payload = payload
        self._on_conflict = _validate_identifier_list(on_conflict) if on_conflict else None
        self._ignore_duplicates = ignore_duplicates
        self._default_to_null = default_to_null
        return self

    def eq(self, column: str, value: Any) -> DbQuery:
        column = _validate_identifier(column)
        return self._add_filter(f"{column} = %s", [value])

    def gt(self, column: str, value: Any) -> DbQuery:
        column = _validate_identifier(column)
        return self._add_filter(f"{column} > %s", [value])

    def gte(self, column: str, value: Any) -> DbQuery:
        column = _validate_identifier(column)
        return self._add_filter(f"{column} >= %s", [value])

    def lt(self, column: str, value: Any) -> DbQuery:
        column = _validate_identifier(column)
        return self._add_filter(f"{column} < %s", [value])

    def lte(self, column: str, value: Any) -> DbQuery:
        column = _validate_identifier(column)
        return self._add_filter(f"{column} <= %s", [value])

    def in_(self, column: str, values: Iterable[Any]) -> DbQuery:
        column = _validate_identifier(column)
        vals = list(values)
        if not vals:
            return self._add_filter("FALSE", [])
        placeholders = ",".join(["%s"] * len(vals))
        return self._add_filter(f"{column} IN ({placeholders})", vals)

    def json_text_in(self, column: str, key: str, values: Iterable[Any]) -> DbQuery:
        column = _validate_identifier(column)
        key = _validate_identifier(key)
        vals = list(values)
        if not vals:
            return self._add_filter("FALSE", [])
        placeholders = ",".join(["%s"] * len(vals))
        return self._add_filter(f"{column} ->> %s IN ({placeholders})", [key, *vals])

    def ilike(self, column: str, pattern: str) -> DbQuery:
        column = _validate_identifier(column)
        return self._add_filter(f"{column} ILIKE %s", [pattern])

    def or_(self, expression: str) -> DbQuery:
        clause, params = _parse_or_expression(expression)
        return self._add_filter(clause, params)

    def is_(self, column: str, value: Any) -> DbQuery:
        column = _validate_identifier(column)
        if value is None or (isinstance(value, str) and value.lower() == "null"):
            return self._add_filter(f"{column} IS NULL", [])
        if isinstance(value, str) and value.lower().replace("_", " ") in {"not null", "notnull"}:
            return self._add_filter(f"{column} IS NOT NULL", [])
        return self._add_filter(f"{column} IS %s", [value])

    def order(self, column: str, *, desc: bool = False, nullsfirst: bool | None = None) -> DbQuery:
        column = _validate_identifier(column)
        self._order.append((column, desc, nullsfirst))
        return self

    def range(self, start: int, end: int) -> DbQuery:
        self._offset = int(start)
        self._limit = int(end) - int(start) + 1
        return self

    def limit(self, count: int) -> DbQuery:
        self._limit = int(count)
        return self

    def single(self) -> DbQuery:
        self._single = True
        return self

    def maybe_single(self) -> DbQuery:
        self._single = True
        return self

    def execute(self) -> DbResponse:
        try:
            if self._op == "select":
                return self._execute_select()
            if self._op == "insert":
                return self._execute_insert()
            if self._op == "update":
                return self._execute_update()
            if self._op == "delete":
                return self._execute_delete()
            if self._op == "upsert":
                return self._execute_upsert()
            raise ValueError("No operation specified")
        except Exception as exc:
            return DbResponse(data=None, error=DbError(message=str(exc)))

    def _add_filter(self, clause: str, params: list[Any]) -> DbQuery:
        if self._negate_next:
            clause = f"NOT ({clause})"
            self._negate_next = False
        self._filters.append((clause, params))
        return self

    def _build_where(self) -> tuple[str, list[Any]]:
        if not self._filters:
            return "", []
        clauses: list[str] = []
        params: list[Any] = []
        for clause, clause_params in self._filters:
            clauses.append(clause)
            params.extend(clause_params)
        return " WHERE " + " AND ".join(clauses), params

    def _build_order(self) -> str:
        if not self._order:
            return ""
        parts: list[str] = []
        for column, desc, nullsfirst in self._order:
            direction = "DESC" if desc else "ASC"
            nulls = ""
            if nullsfirst is True:
                nulls = " NULLS FIRST"
            elif nullsfirst is False:
                nulls = " NULLS LAST"
            parts.append(f"{column} {direction}{nulls}")
        return " ORDER BY " + ", ".join(parts)

    def _build_limit_offset(self) -> str:
        parts: list[str] = []
        if self._limit is not None:
            parts.append(f" LIMIT {self._limit}")
        if self._offset is not None:
            parts.append(f" OFFSET {self._offset}")
        return "".join(parts)

    def _execute_select(self) -> DbResponse:
        where_sql, params = self._build_where()
        order_sql = self._build_order()
        limit_sql = self._build_limit_offset()
        sql = f"SELECT {self._columns} FROM {self._schema}.{self._table}{where_sql}{order_sql}{limit_sql}"
        with db_read_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
            count_val: int | None = None
            if self._count == "exact":
                count_sql = f"SELECT COUNT(*) FROM {self._schema}.{self._table}{where_sql}"
                with conn.cursor() as count_cur:
                    count_cur.execute(count_sql, params)
                    count_val = int(cast("Any", count_cur.fetchone())[0])
        data: Any
        if self._single:
            data = dict(rows[0]) if rows else None
        else:
            data = [dict(r) for r in rows]

        return DbResponse(data=data, error=None, count=count_val)

    def _normalize_rows(self) -> tuple[list[str], list[list[Any]]]:
        if isinstance(self._payload, list):
            rows = self._payload
        else:
            rows = [self._payload]
        if not rows:
            return [], []
        columns = sorted({_validate_identifier(key) for row in rows for key in row.keys()})
        values: list[list[Any]] = []
        for row in rows:
            values.append(
                [Json(row.get(col)) if isinstance(row.get(col), (dict, list)) else row.get(col) for col in columns]
            )
        return columns, values

    def _execute_insert(self) -> DbResponse:
        columns, values = self._normalize_rows()
        if not columns:
            return DbResponse(data=[], error=None)
        cols_sql = ",".join(columns)
        sql = f"INSERT INTO {self._schema}.{self._table} ({cols_sql}) VALUES %s RETURNING *"
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                execute_values(cur, sql, values)
                rows = cur.fetchall()
        return DbResponse(data=[dict(r) for r in rows], error=None)

    def _execute_update(self) -> DbResponse:
        if not isinstance(self._payload, dict):
            raise ValueError("Update payload must be a dict")
        validate_mapping_keys(self._payload)
        set_cols = sorted(self._payload.keys())
        if not set_cols:
            return DbResponse(data=[], error=None)
        set_sql = ",".join([f"{col} = %s" for col in set_cols])
        params = [
            Json(self._payload[col]) if isinstance(self._payload[col], (dict, list)) else self._payload[col]
            for col in set_cols
        ]
        where_sql, where_params = self._build_where()
        sql = f"UPDATE {self._schema}.{self._table} SET {set_sql}{where_sql} RETURNING *"
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params + where_params)
                rows = cur.fetchall()
        return DbResponse(data=[dict(r) for r in rows], error=None)

    def _execute_delete(self) -> DbResponse:
        where_sql, params = self._build_where()
        sql = f"DELETE FROM {self._schema}.{self._table}{where_sql} RETURNING *"
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return DbResponse(data=[dict(r) for r in rows], error=None)

    def _execute_upsert(self) -> DbResponse:
        columns, values = self._normalize_rows()
        if not columns:
            return DbResponse(data=[], error=None)
        cols_sql = ",".join(columns)
        sql = f"INSERT INTO {self._schema}.{self._table} ({cols_sql}) VALUES %s"
        if self._on_conflict:
            if self._ignore_duplicates:
                sql += f" ON CONFLICT ({self._on_conflict}) DO NOTHING"
            else:
                update_cols = list(columns)
                update_sql = ",".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                sql += f" ON CONFLICT ({self._on_conflict}) DO UPDATE SET {update_sql}"
        sql += " RETURNING *"
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                execute_values(cur, sql, values)
                rows = cur.fetchall()
        return DbResponse(data=[dict(r) for r in rows], error=None)


class DbRpc:
    def __init__(self, session: DbSession, schema: str, function_name: str, params: dict[str, Any]):
        self._session = session
        self._schema = schema
        self._function_name = function_name
        self._params = params

    def execute(self) -> DbResponse:
        try:
            if self._params:
                arg_sql = ", ".join([f"{key} := %s" for key in self._params.keys()])
                sql = f"SELECT * FROM {self._schema}.{self._function_name}({arg_sql})"
                params = [
                    self._params[key] if not isinstance(self._params[key], (dict, list)) else Json(self._params[key])
                    for key in self._params
                ]
            else:
                sql = f"SELECT * FROM {self._schema}.{self._function_name}()"
                params = []
            with db_read_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall()
            return DbResponse(data=[dict(r) for r in rows], error=None)
        except Exception as exc:
            return DbResponse(data=None, error=DbError(message=str(exc)))


def get_db_session() -> DbSession:
    return DbSession()


def get_db() -> DbSession:
    """FastAPI dependency compatibility wrapper for DbSession."""
    return get_db_session()
