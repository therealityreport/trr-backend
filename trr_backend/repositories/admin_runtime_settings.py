"""Persistent admin runtime settings."""

from __future__ import annotations

import json
from typing import Any

from trr_backend.db import pg

SHOW_CORE_AUTO_REFRESH_SETTING_KEY = "show_core_auto_refresh"
_TABLE_READY = False


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _ensure_runtime_settings_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    pg.execute(
        """
        create table if not exists core.admin_runtime_settings (
          key text primary key,
          value jsonb not null default '{}'::jsonb,
          updated_at timestamptz not null default now(),
          updated_by text null
        )
        """
    )
    _TABLE_READY = True


def get_show_core_auto_refresh_settings() -> dict[str, Any]:
    _ensure_runtime_settings_table()
    row = pg.fetch_one(
        """
        select
          value,
          updated_at,
          updated_by
        from core.admin_runtime_settings
        where key = %s
        limit 1
        """,
        [SHOW_CORE_AUTO_REFRESH_SETTING_KEY],
    )
    value = row.get("value") if row else {}
    payload = value if isinstance(value, dict) else {}
    return {
        "paused": _normalize_bool(payload.get("paused")),
        "updated_at": row.get("updated_at").isoformat() if row and row.get("updated_at") else None,
        "updated_by": row.get("updated_by") if row else None,
    }


def show_core_auto_refresh_paused() -> bool:
    return bool(get_show_core_auto_refresh_settings()["paused"])


def get_runtime_setting(key: str) -> dict[str, Any]:
    _ensure_runtime_settings_table()
    cleaned_key = str(key or "").strip()
    if not cleaned_key:
        return {}
    row = pg.fetch_one(
        """
        select value
        from core.admin_runtime_settings
        where key = %s
        limit 1
        """,
        [cleaned_key],
    )
    value = row.get("value") if row else {}
    return value if isinstance(value, dict) else {}


def set_runtime_setting(key: str, value: dict[str, Any], *, updated_by: str | None = None) -> dict[str, Any]:
    _ensure_runtime_settings_table()
    cleaned_key = str(key or "").strip()
    if not cleaned_key:
        raise ValueError("runtime setting key is required")
    row = pg.fetch_one(
        """
        insert into core.admin_runtime_settings (key, value, updated_at, updated_by)
        values (%s, %s::jsonb, now(), nullif(btrim(%s), ''))
        on conflict (key) do update set
          value = excluded.value,
          updated_at = now(),
          updated_by = excluded.updated_by
        returning value
        """,
        [
            cleaned_key,
            json.dumps(value if isinstance(value, dict) else {}, ensure_ascii=True, default=str),
            updated_by or "",
        ],
    )
    saved = row.get("value") if row else {}
    return saved if isinstance(saved, dict) else {}


def set_show_core_auto_refresh_paused(*, paused: bool, updated_by: str | None = None) -> dict[str, Any]:
    _ensure_runtime_settings_table()
    row = pg.fetch_one(
        """
        insert into core.admin_runtime_settings (key, value, updated_at, updated_by)
        values (%s, jsonb_build_object('paused', %s::boolean), now(), nullif(btrim(%s), ''))
        on conflict (key) do update set
          value = jsonb_set(
            coalesce(core.admin_runtime_settings.value, '{}'::jsonb),
            '{paused}',
            excluded.value->'paused',
            true
          ),
          updated_at = now(),
          updated_by = excluded.updated_by
        returning
          value,
          updated_at,
          updated_by
        """,
        [SHOW_CORE_AUTO_REFRESH_SETTING_KEY, bool(paused), updated_by or ""],
    )
    value = row.get("value") if row else {}
    payload = value if isinstance(value, dict) else {}
    return {
        "paused": _normalize_bool(payload.get("paused")),
        "updated_at": row.get("updated_at").isoformat() if row and row.get("updated_at") else None,
        "updated_by": row.get("updated_by") if row else None,
    }
