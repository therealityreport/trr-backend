"""Request identity helpers for Crawlee-backed stages."""

from __future__ import annotations


def _normalize_part(value: object | None, *, fallback: str = "_") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in text)
    compact = "_".join(chunk for chunk in cleaned.split("_") if chunk)
    return compact or fallback


def build_request_key(
    *,
    platform: str,
    target: str,
    post_id: str | None,
    cursor: str | None,
    reply_cursor: str | None,
    mode: str,
) -> str:
    """Build deterministic request key: platform|target|post|cursor|reply_cursor|mode."""
    parts = (
        _normalize_part(platform, fallback="unknown"),
        _normalize_part(target),
        _normalize_part(post_id),
        _normalize_part(cursor),
        _normalize_part(reply_cursor),
        _normalize_part(mode, fallback="default"),
    )
    return "|".join(parts)
