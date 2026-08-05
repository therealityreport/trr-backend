"""Pure normalization helpers for persisted Instagram audit cursors."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

AUDIT_CURSOR_REPLY_CHECKPOINT_MAX_ITEMS = 2000
AUDIT_CURSOR_RETRYABLE_STOP_REASONS = frozenset(
    {
        "auth_relay_fallback_recovered",
        "auth_rendered_fallback_recovered",
        "coauthor_auth_relay_fallback_recovered",
        "coauthor_auth_rendered_fallback_recovered",
        "coauthor_comments_endpoint_empty",
        "comments_endpoint_status_only",
        "hidden_comments_blocked",
        "hidden_comments_unavailable",
        "hidden_comments_unavailable_reconciled",
        "hidden_comments_unresolved",
        "html_challenge_or_auth_required",
        "http_429",
        "network_budget_exhausted",
        "network_policy_blocked",
        "network_stop",
        "network_stopped",
        "pagination_page_cap_reached",
        "persisted_reply_topology_gap",
        "proxy_budget_exhausted",
        "proxy_network_stop",
        "reply_tail_budget_exhausted",
        "reply_tail_incomplete",
        "static_cdn_budget_exhausted",
    }
)
AUDIT_CURSOR_TERMINAL_STOP_REASONS = frozenset(
    {
        "coverage_terminal_missing_classified",
        "pagination_repeated_cursor",
    }
)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(decoded) if isinstance(decoded, dict) else {}
    return {}


def _audit_cursor_stop_reason_is_retryable(value: Any) -> bool:
    stop_reason = str(value or "").strip()
    if stop_reason in AUDIT_CURSOR_TERMINAL_STOP_REASONS:
        return False
    return stop_reason in AUDIT_CURSOR_RETRYABLE_STOP_REASONS


def _cursor_param_value(value: Any) -> str | None:
    cursor_param = str(value or "").strip()
    return cursor_param if cursor_param in {"min_id", "max_id"} else None


def _audit_row_stop_reason(
    row: Mapping[str, Any],
    payload: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
) -> str:
    return str(
        checkpoint.get("stop_reason")
        or checkpoint.get("last_error_code")
        or payload.get("stop_reason")
        or row.get("cursor_stop_reason")
        or ""
    ).strip()


def normalize_audit_top_level_checkpoint(row: Mapping[str, Any]) -> dict[str, Any] | None:
    payload = _json_object(row.get("cursor_payload"))
    shortcode = str(row.get("shortcode") or "").strip()
    checkpoint_candidates: list[Mapping[str, Any]] = []
    top_level_checkpoint = payload.get("top_level_checkpoint")
    if isinstance(top_level_checkpoint, Mapping):
        checkpoint_candidates.append(top_level_checkpoint)
    if any(
        str(payload.get(key) or "").strip()
        for key in (
            "next_top_level_cursor",
            "last_top_level_cursor",
            "chosen_cursor",
            "request_cursor",
        )
    ):
        checkpoint_candidates.append(payload)
    for checkpoint in checkpoint_candidates:
        stop_reason = _audit_row_stop_reason(row, payload, checkpoint)
        if not _audit_cursor_stop_reason_is_retryable(stop_reason):
            continue
        target_shortcode = str(
            checkpoint.get("target_shortcode")
            or checkpoint.get("source_id")
            or checkpoint.get("shortcode")
            or shortcode
        ).strip()
        next_cursor = str(
            checkpoint.get("next_top_level_cursor")
            or checkpoint.get("chosen_cursor")
            or row.get("cursor_min_id")
            or ""
        ).strip()
        last_cursor = str(checkpoint.get("last_top_level_cursor") or checkpoint.get("request_cursor") or "").strip()
        payload_next_cursor = str(payload.get("chosen_cursor") or payload.get("next_top_level_cursor") or "").strip()
        cursor_repair: dict[str, Any] | None = None
        if next_cursor and last_cursor and next_cursor == last_cursor and payload_next_cursor != next_cursor:
            cursor_repair = {
                "applied": True,
                "reason": "degenerate_top_level_cursor_replayed",
                "source": "cursor_payload.chosen_cursor",
                "from_next_top_level_cursor": next_cursor,
                "to_next_top_level_cursor": payload_next_cursor,
            }
            next_cursor = payload_next_cursor
        cursor = next_cursor or last_cursor
        if not target_shortcode or not cursor:
            continue
        next_cursor_param = _cursor_param_value(
            checkpoint.get("next_top_level_cursor_param")
            or checkpoint.get("chosen_cursor_param")
            or row.get("cursor_param")
        )
        last_cursor_param = _cursor_param_value(
            checkpoint.get("last_top_level_cursor_param") or checkpoint.get("request_cursor_param")
        )
        payload_next_cursor_param = _cursor_param_value(
            payload.get("chosen_cursor_param") or payload.get("cursor_param")
        )
        if payload_next_cursor and next_cursor == payload_next_cursor and payload_next_cursor_param:
            next_cursor_param = payload_next_cursor_param
        normalized = {
            "platform": "instagram",
            "target_shortcode": target_shortcode,
            "source_id": target_shortcode,
            "stop_reason": stop_reason,
            "retryable": True,
        }
        if next_cursor:
            normalized["next_top_level_cursor"] = next_cursor
        if next_cursor_param:
            normalized["next_top_level_cursor_param"] = next_cursor_param
        if last_cursor:
            normalized["last_top_level_cursor"] = last_cursor
        if last_cursor_param:
            normalized["last_top_level_cursor_param"] = last_cursor_param
        if cursor_repair:
            normalized["cursor_repair_applied"] = True
            normalized["cursor_repair_reason"] = cursor_repair["reason"]
            normalized["cursor_repair_source"] = cursor_repair["source"]
            normalized["cursor_repair"] = cursor_repair
        for key in ("media_id", "pages_seen", "observed_comment_count", "expected_comment_count", "updated_at"):
            if checkpoint.get(key) is not None:
                normalized[key] = checkpoint.get(key)
        return normalized
    return None


def _audit_reply_checkpoint_candidates(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    candidates: list[Mapping[str, Any]] = []
    value = payload.get("reply_checkpoints")
    if isinstance(value, list):
        candidates.extend(item for item in value if isinstance(item, Mapping))
    for key in ("reply_checkpoint_summary", "reply_checkpoint_metadata"):
        value = payload.get(key)
        if not isinstance(value, Mapping):
            continue
        latest = value.get("latest")
        if isinstance(latest, Mapping):
            candidates.append(latest)
        items = value.get("items")
        if isinstance(items, list):
            candidates.extend(item for item in items if isinstance(item, Mapping))
    runtime = payload.get("fetcher_runtime")
    if isinstance(runtime, Mapping):
        checkpoint_metadata = runtime.get("reply_checkpoint_metadata")
        if isinstance(checkpoint_metadata, Mapping) and isinstance(checkpoint_metadata.get("items"), list):
            candidates.extend(item for item in checkpoint_metadata.get("items") or [] if isinstance(item, Mapping))
    return candidates


def normalize_audit_reply_checkpoints(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    payload = _json_object(row.get("cursor_payload"))
    shortcode = str(row.get("shortcode") or "").strip() or None
    by_parent: dict[str, dict[str, Any]] = {}
    for checkpoint in _audit_reply_checkpoint_candidates(payload):
        stop_reason = _audit_row_stop_reason(row, payload, checkpoint)
        if not _audit_cursor_stop_reason_is_retryable(stop_reason):
            continue
        parent_comment_id = str(checkpoint.get("parent_comment_id") or "").strip()
        cursor = str(checkpoint.get("next_reply_cursor") or checkpoint.get("last_reply_cursor") or "").strip()
        if not parent_comment_id or not cursor:
            continue
        next_cursor_param = _cursor_param_value(checkpoint.get("next_reply_cursor_param"))
        last_cursor_param = _cursor_param_value(checkpoint.get("last_reply_cursor_param"))
        normalized = {
            "platform": "instagram",
            "target_shortcode": str(
                checkpoint.get("target_shortcode") or checkpoint.get("source_id") or shortcode or ""
            )
            or None,
            "source_id": str(checkpoint.get("source_id") or checkpoint.get("target_shortcode") or shortcode or "")
            or None,
            "parent_comment_id": parent_comment_id,
            "stop_reason": stop_reason,
            "retryable": True,
        }
        if checkpoint.get("next_reply_cursor"):
            normalized["next_reply_cursor"] = cursor
        else:
            normalized["last_reply_cursor"] = cursor
        if next_cursor_param:
            normalized["next_reply_cursor_param"] = next_cursor_param
        if last_cursor_param:
            normalized["last_reply_cursor_param"] = last_cursor_param
        for key in (
            "attempt_count",
            "expected_reply_count",
            "saved_reply_count_observed",
            "pages_seen",
            "updated_at",
        ):
            if checkpoint.get(key) is not None:
                normalized[key] = checkpoint.get(key)
        by_parent[parent_comment_id] = {key: value for key, value in normalized.items() if value is not None}
    items = list(by_parent.values())
    return items[-AUDIT_CURSOR_REPLY_CHECKPOINT_MAX_ITEMS:]


__all__ = [
    "AUDIT_CURSOR_REPLY_CHECKPOINT_MAX_ITEMS",
    "AUDIT_CURSOR_RETRYABLE_STOP_REASONS",
    "AUDIT_CURSOR_TERMINAL_STOP_REASONS",
    "normalize_audit_reply_checkpoints",
    "normalize_audit_top_level_checkpoint",
]
