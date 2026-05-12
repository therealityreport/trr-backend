"""Instagram account-profile comment breakdown contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any


def _normalize_non_negative_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        text = str(value or "").strip()
        if not text:
            return 0
        digits = "".join(ch for ch in text if ch.isdigit())
        return max(0, int(digits)) if digits else 0


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        candidate = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return candidate.isoformat()
    text = str(value or "").strip()
    return text or None


def _raw_data(row: Mapping[str, Any]) -> dict[str, Any]:
    raw = row.get("raw_data")
    return dict(raw) if isinstance(raw, Mapping) else {}


def instagram_facebook_comment_count_from_row(row: Mapping[str, Any]) -> int:
    raw = _raw_data(row)
    raw_facebook_crosspost = _metadata_dict(raw.get("facebook_crosspost"))
    return max(
        _normalize_non_negative_int(row.get("facebook_comments")),
        _normalize_non_negative_int(row.get("fb_comment_count")),
        _normalize_non_negative_int(raw_facebook_crosspost.get("comments_count")),
        _normalize_non_negative_int(raw_facebook_crosspost.get("fb_comment_count")),
        _normalize_non_negative_int(raw.get("fb_comment_count")),
    )


def instagram_facebook_crosspost_payload_from_row(
    row: Mapping[str, Any],
    *,
    facebook_comments: int | None = None,
) -> dict[str, Any]:
    raw = _raw_data(row)
    raw_facebook_crosspost = _metadata_dict(raw.get("facebook_crosspost"))
    resolved_comments = (
        _normalize_non_negative_int(facebook_comments)
        if facebook_comments is not None
        else instagram_facebook_comment_count_from_row(row)
    )
    metadata = (
        _metadata_dict(row.get("crosspost_metadata"))
        or _metadata_dict(row.get("facebook_crosspost_metadata"))
        or _metadata_dict(raw_facebook_crosspost.get("metadata"))
    )
    social_context = (
        _metadata_dict(row.get("social_context"))
        or _metadata_dict(row.get("facebook_social_context"))
        or _metadata_dict(raw_facebook_crosspost.get("social_context"))
    )
    return {
        "comments_count": resolved_comments,
        "likes_count": (
            _normalize_non_negative_int(row.get("fb_like_count"))
            if row.get("fb_like_count") is not None
            else (
                _normalize_non_negative_int(raw_facebook_crosspost.get("likes_count"))
                if raw_facebook_crosspost.get("likes_count") is not None
                else None
            )
        ),
        "is_shared_to_fb": (
            row.get("is_shared_to_fb")
            if isinstance(row.get("is_shared_to_fb"), bool)
            else (
                raw_facebook_crosspost.get("is_shared_to_fb")
                if isinstance(raw_facebook_crosspost.get("is_shared_to_fb"), bool)
                else None
            )
        ),
        "post_id": str(row.get("facebook_post_id") or raw_facebook_crosspost.get("post_id") or "").strip() or None,
        "post_url": str(row.get("facebook_post_url") or raw_facebook_crosspost.get("post_url") or "").strip() or None,
        "metadata": metadata,
        "social_context": social_context,
        "observed_at": _iso(row.get("facebook_crosspost_observed_at") or raw_facebook_crosspost.get("observed_at")),
        "source": str(row.get("facebook_crosspost_source") or raw_facebook_crosspost.get("source") or "").strip()
        or None,
    }


def _normalized_missing_reasons(
    missing_reasons: Mapping[str, Any] | None,
    *,
    missing_comments: int,
    classified_missing_comments: int,
) -> dict[str, int]:
    reasons: dict[str, int] = {}
    for key, value in dict(missing_reasons or {}).items():
        reason = str(key or "").strip()
        count = _normalize_non_negative_int(value)
        if reason and count > 0:
            reasons[reason] = reasons.get(reason, 0) + count
    if reasons:
        classified_total = sum(reasons.values())
        if missing_comments > classified_total:
            reasons["instagram_not_served_after_all_lanes"] = (
                reasons.get("instagram_not_served_after_all_lanes", 0) + missing_comments - classified_total
            )
        return reasons
    if missing_comments > 0:
        return {"instagram_not_served_after_all_lanes": missing_comments}
    if classified_missing_comments > 0:
        return {"instagram_not_served_after_all_lanes": classified_missing_comments}
    return {}


def _normalize_capture_health(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    phase_counts = {
        str(key or "").strip(): _normalize_non_negative_int(count)
        for key, count in _metadata_dict(value.get("phase_counts")).items()
        if str(key or "").strip()
    }
    cursor_param_counts = {
        str(key or "").strip(): _normalize_non_negative_int(count)
        for key, count in _metadata_dict(value.get("cursor_param_counts")).items()
        if str(key or "").strip()
    }
    payload = {
        "capture_rate": value.get("capture_rate"),
        "phase_counts": phase_counts,
        "cursor_param_counts": cursor_param_counts,
        "covered_comments": _normalize_non_negative_int(value.get("covered_comments")),
        "status_counts": {
            str(key or "").strip(): _normalize_non_negative_int(count)
            for key, count in _metadata_dict(value.get("status_counts")).items()
            if str(key or "").strip()
        },
        "observed_at": _iso(value.get("observed_at")),
    }
    return {key: item for key, item in payload.items() if item not in ({}, None)}


def build_instagram_capture_rate_trend(rows: Sequence[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    trend: list[dict[str, Any]] = []
    for row in list(rows or []):
        fetched = _normalize_non_negative_int(row.get("fetched_comments") or row.get("comments_fetched"))
        reported = _normalize_non_negative_int(row.get("reported_comments") or row.get("target_comments"))
        capture_rate = None
        if reported > 0:
            capture_rate = round(min(fetched / reported, 1.0), 4)
        trend.append(
            {
                "run_id": str(row.get("run_id") or "").strip() or None,
                "observed_at": _iso(row.get("observed_at") or row.get("created_at")),
                "reported_comments": reported,
                "fetched_comments": fetched,
                "capture_rate": capture_rate,
            }
        )
    return trend


def build_instagram_comment_breakdown(
    *,
    reported_comments: Any,
    saved_parent_comments: Any = 0,
    saved_child_replies: Any = 0,
    expected_child_replies: Any | None = None,
    facebook_comments: Any = 0,
    classified_missing_comments: Any = 0,
    missing_reasons: Mapping[str, Any] | None = None,
    facebook_crosspost_observed_at: Any = None,
    facebook_crosspost_source: Any = None,
    capture_health: Mapping[str, Any] | None = None,
    capture_rate_trend: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    reported = _normalize_non_negative_int(reported_comments)
    parents = _normalize_non_negative_int(saved_parent_comments)
    child_replies = _normalize_non_negative_int(saved_child_replies)
    expected_child_reply_count = (
        _normalize_non_negative_int(expected_child_replies) if expected_child_replies is not None else None
    )
    missing_child_replies = (
        max(expected_child_reply_count - child_replies, 0) if expected_child_reply_count is not None else 0
    )
    facebook = _normalize_non_negative_int(facebook_comments)
    classified_missing = _normalize_non_negative_int(classified_missing_comments)
    saved_instagram = parents + child_replies
    missing = max(reported - saved_instagram - facebook - classified_missing, 0)
    accounted = saved_instagram + facebook + classified_missing + missing
    classified_label = f"{classified_missing} classified missing comments + " if classified_missing > 0 else ""
    formula_label = (
        f"{parents} parent comments + {child_replies} child replies + "
        f"{facebook} Facebook comments + {classified_label}"
        f"{missing} missing comments = {reported} reported comments"
    )
    payload: dict[str, Any] = {
        "reported_comments": reported,
        "saved_parent_comments": parents,
        "saved_child_replies": child_replies,
        "facebook_comments": facebook,
        "saved_instagram_comments": saved_instagram,
        "accounted_comments": accounted,
        "missing_comments": missing,
        "missing_reasons": _normalized_missing_reasons(
            missing_reasons,
            missing_comments=missing,
            classified_missing_comments=classified_missing,
        ),
        "formula_label": formula_label,
    }
    if classified_missing > 0:
        payload["classified_missing_comments"] = classified_missing
    if expected_child_reply_count is not None:
        payload["expected_child_replies"] = expected_child_reply_count
        payload["missing_child_replies"] = missing_child_replies
        payload["reply_accounting_status"] = "incomplete_retryable" if missing_child_replies > 0 else "complete"
    if facebook > 0:
        observed_at = _iso(facebook_crosspost_observed_at)
        freshness = {
            "status": "observed" if observed_at else "unknown",
            "observed_at": observed_at,
            "source": str(facebook_crosspost_source or "").strip() or None,
        }
        payload["facebook_comments_freshness"] = freshness
        if not observed_at:
            payload["warnings"] = ["facebook_comment_count_freshness_unknown"]
    normalized_capture_health = _normalize_capture_health(capture_health)
    if normalized_capture_health:
        payload["capture_health"] = normalized_capture_health
    normalized_capture_rate_trend = build_instagram_capture_rate_trend(capture_rate_trend)
    if normalized_capture_rate_trend:
        payload["capture_rate_trend"] = normalized_capture_rate_trend
    return payload


def instagram_comment_completeness_from_breakdown(breakdown: Mapping[str, Any]) -> dict[str, int]:
    reported = _normalize_non_negative_int(breakdown.get("reported_comments"))
    facebook = _normalize_non_negative_int(breakdown.get("facebook_comments"))
    classified_missing = _normalize_non_negative_int(breakdown.get("classified_missing_comments"))
    saved_instagram = _normalize_non_negative_int(breakdown.get("saved_instagram_comments"))
    missing_child_replies = _normalize_non_negative_int(breakdown.get("missing_child_replies"))
    instagram_fetchable = max(reported - facebook, 0)
    return {
        "reported_comments": reported,
        "external_facebook_comments": facebook,
        "instagram_fetchable_comments": instagram_fetchable,
        "saved_instagram_comments": saved_instagram,
        "missing_instagram_comments": max(
            instagram_fetchable - saved_instagram - classified_missing,
            missing_child_replies,
        ),
        "missing_child_replies": missing_child_replies,
    }
