"""Shared post persistence truthfulness metadata for social scraper jobs."""

from __future__ import annotations

from typing import Any


def _normalize_non_negative_int(value: Any) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _normalize_reason_counts(value: Any) -> dict[str, int]:
    payload = value if isinstance(value, dict) else {}
    return {
        str(reason).strip(): _normalize_non_negative_int(count)
        for reason, count in payload.items()
        if str(reason).strip() and _normalize_non_negative_int(count) > 0
    }


def build_post_persist_truthfulness(
    *,
    platform: str,
    account: str | None,
    status: str,
    posts_checked: Any,
    posts_upserted: Any,
    posts_skipped: Any = 0,
    posts_skipped_by_reason: Any = None,
    media_assets_persisted: Any = 0,
) -> dict[str, Any]:
    normalized_platform = str(platform or "social").strip().lower() or "social"
    checked = _normalize_non_negative_int(posts_checked)
    upserted = _normalize_non_negative_int(posts_upserted)
    skipped = _normalize_non_negative_int(posts_skipped)
    media_assets = _normalize_non_negative_int(media_assets_persisted)
    skipped_by_reason = _normalize_reason_counts(posts_skipped_by_reason)
    silent_drop_detected = (
        str(status or "").strip().lower() == "completed" and checked > 0 and upserted == 0 and media_assets <= 0
    )
    summary: dict[str, Any] = {
        "platform": normalized_platform,
        "account": str(account or "").strip().lstrip("@") or None,
        "posts_checked": checked,
        "posts_upserted": upserted,
        "posts_skipped": skipped,
        "posts_skipped_by_reason": skipped_by_reason,
        "media_assets_persisted": media_assets,
        "silent_drop_detected": silent_drop_detected,
    }
    if silent_drop_detected:
        summary["status_resolution"] = "completed_with_silent_drop_alert"
        summary["operator_summary"] = (
            f"{normalized_platform.title()} posts persistence completed with zero saved posts "
            "after checking live posts."
        )
    return summary


def persist_summary_from_truthfulness(truthfulness: dict[str, Any]) -> dict[str, Any]:
    return {
        "posts_upserted": _normalize_non_negative_int(truthfulness.get("posts_upserted")),
        "posts_skipped": _normalize_non_negative_int(truthfulness.get("posts_skipped")),
        "posts_skipped_by_reason": _normalize_reason_counts(truthfulness.get("posts_skipped_by_reason")),
    }


def apply_post_persist_truthfulness_metadata(
    metadata: dict[str, Any],
    *,
    platform: str,
    account: str | None,
    status: str,
    posts_checked: Any,
    posts_upserted: Any,
    posts_skipped: Any = 0,
    posts_skipped_by_reason: Any = None,
    media_assets_persisted: Any = 0,
    alias_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    updated = dict(metadata or {})
    truthfulness = build_post_persist_truthfulness(
        platform=platform,
        account=account,
        status=status,
        posts_checked=posts_checked,
        posts_upserted=posts_upserted,
        posts_skipped=posts_skipped,
        posts_skipped_by_reason=posts_skipped_by_reason,
        media_assets_persisted=media_assets_persisted,
    )
    persist_summary = persist_summary_from_truthfulness(truthfulness)
    persist_counters = dict(updated.get("persist_counters") or {})
    persist_counters.update(persist_summary)
    updated["persist_counters"] = persist_counters
    updated["posts_scrapling_persist_diagnostics"] = persist_summary
    for alias_key in alias_keys:
        normalized_alias = str(alias_key or "").strip()
        if normalized_alias:
            updated[normalized_alias] = persist_summary
    diagnostics = dict(updated.get("diagnostics") or {})
    diagnostics["post_persist_truthfulness"] = truthfulness
    updated["diagnostics"] = diagnostics
    alert_code = f"{str(platform or 'social').strip().lower() or 'social'}_posts_persist_zero_saved"
    alerts = [dict(item) for item in list(updated.get("alerts") or []) if isinstance(item, dict)]
    if truthfulness.get("silent_drop_detected"):
        if not any(str(item.get("code") or "") == alert_code for item in alerts):
            alerts.append(
                {
                    "code": alert_code,
                    "severity": "warning",
                    "message": truthfulness.get("operator_summary"),
                }
            )
        updated["alerts"] = alerts
    else:
        filtered_alerts = [item for item in alerts if str(item.get("code") or "") != alert_code]
        if len(filtered_alerts) != len(alerts):
            if filtered_alerts:
                updated["alerts"] = filtered_alerts
            else:
                updated.pop("alerts", None)
    return updated
