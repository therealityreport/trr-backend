"""Date-independent recovery configuration with legacy launch compatibility."""

from __future__ import annotations

import os
from collections.abc import Mapping

from trr_backend.ingestion.domain import RunIntent


def recovery_value(suffix: str, *, environ: Mapping[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    current = env.get("TRR_INSTAGRAM_" + suffix, "").strip()
    legacy = env.get("TRR_INSTAGRAM_JANUARY_" + suffix, "").strip()
    if current and legacy and current != legacy:
        raise ValueError("Conflicting recovery configuration")
    return current or legacy


def certification_dates(payload: dict, expected_hash: str, account: str) -> tuple[str, str]:
    intent = RunIntent.from_payload(payload)
    if intent.digest != expected_hash or intent.account != account:
        raise ValueError("Certification intent binding mismatch")
    if intent.date_start is None or intent.date_end is None or intent.date_start >= intent.date_end:
        raise ValueError("Certification requires an explicit date range")
    return intent.date_start.isoformat(), intent.date_end.isoformat()


def recovery_overrides(values: Mapping[str, str]) -> dict[str, str]:
    """Set both names, including empty values that deliberately disable a hook."""
    return {
        prefix + suffix: value
        for suffix, value in values.items()
        for prefix in ("TRR_INSTAGRAM_", "TRR_INSTAGRAM_JANUARY_")
    }
