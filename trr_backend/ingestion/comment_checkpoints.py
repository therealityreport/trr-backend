"""Compact immutable page proofs; mutable target progress remains version one.

The existing page evidence and canonical_identity retain each page's comments.
Repeating the accumulated observation and cursor maps in every receipt adds no
recovery evidence. Hash those maps instead, and verify against target progress
on resume or against native evidence replay in the audit. No historical receipt
needs rewriting, and a compact receipt is never itself a resumable checkpoint.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from trr_backend.ingestion.domain import fingerprint

COMPACT_PAGE_VERSION = 2
_STORAGE = "retained_page_evidence"
_MAPS = ("observed", "lanes")


def _page_state(checkpoint: Mapping[str, Any]) -> dict[str, Any]:
    state = checkpoint.get("comment_pages") or {}
    if not isinstance(state, dict) or state.get("version", 1) not in (1, COMPACT_PAGE_VERSION):
        raise ValueError("Unsupported comment page proof version")
    if state.get("version") == COMPACT_PAGE_VERSION and (
        state.get("storage") != _STORAGE or not isinstance(state.get("metadata"), dict)
    ):
        raise ValueError("Invalid compact comment page proof")
    return state


def compact_page_checkpoint(checkpoint: Mapping[str, Any]) -> dict[str, Any]:
    """Build a receipt only after canonical writes, in their shared transaction."""
    state = _page_state(checkpoint)
    if state.get("version", 1) != 1 or any(not isinstance(state.get(key), dict) for key in _MAPS):
        raise ValueError("Compact page proof requires full target progress")
    return {
        "comment_pages": {
            "version": COMPACT_PAGE_VERSION,
            "storage": _STORAGE,
            "metadata": {key: value for key, value in state.items() if key not in (*_MAPS, "reply_access")},
            "observed_sha256": fingerprint(state["observed"]),
            "lanes_sha256": fingerprint(state["lanes"]),
            "observed_count": len(state["observed"]),
            "exhausted_count": sum(lane.get("exhausted") is True for lane in state["lanes"].values()),
        }
    }


def page_progress_metadata(checkpoint: Mapping[str, Any]) -> dict[str, Any]:
    """Read scan metadata from either format without pretending to hydrate maps."""
    state = _page_state(checkpoint)
    return dict(state["metadata"] if state.get("version") == COMPACT_PAGE_VERSION else state)


def page_progress_matches(
    checkpoint: Mapping[str, Any], state: Mapping[str, Any], *, fields: Sequence[str] | None = None
) -> bool:
    """Verify native page progress, excluding independent reply administration.

    Audits compare their explicitly reconstructed fields. Resume compares every
    page-derived field, while retaining the legacy absence of a scan cutoff.
    """
    saved = _page_state(checkpoint)
    if saved.get("version") != COMPACT_PAGE_VERSION:
        saved = {key: value for key, value in saved.items() if key != "reply_access"}
        expected = {key: value for key, value in state.items() if key != "reply_access"}
        if "scan_started_at" not in expected:
            saved.pop("scan_started_at", None)
        if fields is not None:
            return all(saved.get(key) == expected.get(key) for key in fields)
        return saved == expected
    metadata = page_progress_metadata(checkpoint)
    expected = {key: value for key, value in state.items() if key not in (*_MAPS, "reply_access")}
    if "scan_started_at" not in expected:
        metadata.pop("scan_started_at", None)
    compared = tuple(fields) if fields is not None else tuple(set(metadata) | set(expected) | set(_MAPS))
    for key in compared:
        if key in _MAPS:
            if not isinstance(state.get(key), dict) or saved.get(key + "_sha256") != fingerprint(state[key]):
                return False
        elif metadata.get(key) != expected.get(key):
            return False
    if "observed" in compared and (
        type(saved.get("observed_count")) is not int or saved["observed_count"] != len(state.get("observed", {}))
    ):
        return False
    if "lanes" in compared and (
        type(saved.get("exhausted_count")) is not int
        or saved["exhausted_count"] != sum(lane.get("exhausted") is True for lane in state.get("lanes", {}).values())
    ):
        return False
    return True
