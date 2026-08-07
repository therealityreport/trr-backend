"""Unit tests for the Instagram comments backfill date-window helper.

These cover the contract REVISED_PLAN section 1 relies on: ISO 8601 parsing
to timezone-aware UTC, inclusive start / exclusive end semantics, the
unbounded (None, None) passthrough, and ValueError on malformed input. No DB
access is required.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from trr_backend.socials.pipelines.comments.instagram import (
    _comment_date_window_predicate,
    _normalize_comment_date_window,
)


def test_both_absent_returns_none_none() -> None:
    assert _normalize_comment_date_window(None, None) == (None, None)
    # Blank/whitespace strings normalize to absent as well.
    assert _normalize_comment_date_window("", "   ") == (None, None)


def test_parses_iso_to_utc_aware_datetimes() -> None:
    start, end = _normalize_comment_date_window("2024-01-01", "2024-02-01")
    assert start is not None
    assert end is not None
    assert start == datetime(2024, 1, 1, tzinfo=UTC)
    assert end == datetime(2024, 2, 1, tzinfo=UTC)
    assert start.tzinfo is UTC
    assert end.tzinfo is UTC


def test_z_suffix_and_offset_normalize_to_utc() -> None:
    start, _ = _normalize_comment_date_window("2024-03-10T12:00:00Z", None)
    assert start == datetime(2024, 3, 10, 12, 0, tzinfo=UTC)
    # A +02:00 offset is converted back to UTC.
    start2, _ = _normalize_comment_date_window("2024-03-10T12:00:00+02:00", None)
    assert start2 == datetime(2024, 3, 10, 10, 0, tzinfo=UTC)


def test_naive_input_assumed_utc() -> None:
    start, _ = _normalize_comment_date_window("2024-05-01T08:30:00", None)
    assert start == datetime(2024, 5, 1, 8, 30, tzinfo=UTC)


def test_start_inclusive_end_exclusive_predicate() -> None:
    start, end = _normalize_comment_date_window("2024-01-01", "2024-02-01")
    clause, params = _comment_date_window_predicate(start, end, alias="p")
    # Inclusive lower bound, exclusive upper bound.
    assert clause == " and p.posted_at >= %s and p.posted_at < %s"
    assert params == [start, end]


def test_predicate_unbounded_is_empty() -> None:
    assert _comment_date_window_predicate(None, None, alias="p") == ("", [])


def test_predicate_respects_alias_and_column() -> None:
    start, _ = _normalize_comment_date_window("2024-01-01", None)
    clause, params = _comment_date_window_predicate(start, None, alias="p", column="catalog_posted_at")
    assert clause == " and p.catalog_posted_at >= %s"
    assert params == [start]


@pytest.mark.parametrize("bad", ["not-a-date", "2024-13-01", "2024-02-30T99:99"])
def test_malformed_input_raises_value_error(bad: str) -> None:
    with pytest.raises(ValueError):
        _normalize_comment_date_window(bad, None)


def test_start_not_before_end_raises_value_error() -> None:
    with pytest.raises(ValueError):
        _normalize_comment_date_window("2024-02-01", "2024-01-01")
    with pytest.raises(ValueError):
        # Equal bounds are also rejected (empty window).
        _normalize_comment_date_window("2024-02-01", "2024-02-01")
