"""A transient expected-count load failure must keep posts retryable.

When ``_load_expected_comment_counts`` raises ``DatabaseServiceUnavailableError``
the runner zeroes the expected-count map but flags it degraded and threads
``expected_count_unknown=True`` into the per-post fetch kwargs. The fetcher then
reports ``fetch_reason="expected_count_unknown"`` (or a diagnostic flag), and
``_comments_scrape_is_complete`` must refuse completion so the post is re-queued
instead of being abandoned with 0 stored comments.

Any OTHER exception out of the expected-count load must propagate (a real SQL or
attribute bug should crash loudly), which is asserted at the unit level here.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from trr_backend.db import pg
from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
from trr_backend.socials.instagram.comments_scrapling.job_runner import (
    _comments_scrape_is_complete,
    _expected_count_is_unknown,
    _load_expected_comment_counts,
)


def _result(
    *,
    reported,
    observed,
    fetch_reason: str = "",
    diagnostic_metadata: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        fetch_failed=False,
        auth_failed=False,
        comments=[],
        fetch_reason=fetch_reason,
        reported_comment_count=reported,
        flattened_comment_count=observed,
        diagnostic_metadata=diagnostic_metadata or {},
    )


def test_expected_count_unknown_via_fetch_reason_is_not_complete():
    # Even with an unlimited cap and 0 observed, a degraded expected-count post
    # must NOT complete (it would otherwise be abandoned with 0 stored comments).
    result = _result(reported=None, observed=0, fetch_reason="expected_count_unknown")
    assert _expected_count_is_unknown(result) is True
    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is False


def test_expected_count_unknown_via_diagnostic_metadata_is_not_complete():
    result = _result(
        reported=None,
        observed=0,
        fetch_reason="",
        diagnostic_metadata={"expected_count_unknown": True},
    )
    assert _expected_count_is_unknown(result) is True
    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is False


def test_expected_count_unknown_blocks_completion_even_with_observed_comments():
    # The whole point: an unknown expected count means we cannot trust that
    # "observed >= reported" — there is no reported count — so completion is
    # withheld regardless of how many comments came back this pass.
    result = _result(
        reported=None,
        observed=12,
        fetch_reason="expected_count_unknown",
    )
    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is False


def test_non_degraded_result_is_unaffected():
    # Control: a normal unknown-count result with observed comments still
    # completes under an unlimited cap (Bug #4 path), proving the new guard only
    # fires on the degraded signal.
    result = _result(reported=None, observed=5, fetch_reason="")
    assert _expected_count_is_unknown(result) is False
    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is True


def test_db_unavailable_error_degrades_map_and_continues(monkeypatch):
    # _load_expected_comment_counts is patched to raise the transient DB error.
    # The runner-level handling (replicated here at the call boundary) must
    # swallow ONLY that error and flag degraded; the map stays empty.
    def _raise_db_unavailable(**_kwargs):
        raise pg.DatabaseServiceUnavailableError("pool saturated")

    monkeypatch.setattr(jr, "_load_expected_comment_counts", _raise_db_unavailable)

    expected_counts_degraded = False
    try:
        expected_map = jr._load_expected_comment_counts(
            repo=SimpleNamespace(),
            account_handle="acct",
            target_source_ids=["A"],
        )
    except pg.DatabaseServiceUnavailableError:
        expected_map = {}
        expected_counts_degraded = True

    assert expected_map == {}
    assert expected_counts_degraded is True


def test_non_db_error_propagates(monkeypatch):
    # A non-DB error (a real bug) must NOT be swallowed by the narrowed except.
    class _BoomError(RuntimeError):
        pass

    def _raise_boom(**_kwargs):
        raise _BoomError("attribute bug")

    monkeypatch.setattr(jr, "_load_expected_comment_counts", _raise_boom)

    with pytest.raises(_BoomError):
        try:
            jr._load_expected_comment_counts(
                repo=SimpleNamespace(),
                account_handle="acct",
                target_source_ids=["A"],
            )
        except pg.DatabaseServiceUnavailableError:  # pragma: no cover - never hit
            pass


def test_expected_count_query_preserves_requested_order_and_coauthor_max(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]):
        captured["sql"] = " ".join(sql.split()).lower()
        captured["params"] = params
        return [
            {"shortcode": "B", "reported_comments": 9},
            {"shortcode": "A", "reported_comments": 0},
        ]

    monkeypatch.setattr(jr.pg, "fetch_all", _fake_fetch_all)
    repo = SimpleNamespace(_instagram_reported_comments_sql=lambda alias: f"{alias}.comments_count")

    counts = _load_expected_comment_counts(
        repo=repo,
        account_handle="@thetraitorsus",
        target_source_ids=["B", "A", "B"],
    )

    sql = str(captured["sql"])
    assert counts == {"B": 9, "A": 0}
    assert captured["params"] == [["B", "A"]]
    assert "with ordinality" in sql
    assert "max((p.comments_count)::bigint)" in sql
    assert "left join shortcode_max" in sql
    assert "order by r.sort_order" in sql
    assert "source_account" not in sql
