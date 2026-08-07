"""Bug #4: an unknown-count fetch that recovered nothing must not be 'complete'.

Without the guard, a post that never advertised a comment count and returned
zero comments (empty/transiently-blocked first page) was declared complete under
an unlimited cap and abandoned with 0 stored comments. Legitimately-zero posts
advertise reported_comment_count=0 and stay complete.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from trr_backend.socials.instagram.comments_scrapling.job_runner import (
    _comments_scrape_is_complete,
)


def _result(*, reported, observed, fetch_reason: str = "") -> Any:
    return SimpleNamespace(
        fetch_failed=False,
        auth_failed=False,
        comments=[],
        fetch_reason=fetch_reason,
        reported_comment_count=reported,
        flattened_comment_count=observed,
    )


def test_unknown_count_zero_observed_is_not_complete():
    # reported unknown (None), nothing recovered, unlimited cap -> retryable.
    result = _result(reported=None, observed=0)
    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is False


def test_unknown_count_with_observed_comments_is_complete():
    result = _result(reported=None, observed=5)
    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is True


def test_legitimately_zero_post_stays_complete():
    # A post that advertises 0 comments and recovered 0 is genuinely complete.
    result = _result(reported=0, observed=0)
    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is True
