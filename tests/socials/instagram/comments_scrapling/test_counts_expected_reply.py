"""expected_reply_count must mirror the persistence topology's greatest(...).

Bug #1: reply-fetch gates that key off reply_count alone disagree with the
topology query (which uses greatest(reply_count, child_comment_count)), so a
parent with child_comment_count > reply_count is reported incomplete forever
while no reply fetch is ever triggered. The helper unifies the two.
"""

from __future__ import annotations

from dataclasses import dataclass

from trr_backend.socials.instagram.comments_scrapling.counts import (
    expected_reply_count,
    missing_reply_count_for_parent,
)


@dataclass
class _Comment:
    reply_count: int = 0
    child_comment_count: int = 0
    replies: list = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.replies is None:
            self.replies = []


def test_uses_greater_of_reply_count_and_child_comment_count():
    assert expected_reply_count(_Comment(reply_count=2, child_comment_count=5)) == 5
    assert expected_reply_count(_Comment(reply_count=7, child_comment_count=3)) == 7
    assert expected_reply_count(_Comment(reply_count=0, child_comment_count=0)) == 0


def test_handles_missing_and_non_numeric_fields():
    class _Bare:
        pass

    assert expected_reply_count(_Bare()) == 0
    assert expected_reply_count(_Comment(reply_count=None, child_comment_count=4)) == 4  # type: ignore[arg-type]


def test_missing_reply_count_uses_unified_expectation():
    # child_comment_count=5 but reply_count=0 and zero observed replies -> gap of 5,
    # which the old reply_count-only logic would have reported as 0.
    parent = _Comment(reply_count=0, child_comment_count=5, replies=[])
    assert missing_reply_count_for_parent(parent) == 5


def test_missing_reply_count_zero_when_fully_observed():
    @dataclass
    class _Reply:
        comment_id: str
        is_reply: bool = True
        parent_comment_id: str = "p1"
        replies: list = None  # type: ignore[assignment]

        def __post_init__(self):
            if self.replies is None:
                self.replies = []

    parent = _Comment(reply_count=2, child_comment_count=2, replies=[_Reply("r1"), _Reply("r2")])
    assert missing_reply_count_for_parent(parent) == 0
