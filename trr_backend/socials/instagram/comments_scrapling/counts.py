"""Comment tree helpers for the Instagram comments Scrapling lane."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any


def _comment_id(comment: Any) -> str:
    return str(getattr(comment, "comment_id", "") or "").strip()


def _comment_replies(comment: Any) -> list[Any]:
    replies = getattr(comment, "replies", None)
    return replies if isinstance(replies, list) else []


def _unique_count(comments: Iterable[Any]) -> int:
    count = 0
    seen_ids: set[str] = set()
    for comment in comments:
        comment_id = _comment_id(comment)
        if not comment_id:
            continue
        if comment_id in seen_ids:
            continue
        seen_ids.add(comment_id)
        count += 1
    return count


def iter_flattened_comments(comments: Iterable[Any]) -> Iterator[Any]:
    """Yield top-level comments and all nested replies in tree order."""
    for comment in comments:
        yield comment
        yield from iter_flattened_comments(_comment_replies(comment))


def iter_reply_comments(comments: Iterable[Any]) -> Iterator[Any]:
    """Yield all replies nested under top-level comments."""
    for comment in comments:
        for reply in _comment_replies(comment):
            yield reply
            yield from iter_flattened_comments(_comment_replies(reply))


def top_level_count(comments: Iterable[Any]) -> int:
    return _unique_count(comments)


def reply_count_observed(comment: Any) -> int:
    return _unique_count(iter_flattened_comments(_comment_replies(comment)))


def reply_count_observed_for_tree(comments: Iterable[Any]) -> int:
    return _unique_count(iter_reply_comments(comments))


def flattened_comment_count(comments: Iterable[Any]) -> int:
    return _unique_count(iter_flattened_comments(comments))


def media_comment_count(comments: Iterable[Any]) -> int:
    return _unique_count(
        comment
        for comment in iter_flattened_comments(comments)
        if getattr(comment, "media_urls", None) or getattr(comment, "hosted_media_urls", None)
    )


def missing_reply_count_for_parent(comment: Any) -> int:
    try:
        expected = int(getattr(comment, "reply_count", 0) or 0)
    except (TypeError, ValueError):
        expected = 0
    if expected <= 0:
        return 0
    return max(0, expected - reply_count_observed(comment))


def missing_reply_count(comments: Iterable[Any]) -> int:
    return sum(missing_reply_count_for_parent(comment) for comment in comments)


def missing_parent_reply_count(comments: Iterable[Any]) -> int:
    return sum(1 for comment in comments if missing_reply_count_for_parent(comment) > 0)


def merge_comment_replies(
    existing_replies: Iterable[Any],
    fetched_replies: Iterable[Any],
    *,
    parent_comment_id: str | None = None,
) -> list[Any]:
    """Merge preview replies with fetched pages while preserving first-seen rows."""
    merged: list[Any] = []
    seen_ids: set[str] = set()
    for reply in [*list(existing_replies or []), *list(fetched_replies or [])]:
        comment_id = _comment_id(reply)
        if comment_id:
            if comment_id in seen_ids:
                continue
            seen_ids.add(comment_id)
        if parent_comment_id and not getattr(reply, "parent_comment_id", None):
            try:
                reply.parent_comment_id = parent_comment_id
            except Exception:  # noqa: BLE001
                pass
        try:
            reply.is_reply = True
        except Exception:  # noqa: BLE001
            pass
        merged.append(reply)
    return merged
