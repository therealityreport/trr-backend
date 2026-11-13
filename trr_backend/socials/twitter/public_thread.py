"""Explicit bounded branch expansion; never a complete-thread claim."""

from __future__ import annotations

import time

from trr_backend.socials.twitter.public_mirror import fetch_public_conversation


def expand_public_thread(tweet_id: str, *, provider: str, max_requests: int = 5, max_posts: int = 100) -> dict:
    """Read two root pages, then selected branches within one shared budget.

    Reply counts only prioritize known branches. They never prove that an
    unvisited branch is empty or that all replies were discovered.
    """
    if provider != "fxtwitter":
        raise ValueError("Explicit provider='fxtwitter' is required")
    if type(max_requests) is not int or not 1 <= max_requests <= 5:
        raise ValueError("max_requests must be 1-5")
    if type(max_posts) is not int or not 1 <= max_posts <= 100:
        raise ValueError("max_posts must be 1-100")
    deadline = time.monotonic() + 120
    posts, pages, branches = {}, [], []
    pending, visited = [tweet_id], set()
    requests_used = 0
    stop_reason = "known_branches_exhausted"
    while pending:
        if requests_used >= max_requests or len(posts) >= max_posts:
            stop_reason = "budget_exhausted"
            break
        if time.monotonic() >= deadline:
            stop_reason = "deadline"
            break
        branch_id = pending.pop(0)
        if branch_id in visited:
            continue
        visited.add(branch_id)
        budget = min(2 if branch_id == tweet_id else 1, max_requests - requests_used)
        result = fetch_public_conversation(branch_id, max_pages=budget, max_items=max_posts, deadline=deadline)
        # A failed page consumed a request even though no successful page receipt exists.
        used = len(result.pages) + int(result.error is not None)
        requests_used += used
        pages.extend(result.pages)
        added = 0
        for post in result.tweets:
            if post.tweet_id not in posts and len(posts) < max_posts:
                posts[post.tweet_id] = post
                added += 1
            if post.tweet_id in posts and post.replies > 0 and post.tweet_id not in visited:
                if post.tweet_id not in pending:
                    pending.append(post.tweet_id)
        branches.append(
            {
                "id": branch_id,
                "requests": used,
                "added": added,
                "stop_reason": result.stop_reason,
                "error": result.error,
                "continuation_unvisited": bool(result.next_cursor),
            }
        )
        if result.error:
            stop_reason = "deadline" if result.stop_reason == "deadline" else "provider_error"
            break
    return {
        "status": "partial" if posts else "unavailable",
        "provider": provider,
        "root_id": tweet_id,
        "posts": list(posts.values()),
        "pages": pages,
        "branches": branches,
        "requests_used": requests_used,
        "unvisited_branches": pending,
        "missing_parent_ids": sorted(
            {p.reply_to_tweet_id for p in posts.values() if p.reply_to_tweet_id and p.reply_to_tweet_id not in posts}
        ),
        "stop_reason": stop_reason,
        "complete": False,
        "thread_completeness": "unknown",
        "upstream_authentication": "unknown",
        "authentication_provided": False,
        "optional_fields": "provider-reported; absent metrics default to zero",
        "parentage": "provider-reported; only selected relationships independently verified",
        "coverage": "Known branches only; omitted and undiscovered branches may exist",
    }
