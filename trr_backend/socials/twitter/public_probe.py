"""Bounded, state-free deployment check for the anonymous conversation path."""

from __future__ import annotations

import hashlib
from pathlib import Path


def probe_public_conversation(tweet_id: str, *, max_pages: int = 2, max_items: int = 40) -> dict:
    from trr_backend.socials.twitter import direct_scrape, public_mirror

    if type(max_pages) is not int or not 1 <= max_pages <= 2:
        raise ValueError("Probe max_pages must be 1-2")
    if type(max_items) is not int or not 1 <= max_items <= 40:
        raise ValueError("Probe max_items must be 1-40")
    auth_calls = 0

    def forbidden_auth():
        nonlocal auth_calls
        auth_calls += 1
        raise RuntimeError("Anonymous probe must not load authentication")

    result = direct_scrape.fetch_tweet_replies(
        {"tweet_id": tweet_id, "mirror_to_s3": False},
        load_auth=forbidden_auth,
        conversation_provider="fxtwitter",
        mirror_max_pages=max_pages,
        mirror_max_items=max_items,
    )
    return {
        **result,
        "state_free": True,
        "authentication_loader_calls": auth_calls,
        "source_sha256": {
            module.__name__: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
            for module in (direct_scrape, public_mirror)
        },
    }
