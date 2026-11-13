"""Bounded, state-free deployment check for the anonymous conversation path."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import ModuleType


def _source_sha256(module: ModuleType) -> str:
    source_path = module.__file__
    if source_path is None:
        raise ValueError(f"Module {module.__name__} has no source file")
    return hashlib.sha256(Path(source_path).read_bytes()).hexdigest()


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
        "source_sha256": {module.__name__: _source_sha256(module) for module in (direct_scrape, public_mirror)},
    }


def probe_public_search(query: str) -> dict:
    from trr_backend.socials.twitter import public_search

    result = public_search.search_public_posts(query, provider="lurkbird", max_pages=1, max_items=10)
    return {
        **result,
        "state_free": True,
        "source_sha256": _source_sha256(public_search),
    }


def probe_public_research_search(query: str) -> dict:
    """Exercise the UI-facing dispatcher with forbidden auth and persistence."""
    from types import SimpleNamespace

    from trr_backend.socials.twitter import anonymous_search, direct_scrape, public_search

    calls = {"auth": 0, "persist": 0}

    def auth():
        calls["auth"] += 1
        raise RuntimeError("Anonymous probe attempted credential access")

    def persist(*args, **kwargs):
        calls["persist"] += 1
        raise RuntimeError("Anonymous probe attempted persistence")

    result = direct_scrape.search_twitter(
        SimpleNamespace(
            mode="anonymous_research",
            provider="lurkbird",
            query=query,
            sort="latest",
            max_pages=1,
            max_items=10,
            persist=False,
            mirror_to_s3=False,
        ),
        load_auth=auth,
        persist_search=persist,
    )
    return {
        **result,
        "state_free": True,
        "forbidden_calls": calls,
        "source_sha256": {
            module.__name__: _source_sha256(module) for module in (anonymous_search, direct_scrape, public_search)
        },
    }
