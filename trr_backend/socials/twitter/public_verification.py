"""Independent public X parent IDs; missing branches are never invented."""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import UTC, datetime
from urllib.parse import urlencode

from .public_transport import fetch


def verify_parent(post: dict, *, deadline: float, fetcher=None, cancelled=None) -> dict:
    result = {
        "child_id": post["tweet_id"],
        "provider_parent_id": post.get("reply_to_tweet_id"),
        "first_party_parent_id": None,
        "status": "unavailable",
        "reason": None,
        "evidence": None,
    }
    try:
        url = "https://cdn.syndication.twimg.com/tweet-result?" + urlencode(
            {"id": post["tweet_id"], "lang": "en", "token": "1"}
        )
        body = (fetcher or fetch)(
            url, deadline=min(deadline, time.monotonic() + 20), **({"cancelled": cancelled} if cancelled else {})
        )
        data = json.loads(body)
        parent = data.get("in_reply_to_status_id_str")
        if data.get("id_str") != post["tweet_id"]:
            raise ValueError("First-party child ID mismatch")
        if not isinstance(parent, str) or not re.fullmatch(r"[0-9]{2,20}", parent):
            raise ValueError("First-party exact parent ID unavailable")
        embedded = (data.get("parent") or {}).get("id_str")
        if embedded and embedded != parent:
            raise ValueError("First-party parent fields conflict")
        result.update(
            first_party_parent_id=parent,
            status="verified" if parent == result["provider_parent_id"] else "conflict",
            evidence={
                "url": url,
                "sha256": hashlib.sha256(body).hexdigest(),
                "observed_at": datetime.now(UTC).isoformat(),
            },
        )
    except (ValueError, TypeError, AttributeError) as exc:
        result["reason"] = str(exc)
    return result
