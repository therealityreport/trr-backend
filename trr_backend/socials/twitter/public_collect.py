"""Bounded anonymous FxTwitter collection. State belongs to the calling job owner.

No filesystem checkpoint, credential discovery, database, scheduler or fallback.
A caller must retain returned state under its existing exclusive job ownership.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from urllib.parse import urlencode

from .public_models import PublicPost, conversation_posts, normalize, timestamp
from .public_transport import fetch

CAPABILITIES = {"search", "profile", "list", "status", "conversation", "thread"}


def preview(
    query: str,
    *,
    capability: str = "search",
    sort: str = "latest",
    date_start=None,
    date_end=None,
    author: str | None = None,
    language: str | None = None,
    media_only: bool = False,
    min_likes: int | None = None,
) -> dict:
    if capability not in CAPABILITIES or sort not in {"latest", "top"}:
        raise ValueError("Unsupported public capability or sort")
    if not isinstance(query, str) or not 1 <= len(query.strip()) <= 300:
        raise ValueError("Public query must contain 1-300 characters")
    query = query.strip()
    filters = {
        "author": author.lstrip("@").lower() if author else None,
        "language": language,
        "media_only": media_only,
        "min_likes": min_likes,
        "hashtags": [],
    }
    dates = {"start": date_start, "end": date_end}
    warnings = []
    if capability == "search":
        path = "/2/search"
        complex_query = bool(re.search(r'\bOR\b|[()"]|(?:^|\s)-', query))
        if complex_query:
            warnings.append("Complex query semantics are provider-only; structured filters remain local")
        for word in [] if complex_query else query.split():
            if re.fullmatch(r"from:@?\w{1,15}", word, re.I):
                value = word.split(":", 1)[1].lstrip("@").lower()
                if filters["author"] and filters["author"] != value:
                    raise ValueError("Conflicting author constraints")
                filters["author"] = value
            elif word.startswith(("since:", "until:")):
                op, value = word.split(":", 1)
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    raise ValueError("Invalid query date")
                value = timestamp(value + "T00:00:00Z")
                key = "start" if op == "since" else "end"
                old = timestamp(dates[key].isoformat() if isinstance(dates[key], datetime) else dates[key])
                dates[key] = (max if key == "start" else min)(old, value) if old else value
            elif re.fullmatch(r"#\w+", word):
                filters["hashtags"].append(word[1:].casefold())
            elif ":" in word:
                warnings.append("Advanced operator semantics are provider-only")
    elif capability == "profile":
        query = query.rstrip("/").rsplit("/", 1)[-1].lstrip("@").lower()
        if not re.fullmatch(r"[A-Za-z0-9_]{1,15}", query):
            raise ValueError("Invalid public handle")
        path = f"/2/profile/{query}/statuses"
    else:
        query = query.rstrip("/").rsplit("/", 1)[-1]
        if not re.fullmatch(r"[0-9]{2,20}", query):
            raise ValueError("Invalid public post/list ID")
        path = "/2/search" if capability == "list" else f"/2/{capability}/{query}"
        if capability == "list":
            query = "list:" + query
    for key, value in dates.items():
        parsed = timestamp(value.isoformat() if isinstance(value, datetime) else value)
        filters[key] = parsed.isoformat() if parsed else None
    if filters["start"] and filters["end"] and filters["start"] >= filters["end"]:
        raise ValueError("End must be after start")
    if min_likes is not None and (type(min_likes) is not int or min_likes < 0):
        raise ValueError("Invalid minimum likes")
    return {
        "provider": "fxtwitter",
        "capability": capability,
        "query": query,
        "path": path,
        "sort": sort,
        "filters": filters,
        "warnings": warnings,
    }


def collect_public(
    query: str,
    *,
    capability: str = "search",
    sort: str = "latest",
    max_pages: int = 10,
    max_items: int = 100,
    deadline_seconds: float = 120,
    request_timeout: float = 20,
    mode: str = "fresh",
    state: dict | None = None,
    cancelled: Callable[[], bool] | None = None,
    on_progress: Callable[[dict], None] | None = None,
    on_checkpoint: Callable[[dict], None] | None = None,
    fetcher=None,
    **filters,
) -> dict:
    """Return posts, coverage and resumable state; never infer complete history."""
    plan = preview(query, capability=capability, sort=sort, **filters)
    for value, low, high in ((max_pages, 1, 100), (max_items, 0, 10000)):
        if type(value) is not int or not low <= value <= high:
            raise ValueError("Invalid public collection limit")
    if not 0 < deadline_seconds <= 600 or not 0 < request_timeout <= 20:
        raise ValueError("Invalid public deadline")
    if mode not in {"fresh", "resume", "refresh"}:
        raise ValueError("Invalid public collection mode")
    identity = hashlib.sha256(
        json.dumps({"plan": plan, "schema": 2, "max_items": max_items}, sort_keys=True).encode()
    ).hexdigest()
    saved = {
        "version": 2,
        "identity": identity,
        "posts": [],
        "cursor": None,
        "done": False,
        "seen": [],
        "signatures": [],
        "mode": "fresh",
        "known": [],
        "order": [],
        "known_pages": 0,
        "resolved_id": None,
        "issues": [],
        "stop_reason": "provider-exhausted",
    }
    if state is not None:
        if mode == "fresh":
            raise ValueError("State requires resume or refresh")
        if not isinstance(state, dict) or state.get("version") != 2 or state.get("identity") != identity:
            raise ValueError("Incompatible public checkpoint")
        saved = copy.deepcopy(state)
        try:
            if type(saved["done"]) is not bool or saved["mode"] not in {"fresh", "refresh"}:
                raise ValueError("Invalid checkpoint state")
            if saved["cursor"] is not None and not isinstance(saved["cursor"], str):
                raise ValueError("Invalid checkpoint cursor")
            for key in ("seen", "signatures", "known", "order", "issues"):
                if not isinstance(saved[key], list) or any(not isinstance(v, str) for v in saved[key]):
                    raise ValueError("Invalid checkpoint history")
            if type(saved["known_pages"]) is not int or saved["known_pages"] < 0:
                raise ValueError("Invalid checkpoint known pages")
            if saved["resolved_id"] is not None and not re.fullmatch(r"[0-9]{2,20}", saved["resolved_id"]):
                raise ValueError("Invalid checkpoint identity")
            if not isinstance(saved["posts"], list):
                raise ValueError("Invalid checkpoint posts")
            for row in saved["posts"]:
                PublicPost(**row)
                if not re.fullmatch(r"[0-9]{2,20}", row["tweet_id"]) or not isinstance(row["text"], str):
                    raise ValueError("Invalid checkpoint post")
            if saved.get("stop_reason") not in {
                "provider-exhausted",
                "requested-count-reached",
                "known-records-reached",
                "limit-reached",
                "blocked",
                "failed",
                "cancelled",
                "repeated-page",
                "repeated-cursor",
            }:
                raise ValueError("Invalid checkpoint terminal reason")
            ids = {p["tweet_id"] for p in saved["posts"]}
            if not set(saved["order"]) <= ids or len(ids) != len(saved["posts"]):
                raise ValueError("Invalid checkpoint order")
        except (KeyError, TypeError) as exc:
            raise ValueError("Malformed public checkpoint") from exc
    if mode == "refresh":
        saved.update(
            cursor=None,
            done=False,
            seen=[],
            signatures=[],
            mode="refresh",
            known=[p["tweet_id"] for p in saved["posts"]],
            order=[],
            known_pages=0,
            issues=[],
            stop_reason="provider-exhausted",
        )
    posts = {p["tweet_id"]: p for p in saved["posts"]}
    known = set(saved["known"])
    deadline = time.monotonic() + deadline_seconds
    get = fetcher or fetch
    pages, requests, rejected, added, updated, metrics_updated = [], 0, 0, 0, 0, 0
    issues = list(plan["warnings"]) + saved["issues"]
    stop = saved["stop_reason"] if saved["done"] else "provider-exhausted"

    def read(path, params):
        nonlocal requests
        if cancelled and cancelled():
            raise ValueError("Public collection cancelled")
        if time.monotonic() >= deadline:
            raise ValueError("Public provider deadline")
        url = "https://api.fxtwitter.com" + path + ("?" + urlencode(params) if params else "")
        requests += 1
        body = get(
            url,
            deadline=min(deadline, time.monotonic() + request_timeout),
            **({"cancelled": cancelled} if cancelled else {}),
        )
        data = json.loads(body)
        if not isinstance(data, dict) or data.get("code") != 200:
            raise ValueError("Malformed public provider response")
        return data, {
            "url": url,
            "sha256": hashlib.sha256(body).hexdigest(),
            "observed_at": datetime.now(UTC).isoformat(),
        }

    while not saved["done"]:
        if max_items == 0 or saved["mode"] != "refresh" and len(posts) >= max_items:
            stop = "requested-count-reached"
            break
        if len(pages) >= max_pages:
            stop = "limit-reached"
            break
        try:
            params = {"q": plan["query"], "feed": sort} if capability in {"search", "list"} else {}
            if saved["cursor"]:
                params["cursor"] = saved["cursor"]
            path = f"/2/profile/id:{saved['resolved_id']}/statuses" if saved["resolved_id"] else plan["path"]
            try:
                data, receipt = read(path, params)
            except ValueError as exc:
                if capability != "profile" or saved["resolved_id"] or "HTTP 404" not in str(exc):
                    raise
                resolution, _ = read("/2/search", {"q": "from:" + plan["query"], "feed": "latest"})
                candidates = {
                    str(p.get("author", {}).get("id"))
                    for p in resolution.get("results", [])
                    if isinstance(p, dict)
                    and isinstance(p.get("author"), dict)
                    and str(p["author"].get("screen_name", "")).lower() == plan["query"]
                    and re.fullmatch(r"[0-9]{2,20}", str(p["author"].get("id", "")))
                }
                if len(candidates) != 1:
                    raise ValueError("Ambiguous profile identity") from exc
                saved["resolved_id"] = next(iter(candidates))
                data, receipt = read(f"/2/profile/id:{saved['resolved_id']}/statuses", params)
            if capability in {"status", "conversation", "thread"}:
                normalized = [
                    p.to_dict()
                    for p in conversation_posts(data, root_id=plan["query"], capability=capability, evidence=receipt)
                ]
            else:
                rows = data.get("results")
                if not isinstance(rows, list):
                    raise ValueError("Malformed public results")
                normalized = [normalize(row, evidence=receipt).to_dict() for row in rows]
            cursor = data.get("cursor") or {}
            if not isinstance(cursor, dict):
                raise ValueError("Malformed public cursor")
            next_cursor = cursor.get("bottom")
            if next_cursor is not None and (not isinstance(next_cursor, str) or len(next_cursor) > 8192):
                raise ValueError("Malformed public cursor")
            signature = hashlib.sha256(",".join(sorted(p["tweet_id"] for p in normalized)).encode()).hexdigest()
            if normalized and signature in saved["signatures"]:
                issues.append("Repeated page")
                stop = "repeated-page"
                saved["done"] = True
                break
            if normalized:
                saved["signatures"].append(signature)
            page_new = any(p["tweet_id"] not in known for p in normalized)
            page_added = 0
            for post in normalized:
                f = plan["filters"]
                missing = []
                if (f["start"] or f["end"]) and post["date_time"] is None:
                    missing.append("date")
                if f["language"] and post["language"] is None:
                    missing.append("language")
                if f["min_likes"] is not None and post["likes"] is None:
                    missing.append("likes")
                if f["hashtags"] and not post["text"]:
                    missing.append("hashtag text")
                issues.extend("Missing field for local validation: " + field for field in missing)
                accepted = not missing and not (
                    f["author"]
                    and post["username"].lower() != f["author"]
                    or f["start"]
                    and post["date_time"] < f["start"]
                    or f["end"]
                    and post["date_time"] >= f["end"]
                    or f["language"]
                    and post["language"] != f["language"]
                    or f["media_only"]
                    and not post["media"]
                    or f["min_likes"] is not None
                    and post["likes"] < f["min_likes"]
                    or not set(f["hashtags"]) <= {v.casefold() for v in post["hashtags"]}
                )
                if not accepted:
                    rejected += 1
                    continue
                old = posts.get(post["tweet_id"])
                # Repeated observations may update metrics, but must not weaken
                # explicit root/author-thread context to an audience reply.
                rank = {None: 0, "reply": 1, "thread": 2, "root": 3}
                if old and rank.get(old.get("twitter_context_role"), 0) > rank.get(post.get("twitter_context_role"), 0):
                    for key in ("twitter_context_role", "thread_root_tweet_id", "is_thread_part"):
                        post[key] = old.get(key)
                if not old and len(posts) >= max_items and saved["mode"] != "refresh":
                    continue
                if old is None:
                    added += 1
                    page_added += 1
                elif {k: v for k, v in old.items() if k != "evidence"} != {
                    k: v for k, v in post.items() if k != "evidence"
                }:
                    updated += 1
                    if any(
                        post.get(k) is not None and old.get(k) != post.get(k)
                        for k in ("likes", "retweets", "replies", "quotes", "views", "bookmarks")
                    ):
                        metrics_updated += 1
                posts[post["tweet_id"]] = post
                if saved["mode"] == "refresh" and post["tweet_id"] not in saved["order"]:
                    saved["order"].append(post["tweet_id"])
            pages.append({**receipt, "received": len(normalized), "new_ids": page_added})
            saved["known_pages"] = saved["known_pages"] + 1 if normalized and not page_new else 0
            if not next_cursor or capability in {"status", "thread"}:
                saved["done"] = True
            elif next_cursor in saved["seen"]:
                issues.append("Repeated cursor")
                stop = "repeated-cursor"
                saved["done"] = True
            else:
                saved["seen"].append(next_cursor)
                saved["cursor"] = next_cursor
            if saved["mode"] == "refresh" and saved["known_pages"] >= 2:
                saved["done"] = True
                stop = "known-records-reached"
            if on_checkpoint:
                on_checkpoint(
                    copy.deepcopy(
                        {
                            **saved,
                            "posts": list(posts.values()),
                            "issues": list(dict.fromkeys(issues)),
                            "stop_reason": stop,
                        }
                    )
                )
            if on_progress:
                on_progress({"pages": len(pages), "requests": requests, "accepted": len(posts), "rejected": rejected})
        except (ValueError, TypeError, KeyError) as exc:
            message = str(exc)
            stop = (
                "cancelled"
                if "cancelled" in message
                else "limit-reached"
                if "deadline" in message
                else "blocked"
                if re.search(r"HTTP (401|403|429)", message)
                else "failed"
            )
            issues.append(message)
            break
    saved["posts"] = list(posts.values())
    saved["issues"] = list(dict.fromkeys(issues))
    saved["stop_reason"] = stop
    ordered = [posts[k] for k in saved["order"]] + [p for k, p in posts.items() if k not in saved["order"]]
    if sort == "latest":
        ordered.sort(key=lambda p: p["date_time"] or "", reverse=True)
    output = ordered[:max_items]
    if max_items == 0 or len(output) >= max_items and saved["mode"] != "refresh" and stop == "provider-exhausted":
        stop = "requested-count-reached"
    dates = sorted(p["date_time"] for p in output if p["date_time"])
    ids = {p["tweet_id"] for p in output}
    metadata = {
        "schema_version": 2,
        "provider": "fxtwitter",
        "retrieval_mode": "anonymous_research",
        "status": "partial" if issues or stop in {"blocked", "failed", "cancelled", "limit-reached"} else "complete",
        "coverage": "unknown",
        "complete": False,
        "upstream_authentication": "unknown",
        "authentication_provided": False,
        "paid_services": False,
        "stop_reason": stop,
        "pages": pages,
        "pages_scanned": len(pages),
        "requests": requests,
        "rejected": rejected,
        "added": added,
        "updated": updated,
        "metrics_updated": metrics_updated,
        "max_pages": max_pages,
        "max_items": max_items,
        "returned": len(output),
        "resolved_profile_id": saved["resolved_id"],
        "issues": list(dict.fromkeys(issues)),
        "observed_date_range": {"start": dates[0] if dates else None, "end": dates[-1] if dates else None},
        "missing_parents": sorted(
            {p["reply_to_tweet_id"] for p in output if p["reply_to_tweet_id"] and p["reply_to_tweet_id"] not in ids}
        ),
        "parentage": "provider-reported",
        "effective_plan": plan,
        "text_scope": "provider full text; completeness not independently checked for every post",
    }
    if on_checkpoint:
        on_checkpoint(copy.deepcopy(saved))
    return {"posts": output, "metadata": metadata, "state": saved}
