"""Backend port of the episode-discussion discovery service's matching rules.

The caller owns jobs, claim checks and saved-thread writes. Input keys match the
existing app service's camelCase contract. All checkpoints are JSON-compatible;
provide ``on_checkpoint`` to persist collection pages before continuing.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from trr_backend.repositories.reddit_transport import RedditHttpClient, RedditTransportError, build_client

DISCUSSION_ALIASES = {
    "live": ["live episode discussion", "live thread"],
    "post": ["post episode discussion", "post-episode discussion"],
    "weekly": ["weekly episode discussion", "weekly thread"],
}
TYPES = tuple(DISCUSSION_ALIASES)
SORTS = ("new", "hot", "top")


def _text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _strings(values: Any) -> list[str]:
    by_key: dict[str, str] = {}
    for value in values if isinstance(values, list) else []:
        if isinstance(value, str) and (cleaned := re.sub(r"\s+", " ", value).strip()):
            by_key.setdefault(cleaned.lower(), cleaned)
    return sorted(by_key.values(), key=str.lower)


def _canonical_flair(value: Any) -> str:
    text = re.sub(r":[^:\s]+:", " ", _text(value))
    text = re.sub(r"^[\W_]+|[\W_]+$", "", text, flags=re.UNICODE)
    return _text(text)


def _subreddit(value: Any) -> str:
    value = re.sub(r"^https?://(?:www\.)?reddit\.com/r/", "", str(value or "").strip(), flags=re.I)
    value = re.sub(r"^r/", "", value, flags=re.I).strip("/")
    return re.split(r"[/?#]", value)[0].lower()


def _flair(subreddit: str, value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = re.sub(r"\s+", " ", value).strip()
    if subreddit == "realhousewivesofslc":
        if ":whitney:" in text.lower():
            return None
        text = re.sub(r":[^:\s]+:", " ", text)
        text = re.sub(r"^[\W_]+|[\W_]+$", "", text, flags=re.UNICODE)
        text = re.sub(r"\s+", " ", text).strip()
        if re.search(r"[a-z]", text, re.I) and text == text.upper():

            def headline(match: re.Match[str]) -> str:
                word = match.group()
                return (
                    word.upper()
                    if word.upper() in {"RHOSLC", "HW"} or re.fullmatch(r"s\d+", word, re.I)
                    else word.title()
                )

            text = re.sub(r"[A-Za-z0-9]+", headline, text)
    return text or None


def resolve_episode_rules(payload: dict[str, Any]) -> dict[str, Any]:
    """Port resolveEpisodeDiscussionRules; callers may pass these rules into discovery."""
    subreddit = _subreddit(payload.get("subreddit"))
    patterns = _strings(payload.get("episodeTitlePatterns"))
    flairs = _strings(payload.get("analysisAllFlairs"))
    if subreddit == "bravorealhousewives":
        patterns = _strings(
            patterns + ["Live Episode Discussion", "Post Episode Discussion", "Weekly Episode Discussion"]
        )
    rhoslc = "salt lake city" in _text(payload.get("showName")) or any(
        _text(alias) == "rhoslc" for alias in payload.get("showAliases") or []
    )
    auto_seed = not payload.get("isShowFocused", False) and subreddit == "bravorealhousewives" and not flairs and rhoslc
    return {
        "effectiveEpisodeTitlePatterns": patterns,
        "effectiveRequiredFlairs": ["Salt Lake City"] if auto_seed else flairs,
        "autoSeededRequiredFlairs": auto_seed,
    }


def _show_terms(name: Any, aliases: Any) -> list[str]:
    terms: list[str] = []
    for original in [name, *(aliases or [])]:
        if not isinstance(original, str) or not original.strip():
            continue
        normalized = _text(original)
        terms.append(normalized)
        match = re.search(r"real housewives of (.+)$", re.sub(r"^the\s+", "", original.strip(), flags=re.I), re.I)
        if match:
            words = re.sub(r"[^A-Za-z0-9\s]", " ", match[1]).split()
            initials = "".join(word[0].lower() for word in words if word.lower() not in {"of", "the", "and"})
            if initials:
                terms.append("rho" + initials)
        if match := re.match(r"^real housewives of (.+)$", normalized):
            terms.append(match[1])
    return list(dict.fromkeys(terms))[:18]


def _discussion_type(title: str) -> str | None:
    normalized = _text(title)
    hyphenated = re.sub(r"\s*-\s*", "-", normalized)
    for discussion_type, aliases in DISCUSSION_ALIASES.items():
        if any(alias in normalized or alias.replace(" ", "-") in hyphenated for alias in aliases):
            return discussion_type
    return None


def _episode_number(title: str) -> int | None:
    title = _text(title)
    candidates: set[int] = set()
    patterns = [r"\bs\s*0*\d+\s*e\s*0*(\d{1,3})\b", r"\bepisode\s*0*(\d{1,3})\b"]
    if re.search(r"\bseason\s*\d+\b|\bs\s*0*\d+\s*e\s*0*\d+\b|\bepisode discussion\b|\b(thread|discussion)\b", title):
        patterns.append(r"(?:^|[\s\-–—:|,(])e(?:p(?:isode)?)?\s*0*(\d{1,3})(?=$|[\s\-–—:|),])")
    for pattern in patterns:
        if (match := re.search(pattern, title, re.I)) and int(match[1]) > 0:
            candidates.add(int(match[1]))
    return next(iter(candidates)) if len(candidates) == 1 else None


def _date(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    except ValueError:
        return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z") if value else None


def _finite(value: Any, default: float = 0) -> float:
    return (
        value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else default
    )


def _absolute(value: str) -> str:
    return value if re.match(r"^https?://", value, re.I) else "https://www.reddit.com/" + value.lstrip("/")


def _parse_page(payload: Any, sort: str | None) -> tuple[list[dict[str, Any]], str | None]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict) or not isinstance(data.get("children"), list):
        raise RedditTransportError("Malformed Reddit episode discovery listing")
    after = data.get("after")
    if after is not None and (not isinstance(after, str) or not after):
        raise RedditTransportError("Malformed Reddit episode discovery cursor")
    rows = []
    for child in data["children"]:
        row = child.get("data") if isinstance(child, dict) else None
        if not isinstance(row, dict):
            raise RedditTransportError("Malformed Reddit episode discovery child")
        if not row.get("id") or not row.get("title") or not row.get("url"):
            continue
        if any(not isinstance(row[field], str) for field in ("id", "title", "url")):
            raise RedditTransportError("Malformed Reddit episode discovery post fields")
        timestamp = None
        if isinstance(row.get("created_utc"), (int, float)):
            try:
                timestamp = _iso(datetime.fromtimestamp(row["created_utc"], UTC))
            except (ValueError, OverflowError, OSError):
                pass
        rows.append(
            {
                "reddit_post_id": row["id"],
                "title": row["title"],
                "text": row.get("selftext")
                if isinstance(row.get("selftext"), str) and row["selftext"].strip()
                else None,
                "url": _absolute(row["url"]),
                "permalink": _absolute(row["permalink"]) if row.get("permalink") else None,
                "author": row.get("author"),
                "score": _finite(row.get("score")),
                "num_comments": _finite(row.get("num_comments")),
                "posted_at": timestamp,
                "link_flair_text": row.get("link_flair_text")
                if isinstance(row.get("link_flair_text"), str) and row["link_flair_text"].strip()
                else None,
                "source_sorts": [sort] if sort else [],
            }
        )
    return rows, after


def _merge(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row["reddit_post_id"]
        if key not in merged:
            merged[key] = copy.deepcopy(row)
            continue
        previous = merged[key]
        sorts = list(dict.fromkeys(previous["source_sorts"] + row["source_sorts"]))
        merged[key] = {**(row if row["score"] > previous["score"] else previous), "source_sorts": sorts}
    return list(merged.values())


def _collect(rows: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = []
    for row in rows:
        title = _text(row["title"])
        text = _text(row["title"] + " " + (row["text"] or ""))
        discussion_type = _discussion_type(title)
        if discussion_type is None:
            continue
        matched_patterns = [
            pattern
            for pattern in config["patterns"]
            if _text(pattern) in title or _discussion_type(pattern) == discussion_type
        ]
        if not config["patterns"] and config["focused"]:
            matched_patterns = [DISCUSSION_ALIASES[discussion_type][0]]
        matched_terms = [
            term
            for term in config["terms"]
            if re.search(r"(^|[^a-z0-9])" + re.escape(term) + r"([^a-z0-9]|$)", text, re.I)
        ]
        season = config["season"]
        has_season = re.search(rf"\bseason\s*{season}\b|\bs\s*0*{season}\b|\bs\s*0*{season}\s*e\s*0*\d+\b", text, re.I)
        episode = _episode_number(title)
        if not matched_patterns or not matched_terms or not has_season or not episode:
            continue
        if config["expected"] and episode not in config["expected"]:
            continue
        posted = _date(row["posted_at"])
        if config["start"] or config["end"]:
            if (
                posted is None
                or (config["start"] and posted < config["start"])
                or (config["end"] and posted > config["end"])
            ):
                continue
        normalized_flair = _flair(config["subreddit"], row["link_flair_text"])
        key = _canonical_flair(normalized_flair)
        if not config["focused"] and config["flair_keys"]:
            if key not in config["flair_keys"]:
                continue
            assignment = config["assignments"].get(key) or {}
            if assignment.get("season_ids") and config["season_id"] not in assignment["season_ids"]:
                continue
            if assignment.get("episode_ids") and config["episode_ids"].get(episode) not in assignment["episode_ids"]:
                continue
        reasons = [
            f"title pattern: {matched_patterns[0]}",
            f"show term: {matched_terms[0]}",
            f"season: {season}",
            f"episode: {episode}",
            f"type: {discussion_type}",
        ]
        if normalized_flair:
            reasons.append(f"flair: {normalized_flair}")
        candidates.append(
            {
                **row,
                "link_flair_text": normalized_flair or row["link_flair_text"],
                "episode_number": episode,
                "discussion_type": discussion_type,
                "match_reasons": reasons,
            }
        )
    return sorted(
        candidates,
        key=lambda row: (
            row["episode_number"],
            TYPES.index(row["discussion_type"]),
            -row["num_comments"],
            -row["score"],
        ),
    )


def _matrix(candidates: list[dict[str, Any]], expected: list[int]) -> list[dict[str, Any]]:
    rows: dict[int, dict[str, Any]] = {}
    tops: dict[tuple[int, str], tuple[float, float]] = {}
    for episode in sorted(set(expected + [row["episode_number"] for row in candidates])):
        rows[episode] = {
            "episode_number": episode,
            "total_posts": 0,
            "total_comments": 0,
            "total_upvotes": 0,
            **{
                kind: {
                    "post_count": 0,
                    "total_comments": 0,
                    "total_upvotes": 0,
                    "top_post_id": None,
                    "top_post_url": None,
                }
                for kind in TYPES
            },
        }
    for candidate in candidates:
        row = rows[candidate["episode_number"]]
        kind = candidate["discussion_type"]
        cell = row[kind]
        cell["post_count"] += 1
        cell["total_comments"] += candidate["num_comments"]
        cell["total_upvotes"] += candidate["score"]
        key = (candidate["episode_number"], kind)
        rank = (candidate["num_comments"], candidate["score"])
        if rank > tops.get(key, (-1, -1)):
            cell["top_post_id"], cell["top_post_url"] = candidate["reddit_post_id"], candidate["url"]
            tops[key] = rank
        row["total_posts"] += 1
        row["total_comments"] += candidate["num_comments"]
        row["total_upvotes"] += candidate["score"]
    return list(rows.values())


def _query(tokens: list[str], season: int, kind: str, episode: int | None = None) -> str:
    def quoted(value: str) -> str:
        return '"' + value.replace('"', '\\"') + '"'

    clauses = ["(" + " OR ".join(map(quoted, tokens)) + ")"] if tokens else []
    clauses.append(
        quoted(f"season {season}") if episode else f"({quoted(f'season {season}')} OR {quoted(f's{season}')})"
    )
    if episode:
        clauses.append(
            "(" + " OR ".join(map(quoted, [f"episode {episode}", f"e{episode}", f"s{season}e{episode}"])) + ")"
        )
    clauses.append(quoted(f"{kind} episode discussion"))
    return " ".join(clauses)


def discover_episode(
    payload: dict[str, Any],
    client: RedditHttpClient | None = None,
    checkpoint: dict[str, Any] | None = None,
    on_checkpoint: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Collect listings and bounded broad/gap search; return the existing service result shape.

    Completed pages survive retries. Failed queries are retried when resumed;
    malformed responses and non-rate-limit search failures raise with checkpoint.
    ``collection_complete`` and ``unresolved_queries`` are additive coverage evidence.
    """
    if not isinstance(payload, dict):
        raise RedditTransportError("Episode discovery payload must be an object", status=400)
    subreddit = _subreddit(payload.get("subreddit"))
    if not re.fullmatch(r"[a-z0-9_]+", subreddit):
        raise RedditTransportError("subreddit is required", status=400)
    season = max(1, math.floor(_finite(payload.get("seasonNumber"), 1)))
    limit = min(80, max(1, math.floor(_finite(payload.get("limitPerMode"), 65))))
    sorts = payload.get("sortModes") or list(SORTS)
    if not isinstance(sorts, list) or any(sort not in SORTS for sort in sorts):
        raise RedditTransportError("sortModes must contain new, hot or top", status=400)
    sorts = list(dict.fromkeys(sorts))
    terms = _show_terms(payload.get("showName"), payload.get("showAliases"))
    patterns = _strings(payload.get("episodeTitlePatterns"))
    focused = bool(payload.get("isShowFocused", False))
    start, end = _date(payload.get("periodStart")), _date(payload.get("periodEnd"))
    for field, parsed in (("periodStart", start), ("periodEnd", end)):
        if payload.get(field) and parsed is None:
            label = "period_start" if field == "periodStart" else "period_end"
            raise RedditTransportError(f"{label} must be a valid ISO datetime", status=400)
    if start and end and start > end:
        raise RedditTransportError("period_start must be before period_end", status=400)
    if not terms:
        raise RedditTransportError("Show terms are required for discovery", status=400)
    if not focused and not patterns:
        raise RedditTransportError("At least one episode title pattern is required", status=400)
    episodes = payload.get("seasonEpisodes") or []
    expected = sorted(
        {
            math.floor(row["episode_number"])
            for row in episodes
            if isinstance(row, dict) and _finite(row.get("episode_number")) >= 1
        }
    )
    episode_ids: dict[int, str] = {}
    for row in episodes:
        if (
            isinstance(row, dict)
            and _finite(row.get("episode_number")) >= 1
            and isinstance(row.get("id"), str)
            and row["id"].strip()
        ):
            episode_ids.setdefault(math.floor(row["episode_number"]), row["id"].strip())
    flairs_by_key: dict[str, str] = {}
    for raw in payload.get("episodeRequiredFlairs") or []:
        if normalized := _flair(subreddit, raw):
            flairs_by_key.setdefault(_canonical_flair(normalized), normalized)
    flairs = sorted(flairs_by_key.values(), key=str.lower)
    config = {
        "subreddit": subreddit,
        "season": season,
        "season_id": payload.get("resolvedSeasonId"),
        "terms": terms,
        "patterns": patterns,
        "focused": focused,
        "start": start,
        "end": end,
        # Preserve the existing service's lowercased required-label comparison.
        "flair_keys": {value.lower() for value in flairs},
        "assignments": payload.get("episodeFlairAssignments") or {},
        "episode_ids": episode_ids,
        "expected": set(expected),
    }
    fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if checkpoint is None:
        state: dict[str, Any] = {"version": 1, "config_hash": fingerprint, "queries": {}, "gap_queries": None}
    elif (
        not isinstance(checkpoint, dict)
        or checkpoint.get("version") != 1
        or checkpoint.get("config_hash") != fingerprint
        or not isinstance(checkpoint.get("queries"), dict)
    ):
        raise RedditTransportError("Episode discovery checkpoint does not match inputs", status=400)
    else:
        state = copy.deepcopy(checkpoint)
    client = client if client is not None else build_client()

    def save() -> None:
        if on_checkpoint is not None:
            on_checkpoint(copy.deepcopy(state))

    def fetch(key: str, path: str, params: dict[str, Any], pages: int, sort: str | None = None) -> dict[str, Any]:
        progress = state["queries"].setdefault(key, {"rows": [], "pages": 0, "after": None, "done": False})
        progress.pop("error", None)
        while not progress["done"] and progress["pages"] < pages:
            request_params = dict(params)
            if progress["after"]:
                request_params["after"] = progress["after"]
            try:
                page = client.get_json(path, params=request_params)
                rows, after = _parse_page(page, sort)
                if after and after == progress["after"]:
                    raise RedditTransportError("Reddit episode discovery cursor did not advance")
            except RedditTransportError as exc:
                progress["error"] = {
                    "status": exc.status,
                    "message": str(exc),
                    "retry_after_seconds": exc.retry_after_seconds,
                }
                save()
                exc.checkpoint = copy.deepcopy(state)
                raise
            progress["rows"].extend(rows)
            progress["pages"] += 1
            progress["after"] = after
            progress["done"] = after is None or progress["pages"] >= pages
            progress["capped"] = bool(after) and progress["pages"] >= pages and sort is None
            save()
        return progress

    listing_rows: list[dict[str, Any]] = []
    successful, failed, rate_limited = [], [], []
    first_error = None
    for sort in sorts:
        params: dict[str, Any] = {"limit": limit, "raw_json": 1}
        if sort == "top":
            params["t"] = "all"
        try:
            progress = fetch("listing:" + sort, f"/r/{quote(subreddit)}/{sort}.json", params, 1, sort)
            listing_rows.extend(progress["rows"])
            successful.append(sort)
        except RedditTransportError as exc:
            first_error = first_error or exc
            failed.append(sort)
            if exc.status == 429:
                rate_limited.append(sort)
    if not successful and first_error is not None and first_error.status != 429:
        first_error.checkpoint = copy.deepcopy(state)
        raise first_error
    listing_rows = _merge(listing_rows)
    tokens = list(
        dict.fromkeys(
            [_text(payload.get("showName")), *[_text(alias) for alias in payload.get("showAliases") or []], *terms]
        )
    )
    tokens = sorted(
        (token for token in tokens if len(token) >= 4 or token.startswith("rho")), key=lambda token: -len(token)
    )[:4]
    search_rows: list[dict[str, Any]] = []
    search_pages = 0
    gap_queries_run = 0
    search_params = {"restrict_sr": "on", "sort": "new", "t": "all", "limit": 100, "raw_json": 1}

    def search(query: str, pages: int) -> bool:
        nonlocal search_pages
        try:
            progress = fetch(
                "search:" + query, f"/r/{quote(subreddit)}/search.json", {**search_params, "q": query}, pages
            )
        except RedditTransportError as exc:
            progress = state["queries"]["search:" + query]
            search_rows.extend(progress["rows"])
            search_pages += progress["pages"]
            if exc.status != 429:
                raise
            return False
        search_rows.extend(progress["rows"])
        search_pages += progress["pages"]
        return True

    if expected:
        for kind in TYPES:
            if not search(_query(tokens, season, kind), 3):
                break
    candidates = _collect(_merge(listing_rows + search_rows), config)
    slots = [{"episode_number": episode, "discussion_type": kind} for episode in expected for kind in TYPES]

    def missing() -> list[dict[str, Any]]:
        found = {(row["episode_number"], row["discussion_type"]) for row in candidates}
        return [slot for slot in slots if (slot["episode_number"], slot["discussion_type"]) not in found]

    if state["gap_queries"] is None:
        state["gap_queries"] = [
            _query(tokens, season, slot["discussion_type"], slot["episode_number"]) for slot in missing()
        ][:12]
        save()
    for query in state["gap_queries"]:
        if not missing():
            break
        if not search(query, 1):
            break
        gap_queries_run += 1
        candidates = _collect(_merge(listing_rows + search_rows), config)
    # A later retry can fill all slots from a previously failed sort. Keep every
    # saved search result even when that makes replaying old gap queries unnecessary.
    saved_searches = {key: progress for key, progress in state["queries"].items() if key.startswith("search:")}
    search_rows = [row for progress in saved_searches.values() for row in progress["rows"]]
    search_pages = sum(progress["pages"] for progress in saved_searches.values())
    gap_queries_run = sum(bool(saved_searches.get("search:" + query, {}).get("done")) for query in state["gap_queries"])
    candidates = _collect(_merge(listing_rows + search_rows), config)
    missing_slots = missing()
    unresolved = [key for key, progress in state["queries"].items() if progress.get("error") or progress.get("capped")]
    return {
        "subreddit": subreddit,
        "fetched_at": _iso(datetime.now(UTC)),
        "sources_fetched": successful,
        "successful_sorts": successful,
        "failed_sorts": failed,
        "rate_limited_sorts": rate_limited,
        "candidates": candidates,
        "episode_matrix": _matrix(candidates, expected),
        "expected_episode_count": len(expected),
        "expected_episode_numbers": expected,
        "coverage_found_episode_count": len({row["episode_number"] for row in candidates}),
        "coverage_expected_slots": len(slots),
        "coverage_found_slots": len(slots) - len(missing_slots),
        "coverage_missing_slots": missing_slots,
        "discovery_source_summary": {
            "listing_count": len(listing_rows),
            "search_count": len(_merge(search_rows)),
            "search_pages_fetched": search_pages,
            "gap_fill_queries_run": gap_queries_run,
        },
        "filters_applied": {
            "season_number": season,
            "title_patterns": patterns,
            "required_flairs": flairs,
            "show_focused": focused,
            "period_start": _iso(start),
            "period_end": _iso(end),
        },
        "collection_complete": not unresolved,
        "unresolved_queries": unresolved,
        "checkpoint": copy.deepcopy(state),
    }


def evaluate_sync_eligibility(payload: dict[str, Any]) -> dict[str, Any]:
    """Port the refresh route's AutoModerator/title/New York date auto-save gate."""

    def result(eligible: bool, reason: str, code: str) -> dict[str, Any]:
        return {"eligible": eligible, "reason": reason, "reason_code": code}

    if _text(payload.get("author")) != "automoderator":
        return result(False, "Author is not AutoModerator.", "author_not_automoderator")
    if not re.search("episode discussion", str(payload.get("title") or ""), re.I):
        return result(False, "Title does not include 'Episode Discussion'.", "title_missing_episode_discussion")
    air_date, posted_raw = payload.get("episodeAirDateKey"), payload.get("postedAt")
    if not air_date or not posted_raw:
        if not air_date:
            episode = payload.get("episodeNumber")
            label = f"parsed episode number {episode}" if episode is not None else "parsed episode number"
            return result(
                False,
                f"No episode air date found for {label} in season {payload.get('seasonNumber')}.",
                "missing_episode_air_date",
            )
        return result(False, "Post timestamp is missing.", "missing_post_timestamp")
    posted = _date(posted_raw)
    if posted is None:
        return result(False, "Post timestamp is invalid.", "invalid_post_timestamp")
    posted_key = posted.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    next_day = None
    if payload.get("discussionType") == "weekly" and (parsed := _date(air_date)):
        next_day = (parsed + timedelta(days=1)).date().isoformat()
    if posted_key != air_date and posted_key != next_day:
        return result(
            False,
            f"Post date {posted_key} does not match episode air date {air_date} (America/New_York).",
            "posted_date_mismatch",
        )
    return result(True, "Eligible for auto-sync.", "auto_saved_success")
