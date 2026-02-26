from __future__ import annotations

import copy
import re
from typing import Any

RULE_VERSION = 1

# Rule definitions are intentionally conservative and can be overridden from admin endpoints.
_DEFAULT_RULES: tuple[dict[str, Any], ...] = (
    {
        "key": "real-housewives",
        "name": "Real Housewives",
        "primary_url": "https://real-housewives.fandom.com/wiki/Home_Page",
        "review_allpages_url": "https://realitytv-girl.fandom.com/wiki/Special:AllPages",
        "match_terms": [
            "real housewives",
            "rhony",
            "rhoa",
            "rhobh",
            "rhoc",
            "rhonj",
            "rhop",
            "rhod",
            "rhoslc",
            "rhugt",
            "ultimate girls trip",
        ],
        "aliases": ["housewives"],
        "community_domains": ["real-housewives.fandom.com", "realitytv-girl.fandom.com"],
        "include_allpages_scan": True,
        "source_rank": 100,
        "network_terms": ["bravo"],
    },
    {
        "key": "below-deck",
        "name": "Below Deck",
        "primary_url": "https://below-deck.fandom.com/wiki/Below_Deck_Wiki",
        "review_allpages_url": None,
        "match_terms": ["below deck"],
        "aliases": [],
        "community_domains": ["below-deck.fandom.com"],
        "include_allpages_scan": True,
        "source_rank": 100,
        "network_terms": [],
    },
    {
        "key": "traitors",
        "name": "The Traitors",
        "primary_url": "https://the-traitors.fandom.com/wiki/The_Traitors_Wiki",
        "review_allpages_url": None,
        "match_terms": ["traitors", "the traitors"],
        "aliases": [],
        "community_domains": ["the-traitors.fandom.com"],
        "include_allpages_scan": True,
        "source_rank": 100,
        "network_terms": [],
    },
    {
        "key": "kardashians",
        "name": "The Kardashians",
        "primary_url": "https://kardashians.fandom.com/wiki/Kardashians_Wiki",
        "review_allpages_url": None,
        "match_terms": ["kardashian", "kardashians"],
        "aliases": ["keeping up with the kardashians", "kuwtk"],
        "community_domains": ["kardashians.fandom.com"],
        "include_allpages_scan": True,
        "source_rank": 100,
        "network_terms": [],
    },
    {
        "key": "vanderpump",
        "name": "Vanderpump",
        "primary_url": "https://vanderpump-rules.fandom.com/wiki/Vanderpump_Rules_Wiki",
        "review_allpages_url": None,
        "match_terms": ["vanderpump rules", "the valley", "vanderpump villa"],
        "aliases": ["vanderpump"],
        "community_domains": ["vanderpump-rules.fandom.com"],
        "include_allpages_scan": True,
        "source_rank": 100,
        "network_terms": ["bravo"],
    },
    {
        "key": "survivor",
        "name": "Survivor",
        "primary_url": "https://survivor.fandom.com/wiki/Survivor_Wiki",
        "review_allpages_url": None,
        "match_terms": ["survivor"],
        "aliases": [],
        "community_domains": ["survivor.fandom.com"],
        "include_allpages_scan": True,
        "source_rank": 100,
        "network_terms": [],
    },
    {
        "key": "big-brother",
        "name": "Big Brother",
        "primary_url": "https://bigbrother.fandom.com/wiki/Big_Brother_Wiki",
        "review_allpages_url": None,
        "match_terms": ["big brother"],
        "aliases": ["bbus", "bbcan"],
        "community_domains": ["bigbrother.fandom.com"],
        "include_allpages_scan": True,
        "source_rank": 100,
        "network_terms": [],
    },
    {
        "key": "rpdr",
        "name": "RuPaul's Drag Race",
        "primary_url": "https://rupaulsdragrace.fandom.com/wiki/RuPaul%27s_Drag_Race_Wiki",
        "review_allpages_url": None,
        "match_terms": ["rupaul", "drag race", "rpdr"],
        "aliases": ["rupaul's drag race", "rupauls drag race"],
        "community_domains": ["rupaulsdragrace.fandom.com"],
        "include_allpages_scan": True,
        "source_rank": 100,
        "network_terms": [],
    },
)

_STOPWORDS = {"the", "of", "and", "a", "an", "to", "for", "with"}


def normalize_rule_key(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "-", raw).strip("-")


def _normalize_networks(networks: Any) -> set[str]:
    out: set[str] = set()
    if isinstance(networks, (list, tuple)):
        for item in networks:
            if isinstance(item, str):
                cleaned = item.strip().lower()
                if cleaned:
                    out.add(cleaned)
    return out


def _normalize_terms(values: Any) -> list[str]:
    terms: list[str] = []
    if isinstance(values, (list, tuple)):
        for value in values:
            if isinstance(value, str):
                cleaned = value.strip().lower()
                if cleaned and cleaned not in terms:
                    terms.append(cleaned)
    return terms


def _normalize_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if not (cleaned.startswith("http://") or cleaned.startswith("https://")):
        return None
    return cleaned


def normalize_rule_config(raw_rule: dict[str, Any]) -> dict[str, Any]:
    key = normalize_rule_key(str(raw_rule.get("key") or ""))
    name = str(raw_rule.get("name") or key).strip() or key
    primary_url = _normalize_url(raw_rule.get("primary_url"))
    review_allpages_url = _normalize_url(raw_rule.get("review_allpages_url"))
    return {
        "key": key,
        "name": name,
        "primary_url": primary_url,
        "review_allpages_url": review_allpages_url,
        "match_terms": _normalize_terms(raw_rule.get("match_terms")),
        "aliases": _normalize_terms(raw_rule.get("aliases")),
        "community_domains": _normalize_terms(raw_rule.get("community_domains")),
        "include_allpages_scan": bool(raw_rule.get("include_allpages_scan")),
        "source_rank": int(raw_rule.get("source_rank") or 100),
        "network_terms": _normalize_terms(raw_rule.get("network_terms")),
        "rule_version": int(raw_rule.get("rule_version") or RULE_VERSION),
        "is_active": bool(raw_rule.get("is_active", True)),
    }


def default_rules_by_key() -> dict[str, dict[str, Any]]:
    rules: dict[str, dict[str, Any]] = {}
    for raw in _DEFAULT_RULES:
        normalized = normalize_rule_config(copy.deepcopy(raw))
        if normalized["key"]:
            rules[normalized["key"]] = normalized
    return rules


def show_matches_rule(show_name: str | None, networks: Any, rule: dict[str, Any]) -> bool:
    if not bool(rule.get("is_active", True)):
        return False

    normalized_name = str(show_name or "").strip().lower()
    normalized_networks = _normalize_networks(networks)

    terms = _normalize_terms(rule.get("match_terms")) + _normalize_terms(rule.get("aliases"))
    if terms and normalized_name:
        if any(term in normalized_name for term in terms):
            return True

    network_terms = _normalize_terms(rule.get("network_terms"))
    if network_terms and normalized_networks:
        if normalized_name:
            if any(network in normalized_networks for network in network_terms):
                return True
    return False


def classify_show_franchise(
    show_name: str | None,
    networks: Any,
    rules: dict[str, dict[str, Any]] | None = None,
) -> str | None:
    effective_rules = rules or default_rules_by_key()
    normalized_name = str(show_name or "").strip().lower()
    if not normalized_name:
        return None

    best_key: str | None = None
    best_score = -1

    for key, rule in effective_rules.items():
        if not show_matches_rule(show_name, networks, rule):
            continue

        score = 0
        for term in _normalize_terms(rule.get("match_terms")):
            if term and term in normalized_name:
                score += max(1, len(term))
        for alias in _normalize_terms(rule.get("aliases")):
            if alias and alias in normalized_name:
                score += max(1, len(alias) // 2)
        if _normalize_terms(rule.get("network_terms")):
            score += 1

        if score > best_score:
            best_key = key
            best_score = score

    return best_key


def get_candidate_urls_for_rule(rule: dict[str, Any]) -> list[dict[str, Any]]:
    urls: list[dict[str, Any]] = []
    primary_url = _normalize_url(rule.get("primary_url"))
    if primary_url:
        urls.append(
            {
                "url": primary_url,
                "source_rank": int(rule.get("source_rank") or 100),
                "include_allpages_scan": bool(rule.get("include_allpages_scan")),
                "label": f"Fandom ({rule.get('name') or rule.get('key')})",
            }
        )

    review_url = _normalize_url(rule.get("review_allpages_url"))
    if review_url:
        urls.append(
            {
                "url": review_url,
                "source_rank": int(rule.get("source_rank") or 100) + 1,
                "include_allpages_scan": True,
                "label": f"Fandom Review ({rule.get('name') or rule.get('key')})",
            }
        )

    # Preserve URL order while deduping.
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in urls:
        url = str(entry.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(entry)
    return deduped


def is_fallback_link_metadata(metadata: Any, source: str | None = None) -> bool:
    data = metadata if isinstance(metadata, dict) else {}
    if bool(data.get("is_fallback")):
        return True
    if str(data.get("rule_scope") or "").strip().lower() == "franchise_fallback":
        return True
    source_value = str(source or "").strip().lower()
    return source_value in {"franchise_rule", "franchise_rule_definition"}


def detect_suggested_franchises(
    show_names: list[str],
    existing_rule_keys: set[str],
    *,
    min_occurrences: int = 2,
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}

    for show_name in show_names:
        normalized = str(show_name or "").strip().lower()
        if not normalized:
            continue

        normalized = re.sub(r"^[^a-z0-9]*the\s+", "", normalized)
        normalized = re.split(r"[:\-]\s*", normalized, maxsplit=1)[0]
        words = [word for word in re.findall(r"[a-z0-9]+", normalized) if word not in _STOPWORDS]
        if len(words) < 2:
            continue
        candidate_label = f"{words[0]} {words[1]}".strip()
        key = normalize_rule_key(candidate_label)
        if not key or key in existing_rule_keys:
            continue

        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "label": candidate_label.title(),
                "count": 0,
                "sample_shows": [],
            },
        )
        bucket["count"] = int(bucket["count"]) + 1
        samples = bucket["sample_shows"]
        if isinstance(samples, list) and len(samples) < 5:
            samples.append(show_name)

    suggested = [entry for entry in buckets.values() if int(entry.get("count") or 0) >= min_occurrences]
    suggested.sort(key=lambda item: (-(int(item.get("count") or 0)), str(item.get("label") or "")))
    return suggested[:20]
