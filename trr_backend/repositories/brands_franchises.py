from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse, urlunparse

from trr_backend.db import pg

logger = logging.getLogger(__name__)

_RULE_SOURCE_PREFIX = "brands_franchise_rule:"
_SCHEMA_READY = False


@dataclass(frozen=True)
class FranchiseRule:
    key: str
    name: str
    primary_url: str
    review_allpages_url: str | None
    match_terms: tuple[str, ...]
    aliases: tuple[str, ...]
    community_domains: tuple[str, ...]
    include_allpages_scan: bool
    source_rank: int
    network_terms: tuple[str, ...]
    is_active: bool = True
    rule_version: int = 1
    updated_at: str | None = None
    updated_by: str | None = None
    definition_row_id: str | None = None

    def candidate_urls(self) -> list[str]:
        values = [self.primary_url.strip(), (self.review_allpages_url or "").strip()]
        seen: set[str] = set()
        results: list[str] = []
        for value in values:
            if not value:
                continue
            if value in seen:
                continue
            seen.add(value)
            results.append(value)
        return results

    def to_api(self, *, matched_show_count: int = 0, applied_fallback_count: int = 0) -> dict[str, Any]:
        return {
            "key": self.key,
            "name": self.name,
            "primary_url": self.primary_url,
            "review_allpages_url": self.review_allpages_url,
            "match_terms": list(self.match_terms),
            "aliases": list(self.aliases),
            "community_domains": list(self.community_domains),
            "include_allpages_scan": self.include_allpages_scan,
            "source_rank": self.source_rank,
            "network_terms": list(self.network_terms),
            "is_active": self.is_active,
            "rule_version": self.rule_version,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
            "definition_row_id": self.definition_row_id,
            "matched_show_count": matched_show_count,
            "applied_fallback_count": applied_fallback_count,
            "candidate_urls": self.candidate_urls(),
        }


_DEFAULT_RULES: tuple[FranchiseRule, ...] = (
    FranchiseRule(
        key="real-housewives",
        name="Real Housewives",
        primary_url="https://real-housewives.fandom.com/wiki/Real_Housewives_Wiki",
        review_allpages_url="https://real-housewives.fandom.com/wiki/Special:AllPages",
        match_terms=("real housewives", "housewives"),
        aliases=("rhoa", "rhobh", "rhoc", "rhony", "rhonj", "rhop", "rhoslc", "rhodubai", "rhom"),
        community_domains=("real-housewives.fandom.com",),
        include_allpages_scan=True,
        source_rank=10,
        network_terms=("bravo",),
    ),
    FranchiseRule(
        key="love-island",
        name="Love Island",
        primary_url="https://love-island.fandom.com/wiki/Love_Island_Wiki",
        review_allpages_url="https://love-island.fandom.com/wiki/Special:AllPages",
        match_terms=("love island",),
        aliases=("love island usa", "love island uk", "love island games"),
        community_domains=("love-island.fandom.com",),
        include_allpages_scan=True,
        source_rank=20,
        network_terms=("peacock", "itv", "cbs", "nbc"),
    ),
    FranchiseRule(
        key="traitors",
        name="The Traitors",
        primary_url="https://the-traitors.fandom.com/wiki/The_Traitors_Wiki",
        review_allpages_url="https://the-traitors.fandom.com/wiki/Special:AllPages",
        match_terms=("traitors", "the traitors"),
        aliases=("traitors us", "traitors uk"),
        community_domains=("the-traitors.fandom.com",),
        include_allpages_scan=False,
        source_rank=30,
        network_terms=("peacock", "bbc", "nbc"),
    ),
    FranchiseRule(
        key="vanderpump-universe",
        name="Vanderpump Universe",
        primary_url="https://vanderpump-rules.fandom.com/wiki/Vanderpump_Rules_Wiki",
        review_allpages_url="https://vanderpump-rules.fandom.com/wiki/Special:AllPages",
        match_terms=("vanderpump", "the valley"),
        aliases=("vanderpump rules", "vpr", "vanderpump villa", "the valley"),
        community_domains=("vanderpump-rules.fandom.com",),
        include_allpages_scan=True,
        source_rank=40,
        network_terms=("bravo",),
    ),
)


@dataclass(frozen=True)
class _ShowRecord:
    show_id: str
    show_name: str
    canonical_slug: str
    networks: tuple[str, ...]


@dataclass(frozen=True)
class _FandomLinkState:
    explicit_url: str | None = None
    fallback_url: str | None = None
    fallback_key: str | None = None


def _slugify(value: str) -> str:
    lowered = str(value or "").strip().lower()
    lowered = lowered.replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _normalize_phrase(value: str) -> str:
    return _normalize_text(value)


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _canonicalize_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw

    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return raw

    netloc = hostname
    if parsed.port and not ((scheme == "http" and parsed.port == 80) or (scheme == "https" and parsed.port == 443)):
        netloc = f"{netloc}:{parsed.port}"

    if parsed.username:
        auth = parsed.username
        if parsed.password:
            auth = f"{auth}:{parsed.password}"
        netloc = f"{auth}@{netloc}"

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def _url_key(value: str) -> str:
    return _canonicalize_url(value).lower()


def _string_list(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return tuple()
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in value:
        normalized = _normalize_phrase(str(item or ""))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(normalized)
    return tuple(cleaned)


def _ordered_active_rules(rules_by_key: dict[str, FranchiseRule]) -> list[FranchiseRule]:
    return sorted((rule for rule in rules_by_key.values() if rule.is_active), key=lambda rule: (rule.source_rank, rule.key))


def _ensure_schema(*, required: bool) -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    statements = [
        "create schema if not exists admin",
        """
        create table if not exists admin.brands_franchise_rules (
          franchise_key text primary key,
          name text not null,
          primary_url text not null,
          review_allpages_url text,
          match_terms text[] not null default array[]::text[],
          aliases text[] not null default array[]::text[],
          community_domains text[] not null default array[]::text[],
          include_allpages_scan boolean not null default false,
          source_rank int not null default 100,
          network_terms text[] not null default array[]::text[],
          is_active boolean not null default true,
          rule_version int not null default 1,
          updated_by text,
          updated_at timestamptz not null default now(),
          created_at timestamptz not null default now()
        )
        """,
        "create index if not exists brands_franchise_rules_active_rank_idx on admin.brands_franchise_rules (is_active, source_rank, franchise_key)",
        "grant usage on schema admin to service_role",
        "grant all privileges on table admin.brands_franchise_rules to service_role",
    ]

    try:
        with pg.db_connection() as conn:
            with pg.db_cursor(conn=conn) as cur:
                for statement in statements:
                    cur.execute(statement)
                for rule in _DEFAULT_RULES:
                    cur.execute(
                        """
                        insert into admin.brands_franchise_rules (
                          franchise_key,
                          name,
                          primary_url,
                          review_allpages_url,
                          match_terms,
                          aliases,
                          community_domains,
                          include_allpages_scan,
                          source_rank,
                          network_terms,
                          is_active,
                          rule_version,
                          updated_by,
                          updated_at
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                        on conflict (franchise_key) do nothing
                        """,
                        [
                            rule.key,
                            rule.name,
                            rule.primary_url,
                            rule.review_allpages_url,
                            list(rule.match_terms),
                            list(rule.aliases),
                            list(rule.community_domains),
                            rule.include_allpages_scan,
                            rule.source_rank,
                            list(rule.network_terms),
                            rule.is_active,
                            rule.rule_version,
                            "seed",
                        ],
                    )
        _SCHEMA_READY = True
    except Exception as exc:  # noqa: BLE001
        if required:
            raise RuntimeError("Brands franchise rules table is unavailable. Run backend migrations.") from exc
        logger.warning("brands_franchises: schema not ready, using seeded defaults only (%s)", exc)


def _load_rule_rows() -> list[dict[str, Any]]:
    _ensure_schema(required=False)
    try:
        return pg.fetch_all(
            """
            select
              franchise_key,
              name,
              primary_url,
              review_allpages_url,
              match_terms,
              aliases,
              community_domains,
              include_allpages_scan,
              source_rank,
              network_terms,
              is_active,
              rule_version,
              updated_at,
              updated_by
            from admin.brands_franchise_rules
            order by source_rank asc, franchise_key asc
            """
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("brands_franchises: failed to load DB rules, using defaults (%s)", exc)
        return []


def _rule_from_row(row: dict[str, Any]) -> FranchiseRule:
    return FranchiseRule(
        key=_normalize_key(row.get("franchise_key") or ""),
        name=str(row.get("name") or "").strip() or _normalize_key(row.get("franchise_key") or "").replace("-", " ").title(),
        primary_url=str(row.get("primary_url") or "").strip(),
        review_allpages_url=str(row.get("review_allpages_url") or "").strip() or None,
        match_terms=_string_list(row.get("match_terms")),
        aliases=_string_list(row.get("aliases")),
        community_domains=_string_list(row.get("community_domains")),
        include_allpages_scan=bool(row.get("include_allpages_scan")),
        source_rank=max(0, int(row.get("source_rank") or 0)),
        network_terms=_string_list(row.get("network_terms")),
        is_active=bool(row.get("is_active", True)),
        rule_version=max(1, int(row.get("rule_version") or 1)),
        updated_at=str(row.get("updated_at") or "") or None,
        updated_by=str(row.get("updated_by") or "") or None,
    )


def _merged_rules() -> dict[str, FranchiseRule]:
    merged = {rule.key: rule for rule in _DEFAULT_RULES}
    for row in _load_rule_rows():
        rule = _rule_from_row(row)
        if not rule.key:
            continue
        merged[rule.key] = rule
    return merged


def _rule_matches_show(rule: FranchiseRule, *, show_name: str, networks: tuple[str, ...]) -> bool:
    normalized_name = _normalize_text(show_name)
    normalized_networks = tuple(_normalize_text(network) for network in networks)

    terms = tuple(term for term in (rule.match_terms + rule.aliases) if term)
    name_match = any(term in normalized_name for term in terms) if terms else False

    network_terms = tuple(term for term in rule.network_terms if term)
    network_match = any(
        network_term in network_name
        for network_term in network_terms
        for network_name in normalized_networks
    ) if network_terms else False

    if terms and network_terms:
        return name_match and network_match
    if terms:
        return name_match
    if network_terms:
        return network_match
    return False


def _resolve_show_rule(show_name: str, networks: tuple[str, ...], rules_by_key: dict[str, FranchiseRule]) -> FranchiseRule | None:
    for rule in _ordered_active_rules(rules_by_key):
        if _rule_matches_show(rule, show_name=show_name, networks=networks):
            return rule
    return None


def _fetch_shows(*, q: str, limit: int) -> list[_ShowRecord]:
    trimmed = q.strip()
    rows = pg.fetch_all(
        """
        select
          s.id::text as show_id,
          s.name as show_name,
          coalesce(s.networks, '{}'::text[]) as networks
        from core.shows s
        where (%s = '' or s.name ilike %s)
        order by s.name asc
        limit %s
        """,
        [trimmed, f"%{trimmed}%", max(1, min(limit, 1000))],
    )
    result: list[_ShowRecord] = []
    for row in rows:
        show_name = str(row.get("show_name") or "").strip()
        if not show_name:
            continue
        show_id = str(row.get("show_id") or "").strip()
        if not show_id:
            continue
        raw_networks = row.get("networks")
        networks = tuple(
            str(item or "").strip()
            for item in (raw_networks if isinstance(raw_networks, list) else [])
            if str(item or "").strip()
        )
        result.append(
            _ShowRecord(
                show_id=show_id,
                show_name=show_name,
                canonical_slug=_slugify(show_name),
                networks=networks,
            )
        )
    return result


def _fetch_fandom_links(show_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not show_ids:
        return {}
    rows = pg.fetch_all(
        """
        select
          show_id::text as show_id,
          url,
          source,
          metadata,
          updated_at,
          created_at
        from core.entity_links
        where show_id = any(%s::uuid[])
          and entity_type = 'show'
          and lower(link_kind) in ('fandom', 'wikia')
          and status = 'approved'
        order by show_id asc, updated_at desc nulls last, created_at desc nulls last
        """,
        [show_ids],
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get("show_id") or "")
        if not key:
            continue
        grouped.setdefault(key, []).append(row)
    return grouped


def _fetch_generic_link_rules(limit: int = 1000) -> list[dict[str, Any]]:
    try:
        rows = pg.fetch_all(
            """
            select
              id::text as id,
              family_id::text as family_id,
              link_group,
              link_kind,
              label,
              url,
              coverage_type,
              coverage_value,
              source,
              auto_apply,
              is_active,
              priority,
              metadata,
              updated_at
            from admin.brand_family_link_rules
            order by is_active desc, priority asc, updated_at desc
            limit %s
            """,
            [max(1, min(limit, 5000))],
        )
    except Exception:  # noqa: BLE001
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": str(row.get("id") or ""),
                "family_id": str(row.get("family_id") or "") or None,
                "link_group": str(row.get("link_group") or "other"),
                "link_kind": str(row.get("link_kind") or "external"),
                "label": str(row.get("label") or "") or None,
                "url": str(row.get("url") or ""),
                "coverage_type": str(row.get("coverage_type") or "family_all_shows"),
                "coverage_value": str(row.get("coverage_value") or "") or None,
                "source": str(row.get("source") or "manual"),
                "auto_apply": bool(row.get("auto_apply", True)),
                "is_active": bool(row.get("is_active", True)),
                "priority": int(row.get("priority") or 100),
                "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
                "updated_at": str(row.get("updated_at") or "") or None,
            }
        )
    return out


def _is_rule_fallback_link(row: dict[str, Any]) -> tuple[bool, str | None]:
    source = str(row.get("source") or "").strip().lower()
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    metadata_key = _normalize_key(str(metadata.get("franchise_key") or "")) if isinstance(metadata, dict) else ""

    if source.startswith(_RULE_SOURCE_PREFIX):
        return True, _normalize_key(source.removeprefix(_RULE_SOURCE_PREFIX)) or metadata_key or None

    applied_by = str(metadata.get("applied_by") or "").strip().lower() if isinstance(metadata, dict) else ""
    if applied_by == "brands_franchise_rule":
        return True, metadata_key or None

    return False, metadata_key or None


def _classify_links(rows: list[dict[str, Any]]) -> _FandomLinkState:
    explicit_url: str | None = None
    fallback_url: str | None = None
    fallback_key: str | None = None

    for row in rows:
        url = str(row.get("url") or "").strip()
        if not url:
            continue
        is_fallback, parsed_key = _is_rule_fallback_link(row)
        if is_fallback:
            if fallback_url is None:
                fallback_url = url
                fallback_key = parsed_key
            continue
        if explicit_url is None:
            explicit_url = url

    return _FandomLinkState(explicit_url=explicit_url, fallback_url=fallback_url, fallback_key=fallback_key)


def list_shows_franchises(*, q: str = "", limit: int = 300) -> dict[str, Any]:
    rules_by_key = _merged_rules()
    shows = _fetch_shows(q=q, limit=limit)
    links_by_show = _fetch_fandom_links([show.show_id for show in shows])

    groups: dict[str, dict[str, Any]] = {
        rule.key: {
            "franchise_key": rule.key,
            "franchise_name": rule.name,
            "show_count": 0,
            "sample_show_slugs": [],
            "source_rank": rule.source_rank,
        }
        for rule in _ordered_active_rules(rules_by_key)
    }
    groups.setdefault(
        "unassigned",
        {
            "franchise_key": "unassigned",
            "franchise_name": "Unassigned",
            "show_count": 0,
            "sample_show_slugs": [],
            "source_rank": 10_000,
        },
    )

    rows: list[dict[str, Any]] = []
    for show in shows:
        rule = _resolve_show_rule(show.show_name, show.networks, rules_by_key)
        links = _classify_links(links_by_show.get(show.show_id, []))
        fallback_rule = rules_by_key.get(links.fallback_key or "") if links.fallback_key else None

        franchise_key = rule.key if rule else (fallback_rule.key if fallback_rule else None)
        franchise_name = rule.name if rule else (fallback_rule.name if fallback_rule else None)

        effective_url = links.explicit_url or links.fallback_url or (rule.primary_url if rule else None)
        if links.explicit_url:
            effective_source = "explicit"
        elif links.fallback_url:
            effective_source = "fallback"
        elif rule and rule.primary_url:
            effective_source = "rule_default"
        else:
            effective_source = "none"

        candidates = rule.candidate_urls() if rule else (fallback_rule.candidate_urls() if fallback_rule else [])
        include_allpages_scan = bool(rule.include_allpages_scan) if rule else bool(fallback_rule.include_allpages_scan) if fallback_rule else False

        row = {
            "show_id": show.show_id,
            "show_name": show.show_name,
            "canonical_slug": show.canonical_slug,
            "networks": list(show.networks),
            "franchise_key": franchise_key,
            "franchise_name": franchise_name,
            "explicit_fandom_url": links.explicit_url,
            "fallback_fandom_url": links.fallback_url,
            "effective_fandom_url": effective_url,
            "effective_source": effective_source,
            "rule_candidates": candidates,
            "include_allpages_scan": include_allpages_scan,
        }
        rows.append(row)

        group_key = franchise_key or "unassigned"
        if group_key not in groups:
            groups[group_key] = {
                "franchise_key": group_key,
                "franchise_name": franchise_name or group_key.replace("-", " ").title(),
                "show_count": 0,
                "sample_show_slugs": [],
                "source_rank": 9_000,
            }

        group = groups[group_key]
        group["show_count"] = int(group["show_count"]) + 1
        sample_slugs = group["sample_show_slugs"]
        if len(sample_slugs) < 5 and show.canonical_slug not in sample_slugs:
            sample_slugs.append(show.canonical_slug)

    ordered_groups = sorted(
        groups.values(),
        key=lambda group: (int(group.get("source_rank") or 9_999), str(group.get("franchise_name") or "")),
    )
    for group in ordered_groups:
        group.pop("source_rank", None)

    return {
        "rows": rows,
        "count": len(rows),
        "groups": ordered_groups,
        "link_rules": _fetch_generic_link_rules(),
    }


def _counts_for_rule(
    *,
    rule: FranchiseRule,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    matched = 0
    fallback = 0
    for row in rows:
        if row.get("franchise_key") == rule.key:
            matched += 1
        source = str(row.get("effective_source") or "")
        if source == "fallback" and row.get("franchise_key") == rule.key:
            fallback += 1
    return matched, fallback


def list_franchise_rules() -> dict[str, Any]:
    rules_by_key = _merged_rules()
    rows = list_shows_franchises(limit=1000).get("rows", [])
    response_rules: list[dict[str, Any]] = []
    for rule in _ordered_active_rules(rules_by_key):
        matched, fallback = _counts_for_rule(rule=rule, rows=rows)
        response_rules.append(rule.to_api(matched_show_count=matched, applied_fallback_count=fallback))

    # Include inactive DB rules after active defaults/rules.
    inactive_rules = sorted(
        [rule for rule in rules_by_key.values() if not rule.is_active],
        key=lambda rule: (rule.source_rank, rule.key),
    )
    for rule in inactive_rules:
        matched, fallback = _counts_for_rule(rule=rule, rows=rows)
        response_rules.append(rule.to_api(matched_show_count=matched, applied_fallback_count=fallback))

    return {
        "rules": response_rules,
        "suggested_franchises": [],
    }


def update_franchise_rule(*, franchise_key: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    _ensure_schema(required=True)
    key = _normalize_key(franchise_key)
    if not key:
        raise ValueError("Invalid franchise key")

    current_rule = _merged_rules().get(key)
    if current_rule is None:
        raise KeyError("Franchise rule not found")

    name = str(payload.get("name") or current_rule.name).strip() or current_rule.name
    primary_url = str(payload.get("primary_url") or current_rule.primary_url).strip()
    if not primary_url:
        raise ValueError("primary_url is required")

    review_allpages_url = str(payload.get("review_allpages_url") or "").strip() or None
    match_terms = _string_list(payload.get("match_terms") if "match_terms" in payload else list(current_rule.match_terms))
    aliases = _string_list(payload.get("aliases") if "aliases" in payload else list(current_rule.aliases))
    community_domains = _string_list(
        payload.get("community_domains") if "community_domains" in payload else list(current_rule.community_domains)
    )
    network_terms = _string_list(payload.get("network_terms") if "network_terms" in payload else list(current_rule.network_terms))
    include_allpages_scan = bool(payload.get("include_allpages_scan", current_rule.include_allpages_scan))
    source_rank = max(0, int(payload.get("source_rank", current_rule.source_rank)))
    is_active = bool(payload.get("is_active", current_rule.is_active))

    rows = pg.execute_returning(
        """
        insert into admin.brands_franchise_rules (
          franchise_key,
          name,
          primary_url,
          review_allpages_url,
          match_terms,
          aliases,
          community_domains,
          include_allpages_scan,
          source_rank,
          network_terms,
          is_active,
          rule_version,
          updated_by,
          updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        on conflict (franchise_key) do update
        set
          name = excluded.name,
          primary_url = excluded.primary_url,
          review_allpages_url = excluded.review_allpages_url,
          match_terms = excluded.match_terms,
          aliases = excluded.aliases,
          community_domains = excluded.community_domains,
          include_allpages_scan = excluded.include_allpages_scan,
          source_rank = excluded.source_rank,
          network_terms = excluded.network_terms,
          is_active = excluded.is_active,
          rule_version = admin.brands_franchise_rules.rule_version + 1,
          updated_by = excluded.updated_by,
          updated_at = now()
        returning
          franchise_key,
          name,
          primary_url,
          review_allpages_url,
          match_terms,
          aliases,
          community_domains,
          include_allpages_scan,
          source_rank,
          network_terms,
          is_active,
          rule_version,
          updated_at,
          updated_by
        """,
        [
            key,
            name,
            _canonicalize_url(primary_url),
            _canonicalize_url(review_allpages_url or "") if review_allpages_url else None,
            list(match_terms),
            list(aliases),
            list(community_domains),
            include_allpages_scan,
            source_rank,
            list(network_terms),
            is_active,
            current_rule.rule_version,
            actor,
        ],
    )
    if not rows:
        raise RuntimeError("Failed to update franchise rule")

    updated = _rule_from_row(rows[0])
    matched, fallback = _counts_for_rule(rule=updated, rows=list_shows_franchises(limit=1000).get("rows", []))
    return updated.to_api(matched_show_count=matched, applied_fallback_count=fallback)


def _upsert_rule_link(*, show: _ShowRecord, rule: FranchiseRule, actor: str) -> int:
    metadata = {
        "franchise_key": rule.key,
        "applied_by": "brands_franchise_rule",
        "applied_at": datetime.now(tz=UTC).isoformat(),
    }

    rows = pg.execute_returning(
        """
        insert into core.entity_links (
          show_id,
          entity_type,
          entity_id,
          season_number,
          link_group,
          link_kind,
          label,
          url,
          url_key,
          status,
          confidence,
          discovered_by,
          source,
          metadata,
          created_by,
          updated_by
        )
        values (
          %s::uuid,
          'show',
          %s::uuid,
          0,
          'knowledge',
          'fandom',
          %s,
          %s,
          %s,
          'approved',
          0.7,
          'admin.brands.franchise_rules',
          %s,
          %s::jsonb,
          %s,
          %s
        )
        on conflict (show_id, entity_type, entity_id, link_kind, season_number, url_key)
        do update
        set
          label = excluded.label,
          status = excluded.status,
          confidence = excluded.confidence,
          discovered_by = excluded.discovered_by,
          source = excluded.source,
          metadata = coalesce(core.entity_links.metadata, '{}'::jsonb) || excluded.metadata,
          updated_by = excluded.updated_by,
          updated_at = now()
        returning id
        """,
        [
            show.show_id,
            show.show_id,
            f"{rule.name} Fandom",
            _canonicalize_url(rule.primary_url),
            _url_key(rule.primary_url),
            f"{_RULE_SOURCE_PREFIX}{rule.key}",
            json.dumps(metadata),
            actor,
            actor,
        ],
    )
    return len(rows)


def apply_franchise_rule(*, franchise_key: str, missing_only: bool, dry_run: bool, actor: str) -> dict[str, Any]:
    _ensure_schema(required=True)
    key = _normalize_key(franchise_key)
    rules_by_key = _merged_rules()
    rule = rules_by_key.get(key)
    if rule is None:
        raise KeyError("Franchise rule not found")
    if not rule.primary_url:
        raise ValueError("Rule primary_url is required")

    shows = _fetch_shows(q="", limit=10_000)
    links_by_show = _fetch_fandom_links([show.show_id for show in shows])

    matched: list[_ShowRecord] = []
    skipped_explicit = 0
    skipped_already_fallback = 0
    applied_entries: list[dict[str, Any]] = []
    links_upserted = 0

    for show in shows:
        matched_rule = _resolve_show_rule(show.show_name, show.networks, rules_by_key)
        if matched_rule is None or matched_rule.key != key:
            continue
        matched.append(show)

        state = _classify_links(links_by_show.get(show.show_id, []))
        has_explicit = bool(state.explicit_url)
        if missing_only and has_explicit:
            skipped_explicit += 1
            continue

        has_same_fallback = bool(state.fallback_url) and _url_key(state.fallback_url or "") == _url_key(rule.primary_url)
        if has_same_fallback:
            skipped_already_fallback += 1
            continue

        if not dry_run:
            links_upserted += _upsert_rule_link(show=show, rule=rule, actor=actor)

        applied_entries.append(
            {
                "show_id": show.show_id,
                "show_name": show.show_name,
                "canonical_slug": show.canonical_slug,
                "urls": [rule.primary_url],
                "dry_run": dry_run,
            }
        )

    result = {
        "franchise_key": rule.key,
        "rule_name": rule.name,
        "matched_show_count": len(matched),
        "applied_show_count": len(applied_entries),
        "links_upserted": links_upserted,
        "skipped_explicit": skipped_explicit,
        "skipped_already_fallback": skipped_already_fallback,
        "skipped_existing_manual": skipped_explicit,
        "updated_derived_count": 0,
        "errors": [],
        "missing_only": missing_only,
        "dry_run": dry_run,
        "applied": applied_entries,
    }

    logger.info(
        "brands_franchise_rule_apply key=%s matched=%s applied=%s dry_run=%s links_upserted=%s skipped_explicit=%s skipped_existing=%s",
        rule.key,
        len(matched),
        len(applied_entries),
        dry_run,
        links_upserted,
        skipped_explicit,
        skipped_already_fallback,
    )

    return result
