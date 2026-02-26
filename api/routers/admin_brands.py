from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid5

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, HttpUrl

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from api.routers.admin_show_links import _canonicalize_url, _upsert_link, _url_key
from trr_backend.db import pg
from trr_backend.integrations.franchise_rules import (
    RULE_VERSION,
    classify_show_franchise,
    default_rules_by_key,
    detect_suggested_franchises,
    get_candidate_urls_for_rule,
    is_fallback_link_metadata,
    normalize_rule_config,
    normalize_rule_key,
    show_matches_rule,
)

router = APIRouter(prefix="/admin/brands", tags=["admin-brands"])

_RULE_DEFINITION_NAMESPACE = UUID("2b9ab693-4f62-4208-bde7-4ac0b7f5f982")


class FranchiseRuleUpdateRequest(BaseModel):
    name: str | None = None
    primary_url: HttpUrl | None = None
    review_allpages_url: HttpUrl | None = None
    match_terms: list[str] | None = None
    aliases: list[str] | None = None
    community_domains: list[str] | None = None
    include_allpages_scan: bool | None = None
    source_rank: int | None = Field(default=None, ge=0, le=10000)
    network_terms: list[str] | None = None
    is_active: bool | None = None


class FranchiseRuleApplyRequest(BaseModel):
    missing_only: bool = True
    dry_run: bool = False
    limit: int | None = Field(default=None, ge=1, le=5000)


def _admin_actor(admin: AdminUser) -> str:
    return str(admin.get("email") or admin.get("id") or "admin")


def _rule_definition_entity_id(franchise_key: str) -> str:
    return str(uuid5(_RULE_DEFINITION_NAMESPACE, franchise_key))


def _normalize_networks(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip().lower()
        if cleaned and cleaned not in out:
            out.append(cleaned)
    return out


def _load_rule_definition_rows() -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT id, url, label, metadata, updated_at, created_at, updated_by
        FROM core.entity_links
        WHERE show_id IS NULL
          AND entity_type = 'show'
          AND link_kind = 'fandom'
          AND source = 'franchise_rule_definition'
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
        """
    )


def _load_effective_rules() -> dict[str, dict[str, Any]]:
    rules = default_rules_by_key()

    for row in _load_rule_definition_rows():
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if str(metadata.get("rule_scope") or "").strip().lower() != "franchise_fallback_definition":
            continue

        key = normalize_rule_key(str(metadata.get("franchise_key") or metadata.get("key") or ""))
        if not key:
            continue

        base_rule = dict(rules.get(key) or {})
        merged = {
            "key": key,
            "name": metadata.get("name") if isinstance(metadata.get("name"), str) else base_rule.get("name") or key,
            "primary_url": (
                metadata.get("primary_url")
                if isinstance(metadata.get("primary_url"), str)
                else row.get("url")
            ),
            "review_allpages_url": metadata.get("review_allpages_url")
            if isinstance(metadata.get("review_allpages_url"), str)
            else base_rule.get("review_allpages_url"),
            "match_terms": (
                metadata.get("match_terms")
                if isinstance(metadata.get("match_terms"), list)
                else base_rule.get("match_terms") or []
            ),
            "aliases": (
                metadata.get("aliases")
                if isinstance(metadata.get("aliases"), list)
                else base_rule.get("aliases") or []
            ),
            "community_domains": metadata.get("community_domains")
            if isinstance(metadata.get("community_domains"), list)
            else base_rule.get("community_domains") or [],
            "include_allpages_scan": metadata.get("include_allpages_scan")
            if isinstance(metadata.get("include_allpages_scan"), bool)
            else bool(base_rule.get("include_allpages_scan")),
            "source_rank": metadata.get("source_rank")
            if isinstance(metadata.get("source_rank"), int)
            else int(base_rule.get("source_rank") or 100),
            "network_terms": metadata.get("network_terms")
            if isinstance(metadata.get("network_terms"), list)
            else base_rule.get("network_terms") or [],
            "is_active": (
                metadata.get("is_active")
                if isinstance(metadata.get("is_active"), bool)
                else bool(base_rule.get("is_active", True))
            ),
            "rule_version": metadata.get("rule_version")
            if isinstance(metadata.get("rule_version"), int)
            else int(base_rule.get("rule_version") or RULE_VERSION),
            "updated_at": row.get("updated_at"),
            "updated_by": row.get("updated_by"),
            "definition_row_id": row.get("id"),
        }

        rules[key] = normalize_rule_config(merged)
        rules[key]["updated_at"] = merged.get("updated_at")
        rules[key]["updated_by"] = merged.get("updated_by")
        rules[key]["definition_row_id"] = merged.get("definition_row_id")

    return rules


def _query_shows(
    db: SupabaseAdminClient,
    *,
    query: str | None = None,
    limit: int = 300,
    offset: int = 0,
) -> list[dict[str, Any]]:
    q = db.schema("core").table("shows").select("id,name,canonical_slug,networks").order("name")
    if query:
        q = q.ilike("name", f"%{query}%")
    response = q.range(max(0, offset), max(0, offset) + max(1, min(1000, limit)) - 1).execute()
    if hasattr(response, "error") and response.error:
        raise HTTPException(status_code=502, detail=f"Failed to query shows: {response.error}")
    return [row for row in (response.data or []) if isinstance(row, dict)]


def _fetch_show_fandom_links(
    db: SupabaseAdminClient,
    show_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    links_by_show: dict[str, list[dict[str, Any]]] = {show_id: [] for show_id in show_ids}
    if not show_ids:
        return links_by_show

    chunk_size = 200
    for idx in range(0, len(show_ids), chunk_size):
        chunk = show_ids[idx : idx + chunk_size]
        response = (
            db.schema("core")
            .table("entity_links")
            .select("id,show_id,url,metadata,source,status,link_kind,updated_at,created_at")
            .in_("show_id", chunk)
            .eq("entity_type", "show")
            .in_("link_kind", ["fandom", "wikia"])
            .neq("status", "rejected")
            .execute()
        )
        if hasattr(response, "error") and response.error:
            raise HTTPException(status_code=502, detail=f"Failed to query fandom links: {response.error}")
        for row in response.data or []:
            if not isinstance(row, dict):
                continue
            show_id = str(row.get("show_id") or "").strip()
            if not show_id:
                continue
            links_by_show.setdefault(show_id, []).append(row)

    def _sort_key(row: dict[str, Any]) -> tuple[str, str]:
        updated_at = str(row.get("updated_at") or "")
        created_at = str(row.get("created_at") or "")
        return (updated_at, created_at)

    for key in list(links_by_show.keys()):
        links_by_show[key] = sorted(links_by_show[key], key=_sort_key, reverse=True)

    return links_by_show


def _split_explicit_vs_fallback(links: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    explicit: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    for row in links:
        status = str(row.get("status") or "").strip().lower()
        if status != "approved":
            continue
        if is_fallback_link_metadata(row.get("metadata"), str(row.get("source") or "")):
            fallback.append(row)
        else:
            explicit.append(row)
    return explicit, fallback


def _effective_rule_for_show(show_row: dict[str, Any], rules: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    key = classify_show_franchise(
        str(show_row.get("name") or "").strip(),
        show_row.get("networks"),
        rules,
    )
    if not key:
        return None
    return rules.get(key)


@router.get("/shows-franchises")
def list_brand_shows_franchises(
    _: AdminUser,
    db: SupabaseAdminClient,
    q: str | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    shows = _query_shows(db, query=q, limit=limit, offset=offset)
    show_ids = [str(row.get("id") or "").strip() for row in shows if row.get("id")]
    show_ids = [show_id for show_id in show_ids if show_id]
    links_by_show = _fetch_show_fandom_links(db, show_ids)
    rules = _load_effective_rules()

    rows: list[dict[str, Any]] = []
    for show in shows:
        show_id = str(show.get("id") or "").strip()
        show_name = str(show.get("name") or "").strip()
        canonical_slug = str(show.get("canonical_slug") or "").strip() or None
        networks = _normalize_networks(show.get("networks"))
        explicit_links, fallback_links = _split_explicit_vs_fallback(links_by_show.get(show_id, []))
        inferred_key = classify_show_franchise(show_name, networks, rules)
        inferred_rule = rules.get(inferred_key) if inferred_key else None
        inferred_candidates = get_candidate_urls_for_rule(inferred_rule or {})

        explicit_url = str(explicit_links[0].get("url") or "").strip() if explicit_links else None
        fallback_url = str(fallback_links[0].get("url") or "").strip() if fallback_links else None
        proposed_url = str(inferred_candidates[0].get("url") or "").strip() if inferred_candidates else None

        if explicit_url:
            effective_url = explicit_url
            source = "explicit"
        elif fallback_url:
            effective_url = fallback_url
            source = "fallback"
        elif proposed_url:
            effective_url = proposed_url
            source = "rule_default"
        else:
            effective_url = None
            source = "none"

        rows.append(
            {
                "show_id": show_id,
                "show_name": show_name,
                "canonical_slug": canonical_slug,
                "networks": networks,
                "franchise_key": inferred_key,
                "franchise_name": inferred_rule.get("name") if inferred_rule else None,
                "explicit_fandom_url": explicit_url,
                "fallback_fandom_url": fallback_url,
                "effective_fandom_url": effective_url,
                "effective_source": source,
                "rule_candidates": [entry.get("url") for entry in inferred_candidates],
                "include_allpages_scan": bool(inferred_rule.get("include_allpages_scan")) if inferred_rule else False,
            }
        )

    return {
        "rows": rows,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "query": q or "",
    }


@router.get("/franchise-rules")
def list_brand_franchise_rules(
    _: AdminUser,
    db: SupabaseAdminClient,
) -> dict[str, Any]:
    rules = _load_effective_rules()
    shows = _query_shows(db, limit=1000, offset=0)
    show_names = [str(row.get("name") or "").strip() for row in shows if isinstance(row.get("name"), str)]

    show_ids = [str(row.get("id") or "").strip() for row in shows if row.get("id")]
    show_ids = [show_id for show_id in show_ids if show_id]
    links_by_show = _fetch_show_fandom_links(db, show_ids)

    applied_counts: dict[str, int] = {}
    for show_links in links_by_show.values():
        for link in show_links:
            metadata = link.get("metadata") if isinstance(link.get("metadata"), dict) else {}
            if not is_fallback_link_metadata(metadata, str(link.get("source") or "")):
                continue
            key = normalize_rule_key(str(metadata.get("franchise_key") or ""))
            if not key:
                continue
            applied_counts[key] = applied_counts.get(key, 0) + 1

    matched_counts: dict[str, int] = dict.fromkeys(rules, 0)
    for show in shows:
        name = str(show.get("name") or "").strip()
        networks = show.get("networks")
        for key, rule in rules.items():
            if show_matches_rule(name, networks, rule):
                matched_counts[key] = matched_counts.get(key, 0) + 1

    rows: list[dict[str, Any]] = []
    for key in sorted(rules):
        rule = dict(rules[key])
        rule["matched_show_count"] = int(matched_counts.get(key) or 0)
        rule["applied_fallback_count"] = int(applied_counts.get(key) or 0)
        rule["candidate_urls"] = [entry.get("url") for entry in get_candidate_urls_for_rule(rule)]
        rows.append(rule)

    suggested = detect_suggested_franchises(show_names, set(rules.keys()))

    return {
        "rules": rows,
        "suggested_franchises": suggested,
        "count": len(rows),
    }


@router.put("/franchise-rules/{franchise_key}")
def upsert_brand_franchise_rule(
    franchise_key: str,
    payload: FranchiseRuleUpdateRequest,
    admin: AdminUser,
) -> dict[str, Any]:
    normalized_key = normalize_rule_key(franchise_key)
    if not normalized_key:
        raise HTTPException(status_code=400, detail="Invalid franchise key")

    rules = _load_effective_rules()
    base_rule = dict(rules.get(normalized_key) or {"key": normalized_key})
    patch = payload.model_dump(exclude_unset=True, mode="json")
    merged = normalize_rule_config({**base_rule, **patch, "key": normalized_key})

    if not merged.get("primary_url"):
        raise HTTPException(status_code=400, detail="primary_url is required")

    actor = _admin_actor(admin)
    metadata = {
        "rule_scope": "franchise_fallback_definition",
        "franchise_key": normalized_key,
        "key": normalized_key,
        "name": merged.get("name"),
        "primary_url": merged.get("primary_url"),
        "review_allpages_url": merged.get("review_allpages_url"),
        "match_terms": merged.get("match_terms") or [],
        "aliases": merged.get("aliases") or [],
        "community_domains": merged.get("community_domains") or [],
        "include_allpages_scan": bool(merged.get("include_allpages_scan")),
        "source_rank": int(merged.get("source_rank") or 100),
        "network_terms": merged.get("network_terms") or [],
        "is_active": bool(merged.get("is_active", True)),
        "rule_version": int(merged.get("rule_version") or RULE_VERSION),
    }

    pg.execute_returning(
        """
        DELETE FROM core.entity_links
        WHERE show_id IS NULL
          AND entity_type = 'show'
          AND link_kind = 'fandom'
          AND source = 'franchise_rule_definition'
          AND coalesce(metadata->>'franchise_key', '') = %s
        """,
        [normalized_key],
    )

    inserted = pg.execute_returning(
        """
        INSERT INTO core.entity_links (
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
          source,
          discovered_by,
          metadata,
          created_by,
          updated_by
        )
        VALUES (
          NULL,
          'show',
          %s,
          0,
          'knowledge',
          'fandom',
          %s,
          %s,
          %s,
          'approved',
          1.0,
          'franchise_rule_definition',
          'admin_brands',
          %s::jsonb,
          %s,
          %s
        )
        RETURNING id, updated_at, created_at
        """,
        [
            _rule_definition_entity_id(normalized_key),
            f"Franchise Rule ({merged.get('name') or normalized_key})",
            _canonicalize_url(str(merged.get("primary_url") or "")),
            _url_key(str(merged.get("primary_url") or "")),
            json.dumps(metadata),
            actor,
            actor,
        ],
    )

    out = dict(merged)
    out.update(
        {
            "definition_row_id": inserted[0].get("id") if inserted else None,
            "updated_at": inserted[0].get("updated_at") if inserted else None,
            "updated_by": actor,
        }
    )
    out["candidate_urls"] = [entry.get("url") for entry in get_candidate_urls_for_rule(merged)]
    return out


@router.post("/franchise-rules/{franchise_key}/apply")
def apply_brand_franchise_rule(
    franchise_key: str,
    payload: FranchiseRuleApplyRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    normalized_key = normalize_rule_key(franchise_key)
    if not normalized_key:
        raise HTTPException(status_code=400, detail="Invalid franchise key")

    rules = _load_effective_rules()
    rule = rules.get(normalized_key)
    if not rule:
        raise HTTPException(status_code=404, detail=f"Unknown franchise rule: {normalized_key}")
    if not bool(rule.get("is_active", True)):
        raise HTTPException(status_code=400, detail=f"Franchise rule '{normalized_key}' is inactive")

    candidate_urls = get_candidate_urls_for_rule(rule)
    if not candidate_urls:
        raise HTTPException(status_code=400, detail=f"Franchise rule '{normalized_key}' has no valid candidate URLs")

    shows = _query_shows(db, limit=1000, offset=0)
    matched_shows = [
        show
        for show in shows
        if show_matches_rule(str(show.get("name") or "").strip(), show.get("networks"), rule)
    ]
    if payload.limit is not None:
        matched_shows = matched_shows[: payload.limit]

    show_ids = [str(row.get("id") or "").strip() for row in matched_shows if row.get("id")]
    show_ids = [show_id for show_id in show_ids if show_id]
    links_by_show = _fetch_show_fandom_links(db, show_ids)

    actor = _admin_actor(admin)
    links_upserted = 0
    applied_show_count = 0
    skipped_explicit = 0
    skipped_already_fallback = 0

    applied_preview: list[dict[str, Any]] = []

    for show in matched_shows:
        show_id = str(show.get("id") or "").strip()
        show_name = str(show.get("name") or "").strip()
        if not show_id:
            continue

        existing_links = links_by_show.get(show_id, [])
        explicit_links, fallback_links = _split_explicit_vs_fallback(existing_links)

        if payload.missing_only and explicit_links:
            skipped_explicit += 1
            continue

        if payload.missing_only and fallback_links:
            skipped_already_fallback += 1
            continue

        applied_show_count += 1
        preview_row = {
            "show_id": show_id,
            "show_name": show_name,
            "canonical_slug": str(show.get("canonical_slug") or "").strip() or None,
            "urls": [entry.get("url") for entry in candidate_urls],
            "dry_run": bool(payload.dry_run),
        }
        applied_preview.append(preview_row)

        if payload.dry_run:
            continue

        for entry in candidate_urls:
            url = str(entry.get("url") or "").strip()
            if not url:
                continue

            metadata = {
                "rule_scope": "franchise_fallback",
                "franchise_key": normalized_key,
                "is_fallback": True,
                "source_rank": int(entry.get("source_rank") or rule.get("source_rank") or 100),
                "include_allpages_scan": bool(entry.get("include_allpages_scan", rule.get("include_allpages_scan"))),
                "rule_version": int(rule.get("rule_version") or RULE_VERSION),
                "rule_name": rule.get("name"),
            }
            _upsert_link(
                db,
                show_id=show_id,
                entity_type="show",
                entity_id=show_id,
                link_group="knowledge",
                link_kind="fandom",
                url=url,
                label=str(entry.get("label") or f"Fandom ({rule.get('name') or normalized_key})"),
                season_number=0,
                status="approved",
                confidence=0.8,
                source="franchise_rule",
                discovered_by="admin_brands_apply",
                metadata=metadata,
                actor=actor,
            )
            links_upserted += 1

    return {
        "franchise_key": normalized_key,
        "rule_name": rule.get("name"),
        "matched_show_count": len(matched_shows),
        "applied_show_count": applied_show_count,
        "links_upserted": links_upserted,
        "skipped_explicit": skipped_explicit,
        "skipped_already_fallback": skipped_already_fallback,
        "missing_only": bool(payload.missing_only),
        "dry_run": bool(payload.dry_run),
        "applied": applied_preview,
    }
