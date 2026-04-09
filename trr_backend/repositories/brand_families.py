from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import unquote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from trr_backend.db import pg
from trr_backend.repositories import brands_franchises

logger = logging.getLogger(__name__)

EntityType = Literal["network", "streaming"]
CoverageType = Literal[
    "family_all_shows",
    "family_network_shows",
    "family_streaming_shows",
    "franchise_rule",
    "show_wikidata_exact",
    "show_name_contains",
]
LinkGroup = Literal["official", "social", "knowledge", "cast_announcements", "other"]

_ALLOWED_ENTITY_TYPES = {"network", "streaming"}
_ALLOWED_COVERAGE_TYPES = {
    "family_all_shows",
    "family_network_shows",
    "family_streaming_shows",
    "franchise_rule",
    "show_wikidata_exact",
    "show_name_contains",
}
_ALLOWED_LINK_GROUPS = {"official", "social", "knowledge", "cast_announcements", "other"}
_BLOCKED_WIKIPEDIA_PREFIXES = (
    "special:",
    "help:",
    "portal:",
    "wikipedia:",
    "template:",
    "category:",
    "file:",
    "user:",
    "talk:",
    "draft:",
)
_SECTION_HINTS = ("programming", "shows", "series", "original", "current", "former")


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_entity_type(value: str) -> EntityType:
    normalized = _normalize_text(value).lower()
    if normalized not in _ALLOWED_ENTITY_TYPES:
        raise ValueError("entity_type must be network or streaming")
    return normalized  # type: ignore[return-value]


def _normalize_entity_key(value: str) -> str:
    return _normalize_text(value).casefold()


def _normalize_link_group(value: str) -> LinkGroup:
    normalized = _normalize_text(value).lower()
    if normalized not in _ALLOWED_LINK_GROUPS:
        raise ValueError("link_group is invalid")
    return normalized  # type: ignore[return-value]


def _normalize_coverage_type(value: str) -> CoverageType:
    normalized = _normalize_text(value).lower()
    if normalized not in _ALLOWED_COVERAGE_TYPES:
        raise ValueError("coverage_type is invalid")
    return normalized  # type: ignore[return-value]


def _normalize_link_kind(value: str) -> str:
    normalized = _normalize_text(value).lower()
    if normalized == "wikia":
        return "fandom"
    if not normalized:
        raise ValueError("link_kind is required")
    return normalized


def _slugify(value: str) -> str:
    lowered = _normalize_text(value).lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")


def _canonicalize_url(value: str) -> str:
    raw = _normalize_text(value)
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
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def _url_key(value: str) -> str:
    return _canonicalize_url(value).lower()


def _iso_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _family_row_to_api(row: dict[str, Any], *, members: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    payload = {
        "id": str(row.get("id") or ""),
        "family_key": str(row.get("family_key") or ""),
        "display_name": str(row.get("display_name") or ""),
        "owner_wikidata_id": str(row.get("owner_wikidata_id") or "") or None,
        "owner_label": str(row.get("owner_label") or "") or None,
        "is_active": bool(row.get("is_active", True)),
        "notes": str(row.get("notes") or "") or None,
        "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
        "created_by": str(row.get("created_by") or "") or None,
        "updated_by": str(row.get("updated_by") or "") or None,
        "created_at": str(row.get("created_at") or "") or None,
        "updated_at": str(row.get("updated_at") or "") or None,
    }
    if members is not None:
        payload["members"] = members
    return payload


def _member_row_to_api(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row.get("id") or ""),
        "family_id": str(row.get("family_id") or ""),
        "entity_type": str(row.get("entity_type") or ""),
        "entity_key": str(row.get("entity_key") or ""),
        "entity_display_name": str(row.get("entity_display_name") or ""),
        "source": str(row.get("source") or "manual"),
        "confidence": float(row.get("confidence")) if row.get("confidence") is not None else None,
        "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
        "created_by": str(row.get("created_by") or "") or None,
        "updated_by": str(row.get("updated_by") or "") or None,
        "created_at": str(row.get("created_at") or "") or None,
        "updated_at": str(row.get("updated_at") or "") or None,
    }


def _link_rule_row_to_api(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row.get("id") or ""),
        "family_id": str(row.get("family_id") or ""),
        "link_group": str(row.get("link_group") or "other"),
        "link_kind": str(row.get("link_kind") or "external"),
        "label": str(row.get("label") or "") or None,
        "url": str(row.get("url") or ""),
        "url_key": str(row.get("url_key") or ""),
        "coverage_type": str(row.get("coverage_type") or "family_all_shows"),
        "coverage_value": str(row.get("coverage_value") or "") or None,
        "source": str(row.get("source") or "manual"),
        "priority": int(row.get("priority") or 100),
        "auto_apply": bool(row.get("auto_apply", True)),
        "is_active": bool(row.get("is_active", True)),
        "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
        "created_at": str(row.get("created_at") or "") or None,
        "updated_at": str(row.get("updated_at") or "") or None,
        "created_by": str(row.get("created_by") or "") or None,
        "updated_by": str(row.get("updated_by") or "") or None,
    }


def _wiki_link_row_to_api(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row.get("id") or ""),
        "family_id": str(row.get("family_id") or ""),
        "entity_type": str(row.get("entity_type") or ""),
        "entity_key": str(row.get("entity_key") or ""),
        "brand_wikipedia_url": str(row.get("brand_wikipedia_url") or "") or None,
        "show_url": str(row.get("show_url") or ""),
        "show_url_key": str(row.get("show_url_key") or ""),
        "show_title": str(row.get("show_title") or "") or None,
        "wikidata_id": str(row.get("wikidata_id") or "") or None,
        "matched_show_id": str(row.get("matched_show_id") or "") or None,
        "match_method": str(row.get("match_method") or "") or None,
        "import_source": str(row.get("import_source") or "manual"),
        "is_applied": bool(row.get("is_applied", False)),
        "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
        "last_seen_at": str(row.get("last_seen_at") or "") or None,
        "created_at": str(row.get("created_at") or "") or None,
        "updated_at": str(row.get("updated_at") or "") or None,
    }


def _fetch_family_row(family_id: str) -> dict[str, Any]:
    row = pg.fetch_one(
        """
        select *
        from admin.brand_families
        where id = %s::uuid
        limit 1
        """,
        [family_id],
    )
    if not row:
        raise KeyError("Brand family not found")
    return row


def _fetch_family_members(family_id: str) -> list[dict[str, Any]]:
    rows = pg.fetch_all(
        """
        select *
        from admin.brand_family_members
        where family_id = %s::uuid
        order by entity_type asc, entity_display_name asc, created_at asc
        """,
        [family_id],
    )
    return [_member_row_to_api(row) for row in rows]


def _resolve_entity_display_name(entity_type: EntityType, entity_key: str) -> str:
    completion = pg.fetch_one(
        """
        select display_name
        from admin.network_streaming_completion
        where entity_type = %s and entity_key = %s
        order by updated_at desc
        limit 1
        """,
        [entity_type, entity_key],
    )
    completion_name = _normalize_text((completion or {}).get("display_name"))
    if completion_name:
        return completion_name

    if entity_type == "network":
        row = pg.fetch_one(
            """
            select name
            from core.networks
            where lower(btrim(name)) = %s
            order by id asc
            limit 1
            """,
            [entity_key],
        )
        if row and _normalize_text(row.get("name")):
            return _normalize_text(row.get("name"))

    row = pg.fetch_one(
        """
        select provider_name
        from core.watch_providers
        where lower(btrim(provider_name)) = %s
        order by provider_id asc
        limit 1
        """,
        [entity_key],
    )
    if row and _normalize_text(row.get("provider_name")):
        return _normalize_text(row.get("provider_name"))

    return entity_key


def list_families(*, active_only: bool = True) -> dict[str, Any]:
    clauses = []
    params: list[Any] = []
    if active_only:
        clauses.append("is_active = true")

    where_sql = f"where {' and '.join(clauses)}" if clauses else ""
    rows = pg.fetch_all(
        f"""
        select *
        from admin.brand_families
        {where_sql}
        order by updated_at desc, display_name asc
        """,
        params,
    )
    family_ids = [str(row.get("id") or "") for row in rows if str(row.get("id") or "")]
    members_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if family_ids:
        member_rows = pg.fetch_all(
            """
            select *
            from admin.brand_family_members
            where family_id = any(%s::uuid[])
            order by entity_type asc, entity_display_name asc, created_at asc
            """,
            [family_ids],
        )
        for member in member_rows:
            family_id = str(member.get("family_id") or "")
            if family_id:
                members_by_family[family_id].append(_member_row_to_api(member))

    payload_rows = []
    for row in rows:
        family_id = str(row.get("id") or "")
        payload = _family_row_to_api(row, members=members_by_family.get(family_id, []))
        payload["member_count"] = len(payload.get("members") or [])
        payload_rows.append(payload)
    return {
        "rows": payload_rows,
        "count": len(payload_rows),
    }


def create_family(*, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    display_name = _normalize_text(payload.get("display_name"))
    if not display_name:
        raise ValueError("display_name is required")
    family_key = _slugify(_normalize_text(payload.get("family_key")) or display_name)
    if not family_key:
        raise ValueError("family_key is required")

    rows = pg.execute_returning(
        """
        insert into admin.brand_families (
          family_key,
          display_name,
          owner_wikidata_id,
          owner_label,
          is_active,
          notes,
          metadata,
          created_by,
          updated_by
        )
        values (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
        returning *
        """,
        [
            family_key,
            display_name,
            _normalize_text(payload.get("owner_wikidata_id")) or None,
            _normalize_text(payload.get("owner_label")) or None,
            bool(payload.get("is_active", True)),
            _normalize_text(payload.get("notes")) or None,
            json.dumps(payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}),
            actor,
            actor,
        ],
    )
    if not rows:
        raise RuntimeError("Failed to create brand family")
    row = rows[0]
    return _family_row_to_api(row, members=[])


def patch_family(*, family_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    _fetch_family_row(family_id)

    updates: list[str] = ["updated_by = %s", "updated_at = now()"]
    params: list[Any] = [actor]

    if "display_name" in payload:
        display_name = _normalize_text(payload.get("display_name"))
        if not display_name:
            raise ValueError("display_name cannot be empty")
        updates.append("display_name = %s")
        params.append(display_name)

    if "owner_wikidata_id" in payload:
        updates.append("owner_wikidata_id = %s")
        params.append(_normalize_text(payload.get("owner_wikidata_id")) or None)

    if "owner_label" in payload:
        updates.append("owner_label = %s")
        params.append(_normalize_text(payload.get("owner_label")) or None)

    if "is_active" in payload:
        updates.append("is_active = %s")
        params.append(bool(payload.get("is_active")))

    if "notes" in payload:
        updates.append("notes = %s")
        params.append(_normalize_text(payload.get("notes")) or None)

    if "metadata" in payload:
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        updates.append("metadata = %s::jsonb")
        params.append(json.dumps(metadata))

    if len(updates) <= 2:
        row = _fetch_family_row(family_id)
        return _family_row_to_api(row, members=_fetch_family_members(family_id))

    params.append(family_id)
    rows = pg.execute_returning(
        f"""
        update admin.brand_families
        set {", ".join(updates)}
        where id = %s::uuid
        returning *
        """,
        params,
    )
    if not rows:
        raise KeyError("Brand family not found")
    row = rows[0]
    return _family_row_to_api(row, members=_fetch_family_members(family_id))


def add_family_member(*, family_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    _fetch_family_row(family_id)

    entity_type = _normalize_entity_type(_normalize_text(payload.get("entity_type")))
    entity_key = _normalize_entity_key(_normalize_text(payload.get("entity_key")))
    if not entity_key:
        raise ValueError("entity_key is required")

    conflicting = pg.fetch_one(
        """
        select m.id, f.id as family_id, f.display_name
        from admin.brand_family_members m
        join admin.brand_families f on f.id = m.family_id
        where m.entity_type = %s
          and m.entity_key = %s
          and f.is_active = true
          and m.family_id <> %s::uuid
        limit 1
        """,
        [entity_type, entity_key, family_id],
    )
    if conflicting:
        conflict_family = str(conflicting.get("display_name") or conflicting.get("family_id"))
        raise ValueError(f"Entity already belongs to active family {conflict_family}")

    entity_display_name = _normalize_text(payload.get("entity_display_name")) or _resolve_entity_display_name(
        entity_type,
        entity_key,
    )
    source = _normalize_text(payload.get("source")).lower() or "manual"
    if source not in {"manual", "suggested_owner", "system"}:
        source = "manual"

    confidence_raw = payload.get("confidence")
    confidence = None
    if confidence_raw is not None and str(confidence_raw).strip() != "":
        confidence = max(0.0, min(1.0, float(confidence_raw)))

    rows = pg.execute_returning(
        """
        insert into admin.brand_family_members (
          family_id,
          entity_type,
          entity_key,
          entity_display_name,
          source,
          confidence,
          metadata,
          created_by,
          updated_by
        )
        values (%s::uuid, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
        on conflict (family_id, entity_type, entity_key)
        do update
        set
          entity_display_name = excluded.entity_display_name,
          source = excluded.source,
          confidence = excluded.confidence,
          metadata = coalesce(admin.brand_family_members.metadata, '{}'::jsonb) || excluded.metadata,
          updated_by = excluded.updated_by,
          updated_at = now()
        returning *
        """,
        [
            family_id,
            entity_type,
            entity_key,
            entity_display_name,
            source,
            confidence,
            json.dumps(payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}),
            actor,
            actor,
        ],
    )
    if not rows:
        raise RuntimeError("Failed to add family member")
    return _member_row_to_api(rows[0])


def delete_family_member(*, family_id: str, member_id: str) -> dict[str, Any]:
    rows = pg.execute_returning(
        """
        delete from admin.brand_family_members
        where id = %s::uuid
          and family_id = %s::uuid
        returning id
        """,
        [member_id, family_id],
    )
    if not rows:
        raise KeyError("Family member not found")
    return {"deleted": True, "id": str(rows[0].get("id") or member_id)}


def get_family_by_entity(*, entity_type: str, entity_key: str) -> dict[str, Any] | None:
    normalized_type = _normalize_entity_type(entity_type)
    normalized_key = _normalize_entity_key(entity_key)
    if not normalized_key:
        raise ValueError("entity_key is required")

    row = pg.fetch_one(
        """
        select f.*
        from admin.brand_family_members m
        join admin.brand_families f on f.id = m.family_id
        where m.entity_type = %s
          and m.entity_key = %s
        order by f.is_active desc, m.updated_at desc
        limit 1
        """,
        [normalized_type, normalized_key],
    )
    if not row:
        return None
    family_id = str(row.get("id") or "")
    return _family_row_to_api(row, members=_fetch_family_members(family_id))


def list_family_links(*, family_id: str, active_only: bool = False) -> dict[str, Any]:
    _fetch_family_row(family_id)
    clauses = ["family_id = %s::uuid"]
    params: list[Any] = [family_id]
    if active_only:
        clauses.append("is_active = true")

    rows = pg.fetch_all(
        f"""
        select *
        from admin.brand_family_link_rules
        where {" and ".join(clauses)}
        order by priority asc, updated_at desc, created_at desc
        """,
        params,
    )
    payload_rows = [_link_rule_row_to_api(row) for row in rows]
    return {"rows": payload_rows, "count": len(payload_rows)}


def create_family_link_rule(*, family_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    _fetch_family_row(family_id)

    link_group = _normalize_link_group(_normalize_text(payload.get("link_group") or "other"))
    link_kind = _normalize_link_kind(_normalize_text(payload.get("link_kind") or "external"))
    coverage_type = _normalize_coverage_type(_normalize_text(payload.get("coverage_type") or "family_all_shows"))
    coverage_value = _normalize_text(payload.get("coverage_value")) or None
    source = _normalize_text(payload.get("source")).lower() or "manual"
    if source not in {"manual", "wikipedia_import", "system"}:
        source = "manual"

    url = _canonicalize_url(_normalize_text(payload.get("url")))
    if not url:
        raise ValueError("url is required")

    rows = pg.execute_returning(
        """
        insert into admin.brand_family_link_rules (
          family_id,
          link_group,
          link_kind,
          label,
          url,
          url_key,
          coverage_type,
          coverage_value,
          source,
          priority,
          auto_apply,
          is_active,
          metadata,
          created_by,
          updated_by
        )
        values (
          %s::uuid,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb,
          %s,
          %s
        )
        on conflict (family_id, link_kind, coverage_type, coalesce(coverage_value, ''), url_key)
        do update
        set
          link_group = excluded.link_group,
          label = excluded.label,
          source = excluded.source,
          priority = excluded.priority,
          auto_apply = excluded.auto_apply,
          is_active = excluded.is_active,
          metadata = coalesce(admin.brand_family_link_rules.metadata, '{}'::jsonb) || excluded.metadata,
          updated_by = excluded.updated_by,
          updated_at = now()
        returning *
        """,
        [
            family_id,
            link_group,
            link_kind,
            _normalize_text(payload.get("label")) or None,
            url,
            _url_key(url),
            coverage_type,
            coverage_value,
            source,
            max(0, int(payload.get("priority") or 100)),
            bool(payload.get("auto_apply", True)),
            bool(payload.get("is_active", True)),
            json.dumps(payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}),
            actor,
            actor,
        ],
    )
    if not rows:
        raise RuntimeError("Failed to upsert family link rule")
    return _link_rule_row_to_api(rows[0])


def patch_family_link_rule(*, family_id: str, rule_id: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
    _fetch_family_row(family_id)
    existing = pg.fetch_one(
        """
        select *
        from admin.brand_family_link_rules
        where id = %s::uuid
          and family_id = %s::uuid
        limit 1
        """,
        [rule_id, family_id],
    )
    if not existing:
        raise KeyError("Family link rule not found")

    updates: list[str] = ["updated_by = %s", "updated_at = now()"]
    params: list[Any] = [actor]

    if "link_group" in payload:
        updates.append("link_group = %s")
        params.append(_normalize_link_group(_normalize_text(payload.get("link_group"))))

    if "link_kind" in payload:
        updates.append("link_kind = %s")
        params.append(_normalize_link_kind(_normalize_text(payload.get("link_kind"))))

    if "label" in payload:
        updates.append("label = %s")
        params.append(_normalize_text(payload.get("label")) or None)

    if "url" in payload:
        url = _canonicalize_url(_normalize_text(payload.get("url")))
        if not url:
            raise ValueError("url cannot be empty")
        updates.append("url = %s")
        params.append(url)
        updates.append("url_key = %s")
        params.append(_url_key(url))

    if "coverage_type" in payload:
        updates.append("coverage_type = %s")
        params.append(_normalize_coverage_type(_normalize_text(payload.get("coverage_type"))))

    if "coverage_value" in payload:
        updates.append("coverage_value = %s")
        params.append(_normalize_text(payload.get("coverage_value")) or None)

    if "source" in payload:
        source = _normalize_text(payload.get("source")).lower() or "manual"
        if source not in {"manual", "wikipedia_import", "system"}:
            raise ValueError("source is invalid")
        updates.append("source = %s")
        params.append(source)

    if "priority" in payload:
        updates.append("priority = %s")
        params.append(max(0, int(payload.get("priority") or 0)))

    if "auto_apply" in payload:
        updates.append("auto_apply = %s")
        params.append(bool(payload.get("auto_apply")))

    if "is_active" in payload:
        updates.append("is_active = %s")
        params.append(bool(payload.get("is_active")))

    if "metadata" in payload:
        updates.append("metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb")
        params.append(json.dumps(payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}))

    if len(updates) <= 2:
        return _link_rule_row_to_api(existing)

    params.extend([rule_id, family_id])
    rows = pg.execute_returning(
        f"""
        update admin.brand_family_link_rules
        set {", ".join(updates)}
        where id = %s::uuid
          and family_id = %s::uuid
        returning *
        """,
        params,
    )
    if not rows:
        raise KeyError("Family link rule not found")
    return _link_rule_row_to_api(rows[0])


def cleanup_imported_family_link_rules(*, family_id: str | None = None) -> dict[str, Any]:
    clauses = ["source = 'wikipedia_import'"]
    params: list[Any] = []

    if family_id:
        _fetch_family_row(family_id)
        clauses.append("family_id = %s::uuid")
        params.append(family_id)

    deleted = pg.execute_returning(
        f"""
        delete from admin.brand_family_link_rules
        where {" and ".join(clauses)}
        returning id
        """,
        params,
    )
    return {
        "family_id": family_id,
        "deleted_count": len(deleted),
    }


def list_family_suggestions() -> dict[str, Any]:
    rows = pg.fetch_all(
        """
        with grouped as (
          select
            c.owner_wikidata_id,
            nullif(btrim(c.owner_label), '') as owner_label,
            c.entity_type,
            c.entity_key,
            c.display_name,
            c.updated_at
          from admin.network_streaming_completion c
          where nullif(btrim(c.owner_wikidata_id), '') is not null
        ),
        unassigned as (
          select g.*
          from grouped g
          left join admin.brand_family_members m
            on m.entity_type = g.entity_type
           and m.entity_key = g.entity_key
          where m.id is null
        )
        select
          owner_wikidata_id,
          coalesce(owner_label, owner_wikidata_id) as owner_label,
          json_agg(
            json_build_object(
              'entity_type', entity_type,
              'entity_key', entity_key,
              'display_name', display_name,
              'updated_at', updated_at
            )
            order by entity_type asc, display_name asc
          ) as entities,
          count(*)::int as entity_count
        from unassigned
        group by owner_wikidata_id, coalesce(owner_label, owner_wikidata_id)
        having count(*) > 1
        order by entity_count desc, owner_label asc
        """,
    )
    suggestions = []
    for row in rows:
        entities = row.get("entities") if isinstance(row.get("entities"), list) else []
        suggestions.append(
            {
                "owner_wikidata_id": str(row.get("owner_wikidata_id") or ""),
                "owner_label": str(row.get("owner_label") or ""),
                "entity_count": int(row.get("entity_count") or 0),
                "entities": entities,
            }
        )
    return {"rows": suggestions, "count": len(suggestions)}


def _load_family_member_keys(family_id: str) -> tuple[set[str], set[str]]:
    rows = pg.fetch_all(
        """
        select entity_type, entity_key
        from admin.brand_family_members
        where family_id = %s::uuid
        """,
        [family_id],
    )
    network_keys: set[str] = set()
    streaming_keys: set[str] = set()
    for row in rows:
        entity_type = str(row.get("entity_type") or "").strip().lower()
        entity_key = _normalize_entity_key(row.get("entity_key") or "")
        if not entity_key:
            continue
        if entity_type == "network":
            network_keys.add(entity_key)
        elif entity_type == "streaming":
            streaming_keys.add(entity_key)
    return network_keys, streaming_keys


def _resolve_network_show_ids(network_keys: set[str]) -> set[str]:
    if not network_keys:
        return set()
    rows = pg.fetch_all(
        """
        select distinct s.id::text as show_id
        from core.shows s
        cross join lateral unnest(coalesce(s.networks, array[]::text[])) as n(name)
        where lower(btrim(n.name)) = any(%s::text[])
        """,
        [sorted(network_keys)],
    )
    return {str(row.get("show_id") or "") for row in rows if str(row.get("show_id") or "")}


def _resolve_streaming_show_ids(streaming_keys: set[str]) -> set[str]:
    if not streaming_keys:
        return set()
    rows = pg.fetch_all(
        """
        with primary_provider as (
          select distinct swp.show_id::text as show_id
          from core.show_watch_providers swp
          join core.watch_providers wp on wp.provider_id = swp.provider_id
          where lower(btrim(wp.provider_name)) = any(%s::text[])
        ),
        fallback_provider as (
          select distinct s.id::text as show_id
          from core.shows s
          cross join lateral unnest(coalesce(s.streaming_providers, array[]::text[])) as p(name)
          where lower(btrim(p.name)) = any(%s::text[])
        )
        select show_id from primary_provider
        union
        select show_id from fallback_provider
        """,
        [sorted(streaming_keys), sorted(streaming_keys)],
    )
    return {str(row.get("show_id") or "") for row in rows if str(row.get("show_id") or "")}


def _resolve_rule_show_ids(*, family_id: str, rule: dict[str, Any]) -> set[str]:
    coverage_type = str(rule.get("coverage_type") or "family_all_shows")
    coverage_value = _normalize_text(rule.get("coverage_value"))
    network_keys, streaming_keys = _load_family_member_keys(family_id)

    if coverage_type == "family_network_shows":
        return _resolve_network_show_ids(network_keys)
    if coverage_type == "family_streaming_shows":
        return _resolve_streaming_show_ids(streaming_keys)
    if coverage_type == "family_all_shows":
        return _resolve_network_show_ids(network_keys) | _resolve_streaming_show_ids(streaming_keys)

    if coverage_type == "franchise_rule":
        key = _slugify(coverage_value)
        if not key:
            return set()
        rows = brands_franchises.list_shows_franchises(q="", limit=10000).get("rows", [])
        show_ids: set[str] = set()
        for row in rows:
            if _slugify(row.get("franchise_key") or "") == key:
                show_id = _normalize_text(row.get("show_id"))
                if show_id:
                    show_ids.add(show_id)
        return show_ids

    if coverage_type == "show_wikidata_exact":
        wikidata = _normalize_text(coverage_value).upper()
        if not wikidata:
            return set()
        rows = pg.fetch_all(
            """
            select id::text as show_id
            from core.shows
            where upper(btrim(coalesce(wikidata_id, ''))) = %s
            """,
            [wikidata],
        )
        return {str(row.get("show_id") or "") for row in rows if str(row.get("show_id") or "")}

    if coverage_type == "show_name_contains":
        needle = _normalize_text(coverage_value)
        if not needle:
            return set()
        rows = pg.fetch_all(
            """
            select id::text as show_id
            from core.shows
            where lower(name) like %s
            """,
            [f"%{needle.lower()}%"],
        )
        return {str(row.get("show_id") or "") for row in rows if str(row.get("show_id") or "")}

    return set()


def _show_has_non_family_link_kind(show_id: str, *, link_kind: str) -> bool:
    row = pg.fetch_one(
        """
        select id
        from core.entity_links
        where show_id = %s::uuid
          and entity_type = 'show'
          and entity_id = %s::uuid
          and link_kind = %s
          and lower(status) = 'approved'
          and coalesce(source, '') not like 'brand_family_rule:%%'
        limit 1
        """,
        [show_id, show_id, link_kind],
    )
    return bool(row)


def _update_existing_family_rule_link(*, show_id: str, rule: dict[str, Any], source_value: str, actor: str) -> int:
    metadata = {
        "family_id": str(rule.get("family_id") or ""),
        "rule_id": str(rule.get("id") or ""),
        "coverage_type": str(rule.get("coverage_type") or ""),
        "coverage_value": str(rule.get("coverage_value") or "") or None,
        "applied_by": "brand_family_rule",
        "applied_at": _iso_now(),
    }
    rows = pg.execute_returning(
        """
        update core.entity_links
        set
          link_group = %s,
          label = %s,
          url = %s,
          url_key = %s,
          status = 'approved',
          confidence = 0.70,
          discovered_by = 'admin.brand_families',
          source = %s,
          metadata = coalesce(core.entity_links.metadata, '{}'::jsonb) || %s::jsonb,
          updated_by = %s,
          updated_at = now()
        where show_id = %s::uuid
          and entity_type = 'show'
          and entity_id = %s::uuid
          and link_kind = %s
          and source = %s
        returning id
        """,
        [
            str(rule.get("link_group") or "other"),
            _normalize_text(rule.get("label")) or None,
            _canonicalize_url(str(rule.get("url") or "")),
            _url_key(str(rule.get("url") or "")),
            source_value,
            json.dumps(metadata),
            actor,
            show_id,
            show_id,
            str(rule.get("link_kind") or ""),
            source_value,
        ],
    )
    return len(rows)


def _upsert_family_rule_link(*, show_id: str, rule: dict[str, Any], source_value: str, actor: str) -> int:
    metadata = {
        "family_id": str(rule.get("family_id") or ""),
        "rule_id": str(rule.get("id") or ""),
        "coverage_type": str(rule.get("coverage_type") or ""),
        "coverage_value": str(rule.get("coverage_value") or "") or None,
        "applied_by": "brand_family_rule",
        "applied_at": _iso_now(),
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
          %s,
          %s,
          %s,
          %s,
          %s,
          'approved',
          0.70,
          'admin.brand_families',
          %s,
          %s::jsonb,
          %s,
          %s
        )
        on conflict (show_id, entity_type, entity_id, link_kind, season_number, url_key)
        do update
        set
          link_group = excluded.link_group,
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
            show_id,
            show_id,
            str(rule.get("link_group") or "other"),
            str(rule.get("link_kind") or "external"),
            _normalize_text(rule.get("label")) or None,
            _canonicalize_url(str(rule.get("url") or "")),
            _url_key(str(rule.get("url") or "")),
            source_value,
            json.dumps(metadata),
            actor,
            actor,
        ],
    )
    return len(rows)


def apply_family_links(
    *,
    family_id: str,
    dry_run: bool,
    actor: str,
    rule_ids: list[str] | None = None,
) -> dict[str, Any]:
    _fetch_family_row(family_id)

    clauses = ["family_id = %s::uuid", "is_active = true"]
    params: list[Any] = [family_id]
    if rule_ids:
        clauses.append("id = any(%s::uuid[])")
        params.append(rule_ids)

    rule_rows = pg.fetch_all(
        f"""
        select *
        from admin.brand_family_link_rules
        where {" and ".join(clauses)}
        order by priority asc, updated_at desc
        """,
        params,
    )

    matched_show_count = 0
    applied_show_count = 0
    skipped_existing_manual = 0
    updated_derived_count = 0
    errors: list[dict[str, Any]] = []
    applied_entries: list[dict[str, Any]] = []

    for row in rule_rows:
        rule = _link_rule_row_to_api(row)
        source_value = f"brand_family_rule:{rule['id']}"
        show_ids = _resolve_rule_show_ids(family_id=family_id, rule=rule)
        matched_show_count += len(show_ids)

        for show_id in sorted(show_ids):
            try:
                if _show_has_non_family_link_kind(show_id, link_kind=rule["link_kind"]):
                    skipped_existing_manual += 1
                    continue

                if dry_run:
                    applied_show_count += 1
                    applied_entries.append(
                        {
                            "show_id": show_id,
                            "rule_id": rule["id"],
                            "link_kind": rule["link_kind"],
                            "url": rule["url"],
                            "action": "would_apply",
                        }
                    )
                    continue

                updated_existing = _update_existing_family_rule_link(
                    show_id=show_id,
                    rule=rule,
                    source_value=source_value,
                    actor=actor,
                )
                if updated_existing > 0:
                    updated_derived_count += updated_existing
                    applied_show_count += 1
                    applied_entries.append(
                        {
                            "show_id": show_id,
                            "rule_id": rule["id"],
                            "link_kind": rule["link_kind"],
                            "url": rule["url"],
                            "action": "updated_derived",
                        }
                    )
                    continue

                inserted = _upsert_family_rule_link(show_id=show_id, rule=rule, source_value=source_value, actor=actor)
                if inserted > 0:
                    applied_show_count += 1
                    applied_entries.append(
                        {
                            "show_id": show_id,
                            "rule_id": rule["id"],
                            "link_kind": rule["link_kind"],
                            "url": rule["url"],
                            "action": "inserted",
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    {
                        "rule_id": rule["id"],
                        "show_id": show_id,
                        "error": str(exc),
                    }
                )

    return {
        "family_id": family_id,
        "rule_count": len(rule_rows),
        "matched_show_count": matched_show_count,
        "applied_show_count": applied_show_count,
        "skipped_existing_manual": skipped_existing_manual,
        "updated_derived_count": updated_derived_count,
        "dry_run": dry_run,
        "errors": errors,
        "applied": applied_entries,
    }


def _extract_wikipedia_title_from_url(url: str) -> str | None:
    parsed = urlparse(_canonicalize_url(url))
    if not parsed.hostname or not parsed.hostname.endswith("wikipedia.org"):
        return None
    if "/wiki/" not in parsed.path:
        return None
    slug = unquote(parsed.path.split("/wiki/", 1)[1]).strip()
    if not slug:
        return None
    title = slug.split("#", 1)[0].split("?", 1)[0]
    if not title:
        return None
    lower_title = title.replace("_", " ").strip().lower()
    if any(lower_title.startswith(prefix) for prefix in _BLOCKED_WIKIPEDIA_PREFIXES):
        return None
    return title


def _wiki_api_wikidata_id(url: str) -> str | None:
    canonical_url = _canonicalize_url(url)
    parsed = urlparse(canonical_url)
    hostname = str(parsed.hostname or "").strip().lower()
    title = _extract_wikipedia_title_from_url(canonical_url)
    if not hostname or not title:
        return None
    api_url = f"{parsed.scheme or 'https'}://{hostname}/w/api.php"
    try:
        response = requests.get(
            api_url,
            params={
                "action": "query",
                "prop": "pageprops",
                "titles": title,
                "format": "json",
                "redirects": 1,
            },
            timeout=(5, 20),
            headers={"user-agent": "TRR-Backend/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:  # noqa: BLE001
        return None

    pages = ((payload or {}).get("query") or {}).get("pages")
    if not isinstance(pages, dict):
        return None
    for page in pages.values():
        if not isinstance(page, dict):
            continue
        pageprops = page.get("pageprops") if isinstance(page.get("pageprops"), dict) else {}
        wikibase_item = _normalize_text(pageprops.get("wikibase_item"))
        if wikibase_item:
            return wikibase_item.upper()
    return None


def _extract_wikipedia_show_urls_from_html(*, html: str, page_url: str) -> list[dict[str, str]]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    parsed_page = urlparse(_canonicalize_url(page_url))

    def add_link(url: str, *, out: dict[str, dict[str, str]]) -> None:
        canonical = _canonicalize_url(url)
        parsed = urlparse(canonical)
        if not parsed.hostname or not parsed.hostname.endswith("wikipedia.org"):
            return
        title = _extract_wikipedia_title_from_url(canonical)
        if not title:
            return
        key = _url_key(canonical)
        if key in out:
            return
        out[key] = {
            "show_url": canonical,
            "show_title": title.replace("_", " "),
            "show_url_key": key,
        }

    scoped_links: dict[str, dict[str, str]] = {}
    for heading in soup.select("h2, h3, h4"):
        heading_text = _normalize_text(heading.get_text(" ", strip=True)).lower()
        if not any(hint in heading_text for hint in _SECTION_HINTS):
            continue
        cursor = heading.find_next_sibling()
        while cursor is not None and cursor.name not in {"h2", "h3", "h4"}:
            for anchor in cursor.select("a[href]"):
                href = _normalize_text(anchor.get("href"))
                if not href:
                    continue
                add_link(urljoin(f"{parsed_page.scheme or 'https'}://{parsed_page.hostname}", href), out=scoped_links)
            cursor = cursor.find_next_sibling()

    if scoped_links:
        return list(scoped_links.values())

    all_links: dict[str, dict[str, str]] = {}
    for anchor in soup.select("a[href]"):
        href = _normalize_text(anchor.get("href"))
        if not href:
            continue
        add_link(urljoin(f"{parsed_page.scheme or 'https'}://{parsed_page.hostname}", href), out=all_links)
    return list(all_links.values())


def _match_show_for_wikipedia_url(show_url: str, *, wikidata_id: str | None) -> tuple[str | None, str | None]:
    if wikidata_id:
        row = pg.fetch_one(
            """
            select id::text as show_id
            from core.shows
            where upper(btrim(coalesce(wikidata_id, ''))) = %s
            order by id asc
            limit 1
            """,
            [wikidata_id.upper()],
        )
        if row and _normalize_text(row.get("show_id")):
            return _normalize_text(row.get("show_id")), "show_wikidata_id"

    row = pg.fetch_one(
        """
        select show_id::text as show_id
        from core.entity_links
        where entity_type = 'show'
          and lower(link_kind) = 'wikipedia'
          and lower(status) = 'approved'
          and url_key = %s
        order by updated_at desc nulls last, created_at desc
        limit 1
        """,
        [_url_key(show_url)],
    )
    if row and _normalize_text(row.get("show_id")):
        return _normalize_text(row.get("show_id")), "existing_show_wikipedia_link"

    return None, None


def _resolve_entity_wikipedia_url(entity_type: EntityType, entity_key: str) -> str | None:
    completion = pg.fetch_one(
        """
        select wikipedia_url
        from admin.network_streaming_completion
        where entity_type = %s
          and entity_key = %s
        order by updated_at desc
        limit 1
        """,
        [entity_type, entity_key],
    )
    candidate = _canonicalize_url(_normalize_text((completion or {}).get("wikipedia_url")))
    if candidate:
        return candidate

    if entity_type == "network":
        row = pg.fetch_one(
            """
            select wikipedia_url
            from core.networks
            where lower(btrim(name)) = %s
            order by id asc
            limit 1
            """,
            [entity_key],
        )
    else:
        row = pg.fetch_one(
            """
            select wikipedia_url
            from core.watch_providers
            where lower(btrim(provider_name)) = %s
            order by provider_id asc
            limit 1
            """,
            [entity_key],
        )
    return _canonicalize_url(_normalize_text((row or {}).get("wikipedia_url"))) or None


def _upsert_wikipedia_show_link(
    *,
    family_id: str,
    entity_type: EntityType,
    entity_key: str,
    brand_wikipedia_url: str,
    show_url: str,
    show_title: str | None,
    wikidata_id: str | None,
    matched_show_id: str | None,
    match_method: str | None,
    import_source: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = pg.execute_returning(
        """
        insert into admin.brand_family_wikipedia_show_links (
          family_id,
          entity_type,
          entity_key,
          brand_wikipedia_url,
          show_url,
          show_url_key,
          show_title,
          wikidata_id,
          matched_show_id,
          match_method,
          import_source,
          is_applied,
          metadata,
          last_seen_at,
          updated_at
        )
        values (
          %s::uuid,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::uuid,
          %s,
          %s,
          false,
          %s::jsonb,
          now(),
          now()
        )
        on conflict (entity_type, entity_key, show_url_key)
        do update
        set
          family_id = excluded.family_id,
          brand_wikipedia_url = excluded.brand_wikipedia_url,
          show_url = excluded.show_url,
          show_title = excluded.show_title,
          wikidata_id = excluded.wikidata_id,
          matched_show_id = excluded.matched_show_id,
          match_method = excluded.match_method,
          import_source = excluded.import_source,
          metadata = coalesce(admin.brand_family_wikipedia_show_links.metadata, '{}'::jsonb) || excluded.metadata,
          last_seen_at = now(),
          updated_at = now()
        returning *
        """,
        [
            family_id,
            entity_type,
            entity_key,
            brand_wikipedia_url,
            _canonicalize_url(show_url),
            _url_key(show_url),
            _normalize_text(show_title) or None,
            _normalize_text(wikidata_id).upper() or None,
            matched_show_id,
            _normalize_text(match_method) or None,
            _normalize_text(import_source) or "manual",
            json.dumps(metadata or {}),
        ],
    )
    if not rows:
        raise RuntimeError("Failed to upsert wikipedia show link")
    return rows[0]


def import_family_wikipedia_show_links(
    *,
    family_id: str,
    actor: str,
    entity_type: str | None = None,
    entity_key: str | None = None,
    apply_matched: bool = False,
    import_source: str = "manual",
) -> dict[str, Any]:
    _fetch_family_row(family_id)

    if bool(entity_type) ^ bool(entity_key):
        raise ValueError("entity_type and entity_key must be provided together")

    members = _fetch_family_members(family_id)
    scoped_members = members
    if entity_type and entity_key:
        normalized_type = _normalize_entity_type(entity_type)
        normalized_key = _normalize_entity_key(entity_key)
        scoped_members = [
            member
            for member in members
            if (
                member.get("entity_type") == normalized_type
                and _normalize_entity_key(member.get("entity_key") or "") == normalized_key
            )
        ]
    imported_rows: list[dict[str, Any]] = []
    matched_count = 0
    fetch_errors: list[dict[str, Any]] = []

    for member in scoped_members:
        member_entity_type = _normalize_entity_type(str(member.get("entity_type") or ""))
        member_entity_key = _normalize_entity_key(member.get("entity_key") or "")
        wikipedia_url = _resolve_entity_wikipedia_url(member_entity_type, member_entity_key)
        if not wikipedia_url:
            continue

        try:
            response = requests.get(
                wikipedia_url,
                timeout=(5, 20),
                headers={"user-agent": "TRR-Backend/1.0"},
            )
            response.raise_for_status()
            html = response.text
        except Exception as exc:  # noqa: BLE001
            fetch_errors.append(
                {
                    "entity_type": member_entity_type,
                    "entity_key": member_entity_key,
                    "wikipedia_url": wikipedia_url,
                    "error": str(exc),
                }
            )
            continue

        candidates = _extract_wikipedia_show_urls_from_html(html=html, page_url=wikipedia_url)
        for candidate in candidates:
            show_url = str(candidate.get("show_url") or "")
            show_title = str(candidate.get("show_title") or "") or None
            wikidata_id = _wiki_api_wikidata_id(show_url)
            matched_show_id, match_method = _match_show_for_wikipedia_url(show_url, wikidata_id=wikidata_id)
            if matched_show_id:
                matched_count += 1

            row = _upsert_wikipedia_show_link(
                family_id=family_id,
                entity_type=member_entity_type,
                entity_key=member_entity_key,
                brand_wikipedia_url=wikipedia_url,
                show_url=show_url,
                show_title=show_title,
                wikidata_id=wikidata_id,
                matched_show_id=matched_show_id,
                match_method=match_method,
                import_source=import_source,
                metadata={
                    "imported_by": actor,
                },
            )
            imported_rows.append(_wiki_link_row_to_api(row))

    return {
        "family_id": family_id,
        "entity_scope_count": len(scoped_members),
        "imported_count": len(imported_rows),
        "matched_count": matched_count,
        "rules_upserted": 0,
        "apply_matched": apply_matched,
        "apply_result": None,
        "fetch_errors": fetch_errors,
        "rows": imported_rows,
    }


def list_family_wikipedia_show_links(*, family_id: str, limit: int = 500) -> dict[str, Any]:
    _fetch_family_row(family_id)
    rows = pg.fetch_all(
        """
        select *
        from admin.brand_family_wikipedia_show_links
        where family_id = %s::uuid
        order by updated_at desc, created_at desc
        limit %s
        """,
        [family_id, max(1, min(limit, 5000))],
    )
    payload_rows = [_wiki_link_row_to_api(row) for row in rows]
    return {"rows": payload_rows, "count": len(payload_rows)}


def enrich_owner_suggestions_from_completion_row(
    *,
    entity_type: str,
    entity_key: str,
    owner_wikidata_id: str | None,
    owner_label: str | None,
) -> None:
    """Helper intended for sync pipeline hooks where owner fields are known."""
    normalized_type = _normalize_entity_type(entity_type)
    normalized_key = _normalize_entity_key(entity_key)
    if not normalized_key:
        return
    pg.execute_returning(
        """
        update admin.network_streaming_completion
        set owner_wikidata_id = %s,
            owner_label = %s,
            updated_at = now()
        where entity_type = %s
          and entity_key = %s
        returning id
        """,
        [
            _normalize_text(owner_wikidata_id).upper() or None,
            _normalize_text(owner_label) or None,
            normalized_type,
            normalized_key,
        ],
    )
