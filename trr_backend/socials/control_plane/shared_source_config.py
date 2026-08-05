"""Canonical persistence boundary for shared social account sources."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from trr_backend.db import pg
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS
from trr_backend.socials.provider_registry import register_legacy_patchable_namespace
from trr_backend.socials.source_scopes import normalize_source_scope, normalize_source_scope_input

_GENERIC_ACCOUNT_HANDLE_RE = re.compile(r"^[a-z0-9._-]{1,64}$")
_SHARED_PROFILE_KIND_BY_SCOPE = {
    "network": "network_streaming",
    "creator": "creator",
    "community": "community",
    "news": "news",
}
_SHARED_PROFILE_ASSIGNMENT_MODE_BY_KIND = {
    "network_streaming": "multi_show_match",
    "creator": "creator_match",
    "community": "community_match",
    "news": "news_match",
}
_DEFAULT_PLATFORM_ACCOUNTS: dict[str, list[str]] = {
    "facebook": ["bravo"],
    "instagram": ["bravotv", "bravodailydish", "bravowwhl"],
    "tiktok": ["bravotv", "bravowwhl"],
    "threads": ["bravotv", "bravodailydish", "bravowwhl"],
    "twitter": ["bravotv", "bravowwhl"],
    "youtube": ["bravo"],
}


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _json_serializer(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _normalize_platform_name(value: Any) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()
    tokens = [token for token in normalized.split() if token]
    if not tokens:
        return ""
    aliases = {"x": "twitter", "ig": "instagram", "insta": "instagram", "fb": "facebook", "meta": "facebook"}
    if tokens[0] == "meta" and len(tokens) > 1 and tokens[1] == "threads":
        return "threads"
    return aliases.get(tokens[0], tokens[0])


def _resolve_requested_platforms(platforms: list[str] | None) -> list[str]:
    if platforms is None:
        return list(SOCIAL_SUPPORTED_PLATFORMS)
    normalized = [_normalize_platform_name(platform) for platform in platforms]
    normalized = [platform for platform in normalized if platform]
    invalid = sorted({platform for platform in normalized if platform not in SOCIAL_SUPPORTED_PLATFORMS})
    if invalid:
        raise ValueError(f"INVALID_PLATFORM_FILTER: {', '.join(invalid)}")
    deduped = list(dict.fromkeys(normalized))
    if not deduped:
        raise ValueError("INVALID_PLATFORM_FILTER: no valid platforms requested")
    return deduped


def _normalize_account_handle(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    candidate = raw
    if "://" in raw or raw.lower().startswith("www."):
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        path_parts = [segment for segment in str(parsed.path or "").split("/") if segment]
        candidate = path_parts[0] if path_parts else str(parsed.netloc or "")
    candidate = candidate.strip().lstrip("@").split("?")[0].split("#")[0].split("/")[0].strip().lower()
    return candidate if candidate and _GENERIC_ACCOUNT_HANDLE_RE.fullmatch(candidate) else ""


def _shared_profile_contract(
    *,
    source_scope: str,
    platform: str,
    account_handle: str,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source_scope = normalize_source_scope_input(source_scope)
    metadata_dict = _metadata_dict(metadata)
    profile_kind = str(metadata_dict.get("profile_kind") or "").strip().lower()
    if not profile_kind:
        profile_kind = _SHARED_PROFILE_KIND_BY_SCOPE.get(normalize_source_scope(source_scope), "community")
    assignment_mode = str(metadata_dict.get("assignment_mode") or "").strip().lower()
    if not assignment_mode:
        assignment_mode = _SHARED_PROFILE_ASSIGNMENT_MODE_BY_KIND.get(profile_kind, "community_match")
    explicit_rules = metadata_dict.get("assignment_rules")
    assignment_rules = dict(explicit_rules) if isinstance(explicit_rules, Mapping) else {}
    if not assignment_rules:
        assignment_rules = {
            "use_hashtags": True,
            "use_mentions": True,
            "use_collaborators": _normalize_platform_name(platform) == "instagram",
            "use_configured_aliases": True,
            "allow_multi_show_candidates": profile_kind == "network_streaming",
        }
    network_name = ""
    for key in ("network_name", "display_name", "brand_name", "publisher_name"):
        network_name = str(metadata_dict.get(key) or "").strip()
        if network_name:
            break
    if not network_name:
        network_name = normalize_source_scope(source_scope).replace("_", " ").title()
    return {
        "source_scope": source_scope,
        "profile_kind": profile_kind,
        "network_name": network_name,
        "assignment_mode": assignment_mode,
        "assignment_rules": assignment_rules,
        "account_handle": _normalize_account_handle(account_handle) or str(account_handle or "").strip(),
        "platform": _normalize_platform_name(platform),
    }


def _shared_source_defaults(*, source_scope: str = "network") -> list[dict[str, Any]]:
    source_scope = normalize_source_scope_input(source_scope)
    if source_scope != "network":
        return []
    rows: list[dict[str, Any]] = []
    for platform in sorted(SOCIAL_SUPPORTED_PLATFORMS):
        for rank, account_handle in enumerate(_DEFAULT_PLATFORM_ACCOUNTS.get(platform, []), start=1):
            profile_contract = _shared_profile_contract(
                source_scope=source_scope,
                platform=platform,
                account_handle=account_handle,
                metadata={
                    "is_default": True,
                    "network_key": "bravo-tv",
                    "display_name": "Bravo TV",
                    "network_name": "Bravo TV",
                },
            )
            metadata = {
                "is_default": True,
                "network_key": "bravo-tv",
                "display_name": "Bravo TV",
                "network_name": "Bravo TV",
                **profile_contract,
            }
            rows.append(
                {
                    "id": f"default:{platform}:{account_handle}",
                    "platform": platform,
                    "source_scope": source_scope,
                    "account_handle": account_handle,
                    "is_active": True,
                    "scrape_priority": rank * 10,
                    "metadata": metadata,
                    "last_scrape_status": None,
                    "last_scrape_run_id": None,
                    "last_scrape_job_id": None,
                    "last_scrape_at": None,
                    "last_classified_at": None,
                    "updated_by": None,
                    "created_at": None,
                    "updated_at": None,
                    "is_default": True,
                    "profile_kind": profile_contract["profile_kind"],
                    "network_name": profile_contract["network_name"],
                    "assignment_mode": profile_contract["assignment_mode"],
                    "assignment_rules": profile_contract["assignment_rules"],
                }
            )
    return rows


def get_shared_account_sources(
    *,
    source_scope: str = "network",
    include_inactive: bool = True,
    platforms: list[str] | None = None,
) -> dict[str, Any]:
    source_scope = normalize_source_scope(source_scope)
    requested_platforms = _resolve_requested_platforms(platforms) if platforms else None
    sql = """
        select
          id::text as id,
          platform,
          source_scope,
          account_handle,
          is_active,
          scrape_priority,
          metadata,
          last_scrape_status,
          last_scrape_run_id::text as last_scrape_run_id,
          last_scrape_job_id::text as last_scrape_job_id,
          last_scrape_at,
          last_classified_at,
          updated_by,
          created_at,
          updated_at
        from social.shared_account_sources
        where source_scope = %s
    """
    params: list[Any] = [source_scope]
    if not include_inactive:
        sql += " and is_active = true"
    if requested_platforms is not None:
        sql += " and platform = any(%s)"
        params.append(requested_platforms)
    sql += " order by scrape_priority asc, platform asc, account_handle asc"
    payload_rows: list[dict[str, Any]] = []
    for row in pg.fetch_all(sql, params):
        platform = _normalize_platform_name(row.get("platform"))
        if not platform or (requested_platforms is not None and platform not in requested_platforms):
            continue
        payload = dict(row)
        payload["platform"] = platform
        payload["account_handle"] = (
            _normalize_account_handle(row.get("account_handle")) or str(row.get("account_handle") or "").strip()
        )
        payload["metadata"] = _metadata_dict(row.get("metadata"))
        payload.update(
            _shared_profile_contract(
                source_scope=str(row.get("source_scope") or source_scope),
                platform=platform,
                account_handle=payload["account_handle"],
                metadata=payload["metadata"],
            )
        )
        payload["is_default"] = bool(payload["metadata"].get("is_default"))
        payload_rows.append(payload)
    if payload_rows:
        return {"source_scope": source_scope, "sources": payload_rows, "using_defaults": False}
    fallback_rows = _shared_source_defaults(source_scope=source_scope)
    if requested_platforms is not None:
        fallback_rows = [row for row in fallback_rows if row.get("platform") in requested_platforms]
    return {"source_scope": source_scope, "sources": fallback_rows, "using_defaults": True}


def put_shared_account_sources(
    *,
    source_scope: str,
    sources: list[dict[str, Any]],
    updated_by: str | None = None,
) -> dict[str, Any]:
    source_scope = normalize_source_scope(source_scope)
    normalized_sources: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for source in sources:
        platform = _normalize_platform_name(source.get("platform"))
        if platform not in SOCIAL_SUPPORTED_PLATFORMS:
            raise ValueError(f"Unsupported platform: {source.get('platform')}")
        account_handle = _normalize_account_handle(source.get("account_handle"))
        if not account_handle:
            raise ValueError("Shared account source requires account_handle")
        key = (platform, account_handle)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        metadata = _metadata_dict(source.get("metadata"))
        metadata.update(
            _shared_profile_contract(
                source_scope=source_scope,
                platform=platform,
                account_handle=account_handle,
                metadata=metadata,
            )
        )
        normalized_sources.append(
            {
                "platform": platform,
                "account_handle": account_handle,
                "is_active": bool(source.get("is_active", True)),
                "scrape_priority": max(1, int(source.get("scrape_priority") or 100)),
                "metadata": metadata,
            }
        )

    active_pairs = {(row["platform"], row["account_handle"]) for row in normalized_sources}
    with pg.db_connection() as conn:
        with pg.db_cursor(conn=conn) as cur:
            for row in normalized_sources:
                pg.fetch_one_with_cursor(
                    cur,
                    """
                    insert into social.shared_account_sources (
                      platform, source_scope, account_handle, is_active,
                      scrape_priority, metadata, updated_by, updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s::jsonb, %s, now())
                    on conflict (platform, source_scope, account_handle)
                    do update set
                      is_active = excluded.is_active,
                      scrape_priority = excluded.scrape_priority,
                      metadata = excluded.metadata,
                      updated_by = excluded.updated_by,
                      updated_at = now()
                    returning id::text
                    """,
                    [
                        row["platform"],
                        source_scope,
                        row["account_handle"],
                        row["is_active"],
                        row["scrape_priority"],
                        json.dumps(row["metadata"], default=_json_serializer),
                        updated_by,
                    ],
                )
            if active_pairs:
                pair_clauses = " or ".join(["(platform = %s and account_handle = %s)"] * len(active_pairs))
                pair_params: list[Any] = []
                for platform, account_handle in sorted(active_pairs):
                    pair_params.extend([platform, account_handle])
                cur.execute(
                    f"""
                    update social.shared_account_sources
                    set is_active = false, updated_by = %s, updated_at = now()
                    where source_scope = %s and not ({pair_clauses})
                    """,
                    [updated_by, source_scope, *pair_params],
                )
            else:
                cur.execute(
                    """
                    update social.shared_account_sources
                    set is_active = false, updated_by = %s, updated_at = now()
                    where source_scope = %s
                    """,
                    [updated_by, source_scope],
                )
    return get_shared_account_sources(source_scope=source_scope, include_inactive=True)


__all__ = ["get_shared_account_sources", "put_shared_account_sources"]

register_legacy_patchable_namespace(globals(), __all__)
