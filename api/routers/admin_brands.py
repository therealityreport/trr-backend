"""Admin endpoints for brands shows/franchise workflows."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import AdminUser
from trr_backend.db import pg
from trr_backend.repositories import brand_families, brands_franchises

router = APIRouter(prefix="/admin/brands", tags=["admin-brands"])


class UpdateFranchiseRuleRequest(BaseModel):
    name: str | None = None
    primary_url: str | None = None
    review_allpages_url: str | None = None
    match_terms: list[str] | None = None
    aliases: list[str] | None = None
    community_domains: list[str] | None = None
    include_allpages_scan: bool | None = None
    source_rank: int | None = Field(default=None, ge=0)
    network_terms: list[str] | None = None
    is_active: bool | None = None


class ApplyFranchiseRuleRequest(BaseModel):
    missing_only: bool = True
    dry_run: bool = True


class CreateBrandFamilyRequest(BaseModel):
    family_key: str | None = None
    display_name: str
    owner_wikidata_id: str | None = None
    owner_label: str | None = None
    is_active: bool = True
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PatchBrandFamilyRequest(BaseModel):
    display_name: str | None = None
    owner_wikidata_id: str | None = None
    owner_label: str | None = None
    is_active: bool | None = None
    notes: str | None = None
    metadata: dict[str, Any] | None = None


class AddBrandFamilyMemberRequest(BaseModel):
    entity_type: Literal["network", "streaming"]
    entity_key: str
    entity_display_name: str | None = None
    source: Literal["manual", "suggested_owner", "system"] = "manual"
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateBrandFamilyLinkRuleRequest(BaseModel):
    link_group: Literal["official", "social", "knowledge", "cast_announcements", "other"] = "knowledge"
    link_kind: str
    label: str | None = None
    url: str
    coverage_type: Literal[
        "family_all_shows",
        "family_network_shows",
        "family_streaming_shows",
        "franchise_rule",
        "show_wikidata_exact",
        "show_name_contains",
    ] = "family_all_shows"
    coverage_value: str | None = None
    source: Literal["manual", "wikipedia_import", "system"] = "manual"
    priority: int = Field(default=100, ge=0)
    auto_apply: bool = True
    is_active: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class PatchBrandFamilyLinkRuleRequest(BaseModel):
    link_group: Literal["official", "social", "knowledge", "cast_announcements", "other"] | None = None
    link_kind: str | None = None
    label: str | None = None
    url: str | None = None
    coverage_type: Literal[
        "family_all_shows",
        "family_network_shows",
        "family_streaming_shows",
        "franchise_rule",
        "show_wikidata_exact",
        "show_name_contains",
    ] | None = None
    coverage_value: str | None = None
    source: Literal["manual", "wikipedia_import", "system"] | None = None
    priority: int | None = Field(default=None, ge=0)
    auto_apply: bool | None = None
    is_active: bool | None = None
    metadata: dict[str, Any] | None = None


class ApplyBrandFamilyLinksRequest(BaseModel):
    dry_run: bool = True
    rule_ids: list[str] | None = None


class ImportBrandFamilyWikipediaLinksRequest(BaseModel):
    entity_type: Literal["network", "streaming"] | None = None
    entity_key: str | None = None
    apply_matched: bool = False


BrandLogoTargetType = Literal[
    "show",
    "network",
    "streaming",
    "production",
    "franchise",
    "publication",
    "social",
    "other",
]


def _like_term(value: str) -> str:
    return f"%{value}%"


def _list_brand_logos(*, target_type: BrandLogoTargetType, q: str, limit: int, offset: int) -> dict[str, Any]:
    needle = q.strip()
    like = _like_term(needle)

    if target_type in {"network", "streaming", "production"}:
        rows = pg.fetch_all(
            """
            select
              id::text as id,
              entity_type as target_type,
              entity_key as target_key,
              coalesce(display_name, entity_key) as target_label,
              source_url,
              null::text as source_page_url,
              null::text as source_domain,
              hosted_logo_url,
              hosted_logo_black_url,
              hosted_logo_white_url,
              is_primary,
              mirror_status,
              failure_reason,
              metadata,
              created_at,
              updated_at
            from admin.network_streaming_logo_assets
            where entity_type = %s
              and (
                %s = ''
                or entity_key ilike %s
                or coalesce(display_name, '') ilike %s
                or coalesce(source_url, '') ilike %s
              )
            order by is_primary desc, updated_at desc nulls last, created_at desc nulls last
            limit %s offset %s
            """,
            [target_type, needle, like, like, like, limit, offset],
        )
        count_row = pg.fetch_one(
            """
            select count(*)::int as count
            from admin.network_streaming_logo_assets
            where entity_type = %s
              and (
                %s = ''
                or entity_key ilike %s
                or coalesce(display_name, '') ilike %s
                or coalesce(source_url, '') ilike %s
              )
            """,
            [target_type, needle, like, like, like],
        )
        return {"rows": rows, "count": int((count_row or {}).get("count") or 0)}

    if target_type in {"franchise", "publication", "social", "other"}:
        rows = pg.fetch_all(
            """
            select
              id::text as id,
              target_type,
              target_key,
              target_label,
              source_url,
              source_page_url,
              source_domain,
              hosted_logo_url,
              hosted_logo_black_url,
              hosted_logo_white_url,
              is_primary,
              mirror_status,
              failure_reason,
              metadata,
              created_at,
              updated_at
            from admin.brand_logo_assets
            where target_type = %s
              and (
                %s = ''
                or target_key ilike %s
                or target_label ilike %s
                or coalesce(source_domain, '') ilike %s
                or coalesce(source_url, '') ilike %s
              )
            order by is_primary desc, updated_at desc nulls last, created_at desc nulls last
            limit %s offset %s
            """,
            [target_type, needle, like, like, like, like, limit, offset],
        )
        count_row = pg.fetch_one(
            """
            select count(*)::int as count
            from admin.brand_logo_assets
            where target_type = %s
              and (
                %s = ''
                or target_key ilike %s
                or target_label ilike %s
                or coalesce(source_domain, '') ilike %s
                or coalesce(source_url, '') ilike %s
              )
            """,
            [target_type, needle, like, like, like, like],
        )
        return {"rows": rows, "count": int((count_row or {}).get("count") or 0)}

    rows = pg.fetch_all(
        """
        select distinct on (ma.id, ml.entity_id)
          ma.id::text as id,
          'show'::text as target_type,
          ml.entity_id::text as target_key,
          coalesce(s.name, ml.entity_id::text) as target_label,
          ma.url as source_url,
          coalesce(ml.context->>'source_page_url', ma.metadata->>'source_page_url') as source_page_url,
          coalesce(ml.context->>'source_domain', ma.metadata->>'source_domain') as source_domain,
          ma.hosted_url as hosted_logo_url,
          coalesce(ma.metadata->>'logo_black_url', ma.metadata->>'hosted_logo_black_url') as hosted_logo_black_url,
          coalesce(ma.metadata->>'logo_white_url', ma.metadata->>'hosted_logo_white_url') as hosted_logo_white_url,
          case when lower(ml.kind) = 'logo' then true else false end as is_primary,
          'mirrored'::text as mirror_status,
          null::text as failure_reason,
          ma.metadata,
          ma.created_at,
          ma.updated_at
        from core.media_links ml
        join core.media_assets ma on ma.id = ml.media_asset_id
        left join core.shows s on s.id = ml.entity_id
        where ml.entity_type = 'show'
          and lower(coalesce(ml.kind, '')) = 'logo'
          and (
            coalesce(ml.context->>'logo_target_type', ma.metadata->>'logo_target_type', '') = 'show'
            or upper(coalesce(ml.context->>'source_logo', ma.metadata->>'source_logo', '')) = 'SHOW'
          )
          and (
            %s = ''
            or coalesce(s.name, '') ilike %s
            or ml.entity_id::text ilike %s
            or coalesce(ma.url, '') ilike %s
          )
        order by ma.id, ml.entity_id, ma.updated_at desc nulls last, ma.created_at desc nulls last
        limit %s offset %s
        """,
        [needle, like, like, like, limit, offset],
    )
    count_row = pg.fetch_one(
        """
        select count(*)::int as count
        from (
          select distinct ma.id, ml.entity_id
          from core.media_links ml
          join core.media_assets ma on ma.id = ml.media_asset_id
          left join core.shows s on s.id = ml.entity_id
          where ml.entity_type = 'show'
            and lower(coalesce(ml.kind, '')) = 'logo'
            and (
              coalesce(ml.context->>'logo_target_type', ma.metadata->>'logo_target_type', '') = 'show'
              or upper(coalesce(ml.context->>'source_logo', ma.metadata->>'source_logo', '')) = 'SHOW'
            )
            and (
              %s = ''
              or coalesce(s.name, '') ilike %s
              or ml.entity_id::text ilike %s
              or coalesce(ma.url, '') ilike %s
            )
        ) dedup
        """,
        [needle, like, like, like],
    )
    return {"rows": rows, "count": int((count_row or {}).get("count") or 0)}


def _list_logo_targets(*, target_type: BrandLogoTargetType, q: str, limit: int) -> dict[str, Any]:
    needle = q.strip()
    like = _like_term(needle)

    if target_type == "show":
        rows = pg.fetch_all(
            """
            select
              'show'::text as target_type,
              s.id::text as target_key,
              s.name as target_label
            from core.shows s
            where %s = '' or s.name ilike %s
            order by s.name asc
            limit %s
            """,
            [needle, like, limit],
        )
        return {"rows": rows, "count": len(rows)}

    if target_type == "network":
        rows = pg.fetch_all(
            """
            select
              'network'::text as target_type,
              n.id::text as target_key,
              n.name as target_label
            from core.networks n
            where %s = '' or n.name ilike %s
            order by n.name asc
            limit %s
            """,
            [needle, like, limit],
        )
        return {"rows": rows, "count": len(rows)}

    if target_type == "streaming":
        rows = pg.fetch_all(
            """
            select
              'streaming'::text as target_type,
              wp.provider_id::text as target_key,
              wp.provider_name as target_label
            from core.watch_providers wp
            where %s = '' or wp.provider_name ilike %s
            order by wp.provider_name asc
            limit %s
            """,
            [needle, like, limit],
        )
        return {"rows": rows, "count": len(rows)}

    if target_type == "production":
        rows = pg.fetch_all(
            """
            select
              'production'::text as target_type,
              pc.id::text as target_key,
              pc.name as target_label
            from core.production_companies pc
            where %s = '' or pc.name ilike %s
            order by pc.name asc
            limit %s
            """,
            [needle, like, limit],
        )
        return {"rows": rows, "count": len(rows)}

    if target_type == "franchise":
        payload = brands_franchises.list_franchise_rules()
        rules = payload.get("rules", []) if isinstance(payload, dict) else []
        rows = []
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            key = str(rule.get("key") or "").strip()
            label = str(rule.get("name") or key).strip()
            if not key or not label:
                continue
            haystack = f"{key} {label}".lower()
            if needle and needle.lower() not in haystack:
                continue
            rows.append(
                {
                    "target_type": "franchise",
                    "target_key": key,
                    "target_label": label,
                }
            )
            if len(rows) >= limit:
                break
        return {"rows": rows, "count": len(rows)}

    rows = pg.fetch_all(
        """
        select distinct on (target_key)
          target_type,
          target_key,
          target_label
        from admin.brand_logo_assets
        where target_type = %s
          and (
            %s = ''
            or target_key ilike %s
            or target_label ilike %s
          )
        order by target_key, is_primary desc, updated_at desc nulls last
        limit %s
        """,
        [target_type, needle, like, like, limit],
    )
    return {"rows": rows, "count": len(rows)}


def _is_service_unavailable_error(error: RuntimeError) -> bool:
    message = str(error).strip().lower()
    return (
        "table is unavailable" in message
        or "run backend migrations" in message
        or "schema" in message and "missing" in message
        or "is not migrated" in message
        or "connection pool exhausted" in message
        or "database pool initialization failed" in message
    )


def _to_http_exception(error: Exception) -> HTTPException:
    if isinstance(error, KeyError):
        detail = str(error).strip().strip('"').strip("'") or "Not found"
        return HTTPException(status_code=404, detail=detail)
    if isinstance(error, ValueError):
        return HTTPException(status_code=400, detail=str(error))
    if isinstance(error, RuntimeError) and _is_service_unavailable_error(error):
        return HTTPException(status_code=503, detail=str(error))
    return HTTPException(status_code=500, detail=str(error) or "Internal server error")


@router.get("/shows-franchises")
def get_shows_franchises(
    q: str = Query(default=""),
    limit: int = Query(default=300, ge=1, le=1000),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brands_franchises.list_shows_franchises(q=q, limit=limit)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/franchise-rules")
def get_franchise_rules(_: AdminUser = None) -> dict[str, Any]:
    try:
        return brands_franchises.list_franchise_rules()
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.put("/franchise-rules/{franchise_key}")
def put_franchise_rule(
    franchise_key: str,
    payload: UpdateFranchiseRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brands_franchises.update_franchise_rule(
            franchise_key=franchise_key,
            payload=payload.model_dump(exclude_none=True),
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/franchise-rules/{franchise_key}/apply")
def post_apply_franchise_rule(
    franchise_key: str,
    payload: ApplyFranchiseRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brands_franchises.apply_franchise_rule(
            franchise_key=franchise_key,
            missing_only=payload.missing_only,
            dry_run=payload.dry_run,
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families")
def get_brand_families(
    active_only: bool = Query(default=True),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brand_families.list_families(active_only=active_only)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families")
def post_brand_family(
    payload: CreateBrandFamilyRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.create_family(payload=payload.model_dump(), actor=actor)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.patch("/families/{family_id}")
def patch_brand_family(
    family_id: str,
    payload: PatchBrandFamilyRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.patch_family(
            family_id=family_id,
            payload=payload.model_dump(exclude_unset=True),
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families/{family_id}/members")
def post_brand_family_member(
    family_id: str,
    payload: AddBrandFamilyMemberRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.add_family_member(family_id=family_id, payload=payload.model_dump(), actor=actor)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.delete("/families/{family_id}/members/{member_id}")
def delete_brand_family_member(
    family_id: str,
    member_id: str,
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brand_families.delete_family_member(family_id=family_id, member_id=member_id)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families/suggestions")
def get_brand_family_suggestions(_: AdminUser = None) -> dict[str, Any]:
    try:
        return brand_families.list_family_suggestions()
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families/by-entity")
def get_brand_family_by_entity(
    entity_type: Literal["network", "streaming"] = Query(...),
    entity_key: str = Query(...),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        family = brand_families.get_family_by_entity(entity_type=entity_type, entity_key=entity_key)
        suggestions = brand_families.list_family_suggestions()
        if not family:
            return {
                "family": None,
                "family_suggestions": suggestions.get("rows", []),
                "shared_links": [],
                "wikipedia_show_urls": [],
            }
        family_id = str(family.get("id") or "")
        links = brand_families.list_family_links(family_id=family_id, active_only=True)
        wiki_rows = brand_families.list_family_wikipedia_show_links(family_id=family_id, limit=500)
        return {
            "family": family,
            "family_suggestions": suggestions.get("rows", []),
            "shared_links": links.get("rows", []),
            "wikipedia_show_urls": wiki_rows.get("rows", []),
        }
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families/{family_id}/links")
def get_brand_family_links(
    family_id: str,
    active_only: bool = Query(default=False),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brand_families.list_family_links(family_id=family_id, active_only=active_only)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families/{family_id}/links")
def post_brand_family_link(
    family_id: str,
    payload: CreateBrandFamilyLinkRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.create_family_link_rule(family_id=family_id, payload=payload.model_dump(), actor=actor)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.patch("/families/{family_id}/links/{rule_id}")
def patch_brand_family_link(
    family_id: str,
    rule_id: str,
    payload: PatchBrandFamilyLinkRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.patch_family_link_rule(
            family_id=family_id,
            rule_id=rule_id,
            payload=payload.model_dump(exclude_unset=True),
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families/{family_id}/links/apply")
def post_brand_family_links_apply(
    family_id: str,
    payload: ApplyBrandFamilyLinksRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.apply_family_links(
            family_id=family_id,
            dry_run=payload.dry_run,
            actor=actor,
            rule_ids=payload.rule_ids,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families/{family_id}/wikipedia-import")
def post_brand_family_wikipedia_import(
    family_id: str,
    payload: ImportBrandFamilyWikipediaLinksRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        result = brand_families.import_family_wikipedia_show_links(
            family_id=family_id,
            actor=actor,
            entity_type=payload.entity_type,
            entity_key=payload.entity_key,
            apply_matched=payload.apply_matched,
            import_source="manual",
        )
        result["wikipedia_show_urls"] = brand_families.list_family_wikipedia_show_links(family_id=family_id, limit=500)
        return result
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families/{family_id}/wikipedia-show-urls")
def get_brand_family_wikipedia_show_urls(
    family_id: str,
    limit: int = Query(default=500, ge=1, le=5000),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brand_families.list_family_wikipedia_show_links(family_id=family_id, limit=limit)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/logos")
def get_brand_logos(
    target_type: BrandLogoTargetType = Query(...),
    q: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _list_brand_logos(target_type=target_type, q=q, limit=limit, offset=offset)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/logo-targets")
def get_brand_logo_targets(
    target_type: BrandLogoTargetType = Query(...),
    q: str = Query(default=""),
    limit: int = Query(default=50, ge=1, le=200),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _list_logo_targets(target_type=target_type, q=q, limit=limit)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error
