"""
Core browse endpoints for shows, seasons, episodes, and cast.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.deps import (
    SupabaseAdminClient,
    SupabaseClient,
    get_list_result,
    require_single_result,
)
from trr_backend.db.show_images import ShowImagesError, list_tmdb_show_images

router = APIRouter(prefix="/shows", tags=["shows"])


# --- Pydantic models ---


class Show(BaseModel):
    id: UUID
    name: str
    description: str | None
    premiere_date: str | None
    external_ids: dict[str, Any]


class TmdbNetwork(BaseModel):
    id: int
    name: str
    origin_country: str | None = None
    tmdb_logo_path: str | None = None
    logo_path: str | None = None
    hosted_logo_key: str | None = None
    hosted_logo_url: str | None = None
    hosted_logo_sha256: str | None = None
    hosted_logo_content_type: str | None = None
    hosted_logo_bytes: int | None = None
    hosted_logo_etag: str | None = None
    hosted_logo_at: str | None = None


class TmdbProductionCompany(BaseModel):
    id: int
    name: str
    origin_country: str | None = None
    tmdb_logo_path: str | None = None
    logo_path: str | None = None
    hosted_logo_key: str | None = None
    hosted_logo_url: str | None = None
    hosted_logo_sha256: str | None = None
    hosted_logo_content_type: str | None = None
    hosted_logo_bytes: int | None = None
    hosted_logo_etag: str | None = None
    hosted_logo_at: str | None = None


class WatchProvider(BaseModel):
    provider_id: int
    provider_name: str
    display_priority: int | None = None
    tmdb_logo_path: str | None = None
    logo_path: str | None = None
    hosted_logo_key: str | None = None
    hosted_logo_url: str | None = None
    hosted_logo_sha256: str | None = None
    hosted_logo_content_type: str | None = None
    hosted_logo_bytes: int | None = None
    hosted_logo_etag: str | None = None
    hosted_logo_at: str | None = None


class WatchProviderGroup(BaseModel):
    region: str
    offer_type: str
    link: str | None = None
    providers: list[WatchProvider]


class ShowDetail(Show):
    genres: list[str] | None = None
    keywords: list[str] | None = None
    tags: list[str] | None = None
    networks: list[str] | None = None
    streaming_providers: list[str] | None = None
    listed_on: list[str] | None = None
    tmdb_network_ids: list[int] | None = None
    tmdb_production_company_ids: list[int] | None = None
    tmdb_networks: list[TmdbNetwork] | None = None
    tmdb_production_companies: list[TmdbProductionCompany] | None = None
    watch_providers: list[WatchProviderGroup] | None = None


class Season(BaseModel):
    show_name: str | None = None
    imdb_episode_ids: list[str] | None = None
    tmdb_episode_ids: list[int] | None = None
    id: UUID
    show_id: UUID
    season_number: int
    title: str | None
    premiere_date: str | None
    external_ids: dict[str, Any]


class Episode(BaseModel):
    show_name: str | None = None
    id: UUID
    season_id: UUID
    episode_number: int
    title: str | None
    air_date: str | None
    synopsis: str | None
    external_ids: dict[str, Any]


class Person(BaseModel):
    id: UUID
    full_name: str
    known_for: str | None
    external_ids: dict[str, Any]
    # Multi-source canonical fields (jsonb keyed by source; e.g. {"tmdb": "...", "fandom": "..."})
    birthday: dict[str, Any] | None = None
    gender: dict[str, Any] | None = None
    biography: dict[str, Any] | None = None
    place_of_birth: dict[str, Any] | None = None
    homepage: dict[str, Any] | None = None
    profile_image_url: dict[str, Any] | None = None


class CastMember(BaseModel):
    id: UUID
    show_id: UUID
    season_id: UUID | None = None
    person_id: UUID
    role: str | None = None
    credit_category: str | None = None
    billing_order: int | None = None
    notes: str | None = None
    person: Person | None = None


class CastList(BaseModel):
    count: int
    total_count: int
    has_more: bool
    cast: list[CastMember]


class ShowImage(BaseModel):
    id: UUID
    show_id: UUID | None = None
    tmdb_id: int | None = None
    show_name: str | None = None
    source: str
    kind: str
    iso_639_1: str | None = None
    file_path: str
    url_original: str | None = None
    width: int | None = None
    height: int | None = None
    aspect_ratio: float | None = None
    fetched_at: str | None = None


def _fetch_entities_by_ids(
    db: SupabaseClient,
    *,
    table: str,
    ids: list[int],
    fields: str,
    id_column: str = "id",
) -> list[dict]:
    if not ids:
        return []
    response = db.schema("core").table(table).select(fields).in_(id_column, ids).execute()
    return get_list_result(response, f"listing {table}")


def _stringify_temporal(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _normalize_temporal_fields(row: dict[str, Any], *fields: str) -> dict[str, Any]:
    for field in fields:
        if field in row:
            row[field] = _stringify_temporal(row.get(field))
    return row


def _group_watch_providers(rows: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        region = row.get("region")
        offer_type = row.get("offer_type")
        if not isinstance(region, str) or not isinstance(offer_type, str):
            continue
        key = (region, offer_type)
        group = groups.setdefault(
            key,
            {
                "region": region,
                "offer_type": offer_type,
                "link": row.get("link"),
                "providers": [],
            },
        )
        provider = row.get("provider") or {}
        if not isinstance(provider, dict):
            provider = {}
        provider_id = provider.get("provider_id")
        provider_name = provider.get("provider_name")
        if provider_id is None or not provider_name:
            continue
        group["providers"].append(
            {
                "provider_id": provider_id,
                "provider_name": provider_name,
                "display_priority": row.get("display_priority"),
                "tmdb_logo_path": provider.get("tmdb_logo_path"),
                "logo_path": provider.get("logo_path"),
                "hosted_logo_key": provider.get("hosted_logo_key"),
                "hosted_logo_url": provider.get("hosted_logo_url"),
                "hosted_logo_sha256": provider.get("hosted_logo_sha256"),
                "hosted_logo_content_type": provider.get("hosted_logo_content_type"),
                "hosted_logo_bytes": provider.get("hosted_logo_bytes"),
                "hosted_logo_etag": provider.get("hosted_logo_etag"),
                "hosted_logo_at": provider.get("hosted_logo_at"),
            }
        )

    grouped = list(groups.values())
    for group in grouped:
        group["providers"].sort(
            key=lambda p: (
                p.get("display_priority") is None,
                p.get("display_priority") or 0,
                str(p.get("provider_name") or "").casefold(),
            )
        )
    grouped.sort(key=lambda g: (g.get("region") or "", g.get("offer_type") or ""))
    return grouped


def _build_show_external_ids(show: dict[str, Any]) -> dict[str, Any]:
    external_ids: dict[str, Any] = {}
    imdb_id = show.get("imdb_id")
    if imdb_id:
        external_ids["imdb_id"] = imdb_id
        external_ids["imdb"] = imdb_id
    tmdb_id = show.get("tmdb_id")
    if tmdb_id is not None:
        external_ids["tmdb_id"] = tmdb_id
        external_ids["tmdb"] = tmdb_id
    for key in ("tvdb_id", "tvrage_id", "wikidata_id"):
        value = show.get(key)
        if value not in (None, ""):
            external_ids[key] = value
    return external_ids


# --- Endpoints ---


@router.get("", response_model=list[Show])
def list_shows(
    db: SupabaseClient,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """List all shows with pagination."""
    response = db.schema("core").table("shows").select("*").order("name").range(offset, offset + limit - 1).execute()
    rows = get_list_result(response, "listing shows")
    for row in rows:
        _normalize_temporal_fields(row, "premiere_date")
        row["external_ids"] = _build_show_external_ids(row)
    return rows


@router.get("/{show_id}", response_model=ShowDetail)
def get_show(db: SupabaseClient, show_id: UUID) -> dict:
    """Get a specific show by ID."""
    response = db.schema("core").table("shows").select("*").eq("id", str(show_id)).single().execute()
    show = require_single_result(response, "Show")
    _normalize_temporal_fields(show, "premiere_date")
    show["external_ids"] = _build_show_external_ids(show)

    network_ids = show.get("tmdb_network_ids") or []
    if isinstance(network_ids, list) and network_ids:
        networks = _fetch_entities_by_ids(
            db,
            table="networks",
            ids=[int(n) for n in network_ids if isinstance(n, int)],
            fields=(
                "id,name,origin_country,tmdb_logo_path,logo_path,hosted_logo_key,hosted_logo_url,hosted_logo_sha256,"
                "hosted_logo_content_type,hosted_logo_bytes,hosted_logo_etag,hosted_logo_at"
            ),
        )
        for row in networks:
            _normalize_temporal_fields(row, "hosted_logo_at")
        network_map = {row.get("id"): row for row in networks if row.get("id") is not None}
        show["tmdb_networks"] = [network_map.get(nid) for nid in network_ids if nid in network_map]
    else:
        show["tmdb_networks"] = []

    company_ids = show.get("tmdb_production_company_ids") or []
    if isinstance(company_ids, list) and company_ids:
        companies = _fetch_entities_by_ids(
            db,
            table="production_companies",
            ids=[int(c) for c in company_ids if isinstance(c, int)],
            fields=(
                "id,name,origin_country,tmdb_logo_path,logo_path,hosted_logo_key,hosted_logo_url,hosted_logo_sha256,"
                "hosted_logo_content_type,hosted_logo_bytes,hosted_logo_etag,hosted_logo_at"
            ),
        )
        for row in companies:
            _normalize_temporal_fields(row, "hosted_logo_at")
        company_map = {row.get("id"): row for row in companies if row.get("id") is not None}
        show["tmdb_production_companies"] = [company_map.get(cid) for cid in company_ids if cid in company_map]
    else:
        show["tmdb_production_companies"] = []

    providers_response = (
        db.schema("core")
        .table("show_watch_providers")
        .select("region,offer_type,display_priority,link,provider_id")
        .eq("show_id", str(show_id))
        .execute()
    )
    provider_rows = get_list_result(providers_response, "listing watch providers")
    provider_ids = sorted({int(row["provider_id"]) for row in provider_rows if isinstance(row.get("provider_id"), int)})
    providers_by_id: dict[int, dict[str, Any]] = {}
    if provider_ids:
        providers = _fetch_entities_by_ids(
            db,
            table="watch_providers",
            ids=provider_ids,
            id_column="provider_id",
            fields=(
                "provider_id,provider_name,display_priority,tmdb_logo_path,logo_path,hosted_logo_key,hosted_logo_url,"
                "hosted_logo_sha256,hosted_logo_content_type,hosted_logo_bytes,hosted_logo_etag,hosted_logo_at"
            ),
        )
        for provider in providers:
            _normalize_temporal_fields(provider, "hosted_logo_at")
            provider_id = provider.get("provider_id")
            if isinstance(provider_id, int):
                providers_by_id[provider_id] = provider
    for row in provider_rows:
        provider_id = row.get("provider_id")
        row["provider"] = providers_by_id.get(provider_id, {})
    show["watch_providers"] = _group_watch_providers(provider_rows)

    return show


@router.get("/{show_id}/images", response_model=list[ShowImage])
def list_show_images(
    db: SupabaseClient,
    show_id: UUID,
    kind: str | None = Query(default=None),
) -> list[dict]:
    """List TMDb images (posters/logos/backdrops) for a show."""
    try:
        return list_tmdb_show_images(db, show_id=show_id, kind=kind)
    except ShowImagesError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/{show_id}/seasons", response_model=list[Season])
def list_seasons(
    db: SupabaseClient,
    show_id: UUID,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """List all seasons for a show."""
    response = (
        db.schema("core")
        .table("seasons")
        .select("*")
        .eq("show_id", str(show_id))
        .order("season_number")
        .range(offset, offset + limit - 1)
        .execute()
    )
    rows = get_list_result(response, "listing seasons")
    for row in rows:
        _normalize_temporal_fields(row, "premiere_date")
    return rows


@router.get("/{show_id}/seasons/{season_number}", response_model=Season)
def get_season(
    db: SupabaseClient,
    show_id: UUID,
    season_number: int,
) -> dict:
    """Get a specific season by show ID and season number."""
    response = (
        db.schema("core")
        .table("seasons")
        .select("*")
        .eq("show_id", str(show_id))
        .eq("season_number", season_number)
        .single()
        .execute()
    )
    row = require_single_result(response, "Season")
    _normalize_temporal_fields(row, "premiere_date")
    return row


@router.get("/{show_id}/seasons/{season_number}/episodes", response_model=list[Episode])
def list_episodes(
    db: SupabaseClient,
    show_id: UUID,
    season_number: int,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """List all episodes for a season."""
    # First get the season ID
    season_response = (
        db.schema("core")
        .table("seasons")
        .select("id")
        .eq("show_id", str(show_id))
        .eq("season_number", season_number)
        .single()
        .execute()
    )
    season = require_single_result(season_response, "Season")
    season_id = season["id"]

    # Then get episodes
    response = (
        db.schema("core")
        .table("episodes")
        .select("*")
        .eq("season_id", season_id)
        .order("episode_number")
        .range(offset, offset + limit - 1)
        .execute()
    )
    rows = get_list_result(response, "listing episodes")
    for row in rows:
        _normalize_temporal_fields(row, "air_date")
    return rows


@router.get("/{show_id}/cast", response_model=CastList)
def list_show_cast(
    db: SupabaseAdminClient,
    show_id: UUID,
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """
    List cast members for a show.

    Reads from credits-backed views (core.v_show_cast).
    """
    response = (
        db.schema("core")
        .table("v_show_cast")
        .select("*", count="exact")
        .eq("show_id", str(show_id))
        .order("billing_order", nullsfirst=False)
        .range(offset, offset + limit - 1)
        .execute()
    )
    cast_rows = get_list_result(response, "listing cast")
    person_ids = [str(r.get("person_id") or "") for r in cast_rows if r.get("person_id")]
    person_ids = [pid for pid in person_ids if pid]

    people_by_id: dict[str, dict[str, Any]] = {}
    if person_ids:
        people_resp = db.schema("core").table("people").select("*").in_("id", person_ids).execute()
        people_rows = get_list_result(people_resp, "listing cast people")
        people_by_id = {str(p.get("id") or ""): p for p in people_rows if p.get("id")}

    rows: list[dict[str, Any]] = []
    for r in cast_rows:
        pid = str(r.get("person_id") or "")
        rows.append({**r, "person": people_by_id.get(pid)})
    total_count = getattr(response, "count", None)
    if isinstance(total_count, int):
        total_count = total_count
    elif isinstance(total_count, float):
        total_count = int(total_count)
    elif isinstance(total_count, str) and total_count.isdigit():
        total_count = int(total_count)
    else:
        total_count = len(cast_rows)
    has_more = offset + limit < total_count
    return {"count": len(rows), "total_count": total_count, "has_more": has_more, "cast": rows}


@router.get("/people/{person_id}", response_model=Person)
def get_person(db: SupabaseClient, person_id: UUID) -> dict:
    """Get a specific person by ID."""
    response = db.schema("core").table("people").select("*").eq("id", str(person_id)).single().execute()
    return require_single_result(response, "Person")
