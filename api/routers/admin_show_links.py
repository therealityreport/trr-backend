from __future__ import annotations

import re
from typing import Any, Literal
from urllib.parse import quote, urlparse
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, HttpUrl

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.db import pg

router = APIRouter(prefix="/admin/shows", tags=["admin-show-links"])
_BRAVO_VARIANT = "default"

EntityType = Literal["show", "season", "person"]
LinkGroup = Literal["official", "social", "knowledge", "cast_announcements", "other"]
LinkStatus = Literal["pending", "approved", "rejected"]


class LinkDiscoverRequest(BaseModel):
    include_seasons: bool = True
    include_people: bool = True


class LinkCreateRequest(BaseModel):
    entity_type: EntityType
    entity_id: UUID
    link_group: LinkGroup
    link_kind: str
    url: HttpUrl
    label: str | None = None
    season_number: int | None = Field(default=None, ge=0, le=200)
    status: LinkStatus = "approved"
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class LinkPatchRequest(BaseModel):
    link_group: LinkGroup | None = None
    link_kind: str | None = None
    url: HttpUrl | None = None
    label: str | None = None
    status: LinkStatus | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    metadata: dict[str, Any] | None = None


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower().strip()).strip("-")


def _url_key(value: str) -> str:
    return value.strip().lower()


def _upsert_link(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    entity_type: str,
    entity_id: str,
    link_group: str,
    link_kind: str,
    url: str,
    label: str | None,
    season_number: int,
    status: str,
    confidence: float | None,
    source: str | None,
    discovered_by: str | None,
    metadata: dict[str, Any] | None,
    actor: str,
) -> dict[str, Any]:
    payload = {
        "show_id": show_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "link_group": link_group,
        "link_kind": link_kind,
        "url": url,
        "url_key": _url_key(url),
        "label": label,
        "season_number": max(0, season_number),
        "status": status,
        "confidence": confidence,
        "source": source,
        "discovered_by": discovered_by,
        "metadata": metadata or {},
        "created_by": actor,
        "updated_by": actor,
    }
    response = (
        db.schema("core")
        .table("entity_links")
        .upsert(payload, on_conflict="entity_type,entity_id,link_kind,season_number,url_key")
        .execute()
    )
    rows = get_list_result(response, "upserting entity links")
    return rows[0] if rows else payload


def _show_exists(show_id: str) -> bool:
    row = pg.fetch_one("SELECT id FROM core.shows WHERE id = %s", [show_id])
    return bool(row)


def _discover_show_links(show_id: str) -> list[dict[str, Any]]:
    show = pg.fetch_one(
        """
        SELECT id, name, networks, wikidata_id, external_ids
        FROM core.shows
        WHERE id = %s
        """,
        [show_id],
    )
    if not show:
        return []

    show_name = str(show.get("name") or "").strip()
    show_slug = _slug(show_name)
    networks = [str(n).strip().lower() for n in (show.get("networks") or []) if isinstance(n, str)]
    external_ids = show.get("external_ids") if isinstance(show.get("external_ids"), dict) else {}

    discovered: list[dict[str, Any]] = []

    if show_slug:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "official",
                "link_kind": "official_page",
                "label": "BravoTV show page",
                "url": f"https://www.bravotv.com/{show_slug}",
                "source": "derived",
            }
        )
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "wikipedia",
                "label": "Wikipedia",
                "url": f"https://en.wikipedia.org/wiki/{quote(show_name.replace(' ', '_'))}",
                "source": "derived",
            }
        )

    wikidata_id = str(show.get("wikidata_id") or "").strip()
    if wikidata_id:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "wikidata",
                "label": "Wikidata",
                "url": f"https://www.wikidata.org/wiki/{wikidata_id}",
                "source": "core.shows.wikidata_id",
            }
        )

    if "bravo" in networks:
        discovered.extend(
            [
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": "instagram",
                    "label": "Instagram",
                    "url": "https://www.instagram.com/BravoTV",
                    "source": "network_default",
                },
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": "tiktok",
                    "label": "TikTok",
                    "url": "https://www.tiktok.com/@BravoTV",
                    "source": "network_default",
                },
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": "twitter",
                    "label": "Twitter/X",
                    "url": "https://x.com/BravoTV",
                    "source": "network_default",
                },
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": "youtube",
                    "label": "YouTube",
                    "url": "https://www.youtube.com/@Bravo",
                    "source": "network_default",
                },
            ]
        )

    if isinstance(external_ids, dict):
        for kind in ("instagram", "tiktok", "twitter", "youtube"):
            handle = str(external_ids.get(kind) or external_ids.get(f"{kind}_id") or "").strip()
            if not handle:
                continue
            canonical = handle.lstrip("@")
            if kind == "instagram":
                url = f"https://www.instagram.com/{canonical}"
            elif kind == "tiktok":
                url = f"https://www.tiktok.com/@{canonical}"
            elif kind == "twitter":
                url = f"https://x.com/{canonical}"
            else:
                url = f"https://www.youtube.com/@{canonical}"
            discovered.append(
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": kind,
                    "label": kind.title(),
                    "url": url,
                    "source": "core.shows.external_ids",
                }
            )

    snapshot = pg.fetch_one(
        """
        SELECT payload
        FROM core.show_source_latest
        WHERE show_id = %s AND source_id = 'bravo' AND variant = %s
        LIMIT 1
        """,
        [show_id, _BRAVO_VARIANT],
    )
    payload = snapshot.get("payload") if snapshot and isinstance(snapshot.get("payload"), dict) else {}
    normalized = payload.get("normalized") if isinstance(payload, dict) else {}
    news_items = (
        normalized.get("news_show")
        if isinstance(normalized, dict) and isinstance(normalized.get("news_show"), list)
        else []
    )
    for item in news_items:
        if not isinstance(item, dict):
            continue
        headline = str(item.get("headline") or "").strip()
        article_url = str(item.get("article_url") or "").strip()
        if not article_url:
            continue
        if not re.search(r"\b(cast|friend\s*of|full[-\s]*time|joins|returning|returns)\b", headline, re.IGNORECASE):
            continue
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": int(item.get("season_number") or 0),
                "link_group": "cast_announcements",
                "link_kind": "cast_announcement",
                "label": headline or "Cast announcement",
                "url": article_url,
                "source": "bravo_snapshot",
                "metadata": {"published_at": item.get("published_at")},
            }
        )

    return discovered


def _discover_season_links(show_id: str) -> list[dict[str, Any]]:
    rows = pg.fetch_all(
        """
        SELECT id, season_number, external_wikidata_id, external_ids
        FROM core.seasons
        WHERE show_id = %s
        """,
        [show_id],
    )
    show_name_row = pg.fetch_one("SELECT name FROM core.shows WHERE id = %s", [show_id])
    show_name = str(show_name_row.get("name") or "").strip() if show_name_row else ""
    found: list[dict[str, Any]] = []
    for row in rows:
        season_id = str(row.get("id"))
        season_number = int(row.get("season_number") or 0)
        if season_number <= 0:
            continue
        wikidata = str(row.get("external_wikidata_id") or "").strip()
        if wikidata:
            found.append(
                {
                    "entity_type": "season",
                    "entity_id": season_id,
                    "season_number": season_number,
                    "link_group": "knowledge",
                    "link_kind": "wikidata",
                    "label": f"Season {season_number} Wikidata",
                    "url": f"https://www.wikidata.org/wiki/{wikidata}",
                    "source": "core.seasons.external_wikidata_id",
                }
            )
        if show_name:
            found.append(
                {
                    "entity_type": "season",
                    "entity_id": season_id,
                    "season_number": season_number,
                    "link_group": "knowledge",
                    "link_kind": "wikipedia",
                    "label": f"Season {season_number} Wikipedia",
                    "url": (
                        "https://en.wikipedia.org/wiki/"
                        f"{quote((show_name + ' season ' + str(season_number)).replace(' ', '_'))}"
                    ),
                    "source": "derived",
                }
            )
    return found


def _discover_people_links(show_id: str) -> list[dict[str, Any]]:
    show = pg.fetch_one("SELECT networks FROM core.shows WHERE id = %s", [show_id]) or {}
    networks = [str(value).strip().lower() for value in (show.get("networks") or []) if isinstance(value, str)]
    is_bravo_show = "bravo" in networks

    housewife_friend_ids: set[str] = set()
    if is_bravo_show:
        role_rows = pg.fetch_all(
            """
            SELECT DISTINCT sra.person_id::text AS person_id
            FROM core.show_cast_role_assignments sra
            JOIN core.show_role_catalog rc ON rc.id = sra.role_id
            WHERE sra.show_id = %s
              AND lower(rc.name) IN ('housewife', 'friend')
            """,
            [show_id],
        )
        housewife_friend_ids = {
            str(row.get("person_id") or "").strip() for row in role_rows if row.get("person_id")
        }

    rows = pg.fetch_all(
        """
        SELECT DISTINCT p.id, p.full_name, p.external_ids, cf.source_url AS fandom_url
        FROM core.v_show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.cast_fandom cf ON cf.person_id = p.id AND cf.source = 'fandom'
        WHERE sc.show_id = %s
        """,
        [show_id],
    )
    found: list[dict[str, Any]] = []
    for row in rows:
        person_id = str(row.get("id"))
        name = str(row.get("full_name") or "").strip()
        fandom_url = str(row.get("fandom_url") or "").strip()
        has_fandom_profile = bool(fandom_url)
        external_ids = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
        wikidata = str(external_ids.get("wikidata") or external_ids.get("wikidata_id") or "").strip()
        if wikidata:
            found.append(
                {
                    "entity_type": "person",
                    "entity_id": person_id,
                    "season_number": 0,
                    "link_group": "knowledge",
                    "link_kind": "wikidata",
                    "label": f"{name} Wikidata" if name else "Wikidata",
                    "url": f"https://www.wikidata.org/wiki/{wikidata}",
                    "source": "core.people.external_ids",
                }
            )
        if has_fandom_profile and name:
            found.append(
                {
                    "entity_type": "person",
                    "entity_id": person_id,
                    "season_number": 0,
                    "link_group": "knowledge",
                    "link_kind": "wikipedia",
                    "label": f"{name} Wikipedia",
                    "url": f"https://en.wikipedia.org/wiki/{quote(name.replace(' ', '_'))}",
                    "source": "derived",
                }
            )
        if has_fandom_profile:
            found.append(
                {
                    "entity_type": "person",
                    "entity_id": person_id,
                    "season_number": 0,
                    "link_group": "knowledge",
                    "link_kind": "fandom",
                    "label": f"{name} Fandom" if name else "Fandom",
                    "url": fandom_url,
                    "source": "core.cast_fandom",
                }
            )
        if is_bravo_show and person_id in housewife_friend_ids and name:
            slug = _slug(name)
            if slug:
                found.append(
                    {
                        "entity_type": "person",
                        "entity_id": person_id,
                        "season_number": 0,
                        "link_group": "official",
                        "link_kind": "bravo_profile",
                        "label": f"{name} Bravo profile",
                        "url": f"https://www.bravotv.com/people/{slug}",
                        "source": "cast_matrix_sync",
                    }
                )
    return found


@router.post("/{show_id}/links/discover")
def discover_show_links(
    show_id: UUID,
    payload: LinkDiscoverRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    actor = str(admin.get("email") or admin.get("id") or "admin")
    discovered = _discover_show_links(show_id_str)
    if payload.include_seasons:
        discovered.extend(_discover_season_links(show_id_str))
    if payload.include_people:
        discovered.extend(_discover_people_links(show_id_str))

    upserted = 0
    by_group: dict[str, int] = {}
    for row in discovered:
        parsed = urlparse(str(row["url"]))
        if not parsed.scheme.startswith("http"):
            continue
        _upsert_link(
            db,
            show_id=show_id_str,
            entity_type=row["entity_type"],
            entity_id=str(row["entity_id"]),
            link_group=row["link_group"],
            link_kind=str(row["link_kind"]),
            url=str(row["url"]),
            label=(str(row.get("label")) if row.get("label") else None),
            season_number=int(row.get("season_number") or 0),
            status="pending",
            confidence=0.65,
            source=(str(row.get("source")) if row.get("source") else None),
            discovered_by="backend_discovery",
            metadata=(row.get("metadata") if isinstance(row.get("metadata"), dict) else {}),
            actor=actor,
        )
        upserted += 1
        by_group[row["link_group"]] = by_group.get(row["link_group"], 0) + 1

    return {
        "show_id": show_id_str,
        "discovered": upserted,
        "counts_by_group": by_group,
    }


@router.get("/{show_id}/links")
def list_show_links(
    show_id: UUID,
    _: AdminUser,
    status: Literal["all", "pending", "approved", "rejected"] = Query(default="all"),
    entity_type: EntityType | Literal["all"] = Query(default="all"),
) -> list[dict[str, Any]]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    params: list[Any] = [show_id_str]
    clauses = ["show_id = %s"]
    if status != "all":
        clauses.append("status = %s")
        params.append(status)
    if entity_type != "all":
        clauses.append("entity_type = %s")
        params.append(entity_type)

    return pg.fetch_all(
        f"""
        SELECT *
        FROM core.entity_links
        WHERE {' AND '.join(clauses)}
        ORDER BY link_group, season_number DESC, created_at DESC
        """,
        params,
    )


@router.post("/{show_id}/links")
def create_show_link(
    show_id: UUID,
    payload: LinkCreateRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    actor = str(admin.get("email") or admin.get("id") or "admin")
    return _upsert_link(
        db,
        show_id=show_id_str,
        entity_type=payload.entity_type,
        entity_id=str(payload.entity_id),
        link_group=payload.link_group,
        link_kind=payload.link_kind.strip().lower(),
        url=str(payload.url),
        label=payload.label,
        season_number=int(payload.season_number or 0),
        status=payload.status,
        confidence=payload.confidence,
        source=payload.source,
        discovered_by="manual",
        metadata=payload.metadata,
        actor=actor,
    )


@router.patch("/{show_id}/links/{link_id}")
def patch_show_link(
    show_id: UUID,
    link_id: UUID,
    payload: LinkPatchRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    actor = str(admin.get("email") or admin.get("id") or "admin")

    get_response = (
        db.schema("core")
        .table("entity_links")
        .select("*")
        .eq("id", str(link_id))
        .eq("show_id", show_id_str)
        .limit(1)
        .execute()
    )
    rows = get_list_result(get_response, "fetching entity link")
    if not rows:
        raise HTTPException(status_code=404, detail="Link not found")
    current = rows[0]

    updates = payload.model_dump(exclude_unset=True)
    if "url" in updates and updates["url"] is not None:
        updates["url_key"] = _url_key(str(updates["url"]))
    updates["updated_by"] = actor

    response = (
        db.schema("core")
        .table("entity_links")
        .update(updates)
        .eq("id", str(link_id))
        .eq("show_id", show_id_str)
        .execute()
    )
    updated_rows = get_list_result(response, "updating entity link")
    return updated_rows[0] if updated_rows else {**current, **updates}


@router.delete("/{show_id}/links/{link_id}")
def delete_show_link(
    show_id: UUID,
    link_id: UUID,
    db: SupabaseAdminClient,
    _: AdminUser,
) -> dict[str, Any]:
    response = (
        db.schema("core")
        .table("entity_links")
        .delete()
        .eq("id", str(link_id))
        .eq("show_id", str(show_id))
        .execute()
    )
    rows = get_list_result(response, "deleting entity link")
    if not rows:
        raise HTTPException(status_code=404, detail="Link not found")
    return {"deleted": True, "id": str(link_id)}
