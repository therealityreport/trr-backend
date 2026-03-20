from __future__ import annotations

import logging
import os
import re
import time
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.db import pg
from trr_backend.ingestion.show_cast_matrix_scraper import (
    build_default_fandom_url,
    build_default_wikipedia_url,
    build_person_fandom_url,
    build_person_wikipedia_url,
    extract_relationship_data_from_fandom_html,
    extract_relationship_data_from_wikipedia_html,
    infer_relationship_role,
    is_missing_fandom_page,
    is_missing_wikipedia_page,
    merge_cast_matrices,
    parse_fandom_cast_matrix_html,
    parse_wikipedia_cast_matrix_html,
    try_fetch_html,
)
from trr_backend.scraping.bravo_parser import parse_person_page

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-roles"])

_CANONICAL_ROLE_ORDER = [
    "Housewife",
    "Friend",
    "Guest",
    "Husband",
    "Ex-Husband",
    "Boyfriend",
    "Ex-Boyfriend",
    "Fiance",
    "Ex-Fiance",
    "Kid",
]
_SYNC_SOURCE_CAST = "cast_matrix_sync"
_SYNC_SOURCE_RELATIONSHIP = "cast_matrix_relationship_sync"
_SYNC_SOURCE_KID = "cast_matrix_kid_sync"
_ALL_SYNC_SOURCES = [_SYNC_SOURCE_CAST, _SYNC_SOURCE_RELATIONSHIP, _SYNC_SOURCE_KID]
CAST_ROLE_MEMBERS_PERF_LOGS_ENABLED = (
    re.match(
        r"^(1|true)$",
        str(os.getenv("TRR_CAST_ROLE_MEMBERS_PERF_LOGS", "0")).strip().lower(),
    )
    is not None
)


class RoleCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    sort_order: int = 0


class RolePatchRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=80)
    is_active: bool | None = None
    sort_order: int | None = None


class CastRoleAssignRequest(BaseModel):
    season_number: int | None = Field(default=None, ge=0, le=200)
    role_ids: list[UUID] = Field(default_factory=list)
    source: str = "manual"
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)


class CastMatrixSyncRequest(BaseModel):
    season_numbers: list[int] = Field(default_factory=list)
    include_relationship_roles: bool = True
    include_bravo_links: bool = True
    include_bravo_images: bool = True
    dry_run: bool = False


def _normalize_role_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower().strip()).strip("_")


def _normalize_name(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _show_exists(show_id: str) -> bool:
    row = pg.fetch_one("SELECT id FROM core.shows WHERE id = %s", [show_id])
    return bool(row)


def _show_metadata(show_id: str) -> dict[str, Any]:
    row = pg.fetch_one(
        """
        SELECT id::text AS id, name, networks
        FROM core.shows
        WHERE id = %s
        """,
        [show_id],
    )
    if not row:
        raise HTTPException(status_code=404, detail="Show not found")
    return row


def _is_bravo_show(show: dict[str, Any]) -> bool:
    networks = show.get("networks") if isinstance(show.get("networks"), list) else []
    return any(str(network).strip().lower() == "bravo" for network in networks)


def _load_show_cast_people(show_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT
          sc.person_id::text AS person_id,
          COALESCE(p.full_name, sc.cast_member_name) AS person_name,
          sc.cast_member_name,
          p.full_name,
          cf.source_url AS fandom_url,
          (
            SELECT el.url
            FROM core.entity_links el
            WHERE el.show_id = sc.show_id
              AND el.entity_type = 'person'
              AND el.entity_id = sc.person_id
              AND el.link_kind = 'fandom'
              AND el.status <> 'rejected'
            LIMIT 1
          ) AS fandom_link_url,
          (
            SELECT el.url
            FROM core.entity_links el
            WHERE el.show_id = sc.show_id
              AND el.entity_type = 'person'
              AND el.entity_id = sc.person_id
              AND el.link_kind = 'wikipedia'
              AND el.status <> 'rejected'
            LIMIT 1
          ) AS wikipedia_url
        FROM core.v_show_cast sc
        LEFT JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.cast_fandom cf ON cf.person_id = sc.person_id AND cf.source = 'fandom'
        WHERE sc.show_id = %s
        """,
        [show_id],
    )


def _build_person_lookup(cast_people: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, str]]:
    by_norm_name: dict[str, str] = {}
    by_person_id_name: dict[str, str] = {}

    for row in cast_people:
        person_id = str(row.get("person_id") or "").strip()
        if not person_id:
            continue
        name_candidates = [
            str(row.get("person_name") or "").strip(),
            str(row.get("full_name") or "").strip(),
            str(row.get("cast_member_name") or "").strip(),
        ]
        canonical_name = next((name for name in name_candidates if name), person_id)
        by_person_id_name[person_id] = canonical_name

        for candidate in name_candidates:
            key = _normalize_name(candidate)
            if key and key not in by_norm_name:
                by_norm_name[key] = person_id

    return by_norm_name, by_person_id_name


def _match_person_id(name: str, by_norm_name: dict[str, str]) -> str | None:
    key = _normalize_name(name)
    if not key:
        return None
    if key in by_norm_name:
        return by_norm_name[key]

    for norm, person_id in by_norm_name.items():
        if key in norm or norm in key:
            return person_id
    return None


def _load_role_ids(show_id: str) -> dict[str, str]:
    rows = pg.fetch_all(
        """
        SELECT id::text AS id, name
        FROM core.show_role_catalog
        WHERE show_id = %s
        """,
        [show_id],
    )
    return {
        str(row.get("name") or "").strip().lower(): str(row.get("id") or "").strip()
        for row in rows
        if row.get("id") and row.get("name")
    }


def _ensure_canonical_roles(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    actor: str,
    needed_roles: set[str],
    dry_run: bool,
) -> dict[str, str]:
    existing = _load_role_ids(show_id)
    for index, role_name in enumerate(_CANONICAL_ROLE_ORDER):
        if role_name not in needed_roles:
            continue
        if role_name.lower() in existing:
            continue
        if dry_run:
            continue
        row = {
            "show_id": show_id,
            "name": role_name,
            "normalized_name": _normalize_role_name(role_name),
            "sort_order": index,
            "is_active": True,
            "created_by": actor,
            "updated_by": actor,
        }
        response = (
            db.schema("core").table("show_role_catalog").upsert(row, on_conflict="show_id,normalized_name").execute()
        )
        get_list_result(response, "upserting canonical show role")

    return _load_role_ids(show_id)


def _load_existing_bravo_profile_links(show_id: str) -> dict[str, str]:
    rows = pg.fetch_all(
        """
        SELECT entity_id::text AS person_id, url
        FROM core.entity_links
        WHERE show_id = %s
          AND entity_type = 'person'
          AND link_kind = 'bravo_profile'
          AND status <> 'rejected'
        """,
        [show_id],
    )
    mapping: dict[str, str] = {}
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        url = str(row.get("url") or "").strip()
        if person_id and url and person_id not in mapping:
            mapping[person_id] = url
    return mapping


def _upsert_bravo_profile_links(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    actor: str,
    person_ids: set[str],
    person_names_by_id: dict[str, str],
    dry_run: bool,
) -> tuple[int, dict[str, str]]:
    from api.routers import admin_show_links

    existing = _load_existing_bravo_profile_links(show_id)
    upserted = 0

    for person_id in sorted(person_ids):
        person_name = person_names_by_id.get(person_id)
        if not person_name:
            continue
        slug = re.sub(r"[^a-z0-9]+", "-", person_name.lower().strip()).strip("-")
        if not slug:
            continue
        url = f"https://www.bravotv.com/people/{slug}"
        if dry_run:
            if existing.get(person_id) != url:
                upserted += 1
            existing[person_id] = url
            continue

        admin_show_links._upsert_link(
            db,
            show_id=show_id,
            entity_type="person",
            entity_id=person_id,
            link_group="official",
            link_kind="bravo_profile",
            url=url,
            label=f"{person_name} Bravo profile",
            season_number=0,
            status="pending",
            confidence=0.8,
            source="cast_matrix_sync",
            discovered_by="cast_matrix_sync",
            metadata={},
            actor=actor,
        )
        upserted += 1
        existing[person_id] = url

    return upserted, existing


def _person_has_bravo_profile_image(person_id: str) -> bool:
    row = pg.fetch_one(
        """
        SELECT 1
        FROM core.media_links ml
        WHERE ml.entity_type = 'person'
          AND ml.entity_id = %s::uuid
          AND ml.kind = 'gallery'
          AND lower(coalesce(ml.context->>'context_section', '')) = 'bravo_profile'
        LIMIT 1
        """,
        [person_id],
    )
    return bool(row)


def _import_missing_bravo_profile_images(
    *,
    db: SupabaseAdminClient,
    admin_user: AdminUser,
    show_id: str,
    person_ids: set[str],
    person_names_by_id: dict[str, str],
    profile_links_by_person: dict[str, str],
    dry_run: bool,
) -> tuple[int, int]:
    from api.routers import admin_show_bravo

    imported = 0
    skipped = 0

    for person_id in sorted(person_ids):
        if _person_has_bravo_profile_image(person_id):
            skipped += 1
            continue

        profile_url = profile_links_by_person.get(person_id)
        if not profile_url:
            skipped += 1
            continue

        try:
            profile = parse_person_page(profile_url)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to fetch Bravo profile page for person_id=%s", person_id)
            skipped += 1
            continue

        if not dry_run:
            pg.execute_returning(
                """
                UPDATE core.entity_links
                SET
                  status = 'approved',
                  confidence = GREATEST(COALESCE(confidence, 0), 0.9),
                  updated_at = NOW()
                WHERE show_id = %s
                  AND entity_type = 'person'
                  AND entity_id = %s::uuid
                  AND link_kind = 'bravo_profile'
                  AND status <> 'rejected'
                RETURNING id
                """,
                [show_id, person_id],
            )

        hero_image_url = str(profile.get("hero_image_url") or "").strip()
        person_url = str(profile.get("canonical_url") or profile_url).strip() or profile_url
        if not hero_image_url:
            skipped += 1
            continue

        if dry_run:
            imported += 1
            continue

        result = admin_show_bravo._import_bravo_person_image(
            db=db,
            admin_user=admin_user,
            show_id=show_id,
            season_id=None,
            season_number=None,
            person_id=person_id,
            person_url=person_url,
            hero_image_url=hero_image_url,
            person_name=person_names_by_id.get(person_id),
        )
        imported += int(result.get("imported") or 0)
        skipped += int(result.get("skipped") or 0)

    return imported, skipped


def _build_relationship_source_urls(show_name: str, row: dict[str, Any], person_name: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _append(kind: str, raw: str | None) -> None:
        url = str(raw or "").strip()
        if not url:
            return
        key = f"{kind}:{url.lower()}"
        if key in seen:
            return
        seen.add(key)
        out.append((kind, url))

    _append("fandom", str(row.get("fandom_url") or "").strip())
    _append("fandom", str(row.get("fandom_link_url") or "").strip())
    if "real housewives" in show_name.lower():
        _append("fandom", build_person_fandom_url(person_name))

    _append("wikipedia", str(row.get("wikipedia_url") or "").strip())
    _append("wikipedia", build_person_wikipedia_url(person_name))
    return out


def _resolve_season_ids(show_id: str) -> dict[int, str]:
    rows = pg.fetch_all(
        """
        SELECT id::text AS id, season_number
        FROM core.seasons
        WHERE show_id = %s
        """,
        [show_id],
    )
    return {
        int(row.get("season_number")): str(row.get("id") or "").strip()
        for row in rows
        if isinstance(row.get("season_number"), int) and row.get("id")
    }


def _count_replaceable_assignments(show_id: str, delete_sources: list[str], season_numbers: list[int]) -> int:
    if not delete_sources:
        return 0

    if season_numbers:
        row = pg.fetch_one(
            """
            SELECT COUNT(*)::int AS count
            FROM core.show_cast_role_assignments
            WHERE show_id = %s
              AND source = ANY(%s::text[])
              AND (season_number = ANY(%s::int[]) OR season_number = 0)
            """,
            [show_id, delete_sources, season_numbers],
        )
    else:
        row = pg.fetch_one(
            """
            SELECT COUNT(*)::int AS count
            FROM core.show_cast_role_assignments
            WHERE show_id = %s
              AND source = ANY(%s::text[])
            """,
            [show_id, delete_sources],
        )
    return int((row or {}).get("count") or 0)


def _delete_replaceable_assignments(show_id: str, delete_sources: list[str], season_numbers: list[int]) -> int:
    if not delete_sources:
        return 0

    if season_numbers:
        rows = pg.execute_returning(
            """
            DELETE FROM core.show_cast_role_assignments
            WHERE show_id = %s
              AND source = ANY(%s::text[])
              AND (season_number = ANY(%s::int[]) OR season_number = 0)
            RETURNING id
            """,
            [show_id, delete_sources, season_numbers],
        )
    else:
        rows = pg.execute_returning(
            """
            DELETE FROM core.show_cast_role_assignments
            WHERE show_id = %s
              AND source = ANY(%s::text[])
            RETURNING id
            """,
            [show_id, delete_sources],
        )
    return len(rows)


def _build_role_matrix_assignments(
    *,
    merged_matrix: dict[str, dict[int, str]],
    by_norm_name: dict[str, str],
    season_filter: set[int],
) -> tuple[list[dict[str, Any]], set[str], list[str]]:
    assignments: list[dict[str, Any]] = []
    housewife_friend_person_ids: set[str] = set()
    unmatched: list[str] = []

    for source_name, season_roles in merged_matrix.items():
        person_id = _match_person_id(source_name, by_norm_name)
        if not person_id:
            unmatched.append(source_name)
            continue

        for season, role in season_roles.items():
            if season_filter and season not in season_filter:
                continue
            if role not in {"Housewife", "Friend", "Guest"}:
                continue
            if role in {"Housewife", "Friend"}:
                housewife_friend_person_ids.add(person_id)
            assignments.append(
                {
                    "person_id": person_id,
                    "season_number": int(season),
                    "role_name": role,
                    "source": _SYNC_SOURCE_CAST,
                    "confidence": 0.95,
                }
            )

    return assignments, housewife_friend_person_ids, unmatched


def _build_relationship_assignments(
    *,
    show_name: str,
    cast_people: list[dict[str, Any]],
    by_norm_name: dict[str, str],
    season_filter: set[int],
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    from api.routers import admin_show_links

    assignments: list[dict[str, Any]] = []
    unmatched_names: list[str] = []
    missing_season_evidence: list[str] = []

    for row in cast_people:
        source_person_id = str(row.get("person_id") or "").strip()
        person_name = str(row.get("person_name") or row.get("full_name") or row.get("cast_member_name") or "").strip()
        if not person_name:
            continue

        source_urls = _build_relationship_source_urls(show_name, row, person_name)
        if not source_urls:
            continue

        for source_kind, source_url in source_urls:
            validated_source_url, validation_outcome = admin_show_links._validate_person_knowledge_url(
                source_url,
                kind=source_kind,
                expected_name=person_name,
            )
            if validation_outcome != "valid" or not validated_source_url:
                if validation_outcome == "fetch_error":
                    logger.debug(
                        "Relationship source validation fetch failure for %s (%s)",
                        person_name,
                        source_url,
                    )
                continue

            html, final_url, error = try_fetch_html(validated_source_url)
            if not html:
                if error:
                    logger.debug(
                        "Skipping relationship fetch for %s (%s): %s",
                        person_name,
                        validated_source_url,
                        error,
                    )
                continue
            if source_kind == "wikipedia" and is_missing_wikipedia_page(html, final_url or validated_source_url):
                continue
            if source_kind == "fandom" and is_missing_fandom_page(html, final_url or validated_source_url):
                continue

            data = (
                extract_relationship_data_from_wikipedia_html(html)
                if source_kind == "wikipedia"
                else extract_relationship_data_from_fandom_html(html)
            )
            resolved_source_url = final_url or validated_source_url
            for detail in data.get("missing_season_evidence") or []:
                missing_season_evidence.append(f"{person_name}: {detail}")

            for partner_role in data.get("season_partner_roles") or []:
                season = int(partner_role.get("season") or 0)
                if season <= 0:
                    continue
                if season_filter and season not in season_filter:
                    continue
                role_name = str(partner_role.get("role") or "").strip()
                partner_name = str(partner_role.get("name") or "").strip()
                if not role_name or not partner_name:
                    continue
                if role_name not in {"Husband", "Ex-Husband", "Boyfriend", "Ex-Boyfriend", "Fiance", "Ex-Fiance"}:
                    inferred = infer_relationship_role(role_name)
                    if not inferred:
                        continue
                    role_name = inferred

                partner_id = _match_person_id(partner_name, by_norm_name)
                if not partner_id:
                    unmatched_names.append(partner_name)
                    continue
                if source_person_id and partner_id == source_person_id:
                    continue
                assignments.append(
                    {
                        "person_id": partner_id,
                        "season_number": season,
                        "role_name": role_name,
                        "source": _SYNC_SOURCE_RELATIONSHIP,
                        "confidence": 0.9,
                        "metadata": {
                            "relationship_from": person_name,
                            "source_url": resolved_source_url,
                        },
                    }
                )

            for partner_role in data.get("global_partner_roles") or []:
                role_name = str(partner_role.get("role") or "").strip()
                partner_name = str(partner_role.get("name") or "").strip()
                if not role_name or not partner_name:
                    continue
                if role_name not in {"Husband", "Ex-Husband", "Boyfriend", "Ex-Boyfriend", "Fiance", "Ex-Fiance"}:
                    inferred = infer_relationship_role(role_name)
                    if not inferred:
                        continue
                    role_name = inferred

                partner_id = _match_person_id(partner_name, by_norm_name)
                if not partner_id:
                    unmatched_names.append(partner_name)
                    continue
                if source_person_id and partner_id == source_person_id:
                    continue
                assignments.append(
                    {
                        "person_id": partner_id,
                        "season_number": 0,
                        "role_name": role_name,
                        "source": _SYNC_SOURCE_RELATIONSHIP,
                        "confidence": 0.9,
                        "metadata": {
                            "relationship_from": person_name,
                            "source_url": resolved_source_url,
                        },
                    }
                )

            for kid_name in data.get("kid_names") or []:
                kid_id = _match_person_id(str(kid_name), by_norm_name)
                if not kid_id:
                    unmatched_names.append(str(kid_name))
                    continue
                assignments.append(
                    {
                        "person_id": kid_id,
                        "season_number": 0,
                        "role_name": "Kid",
                        "source": _SYNC_SOURCE_KID,
                        "confidence": 0.9,
                        "metadata": {
                            "relationship_from": person_name,
                            "source_url": resolved_source_url,
                        },
                    }
                )

    return assignments, unmatched_names, missing_season_evidence


def _dedupe_assignments(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        season_number = int(row.get("season_number") or 0)
        role_name = str(row.get("role_name") or "").strip()
        if not person_id or not role_name:
            continue
        key = (person_id, season_number, role_name.lower())
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = dict(row)
            continue
        existing_source = str(existing.get("source") or "")
        incoming_source = str(row.get("source") or "")
        if existing_source != _SYNC_SOURCE_RELATIONSHIP and incoming_source == _SYNC_SOURCE_RELATIONSHIP:
            deduped[key] = dict(row)
    return list(deduped.values())


def sync_cast_matrix_for_show(
    *,
    show_id: str,
    payload: CastMatrixSyncRequest,
    db: SupabaseAdminClient,
    admin_user: AdminUser,
) -> dict[str, Any]:
    show = _show_metadata(show_id)
    show_name = str(show.get("name") or "").strip()
    actor = str((admin_user or {}).get("email") or (admin_user or {}).get("id") or "admin")

    wikipedia_url = build_default_wikipedia_url(show_name)
    fandom_url = build_default_fandom_url(show_name)

    wikipedia_html, wikipedia_final_url, _ = try_fetch_html(wikipedia_url)
    fandom_html, fandom_final_url, _ = try_fetch_html(fandom_url)

    wikipedia_matrix = parse_wikipedia_cast_matrix_html(wikipedia_html or "") if wikipedia_html else {}
    fandom_matrix = parse_fandom_cast_matrix_html(fandom_html or "") if fandom_html else {}
    merged_matrix = merge_cast_matrices(wikipedia_matrix, fandom_matrix)

    cast_people = _load_show_cast_people(show_id)
    by_norm_name, person_names_by_id = _build_person_lookup(cast_people)

    season_filter = {
        int(season) for season in payload.season_numbers if isinstance(season, int) and season > 0 and season <= 200
    }

    cast_assignments, housewife_friend_person_ids, unmatched_cast_names = _build_role_matrix_assignments(
        merged_matrix=merged_matrix,
        by_norm_name=by_norm_name,
        season_filter=season_filter,
    )

    relationship_assignments: list[dict[str, Any]] = []
    unmatched_relationship_names: list[str] = []
    missing_season_evidence: list[str] = []
    if payload.include_relationship_roles:
        (
            relationship_assignments,
            unmatched_relationship_names,
            missing_season_evidence,
        ) = _build_relationship_assignments(
            show_name=show_name,
            cast_people=cast_people,
            by_norm_name=by_norm_name,
            season_filter=season_filter,
        )

    combined_assignments = _dedupe_assignments([*cast_assignments, *relationship_assignments])
    needed_roles = {str(row.get("role_name") or "").strip() for row in combined_assignments}
    needed_roles.discard("")

    delete_sources = [_SYNC_SOURCE_CAST]
    if payload.include_relationship_roles:
        delete_sources.extend([_SYNC_SOURCE_RELATIONSHIP, _SYNC_SOURCE_KID])

    role_ids = _ensure_canonical_roles(
        db,
        show_id=show_id,
        actor=actor,
        needed_roles=needed_roles,
        dry_run=payload.dry_run,
    )

    auto_assignments_replaced = _count_replaceable_assignments(show_id, delete_sources, sorted(season_filter))
    if not payload.dry_run:
        auto_assignments_replaced = _delete_replaceable_assignments(show_id, delete_sources, sorted(season_filter))

    season_ids = _resolve_season_ids(show_id)
    rows_to_upsert: list[dict[str, Any]] = []
    for row in combined_assignments:
        role_name = str(row.get("role_name") or "").strip()
        role_id = role_ids.get(role_name.lower())
        if not role_id:
            continue
        season_number = int(row.get("season_number") or 0)
        rows_to_upsert.append(
            {
                "show_id": show_id,
                "person_id": row["person_id"],
                "season_id": season_ids.get(season_number) if season_number > 0 else None,
                "season_number": season_number,
                "role_id": role_id,
                "source": row.get("source") or _SYNC_SOURCE_CAST,
                "confidence": row.get("confidence"),
                "metadata": row.get("metadata") or {},
                "created_by": actor,
                "updated_by": actor,
            }
        )

    if not payload.dry_run:
        for row in rows_to_upsert:
            response = (
                db.schema("core")
                .table("show_cast_role_assignments")
                .upsert(row, on_conflict="show_id,person_id,season_number,role_id")
                .execute()
            )
            get_list_result(response, "upserting cast matrix role assignment")

    season_role_count = sum(1 for row in rows_to_upsert if row.get("source") == _SYNC_SOURCE_CAST)
    relationship_role_count = sum(1 for row in rows_to_upsert if row.get("source") == _SYNC_SOURCE_RELATIONSHIP)
    kid_role_count = sum(1 for row in rows_to_upsert if row.get("source") == _SYNC_SOURCE_KID)

    bravo_links_upserted = 0
    profile_links_by_person: dict[str, str] = {}
    if payload.include_bravo_links and _is_bravo_show(show):
        existing_housewife_friend_ids = {
            str(row.get("person_id") or "").strip()
            for row in pg.fetch_all(
                """
                SELECT DISTINCT sra.person_id::text AS person_id
                FROM core.show_cast_role_assignments sra
                JOIN core.show_role_catalog rc ON rc.id = sra.role_id
                WHERE sra.show_id = %s
                  AND lower(rc.name) IN ('housewife', 'friend')
                """,
                [show_id],
            )
            if row.get("person_id")
        }
        all_housewife_friend_ids = {
            person_id for person_id in [*housewife_friend_person_ids, *existing_housewife_friend_ids] if person_id
        }

        bravo_links_upserted, profile_links_by_person = _upsert_bravo_profile_links(
            db,
            show_id=show_id,
            actor=actor,
            person_ids=all_housewife_friend_ids,
            person_names_by_id=person_names_by_id,
            dry_run=payload.dry_run,
        )
    else:
        profile_links_by_person = _load_existing_bravo_profile_links(show_id)

    bravo_images_imported = 0
    bravo_images_skipped = 0
    if payload.include_bravo_images and _is_bravo_show(show) and profile_links_by_person:
        target_person_ids = set(profile_links_by_person.keys())
        bravo_images_imported, bravo_images_skipped = _import_missing_bravo_profile_images(
            db=db,
            admin_user=admin_user,
            show_id=show_id,
            person_ids=target_person_ids,
            person_names_by_id=person_names_by_id,
            profile_links_by_person=profile_links_by_person,
            dry_run=payload.dry_run,
        )

    deduped_unmatched_cast = sorted({name.strip() for name in unmatched_cast_names if name.strip()})
    deduped_unmatched_relationship = sorted(
        {name.strip() for name in unmatched_relationship_names if str(name).strip()}
    )
    deduped_missing_season_evidence = sorted(
        {detail.strip() for detail in missing_season_evidence if str(detail).strip()}
    )

    return {
        "show_id": show_id,
        "source_urls": {
            "wikipedia": wikipedia_final_url or wikipedia_url,
            "fandom": fandom_final_url or fandom_url,
        },
        "counts": {
            "season_role_assignments_upserted": season_role_count,
            "relationship_role_assignments_upserted": relationship_role_count,
            "global_kid_assignments_upserted": kid_role_count,
            "auto_assignments_replaced": auto_assignments_replaced,
            "bravo_links_upserted": bravo_links_upserted,
            "bravo_images_imported": bravo_images_imported,
            "bravo_images_skipped": bravo_images_skipped,
        },
        "unmatched": {
            "cast_names": deduped_unmatched_cast,
            "relationship_names": deduped_unmatched_relationship,
            "missing_season_evidence": deduped_missing_season_evidence,
        },
    }


@router.get("/{show_id}/roles")
def list_show_roles(
    show_id: UUID,
    _: AdminUser,
    include_inactive: bool = Query(default=False),
) -> list[dict[str, Any]]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    params: list[Any] = [show_id_str]
    filter_sql = ""
    if not include_inactive:
        filter_sql = "AND is_active = true"

    return pg.fetch_all(
        f"""
        SELECT *
        FROM core.show_role_catalog
        WHERE show_id = %s
        {filter_sql}
        ORDER BY sort_order ASC, name ASC
        """,
        params,
    )


@router.post("/{show_id}/roles")
def create_show_role(
    show_id: UUID,
    payload: RoleCreateRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    name = payload.name.strip()
    normalized = _normalize_role_name(name)
    if not normalized:
        raise HTTPException(status_code=400, detail="Role name is invalid")

    actor = str(admin.get("email") or admin.get("id") or "admin")
    row = {
        "show_id": show_id_str,
        "name": name,
        "normalized_name": normalized,
        "sort_order": payload.sort_order,
        "is_active": True,
        "created_by": actor,
        "updated_by": actor,
    }
    response = db.schema("core").table("show_role_catalog").upsert(row, on_conflict="show_id,normalized_name").execute()
    rows = get_list_result(response, "upserting show role")
    return rows[0] if rows else row


@router.patch("/{show_id}/roles/{role_id}")
def patch_show_role(
    show_id: UUID,
    role_id: UUID,
    payload: RolePatchRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided")

    if "name" in updates and updates["name"] is not None:
        updates["name"] = updates["name"].strip()
        updates["normalized_name"] = _normalize_role_name(updates["name"])

    updates["updated_by"] = str(admin.get("email") or admin.get("id") or "admin")

    response = (
        db.schema("core")
        .table("show_role_catalog")
        .update(updates)
        .eq("id", str(role_id))
        .eq("show_id", str(show_id))
        .execute()
    )
    rows = get_list_result(response, "updating show role")
    if not rows:
        raise HTTPException(status_code=404, detail="Role not found")
    return rows[0]


@router.post("/{show_id}/cast/{person_id}/roles")
def replace_cast_roles(
    show_id: UUID,
    person_id: UUID,
    payload: CastRoleAssignRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    person_id_str = str(person_id)
    season_number = int(payload.season_number or 0)
    actor = str(admin.get("email") or admin.get("id") or "admin")

    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    delete_resp = (
        db.schema("core")
        .table("show_cast_role_assignments")
        .delete()
        .eq("show_id", show_id_str)
        .eq("person_id", person_id_str)
        .eq("season_number", season_number)
        .execute()
    )
    get_list_result(delete_resp, "deleting existing role assignments")

    if not payload.role_ids:
        return {
            "show_id": show_id_str,
            "person_id": person_id_str,
            "season_number": season_number,
            "assigned": 0,
            "roles": [],
        }

    role_rows = pg.fetch_all(
        """
        SELECT id::text AS id
        FROM core.show_role_catalog
        WHERE show_id = %s
          AND id = ANY(%s::uuid[])
          AND is_active = true
        """,
        [show_id_str, [str(role_id) for role_id in payload.role_ids]],
    )
    valid_role_ids = [row["id"] for row in role_rows]

    rows = [
        {
            "show_id": show_id_str,
            "person_id": person_id_str,
            "season_number": season_number,
            "role_id": role_id,
            "source": payload.source,
            "confidence": payload.confidence,
            "created_by": actor,
            "updated_by": actor,
        }
        for role_id in valid_role_ids
    ]
    if rows:
        insert_resp = db.schema("core").table("show_cast_role_assignments").insert(rows).execute()
        get_list_result(insert_resp, "inserting role assignments")

    assigned_roles = pg.fetch_all(
        """
        SELECT sra.*, rc.name AS role_name
        FROM core.show_cast_role_assignments sra
        JOIN core.show_role_catalog rc ON rc.id = sra.role_id
        WHERE sra.show_id = %s
          AND sra.person_id = %s
          AND sra.season_number = %s
        ORDER BY rc.sort_order ASC, rc.name ASC
        """,
        [show_id_str, person_id_str, season_number],
    )

    return {
        "show_id": show_id_str,
        "person_id": person_id_str,
        "season_number": season_number,
        "assigned": len(assigned_roles),
        "roles": assigned_roles,
    }


@router.post("/{show_id}/cast-matrix/sync")
def sync_cast_matrix(
    show_id: UUID,
    payload: CastMatrixSyncRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")
    return sync_cast_matrix_for_show(show_id=show_id_str, payload=payload, db=db, admin_user=admin)


@router.get("/{show_id}/cast-role-members")
def list_cast_with_roles(
    show_id: UUID,
    _: AdminUser,
    sort_by: str = Query(default="episodes"),
    order: str = Query(default="desc"),
    seasons: str | None = Query(default=None),
    roles: str | None = Query(default=None),
    has_image: bool | None = Query(default=None),
    exclude_zero_episode_members: bool = Query(default=False),
    archive_mode: str = Query(default="all"),
) -> list[dict[str, Any]]:
    request_started_at = time.perf_counter()
    base_rows_query_ms = 0.0
    role_aggregate_query_ms = 0.0
    scoped_totals_query_ms = 0.0
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")
    archive_mode = archive_mode.lower().strip()
    if archive_mode not in {"all", "exclude", "only"}:
        raise HTTPException(status_code=400, detail="archive_mode must be one of: all, exclude, only")

    season_numbers = [int(value) for value in (seasons or "").split(",") if value.strip().isdigit()]
    role_names = [value.strip().lower() for value in (roles or "").split(",") if value.strip()]

    rows_query_started_at = time.perf_counter()
    rows = pg.fetch_all(
        """
        SELECT
          c.show_id,
          c.person_id,
          c.person_name,
          c.total_episodes,
          c.archive_episodes,
          c.seasons_appeared,
          c.season_numbers,
          c.latest_season,
          c.roles,
          cp.display_url AS photo_url,
          COALESCE(po.full_name_override, c.person_name) AS display_name,
          COALESCE(
              NULLIF(po.instagram_handle, ''),
              ct.instagram_id,
              p.external_ids->>'instagram_id',
              p.external_ids->>'instagram'
          ) AS instagram_handle,
          po.tiktok_handle,
          po.twitter_handle,
          po.youtube_handle
        FROM core.v_show_cast_roles_enriched c
        LEFT JOIN core.people p ON p.id = c.person_id
        LEFT JOIN core.people_overrides po ON po.person_id = c.person_id
        LEFT JOIN core.cast_tmdb ct ON ct.person_id = c.person_id
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(ph.hosted_url, ph.image_url, ph.url, ph.thumb_url) AS display_url
          FROM core.cast_photos ph
          WHERE ph.person_id = c.person_id
          ORDER BY ph.gallery_index ASC NULLS LAST
          LIMIT 1
        ) cp ON true
        WHERE c.show_id = %s
        """,
        [show_id_str],
    )
    base_rows_query_ms = (time.perf_counter() - rows_query_started_at) * 1000.0

    role_rows_query_started_at = time.perf_counter()
    if season_numbers:
        role_rows = pg.fetch_all(
            """
            SELECT
              sra.person_id::text AS person_id,
              array_remove(array_agg(DISTINCT rc.name), NULL) AS role_names,
              array_remove(array_agg(DISTINCT sra.season_number), NULL) AS assignment_seasons
            FROM core.show_cast_role_assignments sra
            JOIN core.show_role_catalog rc ON rc.id = sra.role_id
            WHERE sra.show_id = %s
              AND rc.is_active = true
              AND (sra.season_number = ANY(%s::int[]) OR sra.season_number = 0)
            GROUP BY sra.person_id
            """,
            [show_id_str, season_numbers],
        )
    else:
        role_rows = pg.fetch_all(
            """
            SELECT
              sra.person_id::text AS person_id,
              array_remove(array_agg(DISTINCT rc.name), NULL) AS role_names,
              array_remove(array_agg(DISTINCT sra.season_number), NULL) AS assignment_seasons
            FROM core.show_cast_role_assignments sra
            JOIN core.show_role_catalog rc ON rc.id = sra.role_id
            WHERE sra.show_id = %s
              AND rc.is_active = true
            GROUP BY sra.person_id
            """,
            [show_id_str],
        )
    role_aggregate_query_ms = (time.perf_counter() - role_rows_query_started_at) * 1000.0

    role_map: dict[str, set[str]] = {}
    role_season_map: dict[str, set[int]] = {}
    for row in role_rows:
        person_id = str(row.get("person_id") or "").strip()
        if not person_id:
            continue
        role_names_for_person = {
            str(value).strip()
            for value in (row.get("role_names") or [])
            if isinstance(value, str) and str(value).strip()
        }
        assignment_seasons = {int(value) for value in (row.get("assignment_seasons") or []) if isinstance(value, int)}
        if role_names_for_person:
            role_map[person_id] = role_names_for_person
        if assignment_seasons:
            role_season_map[person_id] = assignment_seasons

    filtered: list[dict[str, Any]] = []
    for row in rows:
        person_id = str(row.get("person_id") or "")
        fallback_roles = [
            str(value).strip() for value in (row.get("roles") or []) if isinstance(value, str) and str(value).strip()
        ]
        selected_roles = sorted(role_map.get(person_id, set())) if person_id in role_map else fallback_roles
        row["roles"] = selected_roles

        row_roles_lc = [value.lower() for value in selected_roles]
        episode_seasons = [
            int(value) for value in (row.get("season_numbers") or []) if isinstance(value, int) and value > 0
        ]
        matched_assignment_seasons = {
            int(value) for value in role_season_map.get(person_id, set()) if isinstance(value, int) and value > 0
        }
        combined_seasons = sorted({*episode_seasons, *matched_assignment_seasons})

        latest_season = max(combined_seasons) if combined_seasons else int(row.get("latest_season") or 0)
        row["latest_season"] = latest_season if latest_season > 0 else None
        row["seasons_appeared"] = len(combined_seasons) if combined_seasons else int(row.get("seasons_appeared") or 0)
        row["season_numbers"] = combined_seasons if combined_seasons else episode_seasons

        if season_numbers:
            has_episode_match = any(value in episode_seasons for value in season_numbers)
            has_assignment_match = any(value in matched_assignment_seasons for value in season_numbers)
            if not has_episode_match and not has_assignment_match:
                continue
        if role_names and not any(role in row_roles_lc for role in role_names):
            continue
        if has_image is True and not row.get("photo_url"):
            continue
        if has_image is False and row.get("photo_url"):
            continue
        archive_episodes = int(row.get("archive_episodes") or 0)
        regular_episodes = int(row.get("total_episodes") or 0)
        if archive_mode == "exclude" and archive_episodes > 0:
            continue
        if archive_mode == "only" and not (archive_episodes > 0 and regular_episodes <= 0):
            continue
        filtered.append(row)

    if season_numbers and filtered:
        scoped_person_ids = [
            str(row.get("person_id") or "").strip() for row in filtered if str(row.get("person_id") or "").strip()
        ]
        if scoped_person_ids:
            scoped_totals_query_started_at = time.perf_counter()
            scoped_rows = pg.fetch_all(
                """
                SELECT
                  person_id::text AS person_id,
                  COUNT(DISTINCT episode_id)::int AS total_episodes
                FROM core.v_episode_credits
                WHERE show_id = %s
                  AND person_id = ANY(%s::uuid[])
                  AND season_number = ANY(%s::int[])
                  AND COALESCE(appearance_type, 'appears') <> 'archive_footage'
                GROUP BY person_id
                """,
                [show_id_str, scoped_person_ids, season_numbers],
            )
            scoped_totals_query_ms = (time.perf_counter() - scoped_totals_query_started_at) * 1000.0
            scoped_totals = {
                str(item.get("person_id") or "").strip(): int(item.get("total_episodes") or 0)
                for item in scoped_rows
                if item.get("person_id")
            }
            for row in filtered:
                person_id = str(row.get("person_id") or "").strip()
                if not person_id:
                    continue
                row["total_episodes"] = scoped_totals.get(person_id, 0)

    if exclude_zero_episode_members:
        filtered = [row for row in filtered if int(row.get("total_episodes") or 0) > 0]

    reverse = order.lower() != "asc"
    if sort_by == "name":
        filtered.sort(key=lambda item: str(item.get("person_name") or "").lower(), reverse=reverse)
    elif sort_by == "season":
        filtered.sort(key=lambda item: int(item.get("latest_season") or 0), reverse=reverse)
    else:
        filtered.sort(key=lambda item: int(item.get("total_episodes") or 0), reverse=reverse)

    if CAST_ROLE_MEMBERS_PERF_LOGS_ENABLED:
        total_query_ms = (time.perf_counter() - request_started_at) * 1000.0
        logger.info(
            "cast-role-members timings show_id=%s rows=%d filtered=%d seasons=%d role_filters=%d "
            "base_rows_ms=%.1f role_aggregate_ms=%.1f scoped_totals_ms=%.1f total_ms=%.1f",
            show_id_str,
            len(rows),
            len(filtered),
            len(season_numbers),
            len(role_names),
            base_rows_query_ms,
            role_aggregate_query_ms,
            scoped_totals_query_ms,
            total_query_ms,
        )

    return filtered
