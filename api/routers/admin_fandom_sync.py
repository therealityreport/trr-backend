"""Admin endpoints for Sync by Fandom workflows."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.ingestion.fandom_person_scraper import fetch_fandom_person_html, parse_fandom_person_html
from trr_backend.ingestion.fandom_season_scraper import parse_fandom_season_html
from trr_backend.integrations.fandom import is_allowlisted_fandom_domain, load_fandom_community_allowlist
from trr_backend.integrations.fandom_discovery import FandomCandidatePage, discover_fandom_candidate_pages
from trr_backend.integrations.openai_fandom_cleanup import cleanup_fandom_payload_with_openai
from trr_backend.repositories.cast_fandom import upsert_cast_fandom
from trr_backend.repositories.season_fandom import list_season_fandom, upsert_season_fandom

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin-fandom-sync"])


class FandomSyncRequest(BaseModel):
    manual_page_urls: list[str] = Field(default_factory=list)
    max_candidates: int = Field(default=8, ge=1, le=30)
    include_allpages_scan: bool = False
    allpages_max_pages: int = Field(default=2, ge=1, le=20)
    community_domains: list[str] = Field(default_factory=list)
    save_source_variants: bool = True


class FandomSyncCommitRequest(FandomSyncRequest):
    selected_page_urls: list[str] = Field(default_factory=list)


def _to_iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_domains(requested: list[str]) -> tuple[str, ...]:
    configured = tuple(requested) if requested else load_fandom_community_allowlist()
    domains: list[str] = []
    for value in configured:
        parsed = urlparse(str(value).strip())
        domain = (parsed.netloc or parsed.path).strip().lower().strip(".")
        if domain.startswith("www."):
            domain = domain[4:]
        if not domain:
            continue
        if not is_allowlisted_fandom_domain(domain, allowlist=load_fandom_community_allowlist()):
            raise HTTPException(status_code=400, detail=f"Domain is not allowlisted: {domain}")
        if domain not in domains:
            domains.append(domain)
    return tuple(domains)


def _validate_manual_urls(manual_urls: list[str], *, allowlist: tuple[str, ...]) -> list[str]:
    out: list[str] = []
    for raw in manual_urls:
        value = str(raw or "").strip()
        if not value:
            continue
        if not is_allowlisted_fandom_domain(value, allowlist=allowlist):
            raise HTTPException(status_code=400, detail=f"Manual URL is not allowlisted: {value}")
        out.append(value)
    return out


def _row_has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True


def _dedupe_dict_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        marker = str(row)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(row)
    return deduped


def _merge_person_payload(
    parsed_rows: list[dict[str, Any]],
    *,
    selected_pages: list[dict[str, Any]],
    entity_label: str,
    use_source_variants: bool,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    if not parsed_rows:
        return {}, ["No valid Fandom pages parsed."]

    canonical_fields = [
        "full_name",
        "birthdate",
        "birthdate_display",
        "gender",
        "resides_in",
        "hair_color",
        "eye_color",
        "height_display",
        "weight_display",
        "romances",
        "family",
        "friends",
        "enemies",
        "installment",
        "installment_url",
        "main_seasons_display",
        "summary",
        "taglines",
        "reunion_seating",
        "trivia",
        "dynamic_sections",
        "bio_card",
        "casting_summary",
    ]
    aggregated: dict[str, Any] = {}
    citations: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    source_variants: dict[str, Any] = {}

    for field in canonical_fields:
        field_values: list[dict[str, Any]] = []
        for row in parsed_rows:
            value = row.get(field)
            if not _row_has_value(value):
                continue
            field_values.append(
                {
                    "value": value,
                    "source_url": row.get("source_url"),
                    "page_title": row.get("page_title"),
                }
            )
        if not field_values:
            continue
        chosen = field_values[0]
        aggregated[field] = chosen["value"]
        citations.append(
            {
                "field": field,
                "source_url": chosen.get("source_url"),
                "page_title": chosen.get("page_title"),
            }
        )
        unique_values = {str(item.get("value")) for item in field_values}
        if len(unique_values) > 1:
            conflicts.append(
                {
                    "field": field,
                    "variants": field_values,
                }
            )
        if use_source_variants:
            source_variants[field] = field_values

    cleanup_payload, ai_model = cleanup_fandom_payload_with_openai(
        entity_kind="person",
        entity_label=entity_label,
        aggregated=aggregated,
        source_variants=parsed_rows,
    )
    if cleanup_payload:
        if _row_has_value(cleanup_payload.get("casting_summary")):
            aggregated["casting_summary"] = cleanup_payload.get("casting_summary")
        if isinstance(cleanup_payload.get("bio_card"), dict):
            aggregated["bio_card"] = cleanup_payload.get("bio_card")
        if isinstance(cleanup_payload.get("sections"), list):
            aggregated["dynamic_sections"] = cleanup_payload.get("sections")
        if isinstance(cleanup_payload.get("citations"), list):
            citations = cleanup_payload.get("citations")
        if isinstance(cleanup_payload.get("conflicts"), list):
            conflicts = cleanup_payload.get("conflicts")
        overrides = cleanup_payload.get("canonical_field_overrides")
        if isinstance(overrides, dict):
            for key, value in overrides.items():
                if _row_has_value(value):
                    aggregated[str(key)] = value
    else:
        warnings.append("OpenAI cleanup unavailable; using deterministic merge.")
        ai_model = None

    primary = parsed_rows[0]
    result = {
        "source": "fandom",
        "source_url": primary.get("source_url") or (selected_pages[0]["url"] if selected_pages else ""),
        "page_title": primary.get("page_title"),
        "page_revision_id": primary.get("page_revision_id"),
        "scraped_at": _to_iso_now(),
        "citations": citations,
        "conflicts": conflicts,
        "source_variants": source_variants if use_source_variants else None,
        "ai_model": ai_model,
        "ai_generated_at": _to_iso_now() if ai_model else None,
    }
    result.update(aggregated)
    return result, warnings


def _merge_season_payload(
    parsed_rows: list[dict[str, Any]],
    *,
    selected_pages: list[dict[str, Any]],
    entity_label: str,
    use_source_variants: bool,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    if not parsed_rows:
        return {}, ["No valid season Fandom pages parsed."]

    summary = next((row.get("summary") for row in parsed_rows if _row_has_value(row.get("summary"))), None)
    sections: list[dict[str, Any]] = []
    for row in parsed_rows:
        payload = row.get("dynamic_sections")
        if isinstance(payload, list):
            for section in payload:
                if isinstance(section, dict):
                    sections.append(section)
    sections = _dedupe_dict_rows(sections)

    citations = [
        {
            "field": "summary",
            "source_url": row.get("source_url"),
            "page_title": row.get("page_title"),
        }
        for row in parsed_rows
        if _row_has_value(row.get("summary"))
    ]
    conflicts = []
    unique_summaries = {str(row.get("summary")) for row in parsed_rows if _row_has_value(row.get("summary"))}
    if len(unique_summaries) > 1:
        conflicts.append(
            {
                "field": "summary",
                "variants": [
                    {
                        "value": row.get("summary"),
                        "source_url": row.get("source_url"),
                        "page_title": row.get("page_title"),
                    }
                    for row in parsed_rows
                    if _row_has_value(row.get("summary"))
                ],
            }
        )

    cleanup_payload, ai_model = cleanup_fandom_payload_with_openai(
        entity_kind="season",
        entity_label=entity_label,
        aggregated={"summary": summary, "dynamic_sections": sections},
        source_variants=parsed_rows,
    )
    if cleanup_payload:
        if _row_has_value(cleanup_payload.get("casting_summary")) and not _row_has_value(summary):
            summary = cleanup_payload.get("casting_summary")
        if isinstance(cleanup_payload.get("sections"), list):
            sections = cleanup_payload.get("sections")
        if isinstance(cleanup_payload.get("citations"), list):
            citations = cleanup_payload.get("citations")
        if isinstance(cleanup_payload.get("conflicts"), list):
            conflicts = cleanup_payload.get("conflicts")
    else:
        warnings.append("OpenAI cleanup unavailable; using deterministic merge.")
        ai_model = None

    source_variants = parsed_rows if use_source_variants else None
    primary = parsed_rows[0]
    return (
        {
            "source": "fandom",
            "source_url": primary.get("source_url") or (selected_pages[0]["url"] if selected_pages else ""),
            "page_title": primary.get("page_title"),
            "page_revision_id": primary.get("page_revision_id"),
            "scraped_at": _to_iso_now(),
            "summary": summary,
            "dynamic_sections": sections or None,
            "citations": citations,
            "conflicts": conflicts,
            "source_variants": source_variants,
            "ai_model": ai_model,
            "ai_generated_at": _to_iso_now() if ai_model else None,
            "raw_html_sha256": primary.get("raw_html_sha256"),
        },
        warnings,
    )


def _candidate_to_json(candidate: FandomCandidatePage) -> dict[str, Any]:
    return {
        "url": candidate.url,
        "title": candidate.title,
        "source": candidate.source,
        "domain": candidate.domain,
        "score": candidate.score,
    }


def _resolve_person_name(db: SupabaseAdminClient, person_id: str) -> str:
    response = (
        db.schema("core")
        .table("people")
        .select("id,full_name")
        .eq("id", person_id)
        .limit(1)
        .execute()
    )
    rows = get_list_result(response, "loading person for fandom sync")
    if not rows:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")
    name = str(rows[0].get("full_name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail=f"Person {person_id} has no full_name")
    return name


def _resolve_season_context(db: SupabaseAdminClient, *, show_id: str, season_number: int) -> dict[str, Any]:
    show_response = db.schema("core").table("shows").select("id,name").eq("id", show_id).limit(1).execute()
    show_rows = get_list_result(show_response, "loading show for season fandom sync")
    if not show_rows:
        raise HTTPException(status_code=404, detail=f"Show {show_id} not found")
    season_response = (
        db.schema("core")
        .table("seasons")
        .select("id,season_number,name,title")
        .eq("show_id", show_id)
        .eq("season_number", season_number)
        .limit(1)
        .execute()
    )
    season_rows = get_list_result(season_response, "loading season for fandom sync")
    if not season_rows:
        raise HTTPException(status_code=404, detail=f"Season {season_number} not found for show {show_id}")
    return {
        "show_id": show_id,
        "show_name": str(show_rows[0].get("name") or "").strip(),
        "season_id": str(season_rows[0].get("id")),
        "season_number": int(season_rows[0].get("season_number") or season_number),
        "season_title": str(season_rows[0].get("title") or season_rows[0].get("name") or "").strip(),
    }


def _collect_person_preview(
    *,
    person_name: str,
    request: FandomSyncRequest,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    domains = _normalize_domains(request.community_domains)
    manual_urls = _validate_manual_urls(request.manual_page_urls, allowlist=domains)
    candidates = discover_fandom_candidate_pages(
        query_name=person_name,
        entity_kind="person",
        manual_page_urls=manual_urls,
        community_domains=domains,
        include_allpages_scan=request.include_allpages_scan,
        allpages_max_pages=request.allpages_max_pages,
        max_candidates=request.max_candidates,
    )
    selected = candidates[: request.max_candidates]
    parsed_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for candidate in selected:
        try:
            html, final_url = fetch_fandom_person_html(candidate.url)
            if not html:
                warnings.append(f"No HTML returned for {candidate.url}")
                continue
            if not is_allowlisted_fandom_domain(final_url, allowlist=domains):
                warnings.append(f"Skipped non-allowlisted redirect target: {final_url}")
                continue
            parsed, _photos = parse_fandom_person_html(html, source_url=final_url)
            if not parsed:
                warnings.append(f"Parser returned no payload for {final_url}")
                continue
            parsed_rows.append(parsed)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Failed to parse {candidate.url}: {exc}")
    return (
        [_candidate_to_json(item) for item in candidates],
        [_candidate_to_json(item) for item in selected],
        parsed_rows,
        warnings,
    )


def _collect_season_preview(
    *,
    query_name: str,
    season_number: int,
    request: FandomSyncRequest,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    domains = _normalize_domains(request.community_domains)
    manual_urls = _validate_manual_urls(request.manual_page_urls, allowlist=domains)
    candidates = discover_fandom_candidate_pages(
        query_name=query_name,
        entity_kind="season",
        season_number=season_number,
        manual_page_urls=manual_urls,
        community_domains=domains,
        include_allpages_scan=request.include_allpages_scan,
        allpages_max_pages=request.allpages_max_pages,
        max_candidates=request.max_candidates,
    )
    selected = candidates[: request.max_candidates]
    parsed_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for candidate in selected:
        try:
            html, final_url = fetch_fandom_person_html(candidate.url)
            if not html:
                warnings.append(f"No HTML returned for {candidate.url}")
                continue
            if not is_allowlisted_fandom_domain(final_url, allowlist=domains):
                warnings.append(f"Skipped non-allowlisted redirect target: {final_url}")
                continue
            parsed = parse_fandom_season_html(html, source_url=final_url)
            if not parsed:
                warnings.append(f"Parser returned no payload for {final_url}")
                continue
            parsed_rows.append(parsed)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Failed to parse {candidate.url}: {exc}")
    return (
        [_candidate_to_json(item) for item in candidates],
        [_candidate_to_json(item) for item in selected],
        parsed_rows,
        warnings,
    )


@router.get("/admin/person/{person_id}/fandom")
def get_person_fandom(person_id: UUID, db: SupabaseAdminClient = None, _: AdminUser = None):
    response = (
        db.schema("core")
        .table("cast_fandom")
        .select("*")
        .eq("person_id", str(person_id))
        .order("scraped_at", desc=True)
        .execute()
    )
    rows = get_list_result(response, "listing person fandom data")
    return {"fandomData": rows, "count": len(rows)}


@router.post("/admin/person/{person_id}/import-fandom/preview")
def preview_person_fandom_sync(
    person_id: UUID,
    payload: FandomSyncRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
):
    person_name = _resolve_person_name(db, str(person_id))
    candidate_pages, selected_pages, parsed_rows, warnings = _collect_person_preview(
        person_name=person_name,
        request=payload,
    )
    merged, merge_warnings = _merge_person_payload(
        parsed_rows,
        selected_pages=selected_pages,
        entity_label=person_name,
        use_source_variants=payload.save_source_variants,
    )
    warnings.extend(merge_warnings)
    return {
        "candidate_pages": candidate_pages,
        "selected_pages": selected_pages,
        "warnings": warnings,
        "profile": merged,
    }


@router.post("/admin/person/{person_id}/import-fandom/commit")
def commit_person_fandom_sync(
    person_id: UUID,
    payload: FandomSyncCommitRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
):
    person_name = _resolve_person_name(db, str(person_id))
    preview_request = FandomSyncRequest(
        manual_page_urls=payload.selected_page_urls or payload.manual_page_urls,
        max_candidates=payload.max_candidates,
        include_allpages_scan=payload.include_allpages_scan,
        allpages_max_pages=payload.allpages_max_pages,
        community_domains=payload.community_domains,
        save_source_variants=payload.save_source_variants,
    )
    candidate_pages, selected_pages, parsed_rows, warnings = _collect_person_preview(
        person_name=person_name,
        request=preview_request,
    )
    merged, merge_warnings = _merge_person_payload(
        parsed_rows,
        selected_pages=selected_pages,
        entity_label=person_name,
        use_source_variants=payload.save_source_variants,
    )
    warnings.extend(merge_warnings)
    if not merged:
        raise HTTPException(status_code=400, detail="No valid Fandom payload to commit")
    row = dict(merged)
    row["person_id"] = str(person_id)
    saved = upsert_cast_fandom(db, row)
    return {
        "saved": saved,
        "candidate_pages": candidate_pages,
        "selected_pages": selected_pages,
        "warnings": warnings,
    }


@router.get("/admin/shows/{show_id}/seasons/{season_number}/fandom")
def get_season_fandom_data(
    show_id: UUID,
    season_number: int,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
):
    season_context = _resolve_season_context(db, show_id=str(show_id), season_number=season_number)
    rows = list_season_fandom(db, season_id=season_context["season_id"])
    return {"fandomData": rows, "count": len(rows)}


@router.post("/admin/shows/{show_id}/seasons/{season_number}/import-fandom/preview")
def preview_season_fandom_sync(
    show_id: UUID,
    season_number: int,
    payload: FandomSyncRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
):
    season_context = _resolve_season_context(db, show_id=str(show_id), season_number=season_number)
    query_name = season_context["season_title"] or (
        f'{season_context["show_name"]} season {season_context["season_number"]}'
    )
    candidate_pages, selected_pages, parsed_rows, warnings = _collect_season_preview(
        query_name=query_name,
        season_number=season_context["season_number"],
        request=payload,
    )
    merged, merge_warnings = _merge_season_payload(
        parsed_rows,
        selected_pages=selected_pages,
        entity_label=query_name,
        use_source_variants=payload.save_source_variants,
    )
    warnings.extend(merge_warnings)
    return {
        "candidate_pages": candidate_pages,
        "selected_pages": selected_pages,
        "warnings": warnings,
        "season_profile": merged,
    }


@router.post("/admin/shows/{show_id}/seasons/{season_number}/import-fandom/commit")
def commit_season_fandom_sync(
    show_id: UUID,
    season_number: int,
    payload: FandomSyncCommitRequest,
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
):
    season_context = _resolve_season_context(db, show_id=str(show_id), season_number=season_number)
    query_name = season_context["season_title"] or (
        f'{season_context["show_name"]} season {season_context["season_number"]}'
    )
    preview_request = FandomSyncRequest(
        manual_page_urls=payload.selected_page_urls or payload.manual_page_urls,
        max_candidates=payload.max_candidates,
        include_allpages_scan=payload.include_allpages_scan,
        allpages_max_pages=payload.allpages_max_pages,
        community_domains=payload.community_domains,
        save_source_variants=payload.save_source_variants,
    )
    candidate_pages, selected_pages, parsed_rows, warnings = _collect_season_preview(
        query_name=query_name,
        season_number=season_context["season_number"],
        request=preview_request,
    )
    merged, merge_warnings = _merge_season_payload(
        parsed_rows,
        selected_pages=selected_pages,
        entity_label=query_name,
        use_source_variants=payload.save_source_variants,
    )
    warnings.extend(merge_warnings)
    if not merged:
        raise HTTPException(status_code=400, detail="No valid Fandom season payload to commit")
    row = dict(merged)
    row["season_id"] = season_context["season_id"]
    row["show_id"] = season_context["show_id"]
    row["season_number"] = season_context["season_number"]
    saved = upsert_season_fandom(db, row)
    return {
        "saved": saved,
        "candidate_pages": candidate_pages,
        "selected_pages": selected_pages,
        "warnings": warnings,
    }
