"""Admin endpoints for person profile refresh workflows."""

from __future__ import annotations

import json
import logging
import re
import threading
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from queue import Empty, SimpleQueue
from typing import Any, cast
from uuid import UUID

import requests
from bs4 import BeautifulSoup, Tag
from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.auth import InternalAdminUser
from api.deps import PostgrestAdminClient
from scripts.sync import sync_episode_appearances, sync_show_cast
from trr_backend.db import pg
from trr_backend.ingestion.fandom_person_scraper import fetch_fandom_person_html, parse_fandom_person_html
from trr_backend.ingestion.show_cast_matrix_scraper import is_missing_wikipedia_page
from trr_backend.integrations.tmdb_person import fetch_tmdb_person_full
from trr_backend.pipeline.admin_operation_registry import (
    get_person_images_capabilities,
    get_show_bravo_capabilities,
    get_show_links_capabilities,
)
from trr_backend.pipeline.admin_operations import (
    operation_stream_response,
    start_operation_for_stream,
)
from trr_backend.repositories.cast_fandom import upsert_cast_fandom
from trr_backend.repositories.cast_tmdb import upsert_cast_tmdb
from trr_backend.scraping.bravo_parser import parse_person_page

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/person", tags=["admin-person"])


class RefreshProfileRequest(BaseModel):
    refresh_links: bool = True
    refresh_credits: bool = True


class ProfileSourceSkippedError(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _iso_utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _sse_event(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


def _coerce_record(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _merge_source_value(existing: Any, *, source: str, value: Any) -> dict[str, Any] | None:
    if value in (None, ""):
        return None
    base = _coerce_record(existing)
    if base.get(source) == value:
        return None
    merged = dict(base)
    merged[source] = value
    return merged


def _normalize_aliases(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        cleaned = " ".join(value.split()).strip()
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(cleaned)
    return normalized


def _merge_source_aliases(existing: Any, *, source: str, values: Any) -> dict[str, Any] | None:
    aliases = _normalize_aliases(values)
    if not aliases:
        return None
    base = _coerce_record(existing)
    current = _normalize_aliases(base.get(source))
    if current == aliases:
        return None
    merged = dict(base)
    merged[source] = aliases
    return merged


def _flatten_aliases(value: Any) -> list[str]:
    alias_map = _coerce_record(value)
    ordered: list[str] = []
    seen: set[str] = set()
    for source in ("tmdb", "imdb", "wikipedia", "fandom", "bravo", "manual"):
        for alias in _normalize_aliases(alias_map.get(source)):
            key = alias.casefold()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(alias)
    for source_value in alias_map.values():
        for alias in _normalize_aliases(source_value):
            key = alias.casefold()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(alias)
    return ordered


def _apply_people_patch(db: PostgrestAdminClient, *, person_id: str, patch: dict[str, Any]) -> None:
    if not patch:
        return
    response = db.schema("core").table("people").update(patch).eq("id", person_id).execute()
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail=f"Failed to update person {person_id}")


def _load_person(*, person_id: str) -> dict[str, Any]:
    try:
        row = pg.fetch_one(
            """
            SELECT
              id::text AS id,
              full_name,
              external_ids,
              birthday,
              gender,
              biography,
              place_of_birth,
              homepage,
              profile_image_url,
              alternative_names
            FROM core.people
            WHERE id = %s::uuid
            """,
            [person_id],
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read person %s via direct pg: %s", person_id, exc)
        raise HTTPException(status_code=502, detail=f"Failed to read person {person_id}") from exc
    if row is None:
        raise HTTPException(status_code=404, detail="Person not found")
    return row


def _load_related_shows_for_person(person_id: str) -> list[dict[str, Any]]:
    rows = pg.fetch_all(
        """
        SELECT DISTINCT
          s.id::text AS show_id,
          s.name AS show_name,
          s.imdb_id AS show_imdb_id,
          s.networks
        FROM core.credits c
        JOIN core.shows s ON s.id = c.show_id
        WHERE c.person_id = %s::uuid
        ORDER BY s.name
        """,
        [person_id],
    )
    if rows:
        return rows
    return pg.fetch_all(
        """
        SELECT DISTINCT
          s.id::text AS show_id,
          s.name AS show_name,
          s.imdb_id AS show_imdb_id,
          s.networks
        FROM core.v_show_cast sc
        JOIN core.shows s ON s.id = sc.show_id
        WHERE sc.person_id = %s::uuid
        ORDER BY s.name
        """,
        [person_id],
    )


def _load_approved_person_links(
    *,
    person_id: str,
    show_ids: list[str],
) -> list[dict[str, Any]]:
    if not show_ids:
        return []
    try:
        return pg.fetch_all(
            """
            SELECT
              show_id::text AS show_id,
              link_kind,
              url,
              status,
              label,
              metadata
            FROM core.entity_links
            WHERE entity_type = 'person'
              AND entity_id = %s::uuid
              AND status = 'approved'
              AND show_id = ANY(%s::uuid[])
            """,
            [person_id, show_ids],
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read approved person links for %s via direct pg: %s", person_id, exc)
        raise HTTPException(status_code=502, detail=f"Failed to read person links for {person_id}") from exc


def _first_link_url(links: list[dict[str, Any]], kind: str) -> str | None:
    for link in links:
        if str(link.get("link_kind") or "").strip().lower() != kind:
            continue
        url = str(link.get("url") or "").strip()
        if url:
            return url
    return None


def _discover_and_persist_person_links(
    db: PostgrestAdminClient,
    *,
    show_id: str,
    person_id: str,
    actor: str,
) -> int:
    admin_show_links = get_show_links_capabilities()
    discovered = admin_show_links._discover_people_links(show_id, person_ids={person_id})
    upserted = 0
    for row in discovered:
        if str(row.get("entity_type") or "").strip().lower() != "person":
            continue
        if str(row.get("entity_id") or "").strip() != person_id:
            continue
        admin_show_links._upsert_link(
            db,
            show_id=show_id,
            entity_type="person",
            entity_id=person_id,
            link_group=str(row.get("link_group") or "knowledge"),
            link_kind=str(row.get("link_kind") or "other"),
            url=str(row.get("url") or ""),
            label=str(row.get("label") or "") or None,
            season_number=int(row.get("season_number") or 0),
            status=str(row.get("status") or "approved"),
            confidence=float(row.get("confidence") or 0.95),
            source=str(row.get("source") or "person_refresh_profile"),
            discovered_by="person_refresh_profile",
            metadata=row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
            actor=actor,
        )
        upserted += 1
    return upserted


def _tmdb_profile_patch(person: Mapping[str, Any], *, tmdb_row: Mapping[str, Any]) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    for field_name, row_key in (
        ("birthday", "birthday"),
        ("gender", "gender"),
        ("biography", "biography"),
        ("place_of_birth", "place_of_birth"),
        ("homepage", "homepage"),
    ):
        merged = _merge_source_value(person.get(field_name), source="tmdb", value=tmdb_row.get(row_key))
        if merged is not None:
            patch[field_name] = merged
    profile_path = tmdb_row.get("profile_path")
    profile_url = None
    if isinstance(profile_path, str) and profile_path.strip():
        cleaned = profile_path.strip()
        if cleaned.startswith("http://") or cleaned.startswith("https://"):
            profile_url = cleaned
        else:
            profile_url = f"https://image.tmdb.org/t/p/original/{cleaned.lstrip('/')}"
    merged = _merge_source_value(person.get("profile_image_url"), source="tmdb", value=profile_url)
    if merged is not None:
        patch["profile_image_url"] = merged
    return patch


def _refresh_tmdb_profile(
    db: PostgrestAdminClient,
    *,
    person: dict[str, Any],
) -> tuple[int, int]:
    admin_person_images = get_person_images_capabilities()
    tmdb_person_id = admin_person_images._get_tmdb_id(db, str(person["id"]), _coerce_record(person.get("external_ids")))
    if not tmdb_person_id:
        raise ProfileSourceSkippedError("No TMDb person id available.")
    person_full = fetch_tmdb_person_full(int(tmdb_person_id))
    if not person_full:
        raise ProfileSourceSkippedError("TMDb profile unavailable.")
    tmdb_row = person_full.to_cast_tmdb_row(str(person["id"]))
    upsert_cast_tmdb(db, tmdb_row)
    patch = _tmdb_profile_patch(person, tmdb_row=tmdb_row)
    merged_aliases = _merge_source_aliases(
        person.get("alternative_names"),
        source="tmdb",
        values=getattr(getattr(person_full, "details", None), "also_known_as", []),
    )
    if merged_aliases is not None:
        patch["alternative_names"] = merged_aliases
    before_aliases = len(_flatten_aliases(person.get("alternative_names")))
    _apply_people_patch(db, person_id=str(person["id"]), patch=patch)
    person.update(patch)
    after_aliases = len(_flatten_aliases(person.get("alternative_names")))
    return len(patch), max(0, after_aliases - before_aliases)


def _extract_ld_json_objects(soup: BeautifulSoup) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string or script.get_text()
        if not text or not text.strip():
            continue
        try:
            parsed = json.loads(text)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
        elif isinstance(parsed, list):
            rows.extend(item for item in parsed if isinstance(item, dict))
    return rows


def _extract_imdb_profile_from_html(*, html: str, page_url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    aliases: list[str] = []
    bio: str | None = None
    birth_date: str | None = None
    birth_place: str | None = None
    image_url: str | None = None

    for obj in _extract_ld_json_objects(soup):
        obj_type = str(obj.get("@type") or "").strip().lower()
        if obj_type != "person":
            continue
        alt = obj.get("alternateName")
        if isinstance(alt, str):
            aliases.append(alt)
        elif isinstance(alt, list):
            aliases.extend(item for item in alt if isinstance(item, str))
        description = obj.get("description")
        if isinstance(description, str) and description.strip() and not bio:
            bio = description.strip()
        birth = obj.get("birthDate")
        if isinstance(birth, str) and birth.strip() and not birth_date:
            birth_date = birth.strip()
        image = obj.get("image")
        if isinstance(image, str) and image.strip() and not image_url:
            image_url = image.strip()
        home_location = obj.get("homeLocation")
        if isinstance(home_location, dict):
            place_name = home_location.get("name")
            if isinstance(place_name, str) and place_name.strip() and not birth_place:
                birth_place = place_name.strip()

    for list_item in soup.select("li[data-testid='nm_pd_aka']"):
        aliases.extend(
            text.strip()
            for text in list_item.get_text(" ", strip=True).split(",")
            if isinstance(text, str) and text.strip()
        )

    born_text = None
    for item in soup.select("[data-testid='nm_pd_bl'] li"):
        text = item.get_text(" ", strip=True)
        if text.lower().startswith("born"):
            born_text = text
            break
    if born_text:
        iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", born_text)
        if iso_match and not birth_date:
            birth_date = iso_match.group(1)
        comma_index = born_text.rfind(",")
        if comma_index != -1 and comma_index + 1 < len(born_text) and not birth_place:
            birth_place = born_text[comma_index + 1 :].strip()

    return {
        "source_url": page_url,
        "biography": bio,
        "birthday": birth_date,
        "place_of_birth": birth_place,
        "profile_image_url": image_url,
        "alternative_names": _normalize_aliases(aliases),
    }


def _fetch_imdb_person_profile(imdb_id: str) -> dict[str, Any] | None:
    response = requests.get(
        f"https://www.imdb.com/name/{imdb_id}/",
        timeout=20,
        headers={"user-agent": "Mozilla/5.0", "accept-language": "en-US,en;q=0.9"},
    )
    response.raise_for_status()
    return _extract_imdb_profile_from_html(html=response.text, page_url=str(response.url))


def _refresh_imdb_profile(
    db: PostgrestAdminClient,
    *,
    person: dict[str, Any],
    approved_links: list[dict[str, Any]],
) -> tuple[int, int]:
    external_ids = _coerce_record(person.get("external_ids"))
    imdb_id = str(external_ids.get("imdb") or external_ids.get("imdb_id") or "").strip()
    if not imdb_id:
        imdb_url = _first_link_url(approved_links, "imdb")
        match = re.search(r"/name/(nm\d+)", imdb_url or "")
        imdb_id = match.group(1) if match else ""
    if not imdb_id:
        raise ProfileSourceSkippedError("No IMDb profile link available.")
    try:
        profile = _fetch_imdb_person_profile(imdb_id)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        if status_code == 404:
            raise ProfileSourceSkippedError("IMDb profile not found.") from exc
        raise
    if not profile:
        raise ProfileSourceSkippedError("IMDb profile unavailable.")
    patch: dict[str, Any] = {}
    for field_name, profile_key in (
        ("birthday", "birthday"),
        ("biography", "biography"),
        ("place_of_birth", "place_of_birth"),
        ("profile_image_url", "profile_image_url"),
    ):
        merged = _merge_source_value(person.get(field_name), source="imdb", value=profile.get(profile_key))
        if merged is not None:
            patch[field_name] = merged
    merged_aliases = _merge_source_aliases(
        person.get("alternative_names"),
        source="imdb",
        values=profile.get("alternative_names"),
    )
    if merged_aliases is not None:
        patch["alternative_names"] = merged_aliases
    before_aliases = len(_flatten_aliases(person.get("alternative_names")))
    _apply_people_patch(db, person_id=str(person["id"]), patch=patch)
    person.update(patch)
    after_aliases = len(_flatten_aliases(person.get("alternative_names")))
    return len(patch), max(0, after_aliases - before_aliases)


def _refresh_fandom_profile(
    db: PostgrestAdminClient,
    *,
    person: dict[str, Any],
    approved_links: list[dict[str, Any]],
) -> tuple[int, int]:
    fandom_url = _first_link_url(approved_links, "fandom") or _first_link_url(approved_links, "wikia")
    if not fandom_url:
        raise ProfileSourceSkippedError("No Fandom profile link available.")
    html, resolved_url = fetch_fandom_person_html(fandom_url)
    cast_fandom, _photos = parse_fandom_person_html(html, source_url=resolved_url)
    cast_fandom["person_id"] = str(person["id"])
    cast_fandom["source"] = "fandom"
    upsert_cast_fandom(db, cast_fandom)
    patch: dict[str, Any] = {}
    birth_value = cast_fandom.get("birthdate") or cast_fandom.get("birthdate_display")
    for field_name, value in (
        ("birthday", birth_value),
        ("gender", cast_fandom.get("gender")),
        ("biography", cast_fandom.get("summary")),
    ):
        merged = _merge_source_value(person.get(field_name), source="fandom", value=value)
        if merged is not None:
            patch[field_name] = merged
    _apply_people_patch(db, person_id=str(person["id"]), patch=patch)
    person.update(patch)
    return len(patch), 0


def _extract_wikipedia_profile_from_html(*, html: str, page_url: str) -> dict[str, Any]:
    if is_missing_wikipedia_page(html, page_url):
        return {}
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one(".mw-parser-output")
    bio = None
    if isinstance(content, Tag):
        for paragraph in content.find_all("p", recursive=False):
            text = paragraph.get_text(" ", strip=True)
            if text:
                bio = text
                break
    birth_date = None
    birth_place = None
    infobox = soup.select_one(".infobox")
    if isinstance(infobox, Tag):
        for row in infobox.select("tr"):
            header = row.find("th")
            cell = row.find("td")
            label = header.get_text(" ", strip=True).lower() if isinstance(header, Tag) else ""
            value = cell.get_text(" ", strip=True) if isinstance(cell, Tag) else ""
            if label != "born" or not value:
                continue
            match = re.search(r"(\d{4}-\d{2}-\d{2}|\b\d{4}\b)", value)
            if match:
                birth_date = match.group(1)
            if "," in value:
                birth_place = value.split(",", 1)[1].strip()
            break
    return {
        "biography": bio,
        "birthday": birth_date,
        "place_of_birth": birth_place,
    }


def _refresh_wikipedia_profile(
    db: PostgrestAdminClient,
    *,
    person: dict[str, Any],
    approved_links: list[dict[str, Any]],
) -> tuple[int, int]:
    wikipedia_url = _first_link_url(approved_links, "wikipedia")
    if not wikipedia_url:
        raise ProfileSourceSkippedError("No Wikipedia profile link available.")
    response = requests.get(wikipedia_url, timeout=20, headers={"user-agent": "Mozilla/5.0"})
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code == 404:
            raise ProfileSourceSkippedError("Wikipedia profile not found.") from exc
        raise
    profile = _extract_wikipedia_profile_from_html(html=response.text, page_url=str(response.url))
    if not profile:
        raise ProfileSourceSkippedError("Wikipedia profile unavailable.")
    patch: dict[str, Any] = {}
    for field_name in ("biography", "birthday", "place_of_birth"):
        merged = _merge_source_value(person.get(field_name), source="wikipedia", value=profile.get(field_name))
        if merged is not None:
            patch[field_name] = merged
    _apply_people_patch(db, person_id=str(person["id"]), patch=patch)
    person.update(patch)
    return len(patch), 0


def _refresh_bravo_profile(
    db: PostgrestAdminClient,
    *,
    person: dict[str, Any],
    approved_links: list[dict[str, Any]],
    related_shows: list[dict[str, Any]],
    actor: str,
) -> tuple[int, int]:
    admin_show_bravo = get_show_bravo_capabilities()
    before_refresh = {
        "biography": person.get("biography"),
        "profile_image_url": person.get("profile_image_url"),
        "social_links": person.get("social_links"),
    }
    bravo_url = _first_link_url(approved_links, "bravo_profile")
    if not bravo_url:
        raise ProfileSourceSkippedError("No BravoTV profile link available.")
    parsed = parse_person_page(bravo_url)
    bio = parsed.get("bio") if isinstance(parsed.get("bio"), str) else None
    hero_image_url = parsed.get("hero_image_url") if isinstance(parsed.get("hero_image_url"), str) else None
    social_links_value = parsed.get("social_links")
    social_links = social_links_value if isinstance(social_links_value, dict) else {}
    admin_show_bravo._persist_person_profile(
        db,
        person_id=str(person["id"]),
        person_url=str(parsed.get("url") or bravo_url),
        bio=bio,
        hero_image_url=hero_image_url,
        social_links={str(key): str(value) for key, value in social_links.items() if isinstance(value, str)},
        source="bravo",
    )
    show_id = str((related_shows[0] or {}).get("show_id") or "").strip() if related_shows else ""
    if show_id and hero_image_url:
        import_result = admin_show_bravo._import_bravo_person_image(
            db=db,
            admin_user={"id": actor},
            show_id=show_id,
            season_id=None,
            season_number=None,
            person_id=str(person["id"]),
            person_url=str(parsed.get("url") or bravo_url),
            hero_image_url=hero_image_url,
            person_name=str(person.get("full_name") or "").strip() or None,
        )
        hosted_url = str(import_result.get("primary_hosted_url") or "").strip() or None
        if hosted_url:
            admin_show_bravo._persist_person_profile(
                db,
                person_id=str(person["id"]),
                person_url=str(parsed.get("url") or bravo_url),
                bio=bio,
                hero_image_url=hosted_url,
                social_links={str(key): str(value) for key, value in social_links.items() if isinstance(value, str)},
                source="bravo",
            )
    refreshed = _load_person(person_id=str(person["id"]))
    person.update(refreshed)
    changed_fields = sum(1 for key, before_value in before_refresh.items() if refreshed.get(key) != before_value)
    return changed_fields, 0


def _refresh_credits_for_related_shows(related_shows: list[dict[str, Any]]) -> tuple[int, list[str]]:
    processed = 0
    failures: list[str] = []
    for show in related_shows:
        show_id = str(show.get("show_id") or "").strip()
        if not show_id:
            continue
        processed += 1
        try:
            cast_result = sync_show_cast.main(["--show-id", show_id])
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{show_id}:show_cast:{exc}")
            cast_result = 1
        try:
            episode_result = sync_episode_appearances.main(["--show-id", show_id, "--concurrency", "2"])
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{show_id}:episode_appearances:{exc}")
            episode_result = 1
        if cast_result != 0:
            failures.append(f"{show_id}:show_cast")
        if episode_result != 0:
            failures.append(f"{show_id}:episode_appearances")
    return processed, failures


def _run_person_profile_refresh(
    *,
    person_id: str,
    payload: RefreshProfileRequest,
    db: PostgrestAdminClient,
    actor: str,
    stage_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    started_at = _iso_utc_now()

    def emit(stage: str, message: str, **extra: Any) -> None:
        if stage_callback is None:
            return
        stage_callback(
            stage,
            {
                "person_id": person_id,
                "stage": stage,
                "message": message,
                "timestamp": _iso_utc_now(),
                **extra,
            },
        )

    person = _load_person(person_id=person_id)
    related_shows = _load_related_shows_for_person(person_id)
    show_ids = [str(row.get("show_id") or "").strip() for row in related_shows if str(row.get("show_id") or "").strip()]
    emit("load_context", "Loaded person context.", related_show_count=len(show_ids))

    links_refreshed = 0
    if payload.refresh_links:
        for index, show in enumerate(related_shows, start=1):
            show_id = str(show.get("show_id") or "").strip()
            if not show_id:
                continue
            emit(
                "links_discovery",
                f"Refreshing approved source links for {show.get('show_name') or show_id}...",
                current=index,
                total=len(related_shows),
                show_id=show_id,
            )
            links_refreshed += _discover_and_persist_person_links(db, show_id=show_id, person_id=person_id, actor=actor)

    approved_links = _load_approved_person_links(person_id=person_id, show_ids=show_ids)
    field_changes = 0
    aliases_added = 0
    source_failures: list[str] = []
    source_skips: list[str] = []

    profile_stages = cast(
        "tuple[tuple[str, str, Callable[..., tuple[int, int]]], ...]",
        (
            ("profile_tmdb", "Refreshing TMDb profile...", _refresh_tmdb_profile),
            ("profile_imdb", "Refreshing IMDb profile...", _refresh_imdb_profile),
            ("profile_fandom", "Refreshing Fandom profile...", _refresh_fandom_profile),
            ("profile_wikipedia", "Refreshing Wikipedia profile...", _refresh_wikipedia_profile),
            ("profile_bravo", "Refreshing Bravo profile...", _refresh_bravo_profile),
        ),
    )
    for stage, label, runner in profile_stages:
        emit(stage, label)
        try:
            if runner is _refresh_tmdb_profile:
                changed, added = runner(db, person=person)
            elif runner is _refresh_bravo_profile:
                changed, added = runner(
                    db,
                    person=person,
                    approved_links=approved_links,
                    related_shows=related_shows,
                    actor=actor,
                )
            else:
                changed, added = runner(db, person=person, approved_links=approved_links)
        except ProfileSourceSkippedError as exc:
            logger.info("person profile source skipped person_id=%s stage=%s reason=%s", person_id, stage, exc.reason)
            source_skips.append(f"{stage}:{exc.reason}")
            emit(stage, exc.reason, status="skipped", skip_reason=exc.reason)
            continue
        except Exception as exc:  # noqa: BLE001
            logger.warning("person profile source refresh failed person_id=%s stage=%s error=%s", person_id, stage, exc)
            source_failures.append(f"{stage}:{exc}")
            continue
        field_changes += changed
        aliases_added += added
        emit(stage, label, status="completed", changes=changed, aliases_added=added)

    credits_processed = 0
    credit_failures: list[str] = []
    credits_updated = 0
    if payload.refresh_credits:
        emit("credits_refresh", "Refreshing show credits from related shows...", current=0, total=len(related_shows))
        credits_processed, credit_failures = _refresh_credits_for_related_shows(related_shows)
        failed_shows = {str(item).split(":", 1)[0] for item in credit_failures if isinstance(item, str) and ":" in item}
        credits_updated = max(credits_processed - len(failed_shows), 0)

    finished_at = _iso_utc_now()
    return {
        "person_id": person_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "links_refreshed": links_refreshed,
        "aliases_added": aliases_added,
        "profile_fields_changed": field_changes,
        "shows_processed": len(related_shows),
        "credits_updated": credits_updated,
        "failures": source_failures + credit_failures,
        "skips": source_skips,
        "status": "ok" if not (source_failures or credit_failures) else "partial",
    }


def _build_refresh_profile_event_stream(
    *,
    person_id: str,
    payload: RefreshProfileRequest,
    db: PostgrestAdminClient,
    actor: str,
) -> Iterator[str]:
    event_queue: SimpleQueue[tuple[str, dict[str, Any]]] = SimpleQueue()
    done = threading.Event()
    result_box: dict[str, Any] = {}
    error_box: dict[str, Any] = {}

    def on_stage(stage: str, stage_payload: dict[str, Any]) -> None:
        current_stage = str(stage_payload.get("current_stage") or "").strip() or stage
        event_queue.put(("progress", {"current_stage": current_stage, **stage_payload}))

    def worker() -> None:
        try:
            result_box["result"] = _run_person_profile_refresh(
                person_id=person_id,
                payload=payload,
                db=db,
                actor=actor,
                stage_callback=on_stage,
            )
        except Exception as exc:  # noqa: BLE001
            error_box["error"] = str(exc)
        finally:
            done.set()

    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()

    yield _sse_event(
        "progress",
        {
            "person_id": person_id,
            "stage": "starting",
            "message": "Starting profile refresh...",
            "timestamp": _iso_utc_now(),
        },
    )
    while True:
        if done.is_set() and event_queue.empty():
            break
        try:
            event_name, event_payload = event_queue.get(timeout=5.0)
        except Empty:
            yield _sse_event(
                "progress",
                {
                    "person_id": person_id,
                    "stage": "heartbeat",
                    "heartbeat": True,
                    "message": "Profile refresh still running...",
                    "timestamp": _iso_utc_now(),
                },
            )
            continue
        yield _sse_event(event_name, event_payload)

    error_detail = str(error_box.get("error") or "").strip()
    if error_detail:
        yield _sse_event(
            "error",
            {
                "person_id": person_id,
                "stage": "error",
                "error": "Profile refresh failed",
                "detail": error_detail,
                "status": "failed",
            },
        )
        return

    result_value = result_box.get("result")
    result = result_value if isinstance(result_value, dict) else {}
    yield _sse_event(
        "complete",
        {
            "person_id": person_id,
            "stage": "complete",
            "status": result.get("status"),
            "summary": result,
            **result,
        },
    )


@router.post("/{person_id}/refresh-profile/stream")
def refresh_person_profile_stream(
    person_id: UUID,
    payload: RefreshProfileRequest = Body(default_factory=RefreshProfileRequest),
    db: PostgrestAdminClient = cast(PostgrestAdminClient, None),
    admin: InternalAdminUser = cast(InternalAdminUser, None),
    request: Request = cast(Request, None),
) -> StreamingResponse:
    person_id_str = str(person_id)
    actor = str((admin or {}).get("email") or (admin or {}).get("id") or "admin")
    request_payload = {
        "person_id": person_id_str,
        "payload": payload.model_dump(mode="json"),
        "initiated_by": actor,
    }

    def producer() -> Iterator[str]:
        return _build_refresh_profile_event_stream(
            person_id=person_id_str,
            payload=payload,
            db=db,
            actor=actor,
        )

    operation = start_operation_for_stream(
        operation_type="admin_person_refresh_profile",
        producer=producer,
        request_payload=request_payload,
        initiated_by=actor,
        request=request,
    )
    return operation_stream_response(str(operation.get("id")), request=request)


def build_person_refresh_profile_operation_producer(
    *,
    request_payload: dict[str, Any],
    operation_id: str | None = None,
    db: PostgrestAdminClient | None = None,
):
    from trr_backend.db.admin import create_supabase_admin_client

    del operation_id
    person_id_str = str(request_payload.get("person_id") or "").strip()
    if not person_id_str:
        raise ValueError("request_payload.person_id is required")
    payload_data = request_payload.get("payload") if isinstance(request_payload.get("payload"), dict) else {}
    payload = RefreshProfileRequest.model_validate(payload_data)
    actor = str(request_payload.get("initiated_by") or "admin")

    def _producer() -> Iterator[str]:
        local_db = db or create_supabase_admin_client()
        return _build_refresh_profile_event_stream(
            person_id=person_id_str,
            payload=payload,
            db=local_db,
            actor=actor,
        )

    return _producer
