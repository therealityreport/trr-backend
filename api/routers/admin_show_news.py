"""Admin endpoints for Google News sync and unified show news reads."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import AdminUser
from api.deps import SupabaseAdminClient
from api.routers import admin_show_bravo
from trr_backend.db import pg
from trr_backend.scraping.google_news_parser import fetch_google_news

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-news"])

_BRAVO_SOURCE_ID = "bravo"
_GOOGLE_SOURCE_ID = "google_news"
_DEFAULT_VARIANT = "default"
_STALE_WINDOW_MINUTES = 30
_TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "casting": ("cast", "housewife", "friend of", "joins", "joined", "returning", "returns"),
    "reunion": ("reunion", "part 1", "part 2", "part 3", "sit-down"),
    "relationship": ("dating", "married", "divorce", "split", "engaged", "boyfriend", "girlfriend"),
    "legal": ("lawsuit", "sued", "arrested", "charges", "legal", "court"),
    "drama": ("feud", "fight", "drama", "clash", "shade"),
    "premiere": ("premiere", "first look", "trailer", "teaser"),
    "finale": ("finale", "wrap-up"),
}
_SEASON_WORD_RE = re.compile(r"\bseason\s*0*(\d{1,3})\b", re.IGNORECASE)
_SEASON_SHORT_RE = re.compile(r"\bs(?:\s*|[-_]?)(\d{1,2})(?:\b|e\d{1,2}\b)", re.IGNORECASE)
_GOOGLE_NEWS_IMAGE_CAPTION = "Google News featured image"


class GoogleNewsSyncRequest(BaseModel):
    force: bool = False


def _to_iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _payload_sha(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _show_exists(db: SupabaseAdminClient, show_id: str) -> bool:
    response = db.schema("core").table("shows").select("id").eq("id", show_id).limit(1).execute()
    if getattr(response, "error", None):
        raise HTTPException(status_code=502, detail="Database error checking show")
    return bool(response.data)


def _ensure_google_source(db: SupabaseAdminClient) -> None:
    response = (
        db.schema("core")
        .table("sources")
        .upsert(
            {
                "id": _GOOGLE_SOURCE_ID,
                "category": "vendor",
                "aliases": "{news.google.com}",
            },
            on_conflict="id",
        )
        .execute()
    )
    if getattr(response, "error", None):
        message = getattr(response.error, "message", str(response.error))
        raise HTTPException(status_code=502, detail=f"Failed to ensure google_news source row: {message}")


def _fetch_show_snapshot(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    source_id: str,
) -> dict[str, Any] | None:
    response = (
        db.schema("core")
        .table("show_source_latest")
        .select("show_id, source_id, variant, fetched_at, status, payload, payload_sha256")
        .eq("show_id", show_id)
        .eq("source_id", source_id)
        .eq("variant", _DEFAULT_VARIANT)
        .limit(1)
        .execute()
    )
    if getattr(response, "error", None):
        message = getattr(response.error, "message", str(response.error))
        raise HTTPException(status_code=502, detail=f"Failed to load source snapshot: {message}")
    rows = response.data or []
    if not rows:
        return None
    return rows[0]


def _upsert_show_snapshot(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    source_id: str,
    payload: dict[str, Any],
    status: str = "success",
    error: str | None = None,
) -> dict[str, Any]:
    fetched_at = _to_iso_now()
    payload_sha = _payload_sha(payload)
    latest_row = {
        "show_id": show_id,
        "source_id": source_id,
        "variant": _DEFAULT_VARIANT,
        "fetched_at": fetched_at,
        "fetch_method": "admin_google_news_sync",
        "status": status,
        "error": error,
        "payload": payload,
        "payload_sha256": payload_sha,
    }
    latest_resp = (
        db.schema("core")
        .table("show_source_latest")
        .upsert(latest_row, on_conflict="show_id,source_id,variant")
        .execute()
    )
    if getattr(latest_resp, "error", None):
        message = getattr(latest_resp.error, "message", str(latest_resp.error))
        raise HTTPException(status_code=502, detail=f"Failed to persist show source snapshot: {message}")

    history_row = {
        **latest_row,
    }
    history_resp = db.schema("core").table("show_source_history").insert(history_row).execute()
    if getattr(history_resp, "error", None):
        logger.warning("Failed to persist source snapshot history for %s: %s", source_id, history_resp.error)

    return {
        "show_id": show_id,
        "source_id": source_id,
        "variant": _DEFAULT_VARIANT,
        "fetched_at": fetched_at,
        "payload_sha256": payload_sha,
    }


def _resolve_google_news_link(show_id: str) -> dict[str, Any] | None:
    rows = pg.fetch_all(
        """
        SELECT id, url, status, created_at, updated_at
        FROM core.entity_links
        WHERE entity_type = 'show'
          AND entity_id = %s
          AND show_id = %s
          AND link_kind = 'google_news_url'
          AND season_number = 0
        ORDER BY
          CASE status
            WHEN 'approved' THEN 0
            WHEN 'pending' THEN 1
            ELSE 2
          END,
          updated_at DESC NULLS LAST,
          created_at DESC NULLS LAST
        LIMIT 1
        """,
        [show_id, show_id],
    )
    return rows[0] if rows else None


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _show_name_and_aliases(show_id: str) -> tuple[str, list[str]]:
    row = pg.fetch_one("SELECT * FROM core.shows WHERE id = %s", [show_id])
    if not row:
        return ("", [])

    show_name = str(row.get("name") or row.get("title") or "").strip()
    aliases_raw = row.get("alternative_names")
    aliases: list[str] = []
    if isinstance(aliases_raw, list):
        for alias in aliases_raw:
            cleaned = str(alias or "").strip()
            if cleaned:
                aliases.append(cleaned)
    if show_name and all(show_name.lower() != alias.lower() for alias in aliases):
        aliases.insert(0, show_name)
    deduped: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        key = alias.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(alias)
    return (show_name, deduped)


def _build_show_cast_index(show_id: str) -> list[dict[str, Any]]:
    rows = pg.fetch_all(
        """
        SELECT person_id, person_name
        FROM core.v_person_show_seasons
        WHERE show_id = %s
        ORDER BY person_name NULLS LAST
        LIMIT 1000
        """,
        [show_id],
    )
    if not rows:
        rows = pg.fetch_all(
            """
            SELECT person_id, cast_member_name AS person_name
            FROM core.v_show_cast
            WHERE show_id = %s
            ORDER BY cast_member_name NULLS LAST
            LIMIT 1000
            """,
            [show_id],
        )

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        person_name = str(row.get("person_name") or "").strip()
        if not person_id or not person_name or person_id in seen:
            continue
        seen.add(person_id)
        out.append(
            {
                "person_id": person_id,
                "person_name": person_name,
                "normalized_name": _normalize_name(person_name),
            }
        )
    return out


def _infer_person_tags(text: str, cast_index: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = _normalize_name(text)
    if not normalized:
        return []
    tags: list[dict[str, Any]] = []
    for ref in cast_index:
        token = str(ref.get("normalized_name") or "").strip()
        if not token:
            continue
        pattern = rf"(^|\s){re.escape(token)}($|\s)"
        if not re.search(pattern, normalized):
            continue
        tags.append(
            {
                "person_id": ref.get("person_id"),
                "person_name": ref.get("person_name"),
                "person_url": None,
            }
        )
    return tags


def _infer_topic_tags(text: str) -> list[str]:
    haystack = text.lower()
    tags: list[str] = []
    for topic, keywords in _TOPIC_KEYWORDS.items():
        if any(keyword in haystack for keyword in keywords):
            tags.append(topic)
    return tags


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _load_season_windows(show_id: str) -> dict[int, tuple[date, date]]:
    rows = pg.fetch_all(
        """
        SELECT
          s.season_number,
          COALESCE(MIN(e.air_date), s.premiere_date, s.air_date) AS start_date,
          COALESCE(MAX(e.air_date), s.air_date, s.premiere_date, MIN(e.air_date)) AS end_date
        FROM core.seasons s
        LEFT JOIN core.episodes e
          ON e.show_id = s.show_id
         AND e.season_number = s.season_number
        WHERE s.show_id = %s
        GROUP BY s.season_number, s.premiere_date, s.air_date
        ORDER BY s.season_number
        """,
        [show_id],
    )

    season_numbers = [int(row.get("season_number") or 0) for row in rows if int(row.get("season_number") or 0) > 0]
    bounds: dict[int, tuple[date | None, date | None]] = {}
    for row in rows:
        season_number = int(row.get("season_number") or 0)
        if season_number <= 0:
            continue
        start = row.get("start_date")
        end = row.get("end_date")
        if isinstance(start, datetime):
            start = start.date()
        if isinstance(end, datetime):
            end = end.date()
        if isinstance(start, date) and not isinstance(end, date):
            end = start + timedelta(days=180)
        if isinstance(end, date) and not isinstance(start, date):
            start = end - timedelta(days=180)
        if isinstance(start, date) and isinstance(end, date) and end < start:
            end = start
        bounds[season_number] = (
            start if isinstance(start, date) else None,
            end if isinstance(end, date) else None,
        )

    for season_number in season_numbers:
        start, end = bounds.get(season_number, (None, None))
        if isinstance(start, date) and isinstance(end, date):
            continue
        prev_end: date | None = None
        next_start: date | None = None
        for prev in sorted(s for s in season_numbers if s < season_number):
            _, candidate_end = bounds.get(prev, (None, None))
            if isinstance(candidate_end, date):
                prev_end = candidate_end
        for nxt in sorted((s for s in season_numbers if s > season_number), reverse=False):
            candidate_start, _ = bounds.get(nxt, (None, None))
            if isinstance(candidate_start, date):
                next_start = candidate_start
                break
        if prev_end and next_start:
            inferred_start = prev_end + timedelta(days=1)
            inferred_end = max(inferred_start, next_start - timedelta(days=1))
            bounds[season_number] = (inferred_start, inferred_end)
        elif prev_end:
            inferred_start = prev_end + timedelta(days=1)
            bounds[season_number] = (inferred_start, inferred_start + timedelta(days=180))
        elif next_start:
            inferred_end = next_start - timedelta(days=1)
            bounds[season_number] = (inferred_end - timedelta(days=180), inferred_end)

    normalized: dict[int, tuple[date, date]] = {}
    for season_number, (start, end) in bounds.items():
        if isinstance(start, date) and isinstance(end, date):
            normalized[season_number] = (start, end)
    return normalized


def _extract_season_mentions(text: str) -> set[int]:
    matches: set[int] = set()
    for token in _SEASON_WORD_RE.findall(text):
        number = int(token)
        if number > 0:
            matches.add(number)
    for token in _SEASON_SHORT_RE.findall(text):
        number = int(token)
        if number > 0:
            matches.add(number)
    return matches


def _infer_season_matches(
    *,
    text: str,
    published_at: str | None,
    season_windows: dict[int, tuple[date, date]],
    explicit_season_number: int | None = None,
) -> list[dict[str, Any]]:
    reasons: dict[int, set[str]] = {}
    for season_number in _extract_season_mentions(text):
        reasons.setdefault(season_number, set()).add("mention")
    if isinstance(explicit_season_number, int) and explicit_season_number > 0:
        reasons.setdefault(explicit_season_number, set()).add("mention")

    published_dt = _parse_datetime(published_at)
    published_date = published_dt.date() if published_dt else None
    if published_date:
        for season_number, (start, end) in season_windows.items():
            if start <= published_date <= end:
                reasons.setdefault(season_number, set()).add("date")

    return [
        {"season_number": season_number, "match_types": sorted(match_types)}
        for season_number, match_types in sorted(reasons.items())
    ]


def _normalize_google_news_items(
    *,
    items: list[dict[str, Any]],
    cast_index: list[dict[str, Any]],
    season_windows: dict[int, tuple[date, date]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw in items:
        headline = str(raw.get("headline") or "").strip() or None
        article_url = str(raw.get("article_url") or "").strip()
        if not article_url:
            continue
        summary = str(raw.get("summary") or "").strip() or None
        text_for_inference = f"{headline or ''} {summary or ''}".strip()
        raw_person_tags = raw.get("person_tags")
        person_tags = (
            raw_person_tags if isinstance(raw_person_tags, list) else _infer_person_tags(text_for_inference, cast_index)
        )
        raw_topic_tags = raw.get("topic_tags")
        topic_tags = (
            [str(tag).strip() for tag in raw_topic_tags if str(tag).strip()]
            if isinstance(raw_topic_tags, list)
            else _infer_topic_tags(text_for_inference)
        )
        raw_season_matches = raw.get("season_matches")
        season_matches = (
            raw_season_matches
            if isinstance(raw_season_matches, list)
            else _infer_season_matches(
                text=text_for_inference,
                published_at=(str(raw.get("published_at") or "").strip() or None),
                season_windows=season_windows,
            )
        )
        published_at = str(raw.get("published_at") or "").strip() or None
        image_url = str(raw.get("image_url") or "").strip() or None
        original_image_url = str(raw.get("original_image_url") or "").strip() or image_url
        hosted_image_url = str(raw.get("hosted_image_url") or "").strip() or None
        media_asset_id = str(raw.get("media_asset_id") or "").strip() or None
        feed_rank_raw = raw.get("feed_rank")
        try:
            feed_rank = int(feed_rank_raw)
        except (TypeError, ValueError):
            feed_rank = 0
        normalized.append(
            {
                "source_id": _GOOGLE_SOURCE_ID,
                "headline": headline,
                "article_url": article_url,
                "summary": summary,
                "image_url": image_url,
                "original_image_url": original_image_url,
                "hosted_image_url": hosted_image_url,
                "media_asset_id": media_asset_id,
                "featured_image_synced": bool(raw.get("featured_image_synced")) or bool(hosted_image_url),
                "published_at": published_at,
                "publisher_name": (str(raw.get("publisher_name") or "").strip() or None),
                "publisher_domain": (str(raw.get("publisher_domain") or "").strip() or None),
                "person_tags": person_tags,
                "topic_tags": topic_tags,
                "season_matches": season_matches,
                "feed_rank": feed_rank,
            }
        )
    return normalized


def _normalize_bravo_news_items(
    *,
    items: list[dict[str, Any]],
    season_windows: dict[int, tuple[date, date]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw in items:
        article_url = str(raw.get("article_url") or "").strip()
        if not article_url:
            continue
        headline = str(raw.get("headline") or "").strip() or None
        text_for_inference = headline or ""
        explicit_season = int(raw.get("season_number") or 0) if isinstance(raw.get("season_number"), int) else None
        normalized.append(
            {
                "source_id": _BRAVO_SOURCE_ID,
                "headline": headline,
                "article_url": article_url,
                "image_url": (str(raw.get("image_url") or "").strip() or None),
                "published_at": (str(raw.get("published_at") or "").strip() or None),
                "publisher_name": "BravoTV",
                "publisher_domain": "bravotv.com",
                "person_tags": raw.get("person_tags") if isinstance(raw.get("person_tags"), list) else [],
                "topic_tags": _infer_topic_tags(text_for_inference),
                "season_matches": _infer_season_matches(
                    text=text_for_inference,
                    published_at=(str(raw.get("published_at") or "").strip() or None),
                    season_windows=season_windows,
                    explicit_season_number=explicit_season,
                ),
                "feed_rank": None,
            }
        )
    return normalized


def _parse_sources(value: str | None) -> list[str]:
    if not value:
        return [_BRAVO_SOURCE_ID, _GOOGLE_SOURCE_ID]
    parsed = [token.strip().lower() for token in value.split(",") if token.strip()]
    allowed = {_BRAVO_SOURCE_ID, _GOOGLE_SOURCE_ID}
    selected = [token for token in parsed if token in allowed]
    return selected or [_BRAVO_SOURCE_ID, _GOOGLE_SOURCE_ID]


def _apply_news_filters(
    *,
    items: list[dict[str, Any]],
    person_id: str | None,
    source_filter: str | None,
    topic_filter: str | None,
    season_number: int | None,
) -> list[dict[str, Any]]:
    out = items
    if person_id:
        out = [
            item
            for item in out
            if any(
                isinstance(tag, dict) and str(tag.get("person_id") or "").strip() == person_id
                for tag in (item.get("person_tags") or [])
            )
        ]
    if source_filter:
        source_token = source_filter.strip().lower()
        out = [
            item
            for item in out
            if source_token
            in {
                str(item.get("publisher_name") or "").strip().lower(),
                str(item.get("publisher_domain") or "").strip().lower(),
            }
            or source_token in str(item.get("publisher_name") or "").strip().lower()
            or source_token in str(item.get("publisher_domain") or "").strip().lower()
        ]
    if topic_filter:
        topic_token = topic_filter.strip().lower()
        out = [
            item
            for item in out
            if any(str(tag).strip().lower() == topic_token for tag in (item.get("topic_tags") or []))
        ]
    if season_number:
        out = [
            item
            for item in out
            if any(
                isinstance(match, dict) and int(match.get("season_number") or 0) == season_number
                for match in (item.get("season_matches") or [])
            )
        ]
    return out


def _sort_news(items: list[dict[str, Any]], *, mode: Literal["trending", "latest"]) -> list[dict[str, Any]]:
    def _published_sort_value(item: dict[str, Any]) -> float:
        parsed = _parse_datetime(item.get("published_at"))
        return parsed.timestamp() if parsed else 0.0

    if mode == "latest":
        sorted_items = sorted(items, key=lambda item: _published_sort_value(item), reverse=True)
        return [{**item, "trending_rank": None} for item in sorted_items]

    def _key(item: dict[str, Any]) -> tuple[int, int, float]:
        is_google = item.get("source_id") == _GOOGLE_SOURCE_ID
        bucket = 0 if is_google else 1
        if is_google:
            rank = int(item.get("feed_rank") or 0)
            return (bucket, rank, -_published_sort_value(item))
        return (bucket, 10**9, -_published_sort_value(item))

    sorted_items = sorted(items, key=_key)
    return [{**item, "trending_rank": index + 1} for index, item in enumerate(sorted_items)]


def _is_snapshot_fresh(snapshot: dict[str, Any] | None) -> bool:
    if not snapshot:
        return False
    if str(snapshot.get("status") or "").lower() != "success":
        return False
    fetched_at = _parse_datetime(snapshot.get("fetched_at"))
    if not fetched_at:
        return False
    age = datetime.now(UTC) - fetched_at.astimezone(UTC)
    return age <= timedelta(minutes=_STALE_WINDOW_MINUTES)


def _snapshot_needs_google_image_backfill(snapshot: dict[str, Any] | None) -> bool:
    if not snapshot:
        return False
    payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}
    normalized = payload.get("normalized") if isinstance(payload, dict) else {}
    news_items = normalized.get("news") if isinstance(normalized, dict) else None
    if not isinstance(news_items, list) or not news_items:
        return False
    for item in news_items:
        if not isinstance(item, dict):
            continue
        image_url = str(item.get("image_url") or "").strip()
        hosted_image_url = str(item.get("hosted_image_url") or "").strip()
        original_image_url = str(item.get("original_image_url") or "").strip()
        # Retry sync when an item has no image at all, or only external image URLs
        # that were never mirrored to hosted storage.
        if not image_url and not hosted_image_url and not original_image_url:
            return True
        if not hosted_image_url and (image_url or original_image_url):
            return True
    return False


def _sync_google_news_featured_images(
    *,
    db: SupabaseAdminClient,
    admin_user: AdminUser,
    show_id: str,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    # Reuse the Bravo media import path so Google featured images are mirrored to S3/Supabase too.
    from api.routers.admin_scrape import ImportImageItem, ImportRequest, import_images

    indexed_by_image_url: dict[str, list[tuple[int, str]]] = {}
    for index, item in enumerate(items):
        article_url = str(item.get("article_url") or "").strip()
        image_url = str(item.get("image_url") or "").strip()
        if not article_url or not image_url:
            continue
        item["original_image_url"] = str(item.get("original_image_url") or image_url).strip()
        item["featured_image_synced"] = bool(item.get("featured_image_synced"))
        indexed_by_image_url.setdefault(image_url, []).append((index, article_url))

    imported = 0
    skipped = 0
    mirrored = 0
    linked_items = 0
    errors: list[str] = []

    for image_index, (image_url, references) in enumerate(indexed_by_image_url.items(), start=1):
        source_item = items[references[0][0]]
        source_article_url = references[0][1]
        headline = str(source_item.get("headline") or "").strip()
        caption = f"{_GOOGLE_NEWS_IMAGE_CAPTION}: {headline[:120]}" if headline else _GOOGLE_NEWS_IMAGE_CAPTION
        try:
            import_request = ImportRequest(
                entity_type="show",
                show_id=UUID(show_id),
                source_url=source_article_url,
                images=[
                    ImportImageItem(
                        candidate_id=f"google-news-featured-{image_index}",
                        url=image_url,
                        caption=caption,
                        kind="promo",
                        context_section="google_news",
                        context_type="featured_image",
                        source_logo="google_news",
                        asset_name=_GOOGLE_NEWS_IMAGE_CAPTION,
                    )
                ],
            )
            import_result = import_images(import_request, db, admin_user)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{source_article_url}: {exc}")
            continue

        imported += int(import_result.imported)
        skipped += int(import_result.skipped_duplicates)
        for error in import_result.errors:
            errors.append(f"{source_article_url}: {error}")

        first_asset = import_result.assets[0] if import_result.assets else None
        hosted_url = str(first_asset.hosted_url).strip() if first_asset and first_asset.hosted_url else ""
        if not hosted_url:
            continue
        media_asset_id = str(first_asset.id).strip() if first_asset and first_asset.id else None
        mirrored += 1
        for item_index, _article_url in references:
            item = items[item_index]
            item["original_image_url"] = str(item.get("original_image_url") or image_url).strip()
            item["hosted_image_url"] = hosted_url
            item["image_url"] = hosted_url
            item["media_asset_id"] = media_asset_id
            item["featured_image_synced"] = True
            linked_items += 1

    return {
        "attempted": len(indexed_by_image_url),
        "imported": imported,
        "skipped": skipped,
        "mirrored": mirrored,
        "linked_items": linked_items,
        "errors": errors,
    }


@router.post("/{show_id}/google-news/sync")
def sync_google_news(
    show_id: UUID,
    payload: GoogleNewsSyncRequest,
    db: SupabaseAdminClient = None,
    admin_user: AdminUser = None,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(db, show_id_str):
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")

    google_link = _resolve_google_news_link(show_id_str)
    if not google_link:
        raise HTTPException(
            status_code=409,
            detail="Google News URL is not configured for this show. Add link_kind=google_news_url in show settings.",
        )
    topic_url = str(google_link.get("url") or "").strip()
    if not topic_url:
        raise HTTPException(
            status_code=409,
            detail="Google News URL is empty for this show. Update the show setting before syncing.",
        )

    existing = _fetch_show_snapshot(db, show_id=show_id_str, source_id=_GOOGLE_SOURCE_ID)
    if not payload.force and _is_snapshot_fresh(existing) and not _snapshot_needs_google_image_backfill(existing):
        existing_payload = existing.get("payload") if isinstance(existing.get("payload"), dict) else {}
        normalized = existing_payload.get("normalized") if isinstance(existing_payload, dict) else {}
        existing_news = normalized.get("news") if isinstance(normalized, dict) else []
        return {
            "show_id": show_id_str,
            "synced": False,
            "stale_guard_skipped": True,
            "count": len(existing_news) if isinstance(existing_news, list) else 0,
            "snapshot": {
                "show_id": existing.get("show_id"),
                "source_id": existing.get("source_id"),
                "variant": existing.get("variant"),
                "fetched_at": existing.get("fetched_at"),
                "payload_sha256": existing.get("payload_sha256"),
            },
        }

    _ensure_google_source(db)
    show_name, show_aliases = _show_name_and_aliases(show_id_str)
    cast_index = _build_show_cast_index(show_id_str)
    season_windows = _load_season_windows(show_id_str)

    try:
        parse_result = fetch_google_news(
            topic_url=topic_url,
            show_name=show_name,
            show_aliases=show_aliases,
        )
    except Exception as exc:  # noqa: BLE001
        _upsert_show_snapshot(
            db,
            show_id=show_id_str,
            source_id=_GOOGLE_SOURCE_ID,
            payload={"normalized": {"news": []}},
            status="error",
            error=str(exc),
        )
        raise HTTPException(status_code=502, detail=f"Google News sync failed: {exc}") from exc

    normalized_items = _normalize_google_news_items(
        items=parse_result.get("items") if isinstance(parse_result.get("items"), list) else [],
        cast_index=cast_index,
        season_windows=season_windows,
    )
    image_sync = _sync_google_news_featured_images(
        db=db,
        admin_user=admin_user,
        show_id=show_id_str,
        items=normalized_items,
    )
    snapshot_payload = {
        "show": {
            "show_id": show_id_str,
            "show_name": show_name,
            "aliases": show_aliases,
        },
        "source": {
            "topic_url": topic_url,
            "resolved_feed_url": parse_result.get("resolved_feed_url"),
            "fallback_used": bool(parse_result.get("fallback_used")),
            "attempted_feeds": (
                parse_result.get("attempted_feeds") if isinstance(parse_result.get("attempted_feeds"), list) else []
            ),
            "errors": parse_result.get("errors") if isinstance(parse_result.get("errors"), list) else [],
            "featured_images_added": int(parse_result.get("featured_images_added") or 0),
            "featured_images_probed": int(parse_result.get("featured_images_probed") or 0),
            "featured_image_errors": (
                parse_result.get("featured_image_errors")
                if isinstance(parse_result.get("featured_image_errors"), list)
                else []
            ),
            "image_sync": image_sync,
        },
        "normalized": {
            "news": normalized_items,
        },
    }
    snapshot = _upsert_show_snapshot(
        db,
        show_id=show_id_str,
        source_id=_GOOGLE_SOURCE_ID,
        payload=snapshot_payload,
    )

    return {
        "show_id": show_id_str,
        "synced": True,
        "stale_guard_skipped": False,
        "count": len(normalized_items),
        "fallback_used": bool(parse_result.get("fallback_used")),
        "image_sync": image_sync,
        "snapshot": snapshot,
    }


@router.get("/{show_id}/news")
def get_show_news(
    show_id: UUID,
    sources: str = Query(default=f"{_BRAVO_SOURCE_ID},{_GOOGLE_SOURCE_ID}"),
    person_id: UUID | None = Query(default=None),
    source: str | None = Query(default=None),
    topic: str | None = Query(default=None),
    season_number: int | None = Query(default=None, ge=1, le=200),
    sort: Literal["trending", "latest"] = Query(default="trending"),
    db: SupabaseAdminClient = None,
    _: AdminUser = None,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(db, show_id_str):
        raise HTTPException(status_code=404, detail=f"Show {show_id_str} not found")

    selected_sources = _parse_sources(sources)
    season_windows = _load_season_windows(show_id_str)

    merged: list[dict[str, Any]] = []
    snapshot_meta: dict[str, Any] = {}

    if _BRAVO_SOURCE_ID in selected_sources:
        bravo_snapshot = _fetch_show_snapshot(db, show_id=show_id_str, source_id=_BRAVO_SOURCE_ID)
        if bravo_snapshot:
            bravo_payload = bravo_snapshot.get("payload") if isinstance(bravo_snapshot.get("payload"), dict) else {}
            bravo_news = admin_show_bravo._extract_news_from_snapshot(bravo_payload, db=db)
            merged.extend(_normalize_bravo_news_items(items=bravo_news, season_windows=season_windows))
            snapshot_meta[_BRAVO_SOURCE_ID] = {
                "fetched_at": bravo_snapshot.get("fetched_at"),
                "payload_sha256": bravo_snapshot.get("payload_sha256"),
            }

    if _GOOGLE_SOURCE_ID in selected_sources:
        google_snapshot = _fetch_show_snapshot(db, show_id=show_id_str, source_id=_GOOGLE_SOURCE_ID)
        if google_snapshot:
            google_payload = google_snapshot.get("payload") if isinstance(google_snapshot.get("payload"), dict) else {}
            normalized = google_payload.get("normalized") if isinstance(google_payload, dict) else {}
            google_news = normalized.get("news") if isinstance(normalized, dict) else []
            merged.extend(
                _normalize_google_news_items(
                    items=google_news if isinstance(google_news, list) else [],
                    cast_index=_build_show_cast_index(show_id_str),
                    season_windows=season_windows,
                )
            )
            snapshot_meta[_GOOGLE_SOURCE_ID] = {
                "fetched_at": google_snapshot.get("fetched_at"),
                "payload_sha256": google_snapshot.get("payload_sha256"),
            }

    filtered = _apply_news_filters(
        items=merged,
        person_id=str(person_id) if person_id else None,
        source_filter=source,
        topic_filter=topic,
        season_number=season_number,
    )
    sorted_items = _sort_news(filtered, mode=sort)

    return {
        "news": sorted_items,
        "count": len(sorted_items),
        "sort": sort,
        "sources": selected_sources,
        "snapshots": snapshot_meta,
    }
