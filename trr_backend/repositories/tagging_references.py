"""Owner reference image selection for person-gallery tagging."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any, TypedDict
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)

TAGGING_REFERENCE_PROFILE_VERSION = "v1"
TAGGING_REFERENCE_MAX_DEFAULT = 12
TAGGING_REFERENCE_PROFILE_TTL_SECONDS_DEFAULT = 21_600
TAGGING_REFERENCE_PIN_SELECTIONS_DEFAULT = True


class TaggingReferenceImage(TypedDict, total=False):
    url: str
    url_candidates: list[str]
    source_url: str
    hosted_url: str
    media_asset_id: str | None
    link_id: str | None
    rank: int
    reasons: list[str]


class TaggingReferenceSkipped(TypedDict):
    url: str
    reason: str


class TaggingReferenceProfile(TypedDict):
    owner_person_id: str
    requested: int
    accepted: int
    used: list[TaggingReferenceImage]
    skipped: list[TaggingReferenceSkipped]
    cache_hit: bool
    generated_at: str


class FacebankInitialReferenceImage(TypedDict, total=False):
    served_url: str
    source_url: str
    hosted_url: str
    hosted_key: str
    media_asset_id: str | None
    link_id: str | None
    source: str | None
    kind: str | None
    width: int | None
    height: int | None
    is_primary: bool
    position: int | None
    facebank_seed: bool
    rank: int
    selection_bucket: int
    selection_reasons: list[str]


class FacebankInitialReferenceProfile(TypedDict):
    owner_person_id: str
    requested: int
    accepted: int
    used: list[FacebankInitialReferenceImage]
    skipped: list[TaggingReferenceSkipped]
    generated_at: str


def _mapping_or_empty(value: Any) -> dict[str, Any]:
    """Return a row's JSON-object value without changing its runtime shape."""
    return value if isinstance(value, dict) else {}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _reference_ttl_seconds() -> int:
    raw = str(os.getenv("TAGGING_REFERENCE_PROFILE_TTL_SECONDS") or "").strip()
    if not raw:
        return TAGGING_REFERENCE_PROFILE_TTL_SECONDS_DEFAULT
    try:
        parsed = int(raw)
    except ValueError:
        return TAGGING_REFERENCE_PROFILE_TTL_SECONDS_DEFAULT
    return max(0, parsed)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = str(raw).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _pin_existing_reference_selections() -> bool:
    return _env_bool("TAGGING_REFERENCE_PIN_SELECTIONS", TAGGING_REFERENCE_PIN_SELECTIONS_DEFAULT)


def _normalize_uuid_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        from uuid import UUID

        return str(UUID(candidate))
    except Exception:  # noqa: BLE001
        return None


def _normalize_name_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip().lower()
    return normalized or None


def _parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:  # noqa: BLE001
        return None


def _is_http_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    trimmed = value.strip().lower()
    return trimmed.startswith("http://") or trimmed.startswith("https://")


def _canonicalize_url(value: Any) -> str | None:
    if not _is_http_url(value):
        return None
    raw = str(value).strip()
    try:
        parsed = urlsplit(raw)
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, "", ""))
    except Exception:  # noqa: BLE001
        return raw


def _preferred_reference_url(row: dict[str, Any]) -> str | None:
    candidates = _build_reference_url_candidates(row)
    if candidates:
        return candidates[0]
    return None


def _build_reference_url_candidates(row: dict[str, Any], *, preferred_url: str | None = None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in (
        preferred_url,
        row.get("source_url"),
        row.get("hosted_url"),
        row.get("url"),
    ):
        canonical = _canonicalize_url(value)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        out.append(str(value).strip() if isinstance(value, str) else canonical)
    return out


def _preferred_facebank_served_url(row: dict[str, Any]) -> str | None:
    for value in (row.get("hosted_url"), row.get("source_url")):
        if _is_http_url(value):
            return str(value).strip()
    return None


def _extract_reference_url_fields(
    row: dict[str, Any],
    *,
    preferred_url: str | None = None,
) -> tuple[str | None, str | None, list[str]]:
    source_url = str(row.get("source_url") or "").strip() if _is_http_url(row.get("source_url")) else None
    hosted_url = str(row.get("hosted_url") or "").strip() if _is_http_url(row.get("hosted_url")) else None
    candidates = _build_reference_url_candidates(row, preferred_url=preferred_url)
    return source_url, hosted_url, candidates


def _normalize_show_alias(value: Any) -> str | None:
    normalized = _normalize_name_key(value)
    if not normalized:
        return None
    return normalized.replace("&", " and ").replace("-", " ").replace("_", " ").replace("  ", " ").strip()


def _is_wwhl_context(*, request_show_name: str | None, show_name_keys: set[str]) -> bool:
    aliases = {
        "watch what happens live",
        "watch what happens live with andy cohen",
        "wwhl",
    }
    request_alias = _normalize_show_alias(request_show_name)
    if request_alias and any(alias in request_alias for alias in aliases):
        return True
    for key in show_name_keys:
        normalized_key = _normalize_show_alias(key)
        if normalized_key and any(alias in normalized_key for alias in aliases):
            return True
    return False


def _read_tagging_reference(context: Any) -> dict[str, Any] | None:
    if not isinstance(context, dict):
        return None
    value = context.get("tagging_reference")
    if isinstance(value, dict):
        return value
    return None


def _row_context(row: dict[str, Any]) -> dict[str, Any]:
    context = row.get("context")
    return context if isinstance(context, dict) else {}


def _row_metadata(row: dict[str, Any]) -> dict[str, Any]:
    metadata = row.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _row_timestamp(row: dict[str, Any]) -> datetime | None:
    for key in (
        "link_updated_at",
        "asset_updated_at",
        "link_created_at",
        "asset_created_at",
    ):
        parsed = _parse_dt(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _coerce_people_count(context: dict[str, Any], metadata: dict[str, Any]) -> int | None:
    for value in (context.get("people_count"), metadata.get("people_count")):
        if isinstance(value, int):
            return max(0, value)
        if isinstance(value, float) and value.is_integer():
            return max(0, int(value))
        if isinstance(value, str) and value.strip():
            try:
                return max(0, int(value.strip()))
            except ValueError:
                continue
    return None


def _is_solo_candidate(context: dict[str, Any], metadata: dict[str, Any]) -> bool:
    people_count = _coerce_people_count(context, metadata)
    if people_count is not None:
        return people_count <= 1

    for value in (context.get("face_boxes"), metadata.get("face_boxes")):
        if isinstance(value, list):
            return len(value) <= 1

    for value in (context.get("people_ids"), metadata.get("people_ids")):
        if isinstance(value, list):
            return len(value) <= 1

    return False


def _is_manual_upload(row: dict[str, Any]) -> bool:
    source = str(row.get("source") or "").strip().lower()
    if source in {"user_upload", "upload", "manual_upload"}:
        return True
    metadata = _row_metadata(row)
    if metadata.get("upload_id"):
        return True
    if str(metadata.get("source_variant") or "").strip().lower() == "user_upload":
        return True
    return False


def _is_seeded(row: dict[str, Any]) -> bool:
    if bool(row.get("facebank_seed")):
        return True
    context = _row_context(row)
    tag_ref = _read_tagging_reference(context)
    if tag_ref and bool(tag_ref.get("selected")):
        return True
    return False


def _is_starred(row: dict[str, Any]) -> bool:
    context = _row_context(row)
    metadata = _row_metadata(row)
    for value in (
        context.get("starred"),
        context.get("is_starred"),
        metadata.get("starred"),
        metadata.get("is_starred"),
    ):
        if isinstance(value, bool):
            return value
    return False


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _resolution_sort_tuple(row: dict[str, Any]) -> tuple[int, int, int]:
    width = max(_coerce_int(row.get("width")) or 0, 0)
    height = max(_coerce_int(row.get("height")) or 0, 0)
    longest = max(width, height)
    area = width * height
    if longest >= 1400 and area >= 900_000:
        tier = 4
    elif longest >= 1000 and area >= 450_000:
        tier = 3
    elif longest >= 800 and area >= 250_000:
        tier = 2
    elif longest >= 600 and area >= 150_000:
        tier = 1
    elif longest >= 400 and area >= 60_000:
        tier = 0
    else:
        tier = -1
    return tier, longest, area


def _is_event_or_glamour_source(row: dict[str, Any]) -> bool:
    source = str(row.get("source") or "").strip().lower()
    if source in {"getty", "nbcumv"}:
        return True
    metadata = _row_metadata(row)
    context = _row_context(row)
    for value in (
        metadata.get("gallery_bucket_type"),
        metadata.get("media_type_label"),
        context.get("gallery_bucket_type"),
        context.get("media_type_label"),
    ):
        normalized = _normalize_name_key(value)
        if normalized == "event":
            return True
    return False


def _is_publicity_like(row: dict[str, Any]) -> bool:
    source = str(row.get("source") or "").strip().lower()
    if source in {"imdb", "tmdb"}:
        return True
    return bool(row.get("is_primary"))


def _is_very_low_resolution(row: dict[str, Any]) -> bool:
    width = _coerce_int(row.get("width")) or 0
    height = _coerce_int(row.get("height")) or 0
    longest = max(width, height)
    area = max(width, 0) * max(height, 0)
    if longest == 0:
        return False
    return longest < 250 or area < 40_000


def _facebank_selection_bucket(*, manual: bool, seeded: bool, starred: bool, show_priority: bool, solo: bool) -> int:
    if manual and solo:
        return 1
    if manual:
        return 2
    if (seeded or starred) and solo:
        return 3
    if show_priority and solo:
        return 4
    if seeded or starred:
        return 5
    if solo:
        return 6
    return 7


def _load_person_show_context(db: Any, person_id: str) -> tuple[set[str], set[str]]:
    show_ids: set[str] = set()
    show_names: set[str] = set()

    try:
        credits_resp = (
            db.schema("core")
            .table("credits")
            .select("show_id")
            .eq("person_id", person_id)
            .not_.is_("show_id", "null")
            .execute()
        )
        rows = credits_resp.data if isinstance(getattr(credits_resp, "data", None), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized = _normalize_uuid_text(row.get("show_id"))
            if normalized:
                show_ids.add(normalized)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Tagging references: credits lookup failed person_id=%s error=%s", person_id, exc)

    if not show_ids:
        return show_ids, show_names

    try:
        shows_resp = db.schema("core").table("shows").select("id,name").in_("id", list(show_ids)).execute()
        show_rows = shows_resp.data if isinstance(getattr(shows_resp, "data", None), list) else []
        for row in show_rows:
            if not isinstance(row, dict):
                continue
            key = _normalize_name_key(row.get("name"))
            if key:
                show_names.add(key)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Tagging references: show-name lookup failed person_id=%s error=%s", person_id, exc)

    return show_ids, show_names


def _row_matches_show_priority(
    row: dict[str, Any],
    *,
    show_ids: set[str],
    show_name_keys: set[str],
    request_show_id: str | None,
    request_show_name: str | None,
) -> bool:
    context = _row_context(row)
    metadata = _row_metadata(row)

    effective_show_ids = set(show_ids)
    if request_show_id:
        effective_show_ids.add(request_show_id)

    effective_show_names = set(show_name_keys)
    request_name_key = _normalize_name_key(request_show_name)
    if request_name_key:
        effective_show_names.add(request_name_key)

    for candidate in (
        context.get("show_id"),
        metadata.get("show_id"),
    ):
        normalized = _normalize_uuid_text(candidate)
        if normalized and normalized in effective_show_ids:
            return True

    for candidate_name in (
        context.get("show_name"),
        metadata.get("show_name"),
        metadata.get("imdb_fallback_show_name"),
    ):
        key = _normalize_name_key(candidate_name)
        if key and key in effective_show_names:
            return True

    caption = str(row.get("caption") or "").strip().lower()
    if caption and effective_show_names:
        for key in effective_show_names:
            if len(key) >= 5 and key in caption:
                return True

    return False


def _list_gallery_rows(db: Any, person_id: str) -> list[dict[str, Any]]:
    try:
        links_resp = (
            db.schema("core")
            .table("media_links")
            .select("id,media_asset_id,facebank_seed,context,position,created_at,updated_at")
            .eq("entity_type", "person")
            .eq("entity_id", person_id)
            .eq("kind", "gallery")
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Tagging references: media_links query failed person_id=%s error=%s", person_id, exc)
        return []

    if hasattr(links_resp, "error") and links_resp.error:
        logger.warning(
            "Tagging references: media_links query failed person_id=%s error=%s",
            person_id,
            links_resp.error,
        )
        return []

    links = links_resp.data or []
    if not links:
        return []

    asset_ids = [str(row.get("media_asset_id") or "").strip() for row in links if row.get("media_asset_id")]
    asset_ids = [asset_id for asset_id in asset_ids if asset_id]
    if not asset_ids:
        return []

    try:
        assets_resp = (
            db.schema("core")
            .table("media_assets")
            .select("id,source,source_url,hosted_url,hosted_key,width,height,caption,metadata,created_at,updated_at")
            .in_("id", asset_ids)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Tagging references: media_assets query failed person_id=%s error=%s", person_id, exc)
        return []

    if hasattr(assets_resp, "error") and assets_resp.error:
        logger.warning(
            "Tagging references: media_assets query failed person_id=%s error=%s",
            person_id,
            assets_resp.error,
        )
        return []

    assets_by_id = {
        str(row.get("id") or "").strip(): row
        for row in (assets_resp.data or [])
        if isinstance(row, dict) and row.get("id")
    }

    rows: list[dict[str, Any]] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        media_asset_id = str(link.get("media_asset_id") or "").strip()
        if not media_asset_id:
            continue
        asset = assets_by_id.get(media_asset_id)
        if not asset:
            continue
        rows.append(
            {
                "link_id": str(link.get("id") or "").strip(),
                "media_asset_id": media_asset_id,
                "facebank_seed": bool(link.get("facebank_seed")),
                "context": link.get("context") if isinstance(link.get("context"), dict) else {},
                "position": link.get("position"),
                "is_primary": bool(link.get("is_primary")),
                "link_created_at": link.get("created_at"),
                "link_updated_at": link.get("updated_at"),
                "source": asset.get("source"),
                "source_url": asset.get("source_url"),
                "hosted_url": asset.get("hosted_url"),
                "hosted_key": asset.get("hosted_key"),
                "width": asset.get("width"),
                "height": asset.get("height"),
                "caption": asset.get("caption"),
                "metadata": asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {},
                "asset_created_at": asset.get("created_at"),
                "asset_updated_at": asset.get("updated_at"),
            }
        )
    return rows


def _selected_from_context(rows: list[dict[str, Any]]) -> list[TaggingReferenceImage]:
    selected: list[TaggingReferenceImage] = []
    for row in rows:
        context = _row_context(row)
        tag_ref = _read_tagging_reference(context)
        if not tag_ref or not bool(tag_ref.get("selected")):
            continue
        url = _preferred_reference_url(row)
        if not _is_http_url(url):
            continue
        source_url, hosted_url, url_candidates = _extract_reference_url_fields(row, preferred_url=url)
        rank_raw = tag_ref.get("rank")
        rank = int(rank_raw) if isinstance(rank_raw, int) else len(selected) + 1
        reasons_raw = tag_ref.get("reasons")
        reasons = (
            [str(value).strip() for value in reasons_raw if isinstance(value, str) and str(value).strip()]
            if isinstance(reasons_raw, list)
            else []
        )
        selected_entry: TaggingReferenceImage = {
            "url": str(url).strip(),
            "media_asset_id": str(row.get("media_asset_id") or "").strip() or None,
            "link_id": str(row.get("link_id") or "").strip() or None,
            "rank": max(1, rank),
            "reasons": reasons,
        }
        if source_url:
            selected_entry["source_url"] = source_url
        if hosted_url:
            selected_entry["hosted_url"] = hosted_url
        if url_candidates:
            selected_entry["url_candidates"] = url_candidates
        selected.append(selected_entry)

    selected.sort(key=lambda entry: int(entry.get("rank") or 0))
    for index, entry in enumerate(selected, start=1):
        entry["rank"] = index
    return selected


def _is_profile_stale(rows: list[dict[str, Any]], *, force_refresh: bool) -> tuple[bool, datetime | None]:
    if force_refresh:
        return True, None

    selected_rows = []
    latest_computed_at: datetime | None = None
    for row in rows:
        context = _row_context(row)
        tag_ref = _read_tagging_reference(context)
        if not tag_ref or not bool(tag_ref.get("selected")):
            continue
        selected_rows.append(row)
        computed_at = _parse_dt(tag_ref.get("computed_at"))
        if computed_at and (latest_computed_at is None or computed_at > latest_computed_at):
            latest_computed_at = computed_at

    if not selected_rows or latest_computed_at is None:
        return True, latest_computed_at

    if _pin_existing_reference_selections():
        return False, latest_computed_at

    ttl_seconds = _reference_ttl_seconds()
    if ttl_seconds >= 0:
        age_seconds = (datetime.now(UTC) - latest_computed_at).total_seconds()
        if age_seconds > ttl_seconds:
            return True, latest_computed_at

    latest_row_timestamp = max(
        (timestamp for row in rows if (timestamp := _row_timestamp(row)) is not None),
        default=None,
    )
    if latest_row_timestamp and latest_row_timestamp > latest_computed_at:
        return True, latest_computed_at

    return False, latest_computed_at


def _apply_selection_to_context(
    db: Any,
    rows: list[dict[str, Any]],
    *,
    selected: list[TaggingReferenceImage],
    computed_at_iso: str,
    profile_version: str = TAGGING_REFERENCE_PROFILE_VERSION,
) -> None:
    selected_by_link_id = {
        str(entry.get("link_id") or "").strip(): entry
        for entry in selected
        if isinstance(entry.get("link_id"), str) and str(entry.get("link_id")).strip()
    }

    updates: list[tuple[str, dict[str, Any]]] = []
    for row in rows:
        link_id = str(row.get("link_id") or "").strip()
        if not link_id:
            continue
        context = dict(row.get("context") or {})
        current = _read_tagging_reference(context)
        selected_entry = selected_by_link_id.get(link_id)

        if selected_entry is not None:
            new_ref = {
                "selected": True,
                "rank": int(selected_entry.get("rank") or 1),
                "reasons": [
                    str(reason).strip()
                    for reason in (selected_entry.get("reasons") or [])
                    if isinstance(reason, str) and str(reason).strip()
                ],
                "profile_version": profile_version,
                "computed_at": computed_at_iso,
            }
            if current != new_ref:
                context["tagging_reference"] = new_ref
                updates.append((link_id, context))
        elif current is not None:
            context.pop("tagging_reference", None)
            updates.append((link_id, context))

    if not updates:
        return

    now_iso = _now_iso()
    for link_id, context in updates:
        try:
            db.schema("core").table("media_links").update({"context": context, "updated_at": now_iso}).eq(
                "id", link_id
            ).execute()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Tagging references: failed to update media_link=%s error=%s", link_id, exc)


def _rank_candidates(
    rows: list[dict[str, Any]],
    *,
    show_ids: set[str],
    show_name_keys: set[str],
    request_show_id: str | None,
    request_show_name: str | None,
    max_refs: int,
) -> tuple[list[TaggingReferenceImage], list[TaggingReferenceSkipped]]:
    skipped: list[TaggingReferenceSkipped] = []
    scored: list[dict[str, Any]] = []
    wwhl_context = _is_wwhl_context(request_show_name=request_show_name, show_name_keys=show_name_keys)

    for row in rows:
        url_candidates = _build_reference_url_candidates(row)
        if not url_candidates:
            skipped.append({"url": "", "reason": "missing_url"})
            continue
        selected_url = url_candidates[0]
        source_url = str(row.get("source_url") or "").strip() if _is_http_url(row.get("source_url")) else None
        hosted_url = str(row.get("hosted_url") or "").strip() if _is_http_url(row.get("hosted_url")) else None

        context = _row_context(row)
        metadata = _row_metadata(row)

        is_manual = _is_manual_upload(row)
        is_seeded = _is_seeded(row)
        is_starred = _is_starred(row)
        is_solo = _is_solo_candidate(context, metadata)
        show_priority = _row_matches_show_priority(
            row,
            show_ids=show_ids,
            show_name_keys=show_name_keys,
            request_show_id=request_show_id,
            request_show_name=request_show_name,
        )

        reasons: list[str] = []
        if is_manual:
            reasons.append("manual_upload")
        if is_seeded:
            reasons.append("seeded")
        if is_starred:
            reasons.append("starred")
        if show_priority:
            reasons.append("show_priority")
        elif wwhl_context and (is_seeded or is_starred):
            reasons.append("cross_title_wwhl")
        if is_solo:
            reasons.append("solo")
        if not reasons:
            reasons.append("fallback")

        if is_manual and is_solo:
            bucket = 1
        elif is_manual:
            bucket = 2
        elif (is_seeded or is_starred) and is_solo:
            bucket = 3
        elif show_priority and is_solo:
            bucket = 4
        elif is_seeded or is_starred:
            bucket = 4 if wwhl_context else 5
        elif is_solo:
            bucket = 5 if wwhl_context else 6
        else:
            bucket = 6 if wwhl_context else 7

        scored.append(
            {
                "row": row,
                "url": str(selected_url).strip(),
                "url_candidates": url_candidates,
                "source_url": source_url,
                "hosted_url": hosted_url,
                "canonical_url": _canonicalize_url(selected_url),
                "media_asset_id": str(row.get("media_asset_id") or "").strip() or None,
                "link_id": str(row.get("link_id") or "").strip() or None,
                "reasons": reasons,
                "bucket": bucket,
                "seed_boost": 1 if is_seeded else 0,
                "star_boost": 1 if is_starred else 0,
                "manual_boost": 1 if is_manual else 0,
                "show_boost": 1 if show_priority else 0,
                "timestamp": _row_timestamp(row) or datetime.fromtimestamp(0, tz=UTC),
                "position": row_position if isinstance((row_position := row.get("position")), int) else 2**31,
            }
        )

    scored.sort(
        key=lambda entry: (
            int(entry["bucket"]),
            -int(entry["manual_boost"]),
            -int(entry["seed_boost"]),
            -int(entry["star_boost"]),
            -int(entry["show_boost"]),
            int(entry["position"]),
            -entry["timestamp"].timestamp(),
        )
    )

    selected: list[TaggingReferenceImage] = []
    seen_asset_ids: set[str] = set()
    seen_urls: set[str] = set()

    for candidate in scored:
        media_asset_id = candidate.get("media_asset_id")
        canonical_url = candidate.get("canonical_url")
        if media_asset_id and media_asset_id in seen_asset_ids:
            skipped.append({"url": str(candidate.get("url") or ""), "reason": "duplicate_media_asset"})
            continue
        if canonical_url and canonical_url in seen_urls:
            skipped.append({"url": str(candidate.get("url") or ""), "reason": "duplicate_url"})
            continue

        candidate_source_url = candidate.get("source_url")
        candidate_hosted_url = candidate.get("hosted_url")
        candidate_url_candidates = candidate.get("url_candidates")
        candidate_link_id = candidate.get("link_id")
        candidate_reasons = candidate.get("reasons")
        selected_entry: TaggingReferenceImage = {
            "url": str(candidate.get("url") or ""),
            "media_asset_id": media_asset_id if isinstance(media_asset_id, str) else None,
            "link_id": candidate_link_id if isinstance(candidate_link_id, str) else None,
            "rank": len(selected) + 1,
            "reasons": [reason for reason in candidate_reasons if isinstance(reason, str)]
            if isinstance(candidate_reasons, list)
            else [],
        }
        if isinstance(candidate_source_url, str):
            selected_entry["source_url"] = candidate_source_url
        if isinstance(candidate_hosted_url, str):
            selected_entry["hosted_url"] = candidate_hosted_url
        if isinstance(candidate_url_candidates, list) and all(isinstance(url, str) for url in candidate_url_candidates):
            selected_entry["url_candidates"] = candidate_url_candidates
        selected.append(selected_entry)
        if media_asset_id:
            seen_asset_ids.add(media_asset_id)
        if canonical_url:
            seen_urls.add(canonical_url)
        if len(selected) >= max_refs:
            break

    return selected, skipped


def _rank_facebank_initial_candidates(
    rows: list[dict[str, Any]],
    *,
    show_ids: set[str],
    show_name_keys: set[str],
    request_show_id: str | None,
    request_show_name: str | None,
    max_refs: int,
) -> tuple[list[FacebankInitialReferenceImage], list[TaggingReferenceSkipped]]:
    skipped: list[TaggingReferenceSkipped] = []
    scored: list[dict[str, Any]] = []

    for row in rows:
        served_url = _preferred_facebank_served_url(row)
        if not served_url:
            skipped.append({"url": "", "reason": "missing_served_url"})
            continue
        if _is_very_low_resolution(row):
            skipped.append({"url": served_url, "reason": "very_low_resolution"})
            continue

        context = _row_context(row)
        metadata = _row_metadata(row)
        manual = _is_manual_upload(row)
        seeded = bool(row.get("facebank_seed"))
        starred = _is_starred(row)
        solo = _is_solo_candidate(context, metadata)
        show_priority = _row_matches_show_priority(
            row,
            show_ids=show_ids,
            show_name_keys=show_name_keys,
            request_show_id=request_show_id,
            request_show_name=request_show_name,
        )
        publicity_like = _is_publicity_like(row)
        glamour_like = _is_event_or_glamour_source(row)
        resolution_tier, longest_edge, area = _resolution_sort_tuple(row)
        bucket = _facebank_selection_bucket(
            manual=manual,
            seeded=seeded,
            starred=starred,
            show_priority=show_priority,
            solo=solo,
        )
        reasons: list[str] = []
        if manual:
            reasons.append("manual_upload")
        if seeded:
            reasons.append("seeded")
        if starred:
            reasons.append("starred")
        if show_priority:
            reasons.append("show_priority")
        if solo:
            reasons.append("solo")
        if bool(row.get("is_primary")):
            reasons.append("primary")
        if publicity_like:
            reasons.append("publicity_like")
        if glamour_like:
            reasons.append("event_or_glamour")
        if resolution_tier >= 1:
            reasons.append("resolution_preferred")
        elif resolution_tier >= 0:
            reasons.append("resolution_acceptable")
        else:
            reasons.append("resolution_fallback")

        scored.append(
            {
                "row": row,
                "served_url": served_url,
                "manual": manual,
                "seeded": seeded,
                "starred": starred,
                "solo": solo,
                "show_priority": show_priority,
                "publicity_like": publicity_like,
                "glamour_like": glamour_like,
                "bucket": bucket,
                "resolution_tier": resolution_tier,
                "longest_edge": longest_edge,
                "area": area,
                "timestamp": _row_timestamp(row) or datetime.fromtimestamp(0, tz=UTC),
                "position": row_position if isinstance((row_position := row.get("position")), int) else 2**31,
                "reasons": reasons,
            }
        )

    scored.sort(
        key=lambda entry: (
            int(entry["bucket"]),
            -int(entry["resolution_tier"]),
            -int(entry["show_priority"]),
            -int(bool(entry["row"].get("is_primary"))),
            -int(entry["publicity_like"]),
            -int(entry["longest_edge"]),
            -int(entry["area"]),
            int(entry["position"]),
            -entry["timestamp"].timestamp(),
        )
    )

    selected: list[FacebankInitialReferenceImage] = []
    seen_asset_ids: set[str] = set()
    seen_urls: set[str] = set()
    glamour_count = 0

    def try_add(
        candidate: dict[str, Any],
        *,
        require_solo: bool,
        allow_glamour: bool,
    ) -> bool:
        nonlocal glamour_count
        media_asset_id = str(candidate["row"].get("media_asset_id") or "").strip()
        canonical_url = _canonicalize_url(candidate.get("served_url"))
        if media_asset_id and media_asset_id in seen_asset_ids:
            return False
        if canonical_url and canonical_url in seen_urls:
            return False
        if require_solo and not bool(candidate.get("solo")):
            return False
        if not allow_glamour and bool(candidate.get("glamour_like")):
            return False
        if bool(candidate.get("glamour_like")) and glamour_count >= 1 and len(selected) >= min(3, max_refs):
            return False

        row = candidate["row"]
        selected_entry: FacebankInitialReferenceImage = {
            "served_url": str(candidate["served_url"]),
            "media_asset_id": media_asset_id or None,
            "link_id": str(row.get("link_id") or "").strip() or None,
            "source": str(row.get("source") or "").strip() or None,
            "kind": str(row.get("kind") or "").strip() or None,
            "width": _coerce_int(row.get("width")),
            "height": _coerce_int(row.get("height")),
            "is_primary": bool(row.get("is_primary")),
            "position": _coerce_int(row.get("position")),
            "facebank_seed": bool(row.get("facebank_seed")),
            "rank": len(selected) + 1,
            "selection_bucket": int(candidate["bucket"]),
            "selection_reasons": [reason for reason in candidate.get("reasons", []) if isinstance(reason, str)],
        }
        source_url = row.get("source_url")
        hosted_url = row.get("hosted_url")
        hosted_key = str(row.get("hosted_key") or "").strip()
        if _is_http_url(source_url):
            selected_entry["source_url"] = str(source_url)
        if _is_http_url(hosted_url):
            selected_entry["hosted_url"] = str(hosted_url)
        if hosted_key:
            selected_entry["hosted_key"] = hosted_key
        selected.append(selected_entry)
        if media_asset_id:
            seen_asset_ids.add(media_asset_id)
        if canonical_url:
            seen_urls.add(canonical_url)
        if bool(candidate.get("glamour_like")):
            glamour_count += 1
        return True

    def targeted_pick(predicate) -> None:  # noqa: ANN001
        if len(selected) >= max_refs:
            return
        for candidate in scored:
            if not predicate(candidate):
                continue
            if try_add(candidate, require_solo=True, allow_glamour=False):
                return
        for candidate in scored:
            if not predicate(candidate):
                continue
            if try_add(candidate, require_solo=True, allow_glamour=True):
                return

    targeted_pick(lambda item: bool(item.get("show_priority")))
    targeted_pick(lambda item: bool(item.get("publicity_like")) or bool(item["row"].get("is_primary")))

    for require_solo, allow_glamour in (
        (True, False),
        (True, True),
        (False, False),
        (False, True),
    ):
        if len(selected) >= max_refs:
            break
        for candidate in scored:
            if len(selected) >= max_refs:
                break
            if not allow_glamour and glamour_count >= 1 and bool(candidate.get("glamour_like")):
                continue
            try_add(candidate, require_solo=require_solo, allow_glamour=allow_glamour)

    for index, entry in enumerate(selected, start=1):
        entry["rank"] = index

    if len(selected) < max_refs:
        used_link_ids = {str(item.get("link_id") or "").strip() for item in selected if item.get("link_id")}
        for candidate in scored:
            if len(selected) >= max_refs:
                break
            link_id = str(candidate["row"].get("link_id") or "").strip()
            if link_id and link_id in used_link_ids:
                continue
            skipped.append(
                {
                    "url": str(candidate.get("served_url") or ""),
                    "reason": "not_selected_initial_facebank_window",
                }
            )

    return selected[:max_refs], skipped


def build_owner_facebank_initial_reference_profile(
    db: Any,
    person_id: str,
    *,
    show_id: Any | None = None,
    show_name: str | None = None,
    max_refs: int = 5,
    seed_only: bool = False,
) -> FacebankInitialReferenceProfile:
    owner_person_id = _normalize_uuid_text(person_id) or str(person_id).strip()
    requested = max(0, int(max_refs or 5))
    rows = _list_gallery_rows(db, owner_person_id)
    if seed_only:
        rows = [row for row in rows if bool(row.get("facebank_seed"))]

    if not rows or requested == 0:
        return {
            "owner_person_id": owner_person_id,
            "requested": requested,
            "accepted": 0,
            "used": [],
            "skipped": [],
            "generated_at": _now_iso(),
        }

    request_show_id = _normalize_uuid_text(show_id)
    request_show_name = show_name.strip() if isinstance(show_name, str) and show_name.strip() else None
    show_ids, show_names = _load_person_show_context(db, owner_person_id)
    selected, skipped = _rank_facebank_initial_candidates(
        rows,
        show_ids=show_ids,
        show_name_keys=show_names,
        request_show_id=request_show_id,
        request_show_name=request_show_name,
        max_refs=requested,
    )
    return {
        "owner_person_id": owner_person_id,
        "requested": requested,
        "accepted": len(selected),
        "used": selected,
        "skipped": skipped,
        "generated_at": _now_iso(),
    }


def build_owner_tagging_reference_profile(
    db: Any,
    person_id: str,
    *,
    show_id: Any | None = None,
    show_name: str | None = None,
    max_refs: int = TAGGING_REFERENCE_MAX_DEFAULT,
    force_refresh: bool = False,
) -> TaggingReferenceProfile:
    owner_person_id = _normalize_uuid_text(person_id) or str(person_id).strip()
    requested = max(0, int(max_refs or TAGGING_REFERENCE_MAX_DEFAULT))
    rows = _list_gallery_rows(db, owner_person_id)

    if not rows or requested == 0:
        return {
            "owner_person_id": owner_person_id,
            "requested": requested,
            "accepted": 0,
            "used": [],
            "skipped": [],
            "cache_hit": False,
            "generated_at": _now_iso(),
        }

    stale, _latest_computed_at = _is_profile_stale(rows, force_refresh=force_refresh)
    if not stale:
        cached = _selected_from_context(rows)[:requested]
        for index, entry in enumerate(cached, start=1):
            entry["rank"] = index
        return {
            "owner_person_id": owner_person_id,
            "requested": requested,
            "accepted": len(cached),
            "used": cached,
            "skipped": [],
            "cache_hit": True,
            "generated_at": _now_iso(),
        }

    request_show_id = _normalize_uuid_text(show_id)
    request_show_name = show_name.strip() if isinstance(show_name, str) and show_name.strip() else None
    show_ids, show_names = _load_person_show_context(db, owner_person_id)
    selected, skipped = _rank_candidates(
        rows,
        show_ids=show_ids,
        show_name_keys=show_names,
        request_show_id=request_show_id,
        request_show_name=request_show_name,
        max_refs=requested,
    )

    computed_at_iso = _now_iso()
    _apply_selection_to_context(db, rows, selected=selected, computed_at_iso=computed_at_iso)

    return {
        "owner_person_id": owner_person_id,
        "requested": requested,
        "accepted": len(selected),
        "used": selected,
        "skipped": skipped,
        "cache_hit": False,
        "generated_at": computed_at_iso,
    }


def sync_owner_tagging_reference_usage(
    db: Any,
    person_id: str,
    *,
    used_references: list[dict[str, Any]],
    preserve_existing: bool = True,
    profile_version: str = TAGGING_REFERENCE_PROFILE_VERSION,
) -> list[TaggingReferenceImage]:
    owner_person_id = _normalize_uuid_text(person_id) or str(person_id).strip()
    rows = _list_gallery_rows(db, owner_person_id)
    if not rows:
        return []

    by_link: dict[str, dict[str, Any]] = {}
    by_asset: dict[str, dict[str, Any]] = {}
    by_url: dict[str, dict[str, Any]] = {}
    for row in rows:
        link_id = str(row.get("link_id") or "").strip()
        media_asset_id = str(row.get("media_asset_id") or "").strip()
        hosted = _canonicalize_url(row.get("hosted_url"))
        source = _canonicalize_url(row.get("source_url"))
        if link_id:
            by_link[link_id] = row
        if media_asset_id:
            by_asset[media_asset_id] = row
        if hosted:
            by_url[hosted] = row
        if source:
            by_url[source] = row

    selected: list[TaggingReferenceImage] = []
    seen_link_ids: set[str] = set()

    for idx, ref in enumerate(used_references, start=1):
        if not isinstance(ref, dict):
            continue
        link_id = str(ref.get("link_id") or "").strip()
        media_asset_id = str(ref.get("media_asset_id") or "").strip()
        raw_url = str(ref.get("url") or "").strip()
        raw_url_candidates = ref.get("url_candidates")
        ref_url_candidates: list[str] = []
        if isinstance(raw_url_candidates, list):
            for value in raw_url_candidates:
                if not isinstance(value, str):
                    continue
                candidate = value.strip()
                if candidate and _is_http_url(candidate):
                    ref_url_candidates.append(candidate)
        for extra in (ref.get("source_url"), ref.get("hosted_url"), raw_url):
            if isinstance(extra, str):
                candidate = extra.strip()
                if candidate and _is_http_url(candidate):
                    ref_url_candidates.append(candidate)
        canonical_candidates = [
            candidate for candidate in (_canonicalize_url(url) for url in ref_url_candidates) if candidate
        ]

        matched_row = None
        if link_id and link_id in by_link:
            matched_row = by_link[link_id]
        elif media_asset_id and media_asset_id in by_asset:
            matched_row = by_asset[media_asset_id]
        else:
            for canonical_url in canonical_candidates:
                if canonical_url in by_url:
                    matched_row = by_url[canonical_url]
                    break

        if not matched_row:
            continue

        matched_link_id = str(matched_row.get("link_id") or "").strip()
        if not matched_link_id or matched_link_id in seen_link_ids:
            continue
        seen_link_ids.add(matched_link_id)

        reasons_raw = ref.get("reasons")
        reasons = (
            [str(value).strip() for value in reasons_raw if isinstance(value, str) and str(value).strip()]
            if isinstance(reasons_raw, list)
            else []
        )
        preferred_url = raw_url or _preferred_reference_url(matched_row) or ""
        source_url, hosted_url, url_candidates = _extract_reference_url_fields(
            matched_row,
            preferred_url=preferred_url or None,
        )

        selected_entry: TaggingReferenceImage = {
            "url": str(preferred_url).strip(),
            "media_asset_id": str(matched_row.get("media_asset_id") or "").strip() or None,
            "link_id": matched_link_id,
            "rank": idx,
            "reasons": reasons,
        }
        if source_url:
            selected_entry["source_url"] = source_url
        if hosted_url:
            selected_entry["hosted_url"] = hosted_url
        if url_candidates:
            selected_entry["url_candidates"] = url_candidates
        selected.append(selected_entry)

    if preserve_existing:
        existing_selected = _selected_from_context(rows)
        for existing in existing_selected:
            existing_link_id = str(existing.get("link_id") or "").strip()
            if not existing_link_id or existing_link_id in seen_link_ids:
                continue
            seen_link_ids.add(existing_link_id)
            selected_entry: TaggingReferenceImage = {
                "url": str(existing.get("url") or "").strip(),
                "media_asset_id": str(existing.get("media_asset_id") or "").strip() or None,
                "link_id": existing_link_id,
                "rank": len(selected) + 1,
                "reasons": [
                    str(reason).strip()
                    for reason in (existing.get("reasons") or [])
                    if isinstance(reason, str) and str(reason).strip()
                ],
            }
            source_url = existing.get("source_url")
            hosted_url = existing.get("hosted_url")
            url_candidates = existing.get("url_candidates")
            if isinstance(source_url, str) and source_url:
                selected_entry["source_url"] = source_url
            if isinstance(hosted_url, str) and hosted_url:
                selected_entry["hosted_url"] = hosted_url
            if isinstance(url_candidates, list):
                selected_entry["url_candidates"] = [url for url in url_candidates if isinstance(url, str)]
            selected.append(selected_entry)

    for index, entry in enumerate(selected, start=1):
        entry["rank"] = index

    computed_at_iso = _now_iso()
    _apply_selection_to_context(
        db,
        rows,
        selected=selected,
        computed_at_iso=computed_at_iso,
        profile_version=profile_version,
    )
    return selected
