from __future__ import annotations

import html
import re
from typing import Any
from uuid import UUID

NAME_SIGNAL_KEYS = (
    "caption",
    "name",
    "title",
    "titles",
    "episode",
    "episode_title",
    "original_source_page",
    "source_page",
)
NAME_CONNECTOR_SPLIT_RE = re.compile(
    r"\s*(?:,|/|&|\band\b|\bwith\b|\bin\b|\bfeat\.?\b|\bfeaturing\b)\s*",
    re.IGNORECASE,
)
NAME_PHRASE_RE = re.compile(r"\b[A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+){1,4}\b")
NON_PERSON_NAME_TOKENS = {
    "and",
    "the",
    "watch",
    "what",
    "happens",
    "live",
    "season",
    "episode",
    "still",
    "frame",
    "poster",
    "photo",
    "gallery",
    "image",
    "imdb",
    "tmdb",
    "fandom",
    "tv",
    "movie",
}


def normalize_uuid_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return str(UUID(candidate))
    except (ValueError, TypeError, AttributeError):
        return None


def _normalize_name_key(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip().lower()
    return normalized or None


def _coerce_people_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for entry in value:
        if not isinstance(entry, str):
            continue
        normalized = entry.strip()
        if normalized:
            out.append(normalized)
    return out


def _coerce_people_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for entry in value:
        if not isinstance(entry, str):
            continue
        normalized = " ".join(entry.split()).strip()
        if normalized:
            out.append(normalized)
    return out


def _has_episode_corroboration(metadata: dict[str, Any]) -> bool:
    imdb_title_type = str(metadata.get("imdb_title_type") or "").strip().upper()
    image_type = str(metadata.get("imdb_image_type") or "").strip().lower()
    return bool(
        imdb_title_type == "TVEPISODE"
        or (isinstance(metadata.get("episode_imdb_id"), str) and str(metadata.get("episode_imdb_id")).strip())
        or (isinstance(metadata.get("episode_title"), str) and str(metadata.get("episode_title")).strip())
        or metadata.get("season_number") is not None
        or metadata.get("episode_number") is not None
        or image_type in {"still_frame", "still frame", "episode_still", "episode still"}
    )


def _resolve_show_id_by_name(
    db: Any,
    show_name: str | None,
    *,
    show_name_cache: dict[str, str | None] | None = None,
) -> str | None:
    if show_name_cache is None:
        show_name_cache = {}
    key = _normalize_name_key(show_name)
    if not key:
        return None
    if key in show_name_cache:
        return show_name_cache[key]

    show_id: str | None = None
    raw_name = " ".join(str(show_name or "").split()).strip()
    try:
        response = db.schema("core").table("shows").select("id,name").ilike("name", raw_name).limit(25).execute()
        rows = response.data if isinstance(getattr(response, "data", None), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_key = _normalize_name_key(row.get("name"))
            if row_key != key:
                continue
            show_id = normalize_uuid_text(str(row.get("id") or ""))
            if show_id:
                break
    except Exception:  # noqa: BLE001
        show_id = None

    if not show_id:
        try:
            alt_response = (
                db.schema("core")
                .table("show_alternative_names")
                .select("show_id,name")
                .ilike("name", raw_name)
                .limit(25)
                .execute()
            )
            alt_rows = alt_response.data if isinstance(getattr(alt_response, "data", None), list) else []
            for row in alt_rows:
                if not isinstance(row, dict):
                    continue
                row_key = _normalize_name_key(row.get("name"))
                if row_key != key:
                    continue
                show_id = normalize_uuid_text(str(row.get("show_id") or ""))
                if show_id:
                    break
        except Exception:  # noqa: BLE001
            show_id = None

    show_name_cache[key] = show_id
    return show_id


def is_trr_show_eligible(
    db: Any,
    *,
    metadata: Any,
    request_show_id: UUID | None = None,
    request_show_name: str | None = None,
    show_exists_cache: dict[str, bool] | None = None,
    show_name_cache: dict[str, str | None] | None = None,
) -> bool:
    if show_exists_cache is None:
        show_exists_cache = {}
    if show_name_cache is None:
        show_name_cache = {}

    def _show_exists(show_id: str | None) -> bool:
        normalized = normalize_uuid_text(show_id)
        if not normalized:
            return False
        if normalized in show_exists_cache:
            return bool(show_exists_cache[normalized])
        try:
            response = db.schema("core").table("shows").select("id").eq("id", normalized).limit(1).execute()
            exists = bool(response.data)
        except Exception:  # noqa: BLE001
            exists = False
        show_exists_cache[normalized] = exists
        return exists

    requested_show_id = normalize_uuid_text(str(request_show_id) if request_show_id is not None else None)
    if not requested_show_id and isinstance(request_show_name, str) and request_show_name.strip():
        requested_show_id = _resolve_show_id_by_name(
            db,
            request_show_name,
            show_name_cache=show_name_cache,
        )
    if requested_show_id and not _show_exists(requested_show_id):
        requested_show_id = None

    metadata_obj = metadata if isinstance(metadata, dict) else {}
    metadata_show_id = normalize_uuid_text(metadata_obj.get("show_id"))
    if metadata_show_id and _show_exists(metadata_show_id):
        return not requested_show_id or metadata_show_id == requested_show_id

    for raw_name in (
        metadata_obj.get("imdb_fallback_show_name"),
        metadata_obj.get("show_name"),
    ):
        if not isinstance(raw_name, str):
            continue
        resolved_show_id = _resolve_show_id_by_name(db, raw_name, show_name_cache=show_name_cache)
        if resolved_show_id and _show_exists(resolved_show_id):
            return not requested_show_id or resolved_show_id == requested_show_id

    source = str(metadata_obj.get("show_context_source") or "").strip().lower()
    if source in {"request_context_inferred", "show_context_request", "request_context"} and requested_show_id:
        has_episode_corroboration = _has_episode_corroboration(metadata_obj)
        if not has_episode_corroboration:
            return False
        fallback_show_id = _resolve_show_id_by_name(
            db,
            str(metadata_obj.get("imdb_fallback_show_name") or "").strip() or None,
            show_name_cache=show_name_cache,
        )
        return bool(fallback_show_id and fallback_show_id == requested_show_id and _show_exists(fallback_show_id))

    return False


def _resolve_person_id_from_name(
    db: Any,
    person_name: str,
    *,
    person_name_id_cache: dict[str, str | None] | None = None,
) -> str | None:
    if person_name_id_cache is None:
        person_name_id_cache = {}
    key = _normalize_name_key(person_name)
    if not key:
        return None
    if key in person_name_id_cache:
        return person_name_id_cache[key]

    resolved: str | None = None
    try:
        response = (
            db.schema("core")
            .table("people")
            .select("id,full_name")
            .ilike("full_name", person_name.strip())
            .limit(5)
            .execute()
        )
        rows = response.data if isinstance(getattr(response, "data", None), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_name_key = _normalize_name_key(row.get("full_name"))
            if row_name_key != key:
                continue
            candidate_id = normalize_uuid_text(str(row.get("id") or "").strip())
            if candidate_id:
                resolved = candidate_id
                break
    except Exception:  # noqa: BLE001
        resolved = None

    person_name_id_cache[key] = resolved
    return resolved


def _is_likely_person_name(value: str) -> bool:
    tokens = [token for token in value.split() if token]
    if len(tokens) < 2 or len(tokens) > 5:
        return False
    blocked = sum(1 for token in tokens if token.lower() in NON_PERSON_NAME_TOKENS)
    return blocked < max(2, len(tokens))


def _iter_name_signal_texts(value: Any) -> list[str]:
    if value is None:
        return []
    texts: list[str] = []
    stack: list[Any] = [value]
    while stack:
        current = stack.pop()
        if current is None:
            continue
        if isinstance(current, str):
            normalized = " ".join(html.unescape(current).split()).strip()
            if normalized:
                texts.append(normalized)
            continue
        if isinstance(current, list):
            stack.extend(current)
            continue
        if isinstance(current, dict):
            for key in NAME_SIGNAL_KEYS:
                if key in current:
                    stack.append(current.get(key))
    return texts


def _extract_candidate_person_names(value: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for raw_text in _iter_name_signal_texts(value):
        text = re.sub(r"https?://\S+", " ", raw_text)
        text = re.sub(r"\(\d{4}\)", " ", text)
        chunks = NAME_CONNECTOR_SPLIT_RE.split(text)
        for chunk in chunks:
            for match in NAME_PHRASE_RE.finditer(chunk):
                candidate = " ".join(match.group(0).split()).strip(" -_.")
                if not candidate:
                    continue
                if not _is_likely_person_name(candidate):
                    continue
                key = candidate.lower()
                if key in seen:
                    continue
                seen.add(key)
                names.append(candidate)
    return names


def _resolve_person_name_from_id(
    db: Any | None,
    person_id: Any,
    *,
    person_id_name_cache: dict[str, str | None] | None = None,
) -> str | None:
    normalized_id = normalize_uuid_text(person_id)
    if not normalized_id:
        return None
    if person_id_name_cache is None:
        person_id_name_cache = {}
    if normalized_id in person_id_name_cache:
        return person_id_name_cache[normalized_id]
    if db is None:
        person_id_name_cache[normalized_id] = None
        return None

    resolved_name: str | None = None
    try:
        response = db.schema("core").table("people").select("full_name").eq("id", normalized_id).limit(1).execute()
        rows = response.data if isinstance(getattr(response, "data", None), list) else []
        if rows and isinstance(rows[0], dict):
            candidate = rows[0].get("full_name")
            if isinstance(candidate, str):
                normalized = " ".join(candidate.split()).strip()
                if normalized:
                    resolved_name = normalized
    except Exception:  # noqa: BLE001
        resolved_name = None

    person_id_name_cache[normalized_id] = resolved_name
    return resolved_name


def _iter_people_pairs(people_ids: Any, people_names: Any) -> list[tuple[str | None, str | None]]:
    ids = _coerce_people_ids(people_ids)
    names = _coerce_people_names(people_names)
    total = max(len(ids), len(names))
    pairs: list[tuple[str | None, str | None]] = []
    for idx in range(total):
        pairs.append((ids[idx] if idx < len(ids) else None, names[idx] if idx < len(names) else None))
    return pairs


def build_assignment_tagged_people(
    *,
    db: Any | None,
    owner_person_id: str | None,
    owner_person_name: str | None = None,
    tagged_people_ids: Any = None,
    tagged_people_names: Any = None,
    row_people_ids: Any = None,
    row_people_names: Any = None,
    metadata_signals: list[Any] | None = None,
    person_name_id_cache: dict[str, str | None] | None = None,
    person_id_name_cache: dict[str, str | None] | None = None,
) -> tuple[list[str], list[str]]:
    if person_name_id_cache is None:
        person_name_id_cache = {}
    if person_id_name_cache is None:
        person_id_name_cache = {}

    merged_ids: list[str] = []
    merged_names: list[str] = []
    seen_ids: set[str] = set()
    seen_names: set[str] = set()

    def _append_candidate(person_id: Any = None, person_name: Any = None) -> None:
        normalized_id = normalize_uuid_text(person_id)
        normalized_name = " ".join(str(person_name).split()).strip() if isinstance(person_name, str) else ""
        normalized_name = normalized_name or None

        if normalized_id and not normalized_name:
            normalized_name = _resolve_person_name_from_id(
                db,
                normalized_id,
                person_id_name_cache=person_id_name_cache,
            )
        if not normalized_id and normalized_name and db is not None:
            normalized_id = _resolve_person_id_from_name(
                db,
                normalized_name,
                person_name_id_cache=person_name_id_cache,
            )
        if normalized_id and normalized_id not in seen_ids:
            seen_ids.add(normalized_id)
            merged_ids.append(normalized_id)
        if normalized_name:
            name_key = normalized_name.lower()
            if name_key not in seen_names:
                seen_names.add(name_key)
                merged_names.append(normalized_name)

    _append_candidate(owner_person_id, owner_person_name)
    for person_id, person_name in _iter_people_pairs(tagged_people_ids, tagged_people_names):
        _append_candidate(person_id, person_name)
    for person_id, person_name in _iter_people_pairs(row_people_ids, row_people_names):
        _append_candidate(person_id, person_name)
    for signal in metadata_signals or []:
        for person_name in _extract_candidate_person_names(signal):
            _append_candidate(None, person_name)

    owner_id = normalize_uuid_text(owner_person_id)
    if owner_id and owner_id not in seen_ids:
        merged_ids.insert(0, owner_id)
    if isinstance(owner_person_name, str):
        owner_name = " ".join(owner_person_name.split()).strip()
        if owner_name:
            owner_key = owner_name.lower()
            if owner_key not in seen_names:
                merged_names.insert(0, owner_name)

    return merged_ids, merged_names


def build_identity_candidate_person_ids(
    *,
    db: Any | None,
    allow_identity_assignment: bool,
    owner_person_id: str | None,
    tagged_people_ids: Any,
    tagged_people_names: Any = None,
    metadata_signals: list[Any] | None = None,
    person_name_id_cache: dict[str, str | None] | None = None,
) -> list[str]:
    if not allow_identity_assignment:
        return []
    candidates: list[str] = []
    seen: set[str] = set()

    def _append(value: Any) -> None:
        normalized = normalize_uuid_text(value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    _append(owner_person_id)
    for tagged_id in _coerce_people_ids(tagged_people_ids):
        _append(tagged_id)

    if db is not None:
        for tagged_name in _coerce_people_names(tagged_people_names):
            resolved_id = _resolve_person_id_from_name(
                db,
                tagged_name,
                person_name_id_cache=person_name_id_cache,
            )
            _append(resolved_id)
        for signal in metadata_signals or []:
            for candidate_name in _extract_candidate_person_names(signal):
                resolved_id = _resolve_person_id_from_name(
                    db,
                    candidate_name,
                    person_name_id_cache=person_name_id_cache,
                )
                _append(resolved_id)

    return candidates
