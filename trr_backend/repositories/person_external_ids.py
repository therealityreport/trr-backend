"""Repository functions for person external ID writes."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

from psycopg2.extras import Json

from trr_backend.db import pg

PERSON_EXTERNAL_ID_SOURCES = (
    "imdb",
    "tmdb",
    "wikidata",
    "tvdb",
    "tvrage",
    "fandom",
    "facebook",
    "instagram",
    "threads",
    "twitter",
    "tiktok",
    "youtube",
)
PERSON_EXTERNAL_ID_SOURCE_SET = frozenset(PERSON_EXTERNAL_ID_SOURCES)

MANAGED_PERSON_EXTERNAL_ID_KEYS = (
    "imdb",
    "imdb_id",
    "tmdb",
    "tmdb_id",
    "wikidata",
    "wikidata_id",
    "tvdb",
    "tvdb_id",
    "tvrage",
    "tvrage_id",
    "fandom",
    "fandom_id",
    "facebook",
    "facebook_id",
    "instagram",
    "instagram_id",
    "twitter",
    "twitter_id",
    "tiktok",
    "tiktok_id",
    "youtube",
    "youtube_id",
)

PERSON_EXTERNAL_ID_UNIQUE_CONFLICT_MESSAGES = {
    "person_external_ids_unique_active_handles_uq": "That social handle is already assigned to another person.",
    "person_external_ids_unique_identifiers_uq": "That external ID is already assigned to another person.",
    "person_external_ids_primary_uq": "Only one primary record is allowed per source for a person.",
}

URL_PREFIX_RE = re.compile(r"^https?://", re.I)


class PersonExternalIdError(RuntimeError):
    """Base error for person external ID writes."""


class PersonExternalIdNotFoundError(PersonExternalIdError):
    """Raised when the requested person row does not exist."""


class UnsupportedPersonExternalIdSourceError(PersonExternalIdError):
    """Raised when a write payload contains an unsupported source."""


class PersonExternalIdConflictError(PersonExternalIdError):
    """Raised when a unique external-ID constraint is violated."""


def _parse_url_or_none(value: str):
    if not URL_PREFIX_RE.search(value):
        return None
    try:
        return urlparse(value)
    except ValueError:
        return None


def _trim_path_segments(pathname: str) -> list[str]:
    return [segment.strip() for segment in pathname.strip("/").split("/") if segment.strip()]


def _first_query_value(query: str, key: str) -> str | None:
    values = parse_qs(query, keep_blank_values=False).get(key)
    if not values:
        return None
    value = values[0].strip()
    return value or None


def _normalize_imdb_value(value: str) -> str:
    trimmed = value.strip()
    parsed = _parse_url_or_none(trimmed)
    candidates = _trim_path_segments(parsed.path) if parsed else []
    for segment in candidates:
        if re.fullmatch(r"nm\d+", segment, flags=re.I):
            return segment.lower()
    inline_match = re.search(r"nm\d+", trimmed, flags=re.I)
    return inline_match.group(0).lower() if inline_match else trimmed


def _normalize_wikidata_value(value: str) -> str:
    trimmed = value.strip()
    parsed = _parse_url_or_none(trimmed)
    candidates = _trim_path_segments(parsed.path) if parsed else []
    matched = next((segment for segment in reversed(candidates) if re.fullmatch(r"[PQ]\d+", segment, re.I)), None)
    inline_match = re.search(r"[PQ]\d+", trimmed, flags=re.I)
    normalized = matched or (inline_match.group(0) if inline_match else trimmed)
    if re.fullmatch(r"[PQ]\d+", normalized, flags=re.I):
        return f"{normalized[0].upper()}{normalized[1:]}"
    return normalized


def _normalize_fandom_value(value: str) -> str:
    trimmed = value.strip()
    parsed = _parse_url_or_none(trimmed)
    if not parsed:
        return trimmed
    pathname = parsed.path.rstrip("/")
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return f"{origin}{pathname or '/'}"


def _normalize_social_path_value(source: str, value: str) -> str:
    trimmed = value.strip()
    parsed = _parse_url_or_none(trimmed)
    if not parsed:
        return trimmed

    path_segments = _trim_path_segments(parsed.path)
    if source == "facebook":
        first = path_segments[0] if path_segments else ""
        if first.lower() == "profile.php":
            return _first_query_value(parsed.query, "id") or trimmed
        if first.lower() == "people" and len(path_segments) > 2:
            return path_segments[2]
        if first.lower() == "pg" and len(path_segments) > 1:
            return path_segments[1]
        return first or trimmed

    if source == "twitter":
        return _first_query_value(parsed.query, "screen_name") or (path_segments[0] if path_segments else trimmed)

    if source == "threads":
        first = path_segments[0] if path_segments else trimmed
        return first.lstrip("@")

    if source == "instagram":
        return path_segments[0] if path_segments else trimmed

    if source == "tiktok":
        first = path_segments[0] if path_segments else trimmed
        if first.startswith("@"):
            return first.lstrip("@")
        if first.lower() == "@" and len(path_segments) > 1:
            return path_segments[1].lstrip("@")
        return first.lstrip("@")

    if source == "youtube":
        first = path_segments[0] if path_segments else ""
        second = path_segments[1] if len(path_segments) > 1 else ""
        if not first:
            return trimmed
        if first.startswith("@"):
            return first
        if first.lower() == "channel" and second:
            return second
        if first.lower() in {"user", "c"} and second:
            return f"{first.lower()}/{second}"
        return second or first

    return trimmed


def normalize_person_external_id_value(source: str, value: str) -> str:
    source_id = source.strip().lower()
    trimmed = value.strip()
    if not trimmed:
        return ""

    if source_id == "imdb":
        return _normalize_imdb_value(trimmed)
    if source_id == "wikidata":
        return _normalize_wikidata_value(trimmed)
    if source_id == "fandom":
        return _normalize_fandom_value(trimmed)
    if source_id in {"tmdb", "tvdb", "tvrage"}:
        digits_only = re.sub(r"\D+", "", trimmed)
        return digits_only or trimmed
    if source_id in {"facebook", "instagram", "threads", "twitter", "tiktok"}:
        return _normalize_social_path_value(source_id, trimmed).lstrip("@")
    if source_id == "youtube":
        normalized = _normalize_social_path_value(source_id, trimmed)
        return normalized if normalized.startswith("@") else normalized
    return trimmed


def _normalize_input(input_row: Mapping[str, Any]) -> dict[str, Any] | None:
    source_id = str(input_row.get("source_id") or "").strip().lower()
    if source_id not in PERSON_EXTERNAL_ID_SOURCE_SET:
        raise UnsupportedPersonExternalIdSourceError(f"Unsupported source: {source_id or 'unknown'}")
    external_id = normalize_person_external_id_value(source_id, str(input_row.get("external_id") or ""))
    if not external_id:
        return None
    return {
        "source_id": source_id,
        "external_id": external_id,
        "is_primary": bool(input_row.get("is_primary", True)),
        "valid_from": input_row.get("valid_from"),
        "valid_to": input_row.get("valid_to"),
    }


def _coerce_numeric_identifier(value: str) -> int | str:
    if not re.fullmatch(r"\d+", value):
        return value
    try:
        return int(value)
    except ValueError:
        return value


def _build_legacy_external_ids_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for record in records:
        if not bool(record.get("is_primary")) or record.get("valid_to"):
            continue
        source_id = str(record.get("source_id") or "").strip().lower()
        external_id = str(record.get("external_id") or "")
        normalized_value = normalize_person_external_id_value(source_id, external_id)
        if not normalized_value:
            continue
        if source_id in {
            "imdb",
            "wikidata",
            "fandom",
            "facebook",
            "instagram",
            "threads",
            "twitter",
            "tiktok",
            "youtube",
        }:
            result[source_id] = normalized_value
            result[f"{source_id}_id"] = normalized_value
        elif source_id in {"tmdb", "tvdb", "tvrage"}:
            coerced = _coerce_numeric_identifier(normalized_value)
            result[source_id] = coerced
            result[f"{source_id}_id"] = coerced
    return result


def _build_mirrored_external_ids(
    existing_external_ids: Mapping[str, Any] | None,
    active_records: list[dict[str, Any]],
) -> dict[str, Any]:
    next_external_ids = dict(existing_external_ids or {})
    for key in MANAGED_PERSON_EXTERNAL_ID_KEYS:
        next_external_ids.pop(key, None)
    next_external_ids.update(_build_legacy_external_ids_from_records(active_records))
    return next_external_ids


def _build_person_override_handles(active_records: list[dict[str, Any]]) -> dict[str, str | None]:
    def find_source_value(source_id: str) -> str | None:
        record = next((candidate for candidate in active_records if candidate.get("source_id") == source_id), None)
        if not record:
            return None
        normalized = normalize_person_external_id_value(source_id, str(record.get("external_id") or ""))
        return normalized or None

    return {
        "instagram_handle": find_source_value("instagram"),
        "tiktok_handle": find_source_value("tiktok"),
        "twitter_handle": find_source_value("twitter"),
        "youtube_handle": find_source_value("youtube"),
    }


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def _map_primary_person_external_id_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    source_id = str(row.get("source_id") or "").strip().lower()
    if source_id not in PERSON_EXTERNAL_ID_SOURCE_SET:
        return None
    external_id = str(row.get("external_id") or "").strip()
    if not external_id:
        return None
    raw_id = row.get("id")
    if isinstance(raw_id, int):
        row_id = raw_id
    elif isinstance(raw_id, str) and raw_id.isdigit():
        row_id = int(raw_id)
    else:
        row_id = None
    return {
        "id": row_id,
        "source_id": source_id,
        "external_id": external_id,
        "is_primary": row.get("is_primary") is not False,
        "valid_from": _json_value(row.get("valid_from")),
        "valid_to": _json_value(row.get("valid_to")),
        "observed_at": _json_value(row.get("observed_at")),
        "created_at": _json_value(row.get("created_at")),
        "updated_at": _json_value(row.get("updated_at")),
    }


def map_primary_person_external_id_rows(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = []
    for row in rows:
        record = _map_primary_person_external_id_row(row)
        if record:
            mapped.append(record)
    return mapped


def _extract_constraint_name_from_error(error: Exception) -> str:
    diag = getattr(error, "diag", None)
    constraint = str(getattr(diag, "constraint_name", "") or "").strip()
    if constraint:
        return constraint
    cause = getattr(error, "__cause__", None)
    if cause is not None:
        return _extract_constraint_name_from_error(cause)
    match = re.search(r'constraint "?([a-zA-Z0-9_]+)"?', str(error or ""), flags=re.I)
    return str(match.group(1) if match else "").strip()


def _is_duplicate_violation(error: Exception) -> bool:
    code = str(getattr(error, "pgcode", "") or "").strip()
    if not code:
        cause = getattr(error, "__cause__", None)
        code = str(getattr(cause, "pgcode", "") or "").strip() if cause is not None else ""
    return code == "23505" or "duplicate key value violates unique constraint" in str(error or "").lower()


def _coerce_repository_error(error: Exception) -> Exception:
    if _is_duplicate_violation(error):
        constraint = _extract_constraint_name_from_error(error)
        return PersonExternalIdConflictError(
            PERSON_EXTERNAL_ID_UNIQUE_CONFLICT_MESSAGES.get(constraint, "External ID conflict")
        )
    return error


def sync_person_external_ids(person_id: str, inputs: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [_normalize_input(input_row) for input_row in inputs]
    deduped_by_source: dict[str, dict[str, Any]] = {}
    for row in normalized:
        if not row:
            continue
        deduped_by_source[str(row["source_id"])] = {**row, "is_primary": True}

    try:
        with pg.db_connection(label="sync-person-external-ids") as conn:
            with pg.db_cursor(conn=conn, label="sync-person-external-ids") as cur:
                cur.execute(
                    """
                    SELECT external_ids
                    FROM core.people
                    WHERE id = %s::uuid
                    LIMIT 1
                    """,
                    [person_id],
                )
                current_person = cur.fetchone()
                if not current_person:
                    raise PersonExternalIdNotFoundError("Person not found")

                desired_rows = list(deduped_by_source.values())
                for row in desired_rows:
                    cur.execute(
                        """
                        INSERT INTO core.person_external_ids (
                          person_id,
                          source_id,
                          external_id,
                          is_primary,
                          valid_from,
                          valid_to,
                          observed_at
                        )
                        VALUES (%s::uuid, %s::text, %s::text, true, %s::date, %s::date, now())
                        ON CONFLICT (person_id, source_id) WHERE (is_primary = true)
                        DO UPDATE
                        SET external_id = EXCLUDED.external_id,
                            valid_from = EXCLUDED.valid_from,
                            valid_to = EXCLUDED.valid_to,
                            observed_at = now(),
                            updated_at = now()
                        """,
                        [
                            person_id,
                            row["source_id"],
                            row["external_id"],
                            row.get("valid_from"),
                            row.get("valid_to"),
                        ],
                    )

                desired_sources = [str(row["source_id"]) for row in desired_rows]
                if desired_sources:
                    cur.execute(
                        """
                        UPDATE core.person_external_ids
                        SET valid_to = COALESCE(valid_to, CURRENT_DATE),
                            updated_at = now()
                        WHERE person_id = %s::uuid
                          AND is_primary = true
                          AND source_id <> ALL(%s::text[])
                        """,
                        [person_id, desired_sources],
                    )
                else:
                    cur.execute(
                        """
                        UPDATE core.person_external_ids
                        SET valid_to = COALESCE(valid_to, CURRENT_DATE),
                            updated_at = now()
                        WHERE person_id = %s::uuid
                          AND is_primary = true
                        """,
                        [person_id],
                    )

                cur.execute(
                    """
                    SELECT
                      id,
                      source_id,
                      external_id,
                      is_primary,
                      valid_from,
                      valid_to,
                      observed_at,
                      created_at,
                      updated_at
                    FROM core.person_external_ids
                    WHERE person_id = %s::uuid
                      AND is_primary = true
                      AND valid_to IS NULL
                    ORDER BY source_id ASC
                    """,
                    [person_id],
                )
                active_records = map_primary_person_external_id_rows([dict(row) for row in cur.fetchall()])

                existing_external_ids = current_person.get("external_ids")
                if not isinstance(existing_external_ids, Mapping):
                    existing_external_ids = {}
                next_external_ids = _build_mirrored_external_ids(existing_external_ids, active_records)
                cur.execute(
                    """
                    UPDATE core.people
                    SET external_ids = %s::jsonb,
                        updated_at = now()
                    WHERE id = %s::uuid
                    """,
                    [Json(next_external_ids), person_id],
                )

                next_handles = _build_person_override_handles(active_records)
                has_any_handle = any(next_handles.values())
                cur.execute(
                    """
                    SELECT person_id
                    FROM core.people_overrides
                    WHERE person_id = %s::uuid
                    LIMIT 1
                    """,
                    [person_id],
                )
                override_row_exists = cur.fetchone() is not None
                if has_any_handle or override_row_exists:
                    cur.execute(
                        """
                        INSERT INTO core.people_overrides (
                          person_id,
                          instagram_handle,
                          tiktok_handle,
                          twitter_handle,
                          youtube_handle
                        )
                        VALUES (%s::uuid, %s::text, %s::text, %s::text, %s::text)
                        ON CONFLICT (person_id)
                        DO UPDATE SET
                          instagram_handle = EXCLUDED.instagram_handle,
                          tiktok_handle = EXCLUDED.tiktok_handle,
                          twitter_handle = EXCLUDED.twitter_handle,
                          youtube_handle = EXCLUDED.youtube_handle,
                          updated_at = now()
                        """,
                        [
                            person_id,
                            next_handles["instagram_handle"],
                            next_handles["tiktok_handle"],
                            next_handles["twitter_handle"],
                            next_handles["youtube_handle"],
                        ],
                    )

                return active_records
    except (PersonExternalIdNotFoundError, UnsupportedPersonExternalIdSourceError):
        raise
    except Exception as error:
        raise _coerce_repository_error(error) from error
