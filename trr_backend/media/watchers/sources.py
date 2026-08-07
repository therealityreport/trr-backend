"""Complete, side-effect-free source discovery for show-season media watches.

This module deliberately stops at discovery and normalization.  It does not
persist watermarks, download bytes, or decide a target season: those are the
watcher service's responsibilities.  The result contract carries enough stable
identity and provenance for that later layer to do those jobs safely.
"""

from __future__ import annotations

import base64
import ipaddress
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast
from urllib.parse import urlparse, urlunparse

from trr_backend.integrations import bravo_jsonapi, nbcumv

WATCHER_SOURCE_CONTRACT_VERSION = 1
DEFAULT_BRAVO_PAGE_SIZE = 50
DEFAULT_BRAVO_PAGE_CAP = 200
DEFAULT_WATERMARK_OVERLAP = timedelta(minutes=5)

_BRAVO_ALLOWED_HOSTS = frozenset({"bravotv.com", "www.bravotv.com"})
_NBCUMV_ALLOWED_SUFFIXES = (
    ".nbcumv.com",
    ".nbcuni.com",
    ".nbcuniversal.com",
    ".nbc.com",
    ".s3.amazonaws.com",
    ".s3.us-west-2.amazonaws.com",
)
_NBCUMV_ALLOWED_HOSTS = frozenset({"nbcumv.com", "nbcuni.com", "nbcuniversal.com", "nbc.com", "s3.amazonaws.com"})
_MPX_ALLOWED_SUFFIXES = (
    ".theplatform.com",
    ".mpx.com",
    ".bravotv.com",
    ".nbcuni.com",
    ".nbcuniversal.com",
)
_MPX_ALLOWED_HOSTS = frozenset({"theplatform.com", "mpx.com"})
_STYLE_PATH_RE = re.compile(r"^(?P<prefix>/sites/[^/]+/files)/styles/[^/]+/public/(?P<original>.+)$", re.IGNORECASE)
_URL_FIELD_HINTS = frozenset({"url", "uri", "href", "src", "stream", "download", "rendition", "file", "video", "audio"})
_SEASON_FIELD_NAMES = (
    "season",
    "season_number",
    "seasonNumber",
    "lbx_season",
    "lbx_seasonNumber",
    "field_season",
    "field_tv_season",
    "field_show_season",
)


class WatcherSourceError(RuntimeError):
    """Base error for a source inventory that cannot safely be advanced."""


class UnsafeSourceURLError(WatcherSourceError):
    """A source URL was outside the small explicit acquisition allowlist."""


class MalformedWatcherSourcePageError(WatcherSourceError):
    """A source page did not satisfy pagination or timestamp guarantees."""


class InvalidSourceContinuationError(WatcherSourceError):
    """A persisted continuation token did not belong to this collector."""


@dataclass(frozen=True)
class SourceWatermarks:
    """The two independent Bravo orderings persisted by the watcher service."""

    created_at: datetime | None = None
    created_source_id: str | None = None
    changed_at: datetime | None = None
    changed_source_id: str | None = None


@dataclass(frozen=True)
class SourceDiscoveryResult:
    """A normalized source slice plus whether it reached a safe terminal page."""

    candidates: tuple[dict[str, Any], ...]
    complete: bool
    continuation: str | None
    pages_fetched: int
    terminal_streams: tuple[str, ...]
    provenance: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates": [dict(candidate) for candidate in self.candidates],
            "complete": self.complete,
            "continuation": self.continuation,
            "pages_fetched": self.pages_fetched,
            "terminal_streams": list(self.terminal_streams),
            "provenance": dict(self.provenance),
        }


def _clean_string(value: Any) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


def _as_datetime(value: Any, *, field: str) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MalformedWatcherSourcePageError(f"{field} was not an ISO-8601 timestamp") from exc
    else:
        raise MalformedWatcherSourcePageError(f"{field} was not an ISO-8601 timestamp")
    if parsed.tzinfo is None:
        raise MalformedWatcherSourcePageError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _watermarks_from(value: SourceWatermarks | Mapping[str, Any] | None) -> SourceWatermarks:
    if value is None:
        return SourceWatermarks()
    if isinstance(value, SourceWatermarks):
        return value
    if not isinstance(value, Mapping):
        raise ValueError("source watermarks must be a mapping or SourceWatermarks")
    return SourceWatermarks(
        created_at=_as_datetime(value.get("created_at"), field="created watermark"),
        created_source_id=_clean_string(value.get("created_source_id")),
        changed_at=_as_datetime(value.get("changed_at"), field="changed watermark"),
        changed_source_id=_clean_string(value.get("changed_source_id")),
    )


def _host_matches(hostname: str, *, exact: frozenset[str], suffixes: Sequence[str]) -> bool:
    return hostname in exact or any(hostname.endswith(suffix) for suffix in suffixes)


def _allowed_host_for_source(hostname: str, source: str) -> bool:
    normalized_source = str(source or "").strip().casefold()
    if normalized_source == "bravo":
        return hostname in _BRAVO_ALLOWED_HOSTS
    if normalized_source == "nbcumv":
        return _host_matches(hostname, exact=_NBCUMV_ALLOWED_HOSTS, suffixes=_NBCUMV_ALLOWED_SUFFIXES)
    if normalized_source in {"mpx", "mpx-cdn", "bravo-mpx"}:
        return _host_matches(hostname, exact=_MPX_ALLOWED_HOSTS, suffixes=_MPX_ALLOWED_SUFFIXES)
    raise ValueError(f"unsupported watcher source URL policy: {source!r}")


def _normalize_source_url(
    value: str | None,
    *,
    source: Literal["nbcumv", "bravo", "mpx", "mpx-cdn", "bravo-mpx"],
    drop_query: bool,
) -> str | None:
    """Validate and canonicalize a source URL before it becomes acquisition state.

    This is intentionally an origin policy, not a redirect policy: the downloader
    must validate every redirect hop again.  Direct IP literals, credentials,
    non-HTTPS URLs, non-standard ports, fragments, and cache-token query strings
    are never allowed to define canonical source identity.
    """
    raw = _clean_string(value)
    if raw is None:
        return None
    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif raw.startswith("/"):
        raw = f"https://www.bravotv.com{raw}"
    parsed = urlparse(raw)
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if parsed.scheme != "https" or not hostname or parsed.username or parsed.password:
        raise UnsafeSourceURLError("source URL must be an HTTPS URL without credentials")
    try:
        if parsed.port not in (None, 443):
            raise UnsafeSourceURLError("source URL must not use a non-standard port")
    except ValueError as exc:
        raise UnsafeSourceURLError("source URL contained an invalid port") from exc
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        pass
    else:
        raise UnsafeSourceURLError("source URL must not use a literal IP address")
    if not _allowed_host_for_source(hostname, source):
        raise UnsafeSourceURLError(f"source URL host is not allowlisted for {source}: {hostname}")
    if not parsed.path or "\\" in parsed.path:
        raise UnsafeSourceURLError("source URL must have a normal absolute path")
    return urlunparse(("https", hostname, parsed.path, "", parsed.query if not drop_query else "", ""))


def normalize_source_url(
    value: str | None,
    *,
    source: Literal["nbcumv", "bravo", "mpx", "mpx-cdn", "bravo-mpx"],
) -> str | None:
    """Return a canonical source identity URL with query tokens removed."""
    return _normalize_source_url(value, source=source, drop_query=True)


def validate_transient_download_url(
    value: str | None,
    *,
    source: Literal["nbcumv", "bravo", "mpx", "mpx-cdn", "bravo-mpx"],
) -> str | None:
    """Validate a one-run acquisition URL while retaining required signed query text."""
    return _normalize_source_url(value, source=source, drop_query=False)


def normalize_bravo_original_url(value: str | None) -> str | None:
    """Resolve a Drupal image-style derivative to its original Bravo file path."""
    raw = _clean_string(value)
    if raw is None:
        return None
    if raw.startswith("//"):
        raw = f"https:{raw}"
    elif raw.startswith("/"):
        raw = f"https://www.bravotv.com{raw}"
    parsed = urlparse(raw)
    match = _STYLE_PATH_RE.match(parsed.path)
    if match:
        raw = urlunparse(
            (
                parsed.scheme or "https",
                parsed.netloc,
                f"{match.group('prefix')}/{match.group('original')}",
                "",
                "",
                "",
            )
        )
    return normalize_source_url(raw, source="bravo")


def _filename_from_url(value: str | None) -> str | None:
    normalized = _clean_string(value)
    if not normalized:
        return None
    filename = urlparse(normalized).path.rsplit("/", 1)[-1].strip()
    return filename or None


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, Iterable) or isinstance(value, (bytes, bytearray, Mapping)):
        return []
    return [cleaned for item in value if (cleaned := _clean_string(item))]


def _json_value(value: Any) -> Any:
    """Keep raw source provenance JSON-serializable without changing its meaning."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(inner) for key, inner in value.items()}
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
        return [_json_value(inner) for inner in value]
    return str(value)


def _compact(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value not in (None, "", [], {}, ())}


def _source_identity(record: Mapping[str, Any]) -> str:
    source_id = _clean_string(record.get("id")) or _clean_string(record.get("lbx_id"))
    if source_id is None:
        raise MalformedWatcherSourcePageError("source record was missing a stable source identity")
    return source_id


def _raw_season_fields(record: Mapping[str, Any]) -> dict[str, Any]:
    attributes = (
        cast("Mapping[str, Any]", record.get("attributes")) if isinstance(record.get("attributes"), Mapping) else {}
    )
    fields: dict[str, Any] = {}
    for name in _SEASON_FIELD_NAMES:
        value = record.get(name, attributes.get(name))
        if value not in (None, "", [], {}):
            fields[name] = _json_value(value)
    return fields


def _nested_text(value: Any) -> str | None:
    if isinstance(value, Mapping):
        for key in ("value", "processed", "title", "name"):
            if (text := _clean_string(value.get(key))) is not None:
                return re.sub(r"<[^>]+>", " ", text).strip() or None
    return _clean_string(value)


def _nbcumv_candidate(image: Mapping[str, Any], *, show_id: str) -> dict[str, Any]:
    source_id = _source_identity(image)
    lbx_id = _clean_string(image.get("lbx_id")) or source_id
    filename = _clean_string(image.get("lbx_filename"))
    if filename is None:
        raise MalformedWatcherSourcePageError(f"NBCUMV image {source_id} was missing lbx_filename")
    location = _clean_string(image.get("location"))
    thumbnail_url = normalize_source_url(location, source="nbcumv") if location else None
    width = image.get("lbx_width", image.get("lbx_resolutionX"))
    height = image.get("lbx_height", image.get("lbx_resolutionY"))
    return _compact(
        {
            "contract_version": WATCHER_SOURCE_CONTRACT_VERSION,
            "source": "nbcumv",
            "source_asset_id": source_id,
            "resource_type": "image",
            "media_type": "image",
            "created_at": _clean_string(image.get("created")),
            "changed_at": _clean_string(image.get("modified")),
            # NBCUMV's durable original is acquired by id+filename through its
            # batch endpoint; a display/location URL is never treated as it.
            "original_url": None,
            "download_url": None,
            "thumbnail_url": thumbnail_url,
            "filename": filename,
            "mime_type": None,
            "width": width if isinstance(width, int) else None,
            "height": height if isinstance(height, int) else None,
            "source_bytes": image.get("lbx_fileSize"),
            "caption": _nested_text(image.get("lbx_caption")),
            "headline": _nested_text(image.get("lbx_headline")),
            "people": [],
            "raw_season_fields": _raw_season_fields(image),
            "acquisition": {"method": "nbcumv_hires_zip", "lbx_id": lbx_id, "filename": filename},
            "provenance": {
                "adapter": "nbcumv.lookImages",
                "show_id": show_id,
                "source_id": source_id,
                "resource_type": "image",
            },
            "raw_record": _json_value(image),
        }
    )


def discover_nbcumv_show_candidates(
    show_id: str,
    *,
    session: Any | None = None,
    page_size: int = nbcumv.DEFAULT_PAGE_SIZE,
) -> SourceDiscoveryResult:
    """Normalize every authoritative NBCUMV ``lookImages`` row for one show."""
    normalized_show_id = nbcumv._normalized_nbcumv_show_id(show_id)
    candidates: list[dict[str, Any]] = []
    pages_fetched = 0
    for page in nbcumv.iter_show_look_image_pages(normalized_show_id, session=session, page_size=page_size):
        pages_fetched += 1
        candidates.extend(_nbcumv_candidate(image, show_id=normalized_show_id) for image in page)
    return SourceDiscoveryResult(
        candidates=tuple(candidates),
        complete=True,
        continuation=None,
        pages_fetched=pages_fetched,
        terminal_streams=("lookImages",),
        provenance={
            "adapter": "nbcumv.lookImages",
            "show_id": normalized_show_id,
            "contract_version": WATCHER_SOURCE_CONTRACT_VERSION,
        },
    )


collect_nbcumv_show_candidates = discover_nbcumv_show_candidates


def _relationship_refs(record: Mapping[str, Any], *, field_predicate: Any) -> list[dict[str, str]]:
    relationships = (
        cast("Mapping[str, Any]", record.get("relationships"))
        if isinstance(record.get("relationships"), Mapping)
        else {}
    )
    refs: list[dict[str, str]] = []
    for field_name, relationship in relationships.items():
        if not field_predicate(str(field_name)) or not isinstance(relationship, Mapping):
            continue
        raw_data = relationship.get("data")
        entries = raw_data if isinstance(raw_data, list) else [raw_data]
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            ref_type = _clean_string(entry.get("type"))
            ref_id = _clean_string(entry.get("id"))
            if ref_type and ref_id:
                refs.append({"field": str(field_name), "type": ref_type, "id": ref_id})
    return refs


def _cast_person_ids(show_record: Mapping[str, Any]) -> list[str]:
    refs = _relationship_refs(show_record, field_predicate=lambda field: "cast" in field.casefold())
    ids: list[str] = []
    for ref in refs:
        if ref["type"] != "node--person" or ref["id"] in ids:
            continue
        ids.append(ref["id"])
    return ids


def _continuation_encode(state: Mapping[str, Any]) -> str:
    raw = json.dumps(_json_value(state), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _continuation_decode(value: str | None, *, show_uuid: str) -> dict[str, Any] | None:
    token = _clean_string(value)
    if token is None:
        return None
    try:
        padded = token + "=" * (-len(token) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise InvalidSourceContinuationError("Bravo continuation token was not valid encoded JSON") from exc
    if not isinstance(decoded, dict):
        raise InvalidSourceContinuationError("Bravo continuation token was not an object")
    if decoded.get("version") != 1 or decoded.get("source") != "bravo" or decoded.get("show_uuid") != show_uuid:
        raise InvalidSourceContinuationError("Bravo continuation token did not belong to this watch source")
    phase = decoded.get("phase")
    if phase not in {"collection", "cast"}:
        raise InvalidSourceContinuationError("Bravo continuation token had an unknown phase")
    return decoded


def _stream_definitions() -> tuple[tuple[str, Literal["created", "changed"]], ...]:
    resources = ("media/image", "media/video", "media/audio", "file/file", "node/blog")
    return tuple((resource, ordering) for resource in resources for ordering in ("created", "changed"))


def _timestamp_for_ordering(record: Mapping[str, Any], ordering: Literal["created", "changed"]) -> datetime:
    attributes = (
        cast("Mapping[str, Any]", record.get("attributes")) if isinstance(record.get("attributes"), Mapping) else {}
    )
    names = ("created", "created_at") if ordering == "created" else ("changed", "changed_at")
    for name in names:
        value = attributes.get(name, record.get(name))
        if (timestamp := _as_datetime(value, field=f"Bravo {ordering} timestamp")) is not None:
            return timestamp
    raise MalformedWatcherSourcePageError(f"Bravo record was missing {ordering} timestamp")


def _validate_descending_page(records: Sequence[Mapping[str, Any]], *, ordering: Literal["created", "changed"]) -> None:
    previous: tuple[datetime, str] | None = None
    for record in records:
        current = (_timestamp_for_ordering(record, ordering), _source_identity(record))
        if previous is not None and current > previous:
            raise MalformedWatcherSourcePageError(f"Bravo {ordering}-ordered page was not newest-first")
        previous = current


def _record_older_than(
    record: Mapping[str, Any],
    *,
    ordering: Literal["created", "changed"],
    watermark_at: datetime | None,
    watermark_source_id: str | None,
    overlap: timedelta,
) -> bool:
    if watermark_at is None:
        return False
    cutoff = watermark_at - overlap
    value = _timestamp_for_ordering(record, ordering)
    source_id = _source_identity(record)
    if value < cutoff:
        return True
    if value > cutoff:
        return False
    return watermark_source_id is not None and source_id < watermark_source_id


def _page_is_older_than_both_watermarks(
    records: Sequence[Mapping[str, Any]], *, watermarks: SourceWatermarks, overlap: timedelta
) -> bool:
    if not records or watermarks.created_at is None or watermarks.changed_at is None:
        return False
    return all(
        _record_older_than(
            record,
            ordering="created",
            watermark_at=watermarks.created_at,
            watermark_source_id=watermarks.created_source_id,
            overlap=overlap,
        )
        and _record_older_than(
            record,
            ordering="changed",
            watermark_at=watermarks.changed_at,
            watermark_source_id=watermarks.changed_source_id,
            overlap=overlap,
        )
        for record in records
    )


def _detail_source_url(resource: str, source_id: str) -> str:
    return f"{bravo_jsonapi.JSONAPI_BASE_URL}/{resource.strip('/')}/{source_id}"


def _record_key(record: Mapping[str, Any]) -> tuple[str, str]:
    record_type = _clean_string(record.get("type"))
    if record_type is None:
        raise MalformedWatcherSourcePageError("Bravo record was missing type")
    return record_type, _source_identity(record)


def _record_index(entries: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], Mapping[str, Any]]:
    index: dict[tuple[str, str], Mapping[str, Any]] = {}
    for entry in entries:
        index.setdefault(_record_key(entry), entry)
    return index


def _file_url(record: Mapping[str, Any]) -> str | None:
    attributes = (
        cast("Mapping[str, Any]", record.get("attributes")) if isinstance(record.get("attributes"), Mapping) else {}
    )
    uri = attributes.get("uri")
    candidates = (
        uri.get("url") if isinstance(uri, Mapping) else None,
        attributes.get("url"),
        (
            attributes.get("image_style_uri", {}).get("url")
            if isinstance(attributes.get("image_style_uri"), Mapping)
            else None
        ),
        attributes.get("image_style_uri"),
    )
    for candidate in candidates:
        if (cleaned := _clean_string(candidate)) is not None:
            return normalize_bravo_original_url(cleaned)
    return None


def _record_people(record: Mapping[str, Any], *, index: Mapping[tuple[str, str], Mapping[str, Any]]) -> list[str]:
    attributes = (
        cast("Mapping[str, Any]", record.get("attributes")) if isinstance(record.get("attributes"), Mapping) else {}
    )
    direct_title = _clean_string(attributes.get("title")) if record.get("type") == "node--person" else None
    explicit_people = _string_list(record.get("_watcher_people"))
    people = explicit_people + ([direct_title] if direct_title and direct_title not in explicit_people else [])
    refs = _relationship_refs(
        record,
        field_predicate=lambda field: "person" in field.casefold() or "cast" in field.casefold(),
    )
    for ref in refs:
        if ref["type"] != "node--person":
            continue
        person = index.get((ref["type"], ref["id"]))
        person_attributes = (
            cast("Mapping[str, Any]", person.get("attributes"))
            if isinstance(person, Mapping) and isinstance(person.get("attributes"), Mapping)
            else {}
        )
        if (name := _clean_string(person_attributes.get("title"))) and name not in people:
            people.append(name)
    return people


def _walk_renditions(
    value: Any,
    *,
    field_name: str = "",
    path: tuple[str, ...] = (),
) -> Iterable[tuple[str, Mapping[str, Any], tuple[str, ...]]]:
    if isinstance(value, Mapping):
        for key, inner in value.items():
            clean_key = str(key)
            lowered = clean_key.casefold()
            if isinstance(inner, str) and any(hint in lowered for hint in _URL_FIELD_HINTS):
                yield inner, value, path + (clean_key,)
            yield from _walk_renditions(inner, field_name=clean_key, path=path + (clean_key,))
    elif isinstance(value, list):
        for index, inner in enumerate(value):
            yield from _walk_renditions(inner, field_name=field_name, path=path + (str(index),))


def select_highest_quality_mpx_rendition(record: Mapping[str, Any]) -> str | None:
    """Select a playable/downloadable MPX rendition without making it canonical."""
    attributes = (
        cast("Mapping[str, Any]", record.get("attributes")) if isinstance(record.get("attributes"), Mapping) else {}
    )
    candidates: list[tuple[tuple[int, int, int, int, str], str]] = []
    for raw_url, metadata, path in _walk_renditions(attributes):
        try:
            rendition = validate_transient_download_url(raw_url, source="mpx")
        except UnsafeSourceURLError:
            continue
        if rendition is None:
            continue
        width = cast(int, metadata.get("width")) if isinstance(metadata.get("width"), int) else 0
        height = cast(int, metadata.get("height")) if isinstance(metadata.get("height"), int) else 0
        bitrate = cast(int, metadata.get("bitrate")) if isinstance(metadata.get("bitrate"), int) else 0
        filesize = cast(int, metadata.get("filesize")) if isinstance(metadata.get("filesize"), int) else 0
        # Prefer actual dimensions first, then bitrate/size.  The final URL tie
        # breaker makes the choice deterministic even when the feed is reordered.
        candidates.append(((width * height, bitrate, filesize, -len(path), rendition), rendition))
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: candidate[0])[1]


def _bravo_candidate(
    record: Mapping[str, Any],
    *,
    resource: str,
    request_url: str,
    index: Mapping[tuple[str, str], Mapping[str, Any]],
    relationship_path: str | None = None,
) -> dict[str, Any] | None:
    record_type, source_id = _record_key(record)
    attributes = (
        cast("Mapping[str, Any]", record.get("attributes")) if isinstance(record.get("attributes"), Mapping) else {}
    )
    normalized_resource = resource.strip("/")
    media_type: str
    original_url: str | None = None
    download_url: str | None = None
    filename: str | None = None
    mime_type: str | None = None
    width: int | None = None
    height: int | None = None
    source_bytes: int | None = None
    acquisition: dict[str, Any] | None = None

    if record_type == "file--file":
        media_type = "file"
        original_url = _file_url(record)
        filename = _clean_string(attributes.get("filename")) or _filename_from_url(original_url)
        mime_type = _clean_string(attributes.get("filemime"))
        source_bytes = attributes.get("filesize") if isinstance(attributes.get("filesize"), int) else None
        download_url = original_url
    elif record_type == "media--image":
        media_type = "image"
        file_refs = _relationship_refs(record, field_predicate=lambda field: field == "field_media_image")
        file_record = next(
            (index.get((ref["type"], ref["id"])) for ref in file_refs if ref["type"] == "file--file"), None
        )
        if isinstance(file_record, Mapping):
            file_attributes = (
                cast("Mapping[str, Any]", file_record.get("attributes"))
                if isinstance(file_record.get("attributes"), Mapping)
                else {}
            )
            original_url = _file_url(file_record)
            filename = _clean_string(file_attributes.get("filename")) or _filename_from_url(original_url)
            mime_type = _clean_string(file_attributes.get("filemime"))
            source_bytes = file_attributes.get("filesize") if isinstance(file_attributes.get("filesize"), int) else None
            download_url = original_url
        else:
            filename = _clean_string(attributes.get("name"))
        width = attributes.get("width") if isinstance(attributes.get("width"), int) else None
        height = attributes.get("height") if isinstance(attributes.get("height"), int) else None
    elif record_type in {"media--video", "media--audio"}:
        media_type = "video" if record_type == "media--video" else "audio"
        # The detail endpoint is a stable source identity; a signed MPX URL is
        # deliberately isolated as download_url and never becomes original_url.
        original_url = normalize_source_url(_detail_source_url(normalized_resource, source_id), source="bravo")
        download_url = select_highest_quality_mpx_rendition(record)
        filename = _clean_string(attributes.get("name"))
        mime_type = _clean_string(attributes.get("field_media_type")) or _clean_string(attributes.get("mime_type"))
        width = attributes.get("width") if isinstance(attributes.get("width"), int) else None
        height = attributes.get("height") if isinstance(attributes.get("height"), int) else None
        source_bytes = attributes.get("filesize") if isinstance(attributes.get("filesize"), int) else None
        acquisition = {"method": "mpx_rendition", "canonical_record_url": original_url}
    elif record_type == "node--blog":
        media_type = "metadata"
        path = attributes.get("path")
        alias = path.get("alias") if isinstance(path, Mapping) else None
        original_url = normalize_source_url(alias, source="bravo") if _clean_string(alias) else None
        filename = _clean_string(attributes.get("title"))
    else:
        return None

    created_at = _clean_string(attributes.get("created")) or _clean_string(record.get("created"))
    changed_at = _clean_string(attributes.get("changed")) or _clean_string(record.get("changed"))
    return _compact(
        {
            "contract_version": WATCHER_SOURCE_CONTRACT_VERSION,
            "source": "bravo",
            "source_asset_id": source_id,
            "resource_type": normalized_resource,
            "media_type": media_type,
            "created_at": created_at,
            "changed_at": changed_at,
            "original_url": original_url,
            "download_url": download_url,
            "filename": filename,
            "mime_type": mime_type,
            "width": width,
            "height": height,
            "source_bytes": source_bytes,
            "caption": _nested_text(attributes.get("field_caption"))
            or _nested_text(attributes.get("field_description")),
            "headline": _nested_text(attributes.get("title")),
            "people": _record_people(record, index=index),
            "raw_season_fields": _raw_season_fields(record),
            "acquisition": acquisition,
            "provenance": {
                "adapter": "bravo.jsonapi",
                "resource": normalized_resource,
                "record_type": record_type,
                "source_id": source_id,
                "request_url": request_url,
                "relationship_path": relationship_path,
            },
            "raw_record": _json_value({key: value for key, value in record.items() if key != "_watcher_people"}),
        }
    )


def _append_page_entries(
    entries: list[tuple[dict[str, Any], str, str, str | None]],
    *,
    records: Sequence[Mapping[str, Any]],
    included: Sequence[Mapping[str, Any]],
    resource: str,
    request_url: str,
) -> None:
    entries.extend((dict(record), resource, request_url, None) for record in records)
    entries.extend(
        (
            dict(record),
            str(record.get("type") or "").replace("--", "/"),
            request_url,
            "included",
        )
        for record in included
    )


def _append_person_profile_entries(
    entries: list[tuple[dict[str, Any], str, str, str | None]],
    *,
    person_record: Mapping[str, Any],
    included: Sequence[Mapping[str, Any]],
    request_url: str,
) -> None:
    """Add a cast member's cover/full photo entries with relationship provenance."""
    attributes = (
        cast("Mapping[str, Any]", person_record.get("attributes"))
        if isinstance(person_record.get("attributes"), Mapping)
        else {}
    )
    person_name = _clean_string(attributes.get("title"))
    fields_by_media_key: dict[tuple[str, str], list[str]] = {}
    for field_name in ("field_person_cover_photo", "field_person_full_photo"):
        for ref in _relationship_refs(
            person_record,
            field_predicate=lambda field, expected=field_name: field == expected,
        ):
            fields_by_media_key.setdefault((ref["type"], ref["id"]), []).append(field_name)

    entries.append((dict(person_record), "node/person", request_url, None))
    for entry in included:
        key = _record_key(entry)
        field_names = fields_by_media_key.get(key)
        if field_names:
            scoped_entry = dict(entry)
            if person_name:
                scoped_entry["_watcher_people"] = [person_name]
            entries.append((scoped_entry, "media/image", request_url, ",".join(field_names)))
    entries.extend(
        (
            dict(entry),
            str(entry.get("type") or "").replace("--", "/"),
            request_url,
            "included",
        )
        for entry in included
    )


def _normalize_entries(entries: Sequence[tuple[dict[str, Any], str, str, str | None]]) -> tuple[dict[str, Any], ...]:
    index = _record_index([entry[0] for entry in entries])
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record, resource, request_url, relationship_path in entries:
        candidate = _bravo_candidate(
            record,
            resource=resource,
            request_url=request_url,
            index=index,
            relationship_path=relationship_path,
        )
        if candidate is None:
            continue
        identity = str(candidate["source_asset_id"])
        if identity in seen:
            continue
        seen.add(identity)
        candidates.append(candidate)
    return tuple(candidates)


def _state(
    *,
    show_uuid: str,
    phase: Literal["collection", "cast"],
    stream_index: int = 0,
    next_url: str | None = None,
    show_nid: str | int | None = None,
    cast_person_ids: Sequence[str] = (),
    person_index: int = 0,
    seen_page_urls: Sequence[str] = (),
) -> dict[str, Any]:
    return _compact(
        {
            "version": 1,
            "source": "bravo",
            "show_uuid": show_uuid,
            "phase": phase,
            "stream_index": stream_index,
            "next_url": next_url,
            "show_nid": show_nid,
            "cast_person_ids": list(cast_person_ids),
            "person_index": person_index,
            "seen_page_urls": list(seen_page_urls),
        }
    )


def _show_context(
    show_uuid: str,
    *,
    client: Any | None,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], str | int | None, list[str]]:
    show_record, included = bravo_jsonapi.fetch_jsonapi_resource_detail(
        "node/tv_show",
        show_uuid,
        client=client,
        params={"include": "field_cast"},
    )
    attributes = (
        cast("Mapping[str, Any]", show_record.get("attributes"))
        if isinstance(show_record.get("attributes"), Mapping)
        else {}
    )
    show_nid = attributes.get("drupal_internal__nid")
    if show_nid in (None, ""):
        raise MalformedWatcherSourcePageError("Bravo show detail was missing drupal_internal__nid for source scoping")
    return show_record, included, show_nid, _cast_person_ids(show_record)


def discover_bravo_incremental_candidates(
    show_uuid: str,
    *,
    client: Any | None = None,
    watermarks: SourceWatermarks | Mapping[str, Any] | None = None,
    overlap: timedelta = DEFAULT_WATERMARK_OVERLAP,
    continuation: str | None = None,
    page_size: int = DEFAULT_BRAVO_PAGE_SIZE,
    page_cap: int = DEFAULT_BRAVO_PAGE_CAP,
) -> SourceDiscoveryResult:
    """Collect a bounded, resumable Bravo JSON:API inventory slice.

    Each resource is traversed in both created and changed order.  A page is a
    valid stop only after every row is older than *both* overlapped watermarks;
    otherwise all ``links.next`` pages are followed.  Reaching the cap returns a
    portable opaque continuation and never claims a source watermark is safe to
    commit.
    """
    clean_show_uuid = _clean_string(show_uuid)
    if clean_show_uuid is None:
        raise ValueError("Bravo show UUID is required")
    if page_size < 1 or page_cap < 1:
        raise ValueError("Bravo page_size and page_cap must be positive")
    if overlap < timedelta(0):
        raise ValueError("Bravo watermark overlap must not be negative")

    resolved_watermarks = _watermarks_from(watermarks)
    state = _continuation_decode(continuation, show_uuid=clean_show_uuid)
    pages_fetched = 0
    entries: list[tuple[dict[str, Any], str, str, str | None]] = []
    terminal_streams: list[str] = []

    if state is None:
        if pages_fetched >= page_cap:
            return SourceDiscoveryResult(
                (),
                False,
                _continuation_encode(_state(show_uuid=clean_show_uuid, phase="collection")),
                0,
                (),
                {},
            )
        show_record, show_included, show_nid, cast_person_ids = _show_context(clean_show_uuid, client=client)
        pages_fetched += 1
        _append_page_entries(
            entries,
            records=(show_record,),
            included=show_included,
            resource="node/tv_show",
            request_url=_detail_source_url("node/tv_show", clean_show_uuid),
        )
        state = _state(
            show_uuid=clean_show_uuid,
            phase="collection",
            show_nid=show_nid,
            cast_person_ids=cast_person_ids,
        )

    phase = state["phase"]
    stream_index = int(state.get("stream_index") or 0)
    next_url = _clean_string(state.get("next_url"))
    show_nid = state.get("show_nid")
    cast_person_ids = _string_list(state.get("cast_person_ids"))
    seen_page_urls = set(_string_list(state.get("seen_page_urls")))

    if phase == "collection":
        streams = _stream_definitions()
        if stream_index < 0 or stream_index > len(streams):
            raise InvalidSourceContinuationError("Bravo continuation stream index was outside the collector plan")
        while stream_index < len(streams):
            if pages_fetched >= page_cap:
                next_state = _state(
                    show_uuid=clean_show_uuid,
                    phase="collection",
                    stream_index=stream_index,
                    next_url=next_url,
                    show_nid=show_nid,
                    cast_person_ids=cast_person_ids,
                    seen_page_urls=sorted(seen_page_urls),
                )
                return SourceDiscoveryResult(
                    _normalize_entries(entries),
                    False,
                    _continuation_encode(next_state),
                    pages_fetched,
                    tuple(terminal_streams),
                    {"adapter": "bravo.jsonapi", "show_uuid": clean_show_uuid, "incomplete_reason": "page_cap"},
                )
            resource, ordering = streams[stream_index]
            params: dict[str, Any] | None = None
            if next_url is None:
                params = {"sort": f"-{ordering}", "page[limit]": str(page_size)}
                if show_nid is not None:
                    params["filter[field_tv_shows.show]"] = str(show_nid)
            elif next_url in seen_page_urls:
                raise MalformedWatcherSourcePageError("Bravo JSON:API pagination repeated a page URL")
            page = bravo_jsonapi.fetch_jsonapi_collection_page(
                resource,
                client=client,
                params=params,
                page_url=next_url,
            )
            pages_fetched += 1
            if page.request_url in seen_page_urls:
                raise MalformedWatcherSourcePageError("Bravo JSON:API pagination repeated a page URL")
            seen_page_urls.add(page.request_url)
            _validate_descending_page(page.records, ordering=ordering)
            _append_page_entries(
                entries,
                records=page.records,
                included=page.included,
                resource=resource,
                request_url=page.request_url,
            )
            stream_name = f"{resource}:{ordering}"
            if _page_is_older_than_both_watermarks(page.records, watermarks=resolved_watermarks, overlap=overlap):
                terminal_streams.append(stream_name)
                stream_index += 1
                next_url = None
                seen_page_urls.clear()
                continue
            if page.next_url is None:
                terminal_streams.append(stream_name)
                stream_index += 1
                next_url = None
                seen_page_urls.clear()
                continue
            next_url = page.next_url

        phase = "cast"
        state = _state(
            show_uuid=clean_show_uuid,
            phase="cast",
            show_nid=show_nid,
            cast_person_ids=cast_person_ids,
        )

    person_index = int(state.get("person_index") or 0)
    if person_index < 0 or person_index > len(cast_person_ids):
        raise InvalidSourceContinuationError("Bravo continuation person index was outside the cast list")
    while person_index < len(cast_person_ids):
        if pages_fetched >= page_cap:
            next_state = _state(
                show_uuid=clean_show_uuid,
                phase="cast",
                show_nid=show_nid,
                cast_person_ids=cast_person_ids,
                person_index=person_index,
            )
            return SourceDiscoveryResult(
                _normalize_entries(entries),
                False,
                _continuation_encode(next_state),
                pages_fetched,
                tuple(terminal_streams),
                {"adapter": "bravo.jsonapi", "show_uuid": clean_show_uuid, "incomplete_reason": "page_cap"},
            )
        person_id = cast_person_ids[person_index]
        person_record, person_included = bravo_jsonapi.fetch_jsonapi_resource_detail(
            "node/person",
            person_id,
            client=client,
            params={
                "include": ",".join(
                    (
                        "field_person_cover_photo",
                        "field_person_cover_photo.field_media_image",
                        "field_person_full_photo",
                        "field_person_full_photo.field_media_image",
                    )
                )
            },
        )
        pages_fetched += 1
        _append_person_profile_entries(
            entries,
            person_record=person_record,
            included=person_included,
            request_url=_detail_source_url("node/person", person_id),
        )
        terminal_streams.append(f"cast:{person_id}")
        person_index += 1

    return SourceDiscoveryResult(
        _normalize_entries(entries),
        True,
        None,
        pages_fetched,
        tuple(terminal_streams),
        {
            "adapter": "bravo.jsonapi",
            "show_uuid": clean_show_uuid,
            "contract_version": WATCHER_SOURCE_CONTRACT_VERSION,
            "watermark_overlap_seconds": int(overlap.total_seconds()),
        },
    )


collect_bravo_incremental_candidates = discover_bravo_incremental_candidates


def normalize_watcher_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the shared candidate contract before the service journals it."""
    source = _clean_string(candidate.get("source"))
    source_asset_id = _clean_string(candidate.get("source_asset_id"))
    resource_type = _clean_string(candidate.get("resource_type"))
    if source not in {"nbcumv", "bravo"} or source_asset_id is None or resource_type is None:
        raise MalformedWatcherSourcePageError("watcher candidate lacked source, source_asset_id, or resource_type")
    normalized = dict(candidate)
    normalized["contract_version"] = WATCHER_SOURCE_CONTRACT_VERSION
    normalized["source"] = source
    normalized["source_asset_id"] = source_asset_id
    normalized["resource_type"] = resource_type
    original_url = _clean_string(candidate.get("original_url"))
    download_url = _clean_string(candidate.get("download_url"))
    if source == "bravo":
        normalized["original_url"] = normalize_bravo_original_url(original_url) if original_url else None
        if download_url:
            # Media image downloads use Bravo's original URL; MPX renditions use
            # their dedicated policy and never replace original_url as identity.
            normalized["download_url"] = (
                validate_transient_download_url(download_url, source="mpx")
                if normalized.get("media_type") in {"video", "audio"}
                else normalize_bravo_original_url(download_url)
            )
    else:
        normalized["original_url"] = normalize_source_url(original_url, source="nbcumv") if original_url else None
        normalized["download_url"] = normalize_source_url(download_url, source="nbcumv") if download_url else None
    provenance = candidate.get("provenance")
    if not isinstance(provenance, Mapping):
        raise MalformedWatcherSourcePageError("watcher candidate lacked stable provenance")
    normalized["provenance"] = _json_value(provenance)
    normalized["raw_record"] = _json_value(candidate.get("raw_record") or {})
    return normalized
