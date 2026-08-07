"""Fenced, revision-preserving acquisition for show-season media watches.

The source adapters deliberately only discover records.  This module owns the
boundary where a normalized source record becomes durable media: it records a
baseline observation first, downloads only when policy permits, adopts a
deterministic R2 object after an interrupted database commit, and writes the
logical asset, immutable revision, and links while holding the watch fence.

No scheduler policy lives here.  Callers must provide a watch that has already
been claimed by ``media_watchers.claim_due_watch`` and pass that exact owner
and fencing token.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import zipfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast
from urllib.parse import urljoin

import requests

from trr_backend.db import pg
from trr_backend.integrations import nbcumv
from trr_backend.media import s3_mirror
from trr_backend.media.watchers import sources
from trr_backend.repositories import media_watchers

DEFAULT_REVALIDATION_TTL = timedelta(hours=6)
DEFAULT_IMAGE_MAX_BYTES = 25 * 1024 * 1024
DEFAULT_AV_MAX_BYTES = 500 * 1024 * 1024
DEFAULT_ARCHIVE_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_ARCHIVE_MAX_MEMBERS = 8
DEFAULT_ARCHIVE_MAX_EXPANSION_RATIO = 100
DEFAULT_MAX_REDIRECTS = 5

_SAFE_SEGMENT_RE = re.compile(r"[^a-z0-9._-]+")
_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
_NUMERIC_SEASON_RE = re.compile(r"(?<!\d)(\d{1,3})(?!\d)")


class WatcherServiceError(RuntimeError):
    """Base acquisition error whose text is safe to retain in a run journal."""


class WatcherFenceLostError(WatcherServiceError):
    """The worker no longer owns the watch lease and must not write state."""


class UnsafeDownloadError(WatcherServiceError):
    """A URL, redirect, response, or archive failed acquisition safety policy."""


class ReconciliationError(WatcherServiceError):
    """An existing deterministic R2 object cannot be proven to match its hash."""


class R2Client(Protocol):
    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class DownloadedMedia:
    data: bytes
    content_type: str
    width: int | None
    height: int | None
    source_url: str


@dataclass(frozen=True)
class StoredObject:
    bucket: str
    key: str
    url: str
    sha256: str
    size_bytes: int
    content_type: str
    etag: str | None
    width: int | None = None
    height: int | None = None


@dataclass(frozen=True)
class Qualification:
    status: str
    evidence: dict[str, Any]
    season_link: bool


@dataclass(frozen=True)
class WatchRunResult:
    run_id: str | None
    status: str
    summary: dict[str, Any]
    source_state_after: dict[str, Any]
    continuation: dict[str, Any]
    error: str | None = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): inner for key, inner in value.items()}
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except ValueError:
            return {}
        return _as_mapping(decoded)
    return {}


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except ValueError:
            return [value]
        return _as_list(decoded)
    return []


def _clean(value: Any) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def _parse_time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _source_updated_at(candidate: Mapping[str, Any]) -> Any:
    return candidate.get("changed_at") or candidate.get("created_at")


def source_fingerprint(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Return the stable identity used for no-download eligibility.

    Signed rendition query strings are intentionally excluded.  They are a
    transient transport detail, not a source change signal.
    """
    acquisition = _as_mapping(candidate.get("acquisition"))
    return {
        "source": _clean(candidate.get("source")),
        "source_asset_id": _clean(candidate.get("source_asset_id")),
        "resource_type": _clean(candidate.get("resource_type")),
        "media_type": _clean(candidate.get("media_type")),
        "source_updated_at": str(_source_updated_at(candidate) or ""),
        "original_url": _clean(candidate.get("original_url")),
        "filename": _clean(candidate.get("filename")),
        "mime_type": _clean(candidate.get("mime_type")),
        "source_bytes": candidate.get("source_bytes"),
        "acquisition_method": _clean(acquisition.get("method")),
    }


def _safe_segment(value: Any, *, fallback: str) -> str:
    normalized = _SAFE_SEGMENT_RE.sub("-", str(value or "").strip().casefold())
    while ".." in normalized:
        normalized = normalized.replace("..", "-")
    normalized = normalized.strip(".-")
    return normalized[:160] or fallback


def _safe_filename(value: Any, *, content_type: str | None) -> str:
    raw = str(value or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
    raw = _SAFE_FILENAME_RE.sub("-", raw).strip(".-")
    while ".." in raw:
        raw = raw.replace("..", "-")
    if not raw:
        extension = s3_mirror.guess_ext_from_content_type(content_type) or ".bin"
        raw = f"asset{extension}"
    if "." not in raw:
        raw += s3_mirror.guess_ext_from_content_type(content_type) or ".bin"
    return raw[:200]


def build_revision_r2_key(
    watch: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    sha256: str,
    content_type: str | None,
) -> str:
    """Build a deterministic object key without accepting source path text."""
    prefix = str(watch.get("r2_prefix") or "").strip().strip("/")
    if not prefix or ".." in prefix.split("/"):
        raise WatcherServiceError("watch r2_prefix was invalid")
    source = _safe_segment(candidate.get("source"), fallback="source")
    source_asset_id = _safe_segment(candidate.get("source_asset_id"), fallback="asset")
    filename = _safe_filename(candidate.get("filename"), content_type=content_type)
    digest = str(sha256 or "").strip().lower()
    if not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise WatcherServiceError("content SHA-256 was invalid")
    key = f"{prefix}/{source}/{source_asset_id}/{digest[:16]}-{filename}"
    if not key.startswith(f"{prefix}/") or ".." in key.split("/"):
        raise WatcherServiceError("derived R2 key escaped its watch prefix")
    return key


def _candidate_source_policy(candidate: Mapping[str, Any]) -> str:
    media_type = _clean(candidate.get("media_type"))
    if media_type in {"video", "audio"}:
        return "mpx"
    return str(candidate.get("source") or "")


def _max_bytes(candidate: Mapping[str, Any]) -> int:
    if _clean(candidate.get("media_type")) in {"video", "audio"}:
        return DEFAULT_AV_MAX_BYTES
    return DEFAULT_IMAGE_MAX_BYTES


def _validate_download_url(value: str, *, source_policy: str) -> str:
    try:
        canonical = sources.validate_transient_download_url(value, source=source_policy)  # type: ignore[arg-type]
    except (ValueError, sources.UnsafeSourceURLError) as exc:
        raise UnsafeDownloadError(f"unsafe_download_url:{exc}") from exc
    if canonical is None:
        raise UnsafeDownloadError("unsafe_download_url:empty")
    public_error = s3_mirror._public_media_url_error(canonical)
    if public_error:
        raise UnsafeDownloadError(public_error)
    return canonical


def _read_response(response: Any, *, max_bytes: int) -> tuple[bytes, str]:
    headers = getattr(response, "headers", {}) or {}
    if s3_mirror._response_content_length_exceeds(headers, max_bytes):
        raise UnsafeDownloadError("asset_too_large")
    data, _size, read_error = s3_mirror._read_response_bytes_with_cap(response, max_bytes=max_bytes)
    if read_error:
        raise UnsafeDownloadError(read_error)
    if not data:
        raise UnsafeDownloadError("empty_response_body")
    content_type = s3_mirror._normalize_content_type(headers.get("Content-Type"))
    return data, content_type


def _fetch_https_bytes(
    url: str,
    *,
    source_policy: str,
    max_bytes: int,
    request_get: Callable[..., Any] = requests.get,
) -> tuple[bytes, str, str]:
    """Fetch with an allowlist and public-DNS check on every redirect hop."""
    current = url
    for _redirects in range(DEFAULT_MAX_REDIRECTS + 1):
        current = _validate_download_url(current, source_policy=source_policy)
        response = request_get(
            current,
            headers={"accept": "*/*", "user-agent": "TRR show-season watcher/1.0"},
            timeout=(5, 30),
            stream=True,
            allow_redirects=False,
        )
        try:
            status_code = int(getattr(response, "status_code", 200) or 200)
            if 300 <= status_code < 400:
                location = _clean((getattr(response, "headers", {}) or {}).get("Location"))
                if location is None:
                    raise UnsafeDownloadError("redirect_without_location")
                current = urljoin(current, location)
                continue
            raise_for_status = getattr(response, "raise_for_status", None)
            if callable(raise_for_status):
                raise_for_status()
            if status_code < 200 or status_code >= 300:
                raise UnsafeDownloadError(f"http_status:{status_code}")
            data, content_type = _read_response(response, max_bytes=max_bytes)
            return data, content_type, current
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()
    raise UnsafeDownloadError("too_many_redirects")


def _decode_image(data: bytes) -> tuple[int | None, int | None]:
    """Require a real raster decode; never execute active image content."""
    try:
        from PIL import Image  # type: ignore
    except Exception as exc:  # pragma: no cover - dependency/runtime boundary
        raise UnsafeDownloadError("image_decoder_unavailable") from exc
    try:
        with Image.open(io.BytesIO(data)) as image:
            image.verify()
        with Image.open(io.BytesIO(data)) as image:
            width, height = int(image.width), int(image.height)
    except Exception as exc:
        raise UnsafeDownloadError("image_decode_failed") from exc
    if width <= 0 or height <= 0:
        raise UnsafeDownloadError("image_dimensions_invalid")
    return width, height


def _validate_media_payload(
    *,
    candidate: Mapping[str, Any],
    source_url: str,
    data: bytes,
    content_type: str,
) -> DownloadedMedia:
    payload_error = s3_mirror._media_payload_error(
        source_url=source_url,
        content_type=content_type,
        data=data,
    )
    if payload_error:
        raise UnsafeDownloadError(payload_error)
    sniffed_image = s3_mirror._sniff_image_content_type(data[:4096])
    normalized_type = s3_mirror._normalize_content_type(content_type) or sniffed_image
    if not normalized_type:
        raise UnsafeDownloadError("asset_missing_content_type")
    media_type = _clean(candidate.get("media_type"))
    if media_type in {"image", "file"} or sniffed_image:
        if not sniffed_image or sniffed_image == "image/svg+xml":
            raise UnsafeDownloadError("asset_wrong_content_type")
        width, height = _decode_image(data)
        normalized_type = sniffed_image
    else:
        # Magic/content-type validation above is the non-executing probe for
        # video/audio.  Dimensions remain source-provided when available.
        width = candidate.get("width") if isinstance(candidate.get("width"), int) else None
        height = candidate.get("height") if isinstance(candidate.get("height"), int) else None
    return DownloadedMedia(data=data, content_type=normalized_type, width=width, height=height, source_url=source_url)


def _extract_hires_zip(
    archive_bytes: bytes,
    *,
    expected_filename: str,
    candidate: Mapping[str, Any],
) -> DownloadedMedia:
    if len(archive_bytes) > DEFAULT_ARCHIVE_MAX_BYTES:
        raise UnsafeDownloadError("archive_too_large")
    try:
        archive = zipfile.ZipFile(io.BytesIO(archive_bytes))
    except zipfile.BadZipFile as exc:
        raise UnsafeDownloadError("invalid_hires_archive") from exc
    with archive:
        members = [member for member in archive.infolist() if not member.is_dir()]
        if not members or len(members) > DEFAULT_ARCHIVE_MAX_MEMBERS:
            raise UnsafeDownloadError("archive_member_limit")
        total_compressed = sum(max(0, member.compress_size) for member in members)
        total_uncompressed = sum(max(0, member.file_size) for member in members)
        if total_uncompressed > DEFAULT_IMAGE_MAX_BYTES:
            raise UnsafeDownloadError("archive_expansion_limit")
        if total_compressed and total_uncompressed > total_compressed * DEFAULT_ARCHIVE_MAX_EXPANSION_RATIO:
            raise UnsafeDownloadError("archive_expansion_ratio")
        expected = _safe_filename(expected_filename, content_type=None).casefold()
        member = next(
            (
                item
                for item in members
                if _safe_filename(item.filename, content_type=None).casefold() == expected
                and "/" not in item.filename.replace("\\", "/").strip("/")
                and not ((item.external_attr >> 16) & 0o170000) == 0o120000
            ),
            None,
        )
        if member is None:
            raise UnsafeDownloadError("archive_expected_member_missing")
        with archive.open(member) as handle:
            data = handle.read(DEFAULT_IMAGE_MAX_BYTES + 1)
        if len(data) > DEFAULT_IMAGE_MAX_BYTES:
            raise UnsafeDownloadError("asset_too_large")
    guessed = s3_mirror._sniff_image_content_type(data[:4096]) or "application/octet-stream"
    return _validate_media_payload(
        candidate=candidate,
        source_url=f"nbcumv-hires:{expected}",
        data=data,
        content_type=guessed,
    )


def secure_download_candidate(
    candidate: Mapping[str, Any],
    *,
    nbcumv_session: Any | None = None,
    request_get: Callable[..., Any] = requests.get,
) -> DownloadedMedia:
    """Download, cap, probe and hash-ready validate one candidate's bytes."""
    source = _clean(candidate.get("source"))
    acquisition = _as_mapping(candidate.get("acquisition"))
    if source == "nbcumv" and acquisition.get("method") == "nbcumv_hires_zip":
        lbx_id = _clean(acquisition.get("lbx_id"))
        filename = _clean(acquisition.get("filename")) or _clean(candidate.get("filename"))
        if not lbx_id or not filename:
            raise UnsafeDownloadError("nbcumv_hires_identity_missing")
        zip_url = nbcumv.request_hires_zip_url(lbx_id=lbx_id, filename=filename, session=nbcumv_session)
        data, _content_type, _final_url = _fetch_https_bytes(
            zip_url,
            source_policy="nbcumv",
            max_bytes=DEFAULT_ARCHIVE_MAX_BYTES,
            request_get=request_get,
        )
        return _extract_hires_zip(data, expected_filename=filename, candidate=candidate)

    source_url = _clean(candidate.get("download_url")) or _clean(candidate.get("original_url"))
    if source_url is None:
        raise UnsafeDownloadError("candidate_has_no_downloadable_url")
    data, content_type, final_url = _fetch_https_bytes(
        source_url,
        source_policy=_candidate_source_policy(candidate),
        max_bytes=_max_bytes(candidate),
        request_get=request_get,
    )
    return _validate_media_payload(
        candidate=candidate,
        source_url=final_url,
        data=data,
        content_type=content_type,
    )


def _flatten_values(value: Any) -> Iterable[Any]:
    if isinstance(value, Mapping):
        for inner in value.values():
            yield from _flatten_values(inner)
    elif isinstance(value, (list, tuple, set)):
        for inner in value:
            yield from _flatten_values(inner)
    else:
        yield value


def _matches_season_value(value: Any, target: int) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, int):
        return value == target
    text = str(value).strip()
    if text == str(target):
        return True
    return any(int(token) == target for token in _NUMERIC_SEASON_RE.findall(text))


def _configured_mappings(rules: Mapping[str, Any], *, source: str) -> list[Mapping[str, Any]]:
    raw = rules.get("mappings", rules.get("source_mappings", rules.get(source)))
    if isinstance(raw, Mapping):
        if source in raw and isinstance(raw[source], (list, tuple, Mapping)):
            raw = raw[source]
        else:
            raw = [raw]
    return [item for item in _as_list(raw) if isinstance(item, Mapping)]


def qualify_candidate(candidate: Mapping[str, Any], watch: Mapping[str, Any]) -> Qualification:
    """Classify confirmed/inferred/unresolved using the watch's immutable rules.

    Unknown rule shapes never become a guess.  They leave the candidate
    unresolved and retain the source fields in the evidence journal.
    """
    target = int(watch["target_season_number"])
    rules = _as_mapping(watch.get("source_season_rules"))
    source = _clean(candidate.get("source")) or ""
    raw_fields = _as_mapping(candidate.get("raw_season_fields"))
    explicit_candidates = [candidate.get("season_id"), candidate.get("season_number")]
    for key, value in raw_fields.items():
        for inner in _flatten_values(value):
            if _matches_season_value(inner, target):
                return Qualification(
                    "confirmed",
                    {"kind": "explicit_source_season", "field": key, "value": inner, "target": target},
                    True,
                )
    for value in explicit_candidates:
        if value == watch.get("season_id") or _matches_season_value(value, target):
            return Qualification(
                "confirmed",
                {"kind": "explicit_candidate_season", "value": value, "target": target},
                True,
            )
    for index, mapping in enumerate(_configured_mappings(rules, source=source)):
        mapped_target = mapping.get("target_season_number", mapping.get("target"))
        source_value = mapping.get("source_season_number", mapping.get("source_value"))
        field = _clean(mapping.get("field"))
        if not _matches_season_value(mapped_target, target) or source_value is None:
            continue
        values = [raw_fields.get(field)] if field else list(raw_fields.values())
        if any(_matches_season_value(value, int(source_value)) for value in values for value in _flatten_values(value)):
            return Qualification(
                "confirmed",
                {
                    "kind": "configured_source_mapping",
                    "mapping_index": index,
                    "field": field,
                    "source_value": source_value,
                    "target": target,
                },
                True,
            )

    inference = _as_mapping(rules.get("inferred"))
    if not bool(inference.get("enabled", False)):
        return Qualification("unresolved", {"kind": "insufficient_season_evidence", "target": target}, False)
    text = " ".join(
        str(candidate.get(key) or "") for key in ("caption", "headline", "filename")
    ).casefold()
    patterns = [str(item).strip() for item in _as_list(inference.get("text_patterns")) if str(item).strip()]
    matched = next((pattern for pattern in patterns if pattern.casefold() in text), None)
    if matched:
        return Qualification(
            "inferred",
            {
                "kind": "configured_text_inference",
                "rule": "inferred.text_patterns",
                "pattern": matched,
                "target": target,
            },
            bool(inference.get("season_link", True)),
        )
    return Qualification("unresolved", {"kind": "inference_rule_not_met", "target": target}, False)


class WatcherAcquisitionService:
    """Orchestrate one already-claimed watch run with injectable side effects."""

    def __init__(
        self,
        *,
        repository: Any = media_watchers,
        discover_nbcumv: Callable[..., sources.SourceDiscoveryResult] = sources.discover_nbcumv_show_candidates,
        discover_bravo: Callable[..., sources.SourceDiscoveryResult] = sources.discover_bravo_incremental_candidates,
        downloader: Callable[[Mapping[str, Any]], DownloadedMedia] | None = None,
        r2_client: R2Client | None = None,
        r2_bucket: str | None = None,
        r2_url_builder: Callable[[str], str] = s3_mirror.build_hosted_url,
        observation_lookup: Callable[[str, str, str], Mapping[str, Any] | None] | None = None,
        commit_acquisition: Callable[..., str | None] | None = None,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.repository = repository
        self.discover_nbcumv = discover_nbcumv
        self.discover_bravo = discover_bravo
        self.downloader = downloader or (lambda candidate: secure_download_candidate(candidate))
        self.r2_client = r2_client
        self.r2_bucket = r2_bucket
        self.r2_url_builder = r2_url_builder
        self.observation_lookup = observation_lookup or self._lookup_observation
        self.commit_acquisition = commit_acquisition or self._commit_acquisition
        self.now = now

    def run(
        self,
        watch: Mapping[str, Any],
        *,
        lease_owner: str,
        lease_fence: int,
        backfill: bool | None = None,
        nbcumv_session: Any | None = None,
        bravo_client: Any | None = None,
    ) -> WatchRunResult:
        """Acquire a complete watch scan, or durably yield an incomplete journal."""
        normalized_watch = dict(watch)
        watch_id = _clean(normalized_watch.get("id"))
        if not watch_id:
            raise ValueError("watch id is required")
        owner = _clean(lease_owner)
        if not owner:
            raise ValueError("lease_owner is required")
        source_state_before = _as_mapping(normalized_watch.get("source_state"))
        effective_backfill = bool(normalized_watch.get("backfill_mode")) if backfill is None else bool(backfill)
        is_baseline = normalized_watch.get("baseline_completed_at") in (None, "")
        baseline = None
        if is_baseline:
            baseline = self.repository.start_baseline_generation(
                watch_id=watch_id,
                lease_owner=owner,
                lease_fence=int(lease_fence),
            )
            if baseline is None:
                return self._fenced_result(None, source_state_before)
        run = self.repository.start_run(
            watch_id=watch_id,
            lease_owner=owner,
            lease_fence=int(lease_fence),
            source_state_before=source_state_before,
            baseline_generation_id=_clean((baseline or {}).get("id")),
        )
        if run is None:
            return self._fenced_result(None, source_state_before)
        run_id = str(run["id"])
        summary = self._empty_summary(is_baseline=is_baseline, backfill=effective_backfill)
        cursor_journal: dict[str, Any] = {}
        candidate_journal: dict[str, Any] = {}
        continuation: dict[str, Any] = {}
        source_state_after = _as_mapping(source_state_before)
        try:
            for source in self._enabled_sources(normalized_watch):
                result = self._discover(
                    source,
                    normalized_watch,
                    source_state_before,
                    bravo_client=bravo_client,
                    nbcumv_session=nbcumv_session,
                )
                cursor_journal[source] = {
                    "pages_fetched": result.pages_fetched,
                    "complete": result.complete,
                    "terminal_streams": list(result.terminal_streams),
                    "provenance": dict(result.provenance),
                }
                for raw_candidate in result.candidates:
                    outcome = self._process_candidate(
                        normalized_watch,
                        raw_candidate,
                        lease_owner=owner,
                        lease_fence=int(lease_fence),
                        baseline_generation_id=_clean((baseline or {}).get("id")),
                        baseline_only=is_baseline and not effective_backfill,
                    )
                    summary[outcome] = int(summary.get(outcome, 0)) + 1
                    if len(candidate_journal) < 2000:
                        candidate = _as_mapping(raw_candidate)
                        candidate_journal[f"{source}:{candidate.get('source_asset_id')}"] = outcome
                if not result.complete:
                    continuation[source] = {
                        "token": result.continuation,
                        "reason": _as_mapping(result.provenance).get("incomplete_reason", "incomplete"),
                    }
                else:
                    source_state_after[source] = self._source_state_after(
                        source,
                        result.candidates,
                        source_state_before,
                    )
                checkpoint = self.repository.update_run_journal(
                    run_id=run_id,
                    watch_id=watch_id,
                    lease_owner=owner,
                    lease_fence=int(lease_fence),
                    cursor_journal=cursor_journal,
                    candidate_journal=candidate_journal,
                    summary=summary,
                    continuation=continuation,
                    source_state_after=source_state_after if result.complete else source_state_before,
                )
                if checkpoint is None:
                    raise WatcherFenceLostError("watch lease was lost while checkpointing")
                if not result.complete:
                    return self._finish(
                        run_id=run_id,
                        watch=normalized_watch,
                        lease_owner=owner,
                        lease_fence=int(lease_fence),
                        status="incomplete",
                        source_state_after=source_state_before,
                        summary=summary,
                        continuation=continuation,
                    )
            if baseline is not None:
                return self._finish_baseline_atomic(
                    run_id=run_id,
                    watch_id=watch_id,
                    watch=normalized_watch,
                    baseline_generation_id=str(baseline["id"]),
                    lease_owner=owner,
                    lease_fence=int(lease_fence),
                    source_state_after=source_state_after,
                    summary=summary,
                )
            return self._finish(
                run_id=run_id,
                watch=normalized_watch,
                lease_owner=owner,
                lease_fence=int(lease_fence),
                status="completed",
                source_state_after=source_state_after,
                summary=summary,
                continuation={},
            )
        except WatcherFenceLostError as exc:
            return self._fenced_result(run_id, source_state_before, summary=summary, error=str(exc))
        except Exception as exc:
            # A failed source scan must retain all prior source watermarks.  The
            # run journal remains the recovery record for any acquired object.
            try:
                return self._finish(
                    run_id=run_id,
                    watch=normalized_watch,
                    lease_owner=owner,
                    lease_fence=int(lease_fence),
                    status="failed",
                    source_state_after=source_state_before,
                    summary=summary,
                    continuation=continuation,
                    error_detail=f"{exc.__class__.__name__}: {exc}",
                )
            except WatcherFenceLostError as fence_error:
                return self._fenced_result(run_id, source_state_before, summary=summary, error=str(fence_error))

    def _enabled_sources(self, watch: Mapping[str, Any]) -> list[str]:
        values = [str(value).strip().casefold() for value in _as_list(watch.get("sources"))]
        enabled = [source for source in values if source in {"nbcumv", "bravo"}]
        if not enabled:
            raise WatcherServiceError("watch has no supported enabled sources")
        return list(dict.fromkeys(enabled))

    def _discover(
        self,
        source: str,
        watch: Mapping[str, Any],
        source_state: Mapping[str, Any],
        *,
        bravo_client: Any | None,
        nbcumv_session: Any | None,
    ) -> sources.SourceDiscoveryResult:
        if source == "nbcumv":
            return self.discover_nbcumv(str(watch["nbcumv_show_id"]), session=nbcumv_session)
        state = _as_mapping(source_state.get("bravo"))
        continuation = _clean(state.get("continuation")) or self._latest_bravo_continuation(str(watch["id"]))
        watermarks = _as_mapping(state.get("watermarks")) or {
            key: state.get(key)
            for key in ("created_at", "created_source_id", "changed_at", "changed_source_id")
            if state.get(key) is not None
        }
        return self.discover_bravo(
            str(watch["bravo_show_uuid"]),
            client=bravo_client,
            watermarks=watermarks,
            overlap=timedelta(seconds=max(0, int(watch.get("overlap_seconds") or 0))),
            continuation=continuation,
        )

    def _resource_enabled(self, watch: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
        enabled = {str(value).strip().casefold() for value in _as_list(watch.get("resource_types"))}
        resource = _clean(candidate.get("resource_type")) or ""
        media_type = _clean(candidate.get("media_type")) or ""
        return (
            not enabled
            or resource.casefold() in enabled
            or media_type.casefold() in enabled
            or resource.rsplit("/", 1)[-1].casefold() in enabled
        )

    def _process_candidate(
        self,
        watch: Mapping[str, Any],
        raw_candidate: Mapping[str, Any],
        *,
        lease_owner: str,
        lease_fence: int,
        baseline_generation_id: str | None,
        baseline_only: bool,
    ) -> str:
        try:
            candidate = sources.normalize_watcher_candidate(raw_candidate)
        except Exception:
            return "rejected"
        if not self._resource_enabled(watch, candidate):
            return "skipped"
        qualification = qualify_candidate(candidate, watch)
        metadata = {
            "candidate": self._compact_candidate_metadata(candidate),
            "qualification": {"status": qualification.status, "evidence": qualification.evidence},
        }
        fingerprint = source_fingerprint(candidate)
        existing = self.observation_lookup(
            str(watch["id"]),
            str(candidate["source"]),
            str(candidate["source_asset_id"]),
        )
        if baseline_only:
            self._write_observation(
                watch=watch,
                candidate=candidate,
                lease_owner=lease_owner,
                lease_fence=lease_fence,
                fingerprint=fingerprint,
                metadata=metadata,
                baseline_generation_id=baseline_generation_id,
                acquisition_state="observed_without_bytes",
                revalidate_after=None,
            )
            return "observed"
        if self._resume_uploaded(
            watch=watch,
            candidate=candidate,
            existing=existing,
            qualification=qualification,
            lease_owner=lease_owner,
            lease_fence=lease_fence,
            fingerprint=fingerprint,
        ):
            return "adopted"
        if not self._needs_acquisition(existing, fingerprint):
            return "unchanged"
        if _clean(candidate.get("media_type")) == "metadata":
            self._write_observation(
                watch=watch,
                candidate=candidate,
                lease_owner=lease_owner,
                lease_fence=lease_fence,
                fingerprint=fingerprint,
                metadata={**metadata, "rejection": "candidate_not_downloadable"},
                baseline_generation_id=baseline_generation_id,
                acquisition_state="rejected",
                revalidate_after=None,
            )
            return "rejected"
        self._heartbeat(watch, lease_owner=lease_owner, lease_fence=lease_fence)
        try:
            downloaded = self.downloader(candidate)
            if not isinstance(downloaded, DownloadedMedia):
                raise WatcherServiceError("downloader did not return DownloadedMedia")
            digest = hashlib.sha256(downloaded.data).hexdigest()
            key = build_revision_r2_key(watch, candidate, sha256=digest, content_type=downloaded.content_type)
            pending = self._pending_acquisition(
                candidate=candidate,
                downloaded=downloaded,
                sha256=digest,
                key=key,
                qualification=qualification,
            )
            self._write_observation(
                watch=watch,
                candidate=candidate,
                lease_owner=lease_owner,
                lease_fence=lease_fence,
                fingerprint=fingerprint,
                metadata={**metadata, "pending_acquisition": pending},
                baseline_generation_id=baseline_generation_id,
                acquisition_state="downloaded",
                revalidate_after=None,
            )
            stored = self._upload_or_adopt(downloaded, sha256=digest, key=key)
            self._write_observation(
                watch=watch,
                candidate=candidate,
                lease_owner=lease_owner,
                lease_fence=lease_fence,
                fingerprint=fingerprint,
                metadata={**metadata, "pending_acquisition": {**pending, "stored": self._stored_dict(stored)}},
                baseline_generation_id=baseline_generation_id,
                acquisition_state="r2_uploaded",
                revalidate_after=None,
            )
            asset_id = self.commit_acquisition(
                watch=watch,
                candidate=candidate,
                qualification=qualification,
                stored=stored,
                lease_owner=lease_owner,
                lease_fence=lease_fence,
                fingerprint=fingerprint,
            )
            if asset_id is None:
                raise WatcherFenceLostError("watch lease was lost before database commit")
            return "revised" if self._existing_byte_backed(existing) else "added"
        except WatcherFenceLostError:
            raise
        except Exception as exc:
            self._write_observation(
                watch=watch,
                candidate=candidate,
                lease_owner=lease_owner,
                lease_fence=lease_fence,
                fingerprint=fingerprint,
                metadata={**metadata, "rejection": f"{exc.__class__.__name__}: {exc}"},
                baseline_generation_id=baseline_generation_id,
                acquisition_state="rejected",
                revalidate_after=None,
            )
            return "rejected"

    def _needs_acquisition(self, observation: Mapping[str, Any] | None, fingerprint: Mapping[str, Any]) -> bool:
        if not observation:
            return True
        if not self._existing_byte_backed(observation):
            return True
        if _as_mapping(observation.get("source_fingerprint")) != dict(fingerprint):
            return True
        revalidate_after = _parse_time(observation.get("revalidate_after"))
        return revalidate_after is None or revalidate_after <= self.now()

    @staticmethod
    def _existing_byte_backed(observation: Mapping[str, Any] | None) -> bool:
        return bool(observation and observation.get("acquisition_state") == "db_committed")

    def _resume_uploaded(
        self,
        *,
        watch: Mapping[str, Any],
        candidate: Mapping[str, Any],
        existing: Mapping[str, Any] | None,
        qualification: Qualification,
        lease_owner: str,
        lease_fence: int,
        fingerprint: Mapping[str, Any],
    ) -> bool:
        if not existing or existing.get("acquisition_state") not in {"downloaded", "r2_uploaded"}:
            return False
        if _as_mapping(existing.get("source_fingerprint")) != dict(fingerprint):
            return False
        pending = _as_mapping(_as_mapping(existing.get("metadata")).get("pending_acquisition"))
        digest = _clean(pending.get("sha256"))
        key = _clean(pending.get("key"))
        content_type = _clean(pending.get("content_type"))
        if not digest or not key or not content_type:
            return False
        stored = self._adopt_existing_object(
            key=key,
            sha256=digest,
            content_type=content_type,
            width=pending.get("width") if isinstance(pending.get("width"), int) else None,
            height=pending.get("height") if isinstance(pending.get("height"), int) else None,
        )
        if stored is None:
            return False
        asset_id = self.commit_acquisition(
            watch=watch,
            candidate=candidate,
            qualification=qualification,
            stored=stored,
            lease_owner=lease_owner,
            lease_fence=lease_fence,
            fingerprint=fingerprint,
        )
        if asset_id is None:
            raise WatcherFenceLostError("watch lease was lost while adopting R2 object")
        return True

    def _pending_acquisition(
        self,
        *,
        candidate: Mapping[str, Any],
        downloaded: DownloadedMedia,
        sha256: str,
        key: str,
        qualification: Qualification,
    ) -> dict[str, Any]:
        return {
            "sha256": sha256,
            "key": key,
            "content_type": downloaded.content_type,
            "bytes": len(downloaded.data),
            "width": downloaded.width,
            "height": downloaded.height,
            "source_url": downloaded.source_url,
            "source_updated_at": _source_updated_at(candidate),
            "qualification": {"status": qualification.status, "evidence": qualification.evidence},
        }

    def _r2(self) -> tuple[R2Client, str]:
        # get_object_storage_client returns an untyped boto3 client satisfying the R2Client protocol.
        client = self.r2_client or cast("R2Client", s3_mirror.get_object_storage_client())
        bucket = self.r2_bucket or s3_mirror.get_object_storage_bucket()
        return client, bucket

    @staticmethod
    def _head(client: R2Client, bucket: str, key: str) -> Mapping[str, Any] | None:
        try:
            return client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            response = getattr(exc, "response", {})
            code = str(_as_mapping(response).get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def _stored_from_head(
        self,
        *,
        bucket: str,
        key: str,
        sha256: str,
        content_type: str,
        head: Mapping[str, Any],
        width: int | None = None,
        height: int | None = None,
    ) -> StoredObject:
        metadata = {str(k).casefold(): str(v) for k, v in _as_mapping(head.get("Metadata")).items()}
        stored_digest = metadata.get("sha256") or metadata.get("x-amz-meta-sha256")
        if stored_digest != sha256:
            raise ReconciliationError("r2_object_sha256_metadata_mismatch")
        length = head.get("ContentLength")
        if not isinstance(length, int) or length < 0:
            raise ReconciliationError("r2_object_length_missing")
        stored_type = s3_mirror._normalize_content_type(head.get("ContentType")) or content_type
        etag = _clean(head.get("ETag"))
        return StoredObject(
            bucket=bucket,
            key=key,
            url=self.r2_url_builder(key),
            sha256=sha256,
            size_bytes=length,
            content_type=stored_type,
            etag=etag.strip('"') if etag else None,
            width=width,
            height=height,
        )

    def _adopt_existing_object(
        self,
        *,
        key: str,
        sha256: str,
        content_type: str,
        width: int | None = None,
        height: int | None = None,
    ) -> StoredObject | None:
        client, bucket = self._r2()
        head = self._head(client, bucket, key)
        if head is None:
            return None
        return self._stored_from_head(
            bucket=bucket,
            key=key,
            sha256=sha256,
            content_type=content_type,
            head=head,
            width=width,
            height=height,
        )

    def _upload_or_adopt(self, downloaded: DownloadedMedia, *, sha256: str, key: str) -> StoredObject:
        client, bucket = self._r2()
        head = self._head(client, bucket, key)
        if head is None:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=downloaded.data,
                ContentType=downloaded.content_type,
                CacheControl="public, max-age=31536000, immutable",
                Metadata={"sha256": sha256},
            )
            head = self._head(client, bucket, key)
        if head is None:
            raise ReconciliationError("r2_upload_head_missing")
        stored = self._stored_from_head(
            bucket=bucket,
            key=key,
            sha256=sha256,
            content_type=downloaded.content_type,
            head=head,
            width=downloaded.width,
            height=downloaded.height,
        )
        if stored.size_bytes != len(downloaded.data):
            raise ReconciliationError("r2_object_length_mismatch")
        return stored

    def _write_observation(
        self,
        *,
        watch: Mapping[str, Any],
        candidate: Mapping[str, Any],
        lease_owner: str,
        lease_fence: int,
        fingerprint: Mapping[str, Any],
        metadata: Mapping[str, Any],
        baseline_generation_id: str | None,
        acquisition_state: str,
        revalidate_after: datetime | None,
    ) -> None:
        row = self.repository.upsert_observation(
            watch_id=str(watch["id"]),
            lease_owner=lease_owner,
            lease_fence=lease_fence,
            source=str(candidate["source"]),
            source_asset_id=str(candidate["source_asset_id"]),
            source_fingerprint=fingerprint,
            source_updated_at=_source_updated_at(candidate),
            source_url=_clean(candidate.get("original_url")),
            raw_season_fields=_as_mapping(candidate.get("raw_season_fields")),
            metadata=metadata,
            baseline_generation_id=baseline_generation_id,
            acquisition_state=acquisition_state,
            revalidate_after=revalidate_after,
        )
        if row is None:
            raise WatcherFenceLostError("watch lease was lost while writing observation")

    def _heartbeat(self, watch: Mapping[str, Any], *, lease_owner: str, lease_fence: int) -> None:
        heartbeat = getattr(self.repository, "heartbeat_lease", None)
        if callable(heartbeat) and not heartbeat(
            watch_id=str(watch["id"]), lease_owner=lease_owner, lease_fence=lease_fence
        ):
            raise WatcherFenceLostError("watch lease expired before acquisition")

    @staticmethod
    def _compact_candidate_metadata(candidate: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: candidate.get(key)
            for key in (
                "resource_type",
                "media_type",
                "filename",
                "mime_type",
                "caption",
                "headline",
                "people",
                "provenance",
            )
            if candidate.get(key) not in (None, "", [], {})
        }

    @staticmethod
    def _stored_dict(stored: StoredObject) -> dict[str, Any]:
        return {
            "bucket": stored.bucket,
            "key": stored.key,
            "url": stored.url,
            "sha256": stored.sha256,
            "bytes": stored.size_bytes,
            "content_type": stored.content_type,
            "etag": stored.etag,
        }

    def _lookup_observation(self, watch_id: str, source: str, source_asset_id: str) -> Mapping[str, Any] | None:
        return pg.fetch_one(
            """
            SELECT source_fingerprint, acquisition_state, revalidate_after, metadata,
                   source_updated_at, source_url
            FROM core.show_season_media_watch_observations
            WHERE watch_id = %s::uuid AND source = %s AND source_asset_id = %s
            """,
            [watch_id, source, source_asset_id],
        )

    def _latest_bravo_continuation(self, watch_id: str) -> str | None:
        row = pg.fetch_one(
            """
            SELECT continuation -> 'bravo' ->> 'token' AS token
            FROM core.show_season_media_watch_runs
            WHERE watch_id = %s::uuid AND status = 'incomplete'
              AND continuation ? 'bravo'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [watch_id],
        )
        return _clean((row or {}).get("token"))

    def _source_state_after(
        self,
        source: str,
        candidates: Sequence[Mapping[str, Any]],
        source_state_before: Mapping[str, Any],
    ) -> dict[str, Any]:
        if source != "bravo":
            return {"last_complete_at": _iso(self.now()), "continuation": None}
        prior = _as_mapping(source_state_before.get("bravo"))
        watermarks = _as_mapping(prior.get("watermarks"))
        for ordering, field in (("created", "created_at"), ("changed", "changed_at")):
            prior_at = _parse_time(watermarks.get(field) or prior.get(field))
            prior_id = _clean(watermarks.get(f"{ordering}_source_id") or prior.get(f"{ordering}_source_id"))
            best: tuple[datetime, str] | None = (prior_at, prior_id or "") if prior_at else None
            for candidate in candidates:
                timestamp = _parse_time(candidate.get(field))
                source_id = _clean(candidate.get("source_asset_id"))
                if timestamp and source_id and (best is None or (timestamp, source_id) > best):
                    best = (timestamp, source_id)
            if best:
                watermarks[field] = _iso(best[0])
                watermarks[f"{ordering}_source_id"] = best[1]
        return {"watermarks": watermarks, "continuation": None, "last_complete_at": _iso(self.now())}

    @staticmethod
    def _empty_summary(*, is_baseline: bool, backfill: bool) -> dict[str, Any]:
        return {
            "mode": "backfill" if backfill else ("baseline" if is_baseline else "poll"),
            "observed": 0,
            "added": 0,
            "adopted": 0,
            "revised": 0,
            "unchanged": 0,
            "skipped": 0,
            "rejected": 0,
        }

    def _finish(
        self,
        *,
        run_id: str,
        watch: Mapping[str, Any],
        lease_owner: str,
        lease_fence: int,
        status: str,
        source_state_after: Mapping[str, Any],
        summary: Mapping[str, Any],
        continuation: Mapping[str, Any],
        error_detail: str | None = None,
    ) -> WatchRunResult:
        row = self.repository.finish_run(
            run_id=run_id,
            watch_id=str(watch["id"]),
            lease_owner=lease_owner,
            lease_fence=lease_fence,
            status=status,
            source_state_after=source_state_after,
            next_check_seconds=max(1, int(watch.get("poll_interval_seconds") or 60)),
            summary=summary,
            continuation=continuation,
            error_detail=error_detail,
        )
        if row is None:
            raise WatcherFenceLostError("watch lease was lost while finishing run")
        return WatchRunResult(
            run_id=run_id,
            status=status,
            summary=dict(summary),
            source_state_after=dict(source_state_after),
            continuation=dict(continuation),
            error=error_detail,
        )

    @staticmethod
    def _fenced_result(
        run_id: str | None,
        source_state: Mapping[str, Any],
        *,
        summary: Mapping[str, Any] | None = None,
        error: str | None = None,
    ) -> WatchRunResult:
        return WatchRunResult(
            run_id=run_id,
            status="fenced",
            summary=dict(summary or {}),
            source_state_after=dict(source_state),
            continuation={},
            error=error,
        )

    def _finish_baseline_atomic(
        self,
        *,
        watch_id: str,
        run_id: str,
        watch: Mapping[str, Any],
        baseline_generation_id: str,
        lease_owner: str,
        lease_fence: int,
        source_state_after: Mapping[str, Any],
        summary: Mapping[str, Any],
    ) -> WatchRunResult:
        """Complete the baseline and run before releasing the current lease.

        ``finish_run`` correctly releases a lease for ordinary runs.  A baseline
        also has to update its generation and watch marker, so those writes must
        be committed in this same fence-guarded transaction rather than after
        the normal finalizer has released the lease.
        """
        with pg.db_connection(label="finish-show-season-media-baseline") as conn:
            guard = pg.fetch_one(
                """
                SELECT id FROM core.show_season_media_watches
                WHERE id = %s::uuid AND lease_owner = %s AND lease_fence = %s::bigint
                  AND lease_expires_at > now()
                FOR UPDATE
                """,
                [watch_id, lease_owner, lease_fence],
                conn=conn,
            )
            if guard is None:
                raise WatcherFenceLostError("watch lease was lost before baseline completion")
            run_rows = pg.execute_returning(
                """
                UPDATE core.show_season_media_watch_runs
                SET status = 'completed', source_state_after = %s::jsonb,
                    summary = %s::jsonb, continuation = '{}'::jsonb,
                    error_detail = NULL, completed_at = now()
                WHERE id = %s::uuid AND watch_id = %s::uuid AND lease_fence = %s::bigint
                RETURNING id
                """,
                [_json(source_state_after), _json(summary), run_id, watch_id, lease_fence],
                conn=conn,
            )
            if not run_rows:
                raise WatcherServiceError("watch run disappeared before baseline completion")
            baseline_rows = pg.execute_returning(
                """
                UPDATE core.show_season_media_watch_baseline_generations
                SET status = 'completed', completed_at = now()
                WHERE id = %s::uuid AND watch_id = %s::uuid AND status = 'running'
                RETURNING id
                """,
                [baseline_generation_id, watch_id],
                conn=conn,
            )
            if not baseline_rows:
                raise WatcherServiceError("baseline generation was not running at completion")
            watch_rows = pg.execute_returning(
                """
                UPDATE core.show_season_media_watches
                SET source_state = %s::jsonb,
                    baseline_completed_at = COALESCE(baseline_completed_at, now()),
                    next_check_at = now() + (%s::int * interval '1 second'),
                    last_success_at = now(), consecutive_failures = 0, last_error = NULL,
                    lease_owner = NULL, lease_expires_at = NULL, lease_heartbeat_at = now()
                WHERE id = %s::uuid AND lease_owner = %s AND lease_fence = %s::bigint
                  AND lease_expires_at > now()
                RETURNING id
                """,
                [
                    _json(source_state_after),
                    max(1, int(watch.get("poll_interval_seconds") or 60)),
                    watch_id,
                    lease_owner,
                    lease_fence,
                ],
                conn=conn,
            )
            if not watch_rows:
                raise WatcherFenceLostError("watch lease was lost before baseline completion")
        return WatchRunResult(
            run_id=run_id,
            status="completed",
            summary=dict(summary),
            source_state_after=dict(source_state_after),
            continuation={},
        )

    def _commit_acquisition(
        self,
        *,
        watch: Mapping[str, Any],
        candidate: Mapping[str, Any],
        qualification: Qualification,
        stored: StoredObject,
        lease_owner: str,
        lease_fence: int,
        fingerprint: Mapping[str, Any],
    ) -> str | None:
        """Commit all DB state after rechecking and locking the current fence."""
        watch_id = str(watch["id"])
        source = str(candidate["source"])
        source_asset_id = str(candidate["source_asset_id"])
        with pg.db_connection(label="commit-show-season-media-revision") as conn:
            guard = pg.fetch_one(
                """
                SELECT id FROM core.show_season_media_watches
                WHERE id = %s::uuid AND lease_owner = %s AND lease_fence = %s::bigint
                  AND lease_expires_at > now()
                FOR UPDATE
                """,
                [watch_id, lease_owner, lease_fence],
                conn=conn,
            )
            if guard is None:
                return None
            # A source record is the logical asset identity.  The immutable
            # revision table, not this current-state row, preserves every hash.
            inserted = pg.execute_returning(
                """
                INSERT INTO core.media_assets (
                  media_type, source, source_asset_id, source_url, content_type,
                  bytes, width, height, caption, hosted_bucket, hosted_key,
                  hosted_url, hosted_etag, hosted_at, fetched_at, metadata
                ) VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  now(), now(), %s::jsonb
                )
                ON CONFLICT (source, source_asset_id) WHERE source_asset_id IS NOT NULL DO NOTHING
                RETURNING id::text AS id
                """,
                [
                    str(candidate.get("media_type") or "image"),
                    source,
                    source_asset_id,
                    _clean(candidate.get("original_url")),
                    stored.content_type,
                    stored.size_bytes,
                    stored.width if stored.width is not None else candidate.get("width"),
                    stored.height if stored.height is not None else candidate.get("height"),
                    _clean(candidate.get("caption")) or _clean(candidate.get("headline")),
                    stored.bucket,
                    stored.key,
                    stored.url,
                    stored.etag,
                    _json({"watcher": {"watch_id": watch_id, "source": source}}),
                ],
                conn=conn,
            )
            asset = inserted[0] if inserted else pg.fetch_one(
                """
                SELECT id::text AS id FROM core.media_assets
                WHERE source = %s AND source_asset_id = %s
                FOR UPDATE
                """,
                [source, source_asset_id],
                conn=conn,
            )
            if asset is None:
                raise WatcherServiceError("logical media asset could not be resolved")
            asset_id = str(asset["id"])
            # Existing asset rows may be constrained by an earlier identical
            # hash.  Hosted fields remain current while revision history owns
            # content identity, avoiding an unsafe duplicate/overwrite path.
            pg.execute_returning(
                """
                UPDATE core.media_assets AS asset
                SET source_url = COALESCE(%s, asset.source_url),
                    content_type = %s, bytes = %s, width = COALESCE(%s, asset.width),
                    height = COALESCE(%s, asset.height),
                    caption = COALESCE(%s, asset.caption), hosted_bucket = %s,
                    hosted_key = %s, hosted_url = %s, hosted_etag = %s,
                    hosted_at = now(), fetched_at = now(),
                    sha256 = CASE WHEN NOT EXISTS (
                      SELECT 1 FROM core.media_assets AS other
                      WHERE other.id <> asset.id AND other.sha256 = %s
                    ) THEN %s ELSE asset.sha256 END,
                    hosted_sha256 = CASE WHEN NOT EXISTS (
                      SELECT 1 FROM core.media_assets AS other
                      WHERE other.id <> asset.id AND other.source = asset.source
                        AND other.hosted_sha256 = %s
                    ) THEN %s ELSE asset.hosted_sha256 END,
                    hosted_content_type = %s, hosted_bytes = %s,
                    metadata = COALESCE(asset.metadata, '{}'::jsonb) || %s::jsonb
                WHERE asset.id = %s::uuid
                RETURNING id::text AS id
                """,
                [
                    _clean(candidate.get("original_url")),
                    stored.content_type,
                    stored.size_bytes,
                    stored.width if stored.width is not None else candidate.get("width"),
                    stored.height if stored.height is not None else candidate.get("height"),
                    _clean(candidate.get("caption")) or _clean(candidate.get("headline")),
                    stored.bucket,
                    stored.key,
                    stored.url,
                    stored.etag,
                    stored.sha256,
                    stored.sha256,
                    stored.sha256,
                    stored.sha256,
                    stored.content_type,
                    stored.size_bytes,
                    _json({"watcher": {"watch_id": watch_id, "source": source, "source_asset_id": source_asset_id}}),
                    asset_id,
                ],
                conn=conn,
            )
            revision = pg.execute_returning(
                """
                INSERT INTO core.media_source_revisions (
                  watch_id, media_asset_id, source, source_asset_id,
                  source_updated_at, sha256, content_type, bytes, width, height,
                  etag, source_url, hosted_bucket, hosted_key, hosted_url,
                  fetched_at, metadata, acquisition_state
                ) VALUES (
                  %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, now(), %s::jsonb, 'db_committed'
                ) ON CONFLICT (media_asset_id, sha256) DO NOTHING
                RETURNING id::text AS id
                """,
                [
                    watch_id,
                    asset_id,
                    source,
                    source_asset_id,
                    _source_updated_at(candidate),
                    stored.sha256,
                    stored.content_type,
                    stored.size_bytes,
                    stored.width if stored.width is not None else candidate.get("width"),
                    stored.height if stored.height is not None else candidate.get("height"),
                    stored.etag,
                    _clean(candidate.get("original_url")),
                    stored.bucket,
                    stored.key,
                    stored.url,
                    _json(
                        {
                            "qualification": {
                                "status": qualification.status,
                                "evidence": qualification.evidence,
                            },
                            "provenance": candidate.get("provenance"),
                        }
                    ),
                ],
                conn=conn,
            )
            self._insert_links(
                conn=conn,
                watch=watch,
                candidate=candidate,
                qualification=qualification,
                asset_id=asset_id,
            )
            observation = pg.execute_returning(
                """
                UPDATE core.show_season_media_watch_observations
                SET acquisition_state = 'db_committed', revalidate_after = %s,
                    source_fingerprint = %s::jsonb,
                    last_acquired_at = now(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
                WHERE watch_id = %s::uuid AND source = %s AND source_asset_id = %s
                RETURNING id
                """,
                [
                    self.now() + DEFAULT_REVALIDATION_TTL,
                    _json(fingerprint),
                    _json({"committed_revision_sha256": stored.sha256, "revision_inserted": bool(revision)}),
                    watch_id,
                    source,
                    source_asset_id,
                ],
                conn=conn,
            )
            if not observation:
                raise WatcherServiceError("acquisition observation disappeared before commit")
        return asset_id

    def _insert_links(
        self,
        *,
        conn: Any,
        watch: Mapping[str, Any],
        candidate: Mapping[str, Any],
        qualification: Qualification,
        asset_id: str,
    ) -> None:
        link_rows: list[tuple[str, str]] = [("show", str(watch["show_id"]))]
        if qualification.season_link and qualification.status in {"confirmed", "inferred"}:
            link_rows.append(("season", str(watch["season_id"])))
        for person_id in self._person_ids(candidate.get("people"), conn=conn):
            link_rows.append(("person", person_id))
        for entity_type, entity_id in link_rows:
            pg.execute_returning(
                """
                INSERT INTO core.media_links (
                  entity_type, entity_id, media_asset_id, kind, is_primary, context
                ) VALUES (%s, %s::uuid, %s::uuid, 'watcher', false, %s::jsonb)
                ON CONFLICT (entity_type, entity_id, kind, media_asset_id) DO NOTHING
                RETURNING id
                """,
                [
                    entity_type,
                    entity_id,
                    asset_id,
                    _json({"watch_id": str(watch["id"]), "season_qualification": qualification.status}),
                ],
                conn=conn,
            )

    @staticmethod
    def _person_ids(value: Any, *, conn: Any) -> list[str]:
        names = list(dict.fromkeys(str(item).strip() for item in _as_list(value) if str(item).strip()))
        ids: list[str] = []
        for name in names:
            rows = pg.fetch_all(
                "SELECT id::text AS id FROM core.people WHERE full_name = %s ORDER BY id ASC LIMIT 2",
                [name],
                conn=conn,
            )
            # An ambiguous display name is evidence, not identity.  Do not
            # create a potentially wrong person link.
            if len(rows) == 1:
                ids.append(str(rows[0]["id"]))
        return ids


def run_show_season_media_watch(
    watch: Mapping[str, Any],
    *,
    lease_owner: str,
    lease_fence: int,
    backfill: bool | None = None,
    service: WatcherAcquisitionService | None = None,
) -> WatchRunResult:
    """Convenience entry point used by the scheduler/admin operation layer."""
    return (service or WatcherAcquisitionService()).run(
        watch,
        lease_owner=lease_owner,
        lease_fence=lease_fence,
        backfill=backfill,
    )


__all__ = [
    "DownloadedMedia",
    "Qualification",
    "ReconciliationError",
    "StoredObject",
    "UnsafeDownloadError",
    "WatcherAcquisitionService",
    "WatcherFenceLostError",
    "WatchRunResult",
    "build_revision_r2_key",
    "qualify_candidate",
    "run_show_season_media_watch",
    "secure_download_candidate",
    "source_fingerprint",
]
