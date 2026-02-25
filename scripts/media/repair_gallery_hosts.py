#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import requests

# Allow direct execution via absolute path without requiring PYTHONPATH.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._sync_common import load_env_and_db
from trr_backend.media.image_variants import generate_cast_photo_variants, generate_media_asset_variants
from trr_backend.media.s3_mirror import mirror_cast_photo_row, mirror_media_asset_row

CandidateKind = Literal["media_link_asset", "cast_photo"]
CandidateStatus = Literal["ok", "repaired", "broken_unreachable", "error"]

DEFAULT_ALLOWED_SOURCES = ("imdb", "tmdb", "fandom", "bravo")


@dataclass(frozen=True)
class RepairCandidate:
    kind: CandidateKind
    person_id: str
    source: str
    source_url: str | None
    hosted_url: str | None
    row_id: str
    row: dict[str, Any]
    metadata: dict[str, Any]
    link_id: str | None = None
    link_context: dict[str, Any] | None = None
    source_page_url: str | None = None


@dataclass(frozen=True)
class ResumeState:
    start_index: int


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_gallery_hosts",
        description=(
            "Repair broken hosted gallery URLs for person gallery media assets and cast photos. "
            "Default mode is dry-run."
        ),
    )
    parser.add_argument("--apply", action="store_true", help="Apply updates (default: dry-run).")
    parser.add_argument(
        "--sources",
        type=str,
        default=",".join(DEFAULT_ALLOWED_SOURCES),
        help="Comma-separated sources to include (default: imdb,tmdb,fandom,bravo).",
    )
    parser.add_argument("--person-id", action="append", default=[], help="Filter by person UUID (repeatable).")
    parser.add_argument("--show-id", action="append", default=[], help="Filter by show UUID (repeatable).")
    parser.add_argument("--limit", type=int, default=None, help="Optional overall candidate cap.")
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout seconds for hosted/source reachability checks.",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=2,
        help="Retry attempts for transient probe failures (default: 2).",
    )
    parser.add_argument(
        "--retry-backoff-ms",
        type=int,
        default=500,
        help="Backoff in milliseconds between transient retries (default: 500).",
    )
    parser.add_argument(
        "--confirm-unreachable-pass",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run a second confirmation probe before marking broken_unreachable (default: true).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Emit progress heartbeat every N processed candidates (default: 100).",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=str,
        default=None,
        help="Optional checkpoint sidecar JSON path for progress/resume metadata.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=250,
        help="Write checkpoint sidecar every N processed candidates (default: 250).",
    )
    parser.add_argument(
        "--resume-from-checkpoint",
        action="store_true",
        help="Resume from the provided --checkpoint-file using its last_index.",
    )
    parser.add_argument(
        "--force-flush-progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Flush heartbeat/progress prints immediately (default: true).",
    )
    parser.add_argument(
        "--fail-fast-on-apply-error",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "When apply mode encounters an exception, stop after the current candidate "
            "instead of continuing (default: false)."
        ),
    )
    parser.add_argument("--output-json", type=str, default=None, help="Optional output report path.")
    parser.add_argument("--verbose", action="store_true", help="Verbose logs.")
    return parser.parse_args(argv)


def _split_csv(values: str) -> set[str]:
    out: set[str] = set()
    for raw in str(values or "").split(","):
        normalized = raw.strip().lower()
        if normalized:
            out.add(normalized)
    return out


def _coerce_str_list(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_resume_state(checkpoint_file: Path | None) -> ResumeState:
    if checkpoint_file is None or not checkpoint_file.exists():
        return ResumeState(start_index=0)
    try:
        payload = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ResumeState(start_index=0)
    last_index = _safe_int(payload.get("last_index"), -1)
    return ResumeState(start_index=max(0, last_index + 1))


def _write_checkpoint(
    *,
    checkpoint_file: Path | None,
    mode: str,
    apply_updates: bool,
    processed: int,
    scanned_in_run: int,
    total_candidates: int,
    last_index: int,
    summary: dict[str, int],
    elapsed_seconds: float,
    sources: list[str],
) -> None:
    if checkpoint_file is None:
        return
    payload = {
        "version": 1,
        "mode": mode,
        "updated_at": _utc_now_iso(),
        "apply": bool(apply_updates),
        "processed": int(processed),
        "scanned_in_run": int(scanned_in_run),
        "total_candidates": int(total_candidates),
        "last_index": int(last_index),
        "elapsed_seconds": round(max(0.0, float(elapsed_seconds)), 3),
        "summary": {
            "ok": int(summary.get("ok", 0)),
            "repaired": int(summary.get("repaired", 0)),
            "broken_unreachable": int(summary.get("broken_unreachable", 0)),
            "error": int(summary.get("error", 0)),
        },
        "sources": list(sources),
    }
    checkpoint_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _emit_heartbeat(
    *,
    processed: int,
    scanned_in_run: int,
    total_candidates: int,
    summary: dict[str, int],
    start_monotonic: float,
    flush: bool,
) -> None:
    elapsed = max(0.001, time.monotonic() - start_monotonic)
    throughput = processed / elapsed
    payload = {
        "event": "heartbeat",
        "processed": int(processed),
        "scanned_in_run": int(scanned_in_run),
        "total_candidates": int(total_candidates),
        "summary": {
            "ok": int(summary.get("ok", 0)),
            "repaired": int(summary.get("repaired", 0)),
            "broken_unreachable": int(summary.get("broken_unreachable", 0)),
            "error": int(summary.get("error", 0)),
        },
        "elapsed_seconds": round(elapsed, 2),
        "throughput_per_sec": round(throughput, 3),
        "timestamp": _utc_now_iso(),
    }
    print(f"[heartbeat] {json.dumps(payload)}", flush=flush)


def _safe_rows(response: Any) -> list[dict[str, Any]]:
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error: {response.error}")
    rows = response.data or []
    return rows if isinstance(rows, list) else []


def _get_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    return trimmed if trimmed else None


def _source_matches(source: str | None, allowed_sources: set[str]) -> bool:
    normalized = (source or "").strip().lower()
    if not normalized:
        return False
    if normalized in allowed_sources:
        return True
    if "fandom" in allowed_sources and normalized.startswith("fandom"):
        return True
    if "bravo" in allowed_sources and normalized.startswith("bravo"):
        return True
    return False


def _request_headers(*, source: str, url: str, source_page_url: str | None = None) -> dict[str, str]:
    normalized_source = (source or "").strip().lower()
    host = urlparse(url).netloc.lower()
    headers: dict[str, str] = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Range": "bytes=0-0",
    }
    if normalized_source.startswith("imdb") or "imdb.com" in host or "media-amazon.com" in host:
        headers["Referer"] = "https://www.imdb.com/"
    elif normalized_source.startswith("fandom") or "fandom.com" in host or "wikia.nocookie.net" in host:
        headers["Referer"] = source_page_url or "https://www.fandom.com/"
    elif source_page_url:
        headers["Referer"] = source_page_url
    return headers


def _check_url_reachability(
    *,
    url: str | None,
    source: str,
    timeout: float,
    source_page_url: str | None = None,
) -> tuple[bool, str]:
    if not isinstance(url, str) or not url.strip():
        return False, "missing_url"
    candidate_url = url.strip()
    try:
        response = requests.get(
            candidate_url,
            headers=_request_headers(source=source, url=candidate_url, source_page_url=source_page_url),
            timeout=timeout,
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        return False, f"request_failed:{exc.__class__.__name__}"
    if response.status_code in {200, 206}:
        return True, f"http_{response.status_code}"
    return False, f"http_{response.status_code}"


def _is_transient_failure_reason(reason: str) -> bool:
    normalized = (reason or "").strip().lower()
    if normalized.startswith("http_"):
        code_raw = normalized.split("_", 1)[1]
        if code_raw.isdigit():
            code = int(code_raw)
            if code == 429:
                return True
            if 500 <= code <= 599:
                return True
        return False
    if normalized.startswith("request_failed:"):
        transient_exception_names = {
            "timeout",
            "connecttimeout",
            "readtimeout",
            "connectionerror",
            "chunkedencodingerror",
            "ssLError".lower(),
            "proxyerror",
            "toomanyredirects",
        }
        exc_name = normalized.split(":", 1)[1].strip().lower()
        return exc_name in transient_exception_names
    return False


@dataclass(frozen=True)
class ReachabilityProbeResult:
    ok: bool
    reason: str
    attempts: int
    transient_failure: bool


def _probe_url_reachability(
    *,
    url: str | None,
    source: str,
    timeout: float,
    source_page_url: str | None,
    retry_attempts: int,
    retry_backoff_ms: int,
) -> ReachabilityProbeResult:
    attempts = max(1, int(retry_attempts))
    backoff_seconds = max(0.0, float(retry_backoff_ms) / 1000.0)
    last_reason = "unknown"

    for idx in range(attempts):
        ok, reason = _check_url_reachability(
            url=url,
            source=source,
            timeout=timeout,
            source_page_url=source_page_url,
        )
        if ok:
            return ReachabilityProbeResult(
                ok=True,
                reason=reason,
                attempts=idx + 1,
                transient_failure=False,
            )
        last_reason = reason
        is_transient = _is_transient_failure_reason(reason)
        if not is_transient:
            return ReachabilityProbeResult(
                ok=False,
                reason=reason,
                attempts=idx + 1,
                transient_failure=False,
            )
        if idx < attempts - 1 and backoff_seconds > 0:
            time.sleep(backoff_seconds)

    return ReachabilityProbeResult(
        ok=False,
        reason=last_reason,
        attempts=attempts,
        transient_failure=_is_transient_failure_reason(last_reason),
    )


def _resolve_show_person_ids(db, show_ids: list[str]) -> set[str]:
    if not show_ids:
        return set()
    out: set[str] = set()
    for show_id in show_ids:
        response = (
            db.schema("core")
            .table("show_cast")
            .select("person_id")
            .eq("show_id", show_id)
            .not_.is_("person_id", "null")
            .limit(5000)
            .execute()
        )
        for row in _safe_rows(response):
            person_id = _get_str(row.get("person_id"))
            if person_id:
                out.add(person_id)
    return out


def _fetch_gallery_link_rows(db, person_ids: set[str] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 500
    while True:
        response = (
            db.schema("core")
            .table("media_links")
            .select("id,entity_id,media_asset_id,context")
            .eq("entity_type", "person")
            .eq("kind", "gallery")
            .not_.is_("media_asset_id", "null")
            .order("created_at", desc=True)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = _safe_rows(response)
        if not page:
            break
        for row in page:
            entity_id = _get_str(row.get("entity_id"))
            if person_ids and entity_id not in person_ids:
                continue
            rows.append(row)
        offset += len(page)
    return rows


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[idx : idx + size] for idx in range(0, len(values), size)]


def _fetch_media_assets_by_ids(db, asset_ids: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for chunk in _chunked(asset_ids, 200):
        response = (
            db.schema("core")
            .table("media_assets")
            .select("id,source,source_url,hosted_url,metadata,hosted_sha256,hosted_key,hosted_bucket")
            .in_("id", chunk)
            .execute()
        )
        for row in _safe_rows(response):
            row_id = _get_str(row.get("id"))
            if row_id:
                out[row_id] = row
    return out


def _resolve_cast_source_url(row: dict[str, Any]) -> str | None:
    for key in ("url", "image_url", "thumb_url"):
        candidate = _get_str(row.get(key))
        if candidate:
            return candidate
    return None


def _extract_crop_payload(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    try:
        x = float(value.get("x"))
        y = float(value.get("y"))
        zoom = float(value.get("zoom"))
    except (TypeError, ValueError):
        return None
    mode_raw = str(value.get("mode") or "auto").strip().lower()
    return {
        "x": max(0.0, min(100.0, x)),
        "y": max(0.0, min(100.0, y)),
        "zoom": max(1.0, min(4.0, zoom)),
        "mode": "manual" if mode_raw == "manual" else "auto",
    }


def _candidate_crop_payload(candidate: RepairCandidate) -> dict[str, Any] | None:
    if candidate.kind == "media_link_asset":
        context_crop = _extract_crop_payload((candidate.link_context or {}).get("thumbnail_crop"))
        if context_crop:
            return context_crop
    return _extract_crop_payload(candidate.metadata.get("thumbnail_crop"))


def _collect_candidates(
    db,
    *,
    allowed_sources: set[str],
    person_ids: list[str],
    show_ids: list[str],
    limit: int | None,
) -> list[RepairCandidate]:
    explicit_person_ids = set(person_ids)
    explicit_person_ids.update(_resolve_show_person_ids(db, show_ids))
    gallery_links = _fetch_gallery_link_rows(db, explicit_person_ids or None)
    person_scope = explicit_person_ids or {
        person_id
        for person_id in (_get_str(row.get("entity_id")) for row in gallery_links)
        if person_id
    }

    media_asset_ids = [
        media_asset_id
        for media_asset_id in (_get_str(row.get("media_asset_id")) for row in gallery_links)
        if media_asset_id
    ]
    media_assets = _fetch_media_assets_by_ids(db, list(dict.fromkeys(media_asset_ids)))

    candidates: list[RepairCandidate] = []

    for link in gallery_links:
        link_id = _get_str(link.get("id"))
        person_id = _get_str(link.get("entity_id"))
        media_asset_id = _get_str(link.get("media_asset_id"))
        if not link_id or not person_id or not media_asset_id:
            continue
        asset = media_assets.get(media_asset_id)
        if not asset:
            continue
        source = _get_str(asset.get("source")) or "unknown"
        if not _source_matches(source, allowed_sources):
            continue
        metadata = asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {}
        source_url = _get_str(asset.get("source_url"))
        hosted_url = _get_str(asset.get("hosted_url"))
        source_page_url = (
            _get_str(metadata.get("source_page_url")) or _get_str(metadata.get("page_url")) or None
        )
        link_context = link.get("context") if isinstance(link.get("context"), dict) else {}
        candidates.append(
            RepairCandidate(
                kind="media_link_asset",
                person_id=person_id,
                source=source,
                source_url=source_url,
                hosted_url=hosted_url,
                row_id=media_asset_id,
                row=asset,
                metadata=metadata,
                link_id=link_id,
                link_context=link_context,
                source_page_url=source_page_url,
            )
        )

    if person_scope:
        for chunk in _chunked(list(person_scope), 200):
            response = (
                db.schema("core")
                .table("cast_photos")
                .select(
                    "id,person_id,source,url,image_url,thumb_url,hosted_url,metadata,source_page_url,"
                    "hosted_sha256,hosted_key,hosted_bucket"
                )
                .in_("person_id", chunk)
                .execute()
            )
            for row in _safe_rows(response):
                row_id = _get_str(row.get("id"))
                person_id = _get_str(row.get("person_id"))
                source = _get_str(row.get("source")) or "unknown"
                if not row_id or not person_id or not _source_matches(source, allowed_sources):
                    continue
                metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                source_url = _resolve_cast_source_url(row)
                hosted_url = _get_str(row.get("hosted_url"))
                source_page_url = _get_str(row.get("source_page_url"))
                candidates.append(
                    RepairCandidate(
                        kind="cast_photo",
                        person_id=person_id,
                        source=source,
                        source_url=source_url,
                        hosted_url=hosted_url,
                        row_id=row_id,
                        row=row,
                        metadata=metadata,
                        source_page_url=source_page_url,
                    )
                )

    if limit is not None and limit > 0:
        return candidates[:limit]
    return candidates


def _update_media_asset(db, asset_id: str, patch: dict[str, Any]) -> None:
    response = db.schema("core").table("media_assets").update(patch).eq("id", asset_id).execute()
    error = getattr(response, "error", None)
    if error:
        raise RuntimeError(str(error))


def _update_cast_photo(db, photo_id: str, patch: dict[str, Any]) -> None:
    response = db.schema("core").table("cast_photos").update(patch).eq("id", photo_id).execute()
    error = getattr(response, "error", None)
    if error:
        raise RuntimeError(str(error))


def _update_media_link_context(db, link_id: str, context: dict[str, Any]) -> None:
    response = db.schema("core").table("media_links").update({"context": context}).eq("id", link_id).execute()
    error = getattr(response, "error", None)
    if error:
        raise RuntimeError(str(error))


def _update_media_link_asset(db, link_id: str, media_asset_id: str) -> None:
    response = (
        db.schema("core")
        .table("media_links")
        .update({"media_asset_id": media_asset_id})
        .eq("id", link_id)
        .execute()
    )
    error = getattr(response, "error", None)
    if error:
        raise RuntimeError(str(error))


def _is_media_asset_source_sha_conflict(error_text: str) -> bool:
    text = str(error_text or "").strip().lower()
    if "media_assets_source_hosted_sha_uq" in text:
        return True
    if "duplicate key value violates unique constraint" in text and "hosted_sha" in text:
        return True
    return False


def _find_existing_media_asset_id_by_source_sha(
    db,
    *,
    source: str | None,
    hosted_sha256: str | None,
    exclude_asset_id: str,
) -> str | None:
    source_value = _get_str(source)
    sha_value = _get_str(hosted_sha256)
    if not source_value or not sha_value:
        return None
    response = (
        db.schema("core")
        .table("media_assets")
        .select("id")
        .eq("source", source_value)
        .eq("hosted_sha256", sha_value)
        .limit(10)
        .execute()
    )
    rows = _safe_rows(response)
    for row in rows:
        row_id = _get_str(row.get("id"))
        if row_id and row_id != exclude_asset_id:
            return row_id
    return None


def _get_media_asset_row_for_repair(db, asset_id: str) -> dict[str, Any]:
    response = (
        db.schema("core")
        .table("media_assets")
        .select("id,source,source_url,hosted_url,metadata,hosted_sha256,hosted_key,hosted_bucket")
        .eq("id", asset_id)
        .limit(1)
        .execute()
    )
    rows = _safe_rows(response)
    if not rows:
        raise RuntimeError(f"Media asset not found for repair: {asset_id}")
    return rows[0]


def _is_missing_hosted_object_error(exc: Exception) -> bool:
    text = str(exc or "")
    lowered = text.lower()
    if "nosuchkey" in lowered:
        return True
    if "specified key does not exist" in lowered:
        return True
    return False


def _generate_media_asset_variants_resilient(
    db,
    *,
    asset_id: str,
    crop: dict[str, Any] | None,
    force: bool,
    verbose: bool,
) -> None:
    try:
        generate_media_asset_variants(db, asset_id=asset_id, crop=crop, force=force)
        return
    except Exception as exc:
        if not _is_missing_hosted_object_error(exc):
            raise
        asset_row = _get_media_asset_row_for_repair(db, asset_id)
        patch = mirror_media_asset_row(asset_row, force=True)
        if patch:
            _update_media_asset(db, asset_id, patch)
        if verbose:
            print(f"[repair] media_asset {asset_id} remirrored after missing hosted object")
    generate_media_asset_variants(db, asset_id=asset_id, crop=crop, force=force)


def _repair_candidate(db, candidate: RepairCandidate, *, verbose: bool) -> None:
    if candidate.kind == "media_link_asset":
        target_asset_id = candidate.row_id
        patch = mirror_media_asset_row(candidate.row, force=True)
        if patch:
            try:
                _update_media_asset(db, candidate.row_id, patch)
            except RuntimeError as exc:
                duplicate_asset_id = None
                if candidate.link_id and _is_media_asset_source_sha_conflict(str(exc)):
                    duplicate_asset_id = _find_existing_media_asset_id_by_source_sha(
                        db,
                        source=_get_str(patch.get("source")) or candidate.source,
                        hosted_sha256=_get_str(patch.get("hosted_sha256")),
                        exclude_asset_id=candidate.row_id,
                    )
                if duplicate_asset_id and candidate.link_id:
                    _update_media_link_asset(db, candidate.link_id, duplicate_asset_id)
                    target_asset_id = duplicate_asset_id
                    if verbose:
                        print(
                            f"[repair] media_link {candidate.link_id} relinked "
                            f"{candidate.row_id} -> {duplicate_asset_id} (source+sha duplicate)"
                        )
                else:
                    raise
        _generate_media_asset_variants_resilient(
            db,
            asset_id=target_asset_id,
            crop=None,
            force=True,
            verbose=verbose,
        )
        crop = _candidate_crop_payload(candidate)
        if crop:
            _generate_media_asset_variants_resilient(
                db,
                asset_id=target_asset_id,
                crop=crop,
                force=True,
                verbose=verbose,
            )
        if verbose:
            print(f"[repair] media_asset {target_asset_id}")
        return

    patch = mirror_cast_photo_row(candidate.row, force=True)
    if patch:
        _update_cast_photo(db, candidate.row_id, patch)
    generate_cast_photo_variants(db, photo_id=candidate.row_id, crop=None, force=True)
    crop = _candidate_crop_payload(candidate)
    if crop:
        generate_cast_photo_variants(db, photo_id=candidate.row_id, crop=crop, force=True)
    if verbose:
        print(f"[repair] cast_photo {candidate.row_id}")


def _mark_candidate_broken(db, candidate: RepairCandidate, *, reason: str) -> None:
    checked_at = datetime.now(UTC).isoformat()
    if candidate.kind == "media_link_asset":
        context = dict(candidate.link_context or {})
        context["gallery_status"] = "broken_unreachable"
        context["gallery_status_reason"] = reason
        context["gallery_status_checked_at"] = checked_at
        if candidate.link_id:
            _update_media_link_context(db, candidate.link_id, context)
        return

    metadata = dict(candidate.metadata or {})
    metadata["gallery_status"] = "broken_unreachable"
    metadata["gallery_status_reason"] = reason
    metadata["gallery_status_checked_at"] = checked_at
    _update_cast_photo(db, candidate.row_id, {"metadata": metadata})


def repair_gallery_hosts(
    db,
    *,
    allowed_sources: set[str],
    person_ids: list[str],
    show_ids: list[str],
    limit: int | None,
    apply_updates: bool,
    timeout: float,
    retry_attempts: int,
    retry_backoff_ms: int,
    confirm_unreachable_pass: bool,
    verbose: bool,
    progress_every: int = 100,
    checkpoint_file: Path | None = None,
    checkpoint_every: int = 250,
    resume_from_index: int = 0,
    force_flush_progress: bool = True,
    fail_fast_on_apply_error: bool = False,
) -> dict[str, Any]:
    candidates = _collect_candidates(
        db,
        allowed_sources=allowed_sources,
        person_ids=person_ids,
        show_ids=show_ids,
        limit=limit,
    )
    summary: dict[CandidateStatus, int] = {
        "ok": 0,
        "repaired": 0,
        "broken_unreachable": 0,
        "error": 0,
    }
    repaired_ids: list[str] = []
    broken_ids: list[str] = []
    error_ids: list[str] = []
    details: list[dict[str, Any]] = []
    total_candidates = len(candidates)
    effective_start_index = max(0, min(int(resume_from_index), total_candidates))
    scanned_in_run = max(0, total_candidates - effective_start_index)
    progress_interval = max(1, int(progress_every))
    checkpoint_interval = max(1, int(checkpoint_every))
    processed = 0
    last_processed_index = effective_start_index - 1
    start_monotonic = time.monotonic()
    heartbeat_due = scanned_in_run > 0
    aborted_early = False

    if effective_start_index > 0:
        print(
            f"[resume] start_index={effective_start_index} scanned_in_run={scanned_in_run} "
            f"total_candidates={total_candidates}",
            flush=force_flush_progress,
        )

    for idx in range(effective_start_index, total_candidates):
        candidate = candidates[idx]
        processed += 1
        last_processed_index = idx
        operation_stage = "probe_hosted"
        hosted_reason: str | None = None
        source_reason: str | None = None
        confirmation_reason: str | None = None
        abort_after_candidate = False
        try:
            hosted_probe = _probe_url_reachability(
                url=candidate.hosted_url,
                source=candidate.source,
                timeout=timeout,
                source_page_url=candidate.source_page_url,
                retry_attempts=retry_attempts,
                retry_backoff_ms=retry_backoff_ms,
            )
            hosted_reason = hosted_probe.reason
            if hosted_probe.ok:
                summary["ok"] += 1
                details.append(
                    {
                        "id": candidate.row_id,
                        "kind": candidate.kind,
                        "status": "ok",
                        "reason": hosted_probe.reason,
                    }
                )
                continue

            operation_stage = "probe_source"
            source_probe = _probe_url_reachability(
                url=candidate.source_url,
                source=candidate.source,
                timeout=timeout,
                source_page_url=candidate.source_page_url,
                retry_attempts=retry_attempts,
                retry_backoff_ms=retry_backoff_ms,
            )
            source_reason = source_probe.reason
            if source_probe.ok:
                operation_stage = "apply_repair" if apply_updates else "classify_repaired"
                if apply_updates:
                    _repair_candidate(db, candidate, verbose=verbose)
                summary["repaired"] += 1
                repaired_ids.append(candidate.row_id)
                details.append(
                    {
                        "id": candidate.row_id,
                        "kind": candidate.kind,
                        "status": "repaired",
                        "reason": f"hosted={hosted_probe.reason};source={source_probe.reason}",
                    }
                )
                continue

            confirmation_probe: ReachabilityProbeResult | None = None
            if (
                confirm_unreachable_pass
                and not hosted_probe.transient_failure
                and not source_probe.transient_failure
            ):
                operation_stage = "probe_source_confirmation"
                confirmation_probe = _probe_url_reachability(
                    url=candidate.source_url,
                    source=candidate.source,
                    timeout=timeout,
                    source_page_url=candidate.source_page_url,
                    retry_attempts=retry_attempts,
                    retry_backoff_ms=retry_backoff_ms,
                )
                confirmation_reason = confirmation_probe.reason
                if confirmation_probe.ok:
                    operation_stage = "apply_repair_confirmation" if apply_updates else "classify_repaired_confirmation"
                    if apply_updates:
                        _repair_candidate(db, candidate, verbose=verbose)
                    summary["repaired"] += 1
                    repaired_ids.append(candidate.row_id)
                    details.append(
                        {
                            "id": candidate.row_id,
                            "kind": candidate.kind,
                            "status": "repaired",
                            "reason": (
                                "hosted="
                                f"{hosted_probe.reason};source={source_probe.reason};"
                                f"confirm_source={confirmation_probe.reason}"
                            ),
                        }
                    )
                    continue

            if (
                hosted_probe.transient_failure
                or source_probe.transient_failure
                or (confirmation_probe is not None and confirmation_probe.transient_failure)
            ):
                operation_stage = "classify_error_transient"
                reason_parts = [
                    f"hosted={hosted_probe.reason}",
                    f"source={source_probe.reason}",
                ]
                if confirmation_probe is not None:
                    reason_parts.append(f"confirm_source={confirmation_probe.reason}")
                reason_parts.append("classification=indeterminate_transient")
                reason = ";".join(reason_parts)
                summary["error"] += 1
                error_ids.append(candidate.row_id)
                details.append(
                    {
                        "id": candidate.row_id,
                        "kind": candidate.kind,
                        "status": "error",
                        "reason": reason,
                    }
                )
                continue

            operation_stage = "classify_broken_unreachable"
            summary["broken_unreachable"] += 1
            broken_ids.append(candidate.row_id)
            reason_parts = [f"hosted={hosted_probe.reason}", f"source={source_probe.reason}"]
            if confirmation_probe is not None:
                reason_parts.append(f"confirm_source={confirmation_probe.reason}")
            reason = ";".join(reason_parts)
            details.append(
                {
                    "id": candidate.row_id,
                    "kind": candidate.kind,
                    "status": "broken_unreachable",
                    "reason": reason,
                }
            )
            if apply_updates:
                operation_stage = "apply_mark_broken"
                _mark_candidate_broken(db, candidate, reason=reason)
        except Exception as exc:  # pragma: no cover - operational protection
            summary["error"] += 1
            error_ids.append(candidate.row_id)
            error_detail: dict[str, Any] = {
                "id": candidate.row_id,
                "kind": candidate.kind,
                "status": "error",
                "reason": str(exc),
                "operation_stage": operation_stage,
                "exception_type": exc.__class__.__name__,
                "apply": bool(apply_updates),
                "source": candidate.source,
            }
            if hosted_reason:
                error_detail["hosted_probe_reason"] = hosted_reason
            if source_reason:
                error_detail["source_probe_reason"] = source_reason
            if confirmation_reason:
                error_detail["confirmation_probe_reason"] = confirmation_reason
            details.append(error_detail)
            if verbose:
                print(
                    f"[error] {candidate.kind} {candidate.row_id} stage={operation_stage}: {exc}",
                    flush=force_flush_progress,
                )
            if apply_updates and fail_fast_on_apply_error:
                abort_after_candidate = True
                aborted_early = True
        finally:
            if heartbeat_due or (processed % progress_interval == 0):
                _emit_heartbeat(
                    processed=processed,
                    scanned_in_run=scanned_in_run,
                    total_candidates=total_candidates,
                    summary=summary,
                    start_monotonic=start_monotonic,
                    flush=force_flush_progress,
                )
                heartbeat_due = False

            if checkpoint_file is not None and (processed % checkpoint_interval == 0):
                _write_checkpoint(
                    checkpoint_file=checkpoint_file,
                    mode="running",
                    apply_updates=apply_updates,
                    processed=processed,
                    scanned_in_run=scanned_in_run,
                    total_candidates=total_candidates,
                    last_index=last_processed_index,
                    summary=summary,
                    elapsed_seconds=time.monotonic() - start_monotonic,
                    sources=sorted(allowed_sources),
                )
        if abort_after_candidate:
            break

    _write_checkpoint(
        checkpoint_file=checkpoint_file,
        mode="completed",
        apply_updates=apply_updates,
        processed=processed,
        scanned_in_run=scanned_in_run,
        total_candidates=total_candidates,
        last_index=last_processed_index,
        summary=summary,
        elapsed_seconds=time.monotonic() - start_monotonic,
        sources=sorted(allowed_sources),
    )

    return {
        "summary": {
            "scanned": scanned_in_run,
            **summary,
            "apply": bool(apply_updates),
        },
        "run_meta": {
            "resume_from_index": effective_start_index,
            "processed": processed,
            "total_candidates": total_candidates,
            "elapsed_seconds": round(max(0.0, time.monotonic() - start_monotonic), 3),
            "checkpoint_file": str(checkpoint_file) if checkpoint_file else None,
            "aborted_early": bool(aborted_early),
        },
        "sources": sorted(allowed_sources),
        "repaired_ids": repaired_ids,
        "broken_ids": broken_ids,
        "error_ids": error_ids,
        "details": details,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()

    allowed_sources = _split_csv(args.sources)
    if not allowed_sources:
        allowed_sources = set(DEFAULT_ALLOWED_SOURCES)

    checkpoint_file = Path(args.checkpoint_file).expanduser() if args.checkpoint_file else None
    resume_state = (
        _load_resume_state(checkpoint_file)
        if bool(args.resume_from_checkpoint)
        else ResumeState(start_index=0)
    )

    report = repair_gallery_hosts(
        db,
        allowed_sources=allowed_sources,
        person_ids=_coerce_str_list(args.person_id),
        show_ids=_coerce_str_list(args.show_id),
        limit=args.limit,
        apply_updates=bool(args.apply),
        timeout=float(args.timeout),
        retry_attempts=int(args.retry_attempts),
        retry_backoff_ms=int(args.retry_backoff_ms),
        confirm_unreachable_pass=bool(args.confirm_unreachable_pass),
        verbose=bool(args.verbose),
        progress_every=int(args.progress_every),
        checkpoint_file=checkpoint_file,
        checkpoint_every=int(args.checkpoint_every),
        resume_from_index=resume_state.start_index,
        force_flush_progress=bool(args.force_flush_progress),
        fail_fast_on_apply_error=bool(args.fail_fast_on_apply_error),
    )

    print(json.dumps(report["summary"], indent=2))
    if args.output_json:
        path = Path(args.output_json)
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"wrote report to {path}")

    return 0 if report["summary"]["error"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
