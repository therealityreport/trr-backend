#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
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
    db.schema("core").table("media_assets").update(patch).eq("id", asset_id).execute()


def _update_cast_photo(db, photo_id: str, patch: dict[str, Any]) -> None:
    db.schema("core").table("cast_photos").update(patch).eq("id", photo_id).execute()


def _update_media_link_context(db, link_id: str, context: dict[str, Any]) -> None:
    db.schema("core").table("media_links").update({"context": context}).eq("id", link_id).execute()


def _repair_candidate(db, candidate: RepairCandidate, *, verbose: bool) -> None:
    if candidate.kind == "media_link_asset":
        patch = mirror_media_asset_row(candidate.row, force=True)
        if patch:
            _update_media_asset(db, candidate.row_id, patch)
        generate_media_asset_variants(db, asset_id=candidate.row_id, crop=None, force=True)
        crop = _candidate_crop_payload(candidate)
        if crop:
            generate_media_asset_variants(db, asset_id=candidate.row_id, crop=crop, force=True)
        if verbose:
            print(f"[repair] media_asset {candidate.row_id}")
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
    verbose: bool,
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

    for candidate in candidates:
        try:
            hosted_ok, hosted_reason = _check_url_reachability(
                url=candidate.hosted_url,
                source=candidate.source,
                timeout=timeout,
                source_page_url=candidate.source_page_url,
            )
            if hosted_ok:
                summary["ok"] += 1
                details.append(
                    {
                        "id": candidate.row_id,
                        "kind": candidate.kind,
                        "status": "ok",
                        "reason": hosted_reason,
                    }
                )
                continue

            source_ok, source_reason = _check_url_reachability(
                url=candidate.source_url,
                source=candidate.source,
                timeout=timeout,
                source_page_url=candidate.source_page_url,
            )
            if source_ok:
                summary["repaired"] += 1
                repaired_ids.append(candidate.row_id)
                details.append(
                    {
                        "id": candidate.row_id,
                        "kind": candidate.kind,
                        "status": "repaired",
                        "reason": hosted_reason,
                    }
                )
                if apply_updates:
                    _repair_candidate(db, candidate, verbose=verbose)
                continue

            summary["broken_unreachable"] += 1
            broken_ids.append(candidate.row_id)
            reason = f"hosted={hosted_reason};source={source_reason}"
            details.append(
                {
                    "id": candidate.row_id,
                    "kind": candidate.kind,
                    "status": "broken_unreachable",
                    "reason": reason,
                }
            )
            if apply_updates:
                _mark_candidate_broken(db, candidate, reason=reason)
        except Exception as exc:  # pragma: no cover - operational protection
            summary["error"] += 1
            error_ids.append(candidate.row_id)
            details.append(
                {
                    "id": candidate.row_id,
                    "kind": candidate.kind,
                    "status": "error",
                    "reason": str(exc),
                }
            )
            if verbose:
                print(f"[error] {candidate.kind} {candidate.row_id}: {exc}")

    return {
        "summary": {
            "scanned": len(candidates),
            **summary,
            "apply": bool(apply_updates),
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

    report = repair_gallery_hosts(
        db,
        allowed_sources=allowed_sources,
        person_ids=_coerce_str_list(args.person_id),
        show_ids=_coerce_str_list(args.show_id),
        limit=args.limit,
        apply_updates=bool(args.apply),
        timeout=float(args.timeout),
        verbose=bool(args.verbose),
    )

    print(json.dumps(report["summary"], indent=2))
    if args.output_json:
        path = Path(args.output_json)
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"wrote report to {path}")

    return 0 if report["summary"]["error"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
