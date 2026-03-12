#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import requests

from scripts._sync_common import load_env_and_db
from trr_backend.media.image_variants import (
    generate_cast_photo_variants,
    generate_media_asset_variants,
)
from trr_backend.media.s3_mirror import mirror_cast_photo_row, mirror_media_asset_row

TableName = Literal["cast_photos", "media_assets"]
StatusName = Literal["match", "mismatch", "unreachable", "error"]


class SourceUnreachableError(RuntimeError):
    """Source URL could not be downloaded in a deterministic way."""


@dataclass(frozen=True)
class Candidate:
    table: TableName
    row_id: str
    source: str
    source_url: str
    hosted_sha256: str
    row: dict[str, Any]


@dataclass
class CandidateResult:
    table: TableName
    row_id: str
    source: str
    source_url: str
    hosted_sha256: str
    status: StatusName
    source_sha256: str | None
    error: str | None
    repair_applied: bool = False
    repair_error: str | None = None


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="restore_changed_originals",
        description=(
            "Audit and optionally repair rows where hosted/original media integrity changed. "
            "Default scope is IMDb only. "
            "For hosted URL availability repair (403/404), use scripts/media/repair_gallery_hosts.py."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply repair actions for mismatch rows. Default is dry-run.",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="imdb",
        help="Source filter (default: imdb).",
    )
    parser.add_argument(
        "--tables",
        type=str,
        choices=["cast_photos", "media_assets", "both"],
        default="both",
        help="Tables to scan (default: both).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional overall candidate cap for staged runs.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for candidate queries (default: 500).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds for source verification (default: 30).",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Optional file path for JSON report output.",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose progress output.")
    return parser.parse_args(argv)


def _request_headers(*, source: str, source_url: str) -> dict[str, str]:
    source_norm = str(source or "").strip().lower()
    host = urlparse(source_url).netloc.lower()
    headers: dict[str, str] = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }
    if source_norm.startswith("imdb") or "imdb.com" in host or "media-amazon.com" in host:
        headers["Referer"] = "https://www.imdb.com/"
    elif "fandom.com" in host or "wikia.nocookie.net" in host:
        headers["Referer"] = "https://www.fandom.com/"
    return headers


def _download_source_bytes(*, source_url: str, source: str, timeout: float) -> bytes:
    try:
        response = requests.get(
            source_url,
            headers=_request_headers(source=source, source_url=source_url),
            timeout=timeout,
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        raise SourceUnreachableError(f"request_failed: {exc}") from exc
    if response.status_code != 200:
        raise SourceUnreachableError(f"http_status_{response.status_code}")
    data = response.content
    if not data:
        raise RuntimeError("empty_response_body")
    return data


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_rows(response: Any) -> list[dict[str, Any]]:
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error: {response.error}")
    rows = response.data or []
    return rows if isinstance(rows, list) else []


def _get_str(row: dict[str, Any], key: str) -> str | None:
    value = row.get(key)
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    return trimmed or None


def _resolve_cast_source_url(row: dict[str, Any]) -> str | None:
    return _get_str(row, "url") or _get_str(row, "image_url") or _get_str(row, "thumb_url")


def _fetch_cast_candidates(db, *, source: str, batch_size: int) -> list[Candidate]:
    candidates: list[Candidate] = []
    offset = 0
    while True:
        query = (
            db.schema("core")
            .table("cast_photos")
            .select(
                "id,source,url,image_url,thumb_url,source_page_url,hosted_sha256,"
                "hosted_url,hosted_key,hosted_bucket,metadata"
            )
            .eq("source", source)
            .not_.is_("hosted_sha256", "null")
            .order("fetched_at", desc=True)
            .range(offset, offset + batch_size - 1)
        )
        rows = _safe_rows(query.execute())
        if not rows:
            break
        for row in rows:
            row_id = _get_str(row, "id")
            hosted_sha = _get_str(row, "hosted_sha256")
            source_url = _resolve_cast_source_url(row)
            source_value = _get_str(row, "source")
            if not row_id or not hosted_sha or not source_url or not source_value:
                continue
            candidates.append(
                Candidate(
                    table="cast_photos",
                    row_id=row_id,
                    source=source_value,
                    source_url=source_url,
                    hosted_sha256=hosted_sha,
                    row=row,
                )
            )
        offset += len(rows)
    return candidates


def _fetch_gallery_media_asset_ids(db, *, batch_size: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    offset = 0
    while True:
        query = (
            db.schema("core")
            .table("media_links")
            .select("media_asset_id")
            .eq("entity_type", "person")
            .eq("kind", "gallery")
            .not_.is_("media_asset_id", "null")
            .order("created_at", desc=True)
            .range(offset, offset + batch_size - 1)
        )
        rows = _safe_rows(query.execute())
        if not rows:
            break
        for row in rows:
            media_asset_id = _get_str(row, "media_asset_id")
            if not media_asset_id or media_asset_id in seen:
                continue
            seen.add(media_asset_id)
            out.append(media_asset_id)
        offset += len(rows)
    return out


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[idx : idx + size] for idx in range(0, len(values), size)]


def _fetch_media_asset_candidates(db, *, source: str, batch_size: int) -> list[Candidate]:
    asset_ids = _fetch_gallery_media_asset_ids(db, batch_size=batch_size)
    if not asset_ids:
        return []
    candidates: list[Candidate] = []
    for chunk in _chunked(asset_ids, 200):
        query = (
            db.schema("core")
            .table("media_assets")
            .select("id,source,source_url,hosted_sha256,hosted_url,hosted_key,hosted_bucket,metadata")
            .in_("id", chunk)
            .eq("source", source)
            .not_.is_("hosted_sha256", "null")
            .not_.is_("source_url", "null")
        )
        rows = _safe_rows(query.execute())
        for row in rows:
            row_id = _get_str(row, "id")
            hosted_sha = _get_str(row, "hosted_sha256")
            source_url = _get_str(row, "source_url")
            source_value = _get_str(row, "source")
            if not row_id or not hosted_sha or not source_url or not source_value:
                continue
            candidates.append(
                Candidate(
                    table="media_assets",
                    row_id=row_id,
                    source=source_value,
                    source_url=source_url,
                    hosted_sha256=hosted_sha,
                    row=row,
                )
            )
    return candidates


def _update_row(db, *, table: str, row_id: str, patch: dict[str, Any]) -> None:
    response = db.schema("core").table(table).update(patch).eq("id", row_id).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"{table} update failed: {response.error}")


def _verify_candidate(candidate: Candidate, *, timeout: float) -> CandidateResult:
    try:
        source_bytes = _download_source_bytes(
            source_url=candidate.source_url,
            source=candidate.source,
            timeout=timeout,
        )
    except SourceUnreachableError as exc:
        return CandidateResult(
            table=candidate.table,
            row_id=candidate.row_id,
            source=candidate.source,
            source_url=candidate.source_url,
            hosted_sha256=candidate.hosted_sha256,
            status="unreachable",
            source_sha256=None,
            error=str(exc),
        )
    except Exception as exc:
        return CandidateResult(
            table=candidate.table,
            row_id=candidate.row_id,
            source=candidate.source,
            source_url=candidate.source_url,
            hosted_sha256=candidate.hosted_sha256,
            status="error",
            source_sha256=None,
            error=str(exc),
        )

    source_sha = _sha256_hex(source_bytes)
    status: StatusName = "match" if source_sha == candidate.hosted_sha256 else "mismatch"
    return CandidateResult(
        table=candidate.table,
        row_id=candidate.row_id,
        source=candidate.source,
        source_url=candidate.source_url,
        hosted_sha256=candidate.hosted_sha256,
        status=status,
        source_sha256=source_sha,
        error=None,
    )


def _repair_candidate(db, candidate: Candidate) -> bool:
    if candidate.table == "cast_photos":
        patch = mirror_cast_photo_row(candidate.row, force=True)
        if patch:
            _update_row(db, table="cast_photos", row_id=candidate.row_id, patch=patch)
        generate_cast_photo_variants(db, photo_id=candidate.row_id, crop=None, force=True)
        return bool(patch)
    if candidate.table == "media_assets":
        patch = mirror_media_asset_row(candidate.row, force=True)
        if patch:
            _update_row(db, table="media_assets", row_id=candidate.row_id, patch=patch)
        generate_media_asset_variants(db, asset_id=candidate.row_id, crop=None, force=True)
        return bool(patch)
    raise RuntimeError(f"Unsupported table: {candidate.table}")


def _table_filter(table_arg: str) -> tuple[bool, bool]:
    if table_arg == "both":
        return True, True
    if table_arg == "cast_photos":
        return True, False
    if table_arg == "media_assets":
        return False, True
    raise RuntimeError(f"Unsupported table option: {table_arg}")


def restore_changed_originals(
    db,
    *,
    source: str,
    tables: str,
    limit: int | None,
    apply_updates: bool,
    timeout: float,
    batch_size: int,
    verbose: bool,
) -> dict[str, Any]:
    include_cast, include_media = _table_filter(tables)
    candidates: list[Candidate] = []
    if include_cast:
        candidates.extend(_fetch_cast_candidates(db, source=source, batch_size=batch_size))
    if include_media:
        candidates.extend(_fetch_media_asset_candidates(db, source=source, batch_size=batch_size))
    if limit is not None and limit >= 0:
        candidates = candidates[:limit]

    candidate_index = {f"{candidate.table}:{candidate.row_id}": candidate for candidate in candidates}
    results: list[CandidateResult] = []

    for idx, candidate in enumerate(candidates, start=1):
        result = _verify_candidate(candidate, timeout=timeout)
        if apply_updates and result.status == "mismatch":
            try:
                result.repair_applied = _repair_candidate(db, candidate)
            except Exception as exc:  # pragma: no cover - operational path
                result.repair_error = str(exc)
        results.append(result)
        if verbose and (idx % 100 == 0 or idx == len(candidates)):
            print(
                f"[progress] {idx}/{len(candidates)} "
                f"match={sum(1 for r in results if r.status == 'match')} "
                f"mismatch={sum(1 for r in results if r.status == 'mismatch')} "
                f"unreachable={sum(1 for r in results if r.status == 'unreachable')} "
                f"error={sum(1 for r in results if r.status == 'error')}"
            )

    by_table: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    by_source: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    mismatch_ids: dict[str, list[str]] = {"cast_photos": [], "media_assets": []}
    unreachable_ids: dict[str, list[str]] = {"cast_photos": [], "media_assets": []}
    error_ids: dict[str, list[str]] = {"cast_photos": [], "media_assets": []}
    repair_failed_ids: dict[str, list[str]] = {"cast_photos": [], "media_assets": []}

    for result in results:
        by_table[result.table]["scanned"] += 1
        by_table[result.table][result.status] += 1
        by_source[result.source]["scanned"] += 1
        by_source[result.source][result.status] += 1
        if result.repair_applied:
            by_table[result.table]["repaired"] += 1
            by_source[result.source]["repaired"] += 1
        if result.repair_error:
            by_table[result.table]["repair_failed"] += 1
            by_source[result.source]["repair_failed"] += 1
            repair_failed_ids[result.table].append(result.row_id)
        if result.status == "mismatch":
            mismatch_ids[result.table].append(result.row_id)
        if result.status == "unreachable":
            unreachable_ids[result.table].append(result.row_id)
        if result.status == "error":
            error_ids[result.table].append(result.row_id)

    summary = {
        "scanned": len(results),
        "match": sum(1 for r in results if r.status == "match"),
        "mismatch": sum(1 for r in results if r.status == "mismatch"),
        "unreachable": sum(1 for r in results if r.status == "unreachable"),
        "error": sum(1 for r in results if r.status == "error"),
        "repaired": sum(1 for r in results if r.repair_applied),
        "repair_failed": sum(1 for r in results if r.repair_error),
    }

    details = [
        asdict(result) for result in results if result.status != "match" or result.repair_applied or result.repair_error
    ]

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "apply": apply_updates,
        "source": source,
        "tables": tables,
        "limit": limit,
        "summary": summary,
        "by_table": {k: dict(v) for k, v in by_table.items()},
        "by_source": {k: dict(v) for k, v in by_source.items()},
        "mismatch_ids": mismatch_ids,
        "unreachable_ids": unreachable_ids,
        "error_ids": error_ids,
        "repair_failed_ids": repair_failed_ids,
        "details": details,
    }

    # Attach row count context so dry-run artifacts can be triaged by ID quickly.
    report["candidate_count"] = len(candidates)
    report["candidates_by_table"] = {
        "cast_photos": sum(1 for candidate in candidates if candidate.table == "cast_photos"),
        "media_assets": sum(1 for candidate in candidates if candidate.table == "media_assets"),
    }
    report["candidate_index_size"] = len(candidate_index)
    return report


def _print_report(report: dict[str, Any]) -> None:
    summary = report["summary"]
    mode = "apply" if report.get("apply") else "dry-run"
    print(
        f"restore_changed_originals ({mode}) source={report.get('source')} tables={report.get('tables')} "
        f"scanned={summary.get('scanned', 0)} match={summary.get('match', 0)} "
        f"mismatch={summary.get('mismatch', 0)} unreachable={summary.get('unreachable', 0)} "
        f"error={summary.get('error', 0)} repaired={summary.get('repaired', 0)} "
        f"repair_failed={summary.get('repair_failed', 0)}"
    )
    by_table = report.get("by_table", {})
    if by_table:
        print("By table:")
        for table_name in sorted(by_table):
            stats = by_table[table_name]
            print(
                f"  - {table_name}: scanned={stats.get('scanned', 0)} "
                f"match={stats.get('match', 0)} mismatch={stats.get('mismatch', 0)} "
                f"unreachable={stats.get('unreachable', 0)} error={stats.get('error', 0)} "
                f"repaired={stats.get('repaired', 0)} repair_failed={stats.get('repair_failed', 0)}"
            )


def _write_json_report(path_value: str, report: dict[str, Any]) -> None:
    output_path = Path(path_value).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(f"JSON report written: {output_path}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()
    if db is None:
        print("[error] Unable to initialize database session", file=sys.stderr)
        return 2

    report = restore_changed_originals(
        db,
        source=str(args.source).strip().lower() or "imdb",
        tables=str(args.tables).strip(),
        limit=max(0, int(args.limit)) if args.limit is not None else None,
        apply_updates=bool(args.apply),
        timeout=max(1.0, float(args.timeout)),
        batch_size=max(1, int(args.batch_size)),
        verbose=bool(args.verbose),
    )
    _print_report(report)
    if args.output_json:
        _write_json_report(str(args.output_json), report)
    if int(report.get("summary", {}).get("repair_failed", 0)) > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
