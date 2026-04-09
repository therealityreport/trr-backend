from __future__ import annotations

import logging
import threading
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

from trr_backend.db import pg
from trr_backend.integrations.fandom import normalize_fandom_community_domain
from trr_backend.integrations.fandom_discovery import crawl_allpages_directory_entries
from trr_backend.repositories import admin_operations

logger = logging.getLogger(__name__)

_MISSING_TABLE_MARKERS = ("fandom_page_directory", "does not exist", "undefined table")


def _is_missing_table_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    return "fandom_page_directory" in message and (
        "does not exist" in message or "undefined table" in message
    )


def _canonical_allpages_url(*, community_domain: str, review_allpages_url: str | None = None) -> str:
    normalized_domain = normalize_fandom_community_domain(community_domain)
    if not normalized_domain:
        raise ValueError("community_domain is required")

    candidate = str(review_allpages_url or "").strip()
    if candidate:
        parsed = urlparse(candidate)
        host = normalize_fandom_community_domain(str(parsed.hostname or ""))
        if host == normalized_domain and "/wiki/Special:AllPages" in str(parsed.path or ""):
            return candidate
    return f"https://{normalized_domain}/wiki/Special:AllPages"


def has_active_page_directory_entries(community_domain: str) -> bool:
    normalized_domain = normalize_fandom_community_domain(community_domain)
    if not normalized_domain:
        return False
    try:
        row = pg.fetch_one(
            """
            SELECT 1 AS present
            FROM core.fandom_page_directory
            WHERE community_domain = %s
              AND is_active = true
            LIMIT 1
            """,
            [normalized_domain],
        )
    except Exception as exc:  # noqa: BLE001
        if _is_missing_table_error(exc):
            return False
        raise
    return bool(row)


def search_active_page_directory_entries(
    *,
    community_domain: str,
    query_values: Sequence[str],
    limit: int = 50,
) -> list[dict[str, Any]]:
    normalized_domain = normalize_fandom_community_domain(community_domain)
    normalized_queries = [str(value or "").strip() for value in query_values if str(value or "").strip()]
    if not normalized_domain or not normalized_queries:
        return []

    clauses: list[str] = []
    params: list[Any] = [normalized_domain]
    for query in normalized_queries:
        like = "%" + "%".join(query.replace("_", " ").split()) + "%"
        clauses.append(
            "(page_title ILIKE %s OR replace(page_slug, '_', ' ') ILIKE %s OR page_url ILIKE %s)"
        )
        params.extend([like, like, like])
    params.append(max(1, min(int(limit), 200)))

    try:
        rows = pg.fetch_all(
            f"""
            SELECT
              community_domain,
              page_title,
              page_slug,
              page_url,
              source_kind,
              is_active,
              first_seen_at,
              last_seen_at
            FROM core.fandom_page_directory
            WHERE community_domain = %s
              AND is_active = true
              AND ({' OR '.join(clauses)})
            ORDER BY last_seen_at DESC, page_title ASC
            LIMIT %s
            """,
            params,
        )
    except Exception as exc:  # noqa: BLE001
        if _is_missing_table_error(exc):
            return []
        raise
    return [dict(row) for row in (rows or [])]


def get_active_page_directory_entry_by_url(
    *,
    community_domain: str,
    page_url: str,
) -> dict[str, Any] | None:
    normalized_domain = normalize_fandom_community_domain(community_domain)
    normalized_url = str(page_url or "").strip()
    if not normalized_domain or not normalized_url:
        return None

    try:
        row = pg.fetch_one(
            """
            SELECT
              community_domain,
              page_title,
              page_slug,
              page_url,
              source_kind,
              is_active,
              first_seen_at,
              last_seen_at
            FROM core.fandom_page_directory
            WHERE community_domain = %s
              AND page_url = %s
              AND is_active = true
            LIMIT 1
            """,
            [normalized_domain, normalized_url],
        )
    except Exception as exc:  # noqa: BLE001
        if _is_missing_table_error(exc):
            return None
        raise
    return dict(row) if row else None


def upsert_page_directory_entries(
    *,
    community_domain: str,
    entries: Sequence[dict[str, Any]],
    source_kind: str = "allpages_html",
    mark_missing_inactive: bool = False,
) -> dict[str, Any]:
    normalized_domain = normalize_fandom_community_domain(community_domain)
    if not normalized_domain:
        raise ValueError("community_domain is required")

    normalized_entries: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for entry in entries:
        page_url = str(entry.get("page_url") or "").strip()
        page_title = str(entry.get("page_title") or "").strip()
        page_slug = str(entry.get("page_slug") or "").strip()
        if not page_url or not page_title or not page_slug or page_url in seen_urls:
            continue
        seen_urls.add(page_url)
        normalized_entries.append(
            {
                "page_url": page_url,
                "page_title": page_title,
                "page_slug": page_slug,
            }
        )

    for entry in normalized_entries:
        pg.fetch_one(
            """
            INSERT INTO core.fandom_page_directory (
              community_domain,
              page_title,
              page_slug,
              page_url,
              source_kind,
              is_active,
              first_seen_at,
              last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, true, now(), now())
            ON CONFLICT (community_domain, page_url)
            DO UPDATE SET
              page_title = EXCLUDED.page_title,
              page_slug = EXCLUDED.page_slug,
              source_kind = EXCLUDED.source_kind,
              is_active = true,
              last_seen_at = now()
            RETURNING page_url
            """,
            [
                normalized_domain,
                entry["page_title"],
                entry["page_slug"],
                entry["page_url"],
                source_kind,
            ],
        )

    if mark_missing_inactive:
        active_urls = [entry["page_url"] for entry in normalized_entries]
        if active_urls:
            pg.fetch_one(
                """
                UPDATE core.fandom_page_directory
                SET is_active = false
                WHERE community_domain = %s
                  AND is_active = true
                  AND NOT (page_url = ANY(%s::text[]))
                RETURNING community_domain
                """,
                [normalized_domain, active_urls],
            )
        else:
            pg.fetch_one(
                """
                UPDATE core.fandom_page_directory
                SET is_active = false
                WHERE community_domain = %s
                  AND is_active = true
                RETURNING community_domain
                """,
                [normalized_domain],
            )

    return {
        "community_domain": normalized_domain,
        "upserted_count": len(normalized_entries),
        "mark_missing_inactive": bool(mark_missing_inactive),
    }


def enqueue_fandom_page_directory_backfill(
    *,
    community_domain: str,
    review_allpages_url: str | None = None,
    actor: str | None = None,
    reason: str | None = None,
    force: bool = False,
) -> dict[str, Any] | None:
    normalized_domain = normalize_fandom_community_domain(community_domain)
    if not normalized_domain:
        return None
    if not force and has_active_page_directory_entries(normalized_domain):
        return None

    crawl_url = _canonical_allpages_url(
        community_domain=normalized_domain,
        review_allpages_url=review_allpages_url,
    )
    request_payload = {
        "community_domain": normalized_domain,
        "review_allpages_url": crawl_url,
        "reason": str(reason or "").strip() or None,
    }
    operation, attached = admin_operations.create_or_attach_operation(
        operation_type="admin_fandom_page_directory_backfill",
        request_payload=request_payload,
        initiated_by=str(actor or "system"),
        allow_attach=True,
    )
    operation = dict(operation or {})
    operation["attached"] = attached
    operation["community_domain"] = normalized_domain
    operation["crawl_url"] = crawl_url
    if attached:
        return operation

    operation_id = str(operation.get("id") or "").strip()
    if not operation_id:
        return operation

    def _runner() -> None:
        try:
            progress_payload = {
                "community_domain": normalized_domain,
                "crawl_url": crawl_url,
                "status": "running",
            }
            admin_operations.update_operation_status(
                operation_id,
                status="running",
                progress_payload=progress_payload,
            )
            admin_operations.append_operation_event(
                operation_id,
                event_type="progress",
                event_payload=progress_payload,
            )
            entries = crawl_allpages_directory_entries(crawl_url, max_pages=200)
            summary = upsert_page_directory_entries(
                community_domain=normalized_domain,
                entries=entries,
                mark_missing_inactive=True,
            )
            result_payload = {
                **summary,
                "crawl_url": crawl_url,
                "status": "completed",
            }
            admin_operations.append_operation_event(
                operation_id,
                event_type="complete",
                event_payload=result_payload,
            )
            admin_operations.update_operation_status(
                operation_id,
                status="completed",
                result_payload=result_payload,
                progress_payload=result_payload,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "fandom_page_directory_backfill_failed community_domain=%s operation_id=%s",
                normalized_domain,
                operation_id,
            )
            error_payload = {
                "community_domain": normalized_domain,
                "crawl_url": crawl_url,
                "error": str(exc),
            }
            admin_operations.append_operation_event(
                operation_id,
                event_type="error",
                event_payload=error_payload,
            )
            admin_operations.update_operation_status(
                operation_id,
                status="failed",
                error_payload=error_payload,
                progress_payload=error_payload,
            )

    thread = threading.Thread(
        target=_runner,
        name=f"fandom-page-directory-{normalized_domain}",
        daemon=True,
    )
    thread.start()
    return operation
