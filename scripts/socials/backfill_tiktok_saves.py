#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from typing import Any

import requests

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.utils.env import load_env

_REHYDRATION_RE = re.compile(
    r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
    re.DOTALL,
)
_SIGI_STATE_RE = re.compile(
    r'<script\s+id="SIGI_STATE"[^>]*>(.*?)</script>',
    re.DOTALL,
)


@dataclass(slots=True)
class BackfillCounters:
    scanned: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill TikTok saves (collectCount) for existing season posts.")
    parser.add_argument(
        "--show-name",
        default="The Real Housewives of Salt Lake City",
        help="Show name used with --season-number (default: RHOSLC).",
    )
    parser.add_argument(
        "--season-number",
        type=int,
        default=6,
        help="Season number used with --show-name when --season-id is omitted (default: 6).",
    )
    parser.add_argument("--season-id", default="", help="Explicit season_id UUID override.")
    parser.add_argument("--limit", type=int, default=0, help="Optional max rows to process.")
    parser.add_argument("--delay-seconds", type=float, default=0.4, help="Delay between TikTok requests.")
    parser.add_argument("--dry-run", action="store_true", help="Compute values without writing updates.")
    return parser.parse_args()


def _coerce_non_negative_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float):
        return max(0, int(value))
    text = str(value).strip()
    if not text:
        return 0
    try:
        return max(0, int(text))
    except ValueError:
        return 0


def _resolve_season_id(*, season_id: str, show_name: str, season_number: int) -> str:
    if season_id.strip():
        return season_id.strip()
    row = pg.fetch_one(
        """
        select s.id::text as season_id
        from core.seasons s
        join core.shows sh on sh.id = s.show_id
        where lower(sh.name) = lower(%s)
          and s.season_number = %s
        limit 1
        """,
        [show_name, season_number],
    )
    if not row:
        raise SystemExit(
            f"Season not found for show={show_name!r} season_number={season_number}. Provide --season-id explicitly."
        )
    return str(row.get("season_id") or "").strip()


def _load_candidate_rows(*, season_id: str, limit: int, has_saves_column: bool) -> list[dict[str, Any]]:
    limit_clause = "limit %s" if limit > 0 else ""
    params: list[Any] = [season_id]
    if limit > 0:
        params.append(limit)
    existing_saves_expr = (
        """
        coalesce(
          p.saves,
          case
            when coalesce(p.raw_data ->> 'saves', '') ~ '^-?[0-9]+$'
              then (p.raw_data ->> 'saves')::int
            else 0
          end,
          0
        )
        """
        if has_saves_column
        else """
        coalesce(
          case
            when coalesce(p.raw_data ->> 'saves', '') ~ '^-?[0-9]+$'
              then (p.raw_data ->> 'saves')::int
            else 0
          end,
          0
        )
        """
    )
    return pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.video_id,
          coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as account,
          {existing_saves_expr} as existing_saves
        from social.tiktok_posts p
        where p.season_id = %s
        order by coalesce(p.posted_at, p.scraped_at) desc
        {limit_clause}
        """,
        params,
    )


def _extract_embedded_payload(html: str) -> dict[str, Any] | None:
    for pattern in (_REHYDRATION_RE, _SIGI_STATE_RE):
        match = pattern.search(html or "")
        if not match:
            continue
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def _extract_candidate_item(payload: Any, *, video_id: str) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        item_info = payload.get("itemInfo")
        if isinstance(item_info, dict):
            item_struct = item_info.get("itemStruct")
            if isinstance(item_struct, dict):
                candidate_id = str(item_struct.get("id") or item_struct.get("aweme_id") or "")
                if not video_id or not candidate_id or candidate_id == video_id:
                    return item_struct
        item_module = payload.get("ItemModule")
        if isinstance(item_module, dict):
            if video_id and isinstance(item_module.get(video_id), dict):
                return item_module.get(video_id)
            for value in item_module.values():
                if not isinstance(value, dict):
                    continue
                candidate_id = str(value.get("id") or value.get("aweme_id") or "")
                if candidate_id and candidate_id == video_id:
                    return value
            for value in item_module.values():
                if isinstance(value, dict):
                    return value
        for value in payload.values():
            candidate = _extract_candidate_item(value, video_id=video_id)
            if candidate:
                return candidate
    if isinstance(payload, list):
        for value in payload:
            candidate = _extract_candidate_item(value, video_id=video_id)
            if candidate:
                return candidate
    return None


def _extract_saves_from_item(item: dict[str, Any]) -> int:
    stats = item.get("stats") if isinstance(item.get("stats"), dict) else {}
    stats_v2 = item.get("statsV2") if isinstance(item.get("statsV2"), dict) else {}
    return _coerce_non_negative_int(
        stats_v2.get("collectCount")
        or stats_v2.get("collect_count")
        or stats.get("collectCount")
        or stats.get("collect_count")
        or stats_v2.get("favoriteCount")
        or stats_v2.get("favorite_count")
        or stats.get("favoriteCount")
        or stats.get("favorite_count")
        or item.get("collectCount")
        or item.get("collect_count")
        or item.get("favoriteCount")
        or item.get("favorite_count")
        or 0
    )


def _build_video_url(*, account: str, video_id: str) -> str:
    handle = str(account or "").strip().lstrip("@")
    if handle:
        return f"https://www.tiktok.com/@{handle}/video/{video_id}"
    return f"https://www.tiktok.com/@_/video/{video_id}"


def _fetch_saves(
    *,
    session: requests.Session,
    cookies: dict[str, str],
    video_url: str,
    video_id: str,
    timeout: tuple[int, int] = (10, 45),
) -> tuple[int | None, str | None]:
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "referer": "https://www.tiktok.com/",
    }
    try:
        response = session.get(video_url, headers=headers, cookies=cookies, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        return None, f"request_failed:{exc}"

    payload = _extract_embedded_payload(response.text or "")
    if not payload:
        return None, "embedded_payload_missing"
    item = _extract_candidate_item(payload, video_id=video_id)
    if not item:
        return None, "item_struct_missing"
    return _extract_saves_from_item(item), None


def _update_row(*, row_id: str, saves: int, has_saves_column: bool) -> None:
    if has_saves_column:
        with pg.db_cursor() as cur:
            cur.execute(
                """
                update social.tiktok_posts
                set saves = %s,
                    raw_data = jsonb_set(
                      coalesce(raw_data, '{}'::jsonb),
                      '{saves}',
                      to_jsonb(%s::int),
                      true
                    )
                where id = %s::uuid
                """,
                [saves, saves, row_id],
            )
        return
    with pg.db_cursor() as cur:
        cur.execute(
            """
            update social.tiktok_posts
            set raw_data = jsonb_set(
              coalesce(raw_data, '{}'::jsonb),
              '{saves}',
              to_jsonb(%s::int),
              true
            )
            where id = %s::uuid
            """,
            [saves, row_id],
        )


def main() -> int:
    load_env()
    args = _parse_args()
    has_saves_column = social_repo._platform_posts_has_column("tiktok", "saves")  # noqa: SLF001
    season_id = _resolve_season_id(
        season_id=str(args.season_id or ""),
        show_name=str(args.show_name or ""),
        season_number=max(1, int(args.season_number)),
    )
    rows = _load_candidate_rows(
        season_id=season_id,
        limit=max(0, int(args.limit)),
        has_saves_column=has_saves_column,
    )
    cookies = social_repo._load_tiktok_cookies()  # noqa: SLF001

    counters = BackfillCounters()
    failures: list[dict[str, str]] = []
    session = requests.Session()
    try:
        for row in rows:
            counters.scanned += 1
            row_id = str(row.get("id") or "").strip()
            video_id = str(row.get("video_id") or "").strip()
            account = str(row.get("account") or "").strip()
            existing_saves = _coerce_non_negative_int(row.get("existing_saves"))
            if not row_id or not video_id:
                counters.failed += 1
                failures.append({"row_id": row_id or "?", "video_id": video_id or "?", "reason": "missing_id"})
                continue

            video_url = _build_video_url(account=account, video_id=video_id)
            fetched_saves, fail_reason = _fetch_saves(
                session=session,
                cookies=cookies,
                video_url=video_url,
                video_id=video_id,
            )
            if fetched_saves is None:
                counters.failed += 1
                failures.append(
                    {
                        "row_id": row_id,
                        "video_id": video_id,
                        "reason": str(fail_reason or "unknown"),
                    }
                )
                continue

            if existing_saves == fetched_saves:
                counters.skipped += 1
            else:
                if not args.dry_run:
                    _update_row(row_id=row_id, saves=fetched_saves, has_saves_column=has_saves_column)
                counters.updated += 1

            time.sleep(max(0.0, float(args.delay_seconds)))
    finally:
        session.close()

    print(
        json.dumps(
            {
                "season_id": season_id,
                "show_name": str(args.show_name or ""),
                "season_number": max(1, int(args.season_number)),
                "dry_run": bool(args.dry_run),
                "has_saves_column": bool(has_saves_column),
                "totals": {
                    "scanned": counters.scanned,
                    "updated": counters.updated,
                    "skipped": counters.skipped,
                    "failed": counters.failed,
                },
                "failures_preview": failures[:25],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
