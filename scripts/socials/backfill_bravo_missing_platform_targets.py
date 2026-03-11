#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.utils.env import load_env

TARGET_PLATFORMS = {"instagram", "tiktok", "twitter", "facebook", "threads"}
CORE_ACCOUNT_PLATFORMS = set(social_repo._BRAVO_CORE_PLATFORM_ACCOUNTS)


@dataclass(slots=True)
class BackfillCounters:
    seasons_scanned: int = 0
    seasons_with_rows: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_skipped_existing: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing Bravo season_targets rows and repair stale core-platform accounts."
    )
    parser.add_argument("--season-id", default="", help="Optional season UUID filter.")
    parser.add_argument(
        "--updated-by",
        default="system:backfill-bravo-missing-platform-targets",
        help="updated_by value for inserted/updated rows.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute inserts without writing.")
    return parser.parse_args()


def _load_candidate_seasons(*, season_id: str) -> list[dict[str, Any]]:
    params: list[Any] = [season_id]
    return pg.fetch_all(
        """
        select
          s.id::text as season_id,
          s.show_id::text as show_id,
          sh.name as show_name,
          s.season_number
        from core.seasons s
        join core.shows sh on sh.id = s.show_id
        where (%s = '' or s.id::text = %s)
          and exists (
            select 1
            from social.season_targets t
            where t.season_id = s.id
              and t.source_scope = 'bravo'
          )
        order by s.season_number asc, s.id asc
        """,
        params + [season_id],
    )


def _load_existing_platform_rows(*, season_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select
          platform,
          is_active,
          accounts
        from social.season_targets
        where season_id = %s::uuid
          and source_scope = 'bravo'
        """,
        [season_id],
    )


def _normalize_accounts(accounts: list[Any] | None) -> list[str]:
    return social_repo._normalize_unique_terms(  # noqa: SLF001
        [
            normalized
            for normalized in (social_repo._normalize_account_handle(item) for item in (accounts or []))  # noqa: SLF001
            if normalized
        ]
    )


def _insert_target_row(
    *,
    season_id: str,
    show_id: str,
    target: dict[str, Any],
    updated_by: str,
) -> None:
    platform = str(target.get("platform") or "").strip().lower()
    with pg.db_cursor() as cur:
        cur.execute(
            """
            insert into social.season_targets (
              season_id,
              show_id,
              platform,
              source_scope,
              timezone,
              accounts,
              hashtags,
              keywords,
              is_active,
              config,
              updated_by,
              updated_at
            )
            select
              %s::uuid,
              %s::uuid,
              %s,
              'bravo',
              %s,
              %s::jsonb,
              %s::jsonb,
              %s::jsonb,
              %s,
              %s::jsonb,
              %s,
              %s
            where not exists (
              select 1
              from social.season_targets t
              where t.season_id = %s::uuid
                and t.platform = %s
                and t.source_scope = 'bravo'
            )
            """,
            [
                season_id,
                show_id,
                platform,
                str(target.get("timezone") or "America/New_York"),
                json.dumps(target.get("accounts") or []),
                json.dumps(target.get("hashtags") or []),
                json.dumps(target.get("keywords") or []),
                bool(target.get("is_active", True)),
                json.dumps(target.get("config") or {}),
                updated_by,
                datetime.now(tz=UTC),
                season_id,
                platform,
            ],
        )


def _update_target_accounts(
    *,
    season_id: str,
    platform: str,
    accounts: list[str],
    updated_by: str | None,
) -> None:
    with pg.db_cursor() as cur:
        cur.execute(
            """
            update social.season_targets
            set
              accounts = %s::jsonb,
              updated_by = %s,
              updated_at = %s
            where season_id = %s::uuid
              and platform = %s
              and source_scope = 'bravo'
            """,
            [
                json.dumps(accounts),
                updated_by,
                datetime.now(tz=UTC),
                season_id,
                platform,
            ],
        )


def main() -> int:
    load_env()
    args = _parse_args()
    season_id = str(args.season_id or "").strip()

    rows = _load_candidate_seasons(season_id=season_id)
    counters = BackfillCounters()
    by_season: dict[str, dict[str, Any]] = {}

    for season_row in rows:
        counters.seasons_scanned += 1
        sid = str(season_row.get("season_id") or "").strip()
        show_id = str(season_row.get("show_id") or "").strip()
        if not sid or not show_id:
            continue

        existing_rows = _load_existing_platform_rows(season_id=sid)
        if not existing_rows:
            continue

        counters.seasons_with_rows += 1
        existing_by_platform = {
            str(row.get("platform") or "").strip().lower(): row
            for row in existing_rows
            if str(row.get("platform") or "").strip()
        }
        present_platforms = set(existing_by_platform)

        context = social_repo.SeasonContext(
            season_id=sid,
            show_id=show_id,
            show_name=season_row.get("show_name"),
            season_number=int(season_row.get("season_number") or 0),
            anchor_date=social_repo.get_season_context(sid).anchor_date,
        )
        defaults = social_repo._default_targets(context, source_scope="bravo")  # noqa: SLF001
        default_map = {
            str(item.get("platform") or "").strip().lower(): item
            for item in defaults
            if str(item.get("platform") or "").strip().lower() in TARGET_PLATFORMS
        }

        inserted_platforms: list[str] = []
        updated_platforms: list[dict[str, Any]] = []
        for platform in sorted(TARGET_PLATFORMS):
            existing_row = existing_by_platform.get(platform)
            if existing_row is None:
                target = default_map.get(platform)
                if not target:
                    continue
                if not args.dry_run:
                    _insert_target_row(
                        season_id=sid,
                        show_id=show_id,
                        target=target,
                        updated_by=str(args.updated_by or "").strip() or None,
                    )
                counters.rows_inserted += 1
                inserted_platforms.append(platform)
                continue

            if platform in CORE_ACCOUNT_PLATFORMS:
                current_accounts = _normalize_accounts(existing_row.get("accounts") or [])
                repaired_accounts = social_repo._ensure_bravo_core_platform_accounts(  # noqa: SLF001
                    source_scope="bravo",
                    platform=platform,
                    accounts=current_accounts,
                )
                if repaired_accounts != current_accounts:
                    if not args.dry_run:
                        _update_target_accounts(
                            season_id=sid,
                            platform=platform,
                            accounts=repaired_accounts,
                            updated_by=str(args.updated_by or "").strip() or None,
                        )
                    counters.rows_updated += 1
                    updated_platforms.append(
                        {
                            "platform": platform,
                            "accounts_before": current_accounts,
                            "accounts_after": repaired_accounts,
                        }
                    )
                    continue

            counters.rows_skipped_existing += 1

        by_season[sid] = {
            "show_id": show_id,
            "season_number": int(season_row.get("season_number") or 0),
            "inserted_platforms": inserted_platforms,
            "updated_platforms": updated_platforms,
            "existing_platforms": sorted(present_platforms),
        }

    print(
        json.dumps(
            {
                "season_id": season_id or None,
                "dry_run": bool(args.dry_run),
                "target_platforms": sorted(TARGET_PLATFORMS),
                "totals": {
                    "seasons_scanned": counters.seasons_scanned,
                    "seasons_with_existing_bravo_rows": counters.seasons_with_rows,
                    "rows_inserted": counters.rows_inserted,
                    "rows_updated": counters.rows_updated,
                    "rows_skipped_existing": counters.rows_skipped_existing,
                },
                "by_season": by_season,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
