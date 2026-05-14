#!/usr/bin/env python3
"""Dispatch SocialBlade refreshes for every saved Instagram account handle."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env", override=False)

from trr_backend.db import pg  # noqa: E402
from trr_backend.modal_dispatch import dispatch_socialblade_scrape  # noqa: E402
from trr_backend.socials.socialblade.service import sanitize_socialblade_handle  # noqa: E402


@dataclass(frozen=True)
class SavedInstagramAccount:
    handle: str
    person_id: str | None
    sources: tuple[str, ...]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of handles to dispatch.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print candidate handles without dispatching Modal jobs.",
    )
    parser.add_argument("--force", action="store_true", help="Force refresh even when the stored row is fresh.")
    parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive social.shared_account_sources rows.",
    )
    parser.add_argument(
        "--source",
        default="all_saved_instagram_backfill",
        help="Refresh source label stored with the SocialBlade snapshot.",
    )
    parser.add_argument(
        "--no-scrape-following",
        action="store_true",
        help="Skip the Instagram following-list sidecar.",
    )
    return parser.parse_args()


def _saved_instagram_candidate_rows(*, include_inactive: bool) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        select null::uuid as person_id, account_handle as raw_handle, 'shared_account_sources' as source
        from social.shared_account_sources
        where platform = 'instagram'
          and (%s or is_active = true)
        union all
        select null::uuid as person_id, normalized_username as raw_handle, 'instagram_profiles' as source
        from social.instagram_profiles
        where normalized_username is not null
        union all
        select null::uuid as person_id, source_account as raw_handle, 'instagram_account_catalog_posts' as source
        from social.instagram_account_catalog_posts
        where source_account is not null
        union all
        select null::uuid as person_id, owner_handle_norm as raw_handle, 'social_posts' as source
        from social.social_posts
        where platform = 'instagram'
          and owner_handle_norm is not null
        union all
        select person_id, account_handle as raw_handle, 'socialblade_growth_data' as source
        from pipeline.socialblade_growth_data
        where platform = 'instagram'
        union all
        select
          id as person_id,
          external_ids->>'instagram_id' as raw_handle,
          'people_external_ids.instagram_id' as source
        from core.people
        where external_ids ? 'instagram_id'
        union all
        select id as person_id, external_ids->>'instagram' as raw_handle, 'people_external_ids.instagram' as source
        from core.people
        where external_ids ? 'instagram'
        union all
        select person_id, instagram_handle as raw_handle, 'people_overrides.instagram_handle' as source
        from core.people_overrides
        where instagram_handle is not null
        union all
        select person_id, instagram_id as raw_handle, 'cast_tmdb.instagram_id' as source
        from core.cast_tmdb
        where instagram_id is not null
        """,
        [bool(include_inactive)],
    )


def load_saved_instagram_accounts(*, include_inactive: bool = False) -> list[SavedInstagramAccount]:
    grouped_sources: dict[str, set[str]] = defaultdict(set)
    person_ids: dict[str, set[str]] = defaultdict(set)
    for row in _saved_instagram_candidate_rows(include_inactive=include_inactive):
        handle = sanitize_socialblade_handle(str(row.get("raw_handle") or ""))
        if not handle:
            continue
        grouped_sources[handle].add(str(row.get("source") or "unknown"))
        person_id = str(row.get("person_id") or "").strip()
        if person_id:
            person_ids[handle].add(person_id)

    accounts: list[SavedInstagramAccount] = []
    for handle in sorted(grouped_sources):
        sorted_person_ids = sorted(person_ids.get(handle) or [])
        accounts.append(
            SavedInstagramAccount(
                handle=handle,
                person_id=sorted_person_ids[0] if len(sorted_person_ids) == 1 else None,
                sources=tuple(sorted(grouped_sources[handle])),
            )
        )
    return accounts


def dispatch_backfill(
    accounts: list[SavedInstagramAccount],
    *,
    source: str,
    force: bool,
    scrape_following: bool,
    dry_run: bool,
) -> dict[str, Any]:
    accepted: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for account in accounts:
        if dry_run:
            accepted.append({**asdict(account), "dry_run": True})
            continue
        result = dispatch_socialblade_scrape(
            person_id=account.person_id,
            handle=account.handle,
            source=source,
            force=force,
            platform="instagram",
            scrape_following=scrape_following,
        )
        if not result.get("dispatched"):
            errors.append(
                {
                    **asdict(account),
                    "reason": result.get("reason") or result.get("error") or "dispatch_failed",
                    "dispatch": result,
                }
            )
            continue
        accepted.append({**asdict(account), "call_id": result.get("call_id")})
    return {
        "ok": not errors,
        "dry_run": dry_run,
        "requested": len(accounts),
        "accepted_count": len(accepted),
        "error_count": len(errors),
        "accepted": accepted,
        "errors": errors,
    }


def main() -> int:
    args = _parse_args()
    accounts = load_saved_instagram_accounts(include_inactive=args.include_inactive)
    if args.limit is not None:
        accounts = accounts[: max(0, args.limit)]
    result = dispatch_backfill(
        accounts,
        source=str(args.source or "all_saved_instagram_backfill").strip() or "all_saved_instagram_backfill",
        force=bool(args.force),
        scrape_following=not bool(args.no_scrape_following),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
