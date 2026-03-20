#!/usr/bin/env python3
"""Trigger one SocialBlade refresh and confirm the persisted row updates."""

from __future__ import annotations

import argparse
import json

from trr_backend.modal_dispatch import dispatch_socialblade_scrape_sync
from trr_backend.repositories.socialblade_growth import get_growth_data
from trr_backend.socials.socialblade.service import SocialBladeRefreshError, refresh_and_persist_socialblade


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--person-id", required=True, help="Target person UUID")
    parser.add_argument("--handle", required=True, help="Instagram handle")
    parser.add_argument("--force", action="store_true", help="Bypass freshness reuse")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    before = get_growth_data(args.person_id, args.handle)
    before_scraped_at = before.get("scraped_at") if before else None
    try:
        after = refresh_and_persist_socialblade(
            person_id=args.person_id,
            handle=args.handle,
            scraper=lambda safe_handle: dispatch_socialblade_scrape_sync(handle=safe_handle),
            source="smoke_test",
            force=args.force,
        )
    except SocialBladeRefreshError as exc:
        raise RuntimeError(str(exc)) from exc

    print(
        json.dumps(
            {
                "ok": True,
                "person_id": args.person_id,
                "handle": args.handle,
                "before_scraped_at": before_scraped_at,
                "after_scraped_at": after.get("scraped_at"),
                "refresh_status": after.get("refresh_status"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
