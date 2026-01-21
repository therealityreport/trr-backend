#!/usr/bin/env python3
"""
Cleanup script for expired media upload sessions.

Finds sessions where:
- expires_at < now()
- status in ('initiated', 'uploaded', 'failed')

Actions:
- Delete temp S3 objects (best-effort)
- Mark sessions as 'expired'
- Log cleanup stats for monitoring

Usage:
    PYTHONPATH=. python scripts/cleanup_expired_media_uploads.py [options]

Options:
    --dry-run       Preview cleanup without making changes
    --limit N       Max sessions to process (default: 1000)
    --verbose       Print detailed logging
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import ClientError

from scripts._sync_common import load_env_and_db
from trr_backend.media.s3_mirror import get_s3_client


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cleanup_expired_media_uploads",
        description="Clean up expired media upload sessions and their temp S3 objects.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview cleanup without making changes.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Max sessions to process (default: 1000).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed logging.",
    )
    return parser.parse_args(argv)


def _fetch_expired_sessions(
    db: Any,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch expired upload sessions that need cleanup."""
    now_iso = datetime.now(UTC).isoformat()

    response = (
        db.schema("core")
        .table("media_uploads")
        .select("id,s3_bucket,s3_temp_key,status,expires_at")
        .in_("status", ["initiated", "uploaded", "failed"])
        .lt("expires_at", now_iso)
        .limit(limit)
        .execute()
    )

    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to fetch expired sessions: {response.error}")

    return response.data or []


def _delete_temp_object(
    s3_client: Any,
    bucket: str,
    key: str,
    *,
    verbose: bool = False,
) -> bool:
    """Delete temp S3 object. Returns True if deleted, False otherwise."""
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        if verbose:
            print(f"  Deleted S3 object: s3://{bucket}/{key}")
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            if verbose:
                print(f"  S3 object already gone: s3://{bucket}/{key}")
            return True  # Already deleted, count as success
        if verbose:
            print(f"  Failed to delete S3 object: s3://{bucket}/{key} - {exc}")
        return False
    except Exception as exc:
        if verbose:
            print(f"  Failed to delete S3 object: s3://{bucket}/{key} - {exc}")
        return False


def _mark_session_expired(
    db: Any,
    session_id: str,
    *,
    verbose: bool = False,
) -> bool:
    """Mark upload session as expired. Returns True on success."""
    try:
        response = (
            db.schema("core")
            .table("media_uploads")
            .update({"status": "expired", "error": "Expired during cleanup"})
            .eq("id", session_id)
            .execute()
        )
        if hasattr(response, "error") and response.error:
            if verbose:
                print(f"  Failed to mark session expired: {session_id} - {response.error}")
            return False
        if verbose:
            print(f"  Marked session as expired: {session_id}")
        return True
    except Exception as exc:
        if verbose:
            print(f"  Failed to mark session expired: {session_id} - {exc}")
        return False


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()

    if args.verbose:
        print(f"cleanup_expired_media_uploads: limit={args.limit} dry_run={args.dry_run}")

    # Fetch expired sessions
    sessions = _fetch_expired_sessions(db, limit=args.limit)

    if not sessions:
        print("cleanup_expired_media_uploads: No expired sessions found.")
        return 0

    print(f"cleanup_expired_media_uploads: Found {len(sessions)} expired sessions.")

    if args.dry_run:
        print("DRY RUN - no changes will be made.")
        for session in sessions:
            print(f"  WOULD cleanup: {session['id']} (status={session['status']}, expires_at={session['expires_at']})")
            print(f"    Temp object: s3://{session['s3_bucket']}/{session['s3_temp_key']}")
        return 0

    # Initialize S3 client
    s3_client = get_s3_client()

    # Process each session
    stats = {
        "processed": 0,
        "s3_deleted": 0,
        "s3_failed": 0,
        "db_updated": 0,
        "db_failed": 0,
    }

    for session in sessions:
        session_id = session["id"]
        bucket = session["s3_bucket"]
        temp_key = session["s3_temp_key"]

        if args.verbose:
            print(f"\nProcessing session: {session_id}")

        stats["processed"] += 1

        # Delete temp S3 object (best-effort)
        if _delete_temp_object(s3_client, bucket, temp_key, verbose=args.verbose):
            stats["s3_deleted"] += 1
        else:
            stats["s3_failed"] += 1

        # Mark session as expired
        if _mark_session_expired(db, session_id, verbose=args.verbose):
            stats["db_updated"] += 1
        else:
            stats["db_failed"] += 1

    # Print summary
    print(
        f"\ncleanup_expired_media_uploads: Complete.\n"
        f"  Processed: {stats['processed']}\n"
        f"  S3 deleted: {stats['s3_deleted']}\n"
        f"  S3 failed: {stats['s3_failed']}\n"
        f"  DB updated: {stats['db_updated']}\n"
        f"  DB failed: {stats['db_failed']}"
    )

    # Return non-zero if any failures
    if stats["db_failed"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
