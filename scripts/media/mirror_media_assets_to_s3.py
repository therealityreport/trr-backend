#!/usr/bin/env python3
"""
Async S3 mirroring worker for media assets.

Fetches media assets with ingest_status='pending' or 'failed',
downloads from source_url, uploads to S3, and updates hosted_* fields.

Usage:
    python scripts/media/mirror_media_assets_to_s3.py --source tmdb --limit 100 --verbose
    python scripts/media/mirror_media_assets_to_s3.py --status failed --max-retries 3
    python scripts/media/mirror_media_assets_to_s3.py --dry-run --limit 10
"""

from __future__ import annotations

import argparse
import hashlib
import mimetypes
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import requests

from scripts._sync_common import load_env_and_db
from trr_backend.db.session import DbSession
from trr_backend.repositories.media_assets import (
    fetch_assets_for_mirroring,
    update_asset_with_mirror_result,
    update_ingest_status,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _load_allowed_domains() -> set[str]:
    """Load allowed domains from env var."""
    default_domains = "image.tmdb.org,m.media-amazon.com,static.wikia.nocookie.net"
    domains_str = os.getenv("MEDIA_MIRROR_ALLOWED_DOMAINS", default_domains)
    return {d.strip().lower() for d in domains_str.split(",") if d.strip()}


ALLOWED_DOMAINS = _load_allowed_domains()

# S3 configuration from environment
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "")
AWS_CDN_BASE_URL = os.getenv("AWS_CDN_BASE_URL", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Retry configuration
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BACKOFF_HOURS = 1.0


# ---------------------------------------------------------------------------
# Domain allowlist
# ---------------------------------------------------------------------------


def is_allowed_domain(url: str) -> bool:
    """Check if URL domain is allowlisted for mirroring."""
    try:
        parsed = urlparse(url)
        hostname = (parsed.hostname or "").lower()
        return hostname in ALLOWED_DOMAINS
    except Exception:
        return False


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _get_s3_client():
    """Create boto3 S3 client."""
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 mirroring. Install with: pip install boto3") from exc

    return boto3.client("s3", region_name=AWS_REGION)


def _guess_extension(content_type: str | None) -> str:
    """Guess file extension from content type."""
    if not content_type:
        return ""
    ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
    if ext == ".jpe":
        ext = ".jpg"
    return ext or ""


def _build_s3_key(sha256: str, content_type: str | None) -> str:
    """Build content-addressed S3 key."""
    ext = _guess_extension(content_type)
    # Use first 2 chars of hash as prefix for better S3 partitioning
    return f"media/{sha256[:2]}/{sha256}{ext}"


# ---------------------------------------------------------------------------
# Mirror result tracking
# ---------------------------------------------------------------------------


@dataclass
class MirrorResult:
    """Result of mirroring a single asset."""

    asset_id: str
    status: str  # 'hosted', 'failed', 'skipped'
    bytes_transferred: int = 0
    error: str | None = None
    retry_count: int = 0


@dataclass
class MirrorSummary:
    """Summary of a mirroring batch."""

    total: int = 0
    hosted: int = 0
    failed: int = 0
    skipped: int = 0
    bytes_transferred: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core mirroring logic
# ---------------------------------------------------------------------------


def _compute_next_retry_at(retry_count: int, backoff_hours: float) -> str:
    """Compute next retry timestamp with exponential backoff."""
    # Exponential backoff: base_hours * 2^(retry_count-1)
    hours = backoff_hours * (2 ** max(0, retry_count - 1))
    next_retry = datetime.now(UTC) + timedelta(hours=hours)
    return next_retry.isoformat()


def mirror_single_asset(
    db: DbSession,
    asset: dict[str, Any],
    *,
    s3_client: Any,
    bucket: str,
    cdn_base_url: str,
    max_retries: int,
    backoff_hours: float,
    dry_run: bool = False,
    verbose: bool = False,
) -> MirrorResult:
    """
    Mirror a single media asset to S3.

    1. Validate source_url domain
    2. Download content
    3. Compute sha256
    4. Upload to S3
    5. Update database with hosted_* fields
    """
    asset_id = str(asset.get("id") or "")
    source_url = str(asset.get("source_url") or "").strip()
    current_retry_count = int(asset.get("ingest_retry_count") or 0)

    if verbose:
        print(f"  Processing asset {asset_id}: {source_url[:80]}...")

    # Check for missing/empty source_url
    if not source_url:
        error = "source_url is empty or null"
        if verbose:
            print(f"    SKIP: {error}")
        if not dry_run:
            update_ingest_status(db, asset_id, "skipped", error=error)
        return MirrorResult(asset_id=asset_id, status="skipped", error=error)

    # Validate domain
    if not is_allowed_domain(source_url):
        error = f"Domain not in allowlist: {urlparse(source_url).hostname}"
        if verbose:
            print(f"    SKIP: {error}")
        if not dry_run:
            update_ingest_status(db, asset_id, "skipped", error=error)
        return MirrorResult(asset_id=asset_id, status="skipped", error=error)

    # Mark as in_progress
    if not dry_run:
        update_ingest_status(db, asset_id, "in_progress")

    try:
        # Download content
        if verbose:
            print("    Downloading...")

        if dry_run:
            return MirrorResult(asset_id=asset_id, status="hosted", bytes_transferred=0)

        response = requests.get(source_url, timeout=60, stream=True)
        response.raise_for_status()

        content = response.content
        content_type = response.headers.get("content-type", "application/octet-stream")
        content_length = len(content)

        # Compute sha256
        sha256 = hashlib.sha256(content).hexdigest()

        if verbose:
            print(f"    Downloaded {content_length} bytes, sha256={sha256[:16]}...")

        # Build S3 key
        s3_key = _build_s3_key(sha256, content_type)

        # Upload to S3
        if verbose:
            print(f"    Uploading to s3://{bucket}/{s3_key}...")

        s3_response = s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=content,
            ContentType=content_type,
            CacheControl="public, max-age=31536000, immutable",
        )

        etag = s3_response.get("ETag", "").strip('"')
        hosted_url = f"{cdn_base_url.rstrip('/')}/{s3_key}"
        completed_at = datetime.now(UTC).isoformat()

        # Update database
        update_asset_with_mirror_result(
            db,
            asset_id,
            sha256=sha256,
            hosted_bucket=bucket,
            hosted_key=s3_key,
            hosted_url=hosted_url,
            hosted_bytes=content_length,
            hosted_content_type=content_type,
            hosted_etag=etag,
            completed_at=completed_at,
        )

        if verbose:
            print(f"    SUCCESS: {hosted_url}")

        return MirrorResult(asset_id=asset_id, status="hosted", bytes_transferred=content_length)

    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 0

        if status_code == 404:
            # Source not found - skip permanently
            error = f"Source returned 404: {source_url}"
            if verbose:
                print(f"    SKIP (404): {error}")
            if not dry_run:
                update_ingest_status(db, asset_id, "skipped", error=error)
            return MirrorResult(asset_id=asset_id, status="skipped", error=error)

        # Other HTTP error - retryable
        return _handle_retryable_error(
            db,
            asset_id,
            current_retry_count,
            str(exc),
            max_retries=max_retries,
            backoff_hours=backoff_hours,
            dry_run=dry_run,
            verbose=verbose,
        )

    except Exception as exc:
        # General error - retryable
        return _handle_retryable_error(
            db,
            asset_id,
            current_retry_count,
            str(exc),
            max_retries=max_retries,
            backoff_hours=backoff_hours,
            dry_run=dry_run,
            verbose=verbose,
        )


def _handle_retryable_error(
    db: DbSession,
    asset_id: str,
    current_retry_count: int,
    error: str,
    *,
    max_retries: int,
    backoff_hours: float,
    dry_run: bool,
    verbose: bool,
) -> MirrorResult:
    """Handle a retryable error with exponential backoff."""
    new_retry_count = current_retry_count + 1

    if new_retry_count >= max_retries:
        # Max retries exceeded - skip permanently
        skip_error = f"Max retries ({max_retries}) exceeded: {error}"
        if verbose:
            print(f"    SKIP (max retries): {skip_error}")
        if not dry_run:
            update_ingest_status(db, asset_id, "skipped", error=skip_error, retry_count=new_retry_count)
        return MirrorResult(asset_id=asset_id, status="skipped", error=skip_error, retry_count=new_retry_count)

    # Mark as failed with next retry time
    next_retry_at = _compute_next_retry_at(new_retry_count, backoff_hours)
    failed_at = datetime.now(UTC).isoformat()

    if verbose:
        print(f"    FAILED (retry {new_retry_count}/{max_retries}, next: {next_retry_at}): {error}")

    if not dry_run:
        update_ingest_status(
            db,
            asset_id,
            "failed",
            error=error,
            retry_count=new_retry_count,
            failed_at=failed_at,
            next_retry_at=next_retry_at,
        )

    return MirrorResult(asset_id=asset_id, status="failed", error=error, retry_count=new_retry_count)


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------


def process_batch(
    db: DbSession,
    assets: list[dict[str, Any]],
    *,
    s3_client: Any,
    bucket: str,
    cdn_base_url: str,
    max_retries: int,
    backoff_hours: float,
    concurrency: int,
    dry_run: bool,
    verbose: bool,
) -> MirrorSummary:
    """Process a batch of assets with optional concurrency."""
    summary = MirrorSummary(total=len(assets))

    if concurrency <= 1:
        # Sequential processing
        for asset in assets:
            result = mirror_single_asset(
                db,
                asset,
                s3_client=s3_client,
                bucket=bucket,
                cdn_base_url=cdn_base_url,
                max_retries=max_retries,
                backoff_hours=backoff_hours,
                dry_run=dry_run,
                verbose=verbose,
            )
            _update_summary(summary, result)
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(
                    mirror_single_asset,
                    db,
                    asset,
                    s3_client=s3_client,
                    bucket=bucket,
                    cdn_base_url=cdn_base_url,
                    max_retries=max_retries,
                    backoff_hours=backoff_hours,
                    dry_run=dry_run,
                    verbose=verbose,
                ): asset
                for asset in assets
            }

            for future in as_completed(futures):
                try:
                    result = future.result()
                    _update_summary(summary, result)
                except Exception as exc:
                    summary.failed += 1
                    summary.errors.append(str(exc))

    return summary


def _update_summary(summary: MirrorSummary, result: MirrorResult) -> None:
    """Update summary with a single result."""
    if result.status == "hosted":
        summary.hosted += 1
        summary.bytes_transferred += result.bytes_transferred
    elif result.status == "failed":
        summary.failed += 1
        if result.error:
            summary.errors.append(f"{result.asset_id}: {result.error}")
    elif result.status == "skipped":
        summary.skipped += 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mirror_media_assets_to_s3",
        description="Mirror media assets from source URLs to S3.",
    )

    parser.add_argument(
        "--source",
        choices=["tmdb", "imdb_graphql", "imdb_html", "fandom", "user_upload", "all"],
        default="all",
        help="Filter by media source (default: all).",
    )
    parser.add_argument(
        "--status",
        choices=["pending", "failed", "all"],
        default="pending",
        help="Filter by ingest status (default: pending).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of assets to process (default: 100).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Batch size for processing (default: 50).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent workers (default: 5).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Maximum retry attempts before marking as skipped (default: {DEFAULT_MAX_RETRIES}).",
    )
    parser.add_argument(
        "--retry-backoff-hours",
        type=float,
        default=DEFAULT_RETRY_BACKOFF_HOURS,
        help=f"Base hours for exponential backoff (default: {DEFAULT_RETRY_BACKOFF_HOURS}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without making changes (validates domain allowlist, etc.).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])

    # Validate S3 configuration
    if not args.dry_run:
        if not AWS_S3_BUCKET:
            print("ERROR: AWS_S3_BUCKET environment variable is required", file=sys.stderr)
            return 1
        if not AWS_CDN_BASE_URL:
            print("ERROR: AWS_CDN_BASE_URL environment variable is required", file=sys.stderr)
            return 1

    # Load database
    db = load_env_and_db()
    if db is None:
        print("ERROR: Database connection required", file=sys.stderr)
        return 1

    # Create S3 client (skip in dry-run)
    s3_client = None
    if not args.dry_run:
        try:
            s3_client = _get_s3_client()
        except Exception as exc:
            print(f"ERROR: Failed to create S3 client: {exc}", file=sys.stderr)
            return 1

    # Fetch assets
    if args.verbose:
        print(f"Fetching assets: source={args.source} status={args.status} limit={args.limit}")

    assets = fetch_assets_for_mirroring(
        db,
        source=args.source if args.source != "all" else None,
        status=args.status,
        limit=args.limit,
        respect_backoff=True,
    )

    if not assets:
        print("No assets to process.")
        return 0

    print(f"Found {len(assets)} assets to process.")

    if args.dry_run:
        print("DRY RUN: No changes will be made.")

    # Process in batches
    total_summary = MirrorSummary()
    batch_size = min(args.batch_size, len(assets))

    for i in range(0, len(assets), batch_size):
        batch = assets[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(assets) + batch_size - 1) // batch_size

        if args.verbose:
            print(f"\nBatch {batch_num}/{total_batches} ({len(batch)} assets):")

        summary = process_batch(
            db,
            batch,
            s3_client=s3_client,
            bucket=AWS_S3_BUCKET,
            cdn_base_url=AWS_CDN_BASE_URL,
            max_retries=args.max_retries,
            backoff_hours=args.retry_backoff_hours,
            concurrency=args.concurrency,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        total_summary.total += summary.total
        total_summary.hosted += summary.hosted
        total_summary.failed += summary.failed
        total_summary.skipped += summary.skipped
        total_summary.bytes_transferred += summary.bytes_transferred
        total_summary.errors.extend(summary.errors)

    # Print summary
    print(f"\n{'=' * 60}")
    print("MIRROR SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total:      {total_summary.total}")
    print(f"Hosted:     {total_summary.hosted}")
    print(f"Failed:     {total_summary.failed}")
    print(f"Skipped:    {total_summary.skipped}")
    print(f"Bytes:      {total_summary.bytes_transferred:,}")

    if total_summary.errors and args.verbose:
        print(f"\nErrors ({len(total_summary.errors)}):")
        for error in total_summary.errors[:10]:
            print(f"  - {error}")
        if len(total_summary.errors) > 10:
            print(f"  ... and {len(total_summary.errors) - 10} more")

    return 0 if total_summary.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
