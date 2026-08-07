from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from trr_backend.integrations.picdetective import ReverseImageCandidate, search_by_image_url
from trr_backend.media.image_variants import generate_media_asset_variants
from trr_backend.media.s3_mirror import build_hosted_url, get_s3_bucket, get_s3_client
from trr_backend.scraping.url_image_scraper import (
    ImageCandidate,
    download_and_hash_image,
    scrape_url_for_images,
)

logger = logging.getLogger(__name__)

APPROVED_PUBLIC_DOMAIN_ORDER = (
    "bravotv.com",
    "nbc.com",
    "nbcinsider.com",
    "nbcuni.com",
)
DEFAULT_SEARCH_LIMIT = 5
SEARCH_CANDIDATE_MULTIPLIER = 3
MIN_FINAL_IMAGE_WIDTH = 1080
MIN_SCRAPE_IMAGE_WIDTH = 800


@dataclass(frozen=True)
class ResolvedPublicReplacement:
    page_url: str
    source_domain: str
    image_url: str
    width: int | None
    height: int | None
    page_title: str | None = None
    mode: str = "auto_picdetective_bravo"


def normalize_domain(value: str | None) -> str:
    host = str(value or "").strip().lower()
    if not host:
        return ""
    if "://" in host:
        host = urlparse(host).hostname or ""
    if host.startswith("www."):
        host = host[4:]
    return host


def is_bravo_network_name(value: Any) -> bool:
    normalized = "".join(ch for ch in str(value or "").strip().lower() if ch.isalnum())
    if not normalized:
        return False
    return normalized == "bravo" or normalized == "bravotv" or "bravo" in normalized


def is_approved_public_domain(value: str | None) -> bool:
    host = normalize_domain(value)
    if not host:
        return False
    return any(host == allowed or host.endswith(f".{allowed}") for allowed in APPROVED_PUBLIC_DOMAIN_ORDER)


def _dimension_score(width: int | None, height: int | None) -> int:
    return max(0, int(width or 0)) * max(0, int(height or 0))


def _ratio_delta(
    width: int | None,
    height: int | None,
    expected_width: int | None,
    expected_height: int | None,
) -> float:
    if not width or not height or not expected_width or not expected_height:
        return 0.0
    if width <= 0 or height <= 0 or expected_width <= 0 or expected_height <= 0:
        return 0.0
    return abs((width / height) - (expected_width / expected_height))


def _size_gap(
    width: int | None,
    height: int | None,
    expected_width: int | None,
    expected_height: int | None,
) -> int:
    if not expected_width and not expected_height:
        return 0
    return max(0, int(expected_width or 0) - int(width or 0)) + max(0, int(expected_height or 0) - int(height or 0))


def _domain_rank(domain: str, *, bravo_only: bool) -> int:
    host = normalize_domain(domain)
    for index, allowed in enumerate(APPROVED_PUBLIC_DOMAIN_ORDER):
        if host == allowed or host.endswith(f".{allowed}"):
            return index
    return 999 if bravo_only else 100


def _candidate_sort_key(
    candidate: ReverseImageCandidate,
    *,
    expected_width: int | None,
    expected_height: int | None,
    bravo_only: bool,
) -> tuple[int, int, float, int, int]:
    width = candidate.width or 0
    height = candidate.height or 0
    domain_rank = _domain_rank(candidate.source_domain, bravo_only=bravo_only)
    is_undersized = 0
    if expected_width or expected_height:
        is_undersized = int(width < int(expected_width or 0) or height < int(expected_height or 0))
    return (
        domain_rank,
        is_undersized,
        _ratio_delta(width, height, expected_width, expected_height),
        _size_gap(width, height, expected_width, expected_height),
        -_dimension_score(width, height),
    )


def _scraped_image_sort_key(
    image: ImageCandidate,
    *,
    expected_width: int | None,
    expected_height: int | None,
) -> tuple[int, float, int, int]:
    width = image.width or 0
    height = image.height or 0
    is_undersized = 0
    if expected_width or expected_height:
        is_undersized = int(width < int(expected_width or 0) or height < int(expected_height or 0))
    return (
        is_undersized,
        _ratio_delta(width, height, expected_width, expected_height),
        _size_gap(width, height, expected_width, expected_height),
        -_dimension_score(width, height),
    )


def search_public_replacement_candidates(
    image_url: str,
    *,
    expected_width: int | None = None,
    expected_height: int | None = None,
    bravo_only: bool = False,
    limit: int = DEFAULT_SEARCH_LIMIT,
) -> list[ReverseImageCandidate]:
    cleaned_url = str(image_url or "").strip()
    if not cleaned_url:
        return []

    raw_candidates = search_by_image_url(
        cleaned_url,
        min_width=MIN_FINAL_IMAGE_WIDTH,
        limit=max(limit * SEARCH_CANDIDATE_MULTIPLIER, limit),
    )
    candidates = [
        candidate
        for candidate in raw_candidates
        if not bravo_only or is_approved_public_domain(candidate.source_domain)
    ]
    candidates.sort(
        key=lambda candidate: _candidate_sort_key(
            candidate,
            expected_width=expected_width,
            expected_height=expected_height,
            bravo_only=bravo_only,
        )
    )
    return candidates[:limit]


def resolve_public_replacement_from_page(
    page_url: str,
    *,
    source_domain: str,
    expected_width: int | None = None,
    expected_height: int | None = None,
    bravo_only: bool = False,
) -> ResolvedPublicReplacement | None:
    if bravo_only and not is_approved_public_domain(source_domain):
        return None

    scrape_result = scrape_url_for_images(page_url, min_width=MIN_SCRAPE_IMAGE_WIDTH)
    if not scrape_result.images:
        return None

    ranked_images = sorted(
        (image for image in scrape_result.images if str(image.best_url or "").strip()),
        key=lambda image: _scraped_image_sort_key(
            image,
            expected_width=expected_width,
            expected_height=expected_height,
        ),
    )
    if not ranked_images:
        return None

    best = ranked_images[0]
    if best.width and best.width < MIN_FINAL_IMAGE_WIDTH:
        return None

    return ResolvedPublicReplacement(
        page_url=str(page_url).strip(),
        source_domain=normalize_domain(source_domain),
        image_url=str(best.best_url).strip(),
        width=best.width,
        height=best.height,
        page_title=scrape_result.page_title,
    )


def resolve_best_public_replacement(
    image_url: str,
    *,
    expected_width: int | None = None,
    expected_height: int | None = None,
    bravo_only: bool = False,
    limit: int = DEFAULT_SEARCH_LIMIT,
) -> ResolvedPublicReplacement | None:
    candidates = search_public_replacement_candidates(
        image_url,
        expected_width=expected_width,
        expected_height=expected_height,
        bravo_only=bravo_only,
        limit=limit,
    )
    for candidate in candidates:
        try:
            resolved = resolve_public_replacement_from_page(
                candidate.page_url,
                source_domain=candidate.source_domain,
                expected_width=expected_width or candidate.width,
                expected_height=expected_height or candidate.height,
                bravo_only=bravo_only,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to resolve replacement page %s: %s", candidate.page_url, exc)
            continue
        if resolved is not None:
            return resolved
    return None


def apply_media_asset_replacement(
    db: Any,
    *,
    asset_id: str,
    row: dict[str, Any],
    replacement: ResolvedPublicReplacement,
    resolution_label: str | None = None,
) -> dict[str, Any]:
    image_data, sha256, content_type = download_and_hash_image(replacement.image_url, referer=replacement.page_url)
    if not image_data:
        raise RuntimeError("Downloaded image was empty")

    s3_client = get_s3_client()
    bucket = get_s3_bucket()
    s3_key = f"media-assets/{asset_id}/replaced.jpg"
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=image_data,
        ContentType=content_type,
    )
    hosted_url = build_hosted_url(s3_key)

    metadata_value = row.get("metadata")
    existing_metadata = metadata_value if isinstance(metadata_value, dict) else {}
    getty_metadata = {
        k: v
        for k, v in existing_metadata.items()
        if k
        in (
            "getty",
            "getty_details",
            "getty_tags",
            "getty_event_title",
            "getty_event_url",
            "object_name",
            "editorial_number",
            "people",
            "resolved_people",
            "unmatched_people",
            "tagged_people",
            "people_count",
            "people_names",
            "published_at",
            "show_name",
            "season_number",
            "episode_number",
            "episode_title",
            "content_type",
        )
    }
    new_metadata: dict[str, Any] = {
        **getty_metadata,
        "original_source": str(row.get("source") or "getty"),
        "original_source_url": str(row.get("source_url") or ""),
        "replaced_from": {
            "url": replacement.page_url,
            "domain": replacement.source_domain,
            "image_url": replacement.image_url,
            "width": replacement.width,
            "height": replacement.height,
            "replaced_at": datetime.now(UTC).isoformat(),
            "mode": replacement.mode,
        },
    }
    if resolution_label:
        new_metadata["source_resolution"] = resolution_label
        new_metadata["source_domain"] = replacement.source_domain
        new_metadata["source_page_url"] = replacement.page_url

    update_payload = {
        "source": replacement.source_domain,
        "source_url": replacement.page_url,
        "hosted_url": hosted_url,
        "hosted_key": s3_key,
        "hosted_bucket": bucket,
        "hosted_sha256": sha256,
        "hosted_bytes": len(image_data),
        "hosted_content_type": content_type,
        "width": replacement.width,
        "height": replacement.height,
        "sha256": sha256,
        "metadata": new_metadata,
        "updated_at": datetime.now(UTC).isoformat(),
    }

    db.schema("core").table("media_assets").update(update_payload).eq("id", asset_id).execute()

    try:
        generate_media_asset_variants(db, asset_id=asset_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Variant generation failed after replace for %s: %s", asset_id, exc)

    return {
        "asset_id": asset_id,
        "status": "replaced",
        "new_source": replacement.source_domain,
        "new_source_url": replacement.page_url,
        "new_hosted_url": hosted_url,
        "width": replacement.width,
        "height": replacement.height,
    }
