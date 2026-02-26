#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.media.s3_mirror import (
    build_hosted_url,
    build_show_image_s3_key,
    download_image,
    ensure_logo_png_bytes,
    get_s3_bucket,
    get_s3_client,
    mirror_logo_monochrome_variants_row,
    mirror_show_image_row,
    upload_bytes_to_s3,
)
from trr_backend.repositories.media_assets import update_asset_with_mirror_result
from trr_backend.repositories.web_scrape_images import (
    create_media_asset_from_scrape,
    create_media_link_for_entity,
    find_asset_by_sha256,
)
from trr_backend.utils.env import load_env

_WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{item_id}.json"
_REQUEST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "TRR-Backend/1.0",
}
_SKIP_IMAGE_RE = re.compile(r"favicon|sprite|icon|avatar|badge|blank", re.IGNORECASE)
_LOGO_HINT_RE = re.compile(r"logo|wordmark|brand|title", re.IGNORECASE)
_SHOW_NAME_TOKEN_RE = re.compile(r"[a-z0-9]{4,}")


@dataclass
class SyncSummary:
    show_logos_discovered: int = 0
    show_logos_imported: int = 0
    show_logos_skipped: int = 0
    show_logo_failures: int = 0
    failures: int = 0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_show_logos",
        description="Harvest show logo candidates from show homepage and Wikipedia URLs.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Accepted for parity; script processes used shows by default.",
    )
    parser.add_argument("--force", action="store_true", help="Re-link/import even when logo links already exist.")
    parser.add_argument("--dry-run", action="store_true", help="Collect and score candidates without writing.")
    parser.add_argument("--skip-s3", action="store_true", help="Skip downloading + mirroring candidate logos.")
    parser.add_argument(
        "--backfill-existing",
        action="store_true",
        help="Backfill PNG normalization + black/white variants for existing show logos.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum shows to process.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args(argv)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _iter_rows_paged(query, *, page_size: int = 1000):
    start = 0
    while True:
        response = query.range(start, start + page_size - 1).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase paging error: {response.error}")
        rows = response.data or []
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if isinstance(row, dict):
                yield row
        if len(rows) < page_size:
            break
        start += page_size


def _extract_enwiki_url(entity: dict[str, Any]) -> str | None:
    sitelinks = entity.get("sitelinks")
    if not isinstance(sitelinks, dict):
        return None
    enwiki = sitelinks.get("enwiki")
    if not isinstance(enwiki, dict):
        return None
    title = _normalize_text(enwiki.get("title"))
    if not title:
        return None
    return f"https://en.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"


def _resolve_show_wikipedia_url(wikidata_id: str | None) -> str | None:
    item_id = _normalize_text(wikidata_id).upper()
    if not item_id:
        return None
    try:
        response = requests.get(
            _WIKIDATA_ENTITY_URL.format(item_id=item_id),
            headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException:
        return None
    entities = payload.get("entities")
    if not isinstance(entities, dict):
        return None
    entity = entities.get(item_id)
    if not isinstance(entity, dict):
        return None
    return _extract_enwiki_url(entity)


def _score_candidate_url(*, url: str, attrs_text: str, is_meta: bool) -> int:
    score = 0
    lowered = url.casefold()
    if _LOGO_HINT_RE.search(lowered):
        score += 50
    if _LOGO_HINT_RE.search(attrs_text):
        score += 30
    if "wikipedia.org" in lowered:
        score += 10
    if is_meta:
        score += 5
    return score


def _extract_logo_candidates_from_html(*, html: str, page_url: str, limit: int) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    scored: list[tuple[int, str]] = []
    source_host = (urlparse(page_url).hostname or "").lower()
    is_wikipedia_source = source_host.endswith("wikipedia.org")

    def add_candidate(raw_url: str | None, attrs_text: str, *, is_meta: bool) -> None:
        url = _normalize_text(raw_url)
        if not url:
            return
        absolute = urljoin(page_url, url)
        if not absolute.startswith("http://") and not absolute.startswith("https://"):
            return
        candidate_host = (urlparse(absolute).hostname or "").lower()
        # Show-brand logo sync is intentionally strict:
        # only ingest Wikipedia-hosted media files from Wikipedia pages.
        if not is_wikipedia_source:
            return
        if candidate_host != "upload.wikimedia.org":
            return
        if _SKIP_IMAGE_RE.search(absolute):
            return
        score = _score_candidate_url(url=absolute, attrs_text=attrs_text, is_meta=is_meta)
        scored.append((score, absolute))

    for selector in (
        ("meta", {"property": "og:image"}),
        ("meta", {"name": "og:image"}),
        ("meta", {"name": "twitter:image"}),
    ):
        tag = soup.find(selector[0], attrs=selector[1])
        if tag is None:
            continue
        add_candidate(tag.get("content"), "meta", is_meta=True)

    for image in soup.find_all("img"):
        src = image.get("src") or image.get("data-src") or image.get("data-original")
        attrs = " ".join(
            [
                _normalize_text(image.get("alt")),
                _normalize_text(image.get("class")),
                _normalize_text(image.get("id")),
                _normalize_text(image.get("title")),
            ]
        ).casefold()
        add_candidate(src, attrs, is_meta=False)

    scored.sort(key=lambda item: (-item[0], item[1]))
    out: list[str] = []
    seen: set[str] = set()
    for _score, url in scored:
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
        if len(out) >= limit:
            break
    return out


def _collect_source_urls(show_row: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    wikipedia_url = _resolve_show_wikipedia_url(_normalize_text(show_row.get("wikidata_id")) or None)
    if (
        wikipedia_url
        and _is_likely_matching_show_page(show_row=show_row, page_url=wikipedia_url)
        and wikipedia_url not in seen
    ):
        seen.add(wikipedia_url)
        urls.append(wikipedia_url)

    return urls[:1]


def _is_likely_matching_show_page(*, show_row: dict[str, Any], page_url: str) -> bool:
    """
    Guard against bad wikidata bindings by requiring token overlap between
    the show name and resolved page URL slug.
    """
    show_name = _normalize_text(show_row.get("name")).lower()
    if not show_name:
        return True

    path_slug = (urlparse(page_url).path or "").replace("/", " ").replace("_", " ").lower()
    if not path_slug:
        return True

    show_tokens = set(_SHOW_NAME_TOKEN_RE.findall(show_name))
    if not show_tokens:
        return True
    page_tokens = set(_SHOW_NAME_TOKEN_RE.findall(path_slug))
    if not page_tokens:
        return False

    return len(show_tokens.intersection(page_tokens)) >= 2


def _load_existing_show_logo_asset_ids(db, *, show_id: str) -> set[str]:
    out: set[str] = set()
    query = (
        db.schema("core")
        .table("media_links")
        .select("media_asset_id")
        .eq("entity_type", "show")
        .eq("entity_id", show_id)
        .eq("kind", "logo")
    )
    for row in _iter_rows_paged(query):
        asset_id = _normalize_text(row.get("media_asset_id"))
        if asset_id:
            out.add(asset_id)
    return out


def _extract_logo_variant_urls_from_metadata(metadata: Any) -> tuple[str | None, str | None]:
    if not isinstance(metadata, dict):
        return None, None
    black = _normalize_text(metadata.get("logo_black_url")) or _normalize_text(metadata.get("hosted_logo_black_url"))
    white = _normalize_text(metadata.get("logo_white_url")) or _normalize_text(metadata.get("hosted_logo_white_url"))
    return black or None, white or None


def _logo_variant_metadata_from_patch(patch: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    black_url = _normalize_text(patch.get("hosted_logo_black_url"))
    white_url = _normalize_text(patch.get("hosted_logo_white_url"))
    if black_url:
        out["logo_black_url"] = black_url
        out["hosted_logo_black_url"] = black_url
    if white_url:
        out["logo_white_url"] = white_url
        out["hosted_logo_white_url"] = white_url
    for key in (
        "hosted_logo_black_key",
        "hosted_logo_black_sha256",
        "hosted_logo_white_key",
        "hosted_logo_white_sha256",
    ):
        if patch.get(key) is not None:
            out[key] = patch[key]
    return out


def _is_png_hosted(*, hosted_key: str | None, hosted_content_type: str | None) -> bool:
    key = _normalize_text(hosted_key).casefold()
    content_type = _normalize_text(hosted_content_type).casefold()
    if key.endswith(".png"):
        return True
    return content_type == "image/png"


def _update_media_asset_metadata(db, *, asset_id: str, metadata: dict[str, Any]) -> None:
    response = (
        db.schema("core")
        .table("media_assets")
        .update({"metadata": metadata, "updated_at": datetime.now(UTC).isoformat()})
        .eq("id", asset_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to update media asset metadata for {asset_id}: {response.error}")


def _update_show_image_metadata(db, *, image_id: str, metadata: dict[str, Any]) -> None:
    response = (
        db.schema("core")
        .table("show_images")
        .update({"metadata": metadata, "updated_at": datetime.now(UTC).isoformat()})
        .eq("id", image_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Failed to update show image metadata for {image_id}: {response.error}")


def _ensure_media_asset_logo_variants(
    db,
    *,
    show_id: str,
    asset_id: str,
    hosted_url: str,
    metadata: dict[str, Any] | None,
    s3_client,
) -> dict[str, Any]:
    metadata_out = dict(metadata) if isinstance(metadata, dict) else {}
    existing_black, existing_white = _extract_logo_variant_urls_from_metadata(metadata_out)
    variant_result = None
    try:
        variant_result = mirror_logo_monochrome_variants_row(
            {
                "id": show_id,
                "hosted_logo_black_url": existing_black,
                "hosted_logo_white_url": existing_white,
            },
            kind="shows",
            id_field="id",
            source_url=hosted_url,
            force=False,
            s3_client=s3_client,
            source="show_logo_sync",
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"WARN show={show_id} asset={asset_id} logo_variant_generation_failed={exc}",
            file=sys.stderr,
        )

    changed = False
    if variant_result and isinstance(variant_result.patch, dict):
        metadata_patch = _logo_variant_metadata_from_patch(variant_result.patch)
        if metadata_patch:
            metadata_out.update(metadata_patch)
            changed = True
    else:
        if existing_black and "logo_black_url" not in metadata_out:
            metadata_out["logo_black_url"] = existing_black
            changed = True
        if existing_white and "logo_white_url" not in metadata_out:
            metadata_out["logo_white_url"] = existing_white
            changed = True

    if changed:
        try:
            _update_media_asset_metadata(db, asset_id=asset_id, metadata=metadata_out)
        except Exception as exc:  # noqa: BLE001
            print(
                f"WARN show={show_id} asset={asset_id} logo_variant_metadata_update_failed={exc}",
                file=sys.stderr,
            )
    return metadata_out


def _ensure_show_image_logo_variants(
    db,
    *,
    image_row: dict[str, Any],
    hosted_url: str,
    metadata: dict[str, Any] | None,
    s3_client,
) -> dict[str, Any]:
    show_id = _normalize_text(image_row.get("show_id"))
    if not show_id:
        return dict(metadata) if isinstance(metadata, dict) else {}
    metadata_out = dict(metadata) if isinstance(metadata, dict) else {}
    existing_black, existing_white = _extract_logo_variant_urls_from_metadata(metadata_out)
    variant_result = None
    try:
        variant_result = mirror_logo_monochrome_variants_row(
            {
                "id": show_id,
                "hosted_logo_black_url": existing_black,
                "hosted_logo_white_url": existing_white,
            },
            kind="shows",
            id_field="id",
            source_url=hosted_url,
            force=False,
            s3_client=s3_client,
            source="show_logo_sync",
        )
    except Exception as exc:  # noqa: BLE001
        print(
            "WARN "
            f"show={show_id} "
            f"show_image={_normalize_text(image_row.get('id'))} "
            f"logo_variant_generation_failed={exc}",
            file=sys.stderr,
        )

    changed = False
    if variant_result and isinstance(variant_result.patch, dict):
        metadata_patch = _logo_variant_metadata_from_patch(variant_result.patch)
        if metadata_patch:
            metadata_out.update(metadata_patch)
            changed = True
    else:
        if existing_black and "logo_black_url" not in metadata_out:
            metadata_out["logo_black_url"] = existing_black
            changed = True
        if existing_white and "logo_white_url" not in metadata_out:
            metadata_out["logo_white_url"] = existing_white
            changed = True

    image_id = _normalize_text(image_row.get("id"))
    if changed and image_id:
        try:
            _update_show_image_metadata(db, image_id=image_id, metadata=metadata_out)
        except Exception as exc:  # noqa: BLE001
            print(
                f"WARN show={show_id} show_image={image_id} logo_variant_metadata_update_failed={exc}",
                file=sys.stderr,
            )
    return metadata_out


def _mirror_show_logo_asset(
    db,
    *,
    show_row: dict[str, Any],
    source_page_url: str,
    candidate_url: str,
    existing_asset_ids: set[str],
    force: bool,
    dry_run: bool,
    skip_s3: bool,
    s3_client,
) -> str:
    show_id = _normalize_text(show_row.get("id"))
    if not show_id:
        return "failed"

    if skip_s3:
        return "skipped"

    data, content_type = download_image(candidate_url, source="show_logo_sync", referer=source_page_url)
    png_payload = ensure_logo_png_bytes(data, content_type)
    if not png_payload:
        return "failed"
    png_bytes, png_content_type, ext = png_payload
    sha256 = hashlib.sha256(png_bytes).hexdigest()

    existing_asset = find_asset_by_sha256(db, sha256)
    if existing_asset:
        asset_id = _normalize_text(existing_asset.get("id"))
        if not asset_id:
            return "failed"
        if not force and asset_id in existing_asset_ids:
            return "skipped"

        if not dry_run:
            existing_metadata = (
                dict(existing_asset.get("metadata")) if isinstance(existing_asset.get("metadata"), dict) else {}
            )
            hosted_url = _normalize_text(existing_asset.get("hosted_url"))
            hosted_key = _normalize_text(existing_asset.get("hosted_key"))
            hosted_content_type = _normalize_text(existing_asset.get("hosted_content_type"))
            show_identifier = _normalize_text(show_row.get("imdb_id")) or show_id
            if not hosted_url or not _is_png_hosted(hosted_key=hosted_key, hosted_content_type=hosted_content_type):
                s3_key = build_show_image_s3_key(
                    show_identifier=show_identifier,
                    kind="logo",
                    source="show-logo-sync",
                    sha256=sha256,
                    ext=ext,
                )
                bucket = get_s3_bucket()
                etag, file_size = upload_bytes_to_s3(
                    s3_client,
                    bucket=bucket,
                    key=s3_key,
                    data=png_bytes,
                    content_type=png_content_type,
                )
                hosted_url = build_hosted_url(s3_key)
                update_asset_with_mirror_result(
                    db,
                    asset_id=asset_id,
                    sha256=sha256,
                    hosted_bucket=bucket,
                    hosted_key=s3_key,
                    hosted_url=hosted_url,
                    hosted_bytes=file_size,
                    hosted_content_type=png_content_type,
                    hosted_etag=etag,
                    completed_at=datetime.now(UTC).isoformat(),
                    metadata=existing_metadata,
                )

            if hosted_url:
                _ensure_media_asset_logo_variants(
                    db,
                    show_id=show_id,
                    asset_id=asset_id,
                    hosted_url=hosted_url,
                    metadata=existing_metadata,
                    s3_client=s3_client,
                )

        if not dry_run:
            create_media_link_for_entity(
                db,
                entity_type="show",
                entity_id=show_id,
                media_asset_id=asset_id,
                kind="logo",
                position=0,
                context={
                    "source_url": candidate_url,
                    "source_page_url": source_page_url,
                    "source": "show_logo_harvest",
                },
            )
        existing_asset_ids.add(asset_id)
        return "imported"

    if dry_run:
        return "imported"

    show_identifier = _normalize_text(show_row.get("imdb_id")) or show_id
    s3_key = build_show_image_s3_key(
        show_identifier=show_identifier,
        kind="logo",
        source="show-logo-sync",
        sha256=sha256,
        ext=ext,
    )
    bucket = get_s3_bucket()
    etag, file_size = upload_bytes_to_s3(
        s3_client,
        bucket=bucket,
        key=s3_key,
        data=png_bytes,
        content_type=png_content_type,
    )
    hosted_url = build_hosted_url(s3_key)

    metadata_base: dict[str, Any] = {
        "page_url": source_page_url,
        "source_page_url": source_page_url,
        "source_type": "show_logo_harvest",
        "kind": "logo",
    }
    asset = create_media_asset_from_scrape(
        db,
        source="show_logo_harvest",
        source_url=candidate_url,
        sha256=sha256,
        hosted_bucket=bucket,
        hosted_key=s3_key,
        hosted_url=hosted_url,
        hosted_bytes=file_size,
        hosted_etag=etag,
        content_type=png_content_type,
        width=None,
        height=None,
        caption=None,
        metadata=metadata_base,
    )
    asset_id = _normalize_text(asset.get("id"))
    if not asset_id:
        return "failed"

    _ensure_media_asset_logo_variants(
        db,
        show_id=show_id,
        asset_id=asset_id,
        hosted_url=hosted_url,
        metadata=metadata_base,
        s3_client=s3_client,
    )

    create_media_link_for_entity(
        db,
        entity_type="show",
        entity_id=show_id,
        media_asset_id=asset_id,
        kind="logo",
        position=0,
        context={
            "source_url": candidate_url,
            "source_page_url": source_page_url,
            "source": "show_logo_harvest",
        },
    )
    existing_asset_ids.add(asset_id)
    return "imported"


def _backfill_existing_media_asset_show_logos(
    db,
    *,
    show_imdb_map: dict[str, str | None],
    allowed_show_ids: set[str] | None,
    dry_run: bool,
    skip_s3: bool,
    s3_client,
    summary: SyncSummary,
    verbose: bool,
) -> None:
    link_rows = list(
        _iter_rows_paged(
            db.schema("core")
            .table("media_links")
            .select("entity_id,media_asset_id")
            .eq("entity_type", "show")
            .eq("kind", "logo")
        )
    )
    first_show_by_asset: dict[str, str] = {}
    for row in link_rows:
        show_id = _normalize_text(row.get("entity_id"))
        asset_id = _normalize_text(row.get("media_asset_id"))
        if allowed_show_ids is not None and show_id not in allowed_show_ids:
            continue
        if show_id and asset_id and asset_id not in first_show_by_asset:
            first_show_by_asset[asset_id] = show_id

    for asset_id, show_id in first_show_by_asset.items():
        summary.show_logos_discovered += 1
        response = db.schema("core").table("media_assets").select("*").eq("id", asset_id).limit(1).execute()
        if hasattr(response, "error") and response.error:
            summary.show_logo_failures += 1
            summary.failures += 1
            if verbose:
                print(f"WARN asset={asset_id} failed_to_load_media_asset={response.error}", file=sys.stderr)
            continue
        rows = response.data or []
        if not rows:
            summary.show_logo_failures += 1
            summary.failures += 1
            continue
        asset_row = rows[0]
        if not isinstance(asset_row, dict):
            summary.show_logo_failures += 1
            summary.failures += 1
            continue

        hosted_url = _normalize_text(asset_row.get("hosted_url"))
        hosted_key = _normalize_text(asset_row.get("hosted_key"))
        hosted_content_type = _normalize_text(asset_row.get("hosted_content_type"))
        source_url = _normalize_text(asset_row.get("source_url"))
        metadata = dict(asset_row.get("metadata")) if isinstance(asset_row.get("metadata"), dict) else {}
        existing_black, existing_white = _extract_logo_variant_urls_from_metadata(metadata)

        if (
            _is_png_hosted(hosted_key=hosted_key, hosted_content_type=hosted_content_type)
            and existing_black
            and existing_white
        ):
            summary.show_logos_skipped += 1
            continue

        if skip_s3:
            summary.show_logos_skipped += 1
            continue

        try:
            if not dry_run and (
                not hosted_url or not _is_png_hosted(hosted_key=hosted_key, hosted_content_type=hosted_content_type)
            ):
                candidate_url = hosted_url or source_url
                if not candidate_url:
                    raise RuntimeError("missing_candidate_url_for_png_backfill")
                data, content_type = download_image(candidate_url, source="show_logo_backfill")
                png_payload = ensure_logo_png_bytes(data, content_type)
                if not png_payload:
                    raise RuntimeError("logo_png_normalization_failed")
                png_bytes, png_content_type, ext = png_payload
                sha256 = hashlib.sha256(png_bytes).hexdigest()
                show_identifier = show_imdb_map.get(show_id) or show_id
                s3_key = build_show_image_s3_key(
                    show_identifier=show_identifier,
                    kind="logo",
                    source="show-logo-backfill",
                    sha256=sha256,
                    ext=ext,
                )
                bucket = get_s3_bucket()
                etag, file_size = upload_bytes_to_s3(
                    s3_client,
                    bucket=bucket,
                    key=s3_key,
                    data=png_bytes,
                    content_type=png_content_type,
                )
                hosted_url = build_hosted_url(s3_key)
                update_asset_with_mirror_result(
                    db,
                    asset_id=asset_id,
                    sha256=sha256,
                    hosted_bucket=bucket,
                    hosted_key=s3_key,
                    hosted_url=hosted_url,
                    hosted_bytes=file_size,
                    hosted_content_type=png_content_type,
                    hosted_etag=etag,
                    completed_at=datetime.now(UTC).isoformat(),
                    metadata=metadata,
                )
            if not dry_run and hosted_url:
                _ensure_media_asset_logo_variants(
                    db,
                    show_id=show_id,
                    asset_id=asset_id,
                    hosted_url=hosted_url,
                    metadata=metadata,
                    s3_client=s3_client,
                )
            summary.show_logos_imported += 1
        except Exception as exc:  # noqa: BLE001
            summary.show_logo_failures += 1
            summary.failures += 1
            if verbose:
                print(f"WARN show={show_id} asset={asset_id} media_asset_backfill_failed={exc}", file=sys.stderr)


def _backfill_existing_show_image_logos(
    db,
    *,
    allowed_show_ids: set[str] | None,
    dry_run: bool,
    skip_s3: bool,
    s3_client,
    summary: SyncSummary,
    verbose: bool,
) -> None:
    select_columns = ",".join(
        (
            "id",
            "show_id",
            "source",
            "kind",
            "image_type",
            "file_path",
            "url",
            "url_original",
            "hosted_url",
            "hosted_key",
            "hosted_content_type",
            "hosted_sha256",
            "metadata",
        )
    )
    seen_row_ids: set[str] = set()
    for query in (
        db.schema("core").table("show_images").select(select_columns).eq("kind", "logo"),
        db.schema("core").table("show_images").select(select_columns).eq("image_type", "logo"),
    ):
        for row in _iter_rows_paged(query):
            row_id = _normalize_text(row.get("id"))
            if row_id and row_id in seen_row_ids:
                continue
            if row_id:
                seen_row_ids.add(row_id)

            show_id = _normalize_text(row.get("show_id"))
            if allowed_show_ids is not None and show_id not in allowed_show_ids:
                continue
            summary.show_logos_discovered += 1
            hosted_url = _normalize_text(row.get("hosted_url"))
            hosted_key = _normalize_text(row.get("hosted_key"))
            hosted_content_type = _normalize_text(row.get("hosted_content_type"))
            metadata = dict(row.get("metadata")) if isinstance(row.get("metadata"), dict) else {}
            existing_black, existing_white = _extract_logo_variant_urls_from_metadata(metadata)

            if (
                _is_png_hosted(hosted_key=hosted_key, hosted_content_type=hosted_content_type)
                and existing_black
                and existing_white
            ):
                summary.show_logos_skipped += 1
                continue

            if skip_s3:
                summary.show_logos_skipped += 1
                continue

            try:
                row_for_mirror = dict(row)
                if not dry_run and (
                    not hosted_url or not _is_png_hosted(hosted_key=hosted_key, hosted_content_type=hosted_content_type)
                ):
                    patch = mirror_show_image_row(row_for_mirror, force=True, s3_client=s3_client)
                    if patch:
                        update_response = (
                            db.schema("core")
                            .table("show_images")
                            .update(patch)
                            .eq("id", _normalize_text(row.get("id")))
                            .execute()
                        )
                        if hasattr(update_response, "error") and update_response.error:
                            raise RuntimeError(f"show_image_png_update_failed: {update_response.error}")
                        row_for_mirror.update(patch)
                        hosted_url = _normalize_text(patch.get("hosted_url"))

                hosted_for_variants = hosted_url or _normalize_text(row_for_mirror.get("hosted_url"))
                if not hosted_for_variants:
                    summary.show_logos_skipped += 1
                    continue

                if not dry_run:
                    _ensure_show_image_logo_variants(
                        db,
                        image_row=row_for_mirror,
                        hosted_url=hosted_for_variants,
                        metadata=metadata,
                        s3_client=s3_client,
                    )
                summary.show_logos_imported += 1
            except Exception as exc:  # noqa: BLE001
                summary.show_logo_failures += 1
                summary.failures += 1
                if verbose:
                    print(
                        "WARN "
                        f"show_image={_normalize_text(row.get('id'))} "
                        f"show={_normalize_text(row.get('show_id'))} "
                        f"backfill_failed={exc}",
                        file=sys.stderr,
                    )


def run_sync(args: argparse.Namespace) -> SyncSummary:
    load_env()
    db = create_supabase_admin_client()
    summary = SyncSummary()

    s3_client = None if args.skip_s3 or args.dry_run else get_s3_client()

    show_rows = []
    query = db.schema("core").table("shows").select("id,name,imdb_id,tmdb_meta,wikidata_id").order("id")
    for row in _iter_rows_paged(query):
        show_rows.append(row)
        if isinstance(args.limit, int) and args.limit > 0 and len(show_rows) >= args.limit:
            break
    allowed_show_ids = {_normalize_text(row.get("id")) for row in show_rows if _normalize_text(row.get("id"))}
    show_imdb_map = {
        show_id: (_normalize_text(row.get("imdb_id")) or None)
        for row in show_rows
        for show_id in [_normalize_text(row.get("id"))]
        if show_id
    }

    if args.backfill_existing:
        _backfill_existing_media_asset_show_logos(
            db,
            show_imdb_map=show_imdb_map,
            allowed_show_ids=allowed_show_ids if allowed_show_ids else None,
            dry_run=bool(args.dry_run),
            skip_s3=bool(args.skip_s3),
            s3_client=s3_client,
            summary=summary,
            verbose=bool(args.verbose),
        )
        _backfill_existing_show_image_logos(
            db,
            allowed_show_ids=allowed_show_ids if allowed_show_ids else None,
            dry_run=bool(args.dry_run),
            skip_s3=bool(args.skip_s3),
            s3_client=s3_client,
            summary=summary,
            verbose=bool(args.verbose),
        )

    for show_row in show_rows:
        show_name = _normalize_text(show_row.get("name")) or _normalize_text(show_row.get("id"))
        source_urls = _collect_source_urls(show_row)
        if not source_urls:
            continue

        existing_asset_ids = _load_existing_show_logo_asset_ids(db, show_id=_normalize_text(show_row.get("id")))
        imported_for_show = 0
        seen_candidate_urls: set[str] = set()

        for source_url in source_urls:
            if imported_for_show >= 12:
                break

            try:
                response = requests.get(source_url, headers=_REQUEST_HEADERS, timeout=20)
                response.raise_for_status()
            except requests.RequestException as exc:
                summary.show_logo_failures += 1
                summary.failures += 1
                if args.verbose:
                    print(f"WARN show={show_name} source={source_url} fetch_failed={exc}", file=sys.stderr)
                continue

            content_type = _normalize_text(response.headers.get("content-type")).lower()
            if content_type.startswith("image/"):
                candidates = [source_url]
            else:
                candidates = _extract_logo_candidates_from_html(
                    html=response.text,
                    page_url=source_url,
                    limit=8,
                )

            if not candidates:
                continue

            for candidate_url in candidates:
                if imported_for_show >= 12:
                    break
                if candidate_url in seen_candidate_urls:
                    continue
                seen_candidate_urls.add(candidate_url)
                summary.show_logos_discovered += 1

                try:
                    outcome = _mirror_show_logo_asset(
                        db,
                        show_row=show_row,
                        source_page_url=source_url,
                        candidate_url=candidate_url,
                        existing_asset_ids=existing_asset_ids,
                        force=bool(args.force),
                        dry_run=bool(args.dry_run),
                        skip_s3=bool(args.skip_s3),
                        s3_client=s3_client,
                    )
                except Exception as exc:  # noqa: BLE001
                    summary.show_logo_failures += 1
                    summary.failures += 1
                    if args.verbose:
                        print(
                            f"WARN show={show_name} source={source_url} candidate={candidate_url} import_failed={exc}",
                            file=sys.stderr,
                        )
                    continue

                if outcome == "imported":
                    summary.show_logos_imported += 1
                    imported_for_show += 1
                else:
                    summary.show_logos_skipped += 1

    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    summary = run_sync(args)
    summary_dict = summary.__dict__

    print("Summary")
    for key in (
        "show_logos_discovered",
        "show_logos_imported",
        "show_logos_skipped",
        "show_logo_failures",
        "failures",
    ):
        print(f"{key}={summary_dict[key]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
