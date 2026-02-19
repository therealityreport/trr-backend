#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import requests

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.media.s3_mirror import (
    MonochromeLogoMirrorResult,
    get_s3_client,
    mirror_external_logo_row,
    mirror_logo_monochrome_variants_row,
)
from trr_backend.utils.env import load_env

WIKIDATA_SEARCH_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{item_id}.json"
WIKIDATA_ITEM_RE = re.compile(r"^Q\d+$", re.IGNORECASE)
LOGO_CLAIM_IDS = ("P154", "P2910")
REQUEST_HEADERS = {
    "accept": "application/json",
    "user-agent": "TRR-Backend/1.0",
}
COMMON_FIELDS = (
    "hosted_logo_url,hosted_logo_key,hosted_logo_sha256,"
    "hosted_logo_black_url,hosted_logo_black_key,hosted_logo_black_sha256,"
    "hosted_logo_white_url,hosted_logo_white_key,hosted_logo_white_sha256,"
    "wikidata_id,wikipedia_url,wikimedia_logo_file"
)


@dataclass
class UnresolvedLogo:
    type: str
    id: str
    name: str
    reason: str


@dataclass
class SyncSummary:
    processed: int = 0
    links_enriched: int = 0
    wikidata_linked: int = 0
    wikipedia_linked: int = 0
    logos_mirrored: int = 0
    variants_black_mirrored: int = 0
    variants_white_mirrored: int = 0
    failures: int = 0
    unresolved_logos: list[UnresolvedLogo] = field(default_factory=list)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_networks_streaming_links",
        description=(
            "Enrich used core.networks/core.watch_providers rows with Wikidata + Wikipedia links, "
            "mirror missing logo assets from Wikimedia, and generate black/white transparent variants."
        ),
    )
    parser.add_argument("--all", action="store_true", help="Accepted for CLI parity. Script processes all used rows.")
    parser.add_argument("--force", action="store_true", help="Re-enrich rows and force logo/variant re-mirror.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve + print intended updates without writing.")
    parser.add_argument("--skip-s3", action="store_true", help="Skip logo and variant mirroring.")
    parser.add_argument("--limit", type=int, default=None, help="Optional per-table processing cap.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _name_key(value: Any) -> str:
    return _normalize_text(value).casefold()


def _score_search_result(name: str, candidate: dict[str, Any]) -> int:
    score = 0
    target = name.casefold()
    label = _normalize_text(candidate.get("label")).casefold()
    description = _normalize_text(candidate.get("description")).casefold()

    if label == target:
        score += 100
    if target and target in label:
        score += 35
    if target and label and label in target:
        score += 20
    if "network" in description:
        score += 15
    if "television" in description:
        score += 10
    if "streaming" in description:
        score += 10
    return score


def _extract_commons_logo_file(entity: dict[str, Any]) -> str | None:
    claims = entity.get("claims")
    if not isinstance(claims, dict):
        return None

    for claim_id in LOGO_CLAIM_IDS:
        claim_rows = claims.get(claim_id)
        if not isinstance(claim_rows, list):
            continue
        for claim in claim_rows:
            if not isinstance(claim, dict):
                continue
            mainsnak = claim.get("mainsnak")
            if not isinstance(mainsnak, dict):
                continue
            datavalue = mainsnak.get("datavalue")
            if not isinstance(datavalue, dict):
                continue
            value = datavalue.get("value")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _commons_file_urls(file_name: str | None) -> list[str]:
    text = _normalize_text(file_name)
    if not text:
        return []
    normalized = text.replace(" ", "_")
    encoded = quote(normalized, safe="()_-")
    return [
        f"https://commons.wikimedia.org/wiki/Special:FilePath/{encoded}?width=1024",
        f"https://commons.wikimedia.org/wiki/Special:FilePath/{encoded}",
    ]


def _fetch_wikidata_entity(item_id: str) -> dict[str, Any] | None:
    item_id = _normalize_text(item_id).upper()
    if not WIKIDATA_ITEM_RE.match(item_id):
        return None

    response = requests.get(
        WIKIDATA_ENTITY_URL.format(item_id=item_id),
        headers=REQUEST_HEADERS,
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    entities = payload.get("entities")
    if not isinstance(entities, dict):
        return None
    entity = entities.get(item_id)
    return entity if isinstance(entity, dict) else None


def _search_wikidata_item(name: str) -> str | None:
    candidate = _normalize_text(name)
    if not candidate:
        return None

    response = requests.get(
        WIKIDATA_SEARCH_URL,
        headers=REQUEST_HEADERS,
        params={
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "type": "item",
            "limit": 10,
            "search": candidate,
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("search")
    if not isinstance(rows, list) or not rows:
        return None

    best = None
    best_score = -1
    for row in rows:
        if not isinstance(row, dict):
            continue
        item_id = _normalize_text(row.get("id")).upper()
        if not WIKIDATA_ITEM_RE.match(item_id):
            continue
        score = _score_search_result(candidate, row)
        if score > best_score:
            best_score = score
            best = item_id
    return best


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


def _resolve_entity_metadata(name: str, existing_wikidata_id: str | None) -> dict[str, str | None]:
    wikidata_id = _normalize_text(existing_wikidata_id).upper() or None
    entity = None

    if wikidata_id and WIKIDATA_ITEM_RE.match(wikidata_id):
        try:
            entity = _fetch_wikidata_entity(wikidata_id)
        except requests.RequestException:
            entity = None

    if entity is None:
        found_id = _search_wikidata_item(name)
        if not found_id:
            return {
                "wikidata_id": None,
                "wikipedia_url": None,
                "wikimedia_logo_file": None,
            }
        wikidata_id = found_id
        entity = _fetch_wikidata_entity(found_id)

    if not isinstance(entity, dict):
        return {
            "wikidata_id": wikidata_id,
            "wikipedia_url": None,
            "wikimedia_logo_file": None,
        }

    return {
        "wikidata_id": wikidata_id,
        "wikipedia_url": _extract_enwiki_url(entity),
        "wikimedia_logo_file": _extract_commons_logo_file(entity),
    }


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


def _collect_used_network_keys(db) -> set[str]:
    keys: set[str] = set()
    query = db.schema("core").table("shows").select("networks").order("id")
    for row in _iter_rows_paged(query):
        values = row.get("networks")
        if not isinstance(values, list):
            continue
        for value in values:
            key = _name_key(value)
            if key:
                keys.add(key)
    return keys


def _collect_primary_provider_keys(db) -> set[str]:
    keys: set[str] = set()
    query = (
        db.schema("core")
        .table("show_watch_providers")
        .select("provider:watch_providers(provider_name)")
        .eq("region", "US")
        .in_("offer_type", ["flatrate", "ads"])
        .order("show_id")
    )
    for row in _iter_rows_paged(query):
        provider = row.get("provider")
        if isinstance(provider, dict):
            key = _name_key(provider.get("provider_name"))
            if key:
                keys.add(key)
            continue
        if isinstance(provider, list):
            for entry in provider:
                if not isinstance(entry, dict):
                    continue
                key = _name_key(entry.get("provider_name"))
                if key:
                    keys.add(key)
    return keys


def _collect_fallback_provider_keys(db, *, primary_keys: set[str]) -> set[str]:
    keys: set[str] = set()
    query = db.schema("core").table("shows").select("streaming_providers").order("id")
    for row in _iter_rows_paged(query):
        values = row.get("streaming_providers")
        if not isinstance(values, list):
            continue
        for value in values:
            key = _name_key(value)
            if key and key not in primary_keys:
                keys.add(key)
    return keys


def _collect_used_provider_keys(db) -> set[str]:
    primary_keys = _collect_primary_provider_keys(db)
    fallback_keys = _collect_fallback_provider_keys(db, primary_keys=primary_keys)
    return primary_keys | fallback_keys


def _list_rows(
    db,
    *,
    table: str,
    id_field: str,
    name_field: str,
    used_name_keys: set[str],
    limit: int | None,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    query = db.schema("core").table(table).select(f"{id_field},{name_field},{COMMON_FIELDS}").order(name_field)
    for row in _iter_rows_paged(query):
        name = _normalize_text(row.get(name_field))
        if not name:
            continue
        if _name_key(name) not in used_name_keys:
            continue
        selected.append({**row, "_name": name})
        if limit is not None and limit > 0 and len(selected) >= limit:
            break
    return selected


def _update_row(
    db,
    *,
    table: str,
    id_field: str,
    entity_id: Any,
    patch: dict[str, Any],
) -> None:
    response = db.schema("core").table(table).update(patch).eq(id_field, entity_id).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error updating {table}: {response.error}")


def _reason_from_exception(exc: Exception) -> str:
    text = _normalize_text(str(exc)).lower()
    if "logo_decode_failed" in text:
        return "logo_decode_failed"
    if "transparent_extraction_failed" in text:
        return "transparent_extraction_failed"
    if "s3" in text or "upload" in text or "bucket" in text:
        return "s3_upload_failed"
    return "download_failed"


def _record_unresolved(summary: SyncSummary, *, row_type: str, entity_id: Any, name: str, reason: str) -> None:
    summary.unresolved_logos.append(
        UnresolvedLogo(
            type=row_type,
            id=str(entity_id),
            name=name,
            reason=reason,
        )
    )


def _sync_table(
    db,
    *,
    table: str,
    id_field: str,
    name_field: str,
    row_type: str,
    logo_kind: str,
    used_name_keys: set[str],
    args: argparse.Namespace,
    summary: SyncSummary,
    s3_client,
) -> None:
    rows = _list_rows(
        db,
        table=table,
        id_field=id_field,
        name_field=name_field,
        used_name_keys=used_name_keys,
        limit=args.limit,
    )

    for row in rows:
        summary.processed += 1
        entity_id = row.get(id_field)
        name = _normalize_text(row.get("_name"))
        if entity_id is None or not name:
            continue

        unresolved_reason: str | None = None
        try:
            metadata = _resolve_entity_metadata(name, row.get("wikidata_id"))

            patch: dict[str, Any] = {}
            wikidata_id = _normalize_text(metadata.get("wikidata_id"))
            wikipedia_url = _normalize_text(metadata.get("wikipedia_url"))
            wikimedia_logo_file = _normalize_text(metadata.get("wikimedia_logo_file"))

            existing_wikidata = _normalize_text(row.get("wikidata_id"))
            existing_wikipedia = _normalize_text(row.get("wikipedia_url"))
            existing_logo_file = _normalize_text(row.get("wikimedia_logo_file"))

            if wikidata_id and (args.force or not existing_wikidata):
                if wikidata_id != existing_wikidata:
                    patch["wikidata_id"] = wikidata_id
                if not existing_wikidata:
                    summary.wikidata_linked += 1

            if wikipedia_url and (args.force or not existing_wikipedia):
                if wikipedia_url != existing_wikipedia:
                    patch["wikipedia_url"] = wikipedia_url
                if not existing_wikipedia:
                    summary.wikipedia_linked += 1

            if wikimedia_logo_file and (args.force or not existing_logo_file):
                if wikimedia_logo_file != existing_logo_file:
                    patch["wikimedia_logo_file"] = wikimedia_logo_file

            if patch:
                patch["link_enriched_at"] = _now_iso()
                patch["link_enrichment_source"] = "wikidata"
                summary.links_enriched += 1

            commons_file = _normalize_text(patch.get("wikimedia_logo_file")) or existing_logo_file
            commons_urls = _commons_file_urls(commons_file)

            has_hosted_logo = bool(_normalize_text(row.get("hosted_logo_url")))
            should_try_logo = (args.force or not has_hosted_logo) and bool(commons_urls)

            if not has_hosted_logo and not commons_urls:
                unresolved_reason = "no_wikidata_match" if not (wikidata_id or existing_wikidata) else "no_logo_claim"

            if not args.skip_s3 and not args.dry_run and should_try_logo:
                last_error: Exception | None = None
                for candidate_url in commons_urls:
                    try:
                        logo_patch = mirror_external_logo_row(
                            row,
                            kind=logo_kind,
                            id_field=id_field,
                            source_url=candidate_url,
                            force=bool(args.force),
                            s3_client=s3_client,
                        )
                        if logo_patch:
                            summary.logos_mirrored += 1
                            patch.update(logo_patch)
                        last_error = None
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_error = exc
                if last_error is not None and unresolved_reason is None:
                    unresolved_reason = _reason_from_exception(last_error)

            hosted_logo_url = _normalize_text(patch.get("hosted_logo_url")) or _normalize_text(
                row.get("hosted_logo_url")
            )
            has_base_logo = bool(hosted_logo_url)
            if (args.force or not has_hosted_logo) and not has_base_logo and unresolved_reason is None:
                unresolved_reason = "download_failed"

            if not args.skip_s3 and not args.dry_run:
                source_url = hosted_logo_url or (commons_urls[0] if commons_urls else "")
                if source_url:
                    merged_row = {**row, **patch}
                    try:
                        variant_result = mirror_logo_monochrome_variants_row(
                            merged_row,
                            kind=logo_kind,
                            id_field=id_field,
                            source_url=source_url,
                            force=bool(args.force),
                            s3_client=s3_client,
                            source="wikimedia",
                        )
                        if isinstance(variant_result, MonochromeLogoMirrorResult):
                            patch.update(variant_result.patch)
                            summary.variants_black_mirrored += int(variant_result.black_mirrored)
                            summary.variants_white_mirrored += int(variant_result.white_mirrored)
                    except Exception as exc:  # noqa: BLE001
                        if unresolved_reason is None:
                            unresolved_reason = _reason_from_exception(exc)
                elif unresolved_reason is None:
                    unresolved_reason = "no_logo_claim"

            if patch:
                if args.verbose:
                    print(f"UPDATE {table} {id_field}={entity_id} keys={sorted(patch.keys())}")
                if not args.dry_run:
                    _update_row(db, table=table, id_field=id_field, entity_id=entity_id, patch=patch)

            final_black = _normalize_text(patch.get("hosted_logo_black_url")) or _normalize_text(
                row.get("hosted_logo_black_url")
            )
            final_white = _normalize_text(patch.get("hosted_logo_white_url")) or _normalize_text(
                row.get("hosted_logo_white_url")
            )
            if not args.skip_s3 and not args.dry_run and (args.force or not (final_black and final_white)):
                if not (final_black and final_white) and unresolved_reason is None:
                    unresolved_reason = "transparent_extraction_failed"

            if unresolved_reason:
                _record_unresolved(
                    summary,
                    row_type=row_type,
                    entity_id=entity_id,
                    name=name,
                    reason=unresolved_reason,
                )
        except Exception as exc:  # noqa: BLE001
            summary.failures += 1
            reason = _reason_from_exception(exc)
            _record_unresolved(
                summary,
                row_type=row_type,
                entity_id=entity_id,
                name=name,
                reason=reason,
            )
            if args.verbose:
                print(f"ERROR {table} {id_field}={entity_id}: {exc}", file=sys.stderr)


def run_sync(args: argparse.Namespace) -> SyncSummary:
    load_env()
    db = create_supabase_admin_client()
    summary = SyncSummary()
    s3_client = None if args.skip_s3 or args.dry_run else get_s3_client()

    used_network_keys = _collect_used_network_keys(db)
    used_provider_keys = _collect_used_provider_keys(db)

    _sync_table(
        db,
        table="networks",
        id_field="id",
        name_field="name",
        row_type="network",
        logo_kind="networks",
        used_name_keys=used_network_keys,
        args=args,
        summary=summary,
        s3_client=s3_client,
    )
    _sync_table(
        db,
        table="watch_providers",
        id_field="provider_id",
        name_field="provider_name",
        row_type="streaming",
        logo_kind="watch-providers",
        used_name_keys=used_provider_keys,
        args=args,
        summary=summary,
        s3_client=s3_client,
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    summary = run_sync(args)
    summary_dict = asdict(summary)

    print("Summary")
    for key in (
        "processed",
        "links_enriched",
        "wikidata_linked",
        "wikipedia_linked",
        "logos_mirrored",
        "variants_black_mirrored",
        "variants_white_mirrored",
        "failures",
    ):
        print(f"{key}={summary_dict[key]}")

    print(f"unresolved_logos={len(summary.unresolved_logos)}")
    print("Unresolved Logos")
    for item in summary.unresolved_logos:
        print("unresolved_logo=" + json.dumps(asdict(item), ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
