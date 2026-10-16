"""Pure listing/detail-cache composition; no source calls or database connections."""

import copy
from datetime import UTC, datetime, timedelta, timezone

import pytest

from trr_backend.ingestion.canonical import normalize_post_source
from trr_backend.ingestion.domain import RunIntent
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.freshness import detail_eligibility
from trr_backend.ingestion.listing import ROOT_FIELD, _listing_post_source, parse_listing_page
from trr_backend.ingestion.providers import PARSER_VERSION

CAPTURED = datetime(2026, 10, 16, 8, tzinfo=timezone(timedelta(hours=-4)))
NOW = datetime(2026, 10, 16, 12, 5, tzinfo=UTC)


def node():
    return {
        "__typename": "GraphImage",
        "id": "native-1",
        "shortcode": "Listing1",
        "owner": {"id": "owner-1", "username": "fixture_account"},
        "taken_at_timestamp": 1792065600,
        "display_url": "https://cdn.example/image.jpg",
        "edge_media_to_caption": {"edges": []},
        "edge_media_preview_like": {"count": 2},
        "edge_media_to_comment": {"count": 0},
    }


def derive(raw):
    evidence = Evidence.capture(
        source_identity="instagram:listing:fixture_account:page:root",
        captured_at=CAPTURED,
        parser_version=PARSER_VERSION,
        payload={
            "data": {ROOT_FIELD: {"edges": [{"node": raw}], "page_info": {"has_next_page": False, "end_cursor": None}}}
        },
    )
    native = parse_listing_page(evidence.read(pinned=True)).posts[0]
    normalized = normalize_post_source(native, account="fixture_account")
    source = _listing_post_source(
        native,
        normalized,
        evidence=evidence,
        evidence_id="listing-evidence-1",
        run_id="current-run",
        page_key="page:root",
    )
    return source, native, normalized, evidence


def eligible(source, normalized, *, force=False):
    return detail_eligibility(
        post={
            "id": "post-1",
            "shortcode": normalized.shortcode,
            "source_account": "fixture_account",
            "likes": 2,
            "comments_count": 0,
            "media_urls": normalized.media_urls,
            "raw_data": source,
        },
        sidecar={"raw_data": source},
        canonical_identity={"post_id": "post-1", "catalog_post_id": "catalog-1", "canonical_post_id": "canonical-1"},
        intent=RunIntent(account="fixture_account", force=force),
        now=NOW,
    )


def test_complete_listing_satisfies_existing_cache_without_detail_fetch():
    source, native, normalized, evidence = derive(node())
    assert eligible(native, normalized).reason == "detail_proof_missing"
    decision = eligible(source, normalized)
    assert decision.eligible and decision.reason == "complete_detail_snapshot_fresh"
    assert decision.observed_at == evidence.captured_at == CAPTURED.astimezone(UTC)
    assert source["detail_snapshot"]["media_count"] == 1
    assert source["detail_snapshot"]["observed_at"] == evidence.captured_at.isoformat()
    assert source["detail_snapshot"]["evidence_reference"] == {
        "evidence_id": "listing-evidence-1",
        "run_id": "current-run",
        "stage": "listing",
        "target_key": "page:root",
        "captured_at": evidence.captured_at.isoformat(),
        "content_hash": evidence.content_hash,
        "parser_version": evidence.parser_version,
        "schema_version": evidence.schema_version,
    }
    # The immutable page evidence never acquires derived detail-stage content.
    assert "detail_snapshot" not in native
    assert parse_listing_page(evidence.read(pinned=True)).posts[0] == native
    assert evidence.source_identity.startswith("instagram:listing:")


@pytest.mark.parametrize(
    "missing", ["edge_media_to_caption", "edge_media_preview_like", "edge_media_to_comment", "display_url"]
)
def test_incomplete_listing_stays_listing_only(missing):
    raw = node()
    raw.pop(missing)
    source, native, normalized, _ = derive(raw)
    assert source == native and "detail_snapshot" not in source
    assert not eligible(source, normalized).eligible


def test_force_still_requires_detail_refetch_and_stale_listing_is_not_freshened():
    source, _, normalized, _ = derive(node())
    assert eligible(source, normalized, force=True).reason == "forced_refetch"
    stale = copy.deepcopy(source)
    stale["detail_snapshot"]["observed_at"] = (NOW - timedelta(hours=24)).isoformat()
    assert eligible(stale, normalized).reason == "metrics_stale"


def test_carousel_requires_declared_slide_inventory():
    raw = node()
    raw.update(
        __typename="GraphSidecar",
        carousel_media_count=2,
        edge_sidecar_to_children={
            "edges": [{"node": {"__typename": "GraphImage", "display_url": "https://cdn.example/slide1.jpg"}}]
        },
    )
    source, native, normalized, _ = derive(raw)
    assert "detail_snapshot" not in source and source == native
    assert not eligible(source, normalized).eligible
    raw["edge_sidecar_to_children"]["edges"].append(
        {"node": {"__typename": "GraphVideo", "video_url": "https://cdn.example/slide2.mp4"}}
    )
    source, _, normalized, _ = derive(raw)
    assert source["detail_snapshot"]["media_count"] == 2
    assert eligible(source, normalized).eligible


def test_existing_cache_rejects_hidden_listing_metrics():
    raw = node()
    raw["like_and_view_counts_disabled"] = True
    source, _, normalized, _ = derive(raw)
    assert eligible(source, normalized).reason == "source_metrics_unknown"
