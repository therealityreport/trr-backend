"""Pure targeted REST fixtures; no credential, browser, storage or HTTP calls."""

from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from trr_backend.ingestion.comment_attachment_refresh import (
    ATTACHMENT_PARSER_VERSION,
    AttachmentRefresh,
    AttachmentRefreshError,
    attachment_baseline,
    parse_targeted_rest_attachment,
    validate_attachment_order,
)

CODE = "Dd37XeDly4U"
MEDIA_ID = "3996924291879218708"
COMMENT_ID = "18029668664695097"
GIF_ID = "1072011872270984"
REAL = "https://cdn.fbsbx.com/v/t59.2708-21/469205396_963532625643465_1425538320113013826_n.gif"
SIGNED = REAL + "?oh=private-delivery&oe=private-expiry"
PLACEHOLDER = "https://static.cdninstagram.com/rsrc.php/v4/yr/r/xg_5YoVlvjp.gif"
CAPTURED = datetime(2026, 10, 22, 8, tzinfo=UTC)


def row():
    return {
        "id": "14cf1d59-e5b5-433f-b928-a927d66cda94",
        "post_id": "6a20f4f2-4025-494d-8a2e-143cdd43faed",
        "comment_id": COMMENT_ID,
        "user_id": "90000000001",
        "username": "fixture_commenter",
        "text": "",
        "source_account": "bravotv",
        "is_reply": False,
        "parent_comment_id": None,
        "parent_comment_external_id": None,
        "is_missing": False,
        "deleted_at": None,
        "scraped_at": CAPTURED - timedelta(days=1),
        "media_urls": [PLACEHOLDER],
        "hosted_media_urls": [],
        "media_mirror_status": "failed",
        "media_mirror_error": "media_source_placeholder",
        "raw_data": {"giphy_media_info": {"id": GIF_ID, "images": {"fixed_height": {"url": PLACEHOLDER}}}},
    }


def payload():
    return {
        "status": "ok",
        "comments": [
            {
                "pk": COMMENT_ID,
                "media_id": MEDIA_ID,
                "user_id": "90000000001",
                "user": {"username": "fixture_commenter"},
                "text": "",
                "child_comment_count": 0,
                "giphy_media_info": {
                    "id": GIF_ID,
                    "gif_media_id": "2FEXOysdjGJ8s",
                    "is_sticker": False,
                    "title": "johnny depp lol GIF",
                    "images": {"fixed_height": {"url": SIGNED, "height": 200, "width": 200}},
                },
            }
        ],
    }


def parse(value=None, baseline=None, **kwargs):
    return parse_targeted_rest_attachment(
        payload() if value is None else value,
        row=row() if baseline is None else baseline,
        shortcode=kwargs.get("shortcode", CODE),
        expected_gif_id=kwargs.get("expected_gif_id", GIF_ID),
        captured_at=kwargs.get("captured_at", CAPTURED),
    )


def test_actual_native_shape_keeps_ephemeral_url_but_retains_only_public_references():
    native, baseline = payload(), row()
    originals = deepcopy((native, baseline))
    proof = parse(native, baseline)
    proof.validate()
    assert proof.source_url == SIGNED
    assert "private-delivery" not in repr(proof)
    assert proof.giphy_media_info == {
        "id": GIF_ID,
        "gif_media_id": "2FEXOysdjGJ8s",
        "is_sticker": False,
        "title": "johnny depp lol GIF",
        "images": {"fixed_height": {"url": REAL, "height": 200, "width": 200}},
    }
    assert (native, baseline) == originals
    retained = proof.to_payload()
    assert retained["source_url"] == REAL
    assert retained["version"] == ATTACHMENT_PARSER_VERSION
    assert "private-delivery" not in str(retained)
    assert proof.evidence().content_hash == proof.evidence_sha256
    assert proof.evidence().read(pinned=True) == {k: v for k, v in retained.items() if k != "evidence_sha256"}
    assert not {"full_observation", "comments_complete", "exhausted"} & retained.keys()


def test_serialization_roundtrip_reproduces_evidence_and_does_not_alias_input():
    original = parse()
    retained = original.to_payload()
    replay = AttachmentRefresh.from_payload(retained)
    assert replay.to_payload() == retained
    assert replay.evidence() == original.evidence()
    retained["giphy_media_info"]["id"] = "9"
    replay.validate()
    assert replay.giphy_media_info["id"] == GIF_ID


def test_native_active_comment_status_is_not_an_envelope_failure():
    value = payload()
    value["comments"][0]["status"] = "Active"
    assert parse(value).native_gif_id == GIF_ID
    value["comments"][0]["error"] = 1357001
    with pytest.raises(AttachmentRefreshError):
        parse(value)


@pytest.mark.parametrize("status", ["Inactive", "Deleted", "fail", "error", "ok", False])
def test_inactive_or_unrecognized_native_comment_status_rejects(status):
    value = payload()
    value["comments"][0]["status"] = status
    with pytest.raises(AttachmentRefreshError):
        parse(value)


@pytest.mark.parametrize(
    "change",
    [
        "missing_comments",
        "missing_target",
        "duplicate",
        "missing_pk",
        "conflicting_id",
        "missing_media",
        "wrong_media",
        "wrong_envelope_media",
        "wrong_parent",
        "wrong_gif",
        "missing_gif",
        "wrong_author",
        "missing_author",
        "conflicting_author",
        "wrong_username",
        "success_false",
        "failed_true",
        "native_error",
        "rate_limited",
        "node_failure",
        "missing_status",
        "failed_status",
    ],
)
def test_native_binding_or_failure_rejects(change):
    value = payload()
    node = value["comments"][0]
    if change == "missing_comments":
        value.pop("comments")
    elif change == "missing_target":
        node["pk"] = "9"
    elif change == "duplicate":
        value["comments"].append(deepcopy(node))
    elif change == "missing_pk":
        node.pop("pk")
        node["id"] = COMMENT_ID
    elif change == "conflicting_id":
        node["id"] = "9"
    elif change == "missing_media":
        node.pop("media_id")
        value["media_id"] = MEDIA_ID
    elif change == "wrong_media":
        node["media_id"] = "9"
    elif change == "wrong_envelope_media":
        value["media_id"] = "9"
    elif change == "wrong_parent":
        node["parent_comment_id"] = "9"
    elif change == "wrong_gif":
        node["giphy_media_info"]["id"] = "9"
    elif change == "missing_gif":
        node.pop("giphy_media_info")
    elif change == "wrong_author":
        node["user_id"] = "9"
    elif change == "missing_author":
        node.pop("user_id")
    elif change == "conflicting_author":
        node["user"]["pk"] = "9"
    elif change == "wrong_username":
        node["user"]["username"] = "other"
    elif change == "success_false":
        value["success"] = False
    elif change == "failed_true":
        value["failed"] = True
    elif change == "native_error":
        value["error"] = 1357001
    elif change == "rate_limited":
        value["rate_limited"] = True
    elif change == "node_failure":
        node["failed"] = True
    elif change == "missing_status":
        value.pop("status")
    else:
        value["status"] = "fail"
    with pytest.raises(AttachmentRefreshError, match="^invalid_comment_attachment_refresh$"):
        parse(value)


def test_explicit_matching_envelope_and_child_parent_are_supported():
    value, baseline = payload(), row()
    value["media_id"] = MEDIA_ID
    baseline.update(
        is_reply=True, parent_comment_id="de1ec38c-165a-4382-b6d6-3f2d5796ff4c", parent_comment_external_id="8"
    )
    baseline["raw_data"]["parent_comment_id"] = "8"
    value["comments"][0]["parent_comment_id"] = "8"
    assert parse(value, baseline).parent_comment_id == "8"
    value["comments"][0].pop("parent_comment_id")
    with pytest.raises(AttachmentRefreshError):
        parse(value, baseline)


@pytest.mark.parametrize("preferred", [None, {}, {"fixed_height": {"url": PLACEHOLDER}}, {"url": SIGNED}])
def test_native_nullish_preferred_branch_is_not_truthy_fallback(preferred):
    value = payload()
    value["comments"][0]["giphy_media_info"]["first_party_cdn_proxied_images"] = preferred
    if preferred is None:
        assert parse(value).source_url == SIGNED
    else:
        with pytest.raises(AttachmentRefreshError):
            parse(value)


def test_preferred_real_rendition_retains_both_branches_without_selecting_placeholder():
    value = payload()
    info = value["comments"][0]["giphy_media_info"]
    info["first_party_cdn_proxied_images"] = deepcopy(info["images"])
    info["images"]["fixed_height"]["url"] = PLACEHOLDER
    proof = parse(value)
    assert proof.source_url == SIGNED
    assert proof.giphy_media_info["images"]["fixed_height"]["url"] == PLACEHOLDER


@pytest.mark.parametrize(
    "url",
    [
        PLACEHOLDER,
        PLACEHOLDER + "?oh=signature",
        "http://cdn.fbsbx.com/real.gif",
        "https://evil.example/real.gif",
        "https://127.0.0.1/real.gif",
        "https://cdn.fbsbx.com.evil.example/real.gif",
        "https://cdn.fbsbx.com/real.mp4",
        "https://user:private@cdn.fbsbx.com/real.gif",
        "https://cdn.fbsbx.com:8443/real.gif",
        REAL + "#private",
        " " + REAL,
        "javascript:placeholder",
        "https://cdn.fbsbx.com/%78g_5YoVlvjp.gif",
    ],
)
def test_unsafe_non_gif_or_placeholder_native_url_rejects(url):
    value = payload()
    value["comments"][0]["giphy_media_info"]["images"]["fixed_height"]["url"] = url
    with pytest.raises(AttachmentRefreshError):
        parse(value)


@pytest.mark.parametrize("stamp", [None, "2026-10-22T08:00:00+00:00", datetime(2026, 10, 22, 8)])
def test_capture_time_requires_aware_datetime(stamp):
    with pytest.raises(AttachmentRefreshError):
        parse(captured_at=stamp)


@pytest.mark.parametrize("change", ["extra", "missing", "hash", "identity", "gif", "url_query", "timestamp", "version"])
def test_replay_metadata_tampering_or_wrong_types_rejects(change):
    retained = parse().to_payload()
    if change == "extra":
        retained["sessionid"] = "private"
    elif change == "missing":
        retained.pop("row_id")
    elif change == "hash":
        retained["evidence_sha256"] = "0" * 64
    elif change == "identity":
        retained["source_identity"] += ":other"
    elif change == "gif":
        retained["giphy_media_info"]["id"] = "9"
    elif change == "url_query":
        retained["source_url"] += "?oh=private"
    elif change == "timestamp":
        retained["captured_at"] = "2026-10-22T08:00:00"
    else:
        retained["version"] = "other"
    with pytest.raises(AttachmentRefreshError):
        AttachmentRefresh.from_payload(retained)


def test_baseline_ignores_delivery_signatures_but_covers_protected_and_attachment_state():
    original = row()
    original["media_urls"] = [SIGNED]
    other = deepcopy(original)
    other["media_urls"] = [REAL + "?oh=rotated"]
    assert attachment_baseline(other) == attachment_baseline(original)
    for key, changed in [
        ("text", "changed"),
        ("user_id", "9"),
        ("is_missing", True),
        ("media_urls", [PLACEHOLDER]),
        ("hosted_media_urls", ["https://cdn.thereality.report/new.gif"]),
        ("media_mirror_error", None),
        ("scraped_at", CAPTURED),
    ]:
        other = deepcopy(original)
        other[key] = changed
        assert attachment_baseline(other) != attachment_baseline(original)
    other = deepcopy(original)
    other["raw_data"]["ingestion_attachment"] = {"evidence_sha256": "1" * 64}
    assert attachment_baseline(other) != attachment_baseline(original)


def test_mutating_nested_proof_metadata_invalidates_its_hash():
    proof = parse()
    proof.giphy_media_info["title"] = "changed"
    with pytest.raises(AttachmentRefreshError):
        proof.validate()
    with pytest.raises(AttachmentRefreshError):
        replace(parse(), captured_at=CAPTURED.replace(tzinfo=None)).validate()


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_attachment_order_compares_original_row_observation(offset):
    baseline = row()
    baseline["scraped_at"] = CAPTURED
    stamp = CAPTURED + timedelta(seconds=offset)
    if offset < 0:
        with pytest.raises(AttachmentRefreshError):
            validate_attachment_order(baseline, stamp)
        with pytest.raises(AttachmentRefreshError):
            parse(baseline=baseline, captured_at=stamp)
    else:
        validate_attachment_order(baseline, stamp)
        parse(baseline=baseline, captured_at=stamp)


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_attachment_order_requires_strictly_newer_than_valid_retained_marker(offset):
    baseline = row()
    baseline["raw_data"]["ingestion_attachment"] = parse().to_payload()
    stamp = CAPTURED + timedelta(seconds=offset)
    if offset <= 0:
        with pytest.raises(AttachmentRefreshError):
            validate_attachment_order(baseline, stamp)
        with pytest.raises(AttachmentRefreshError):
            parse(baseline=baseline, captured_at=stamp)
    else:
        validate_attachment_order(baseline, stamp)
        parse(baseline=baseline, captured_at=stamp)


@pytest.mark.parametrize("change", ["hash", "row", "missing_keys", "unaware", "missing_observation"])
def test_attachment_order_rejects_invalid_retained_marker_or_observation(change):
    baseline = row()
    marker = parse().to_payload()
    baseline["raw_data"]["ingestion_attachment"] = marker
    if change == "hash":
        marker["evidence_sha256"] = "0" * 64
    elif change == "row":
        other = row()
        other["id"] = "3fdb6afb-f17f-4e4b-a127-a2b500d355e0"
        baseline["raw_data"]["ingestion_attachment"] = parse(baseline=other).to_payload()
    elif change == "missing_keys":
        marker.pop("source_identity")
    elif change == "unaware":
        baseline["scraped_at"] = CAPTURED.replace(tzinfo=None)
    else:
        baseline.pop("scraped_at")
    with pytest.raises(AttachmentRefreshError):
        validate_attachment_order(baseline, CAPTURED + timedelta(days=1))


def test_attachment_order_accepts_aware_database_timestamp_string():
    baseline = row()
    baseline["scraped_at"] = baseline["scraped_at"].isoformat()
    validate_attachment_order(baseline, CAPTURED)
