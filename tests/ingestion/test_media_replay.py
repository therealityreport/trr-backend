"""Verified retained outputs replay with private SQL and mock storage only."""

import json
import re
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import requests

from tests.ingestion import test_media
from tests.ingestion.test_replay_commands import command, rows, target_state
from trr_backend.ingestion.domain import Conflict, LostOwnership, ReplayUnavailable
from trr_backend.ingestion.evidence import Evidence, sanitize_payload
from trr_backend.ingestion.media import AvatarSource, CommentMedia, legacy_comment_source_key, mirror_snapshot
from trr_backend.ingestion.providers import InvalidSourceError
from trr_backend.ingestion.queries import Queries
from trr_backend.ingestion.replay import (
    MEDIA_RESULT_REPLAY_VERSION,
    Replay,
    _decode_media_result,
    serialize_media_result,
)
from trr_backend.ingestion.replay_commands import ReplayCommands

database = test_media.database
media_database = test_media.media_database


@pytest.fixture(scope="module")
def replay_database(media_database):
    with media_database[1]() as conn, conn.cursor() as cur:
        cur.execute(
            (
                Path(__file__).parents[2] / "supabase/migrations/20261016070300_instagram_ingestion_replay_receipts.sql"
            ).read_text()
        )
    return media_database


def retained(database, *, comments=True):
    owned, token, source = test_media.launch_media(database, comments=comments)
    receipt = test_media.execute(database, owned, token, source)
    return owned, source, UUID(receipt["evidence_id"])


def test_result_round_trip_preserves_exact_proof_and_individual_observation_times():
    original = test_media.OBSERVED - timedelta(days=1)
    row_id = str(uuid4())
    source = test_media.snapshot(
        comments=(
            CommentMedia(
                "comment1",
                row_id,
                (test_media.SOURCE,),
                legacy_comment_source_key(row_id, (test_media.SOURCE,)),
                original,
            ),
        ),
        avatars=(
            AvatarSource("owner", test_media.SOURCE, "owner", observed_at=original),
            AvatarSource("tagged", test_media.SOURCE, "tagged", observed_at=original),
        ),
    )
    result = mirror_snapshot(source, test_media.storage(), check=test_media.noop, request=test_media.noop)
    payload = serialize_media_result(result)
    decoded = _decode_media_result(payload)
    assert decoded.post_fields == result.post_fields
    assert decoded.comment_fields == result.comment_fields and decoded.avatars == result.avatars
    assert decoded.snapshot.observed_at == source.observed_at
    assert decoded.snapshot.avatars[0].observed_at == original
    assert decoded.snapshot.comments[0].observed_at == original
    assert payload["media_result"]["asset_manifest"]["originals"]["cover"]["sha256"]
    # Variants keep exactly the fields storage returned: content-addressed variant
    # writes (98ccb8ed) report a hash, which must then be a valid digest.
    variants = payload["media_result"]["asset_manifest"]["display_variants"]
    assert variants and all(
        re.fullmatch(r"[a-f0-9]{64}", variant["sha256"]) for variant in variants if "sha256" in variant
    )


def summary_payload():
    result = mirror_snapshot(
        test_media.snapshot(), test_media.storage(), check=test_media.noop, request=test_media.noop
    )
    return serialize_media_result(result)


def test_fingerprint_and_count_gap_survive_sanitizer_without_viewer_state():
    fingerprint = {"version": "instagram-media-inventory-v1", "sha256": "a" * 64}
    settlement = {"basis": "public_native_exhaustion", "expected_total": 9, "observed_total": 1, "count_gap": 8}
    payload = sanitize_payload(
        {
            "inventory_fingerprint": fingerprint,
            "comment_settlement": settlement,
            "sessionid": "private",
            "viewer_state": {"id": "private"},
        }
    )
    assert payload == {"inventory_fingerprint": fingerprint, "comment_settlement": settlement}


def test_offline_result_cannot_apply_to_changed_comment_inventory(replay_database):
    owned, source, evidence = retained(replay_database, comments=False)
    store, tx = replay_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_comments(id,comment_id,post_id,username,text,scraped_at,last_seen_at)
            values(%s,'after-snapshot',%s,'reader','new inventory',%s,%s)""",
            (str(uuid4()), source.post_id, source.observed_at, source.observed_at),
        )
    with pytest.raises(Conflict, match="canonical data contract"):
        command(
            ReplayCommands(store),
            UUID(owned["run_id"]),
            evidence,
            MEDIA_RESULT_REPLAY_VERSION,
            apply=True,
            account="fixture_account",
        )
    assert rows(
        replay_database,
        "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s",
        (owned["run_id"],),
    ) == [(0,)]


@pytest.mark.parametrize("summary_kind", ["physical", "legacy"])
@pytest.mark.parametrize("shape", ["shared_original", "distinct_original_key_same_url", "shared_variant"])
def test_storage_summary_accepts_exact_physical_and_preserved_legacy_proof(summary_kind, shape):
    payload = summary_payload()
    manifest = payload["media_result"]["asset_manifest"]
    original = manifest["originals"]["cover"]
    byte_count = original["bytes"]
    assert all(item["bytes"] == byte_count for item in manifest["display_variants"])
    if shape == "distinct_original_key_same_url":
        manifest["originals"]["media"][0]["storage_key"] = "separate/original.png"
    elif shape == "shared_variant":
        first = manifest["display_variants"][0]
        shared = next(item for item in manifest["display_variants"][1:] if item["format"] == first["format"])
        for key in ("storage_key", "url", "bytes", "content_type", "width", "height"):
            shared[key] = first[key]
    original_count = 2 if shape == "distinct_original_key_same_url" else 1
    variant_count = 9 if shape == "shared_variant" else 10
    expected = {
        "original_bytes": byte_count * original_count,
        "variant_bytes": byte_count * variant_count,
        "object_count": original_count + variant_count,
        "variant_count": variant_count,
        # Distinct keys need not imply distinct retained URLs.
        "cdn_url_count": 1 + variant_count,
    }
    if summary_kind == "legacy":
        expected.update(
            original_bytes=2 * byte_count,
            variant_bytes=10 * byte_count,
            object_count=12,
            variant_count=10,
            cdn_url_count=12,
        )
    manifest["storage_summary"] = expected
    decoded = _decode_media_result(payload)
    assert decoded.post_fields["asset_manifest"]["storage_summary"] == expected
    assert decoded.snapshot.observed_at == test_media.OBSERVED
    assert decoded.post_fields["asset_manifest"]["originals"]["cover"] == original


@pytest.mark.parametrize("field", ["bytes", "sha256", "url", "format", "content_type", "width", "height"])
def test_storage_summary_rejects_conflicting_shared_key_metadata_before_deduplication(field):
    payload = summary_payload()
    manifest = payload["media_result"]["asset_manifest"]
    media = manifest["originals"]["media"][0]
    replacement = {
        "bytes": media["bytes"] + 1,
        "sha256": "0" * 64,
        "url": "https://media.example/conflicting.png",
        "format": "jpeg",
        "content_type": "image/jpeg",
        "width": 13,
        "height": 9,
    }[field]
    media[field] = replacement
    if field == "url":
        payload["media_result"]["post_fields"]["hosted_media_urls"][0] = replacement
    with pytest.raises(InvalidSourceError, match="shared object"):
        _decode_media_result(payload)


@pytest.mark.parametrize("tamper", ["hybrid", "mismatch", "boolean"])
def test_storage_summary_rejects_hybrid_arbitrary_and_boolean_totals(tamper):
    payload = summary_payload()
    summary = payload["media_result"]["asset_manifest"]["storage_summary"]
    if tamper == "hybrid":
        summary.update(object_count=12, cdn_url_count=12)
    elif tamper == "mismatch":
        summary["original_bytes"] += 1
    else:
        summary["object_count"] = True
    with pytest.raises(InvalidSourceError, match="summary differs"):
        _decode_media_result(payload)


@pytest.mark.parametrize(
    "tamper",
    [
        lambda p: p["media_result"]["asset_manifest"]["originals"]["cover"].pop("sha256"),
        lambda p: p["media_result"]["asset_manifest"]["display_variants"].pop(),
        lambda p: p["media_result"]["asset_manifest"]["storage_summary"].update(object_count=1),
        lambda p: p["media_result"]["post_fields"].update(caption="must never become a writer column"),
        lambda p: p["media_result"]["post_fields"].update(hosted_media_urls=[]),
        lambda p: p["media_result"]["post_fields"].update(hosted_thumbnail_url="https://127.0.0.1/private"),
        lambda p: p["media_result"]["asset_manifest"].update(updated_at="2026-10-15T12:00:00+00:00"),
        lambda p: p.update(media=[{"id": "cover_original", "storage_source_key": "different/key"}]),
        lambda p: p.update(comments_complete=False),
    ],
)
def test_result_decoder_denies_missing_invalid_or_unapproved_output(tamper):
    result = mirror_snapshot(
        test_media.snapshot(), test_media.storage(), check=test_media.noop, request=test_media.noop
    )
    payload = deepcopy(serialize_media_result(result))
    tamper(payload)
    with pytest.raises(InvalidSourceError):
        _decode_media_result(payload)


def test_offline_apply_restores_hosted_fields_and_receipt_without_settling_target(replay_database, monkeypatch):
    owned, source, evidence = retained(replay_database)
    store, tx = replay_database
    run = UUID(owned["run_id"])
    before = target_state(replay_database, run)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set hosted_media_urls='[]',hosted_thumbnail_url=null where id=%s",
            (source.post_id,),
        )
        cur.execute("update social.instagram_comments set hosted_media_urls='[]' where post_id=%s", (source.post_id,))
        cur.execute(
            "update social.social_post_media_assets set hosted_url=null where post_id=%s", (source.canonical_post_id,)
        )
    monkeypatch.setattr(requests.Session, "request", lambda *a, **k: pytest.fail("Replay requested network"))
    monkeypatch.setattr(test_media.MediaStorage, "host", lambda *a, **k: pytest.fail("Replay invoked storage"))
    preview = Replay(Queries(tx)).preview(str(evidence), "fixture_account")
    assert preview["parser_version"] == MEDIA_RESULT_REPLAY_VERSION
    assert preview["normalized"]["apply_supported"] is True
    assert preview["normalized"]["remote_objects_reverified"] is False
    key = str(uuid4())
    subject = ReplayCommands(store)
    result = command(
        subject, run, evidence, MEDIA_RESULT_REPLAY_VERSION, apply=True, account="fixture_account", key=key
    )
    assert result.receipt_id and result.source_requests == 0 and result.writes == 4
    assert result == command(
        subject, run, evidence, MEDIA_RESULT_REPLAY_VERSION, apply=True, account="fixture_account", key=key
    )
    assert target_state(replay_database, run) == before
    assert rows(
        replay_database,
        "select scraped_at,jsonb_array_length(hosted_media_urls) from social.instagram_posts where id=%s",
        (source.post_id,),
    ) == [(source.observed_at, 1)]
    assert rows(
        replay_database,
        "select text,jsonb_array_length(hosted_media_urls) from social.instagram_comments where post_id=%s",
        (source.post_id,),
    ) == [("Preserve comment", 1)]
    assert rows(
        replay_database,
        "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s and stage='media'",
        (str(run),),
    ) == [(1,)]
    assert rows(
        replay_database, "select count(*) from social.instagram_ingestion_evidence where run_id=%s", (str(run),)
    ) == [(2,)]


def test_media_apply_newer_guard_rolls_back_canonical_and_receipt(replay_database):
    owned, source, evidence = retained(replay_database, comments=False)
    store, tx = replay_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.social_post_media_assets set media_payload="
            "jsonb_set(media_payload,'{ingestion_media}',%s::jsonb,true) where post_id=%s",
            (
                json.dumps(
                    {
                        "version": "instagram-media-v1",
                        "observed_at": (source.observed_at + timedelta(hours=1)).isoformat(),
                    }
                ),
                source.canonical_post_id,
            ),
        )
        cur.execute("update social.instagram_posts set hosted_media_urls='[]' where id=%s", (source.post_id,))
    with pytest.raises(Conflict, match="newer"):
        command(
            ReplayCommands(store),
            UUID(owned["run_id"]),
            evidence,
            MEDIA_RESULT_REPLAY_VERSION,
            apply=True,
            account="fixture_account",
        )
    assert rows(
        replay_database, "select hosted_media_urls from social.instagram_posts where id=%s", (source.post_id,)
    ) == [([],)]
    assert rows(
        replay_database,
        "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s",
        (owned["run_id"],),
    ) == [(0,)]


def test_invalid_retained_output_never_reaches_writer(replay_database):
    owned, token, source = test_media.launch_media(replay_database)
    store, _ = replay_database
    store.acquire(**owned, owner_token=token)
    result = mirror_snapshot(source, test_media.storage(), check=test_media.noop, request=test_media.noop)
    payload = serialize_media_result(result)
    payload["media_result"]["post_fields"]["caption"] = "invalid column"
    evidence = store.retain_evidence(
        **owned,
        owner_token=token,
        evidence=Evidence.capture(
            source_identity=f"instagram:media:{source.shortcode}",
            captured_at=source.observed_at,
            parser_version=MEDIA_RESULT_REPLAY_VERSION,
            payload=payload,
        ),
    )
    with pytest.raises(ReplayUnavailable):
        command(
            ReplayCommands(store, media_writer=lambda *a, **k: pytest.fail("Invalid proof reached writer")),
            UUID(owned["run_id"]),
            UUID(evidence),
            MEDIA_RESULT_REPLAY_VERSION,
            apply=True,
            account="fixture_account",
        )


@pytest.mark.parametrize("failure", ["ownership", "storage"])
def test_result_capture_requires_owner_and_durable_evidence_before_canonical_commit(
    replay_database, monkeypatch, failure
):
    import trr_backend.ingestion.media as media_module

    owned, token, source = test_media.launch_media(replay_database)
    store, _ = replay_database
    if failure == "ownership":
        mirror = media_module.mirror_snapshot

        def cancel_after_mirror(*args, **kwargs):
            result = mirror(*args, **kwargs)
            store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
            return result

        monkeypatch.setattr(media_module, "mirror_snapshot", cancel_after_mirror)
        error = LostOwnership
    else:
        retain = store.retain_evidence

        def fail_result_capture(*args, evidence, **kwargs):
            if evidence.parser_version == MEDIA_RESULT_REPLAY_VERSION:
                raise OSError("Private evidence sink failed")
            return retain(*args, evidence=evidence, **kwargs)

        monkeypatch.setattr(store, "retain_evidence", fail_result_capture)
        error = OSError
    with pytest.raises(error):
        test_media.execute(replay_database, owned, token, source)
    assert rows(
        replay_database,
        "select parser_version from social.instagram_ingestion_evidence where run_id=%s",
        (owned["run_id"],),
    ) == [("instagram-media-v1",)]
    assert rows(
        replay_database,
        "select count(*) from social.instagram_ingestion_receipts where run_id=%s",
        (owned["run_id"],),
    ) == [(0,)]
    assert rows(
        replay_database, "select hosted_media_urls from social.instagram_posts where id=%s", (source.post_id,)
    ) == [([],)]
