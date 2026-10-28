from datetime import UTC, datetime

import pytest

from tests.ingestion.test_persistence import database as database_fixture
from tests.ingestion.test_persistence import launch, target
from trr_backend.ingestion.domain import Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.providers import PARSER_VERSION, InvalidSourceError, parse_evidence
from trr_backend.ingestion.queries import Queries
from trr_backend.ingestion.replay import Replay

database = database_fixture


def test_replay_uses_original_observation_without_source_or_writes(database, monkeypatch):
    store, tx = database
    args = target(store, launch(store))
    token = str(store.reserve(**args)["owner_token"])
    store.acquire(**args, owner_token=token)
    captured_at = datetime(2026, 1, 1, tzinfo=UTC)
    evidence = Evidence.capture(
        source_identity="instagram:post1",
        captured_at=captured_at,
        parser_version=PARSER_VERSION,
        payload={
            "shortcode": "post1",
            "pk": "1001",
            "taken_at": 1767225600,
            "owner": {"username": "bravotv"},
            "display_url": "https://cdn.example/image.jpg?token=secret",
            "caption": "Example",
            "headers": {"Cookie": "secret"},
        },
    )
    evidence_id = store.retain_evidence(**args, owner_token=token, evidence=evidence)
    import requests

    monkeypatch.setattr(requests.Session, "request", lambda *a, **k: pytest.fail("Replay called the source"))
    before = Queries(tx).progress(args["run_id"], "bravotv")
    preview = Replay(Queries(tx)).preview(evidence_id, "bravotv")
    assert preview["captured_at"] == captured_at
    assert preview["writes"] == preview["source_requests"] == 0
    assert "secret" not in str(preview)
    assert Queries(tx).progress(args["run_id"], "bravotv") == before
    with pytest.raises(LookupError):
        Replay(Queries(tx)).preview(evidence_id, "another_account")


def test_parser_rejects_identity_mismatch_and_unavailable_versions():
    evidence = Evidence.capture(
        source_identity="instagram:one",
        captured_at=datetime.now(UTC),
        parser_version=PARSER_VERSION,
        payload={"shortcode": "one", "pk": "1", "owner": {"username": "bravotv"}},
    )
    with pytest.raises(InvalidSourceError, match="identity"):
        parse_evidence(evidence, account="bravotv", stage=Stage.DETAILS, target_key="two")
    with pytest.raises(InvalidSourceError, match="version"):
        parse_evidence(evidence, account="bravotv", stage=Stage.DETAILS, target_key="one", parser_version="future")


def test_native_envelope_rich_content_and_comment_media_survive_safe_projection():
    now = datetime.now(UTC)
    source = {
        "data": {
            "xdt_shortcode_media": {
                "id": "1",
                "shortcode": "one",
                "owner": {"username": "bravotv"},
                "caption": {"text": "Public caption", "is_edited": True, "has_translation": True},
                "is_paid_partnership": True,
                "like_and_view_counts_disabled": True,
                "clips_metadata": {
                    "music_info": {
                        "music_asset_info": {
                            "title": "Public song",
                            "progressive_download_url": "https://cdn.example/audio.mp3?signature=private",
                        },
                        "viewer_token": "private",
                    }
                },
                "display_url": "https://cdn.example/post.jpg?signature=private",
                "session": "private",
                "friendship_status": {"following": True},
            }
        }
    }
    evidence = Evidence.capture(
        source_identity="instagram:one", captured_at=now, parser_version=PARSER_VERSION, payload=source
    )
    parsed = parse_evidence(evidence, account="bravotv", stage=Stage.DETAILS, target_key="one")
    assert parsed.post is not None
    assert parsed.post.caption.is_edited is True
    assert parsed.post.flags["paid_partnership"] is True
    assert parsed.post.music_info is not None
    assert parsed.post.music_info["music_asset_info"]["title"] == "Public song"
    assert "private" not in evidence.payload_json
    comments = Evidence.capture(
        source_identity="instagram:one:comments",
        captured_at=now,
        parser_version=PARSER_VERSION,
        payload={
            "comments": [
                {
                    "pk": "c1",
                    "text": "Public reply",
                    "owner": {"username": "reader"},
                    "gif": {"images": {"original": {"url": "https://cdn.example/gif.webp?token=private"}}},
                    "did_report_as_spam": True,
                    "cursor_payload": {"min_id": "opaque", "Cookie": "private"},
                }
            ]
        },
    )
    safe = comments.read()["comments"][0]
    assert safe["gif"]["images"]["original"]["url"] == "https://cdn.example/gif.webp"
    assert safe["cursor_payload"] == {"min_id": "opaque"}
    assert "did_report_as_spam" not in safe


def replay_source(stage, payload, *, parser, target="post1"):
    from uuid import uuid4

    from trr_backend.ingestion.replay import prepare_replay

    captured = datetime(2026, 10, 16, 12, tzinfo=UTC)
    evidence = Evidence.capture(
        source_identity=f"instagram:media:{target}" if stage == Stage.MEDIA else f"instagram:listing:bravotv:{target}",
        captured_at=captured,
        parser_version=parser,
        payload=payload,
    )
    retained = {"evidence": evidence, "stage": stage, "target_key": target, "run_id": str(uuid4()), "pinned": True}
    return prepare_replay(str(uuid4()), "bravotv", retained, parser_version=parser)


def native_media():
    from uuid import uuid4

    from trr_backend.ingestion.replay import MEDIA_REPLAY_VERSION

    return {
        "shortcode": "post1",
        "ingestion_media": {"version": MEDIA_REPLAY_VERSION, "observed_at": "2026-10-16T12:00:00Z"},
        "post_id": str(uuid4()),
        "canonical_post_id": str(uuid4()),
        "media_urls": [],
        "expected_media_count": 1,
        "comments_complete": False,
        "avatars_complete": False,
        "include_comment_avatars": False,
        "comments": [],
        "avatars": [],
        "media": [],
    }


def test_partial_media_inventory_preview_does_not_invent_completion_or_hosted_results():
    from trr_backend.ingestion.replay import MEDIA_REPLAY_VERSION

    preview, parsed = replay_source(Stage.MEDIA, native_media(), parser=MEDIA_REPLAY_VERSION)
    assert preview["normalized"]["inventory"]["comments_complete"] is False
    assert preview["normalized"]["inventory"]["avatars_complete"] is False
    assert preview["normalized"]["inventory"]["media_urls"] == []
    assert "hosted_media_urls" not in preview["normalized"]["inventory"]
    assert preview["normalized"]["apply_supported"] is False
    assert preview["writes"] == preview["source_requests"] == 0
    assert parsed.observed_at == datetime(2026, 10, 16, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    "change",
    [
        {"shortcode": "wrong"},
        {"post_id": "invalid"},
        {"canonical_post_id": None},
        {"ingestion_media": {"version": "future", "observed_at": "2026-10-16T12:00:00Z"}},
        {"ingestion_media": {"version": "instagram-media-v1", "observed_at": "2026-10-15T12:00:00Z"}},
        {"expected_media_count": True},
        {"comments_complete": None},
        {"media_urls": [None]},
    ],
)
def test_malformed_or_mismatched_media_inventory_is_rejected(change):
    from trr_backend.ingestion.replay import MEDIA_REPLAY_VERSION

    payload = native_media()
    payload.update(change)
    with pytest.raises(InvalidSourceError):
        replay_source(Stage.MEDIA, payload, parser=MEDIA_REPLAY_VERSION)


def test_listing_preview_rejects_missing_pagination_and_duplicate_post_identities():
    from trr_backend.ingestion.listing import ROOT_FIELD

    node = {"shortcode": "post1", "pk": "native1", "owner": {"username": "bravotv"}}
    payload = {"data": {ROOT_FIELD: {"edges": [{"node": node}], "page_info": {"has_next_page": True}}}}
    with pytest.raises(InvalidSourceError, match="cursor"):
        replay_source(Stage.LISTING, payload, parser=PARSER_VERSION, target="page:root")
    payload["data"][ROOT_FIELD]["page_info"] = {"has_next_page": False, "end_cursor": None}
    payload["data"][ROOT_FIELD]["edges"].append({"node": node})
    with pytest.raises(InvalidSourceError, match="duplicate"):
        replay_source(Stage.LISTING, payload, parser=PARSER_VERSION, target="page:root")
