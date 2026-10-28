"""Offline replay commands against isolated PostgreSQL, never configured data."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import psycopg2
import pytest

from api.schemas.v2.instagram_ingestion import InstagramReplayV2
from tests.ingestion import test_comments
from trr_backend.ingestion.comments import COMMENT_PARSER_VERSION
from trr_backend.ingestion.domain import Conflict, ReplayUnavailable, RunIntent, Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.providers import PARSER_VERSION
from trr_backend.ingestion.queries import Queries
from trr_backend.ingestion.replay import Replay
from trr_backend.ingestion.replay_commands import ReplayCommands

source_database = test_comments.database

NOW = datetime(2026, 10, 16, 12, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def database(source_database):
    store, transaction = source_database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute((ROOT / "supabase/migrations/20261016070300_instagram_ingestion_replay_receipts.sql").read_text())
    return store, transaction


def rows(database, query, params=()):
    _, transaction = database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def captured(database, stage=Stage.DETAILS, *, observed=NOW, caption="Original", partial=False):
    store, transaction = database
    run = store.start(
        RunIntent(account="bravotv"),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="replay-v1",
        initiated_by="admin",
    )
    shortcode = "post" + uuid4().hex
    max_attempts = 3 if stage == Stage.DETAILS else 12
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=stage,
        target_keys=[shortcode],
        max_attempts=max_attempts,
        cursor_state={},
        page_id="first",
        expected_cursor_state={},
        seal=True,
    )
    owned = {"run_id": run["run_id"], "account": "bravotv", "generation": 1, "stage": stage, "target_key": shortcode}
    owned["owner_token"] = str(store.reserve(**owned)["owner_token"])
    store.acquire(**owned)
    payload = {
        "shortcode": shortcode,
        "pk": "1001",
        "taken_at": int(observed.timestamp()),
        "owner": {"username": "bravotv"},
        "caption": caption,
        "like_count": 12,
        "comment_count": 4,
        "display_url": "https://cdn.example/post.jpg?signature=private",
        "media_type": 1,
    }
    if partial:
        payload.pop("comment_count")
    parser = PARSER_VERSION
    if stage == Stage.COMMENTS:
        payload = {
            "comments": [
                {
                    "pk": "comment1",
                    "text": "Captured comment",
                    "created_at": int(observed.timestamp()),
                    "user": {"username": "reader"},
                    "comment_like_count": 2,
                    "child_comment_count": 0,
                }
            ],
            "has_more_comments": True,
            "next_max_id": "next",
            "comment_count": 20,
        }
        parser = COMMENT_PARSER_VERSION
        with transaction() as conn, conn.cursor() as cur:
            cur.execute(
                "insert into social.instagram_posts(shortcode,username,scraped_at) values(%s,'bravotv',%s)",
                (shortcode, observed),
            )
    evidence = Evidence.capture(
        source_identity=f"instagram:{stage}:{shortcode}", captured_at=observed, parser_version=parser, payload=payload
    )
    evidence_id = store.retain_evidence(**owned, evidence=evidence)
    return UUID(run["run_id"]), UUID(evidence_id), shortcode, parser


def command(subject, run, evidence, parser, *, key=None, apply=False, account="bravotv", actor: str | None = "admin"):
    return subject(
        account,
        run,
        InstagramReplayV2(evidence_id=evidence, parser_version=parser, apply=apply),
        idempotency_key=key or str(uuid4()),
        initiated_by=actor,
    )


def target_state(database, run):
    return rows(
        database,
        "select stage,target_key,outcome,attempts,requests,checkpoint,owner_token,committed_at "
        "from social.instagram_ingestion_targets where run_id=%s order by stage,target_key",
        (str(run),),
    )


def test_dry_run_is_durable_idempotent_and_has_no_source_or_canonical_effects(database, monkeypatch):
    import requests

    monkeypatch.setattr(requests.Session, "request", lambda *a, **k: pytest.fail("Replay requested source data"))
    run, evidence, shortcode, parser = captured(database)
    store, _ = database
    subject = ReplayCommands(store, detail_writer=lambda *a, **k: pytest.fail("Dry run invoked canonical writer"))
    before = target_state(database, run)
    key = str(uuid4())
    result = command(subject, run, evidence, parser, key=key)
    assert result.mode == "dry_run" and result.writes == result.source_requests == 0
    assert result.receipt_id is None and result.captured_at == NOW
    assert result.comparison.previous == result.comparison.current
    assert result.comparison.changed is False
    assert isinstance(result.model_dump(mode="json")["captured_at"], str)
    assert result == command(subject, run, evidence, parser, key=key)
    assert rows(
        database,
        "select count(*) from social.instagram_ingestion_commands where account='bravotv' and idempotency_key=%s",
        (key,),
    ) == [(1,)]
    assert rows(
        database, "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s", (str(run),)
    ) == [(0,)]
    assert rows(database, "select count(*) from social.instagram_posts where shortcode=%s", (shortcode,)) == [(0,)]
    assert target_state(database, run) == before
    assert "private" not in result.model_dump_json()
    # Retention may remove the payload after successful preview. Its persisted
    # response remains stable without re-reading or re-parsing missing bytes.
    _, transaction = database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_evidence set payload=null where evidence_id=%s", (str(evidence),)
        )
    assert result == command(subject, run, evidence, parser, key=key)
    with pytest.raises(ReplayUnavailable):
        command(subject, run, evidence, parser)


def test_apply_has_real_receipt_original_timestamp_and_concurrent_idempotency(database):
    run, evidence, shortcode, parser = captured(database)
    store, _ = database
    subject = ReplayCommands(store)
    store.control(str(run), "bravotv", action="cancel", idempotency_key=str(uuid4()))
    before = target_state(database, run)
    key = str(uuid4())
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: command(subject, run, evidence, parser, key=key, apply=True), range(8)))
    assert all(result == results[0] for result in results)
    assert results[0].mode == "apply" and results[0].writes == 1 and results[0].receipt_id
    assert rows(database, "select scraped_at,caption from social.instagram_posts where shortcode=%s", (shortcode,)) == [
        (NOW, "Original")
    ]
    assert rows(
        database,
        "select captured_at,receipt_id from social.instagram_ingestion_replay_receipts where run_id=%s",
        (str(run),),
    ) == [(NOW, str(results[0].receipt_id))]
    assert rows(
        database, "select count(*) from social.social_post_observations where scrape_run_id=%s", (str(run),)
    ) == [(2,)]
    assert rows(database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (str(run),)) == [
        (0,)
    ]
    assert target_state(database, run) == before
    with pytest.raises(Conflict):
        command(subject, run, evidence, parser, key=key, apply=False)
    with pytest.raises(Conflict):
        command(subject, run, evidence, "different-parser", key=key, apply=True)


def test_receipt_failure_rolls_back_canonical_write_and_command_then_retry_succeeds(database):
    run, evidence, shortcode, parser = captured(database)
    store, _ = database
    subject = ReplayCommands(store)
    key = str(uuid4())
    with pytest.raises(psycopg2.IntegrityError):
        command(subject, run, evidence, parser, key=key, apply=True, actor=None)
    assert rows(database, "select count(*) from social.instagram_posts where shortcode=%s", (shortcode,)) == [(0,)]
    assert rows(
        database, "select count(*) from social.instagram_ingestion_commands where idempotency_key=%s", (key,)
    ) == [(0,)]
    assert rows(
        database, "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s", (str(run),)
    ) == [(0,)]
    assert command(subject, run, evidence, parser, key=key, apply=True).receipt_id


@pytest.mark.parametrize("wrong_account", [False, True])
def test_wrong_run_or_account_is_rejected_before_effects(database, wrong_account):
    run, evidence, shortcode, parser = captured(database)
    store, _ = database
    subject = ReplayCommands(store, detail_writer=lambda *a, **k: pytest.fail("Foreign evidence reached writer"))
    with pytest.raises(LookupError):
        command(
            subject,
            run if wrong_account else uuid4(),
            evidence,
            parser,
            apply=True,
            account="unrelated" if wrong_account else "bravotv",
        )
    assert rows(database, "select count(*) from social.instagram_posts where shortcode=%s", (shortcode,)) == [(0,)]
    assert rows(
        database, "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s", (str(run),)
    ) == [(0,)]


def test_unrecognized_media_payload_fails_explicitly(database):
    run, evidence, _, parser = captured(database, Stage.MEDIA)
    store, _ = database
    with pytest.raises(ReplayUnavailable, match="cannot satisfy this replay parser"):
        command(ReplayCommands(store), run, evidence, parser, apply=True)


def test_older_post_capture_cannot_overwrite_newer_canonical_observation(database):
    run, evidence, shortcode, parser = captured(database)
    store, transaction = database
    subject = ReplayCommands(store)
    command(subject, run, evidence, parser, apply=True)
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set scraped_at=%s,caption='Newer' where shortcode=%s",
            (NOW + timedelta(hours=1), shortcode),
        )
    with pytest.raises(Conflict, match="predates"):
        command(subject, run, evidence, parser, apply=True)
    assert rows(database, "select caption from social.instagram_posts where shortcode=%s", (shortcode,)) == [("Newer",)]
    assert rows(
        database, "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s", (str(run),)
    ) == [(1,)]


def test_comments_replay_stays_partial_and_uses_native_parser_without_missing_marking(database):
    run, evidence, shortcode, parser = captured(database, Stage.COMMENTS)
    store, transaction = database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at) "
            "select id,'unseen','reader','Preserve unseen',%s from social.instagram_posts where shortcode=%s",
            (NOW - timedelta(days=1), shortcode),
        )
    preview = Replay(Queries(transaction)).preview(str(evidence), "bravotv", parser_version=parser)
    assert preview["normalized"][0]["comment_id"] == "comment1"
    assert not preview["comparison"]["changed"]
    subject = ReplayCommands(store)
    result = command(subject, run, evidence, parser, apply=True)
    assert result.writes == 1 and result.captured_at == NOW
    assert rows(
        database,
        "select c.text,c.scraped_at,c.is_missing from social.instagram_comments c "
        "join social.instagram_posts p on p.id=c.post_id where p.shortcode=%s and c.comment_id='comment1'",
        (shortcode,),
    ) == [("Captured comment", NOW, False)]
    assert rows(
        database,
        "select c.is_missing from social.instagram_comments c join social.instagram_posts p on p.id=c.post_id "
        "where p.shortcode=%s and c.comment_id='unseen'",
        (shortcode,),
    ) == [(False,)]
    assert target_state(database, run)[0][2] == "unresolved"
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_comments c set scraped_at=%s,text='Newer' from social.instagram_posts p "
            "where p.id=c.post_id and p.shortcode=%s and c.comment_id='comment1'",
            (NOW + timedelta(hours=1), shortcode),
        )
    with pytest.raises(Conflict, match="predates"):
        command(subject, run, evidence, parser, apply=True)
    assert rows(
        database, "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s", (str(run),)
    ) == [(1,)]


def test_partial_detail_can_be_previewed_but_cannot_be_applied(database, monkeypatch):
    import requests

    monkeypatch.setattr(requests.Session, "request", lambda *a, **k: pytest.fail("Replay requested source data"))
    run, evidence, shortcode, parser = captured(database, partial=True)
    store, _ = database
    subject = ReplayCommands(
        store, detail_writer=lambda *a, **k: pytest.fail("Partial replay reached canonical writer")
    )
    before = target_state(database, run)
    key = str(uuid4())
    preview = command(subject, run, evidence, parser, key=key)
    assert preview.mode == "dry_run" and preview.writes == preview.source_requests == 0
    assert preview.receipt_id is None
    assert isinstance(preview.normalized, dict)
    assert preview.normalized["shortcode"] == shortcode
    assert preview.comparison.previous == preview.comparison.current
    assert preview == command(subject, run, evidence, parser, key=key)
    with pytest.raises(ReplayUnavailable):
        command(subject, run, evidence, parser, apply=True)
    assert target_state(database, run) == before
    assert rows(database, "select count(*) from social.instagram_posts where shortcode=%s", (shortcode,)) == [(0,)]
    assert rows(
        database, "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s", (str(run),)
    ) == [(0,)]


def capture_page(database, stage, payload, *, target_key, parser=PARSER_VERSION, intent=None):
    store, _ = database
    run = store.start(
        intent or RunIntent(account="bravotv"),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="replay-page-v1",
        initiated_by="fixture",
    )
    budget = 3 if stage in {Stage.DETAILS, Stage.MEDIA} else 12
    store.discover(
        run["run_id"],
        "bravotv",
        generation=1,
        stage=stage,
        target_keys=[target_key],
        max_attempts=budget,
        cursor_state={},
        expected_cursor_state={},
        page_id="source",
        seal=True,
    )
    owned = {"run_id": run["run_id"], "account": "bravotv", "generation": 1, "stage": stage, "target_key": target_key}
    owned["owner_token"] = str(store.reserve(**owned)["owner_token"])
    store.acquire(**owned)
    evidence = Evidence.capture(
        source_identity=f"instagram:media:{target_key}"
        if stage == Stage.MEDIA
        else f"instagram:listing:bravotv:{target_key}",
        captured_at=NOW,
        parser_version=parser,
        payload=payload,
    )
    evidence_id = store.retain_evidence(**owned, evidence=evidence)
    return UUID(run["run_id"]), UUID(evidence_id)


def listing_post(shortcode, *, posted=NOW) -> dict[str, Any]:
    return {
        "id": "native-" + shortcode,
        "shortcode": shortcode,
        "owner": {"username": "bravotv"},
        "taken_at": int(posted.timestamp()),
        "display_url": "https://cdn.example/image.jpg?signature=private",
        "caption": "Listing capture",
        "like_count": 0,
        "comment_count": 0,
    }


def listing_page(posts) -> dict[str, Any]:
    from trr_backend.ingestion.listing import ROOT_FIELD

    return {
        "data": {
            ROOT_FIELD: {
                "edges": [{"node": post} for post in posts],
                "page_info": {"has_next_page": True, "end_cursor": "next-page"},
            }
        }
    }


def test_listing_page_preview_and_apply_replay_all_posts_without_discovery_or_settlement(database, monkeypatch):
    import requests

    monkeypatch.setattr(requests.Session, "request", lambda *args, **kwargs: pytest.fail("Replay called source"))
    codes = ["A" + uuid4().hex, "B" + uuid4().hex]
    run, evidence = capture_page(
        database, Stage.LISTING, listing_page([listing_post(code) for code in codes]), target_key="page:root"
    )
    store, tx = database
    subject = ReplayCommands(store)
    targets_before = target_state(database, run)
    manifests_before = rows(
        database,
        """select stage,discovery_complete,cursor_state,sealed_at
        from social.instagram_ingestion_manifests where run_id=%s order by stage""",
        (str(run),),
    )
    preview = command(subject, run, evidence, PARSER_VERSION)
    assert isinstance(preview.normalized, dict)
    posts = cast(list[dict[str, Any]], preview.normalized["posts"])
    assert [post["shortcode"] for post in posts] == codes
    assert preview.normalized["pagination"] == {"has_next_page": True, "end_cursor": "next-page"}
    assert preview.comparison.previous == preview.comparison.current
    assert preview.writes == preview.source_requests == 0
    assert not preview.comparison.changed
    assert "private" not in preview.model_dump_json()
    applied = command(subject, run, evidence, PARSER_VERSION, apply=True)
    assert applied.writes == 2 and applied.source_requests == 0 and applied.receipt_id
    assert rows(
        database,
        "select shortcode,scraped_at from social.instagram_posts where shortcode=any(%s) order by shortcode",
        (codes,),
    ) == [(code, NOW) for code in codes]
    assert target_state(database, run) == targets_before
    assert (
        rows(
            database,
            """select stage,discovery_complete,cursor_state,sealed_at
        from social.instagram_ingestion_manifests where run_id=%s order by stage""",
            (str(run),),
        )
        == manifests_before
    )
    assert rows(database, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (str(run),)) == [
        (0,)
    ]


def test_listing_apply_uses_frozen_date_window(database):
    selected, excluded = "In" + uuid4().hex, "Out" + uuid4().hex
    intent = RunIntent(account="bravotv", scope="bounded_window", date_start=NOW - timedelta(days=2), date_end=NOW)
    run, evidence = capture_page(
        database,
        Stage.LISTING,
        listing_page(
            [
                listing_post(selected, posted=NOW - timedelta(days=1)),
                listing_post(excluded),
            ]
        ),
        target_key="page:root",
        intent=intent,
    )
    result = command(ReplayCommands(database[0]), run, evidence, PARSER_VERSION, apply=True)
    assert result.writes == 1
    assert rows(
        database, "select shortcode from social.instagram_posts where shortcode=any(%s)", ([selected, excluded],)
    ) == [(selected,)]


def test_listing_page_apply_is_atomic_when_one_post_has_newer_observation(database):
    first, newer = "A" + uuid4().hex, "Z" + uuid4().hex
    run, evidence = capture_page(
        database, Stage.LISTING, listing_page([listing_post(first), listing_post(newer)]), target_key="page:root"
    )
    store, tx = database
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_posts(shortcode,username,caption,scraped_at)
            values(%s,'bravotv','Preserve newer',%s)""",
            (newer, NOW + timedelta(hours=1)),
        )
    before = target_state(database, run)
    with pytest.raises(Conflict, match="predates"):
        command(ReplayCommands(store), run, evidence, PARSER_VERSION, apply=True)
    assert rows(
        database, "select shortcode,caption from social.instagram_posts where shortcode=any(%s)", ([first, newer],)
    ) == [(newer, "Preserve newer")]
    assert rows(
        database, "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s", (str(run),)
    ) == [(0,)]
    assert target_state(database, run) == before


def test_legacy_single_post_listing_can_apply_without_detail_coverage_claim(database):
    run, evidence, shortcode, parser = captured(database, Stage.LISTING, partial=True)
    result = command(ReplayCommands(database[0]), run, evidence, parser, apply=True)
    assert result.writes == 1 and result.source_requests == 0
    assert rows(database, "select shortcode from social.instagram_posts where shortcode=%s", (shortcode,)) == [
        (shortcode,)
    ]
    assert target_state(database, run)[0][2] == "unresolved"


def media_inventory(shortcode) -> dict[str, Any]:
    from trr_backend.ingestion.replay import MEDIA_REPLAY_VERSION

    return {
        "shortcode": shortcode,
        "ingestion_media": {"version": MEDIA_REPLAY_VERSION, "observed_at": NOW.isoformat()},
        "post_id": str(uuid4()),
        "canonical_post_id": str(uuid4()),
        "show_id": str(uuid4()),
        "season_number": 1,
        "media_urls": ["https://cdn.example/photo.jpg?signature=private"],
        "thumbnail_url": "https://cdn.example/photo.jpg",
        "expected_media_count": 1,
        "comments_complete": True,
        "avatars_complete": True,
        "include_comment_avatars": False,
        "media": [{"id": "cover_original", "storage_source_key": "show/season/post/cover.jpg"}],
        "comments": [],
        "avatars": [],
    }


def test_media_inventory_preview_compares_offline_and_apply_explains_missing_hosted_results(database, monkeypatch):
    import requests

    from trr_backend.ingestion.replay import MEDIA_REPLAY_VERSION

    monkeypatch.setattr(requests.Session, "request", lambda *args, **kwargs: pytest.fail("Replay called CDN/source"))
    shortcode = "Media" + uuid4().hex
    run, evidence = capture_page(
        database, Stage.MEDIA, media_inventory(shortcode), target_key=shortcode, parser=MEDIA_REPLAY_VERSION
    )
    store, tx = database
    before = target_state(database, run)
    subject = ReplayCommands(
        store,
        detail_writer=lambda *args, **kwargs: pytest.fail("Media reached post writer"),
        comment_writer=lambda *args, **kwargs: pytest.fail("Media reached comment writer"),
    )
    preview = command(subject, run, evidence, MEDIA_REPLAY_VERSION)
    assert preview.mode == "dry_run" and preview.writes == preview.source_requests == 0
    assert isinstance(preview.normalized, dict)
    assert isinstance(preview.normalized["inventory"], dict)
    assert preview.normalized["inventory"]["media_urls"] == ["https://cdn.example/photo.jpg"]
    assert preview.normalized["apply_supported"] is False
    assert isinstance(preview.normalized["apply_limitation"], str)
    assert "hosted-result URLs" in preview.normalized["apply_limitation"]
    assert preview.comparison.previous == preview.comparison.current
    assert "private" not in preview.model_dump_json()
    from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION

    assert Replay(Queries(tx)).preview(str(evidence), "bravotv")["parser_version"] == MEDIA_RESULT_REPLAY_VERSION
    with pytest.raises(ReplayUnavailable, match="hosted-result URLs"):
        command(subject, run, evidence, MEDIA_REPLAY_VERSION, apply=True)
    assert target_state(database, run) == before
    assert rows(
        database, "select count(*) from social.instagram_ingestion_replay_receipts where run_id=%s", (str(run),)
    ) == [(0,)]
    assert rows(database, "select count(*) from social.instagram_posts where shortcode=%s", (shortcode,)) == [(0,)]


def test_media_source_unavailable_proof_can_be_previewed_but_never_settles_target(database):
    from trr_backend.ingestion.replay import MEDIA_REPLAY_VERSION

    shortcode = "Media" + uuid4().hex
    run, evidence = capture_page(
        database,
        Stage.MEDIA,
        {"shortcode": shortcode, "status": "media_unavailable"},
        target_key=shortcode,
        parser="source-proof-v1",
    )
    before = target_state(database, run)
    result = command(ReplayCommands(database[0]), run, evidence, MEDIA_REPLAY_VERSION)
    assert isinstance(result.normalized, dict)
    assert result.normalized["source_unavailable"] == "media_unavailable"
    assert result.comparison.changed is False
    assert target_state(database, run) == before
