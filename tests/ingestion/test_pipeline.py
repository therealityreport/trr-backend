"""One connected local pipeline with retained fake sources and in-memory storage.

Uses classified existing posts so the real media inventory has its required
show/season storage context. Provider transports, Modal and DBOS SDK are absent;
the real scheduler, fencing, activities and PostgreSQL persistence are exercised.
"""

import copy
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import requests
from psycopg2.extras import RealDictCursor

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from tests.ingestion import test_comments, test_media
from trr_backend.ingestion.command_service import CommandService
from trr_backend.ingestion.comments import CommentFetchSummary
from trr_backend.ingestion.listing import ROOT_FIELD
from trr_backend.ingestion.media import MediaStorage
from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader
from trr_backend.ingestion.queries import Queries
from trr_backend.ingestion.worker import Worker
from trr_backend.ingestion.workflow_store import WorkflowStore
from trr_backend.ingestion.workflows import Scheduler

base_database = test_comments.database
ROOT = Path(__file__).resolve().parents[2]
ACCOUNT = "pipeline_account"
CODES = ("PipelineA", "PipelineB")
CDN = "https://scontent.cdninstagram.com"
HOSTED = "https://media.example/"


@pytest.fixture(scope="module")
def pipeline_database(base_database):
    _, transaction = base_database
    with transaction() as conn, conn.cursor() as cur:
        cur.execute((ROOT / "supabase/migrations/20261016070100_instagram_ingestion_workflows.sql").read_text())
        cur.execute("""alter table core.seasons add column show_id uuid,add column season_number integer;
            alter table social.scrape_jobs add column config jsonb;
            create table social.avatar_registry(id uuid primary key default gen_random_uuid(),platform text,
            account_handle text,source_url text,content_hash text,hosted_url text,status text,failure_reason text,
            last_checked_at timestamptz,created_at timestamptz default now(),updated_at timestamptz,
            unique(platform,account_handle,source_url));""")
    return WorkflowStore(transaction), transaction


def rows(database, statement, params=()):
    with database[1]() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(statement, params)
        return [dict(row) for row in cur.fetchall()] if cur.description else []


class FakeStorageClient(test_media.Storage):
    def __init__(self):
        super().__init__()
        self.calls = Counter()
        self.closes = 0

    def head_object(self, **kwargs):
        self.calls["head"] += 1
        return super().head_object(**kwargs)

    def put_object(self, **kwargs):
        self.calls["put"] += 1
        return super().put_object(**kwargs)

    def get_object(self, **kwargs):
        self.calls["get"] += 1
        return super().get_object(**kwargs)

    def close(self):
        self.closes += 1


def test_complete_pipeline_survives_lost_listing_ack_and_generation_resume(pipeline_database, monkeypatch):
    database = pipeline_database
    store, transaction = database
    monkeypatch.setattr(requests.Session, "request", lambda *a, **k: pytest.fail("Unexpected source network"))
    show_id, season_id = str(uuid4()), str(uuid4())
    legacy_ids = {code: str(uuid4()) for code in CODES}
    with transaction() as conn, conn.cursor() as cur:
        # Existing classification is independent of ingestion and must survive
        # its listing/detail upserts. No completion state is fabricated here.
        cur.execute("insert into core.shows values(%s)", (show_id,))
        cur.execute("insert into core.seasons(id,show_id,season_number) values(%s,%s,3)", (season_id, show_id))
        for code in CODES:
            cur.execute(
                """insert into social.instagram_posts
                (id,shortcode,username,source_account,show_id,season_id,scraped_at)
                values(%s,%s,%s,%s,%s,%s,%s)""",
                (legacy_ids[code], code, ACCOUNT, ACCOUNT, show_id, season_id, datetime.now(UTC) - timedelta(days=2)),
            )
    api = CommandService(
        store,
        Queries(transaction),
        engine="dbos",
        workflow_version="pipeline-v1",
        enabled=True,
        capacity=lambda *_: None,
    )
    command = api.launch(
        ACCOUNT, InstagramIngestionLaunchV2(), idempotency_key="pipeline-start", initiated_by="fixture"
    )
    run_id = str(command.run_id)
    source_calls = Counter()
    source_time = int((datetime.now(UTC) - timedelta(days=1)).timestamp())

    def post(code, *, detail=False):
        return {
            "shortcode": code,
            "pk": "native-" + code,
            "taken_at": source_time,
            "media_type": 1,
            "owner": {"username": ACCOUNT, "profile_pic_url": CDN + "/owner.png?signature=private"},
            "caption": ("Detail " if detail else "Listed ") + code,
            "like_count": 7 if detail else 2,
            "comment_count": 2,
            "display_url": CDN + "/" + code + ".png?signature=private",
        }

    def listing_source(context, owned):
        def fetch(**callbacks):
            callbacks["pre_source_request"]()
            cursor = callbacks["cursor"]
            assert cursor in (None, "after-first")
            source_calls[("listing", cursor)] += 1
            payload = {
                "data": {
                    ROOT_FIELD: {
                        "edges": [{"node": post(CODES[0] if cursor is None else CODES[1])}],
                        "page_info": {
                            "has_next_page": cursor is None,
                            "end_cursor": "after-first" if cursor is None else None,
                        },
                    }
                }
            }
            callbacks["source_observer"](payload, {"cookies": {"session": "private"}})
            return payload

        return fetch

    def detail_source(context, owned):
        def fetch(**callbacks):
            callbacks["check_cancelled"]()
            callbacks["on_request"]()
            source_calls[("post_details", owned["target_key"])] += 1
            payload = post(owned["target_key"], detail=True)
            callbacks["source_observer"]("details", payload)
            return payload

        return fetch

    def comment_source(context, owned):
        def fetch(**callbacks):
            assert callbacks["shortcode"] == owned["target_key"]
            code = owned["target_key"]
            parent_id, reply_id = code + "-parent", code + "-reply"
            pages = [
                (
                    "comments_api",
                    {
                        "comments": [
                            {
                                "pk": parent_id,
                                "text": "Parent " + code,
                                "created_at": source_time,
                                "comment_like_count": 3,
                                "child_comment_count": 1,
                                "user": {"username": "reader", "profile_pic_url": CDN + "/reader.png"},
                                "comment_gif": {"url": CDN + "/comment.png?signature=private"},
                            }
                        ],
                        "comment_count": 2,
                        "has_more_comments": False,
                        "has_more_headload_comments": False,
                    },
                ),
                (
                    "graphql_replies",
                    {
                        "parent_comment_id": parent_id,
                        "child_comments": [
                            {
                                "pk": reply_id,
                                "text": "Reply " + code,
                                "created_at": source_time,
                                "comment_like_count": 1,
                                "child_comment_count": 0,
                                "user": {"username": "replier", "profile_pic_url": CDN + "/replier.png"},
                            }
                        ],
                        "has_more_tail_child_comments": False,
                    },
                ),
            ]
            for kind, payload in pages:
                callbacks["pre_source_request"]()
                source_calls[("comments", code)] += 1
                callbacks["source_observer"](kind, copy.deepcopy(payload))
            return CommentFetchSummary(expected_count=2, full_observation=True)

        return fetch

    client = FakeStorageClient()
    downloads = []

    def opener(*args, **kwargs):
        downloads.append(args[1])
        return test_media.Response()

    def storage(context, owned):
        assert owned["stage"] == "media"
        assert context["target"]["owner_expires_at"] is not None
        return MediaStorage(
            client=client,
            bucket="pipeline",
            build_hosted_url=lambda key: HOSTED + key,
            opener=opener,
            variant_generator=test_media.quick_variants,
        )

    def build_worker():
        loader = MediaSnapshotLoader(transaction, include_comment_avatars=True)
        return Worker(
            store,
            listing_source=listing_source,
            detail_source=detail_source,
            comment_source=comment_source,
            media_storage=storage,
            media_loader=lambda context, owned, **callbacks: loader.load(
                **{key: owned[key] for key in ("run_id", "account", "generation", "target_key")}, **callbacks
            ),
        )

    executions = []
    initial_reservations = []
    worker = build_worker()

    def lost_ack(reservation):
        initial_reservations.append(reservation)
        executions.append(worker.execute(reservation))
        # The activity committed its receipt; the caller loses the remote ack.
        raise RuntimeError("Simulated interruption after durable activity completion")

    initial_item = dict(command.model_dump(mode="json"), account=ACCOUNT)
    initial_partition = store.seed(initial_item)
    scheduler = Scheduler(
        store,
        admit=lambda _: True,
        dispatch=lost_ack,
        reconcile=lambda _: pytest.fail("Completed receipt must precede remote inspection"),
    )
    assert scheduler.tick(initial_partition, 0) == {"sleep": 1.0}
    assert executions == [{"outcome": "committed", "stage": "listing"}]
    # A new coordinator/worker reads the same durable decision and receipt.
    worker = build_worker()
    scheduler = Scheduler(
        store,
        admit=lambda _: True,
        dispatch=lost_ack,
        reconcile=lambda _: pytest.fail("Completed listing was re-dispatched"),
    )
    assert scheduler.tick(initial_partition, 0) == {"ready": True}
    assert source_calls[("listing", None)] == 1
    assert not any(stage.discovery_complete for stage in api.progress(ACCOUNT, command.run_id).stages)
    root_receipt = rows(
        database, "select receipt_id from social.instagram_ingestion_receipts where run_id=%s", (run_id,)
    )[0]
    api.control(ACCOUNT, command.run_id, action="cancel", idempotency_key="pipeline-cancel", initiated_by="fixture")
    assert worker.execute(initial_reservations[0])["outcome"] == "fenced"
    resumed = api.control(
        ACCOUNT, command.run_id, action="resume", idempotency_key="pipeline-resume", initiated_by="fixture"
    )
    assert resumed.generation == 3
    worker = build_worker()

    def dispatch(reservation):
        executions.append(worker.execute(reservation))
        return "local:" + reservation["owner_token"]

    item = dict(resumed.model_dump(mode="json"), account=ACCOUNT)
    partition = store.seed(item)
    scheduler = Scheduler(store, admit=lambda _: True, dispatch=dispatch, reconcile=lambda _: "unknown")
    for ordinal in range(16):
        result = scheduler.tick(partition, ordinal)
        assert all(entry["outcome"] == "committed" for entry in executions), executions
        if result.get("stop"):
            assert result["stop"] == "completed"
            store.complete_partition(partition, continue_run=False, stop_state=result["stop"])
            break
        assert result == {"ready": True}, result
    else:
        pytest.fail("Connected pipeline did not finish within its bounded scheduling steps")

    progress = api.progress(ACCOUNT, command.run_id)
    assert progress.complete and progress.state == "completed" and progress.generation == 3
    assert {
        stage.stage: (stage.total, stage.committed, stage.unresolved, stage.attempts) for stage in progress.stages
    } == dict.fromkeys(("listing", "post_details", "comments", "media"), (2, 2, 0, 2))
    assert all(stage.discovery_complete and stage.complete and stage.sealed_at for stage in progress.stages)
    assert Counter(entry["stage"] for entry in executions) == Counter(
        dict.fromkeys(("listing", "post_details", "comments", "media"), 2)
    )
    assert source_calls == Counter(
        {
            ("listing", None): 1,
            ("listing", "after-first"): 1,
            **{("post_details", code): 1 for code in CODES},
            **{("comments", code): 2 for code in CODES},
        }
    )
    assert {stage.stage: stage.requests for stage in progress.stages if stage.stage != "media"} == {
        "listing": 2,
        "post_details": 2,
        "comments": 4,
    }
    assert next(stage.requests for stage in progress.stages if stage.stage == "media") == len(downloads) + sum(
        client.calls.values()
    )
    # Two post originals, twenty variants, two comment images and three
    # shared avatar identities. Exact cover/original URLs share one physical
    # object while preserving their separate manifest roles.
    assert client.closes == 2 and len(client.objects) == 27
    avatar_urls = {
        row["hosted_url"]
        for row in rows(database, "select hosted_url from social.avatar_registry where status='mirrored'")
    }
    assert len(avatar_urls) == 3
    assert all(url.removeprefix(HOSTED) in client.objects for url in avatar_urls)
    assert rows(
        database, "select count(*) as count from social.instagram_ingestion_receipts where run_id=%s", (run_id,)
    ) == [{"count": 8}]
    assert rows(
        database, "select count(*) as count from social.instagram_ingestion_evidence where run_id=%s", (run_id,)
    ) == [{"count": 12}]
    assert rows(
        database, "select count(*) as count from social.instagram_ingestion_comment_pages where run_id=%s", (run_id,)
    ) == [{"count": 4}]
    assert root_receipt in rows(
        database, "select receipt_id from social.instagram_ingestion_receipts where run_id=%s", (run_id,)
    )
    assert rows(
        database,
        "select count(*) as count from social.instagram_ingestion_targets where run_id=%s and outcome='unresolved'",
        (run_id,),
    ) == [{"count": 0}]

    for code in CODES:
        post_row = rows(database, "select * from social.instagram_posts where shortcode=%s", (code,))[0]
        canonical = rows(
            database, "select * from social.social_posts where source_id=%s and platform='instagram'", (code,)
        )[0]
        assert str(post_row["id"]) == legacy_ids[code] and str(canonical["id"]) != legacy_ids[code]
        assert post_row["caption"] == "Detail " + code and post_row["likes"] == 7
        assert str(post_row["show_id"]) == show_id and str(post_row["season_id"]) == season_id
        assert post_row["media_mirror_status"] == "mirrored"
        expected_media = f"{HOSTED}social/instagram/{show_id}/3/week-unknown/{code}/cover-original.png"
        assert post_row["hosted_media_urls"] == [expected_media]
        assert post_row["hosted_thumbnail_url"] == expected_media
        originals = post_row["asset_manifest"]["originals"]
        assert originals["cover"]["source_role"] == "cover"
        assert originals["media"][0]["source_role"] == "media"
        assert post_row["hosted_owner_profile_pic_url"].startswith(HOSTED)
        assert post_row["hosted_owner_profile_pic_url"] in avatar_urls
        assert len(post_row["asset_manifest"]["display_variants"]) == 10
        asset = rows(
            database, "select * from social.social_post_media_assets where post_id=%s", (str(canonical["id"]),)
        )[0]
        assert asset["hosted_url"] == expected_media and asset["mirror_status"] == "mirrored"
        comments = rows(
            database,
            "select * from social.instagram_comments where post_id=%s order by comment_id",
            (legacy_ids[code],),
        )
        parent, reply = comments
        assert parent["comment_id"] == code + "-parent" and reply["comment_id"] == code + "-reply"
        assert str(reply["parent_comment_id"]) == str(parent["id"])
        assert parent["likes"] == 3 and reply["likes"] == 1
        assert parent["hosted_media_urls"][0].startswith(HOSTED)
        assert all(
            row["hosted_author_profile_pic_url"].startswith(HOSTED) and not row["is_missing"] for row in comments
        )
        assert all(row["hosted_author_profile_pic_url"] in avatar_urls for row in comments)
        receipts = rows(
            database,
            """select stage,canonical_identity from social.instagram_ingestion_receipts
            where run_id=%s and target_key=%s""",
            (run_id, code),
        )
        assert {receipt["stage"] for receipt in receipts} == {"post_details", "comments", "media"}
        assert all(receipt["canonical_identity"]["post_id"] == legacy_ids[code] for receipt in receipts)
    assert rows(database, "select count(*) as count from social.avatar_registry where status='mirrored'") == [
        {"count": 3}
    ]
    assert rows(
        database,
        """select count(*) as count from social.instagram_ingestion_evidence
        where run_id=%s and payload like '%%private%%'""",
        (run_id,),
    ) == [{"count": 0}]
