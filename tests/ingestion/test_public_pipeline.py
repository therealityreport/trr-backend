"""One public pipeline through production composition; isolated PG and fake IO."""

import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import uuid4

import boto3
import httpx
import pytest
import requests

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from tests.ingestion import test_media
from tests.ingestion.test_pipeline import FakeStorageClient, rows
from tests.ingestion.test_pipeline import base_database as base_database_fixture
from tests.ingestion.test_pipeline import pipeline_database as pipeline_database_fixture
from tests.ingestion.test_public_comment_transport import body, bootstrap, node, page
from tests.ingestion.test_public_transport import html
from trr_backend.ingestion import composition, media
from trr_backend.ingestion.command_service import CommandService
from trr_backend.ingestion.domain import SourceProfile
from trr_backend.ingestion.listing import ROOT_FIELD
from trr_backend.ingestion.queries import Queries
from trr_backend.ingestion.workflows import Scheduler
from trr_backend.media import s3_mirror
from trr_backend.socials.instagram import auth_resolver, scraper
from trr_backend.socials.instagram.comments_scrapling import fetcher as comments_fetcher

base_database = base_database_fixture
pipeline_database = pipeline_database_fixture
HTTP_CLIENT = httpx.AsyncClient
ACCOUNT, CODE = "public_pipeline", "AAAABC"
CDN, HOSTED = "https://scontent.cdninstagram.com", "https://media.example/"


def test_public_composed_pipeline_commits_native_parent_reply_post_and_avatar_media(
    pipeline_database, monkeypatch, profile_mode=False
):
    store, transaction = pipeline_database
    source_calls, downloads = [], []
    sessions, async_clients = [], []
    original_storage = media.MediaStorage
    client = FakeStorageClient()
    source_time = int((datetime.now(UTC) - timedelta(days=1)).timestamp())
    show_id, season_id, post_id = (str(uuid4()) for _ in range(3))

    def forbidden(*args, **kwargs):
        pytest.fail("public integration attempted unmocked source, credentials, browser or provider access")

    monkeypatch.setattr(composition, "_credentials", forbidden)
    monkeypatch.setattr(auth_resolver, "resolve_instagram_auth_session", forbidden)
    monkeypatch.setattr(scraper.InstagramScraper, "_build_identity_pool", forbidden)
    monkeypatch.setattr(scraper.InstagramScraper, "_attach_current_auth_session", forbidden)
    monkeypatch.setattr(requests.sessions.Session, "send", forbidden)
    monkeypatch.setattr(httpx, "Client", forbidden)
    monkeypatch.setattr(comments_fetcher, "build_stealthy_fetcher", forbidden)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "authenticated")  # Frozen intent must win.
    monkeypatch.setenv("SOCIAL_INSTAGRAM_MEDIA_MIRROR_COMMENT_AVATARS", "true")
    monkeypatch.setenv("HTTPS_PROXY", "http://forbidden-proxy.invalid")
    monkeypatch.setattr(requests.sessions, "get_netrc_auth", forbidden)
    # Keep maintained request preparation, parsers and pagination; bypass only sleep.
    monkeypatch.setattr(scraper.InstagramRateController, "before_query", lambda *a, **kw: None)

    async def deny_http(*args, **kwargs):
        forbidden()

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", deny_http)

    def native_post(*, details):
        return {
            "pk": "66",
            "code": CODE,
            "taken_at": source_time,
            "media_type": 1,
            "user": {"pk": "99", "username": ACCOUNT, "profile_pic_url": CDN + "/owner.png?sig=private"},
            "caption": {"text": "Native detail" if details else "Native listing"},
            "like_count": 7 if details else 2,
            "comment_count": 2,
            "image_versions2": {"candidates": [{"url": CDN + "/post.png?sig=private", "width": 12, "height": 8}]},
        }

    listing = {
        "data": {
            ROOT_FIELD: {
                "edges": [{"node": native_post(details=False)}],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }
    detail = {"data": {"xdt_api__v1__media__shortcode__web_info": {"items": [native_post(details=True)]}}}

    class SourceSession(requests.sessions.Session):
        def __init__(self):
            super().__init__()
            self.sent, self.closed = [], False
            sessions.append(self)

        def send(self, request, **options):
            path = urlsplit(request.url).path
            assert options["allow_redirects"] is False and options["proxies"] == {}
            assert self.trust_env is False and self.auth is None
            assert "Authorization" not in request.headers
            assert not any(key in request.headers.get("Cookie", "") for key in ("sessionid", "ds_user_id"))
            self.sent.append(request)
            source_calls.append(("requests", request.method, path))
            response = requests.Response()
            response.status_code, response.request = 200, request
            if request.method == "GET" and path == f"/{ACCOUNT}/":
                value, content_type = "<html>anonymous bootstrap</html>", "text/html"
            elif request.method == "POST" and path == "/graphql/query":
                value, content_type = json.dumps(listing), "application/json"
            elif request.method == "GET" and path == f"/p/{CODE}/":
                value, content_type = html(detail), "text/html"
            elif request.method == "GET" and path == "/api/v1/users/web_profile_info/":
                username = request.url.rsplit("?username=", 1)[-1]
                assert username in ({"reader", "replier"} if profile_mode else {"replier"})
                value, content_type = (
                    json.dumps(
                        {
                            "status": "ok",
                            "data": {
                                "user": {
                                    "id": "101" if username == "reader" else "102",
                                    "username": username,
                                    "profile_pic_url_hd": CDN + "/" + username + ".png?sig=private",
                                    "full_name": username.title(),
                                    "biography": "Public metadata",
                                    "follower_count": 0,
                                    "is_verified": False,
                                    "edge_owner_to_timeline_media": {
                                        "count": 7,
                                        "edges": [{"id": "excluded-author-post"}],
                                    },
                                }
                            },
                        }
                    ),
                    "application/json",
                )
            else:
                pytest.fail("unexpected mocked source route: " + path)
            response._content = value.encode()
            response.headers["content-type"] = content_type
            return response

        def close(self):
            self.closed = True
            super().close()

    monkeypatch.setattr(requests, "Session", SourceSession)

    def relay(request):
        source_calls.append(("httpx", request.method, request.url.path))
        if request.method == "GET":
            return bootstrap()
        _, variables = body(request)
        if variables.get("parent_comment_id"):
            assert variables["parent_comment_id"] == "111"
            return httpx.Response(
                200,
                json=page(
                    [
                        node(
                            "112",
                            created_at=source_time,
                            comment_like_count=1,
                            user={"pk": "102", "username": "replier"},
                        )
                    ],
                    child=True,
                    count=1,
                ),
            )
        assert variables.get("after") is None
        return httpx.Response(
            200,
            json=page(
                [
                    node(
                        "111",
                        1,
                        created_at=source_time,
                        comment_like_count=3,
                        user={"pk": "101", "username": "reader", "profile_pic_url": CDN + "/reader.png?sig=private"},
                    )
                ]
            ),
        )

    def async_client(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        assert kwargs.get("proxy") is None and "cookies" not in kwargs
        result = HTTP_CLIENT(transport=httpx.MockTransport(relay), **kwargs)
        async_clients.append(result)
        return result

    monkeypatch.setattr(httpx, "AsyncClient", async_client)
    monkeypatch.setattr(comments_fetcher, "build_comments_async_client", forbidden)
    fetcher_init = comments_fetcher.InstagramCommentsScraplingFetcher.__init__

    def fetcher_constructor(self, *args, **kwargs):
        fetcher_init(self, *args, **kwargs)
        self._api_delay_seconds = self._global_api_delay_seconds = 0

    monkeypatch.setattr(comments_fetcher.InstagramCommentsScraplingFetcher, "__init__", fetcher_constructor)
    monkeypatch.setattr(composition, "Store", lambda: store)
    monkeypatch.setattr(composition, "Queries", lambda: Queries(transaction))
    monkeypatch.setattr(boto3, "Session", lambda **kw: SimpleNamespace(client=lambda *a, **kw: client))
    monkeypatch.setattr(
        s3_mirror,
        "get_hosted_media_storage_config",
        lambda: SimpleNamespace(
            profile_name=None,
            region="us-east-1",
            endpoint_url="https://mock-storage.invalid",
            access_key_id=None,
            secret_access_key=None,
            session_token=None,
            bucket="public-pipeline",
            prefix="",
        ),
    )
    monkeypatch.setattr(s3_mirror, "build_public_object_url", lambda key: HOSTED + key)

    def download(_requests, url, **kwargs):
        assert url.startswith(CDN + "/") and kwargs["allow_redirects"] is False
        downloads.append(url)
        return test_media.Response()

    monkeypatch.setattr(media, "MediaStorage", lambda **kw: original_storage(**kw, opener=download))
    with transaction() as conn, conn.cursor() as cur:
        if not profile_mode:  # The profile schema fixture already includes this shared gate table.
            cur.execute(
                (
                    Path(__file__).parents[2] / "supabase/migrations/20260911121000_instagram_detail_request_lanes.sql"
                ).read_text()
            )
        cur.execute("insert into core.shows values(%s)", (show_id,))
        cur.execute("insert into core.seasons(id,show_id,season_number) values(%s,%s,3)", (season_id, show_id))
        cur.execute(
            """insert into social.instagram_posts
            (id,shortcode,username,source_account,show_id,season_id,scraped_at)
            values(%s,%s,%s,%s,%s,%s,%s)""",
            (post_id, CODE, ACCOUNT, ACCOUNT, show_id, season_id, datetime.now(UTC) - timedelta(days=2)),
        )

    api = CommandService(
        store,
        Queries(transaction),
        engine="dbos",
        workflow_version=composition.WORKFLOW_VERSION if profile_mode else "public-pipeline-v1",
        enabled=True,
        capacity=lambda *_: None,
        source_profile=SourceProfile.for_mode("public"),
    )
    command = api.launch(
        ACCOUNT,
        InstagramIngestionLaunchV2(force_catalog_rediscovery=True),
        idempotency_key="public-pipeline",
        initiated_by="fixture",
    )
    run_id = str(command.run_id)
    worker = composition.build_worker()
    outcomes = []

    def dispatch(reservation):
        outcomes.append(worker.execute(reservation))
        return "local:" + reservation["owner_token"]

    partition = store.seed(dict(command.model_dump(mode="json"), account=ACCOUNT))
    scheduler = Scheduler(store, admit=lambda _: True, dispatch=dispatch, reconcile=lambda _: forbidden())
    for ordinal in range(20):
        result = scheduler.tick(partition, ordinal)
        assert all(item["outcome"] == "committed" for item in outcomes), outcomes
        if result.get("stop"):
            assert result["stop"] == "completed"
            store.complete_partition(partition, continue_run=False, stop_state="completed")
            break
        assert result == {"ready": True} or (profile_mode and result == {"sleep": 0.01}), result
    else:
        pytest.fail("public pipeline did not complete in bounded scheduling steps")

    database = pipeline_database
    persisted = Queries(transaction).progress(run_id, ACCOUNT)
    assert persisted["intent"]["source_profile"]["access_mode"] == "public"
    progress = SimpleNamespace(**{**persisted, "stages": [SimpleNamespace(**stage) for stage in persisted["stages"]]})
    assert progress.complete and progress.state == "completed" and progress.generation == 1
    assert {stage.stage: (stage.total, stage.committed, stage.unresolved) for stage in progress.stages} == {
        **dict.fromkeys(("listing", "post_details", "comments"), (1, 1, 0)),
        "media": (3, 3, 0) if profile_mode else (1, 1, 0),
    }
    assert all(stage.complete and stage.discovery_complete and stage.sealed_at for stage in progress.stages)
    assert Counter(item["stage"] for item in outcomes) == {
        **dict.fromkeys(("listing", "post_details", "comments"), 1),
        "media": 3 if profile_mode else 1,
    }

    receipts = rows(database, "select * from social.instagram_ingestion_receipts where run_id=%s", (run_id,))
    assert len(receipts) == (6 if profile_mode else 4)
    assert rows(
        database,
        "select count(*) as count from social.instagram_ingestion_targets where run_id=%s and outcome='unresolved'",
        (run_id,),
    ) == [{"count": 0}]
    assert {stage.stage: stage.requests for stage in progress.stages if stage.stage != "media"} == {
        "listing": 2,
        "post_details": 1,
        "comments": 3,
    }
    assert len(source_calls) == (
        9 if profile_mode else 7
    )  # Two listing, one detail, three Relay and one missing-avatar lookup.
    assert next(stage.requests for stage in progress.stages if stage.stage == "media") == (
        (3 if profile_mode else 1) + len(downloads) + sum(client.calls.values())
    )
    assert len(downloads) == 4 and len(client.objects) == 14 and client.closes == 1
    assert all(session.closed for session in sessions if session.sent)
    assert async_clients and all(item.is_closed for item in async_clients)

    post = rows(database, "select * from social.instagram_posts where id=%s", (post_id,))[0]
    assert post["caption"] == "Native detail" and post["likes"] == 7
    assert post["posted_at"] == datetime.fromtimestamp(source_time, UTC)
    assert str(post["show_id"]) == show_id and str(post["season_id"]) == season_id
    assert post["media_mirror_status"] == "mirrored" and post["hosted_media_urls"] == [post["hosted_thumbnail_url"]]
    assert post["hosted_thumbnail_url"].startswith(HOSTED)
    assert len(post["asset_manifest"]["display_variants"]) == 10
    assert post["hosted_owner_profile_pic_url"].startswith(HOSTED)
    comments = rows(
        database, "select * from social.instagram_comments where post_id=%s order by comment_id", (post_id,)
    )
    assert [item["comment_id"] for item in comments] == ["111", "112"]
    assert str(comments[1]["parent_comment_id"]) == str(comments[0]["id"])
    assert all(item["created_at"] == datetime.fromtimestamp(source_time, UTC) for item in comments)
    assert all(not item["is_missing"] and item["hosted_author_profile_pic_url"].startswith(HOSTED) for item in comments)
    canonical = rows(
        database,
        "select * from social.social_posts where platform='instagram' and source_id=%s",
        (CODE,),
    )
    assert len(canonical) == 1
    assert all(
        item["canonical_identity"]["post_id"] == post_id
        for item in receipts
        if item["stage"] != "listing" and not item["target_key"].startswith("profile:")
    )
    detail_receipt = next(item for item in receipts if item["stage"] == "post_details")
    assert detail_receipt["canonical_identity"]["canonical_post_id"] == str(canonical[0]["id"])
    assets = rows(database, "select * from social.social_post_media_assets where post_id=%s", (canonical[0]["id"],))
    assert len(assets) == 1 and assets[0]["hosted_url"] == post["hosted_media_urls"][0]
    evidence = rows(database, "select * from social.instagram_ingestion_evidence where run_id=%s", (run_id,))
    assert len(evidence) == (
        9 if profile_mode else 7
    )  # Listing, detail, two pages, comment summary, avatar and media inventory.
    assert all("private" not in item["payload"] and "ephemeral-fixture-lsd" not in item["payload"] for item in evidence)
    detail_evidence = next(item for item in evidence if item["stage"] == "post_details")
    assert post["scraped_at"] == detail_evidence["captured_at"]
    assert next(item for item in receipts if item["stage"] == "media")["observed_at"] == detail_evidence["captured_at"]
    for comment in comments:
        observed = rows(
            database,
            """select p.observed_at from social.instagram_ingestion_comment_pages p
            where run_id=%s and content_hash in
            (select content_hash from social.instagram_ingestion_evidence where run_id=%s and payload like %s)""",
            (run_id, run_id, '%"' + comment["comment_id"] + '"%'),
        )
        assert any(item["observed_at"] == comment["scraped_at"] for item in observed)
    assert post["asset_manifest"]["originals"]["media"][0]["source_role"] == "media"
    assert rows(database, "select count(*) as count from social.avatar_registry where status='mirrored'") == [
        {"count": 3}
    ]
    # Public routing is internal frozen metadata; the operator API keeps its
    # existing strict contract while this full intent remains in persistence.
    assert api.progress(ACCOUNT, command.run_id).complete is True

    if profile_mode:
        profiles = rows(database, "select * from social.instagram_profiles order by profile_id")
        assert [(p["profile_id"], p["username"], p["followers_count"], p["is_verified"]) for p in profiles] == [
            ("101", "reader", 0, False),
            ("102", "replier", 0, False),
        ]
        profile_receipts = [r for r in receipts if r["target_key"].startswith("profile:")]
        assert len(profile_receipts) == 2
        for profile in profiles:
            proof = profile["raw_data"]["ingestion_public_profile"]
            matching = next(
                r for r in profile_receipts if r["canonical_identity"]["instagram_profile_id"] == profile["profile_id"]
            )
            assert proof["collection_complete"] and matching["observed_at"] == profile["last_scraped_at"]
            assert proof["coverage"]["follows_count"] == "unknown" and profile["follows_count"] is None
            assert profile["hosted_profile_pic_url"].startswith(HOSTED)
            retained = next(e for e in evidence if str(e["evidence_id"]) == proof["evidence_id"])
            assert matching["observed_at"] == retained["captured_at"]
            assert "excluded-author-post" not in retained["payload"] and "edges" not in retained["payload"]
        assert len(downloads) == 4  # Both profiles reused exact post-media avatar byte proofs.
