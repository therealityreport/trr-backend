"""Characterization and real local SQL; run with --confcutdir=tests/ingestion."""

import copy
import json
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.canonical import (
    CanonicalWriteError,
    OlderObservation,
    normalize_comment_source,
    normalize_post_source,
    write_comments,
    write_post,
)
from trr_backend.ingestion.evidence import Evidence

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/ingestion"
CAPTURE = json.loads((FIXTURES / "canonical_capture.json").read_text())
OBSERVED = datetime(2026, 10, 16, 12, tzinfo=UTC)


@pytest.fixture(scope="module")
def database(tmp_path_factory):
    root = tmp_path_factory.mktemp("canonical")
    binaries = Path("/opt/homebrew/opt/postgresql@17/bin")
    if not (binaries / "initdb").exists():
        pytest.skip("isolated PostgreSQL 17 binaries required")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "pg"
    subprocess.run(
        [
            str(binaries / "initdb"),
            "-D",
            str(data),
            "-A",
            "trust",
            "-U",
            "canonical_test",
            "--no-locale",
            "--encoding=UTF8",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            str(binaries / "pg_ctl"),
            "-D",
            str(data),
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-p {port} -h 127.0.0.1 -k ''",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    dsn = f"postgresql://canonical_test@127.0.0.1:{port}/postgres"

    @contextmanager
    def transaction():
        conn = psycopg2.connect(dsn)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    try:
        with transaction() as conn, conn.cursor() as cur:
            cur.execute((FIXTURES / "canonical_schema.sql").read_text())
        yield transaction
    finally:
        subprocess.run(
            [str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
        )


def load(conn, table, **keys):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"select * from social.{table} where " + " and ".join(f"{k}=%s" for k in keys), list(keys.values()))
        return dict(cur.fetchone() or {})


class CommentQueryRecorder:
    """Observe the real isolated SQL without changing transaction behavior."""

    def __init__(self, connection):
        self.connection = connection
        self.reads = []
        self.writes = []

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def cursor(self, *args, **kwargs):
        cursor = self.connection.cursor(*args, **kwargs)
        recorder = self

        class Cursor:
            def __enter__(self):
                cursor.__enter__()
                return self

            def __exit__(self, *args):
                return cursor.__exit__(*args)

            def __getattr__(self, name):
                return getattr(cursor, name)

            def execute(self, query, params=None):
                rendered = query if isinstance(query, str) else query.as_string(recorder.connection)
                statement = " ".join(rendered.lower().replace('"', "").split())
                if "from social.instagram_comments" in statement:
                    recorder.reads.append((statement, params))
                elif "insert into social.instagram_comments" in statement:
                    recorder.writes.append((statement, params))
                return cursor.execute(query, params)

        return Cursor()


def post(conn, *, source=None, observed=OBSERVED, run_id=None):
    source = copy.deepcopy(source or CAPTURE["post"])
    if run_id is None:
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))
    return write_post(
        conn,
        run_id=run_id,
        account="fixture_account",
        observed_at=observed,
        normalized=normalize_post_source(source, account="fixture_account"),
        source_payload=source,
    )


def test_scraper_dto_and_native_comment_characterization():
    comment = normalize_comment_source(CAPTURE["comment"])
    assert (comment.comment_id, comment.username, comment.user_id) == ("comment1", "commenter", "user1")
    assert comment.owner_full_name == "Fixture Commenter"
    assert comment.owner_is_verified is False
    assert comment.replies[0].parent_comment_id == "comment1"
    assert comment.likes == 2
    native = normalize_comment_source(
        {
            "pk": "native1",
            "text": "Native",
            "user": {"pk": "u1", "username": "native"},
            "comment_like_count": 8,
            "child_comment_count": 3,
        }
    )
    assert (native.comment_id, native.username, native.likes, native.reply_count) == ("native1", "native", 8, 3)


def test_post_all_models_metadata_and_original_observation(database):
    with database() as conn:
        ids = post(conn)
        row = load(conn, "instagram_posts", id=ids["post_id"])
        assert row["owner_user_id"] == "owner-1"
        assert row["caption_is_edited"] is False
        assert row["owner_full_name"] == "Fixture Owner"
        assert row["media_repost_count"] == 6
        assert row["fb_comment_count"] == 2
        assert row["location_raw"]["name"] == "Fixture Place"
        assert row["child_posts_data"] == CAPTURE["post"]["child_posts_data"]
        assert row["scraped_at"] == OBSERVED
        sidecar = load(conn, "instagram_post_payloads", post_id=ids["post_id"])
        assert {k: sidecar["raw_data"][k] for k in CAPTURE["post"]} == CAPTURE["post"]
        assert sidecar["raw_data"]["view_metrics"]["observed_at"] == OBSERVED.isoformat()
        assert sidecar["payload_updated_at"] == OBSERVED
        canonical = load(conn, "social_posts", id=ids["canonical_post_id"])
        assert (
            canonical["like_count"],
            canonical["comment_count"],
            canonical["share_count"],
            canonical["view_count"],
        ) == (12, 4, 6, 101)
        assert canonical["last_scraped_at"] == OBSERVED
        assert load(conn, "social_post_legacy_refs", legacy_table="instagram_posts", legacy_pk=ids["post_id"])
        assert load(
            conn,
            "social_post_legacy_refs",
            legacy_table="instagram_account_catalog_posts",
            legacy_pk=ids["catalog_post_id"],
        )
        assert (
            load(conn, "social_post_entities", post_id=ids["canonical_post_id"], entity_type="tagged_user")[
                "entity_payload"
            ]["tag_x"]
            == 0.2
        )
        assert (
            load(conn, "social_post_entities", post_id=ids["canonical_post_id"], entity_type="location")["entity_key"]
            == "place1"
        )
        assert load(conn, "social_post_media_assets", post_id=ids["canonical_post_id"], position=1)[
            "source_url"
        ].endswith("slide2.mp4")
        assert (
            load(conn, "instagram_account_catalog_post_collaborators", catalog_post_id=ids["catalog_post_id"])[
                "collaborator_handle"
            ]
            == "partner"
        )
        observation = load(conn, "social_post_observations", post_id=ids["canonical_post_id"])
        assert observation["observed_at"] == OBSERVED


def test_thin_refresh_preserves_rich_hosted_payloads_and_metric_max(database):
    source = copy.deepcopy(CAPTURE["post"])
    source["shortcode"] = "ThinFixture"
    source.update(
        hosted_media_urls=["https://media.example/hosted.jpg"], hosted_thumbnail_url="https://media.example/thumb.jpg"
    )
    with database() as conn:
        ids = post(conn, source=source)
        thin = {
            "shortcode": "ThinFixture",
            "pk": "100001",
            "post_type": "carousel",
            "username": "fixture_account",
            "caption": "",
            "media_urls": [],
            "likes": 1,
            "comments": 0,
            "video_views_observed": 4,
        }
        post(conn, source=thin, observed=OBSERVED + timedelta(hours=1))
        row = load(conn, "instagram_posts", id=ids["post_id"])
        assert row["caption"] == source["caption"]
        assert row["media_urls"] == source["media_urls"]
        assert row["hosted_media_urls"] == source["hosted_media_urls"]
        assert row["views"] == 101
        assert load(conn, "instagram_account_catalog_posts", id=ids["catalog_post_id"])["views"] == 101
        assert load(conn, "social_posts", id=ids["canonical_post_id"])["view_count"] == 101
        assert row["owner_full_name"] == "Fixture Owner"
        assert row["child_posts_data"] == source["child_posts_data"]
        assert row["asset_manifest"] == source["asset_manifest"]
        sidecar = load(conn, "instagram_post_payloads", post_id=ids["post_id"])
        assert sidecar["raw_data"]["caption"] == source["caption"]
        assert (
            load(conn, "social_post_media_assets", post_id=ids["canonical_post_id"], position=0)["hosted_url"]
            == source["hosted_media_urls"][0]
        )
        assert load(conn, "social_posts", id=ids["canonical_post_id"])["like_count"] == 12


def test_older_observation_rejected_without_any_model_update(database):
    source = {**CAPTURE["post"], "shortcode": "OlderFixture"}
    with database() as conn:
        ids = post(conn, source=source)
    with database() as conn:
        with pytest.raises(OlderObservation):
            post(conn, source={**source, "caption": "old replacement"}, observed=OBSERVED - timedelta(days=1))
        assert load(conn, "instagram_posts", id=ids["post_id"])["caption"] == source["caption"]
        assert load(conn, "instagram_account_catalog_posts", id=ids["catalog_post_id"])["caption"] == source["caption"]
        with conn.cursor() as cur:
            cur.execute(
                "select count(*) from social.social_post_observations where post_id=%s", (ids["canonical_post_id"],)
            )
            assert cur.fetchone()[0] == 2


def test_failure_after_post_write_rolls_back_every_model(database):
    source = {**CAPTURE["post"], "shortcode": "RollbackFixture"}
    with database() as conn, conn.cursor() as cur:
        cur.execute("""create function social.reject_fixture() returns trigger language plpgsql as $$
            begin if new.source_id='RollbackFixture' then raise exception 'injected canonical failure'; end if;
            return new; end $$;
            create trigger reject_fixture before insert on social.social_posts
            for each row execute function social.reject_fixture();""")
    with pytest.raises(psycopg2.errors.RaiseException, match="injected canonical failure"):
        with database() as conn:
            post(conn, source=source)
    with database() as conn:
        assert not load(conn, "instagram_posts", shortcode="RollbackFixture")
        assert not load(conn, "instagram_account_catalog_posts", source_id="RollbackFixture")
        with conn.cursor() as cur:
            cur.execute("drop trigger reject_fixture on social.social_posts; drop function social.reject_fixture()")


def test_comment_tree_post_scope_ranked_merge_and_lifecycle(database):
    run_id = str(uuid4())
    raw = copy.deepcopy(CAPTURE["comment"])
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "CommentsFixture"}, run_id=None)
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))
        result = write_comments(
            conn,
            run_id=run_id,
            account="fixture_account",
            observed_at=OBSERVED,
            post_id=ids["post_id"],
            comments=[normalize_comment_source(raw)],
            source_payloads=[raw],
        )
        assert result["observed_comment_ids"] == ["comment1", "reply1"]
        parent = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="comment1")
        reply = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="reply1")
        assert reply["parent_comment_id"] == parent["id"]
        assert reply["parent_comment_external_id"] == "comment1"
        assert reply["reply_depth"] == 1
        assert parent["author_full_name"] == "Fixture Commenter"
        assert parent["author_is_private"] is False
        assert parent["author_fbid_v2"] == "fb-user1"
        assert parent["scraped_at"] == OBSERVED
        assert parent["last_seen_run_id"] == run_id
        assert result["media"][0]["comment_id"] == str(parent["id"])
        headload = {
            **raw,
            "phase": "headload",
            "is_ranked": False,
            "comment_index": None,
            "cursor_payload": {"cursor_param": "next"},
        }
        write_comments(
            conn,
            run_id=run_id,
            account="fixture_account",
            observed_at=OBSERVED + timedelta(hours=1),
            post_id=ids["post_id"],
            comments=[normalize_comment_source(headload)],
            source_payloads=[headload],
        )
        parent = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="comment1")
        assert parent["is_ranked"] is True and parent["comment_index"] == 3
        assert parent["cursor_payload"] == {
            "cursor_min_id": "cursor1",
            "cursor_param": "next",
            "phase_observations": ["ranked", "headload"],
        }
        other = post(conn, source={**CAPTURE["post"], "shortcode": "OtherCommentsFixture"})
        other_result = write_comments(
            conn,
            run_id=run_id,
            account="fixture_account",
            observed_at=OBSERVED,
            post_id=other["post_id"],
            comments=[normalize_comment_source(raw)],
            source_payloads=[raw],
        )
        assert result["comment_ids"]["comment1"] != other_result["comment_ids"]["comment1"]
        with pytest.raises(OlderObservation):
            write_comments(
                conn,
                run_id=run_id,
                account="fixture_account",
                observed_at=OBSERVED,
                post_id=ids["post_id"],
                comments=[normalize_comment_source(raw)],
                source_payloads=[raw],
            )


@pytest.mark.parametrize(
    "saved,native,repair",
    [
        (None, OBSERVED - timedelta(hours=1), True),
        (datetime(1970, 1, 1, tzinfo=UTC), OBSERVED - timedelta(hours=1), True),
        (OBSERVED - timedelta(hours=2), OBSERVED - timedelta(hours=1), False),
        (datetime(1970, 1, 1, tzinfo=UTC), None, False),
        (datetime(1970, 1, 1, tzinfo=UTC), OBSERVED - timedelta(days=2), False),
        (datetime(1970, 1, 1, tzinfo=UTC), OBSERVED + timedelta(hours=1), False),
    ],
)
def test_comment_native_date_repairs_only_missing_or_epoch_without_losing_reply(database, saved, native, repair):
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "DateRepair" + uuid4().hex[:8]})
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))
        parent = {"comment_id": "date-parent", "username": "parent", "text": "Parent"}
        reply = {"comment_id": "date-reply", "username": "reply", "text": "Reply", "parent_comment_id": "date-parent"}

        def write(raw, observed):
            return write_comments(
                conn,
                run_id=run_id,
                account="fixture_account",
                observed_at=observed,
                post_id=ids["post_id"],
                comments=[normalize_comment_source(item) for item in raw],
                source_payloads=raw,
            )

        write([parent, reply], OBSERVED - timedelta(minutes=1))
        with conn.cursor() as cur:
            cur.execute(
                "update social.instagram_comments set created_at=%s where post_id=%s and comment_id=%s",
                (saved, ids["post_id"], "date-reply"),
            )
        before = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="date-reply")
        if native is not None:
            reply["created_at"] = int(native.timestamp())
        write([reply], OBSERVED)
        after = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="date-reply")
        assert after["created_at"] == (native if repair else saved)
        for key in ("id", "first_seen_at", "parent_comment_id", "parent_comment_external_id", "reply_depth"):
            assert after[key] == before[key]
        assert after["last_seen_at"] == OBSERVED


def test_new_gif_only_comments_and_unknown_authors_persist_without_erasing_known_fields(database):
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "EmptyCommentFields"})
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))
        raw = [
            {
                "comment_id": "gif-parent",
                "username": "known",
                "text": "",
                "comment_gif": {"url": "https://cdn.example/gif.mp4"},
            },
            {"comment_id": "unknown-reply", "text": "Reply", "parent_comment_id": "gif-parent"},
        ]

        def write(observed):
            return write_comments(
                conn,
                run_id=run_id,
                account="fixture_account",
                observed_at=observed,
                post_id=ids["post_id"],
                comments=[normalize_comment_source(item) for item in raw],
                source_payloads=raw,
            )

        result = write(OBSERVED)
        parent = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="gif-parent")
        reply = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="unknown-reply")
        assert result["observed_comment_ids"] == ["gif-parent", "unknown-reply"]
        assert parent["text"] == "" and parent["username"] == "known"
        assert parent["media_urls"] == ["https://cdn.example/gif.mp4"]
        assert reply["username"] == "" and reply["text"] == "Reply"
        assert reply["parent_comment_id"] == parent["id"]
        raw[0]["username"] = ""
        raw[1]["text"] = ""
        write(OBSERVED + timedelta(minutes=1))
        assert load(conn, "instagram_comments", id=parent["id"])["username"] == "known"
        assert load(conn, "instagram_comments", id=reply["id"])["text"] == "Reply"


def test_comment_permalink_uses_loaded_post_and_preserves_supplied_current_links(database):
    raw = copy.deepcopy(CAPTURE["comment"])
    raw.pop("comment_url")
    raw["post_shortcode"] = "UntrustedSourceShortcode"
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "LoadedPermalinkPost"})
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))

        def write(source, hours):
            write_comments(
                conn,
                run_id=run_id,
                account="fixture_account",
                observed_at=OBSERVED + timedelta(hours=hours),
                post_id=ids["post_id"],
                comments=[normalize_comment_source(source)],
                source_payloads=[source],
            )

        write(raw, 0)
        parent = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="comment1")
        reply = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="reply1")
        assert parent["comment_url"] == "https://www.instagram.com/p/LoadedPermalinkPost/c/comment1/"
        assert reply["comment_url"] == "https://www.instagram.com/p/LoadedPermalinkPost/c/reply1/"
        assert reply["parent_comment_id"] == parent["id"] and reply["post_id"] == parent["post_id"]

        supplied = copy.deepcopy(raw)
        supplied["comment_url"] = "https://www.instagram.com/p/SuppliedPost/c/comment1/"
        supplied["replies"][0]["comment_url"] = "https://www.instagram.com/p/SuppliedPost/c/reply1/"
        write(supplied, 1)
        write(raw, 2)
        for source in (supplied, supplied["replies"][0]):
            row = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id=source["comment_id"])
            assert row["comment_url"] == source["comment_url"]
            assert row["scraped_at"] == OBSERVED + timedelta(hours=2)


def test_inline_comments_fail_closed_and_connection_is_explicit(database):
    raw = {**CAPTURE["comment"], "source_snapshot_type": "listing_inline_sample"}
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "InlineFixture"})
        with pytest.raises(CanonicalWriteError, match="full-comments"):
            write_comments(
                conn,
                run_id=str(uuid4()),
                account="fixture_account",
                observed_at=OBSERVED,
                post_id=ids["post_id"],
                comments=[normalize_comment_source(raw)],
                source_payloads=[raw],
            )
    with pytest.raises(CanonicalWriteError, match="non-autocommit"):
        write_post(
            None,
            run_id=str(uuid4()),
            account="fixture_account",
            observed_at=OBSERVED,
            normalized=normalize_post_source(CAPTURE["post"], account="fixture_account"),
            source_payload=CAPTURE["post"],
        )


def test_import_boundary_in_clean_interpreter():
    code = """import sys
from trr_backend.ingestion import canonical
assert not any('social_season_analytics' in k for k in sys.modules)
assert 'trr_backend.socials.provider_registry' not in sys.modules
"""
    subprocess.run([str(Path(__import__("sys").executable)), "-c", code], check=True, capture_output=True)


def test_native_carousel_queryable_shape_and_hidden_metrics(database):
    raw = {
        "code": "NativeFixture",
        "pk": "native-post",
        "media_type": 8,
        "taken_at": 1792065600,
        "caption": {"text": "Native caption"},
        "user": {"pk": "native-owner", "username": "fixture_account"},
        "like_count": 44,
        "comment_count": 6,
        "view_count": 90,
        "carousel_media": [
            {
                "media_type": 1,
                "original_width": 1080,
                "original_height": 720,
                "image_versions2": {"candidates": [{"url": "https://cdn.example/native.jpg"}]},
                "accessibility_caption": "Native image",
            }
        ],
    }
    with database() as conn:
        ids = post(conn, source=raw)
        row = load(conn, "instagram_posts", id=ids["post_id"])
        assert row["raw_data"]["post_type"] == "carousel"
        assert row["raw_data"]["caption"] == "Native caption"
        assert row["raw_data"]["comments"] == 6
        observation = load(
            conn, "social_post_observations", post_id=ids["canonical_post_id"], source_table="instagram_posts"
        )
        assert observation["raw_payload"] == raw
        assert row["child_posts_data"] == [
            {
                "slide_index": 0,
                "type": "Image",
                "display_url": "https://cdn.example/native.jpg",
                "video_url": None,
                "width": 1080,
                "height": 720,
                "alt": "Native image",
                "tagged_users_detail": [],
            }
        ]
        post(
            conn,
            source={
                **raw,
                "like_count": 0,
                "like_count_hidden": True,
                "comment_count": 0,
                "comment_count_hidden": True,
                "view_count": 0,
                "views_hidden": True,
            },
            observed=OBSERVED + timedelta(hours=1),
        )
        catalog = load(conn, "instagram_account_catalog_posts", id=ids["catalog_post_id"])
        assert (catalog["likes"], catalog["comments_count"], catalog["views"]) == (44, 6, 90)


def test_comment_baselines_and_external_parents_use_one_post_scoped_locked_read(database):
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "BatchLookupFixture"})
        other = post(conn, source={**CAPTURE["post"], "shortcode": "BatchLookupOtherPost"})
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))

        def write(connection, post_id, raw, observed):
            return write_comments(
                connection,
                run_id=run_id,
                account="fixture_account",
                observed_at=observed,
                post_id=post_id,
                comments=[normalize_comment_source(item) for item in raw],
                source_payloads=raw,
            )

        parents = [{"comment_id": key, "username": "parent", "text": key} for key in ("z-parent", "a-parent")]
        # Parent versions are references, not observations in this reply page.
        parent_ids = write(conn, ids["post_id"], parents, OBSERVED + timedelta(hours=2))["comment_ids"]
        foreign_ids = write(conn, other["post_id"], parents, OBSERVED + timedelta(hours=3))["comment_ids"]
        replies = [
            {"comment_id": f"reply-{index}", "username": "reply", "text": str(index), "parent_comment_id": parent}
            for index, parent in enumerate(("z-parent", "a-parent", "z-parent", "a-parent"))
        ]
        initial = write(conn, ids["post_id"], replies, OBSERVED)
        before = {key: load(conn, "instagram_comments", post_id=ids["post_id"], comment_id=key) for key in parent_ids}
        recorder = CommentQueryRecorder(conn)
        result = write(recorder, ids["post_id"], replies, OBSERVED + timedelta(hours=1))
        assert len(recorder.reads) == 1
        statement, params = recorder.reads[0]
        assert "post_id=%s" in statement and "comment_id=any(" in statement
        assert "order by comment_id for update" in statement
        assert params == (ids["post_id"], sorted([*initial["comment_ids"], *parent_ids]))
        assert len(recorder.writes) == len(replies)
        assert result == initial
        for raw in replies:
            row = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id=raw["comment_id"])
            parent = raw["parent_comment_id"]
            assert str(row["parent_comment_id"]) == parent_ids[parent]
            assert str(row["parent_comment_id"]) != foreign_ids[parent]
            assert row["parent_comment_external_id"] == parent and row["reply_depth"] == 1
            assert row["text"] == raw["text"] and row["scraped_at"] == OBSERVED + timedelta(hours=1)
        for key in parent_ids:
            assert load(conn, "instagram_comments", post_id=ids["post_id"], comment_id=key) == before[key]


def test_comment_batch_keeps_stale_page_missing_parent_and_cycle_errors(database):
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "BatchErrorsFixture"})
        other = post(conn, source={**CAPTURE["post"], "shortcode": "BatchErrorsOtherPost"})
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))

        def write(connection, post_id, raw, observed=OBSERVED):
            return write_comments(
                connection,
                run_id=run_id,
                account="fixture_account",
                observed_at=observed,
                post_id=post_id,
                comments=[normalize_comment_source(item) for item in raw],
                source_payloads=raw,
            )

        raw = [{"comment_id": "stale", "username": "author", "text": "current"}]
        write(conn, ids["post_id"], raw, OBSERVED + timedelta(hours=1))
        recorder = CommentQueryRecorder(conn)
        with pytest.raises(OlderObservation):
            write(recorder, ids["post_id"], raw)
        assert not recorder.writes

        parent = [{"comment_id": "foreign-only", "username": "author", "text": "parent"}]
        write(conn, other["post_id"], parent)
        reply = [{"comment_id": "missing-parent-reply", "parent_comment_id": "foreign-only", "text": "reply"}]
        recorder = CommentQueryRecorder(conn)
        with pytest.raises(CanonicalWriteError, match="Reply parent is absent from this post"):
            write(recorder, ids["post_id"], reply)
        assert len(recorder.reads) == 1 and not recorder.writes

        cycle = [
            {"comment_id": "cycle-a", "parent_comment_id": "cycle-b", "text": "A"},
            {"comment_id": "cycle-b", "parent_comment_id": "cycle-a", "text": "B"},
        ]
        recorder = CommentQueryRecorder(conn)
        with pytest.raises(CanonicalWriteError, match="Comment parent cycle"):
            write(recorder, ids["post_id"], cycle)
        assert len(recorder.reads) == 1 and not recorder.writes


def test_empty_comment_page_keeps_transaction_and_post_guards(database):
    kwargs = {
        "run_id": str(uuid4()),
        "account": "fixture_account",
        "observed_at": OBSERVED,
        "comments": [],
        "source_payloads": [],
    }
    with pytest.raises(CanonicalWriteError, match="non-autocommit"):
        write_comments(None, post_id=str(uuid4()), **kwargs)
    with database() as conn:
        with pytest.raises(CanonicalWriteError, match="Comment parent post does not exist"):
            write_comments(conn, post_id=str(uuid4()), **kwargs)
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "EmptyBatchFixture"})
        recorder = CommentQueryRecorder(conn)
        assert write_comments(recorder, post_id=ids["post_id"], **kwargs) == {
            "post_id": ids["post_id"],
            "comment_ids": {},
            "observed_comment_ids": [],
            "media": [],
        }
        assert not recorder.reads and not recorder.writes


def test_comment_metadata_disabled_preserves_hosted_and_rejects_missing_parent(database):
    raw = copy.deepcopy(CAPTURE["comment"])
    raw["replies"] = []
    raw["hosted_media_urls"] = ["https://media.example/comment.gif"]
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "MetadataFlagFixture"})
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))
        write_comments(
            conn,
            run_id=run_id,
            account="fixture_account",
            observed_at=OBSERVED,
            post_id=ids["post_id"],
            comments=[normalize_comment_source(raw)],
            source_payloads=[raw],
        )
        poor = {
            "comment_id": "comment1",
            "username": "commenter",
            "text": "Updated",
            "media_urls": [],
            "is_ranked": False,
            "phase": "headload",
            "replies": [],
        }
        write_comments(
            conn,
            run_id=run_id,
            account="fixture_account",
            observed_at=OBSERVED + timedelta(hours=1),
            post_id=ids["post_id"],
            comments=[normalize_comment_source(poor)],
            source_payloads=[poor],
            metadata_writes_enabled=False,
        )
        row = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="comment1")
        assert row["hosted_media_urls"] == raw["hosted_media_urls"]
        assert row["author_full_name"] == "Fixture Commenter"
        assert row["is_ranked"] is True
        assert "phase" not in row["raw_data"] and "is_ranked" not in row["raw_data"]
        missing = {
            "comment_id": "missing-reply",
            "text": "Reply",
            "username": "replier",
            "parent_comment_id": "absent-parent",
            "replies": [],
        }
        with pytest.raises(CanonicalWriteError, match="parent is absent"):
            write_comments(
                conn,
                run_id=run_id,
                account="fixture_account",
                observed_at=OBSERVED,
                post_id=ids["post_id"],
                comments=[normalize_comment_source(missing)],
                source_payloads=[missing],
            )


def test_legacy_timestamp_prevents_old_replay_before_canonical_materialization(database):
    with database() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_posts(shortcode,username,caption,scraped_at)
            values ('LegacyFixture','fixture_account','Existing legacy',%s)""",
            (OBSERVED,),
        )
        with pytest.raises(OlderObservation):
            post(
                conn, source={**CAPTURE["post"], "shortcode": "LegacyFixture"}, observed=OBSERVED - timedelta(seconds=1)
            )
        assert not load(conn, "social_posts", source_id="LegacyFixture")
        assert load(conn, "instagram_posts", shortcode="LegacyFixture")["caption"] == "Existing legacy"


def test_concurrent_runs_serialize_by_canonical_identity(database):
    raw = {**CAPTURE["post"], "shortcode": "ConcurrentFixture"}
    with database() as conn:
        ids = post(conn, source=raw)

    def update(offset):
        with database() as conn:
            try:
                post(conn, source={**raw, "caption": f"capture-{offset}"}, observed=OBSERVED + timedelta(hours=offset))
            except OlderObservation:
                return "older-rejected"
        return "written"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(update, [2, 1]))
    assert outcomes[0] == "written"
    with database() as conn:
        row = load(conn, "instagram_posts", id=ids["post_id"])
        assert row["caption"] == "capture-2" and row["scraped_at"] == OBSERVED + timedelta(hours=2)


def test_same_capture_duplicate_phases_keep_rank_and_replies(database):
    ranked = copy.deepcopy(CAPTURE["comment"])
    headload = {
        **ranked,
        "is_ranked": False,
        "comment_index": None,
        "phase": "headload",
        "cursor_payload": {"cursor_param": "next"},
        "replies": [],
    }
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "DuplicatePhaseFixture"})
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))
        result = write_comments(
            conn,
            run_id=run_id,
            account="fixture_account",
            observed_at=OBSERVED,
            post_id=ids["post_id"],
            comments=[normalize_comment_source(ranked), normalize_comment_source(headload)],
            source_payloads=[ranked, headload],
        )
        row = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="comment1")
        assert row["is_ranked"] is True and row["comment_index"] == 3
        assert row["cursor_payload"]["phase_observations"] == ["ranked", "headload"]
        assert result["observed_comment_ids"] == ["comment1", "reply1"]


def test_sanitized_native_capture_replays_using_original_utc_instant(database):
    captured = datetime(2026, 10, 16, 17, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    raw = {
        "items": [
            {
                "code": "CapturedFixture",
                "pk": "native-captured",
                "media_type": 1,
                "taken_at": "1792065600",
                "caption": {"text": "Current captured caption"},
                "like_count": 4,
                "user": {"username": "fixture_account", "pk": "native-owner"},
                "image_versions2": {"candidates": [{"url": "https://cdn.example/capture.jpg?sig=excluded"}]},
            }
        ]
    }
    evidence = Evidence.capture(
        source_identity="instagram:CapturedFixture", captured_at=captured, parser_version="fixture-v1", payload=raw
    )
    source = evidence.read(pinned=True)
    assert evidence.captured_at == OBSERVED
    with database() as conn:
        ids = post(conn, source=source, observed=evidence.captured_at)
        row = load(conn, "instagram_posts", id=ids["post_id"])
        assert row["scraped_at"] == OBSERVED
        assert row["posted_at"] == datetime.fromtimestamp(1792065600, UTC)
        assert row["media_urls"] == ["https://cdn.example/capture.jpg"]
        observation = load(
            conn, "social_post_observations", post_id=ids["canonical_post_id"], source_table="instagram_posts"
        )
        assert observation["observed_at"] == evidence.captured_at
        assert observation["raw_payload"] == source
        older = Evidence.capture(
            source_identity="instagram:CapturedFixture",
            captured_at=captured - timedelta(seconds=1),
            parser_version="fixture-v1",
            payload=raw,
        )
        with pytest.raises(OlderObservation):
            post(conn, source=older.read(pinned=True), observed=older.captured_at)


def test_detail_snapshot_and_metadata_timestamp_preservation(database):
    snapshot = {
        "version": 1,
        "source": "fixture-response",
        "observed_at": OBSERVED.isoformat(),
        "coverage_version": 2,
        "media_complete": True,
        "caption_present": True,
        "likes_present": True,
        "comments_present": True,
        "media_count": 2,
    }
    rich = {
        **CAPTURE["post"],
        "shortcode": "CoverageFixture",
        "detail_snapshot": snapshot,
        "metadata_scraped_at": "2026-10-16T08:00:00-04:00",
    }
    with database() as conn:
        ids = post(conn, source=rich)
        assert load(conn, "instagram_posts", id=ids["post_id"])["metadata_scraped_at"] == OBSERVED
        poor = {
            "shortcode": "CoverageFixture",
            "pk": "100001",
            "post_type": "carousel",
            "username": "fixture_account",
            "comments": 2,
        }
        post(conn, source=poor, observed=OBSERVED + timedelta(hours=1))
        row = load(conn, "instagram_posts", id=ids["post_id"])
        assert row["raw_data"]["detail_snapshot"] == snapshot
        assert row["raw_data"]["detail_snapshot"]["observed_at"] != row["scraped_at"].isoformat()
        invalid = {**rich, "metadata_scraped_at": "2026-10-16T12:00:00"}
        with pytest.raises(CanonicalWriteError, match="timezone"):
            post(conn, source=invalid, observed=OBSERVED + timedelta(hours=2))
        assert load(conn, "instagram_posts", id=ids["post_id"])["scraped_at"] == OBSERVED + timedelta(hours=1)


@pytest.mark.parametrize("new_reference", [None, {"evidence_id": "new-capture", "captured_at": "new-time"}])
def test_new_detail_observation_replaces_its_complete_proof(database, new_reference):
    shortcode = "ProofReplacement" + ("NoRef" if new_reference is None else "NewRef")
    old_proof = {
        "version": 1,
        "coverage_version": 2,
        "observed_at": OBSERVED.isoformat(),
        "evidence_reference": {"evidence_id": "old-capture", "captured_at": "old-time"},
        "old_only_coverage": True,
    }
    new_time = OBSERVED + timedelta(hours=1)
    new_proof = {"version": 1, "coverage_version": 2, "observed_at": new_time.isoformat()}
    if new_reference is not None:
        new_proof["evidence_reference"] = new_reference
    with database() as conn:
        ids = post(
            conn,
            source={
                **CAPTURE["post"],
                "shortcode": shortcode,
                "detail_snapshot": old_proof,
                "preserved": {"old": True},
            },
        )
        post(
            conn,
            source={
                **CAPTURE["post"],
                "shortcode": shortcode,
                "detail_snapshot": new_proof,
                "preserved": {"new": True},
            },
            observed=new_time,
        )
        for table, key, identity in (
            ("instagram_posts", "id", ids["post_id"]),
            ("instagram_post_payloads", "post_id", ids["post_id"]),
            ("instagram_account_catalog_posts", "id", ids["catalog_post_id"]),
            ("instagram_account_catalog_post_payloads", "catalog_post_id", ids["catalog_post_id"]),
        ):
            row = load(conn, table, **{key: identity})
            assert row["raw_data"]["detail_snapshot"] == new_proof
            assert row["raw_data"]["preserved"] == {"old": True, "new": True}


def test_native_comment_media_owner_and_reported_reply_count(database):
    raw = {
        "pk": "native-media-comment",
        "user": {
            "pk": "user1",
            "username": "native_commenter",
            "hd_profile_pic_url_info": {"url": "https://cdn.example/avatar-hd.jpg"},
        },
        "text": "Native media",
        "created_at": "2026-10-15T08:00:00-04:00",
        "fbid_v2": "fb-native",
        "is_private": False,
        "media": {
            "image_versions2": {"candidates": [{"url": "https://cdn.example/comment-primary.jpg"}]},
            "gif": {"images": {"original": {"url": "https://cdn.example/comment.gif"}}},
        },
        "child_comments": [
            {
                "pk": "nested-native-reply",
                "user": {"username": "replier"},
                "text": "Reply without reported count",
                "created_at": 1792065600,
            }
        ],
    }
    normalized = normalize_comment_source(raw)
    assert normalized.reply_count == 0 and len(normalized.replies) == 1
    assert normalized.owner_profile_pic_url_hd == "https://cdn.example/avatar-hd.jpg"
    with database() as conn:
        ids = post(conn, source={**CAPTURE["post"], "shortcode": "NativeMediaFixture"})
        run_id = str(uuid4())
        with conn.cursor() as cur:
            cur.execute("insert into social.scrape_runs values(%s)", (run_id,))
        result = write_comments(
            conn,
            run_id=run_id,
            account="fixture_account",
            observed_at=OBSERVED,
            post_id=ids["post_id"],
            comments=[normalized],
            source_payloads=[raw],
        )
        row = load(conn, "instagram_comments", post_id=ids["post_id"], comment_id="native-media-comment")
        assert row["reply_count"] == 0
        assert row["raw_data"]["reply_count_observed"] == 1
        assert row["created_at"] == datetime(2026, 10, 15, 12, tzinfo=UTC)
        assert row["author_fbid_v2"] == "fb-native" and row["author_is_private"] is False
        assert row["media_urls"] == ["https://cdn.example/comment-primary.jpg", "https://cdn.example/comment.gif"]
        assert result["media"][0]["media_urls"] == row["media_urls"]
    with pytest.raises(CanonicalWriteError, match="timezone"):
        normalize_comment_source({**raw, "created_at": "2026-10-15T12:00:00"})


@pytest.mark.parametrize(
    "assignment", [{"assignment_status": "assigned"}, {"assigned_show_id": str(uuid4())}, {"candidate_matches": []}]
)
def test_catalog_assignments_cannot_be_written_to_legacy_post_columns(assignment):
    with pytest.raises(CanonicalWriteError, match="Unsupported assignment"):
        write_post(
            None,
            run_id=str(uuid4()),
            account="fixture_account",
            observed_at=OBSERVED,
            normalized=normalize_post_source(CAPTURE["post"], account="fixture_account"),
            source_payload=CAPTURE["post"],
            assignment=assignment,
        )


@pytest.mark.parametrize(
    "assignment", [{"show_id": str(uuid4())}, {"person_id": str(uuid4())}, {"assignment_status": "assigned"}]
)
def test_comment_assignment_columns_match_migrations(assignment):
    with pytest.raises(CanonicalWriteError, match="Unsupported assignment"):
        write_comments(
            None,
            run_id=str(uuid4()),
            account="fixture_account",
            observed_at=OBSERVED,
            post_id=str(uuid4()),
            comments=[],
            source_payloads=[],
            assignment=assignment,
        )
