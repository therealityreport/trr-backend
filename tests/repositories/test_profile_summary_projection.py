"""Summary parity and projection maintenance against isolated, disposable Postgres."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

import trr_backend.socials.social_season_analytics_impl as repo

pytestmark = pytest.mark.live
ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = [
    ROOT / "supabase/migrations/20261022042524_instagram_profile_summary_projection.sql",
    ROOT / "supabase/migrations/20261022042542_profile_recent_run_indexes.sql",
    ROOT / "supabase/migrations/20261022072404_instagram_profile_header_scalars.sql",
    ROOT / "supabase/migrations/20261022191037_instagram_profile_catalog_accounts.sql",
    ROOT / "supabase/migrations/20261022202851_instagram_catalog_compact_summary.sql",
]


@pytest.fixture(scope="module")
def postgres():
    binary_dir = os.getenv("POSTGRES_TEST_BINDIR")
    initdb = str(Path(binary_dir) / "initdb") if binary_dir else shutil.which("initdb")
    pg_ctl = str(Path(binary_dir) / "pg_ctl") if binary_dir else shutil.which("pg_ctl")
    if not initdb or not pg_ctl:
        pytest.skip("Local PostgreSQL binaries are required")
    with tempfile.TemporaryDirectory(prefix="trr-summary-pg-", dir="/tmp") as scratch:
        data = str(Path(scratch) / "data")
        subprocess.run(
            [initdb, "-D", data, "-A", "trust", "-U", "postgres", "--no-locale", "--encoding=UTF8"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        subprocess.run(
            [
                pg_ctl,
                "-D",
                data,
                "-l",
                str(Path(scratch) / "server.log"),
                "-o",
                f"-F -h '' -k {scratch}",
                "-w",
                "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        try:
            conn = psycopg2.connect(host=scratch, user="postgres", dbname="postgres")
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("create schema social; create role anon; create role authenticated")
                for table in ("instagram_posts", "instagram_account_catalog_posts"):
                    counter_type = "integer" if table == "instagram_account_catalog_posts" else "bigint"
                    cur.execute(f"""create table social.{table} (
                        id uuid primary key, source_account text, owner_username text, username text,
                        raw_data jsonb, collaborators jsonb, collaborators_detail jsonb,
                        likes {counter_type}, comments_count {counter_type}, fb_comment_count bigint, views bigint,
                        posted_at timestamptz, shortcode text, media_urls jsonb, hosted_media_urls jsonb,
                        hosted_owner_profile_pic_url text, assignment_status text, caption text,
                        text text, hashtags jsonb)""")
                cur.execute("""create table social.instagram_post_comment_rollups (
                    post_id uuid, active_comment_count int, missing_comment_count int);
                    create table social.scrape_jobs (id uuid, platform text, config jsonb, metadata jsonb,
                    completed_at timestamptz, started_at timestamptz, created_at timestamptz,
                    job_type text, run_id uuid)""")
                for migration in MIGRATIONS:
                    cur.execute(migration.read_text())
            yield conn
            conn.close()
        finally:
            subprocess.run(
                [pg_ctl, "-D", data, "-m", "immediate", "-w", "stop"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )


@pytest.fixture
def database(postgres, monkeypatch):
    with postgres.cursor() as cur:
        cur.execute(
            "truncate social.instagram_posts, social.instagram_account_catalog_posts, "
            "social.instagram_post_comment_rollups cascade"
        )

    def fetch(sql, params):
        with postgres.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return dict(cur.fetchone())

    monkeypatch.setattr(repo.pg, "fetch_one", fetch)
    return postgres


@pytest.mark.parametrize(
    "raw",
    [
        None,
        [],
        "invalid-shape",
        7,
        {
            "source_account": " @BravoTV ",
            "comments_count": "1,200",
            "comment_count": 300,
            "media": {"commentsCount": 1500, "ignored": "x" * 10000},
            "metrics": {"comments_count": 1600},
            "facebook_crosspost": {"comments_count": 50},
            "edge_media_to_parent_comment": {"count": 1700},
        },
        {
            "owner_username": "elsewhere",
            "collaborators": [{"username": "@BravoTV"}, "OTHER", 123],
            "collaborators_detail": {"handle": "bravotv"},
            "comments": 8,
            "edge_media_preview_comment": {"count": 15},
            "fb_comment_count": "3",
        },
        {"username": "bravotv", "media": [1, 2], "metrics": "bad", "comments": {"count": 12}},
        {"username": "bravotv", "comments": {"x123": None}},
        {"profile_tags": ["bravotv"], "source_account": "unrelated", "ignored": "x" * 10000},
    ],
)
def test_projection_preserves_summary_results(database, monkeypatch, raw):
    with database.cursor() as cur:
        for table in ("instagram_posts", "instagram_account_catalog_posts"):
            cur.execute(
                f"""insert into social.{table}
                (id,source_account,raw_data,likes,comments_count,fb_comment_count,views,shortcode,
                 media_urls,hosted_media_urls,assignment_status,hashtags)
                values (%s,'unrelated',%s::jsonb,10,4,2,20,'abc','["a","b"]','["a"]','assigned','["tag"]')""",
                [str(uuid4()), json.dumps(raw)],
            )
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: set())
    before = (
        repo._shared_catalog_summary_totals("instagram", "bravotv"),
        repo._instagram_social_account_lite_header_stats("bravotv"),
    )
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data"})
    after = (
        repo._shared_catalog_summary_totals("instagram", "bravotv"),
        repo._instagram_social_account_lite_header_stats("bravotv"),
    )
    assert after == before
    monkeypatch.setattr(
        repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data", "profile_summary_header"}
    )
    assert repo._instagram_social_account_lite_header_stats("bravotv") == before[1]


def test_trigger_updates_projection_and_ignores_supplied_derived_value(database):
    with database.cursor() as cur:
        row_id = str(uuid4())
        cur.execute(
            """insert into social.instagram_posts(id,raw_data,profile_summary_raw_data)
            values (%s,'{"comments_count":10,"ignored":"payload"}','{"comments_count":999}')""",
            [row_id],
        )
        cur.execute("select profile_summary_raw_data from social.instagram_posts where id=%s", [row_id])
        projection = cur.fetchone()[0]
        assert projection["comments_count"] == 10 and "ignored" not in projection
        cur.execute(
            "update social.instagram_posts set raw_data='{" + '"comments_count":20' + "}' where id=%s", [row_id]
        )
        cur.execute(
            "select profile_summary_raw_data->>'comments_count' from social.instagram_posts where id=%s", [row_id]
        )
        assert cur.fetchone()[0] == "20"
        cur.execute("update social.instagram_posts set profile_summary_raw_data=null where id=%s", [row_id])
        cur.execute(
            "select profile_summary_raw_data->>'comments_count' from social.instagram_posts where id=%s", [row_id]
        )
        assert cur.fetchone()[0] == "20"


def test_null_projection_falls_back_and_migrations_are_repeatable(database, monkeypatch):
    with database.cursor() as cur:
        cur.execute("alter table social.instagram_posts disable trigger trg_instagram_profile_summary_raw")
        cur.execute(
            """insert into social.instagram_posts(id,source_account,raw_data,shortcode)
            values (%s,'bravotv','{"comments_count":9}','legacy')""",
            [str(uuid4())],
        )
        cur.execute("alter table social.instagram_posts enable trigger trg_instagram_profile_summary_raw")
        for migration in MIGRATIONS:
            cur.execute(migration.read_text())
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data"})
    summary = repo._instagram_social_account_lite_header_stats("bravotv")
    assert summary["comments_saved_summary"]["retrieved_comments"] == 9


@pytest.mark.parametrize(
    "path",
    [
        "comments_count",
        "comments",
        "comment_count",
        "commentsCount",
        "edge_media_to_comment.count",
        "edge_media_to_parent_comment.count",
        "edge_media_preview_comment.count",
        "media.comments_count",
        "media.comments",
        "media.comment_count",
        "media.commentsCount",
        "metrics.comments_count",
        "metrics.comments",
        "facebook_crosspost.comments_count",
        "facebook_crosspost.fb_comment_count",
        "fb_comment_count",
    ],
)
def test_header_projection_preserves_each_count_path(database, monkeypatch, path):
    raw = {}
    nested = raw
    parts = path.split(".")
    for key in parts[:-1]:
        nested[key] = {}
        nested = nested[key]
    nested[parts[-1]] = {"x123": None}
    row_id = str(uuid4())
    with database.cursor() as cur:
        cur.execute(
            """insert into social.instagram_posts
            (id,source_account,raw_data,comments_count,fb_comment_count,shortcode,media_urls,hosted_media_urls)
            values (%s,'bravotv',%s::jsonb,4,-2,'abc','["a","b"]','["a","b","c"]')""",
            [row_id, json.dumps(raw)],
        )
        cur.execute("insert into social.instagram_post_comment_rollups values (%s,7,3)", [row_id])
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: set())
    before = repo._instagram_social_account_lite_header_stats("bravotv")
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_header"})
    after = repo._instagram_social_account_lite_header_stats("bravotv")
    assert after == before
    assert after["comments_saved_summary"]["retrieved_comments"] >= 123
    assert after["media_coverage"]["saved_post_media_files"] == 2


@pytest.mark.parametrize(
    "field,value,account",
    [
        ("source_account", " @@BrAvOtV ", "bravotv"),
        ("owner_username", " @@BrAvOtV ", "bravotv"),
        ("username", " @@BrAvOtV ", "bravotv"),
        ("collaborators", {"handle": " @@BrAvOtV "}, "bravotv"),
        ("collaborators_detail", " @@BrAvOtV ", "bravotv"),
        ("collaborators", [{"username": "", "handle": "bravotv"}], "bravotv"),
        ("collaborators_detail", [None, True, 123], "123"),
        ("raw_data", {"collaborators_detail": {"user_name": " @@BrAvOtV "}}, "bravotv"),
        ("raw_data", {"profile_tags": ["bravotv"]}, "bravotv"),
    ],
)
def test_header_projection_preserves_account_matching(database, monkeypatch, field, value, account):
    with database.cursor() as cur:
        sql_type = "jsonb" if field in {"collaborators", "collaborators_detail", "raw_data"} else "text"
        bound = json.dumps(value) if sql_type == "jsonb" else value
        cur.execute(
            f"""insert into social.instagram_posts(id,{field},shortcode)
            values (%s,%s::{sql_type},'abc')""",
            [str(uuid4()), bound],
        )
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: set())
    before = repo._instagram_social_account_lite_header_stats(account)
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_header"})
    assert repo._instagram_social_account_lite_header_stats(account) == before


def test_header_projection_tracks_source_updates_and_rejects_supplied_value(database, monkeypatch):
    row_id = str(uuid4())
    with database.cursor() as cur:
        cur.execute(
            """insert into social.instagram_posts(id,source_account,shortcode,profile_summary_header)
            values (%s,'bravotv','abc','{"reported_comments":999,"accounts":["fake"]}')""",
            [row_id],
        )
        updates = [
            ("source_account", "other", "text"),
            ("owner_username", "bravotv", "text"),
            ("username", "another", "text"),
            ("collaborators", '["collab"]', "jsonb"),
            ("collaborators_detail", '{"handle":"details"}', "jsonb"),
            ("raw_data", '{"username":"rawowner","comments_count":15}', "jsonb"),
            ("comments_count", 20, "bigint"),
            ("fb_comment_count", -6, "bigint"),
            ("media_urls", '["a","b"]', "jsonb"),
            ("hosted_media_urls", '["a"]', "jsonb"),
            ("profile_summary_header", '{"reported_comments":999}', "jsonb"),
        ]
        for field, value, sql_type in updates:
            cur.execute(f"update social.instagram_posts set {field}=%s::{sql_type} where id=%s", [value, row_id])
            monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: set())
            before = repo._instagram_social_account_lite_header_stats("bravotv")
            monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_header"})
            assert repo._instagram_social_account_lite_header_stats("bravotv") == before, field
        cur.execute("select profile_summary_header from social.instagram_posts where id=%s", [row_id])
        header = cur.fetchone()[0]
        assert header["reported_comments"] == 20
        assert header["facebook_comments"] == 6
        assert set(header["accounts"]) == {"other", "bravotv", "another", "collab", "details", "rawowner"}


@pytest.mark.parametrize("raw,media", [({"comments_count": "9" * 30}, []), ({}, {}), ({}, None)])
def test_bad_legacy_values_do_not_break_writes_and_keep_read_fallback(database, monkeypatch, raw, media):
    row_id = str(uuid4())
    with database.cursor() as cur:
        cur.execute(
            """insert into social.instagram_posts(id,source_account,shortcode,raw_data,media_urls)
            values (%s,'bravotv','abc',%s::jsonb,%s::jsonb)""",
            [row_id, json.dumps(raw), json.dumps(media)],
        )
        cur.execute("select profile_summary_header from social.instagram_posts where id=%s", [row_id])
        assert cur.fetchone()[0] is None
    errors = []
    for columns in [set(), {"profile_summary_header"}]:
        monkeypatch.setattr(repo, "_relation_columns", lambda *a, _columns=columns, **k: _columns)
        with pytest.raises(psycopg2.Error) as error:
            repo._instagram_social_account_lite_header_stats("bravotv")
        errors.append(error.value.pgcode)
    assert errors[0] == errors[1]


def test_unbackfilled_header_and_empty_accounts_keep_results(database, monkeypatch):
    with database.cursor() as cur:
        cur.execute("alter table social.instagram_posts disable trigger trg_instagram_profile_header")
        cur.execute(
            """insert into social.instagram_posts(id,source_account,shortcode,comments_count)
            values (%s,'bravotv','abc',9)""",
            [str(uuid4())],
        )
        cur.execute("alter table social.instagram_posts enable trigger trg_instagram_profile_header")
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: set())
    before = repo._instagram_social_account_lite_header_stats("bravotv")
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_header"})
    assert repo._instagram_social_account_lite_header_stats("bravotv") == before
    assert repo._instagram_social_account_lite_header_stats("missing")["totals"]["total_posts"] == 0


def test_catalog_native_short_circuit_preserves_legacy_matching(database):
    values = [
        (" @@BrAvOtV ", None, None, None, {}),
        (None, " @bravotv ", None, None, {}),
        (None, None, "bravotv", None, {}),  # Not a native catalog identity.
        ("other", None, None, '" @bravotv "', {}),
        ("other", None, None, '{"handle":"bravotv"}', {}),
        ("other", None, None, '[{"username":"","handle":"bravotv"}]', {}),
        (None, None, None, "[null,true,123]", {}),
        ("other", None, None, None, {"username": " @bravotv "}),
        ("other", None, None, None, {"owner_username": "bravotv"}),
        ("other", None, None, None, {"collaborators_detail": {"user_name": " @bravotv "}}),
        ("other", None, None, None, {"profile_tags": ["bravotv"]}),
    ]
    with database.cursor() as cur:
        for source, owner, username, collaborators, raw in values:
            cur.execute(
                """insert into social.instagram_account_catalog_posts
                (id,source_account,owner_username,username,collaborators,raw_data)
                values (%s,%s,%s,%s,%s::jsonb,%s::jsonb)""",
                [str(uuid4()), source, owner, username, collaborators, json.dumps(raw)],
            )
        for account in ["bravotv", "other", "123", "true", "missing"]:
            matches = []
            for short_circuit in [False, True]:
                predicate = repo._instagram_account_match_sql(
                    alias="p",
                    native_columns=frozenset({"source_account", "owner_username", "raw_data", "collaborators"}),
                    raw_data_sql="coalesce(p.profile_summary_raw_data,p.raw_data,'{}'::jsonb)",
                    short_circuit_direct=short_circuit,
                )
                cur.execute(
                    "select id from social.instagram_account_catalog_posts p where " + predicate + " order by id",
                    [account],
                )
                matches.append(cur.fetchall())
            assert matches[0] == matches[1], account
            if account == "bravotv":
                assert len(matches[0]) == 7


@pytest.mark.parametrize(
    "source,owner,collaborators,raw",
    [
        (" @@BrAvOtV ", None, None, None),
        (None, " @BrAvOtV ", None, []),
        (None, None, None, "invalid-shape"),
        (None, None, None, 7),
        ("other", None, " @bravotv ", {}),
        ("other", None, {"handle": "@bravotv"}, {}),
        (None, None, [{"username": "", "handle": "bravotv"}], {}),
        (None, None, [None, True, 123, {"name": "@bravotv"}], {}),
        (None, None, {"username": ["bravotv"]}, {}),
        (None, None, None, {"source_account": " @bravotv "}),
        (None, None, None, {"owner_username": "bravotv"}),
        (None, None, None, {"username": "bravotv", "comments_count": "9" * 30}),
        (None, None, None, {"collaborators": ["@BravoTV", "bravotv"]}),
        (None, None, None, {"collaborators_detail": {"user_name": " @bravotv "}}),
        (None, None, None, {"collaborators_detail": True, "profile_tags": ["bravotv"]}),
        ("bravotv", "bravotv", ["bravotv"], {"username": "bravotv"}),
    ],
)
def test_catalog_accounts_preserve_membership_and_all_aggregates(
    database, monkeypatch, source, owner, collaborators, raw
):
    with database.cursor() as cur:
        # Native username/details exist only in this superset fixture. They must
        # not become catalog identities, unlike their raw JSON counterparts.
        cur.execute(
            """insert into social.instagram_account_catalog_posts
            (id,source_account,owner_username,username,collaborators,collaborators_detail,raw_data,
             likes,comments_count,views,posted_at,assignment_status,caption,text,hashtags)
            values (%s,%s,%s,'excluded',%s::jsonb,'["excluded"]',%s::jsonb,
                    10,4,20,'2026-10-22Z','assigned','caption','text','["one","two"]')""",
            [str(uuid4()), source, owner, json.dumps(collaborators), json.dumps(raw)],
        )
        predicate = repo._instagram_account_match_sql(
            alias="p",
            native_columns=frozenset({"source_account", "owner_username", "raw_data", "collaborators"}),
            raw_data_sql="coalesce(p.profile_summary_raw_data,p.raw_data,'{}'::jsonb)",
            short_circuit_direct=True,
        )
        for account in ["bravotv", "other", "123", "true", "excluded", "missing"]:
            cur.execute("select id from social.instagram_account_catalog_posts p where " + predicate, [account])
            legacy_ids = cur.fetchall()
            cur.execute(
                "select id from social.instagram_account_catalog_posts where profile_summary_accounts ? %s::text",
                [account],
            )
            assert cur.fetchall() == legacy_ids, account
            monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data"})
            before = repo._shared_catalog_summary_totals("instagram", account)
            monkeypatch.setattr(
                repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data", "profile_summary_accounts"}
            )
            assert repo._shared_catalog_summary_totals("instagram", account) == before, account
            monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_catalog_ready"})
            assert repo._shared_catalog_summary_totals("instagram", account) == before, account


def test_catalog_accounts_updates_and_mixed_backfill_are_exact(database, monkeypatch):
    row_id = str(uuid4())
    with database.cursor() as cur:
        cur.execute(
            """insert into social.instagram_account_catalog_posts(id,source_account,profile_summary_accounts)
            values (%s,'bravotv','["forged"]'::jsonb)""",
            [row_id],
        )
        updates = [
            ("source_account", "other", "text", ["other"]),
            ("owner_username", "@bravotv", "text", ["bravotv", "other"]),
            ("collaborators", '["collab"]', "jsonb", ["bravotv", "collab", "other"]),
            (
                "raw_data",
                '{"username":"rawowner","collaborators_detail":{"name":"detail"}}',
                "jsonb",
                ["bravotv", "collab", "detail", "other", "rawowner"],
            ),
            ("profile_summary_accounts", '["forged"]', "jsonb", ["bravotv", "collab", "detail", "other", "rawowner"]),
        ]
        for field, value, sql_type, expected in updates:
            cur.execute(
                f"update social.instagram_account_catalog_posts set {field}=%s::{sql_type} where id=%s", [value, row_id]
            )
            cur.execute(
                "select profile_summary_accounts from social.instagram_account_catalog_posts where id=%s", [row_id]
            )
            assert cur.fetchone()[0] == expected, field
        cur.execute("select profile_summary_accounts from social.instagram_account_catalog_posts where id=%s", [row_id])
        assert cur.fetchone()[0] == ["bravotv", "collab", "detail", "other", "rawowner"]
        cur.execute(
            "alter table social.instagram_account_catalog_posts disable trigger trg_instagram_profile_catalog_accounts"
        )
        cur.execute(
            """insert into social.instagram_account_catalog_posts(id,raw_data,likes,comments_count)
            values (%s,'{"collaborators":["bravotv"]}',7,3)""",
            [str(uuid4())],
        )
        cur.execute(
            "alter table social.instagram_account_catalog_posts enable trigger trg_instagram_profile_catalog_accounts"
        )
        cur.execute("insert into social.instagram_account_catalog_posts(id) values (%s)", [str(uuid4())])
        # Reapplying the additive migration must preserve all rows and NULL fallback.
        cur.execute(MIGRATIONS[-1].read_text())
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data"})
    before = repo._shared_catalog_summary_totals("instagram", "bravotv")
    assert before["catalog_total_posts"] == 2
    monkeypatch.setattr(
        repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data", "profile_summary_accounts"}
    )
    assert repo._shared_catalog_summary_totals("instagram", "bravotv") == before
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_catalog_ready"})
    assert repo._shared_catalog_summary_totals("instagram", "bravotv") == before
    assert repo._shared_catalog_summary_totals("instagram", "missing")["catalog_total_posts"] == 0


@pytest.mark.parametrize(
    "identity",
    ["x" * 16384, "é" * 8192, {"username": {"nested": "x" * 16384}}],
    ids=["ascii", "multibyte", "nested-object"],
)
def test_catalog_oversized_identities_remain_writable_and_exact(database, monkeypatch, identity):
    row_id = str(uuid4())
    with database.cursor() as cur:
        cur.execute(
            """insert into social.instagram_account_catalog_posts(id,source_account,raw_data)
            values (%s,'bravotv',%s::jsonb)""",
            [row_id, json.dumps({"collaborators": [identity]})],
        )
        cur.execute("select profile_summary_accounts from social.instagram_account_catalog_posts where id=%s", [row_id])
        accounts = cur.fetchone()[0]
        long_identity = next(account for account in accounts if account != "bravotv")
        assert len(long_identity.encode()) > 8191
        # The indexed membership operator must also recheck the full long value.
        cur.execute(
            "select id::text from social.instagram_account_catalog_posts where profile_summary_accounts ? %s::text",
            [long_identity],
        )
        assert cur.fetchall() == [(row_id,)]
        cur.execute(
            "update social.instagram_account_catalog_posts set collaborators=%s::jsonb where id=%s",
            [json.dumps([identity]), row_id],
        )
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data"})
    before = repo._shared_catalog_summary_totals("instagram", "bravotv")
    monkeypatch.setattr(
        repo, "_relation_columns", lambda *a, **k: {"profile_summary_raw_data", "profile_summary_accounts"}
    )
    assert repo._shared_catalog_summary_totals("instagram", "bravotv") == before
    monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_catalog_ready"})
    assert repo._shared_catalog_summary_totals("instagram", "bravotv") == before


def _assert_compact_catalog_parity(monkeypatch, accounts=("bravotv", "other", "missing")):
    for account in accounts:
        monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: set())
        expected = repo._shared_catalog_summary_totals("instagram", account)
        monkeypatch.setattr(repo, "_relation_columns", lambda *a, **k: {"profile_summary_catalog_ready"})
        assert repo._shared_catalog_summary_totals("instagram", account) == expected, account


@pytest.mark.parametrize(
    "caption,text,hashtags",
    [
        (None, None, None),
        (None, "fallback", []),
        ("", "ignored", [None, {}, 1]),
        ("   ", "ignored", []),
        ("\t", None, ["a", "a"]),
        ("caption", "other", ["one", "two"]),
    ],
)
def test_compact_catalog_preserves_all_aggregate_inputs(database, monkeypatch, caption, text, hashtags):
    with database.cursor() as cur:
        for status, likes, comments, views, posted_at in [
            ("assigned", 10, 4, 20, "2026-10-22Z"),
            ("needs_review", -10, None, -20, "2026-10-21Z"),
            ("unassigned", None, -3, None, None),
            (None, 0, 0, 0, "2026-10-23Z"),
        ]:
            cur.execute(
                """insert into social.instagram_account_catalog_posts
                (id,source_account,likes,comments_count,views,posted_at,assignment_status,caption,text,hashtags)
                values (%s,'bravotv',%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
                [
                    str(uuid4()),
                    likes,
                    comments,
                    views,
                    posted_at,
                    status,
                    caption,
                    text,
                    json.dumps(hashtags) if hashtags is not None else None,
                ],
            )
    _assert_compact_catalog_parity(monkeypatch)


def test_compact_catalog_tracks_every_input_and_protects_markers(database, monkeypatch):
    row_id = str(uuid4())
    with database.cursor() as cur:
        cur.execute(
            """insert into social.instagram_account_catalog_posts
            (id,source_account,profile_summary_catalog_ready,profile_summary_accounts)
            values (%s,'bravotv',false,'["forged"]')""",
            [row_id],
        )
        updates = [
            ("likes", 12, "integer"),
            ("comments_count", -3, "integer"),
            ("views", 42, "bigint"),
            ("posted_at", "2026-10-22Z", "timestamptz"),
            ("assignment_status", "needs_review", "text"),
            ("caption", "caption", "text"),
            ("caption", None, "text"),
            ("text", "fallback", "text"),
            ("hashtags", '["one",null,{}]', "jsonb"),
            ("source_account", "other", "text"),
            ("owner_username", "bravotv", "text"),
            ("collaborators", '["collab"]', "jsonb"),
            ("raw_data", '{"username":"rawowner"}', "jsonb"),
            ("profile_summary_accounts", '["forged"]', "jsonb"),
            ("profile_summary_catalog_ready", False, "boolean"),
            ("profile_summary_catalog_ready", None, "boolean"),
        ]
        for field, value, sql_type in updates:
            cur.execute(
                f"update social.instagram_account_catalog_posts set {field}=%s::{sql_type} where id=%s", [value, row_id]
            )
            _assert_compact_catalog_parity(monkeypatch, ("bravotv", "other", "collab", "rawowner", "forged"))
            cur.execute(
                "select profile_summary_catalog_ready from social.instagram_account_catalog_posts where id=%s", [row_id]
            )
            assert cur.fetchone()[0] is True, field
        cur.execute("select xmin::text from social.instagram_profile_catalog_summary where post_id=%s", [row_id])
        before_xmin = cur.fetchone()[0]
        # An unrelated update must refresh missing inputs if needed, but should
        # not rewrite an already identical projection tuple.
        cur.execute("update social.instagram_account_catalog_posts set shortcode='unrelated' where id=%s", [row_id])
        cur.execute("select xmin::text from social.instagram_profile_catalog_summary where post_id=%s", [row_id])
        assert cur.fetchone()[0] == before_xmin
        new_id = str(uuid4())
        cur.execute("update social.instagram_account_catalog_posts set id=%s where id=%s", [new_id, row_id])
        cur.execute("select post_id::text from social.instagram_profile_catalog_summary")
        assert cur.fetchall() == [(new_id,)]
        cur.execute("delete from social.instagram_account_catalog_posts where id=%s", [new_id])
        cur.execute("select count(*) from social.instagram_profile_catalog_summary")
        assert cur.fetchone()[0] == 0


@pytest.mark.parametrize("hashtags", [None, {}, "scalar", 7, True])
def test_compact_catalog_malformed_hashtags_keep_source_writes_and_read_errors(database, monkeypatch, hashtags):
    row_id = str(uuid4())
    with database.cursor() as cur:
        cur.execute(
            """insert into social.instagram_account_catalog_posts
            (id,source_account,hashtags,profile_summary_catalog_ready)
            values (%s,'bravotv',%s::jsonb,true)""",
            [row_id, json.dumps(hashtags)],
        )
        cur.execute(
            "select profile_summary_catalog_ready from social.instagram_account_catalog_posts where id=%s", [row_id]
        )
        assert cur.fetchone()[0] is False
        cur.execute("select count(*) from social.instagram_profile_catalog_summary")
        assert cur.fetchone()[0] == 0
    errors = []
    for columns in (set(), {"profile_summary_catalog_ready"}):
        monkeypatch.setattr(repo, "_relation_columns", lambda *a, _columns=columns, **k: _columns)
        with pytest.raises(psycopg2.Error) as error:
            repo._shared_catalog_summary_totals("instagram", "bravotv")
        errors.append((error.value.pgcode, error.value.diag.message_primary))
        assert repo._shared_catalog_summary_totals("instagram", "missing")["catalog_total_posts"] == 0
    assert errors[0] == errors[1]
    with database.cursor() as cur:
        cur.execute("update social.instagram_account_catalog_posts set hashtags='[]' where id=%s", [row_id])
    _assert_compact_catalog_parity(monkeypatch)
    with database.cursor() as cur:
        cur.execute(
            "update social.instagram_account_catalog_posts set hashtags=%s::jsonb where id=%s",
            [json.dumps(hashtags), row_id],
        )
        cur.execute("select count(*) from social.instagram_profile_catalog_summary")
        assert cur.fetchone()[0] == 0


def test_compact_catalog_unbackfilled_branches_and_table_absence(database, monkeypatch):
    with database.cursor() as cur:
        cur.execute(
            "insert into social.instagram_account_catalog_posts(id,source_account,likes) values (%s,'bravotv',10)",
            [str(uuid4())],
        )
        cur.execute(
            "alter table social.instagram_account_catalog_posts disable trigger "
            "trg_instagram_profile_catalog_summary_ready"
        )
        cur.execute(
            "alter table social.instagram_account_catalog_posts disable trigger trg_instagram_profile_catalog_summary"
        )
        legacy_id = str(uuid4())
        try:
            cur.execute(
                """insert into social.instagram_account_catalog_posts(id,raw_data,likes)
                        values (%s,'{"collaborators":["bravotv","bravotv"]}',7)""",
                [legacy_id],
            )
        finally:
            cur.execute(
                "alter table social.instagram_account_catalog_posts enable trigger "
                "trg_instagram_profile_catalog_summary_ready"
            )
            cur.execute(
                "alter table social.instagram_account_catalog_posts enable trigger "
                "trg_instagram_profile_catalog_summary"
            )
        cur.execute(MIGRATIONS[-1].read_text())
        cur.execute(
            "select profile_summary_catalog_ready from social.instagram_account_catalog_posts where id=%s", [legacy_id]
        )
        assert cur.fetchone()[0] is None
    _assert_compact_catalog_parity(monkeypatch)
    assert repo._shared_catalog_summary_totals("instagram", "bravotv")["catalog_total_posts"] == 2
    # An older schema is already exercised above; a partial rollout with the
    # marker present and the projection table absent must also use the old path.
    with database.cursor() as cur:
        cur.execute("alter table social.instagram_profile_catalog_summary rename to compact_temporarily_unavailable")
        try:
            _assert_compact_catalog_parity(monkeypatch)
        finally:
            cur.execute(
                "alter table social.compact_temporarily_unavailable rename to instagram_profile_catalog_summary"
            )
        cur.execute(
            "update social.instagram_account_catalog_posts set profile_summary_catalog_ready=null where id=%s",
            [legacy_id],
        )
        cur.execute("select count(*) from social.instagram_profile_catalog_summary")
        assert cur.fetchone()[0] == 2
    _assert_compact_catalog_parity(monkeypatch)


@pytest.mark.parametrize("counter_type,maximum", [("integer", 2**31 - 1), ("bigint", 2**63 - 1)])
def test_compact_catalog_preserves_native_counter_types_and_read_overflow(database, monkeypatch, counter_type, maximum):
    with database.cursor() as cur:
        cur.execute("begin")
        try:
            cur.execute("drop table social.instagram_profile_catalog_summary")
            cur.execute(
                f"alter table social.instagram_account_catalog_posts alter column likes type {counter_type}, "
                f"alter column comments_count type {counter_type}"
            )
            cur.execute(MIGRATIONS[-1].read_text().replace("begin;", "").replace("commit;", ""))
            cur.execute("""select format_type(atttypid,atttypmod) from pg_attribute
                        where attrelid='social.instagram_profile_catalog_summary'::regclass
                        and attname in ('likes','comments_count')""")
            assert cur.fetchall() == [(counter_type,), (counter_type,)]
            cur.execute(
                """insert into social.instagram_account_catalog_posts(id,source_account,likes,comments_count,views)
                        values (%s,'bravotv',%s,1,0)""",
                [str(uuid4()), maximum],
            )
            errors = []
            for columns in (set(), {"profile_summary_catalog_ready"}):
                monkeypatch.setattr(repo, "_relation_columns", lambda *a, _columns=columns, **k: _columns)
                cur.execute("savepoint read_error")
                with pytest.raises(psycopg2.errors.NumericValueOutOfRange) as error:
                    repo._shared_catalog_summary_totals("instagram", "bravotv")
                errors.append((error.value.pgcode, error.value.diag.message_primary))
                cur.execute("rollback to savepoint read_error")
            assert errors[0] == errors[1]
            cur.execute("update social.instagram_account_catalog_posts set comments_count=0")
            _assert_compact_catalog_parity(monkeypatch)
        finally:
            cur.execute("rollback")


def test_compact_catalog_aggregate_bigint_cast_errors_match(database, monkeypatch):
    with database.cursor() as cur:
        for _ in range(2):
            cur.execute(
                "insert into social.instagram_account_catalog_posts(id,source_account,views) values (%s,'bravotv',%s)",
                [str(uuid4()), 2**63 - 1],
            )
    errors = []
    for columns in (set(), {"profile_summary_catalog_ready"}):
        monkeypatch.setattr(repo, "_relation_columns", lambda *a, _columns=columns, **k: _columns)
        with pytest.raises(psycopg2.errors.NumericValueOutOfRange) as error:
            repo._shared_catalog_summary_totals("instagram", "bravotv")
        errors.append((error.value.pgcode, error.value.diag.message_primary))
    assert errors[0] == errors[1]


def test_compact_catalog_is_private_and_canonical_accounts_fill_missing_legacy_projection(database, monkeypatch):
    with database.cursor() as cur:
        cur.execute(
            "select relrowsecurity from pg_class where oid='social.instagram_profile_catalog_summary'::regclass"
        )
        assert cur.fetchone()[0] is True
        for role in ("anon", "authenticated"):
            cur.execute(
                "select has_table_privilege(%s,'social.instagram_profile_catalog_summary',"
                "'select,insert,update,delete')",
                [role],
            )
            assert cur.fetchone()[0] is False
        cur.execute(
            "alter table social.instagram_account_catalog_posts disable trigger trg_instagram_profile_catalog_accounts"
        )
        try:
            cur.execute(
                """insert into social.instagram_account_catalog_posts(id,raw_data)
                        values (%s,'{"collaborators_detail":{"handle":"bravotv"}}')""",
                [str(uuid4())],
            )
        finally:
            cur.execute(
                "alter table social.instagram_account_catalog_posts enable trigger "
                "trg_instagram_profile_catalog_accounts"
            )
        cur.execute("select profile_summary_accounts from social.instagram_profile_catalog_summary")
        assert cur.fetchone()[0] == ["bravotv"]
    _assert_compact_catalog_parity(monkeypatch)


def test_compact_catalog_preserves_non_owner_source_writer_permissions(database):
    row_id = str(uuid4())
    new_id = str(uuid4())
    with database.cursor() as cur:
        cur.execute("create role compact_source_writer")
        cur.execute("grant usage on schema social to compact_source_writer")
        cur.execute(
            "grant select,insert,update,delete on social.instagram_account_catalog_posts to compact_source_writer"
        )
        cur.execute(
            "select has_table_privilege('compact_source_writer',"
            "'social.instagram_profile_catalog_summary','select,insert,update,delete'), "
            "has_function_privilege('compact_source_writer','social.refresh_instagram_profile_catalog_summary()','execute')"
        )
        assert cur.fetchone() == (False, False)
        cur.execute("set role compact_source_writer")
        try:
            cur.execute(
                """insert into social.instagram_account_catalog_posts(id,source_account,likes)
                values (%s,'bravotv',1)""",
                [row_id],
            )
            cur.execute("update social.instagram_account_catalog_posts set likes=2 where id=%s", [row_id])
            cur.execute("update social.instagram_account_catalog_posts set hashtags='{}' where id=%s", [row_id])
            cur.execute(
                "update social.instagram_account_catalog_posts set hashtags='[]',id=%s where id=%s", [new_id, row_id]
            )
        finally:
            cur.execute("reset role")
        cur.execute("select post_id::text,likes from social.instagram_profile_catalog_summary")
        assert cur.fetchall() == [(new_id, 2)]
        cur.execute("set role compact_source_writer")
        try:
            cur.execute("delete from social.instagram_account_catalog_posts where id=%s", [new_id])
        finally:
            cur.execute("reset role")
        cur.execute("select count(*) from social.instagram_profile_catalog_summary")
        assert cur.fetchone()[0] == 0
