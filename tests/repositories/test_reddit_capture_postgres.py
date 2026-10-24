"""Real transactions in a task-owned, registered loopback PostgreSQL cluster.

No configured databases, provider APIs, buckets or existing local services are used.
The fixture executes checked-in prerequisites and both new migrations verbatim.
"""

from __future__ import annotations

import json
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from trr_backend.db import pg
from trr_backend.repositories import reddit_capture as capture
from trr_backend.repositories import reddit_workflows as workflows

MIGRATIONS = Path(__file__).resolve().parents[2] / "supabase" / "migrations"
CAPTURE_MIGRATION = "20261021210000_reddit_capture_v2.sql"
WORKFLOW_MIGRATION = "20261021210100_reddit_workflows.sql"


@pytest.fixture(scope="module")
def postgres_cluster(tmp_path_factory, register_isolated_postgres):
    root = tmp_path_factory.mktemp("reddit-capture-postgres")
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
            "reddit_test",
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

    @contextmanager
    def connection(**_kwargs):
        existing = pg._transaction_connection.get()
        if existing is not None:
            yield existing
            return
        conn = psycopg2.connect(host="127.0.0.1", port=port, dbname="postgres", user="reddit_test")
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("set local statement_timeout='8s'; set local lock_timeout='3s'")
                yield conn
        finally:
            conn.close()

    with register_isolated_postgres(host="127.0.0.1", port=port, dbname="postgres", user="reddit_test"):
        try:
            with connection() as conn, conn.cursor() as cur:
                cur.execute("""
                    create role anon; create role authenticated; create role service_role;
                    create schema core; create schema social; create schema admin;
                    create table core.seasons(id uuid primary key);
                    create table core.admin_operations(id uuid primary key,status text,
                        created_at timestamptz default now(),updated_at timestamptz default now());
                    create table core.google_news_sync_jobs(id uuid primary key,status text,
                        heartbeat_at timestamptz,created_at timestamptz default now(),
                        updated_at timestamptz default now());
                    grant usage on schema social,admin,core to anon,authenticated,service_role;
                """)
                for name in (
                    "0157_reddit_refresh_pipeline.sql",
                    "0166_enhanced_reddit_post_columns.sql",
                    "0167_enhanced_reddit_comment_columns.sql",
                    "0170_reddit_refresh_run_indexes.sql",
                    "0173_remote_job_plane_claims.sql",
                ):
                    cur.execute((MIGRATIONS / name).read_text())
                # Use the checked-in admin table definition, excluding unrelated application tables.
                snapshot = (MIGRATIONS / "20260402213000_supabase_connection_index_hardening.sql").read_text()
                start = snapshot.index('CREATE TABLE IF NOT EXISTS "admin"."reddit_threads"')
                end = snapshot.index('CREATE SCHEMA IF NOT EXISTS "admin";', start)
                cur.execute(snapshot[start:end])
                cur.execute((MIGRATIONS / CAPTURE_MIGRATION).read_text())
                cur.execute((MIGRATIONS / WORKFLOW_MIGRATION).read_text())
            yield SimpleNamespace(connection=connection, root=root, port=port)
        finally:
            subprocess.run(
                [str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"],
                check=True,
                capture_output=True,
            )


@pytest.fixture
def database(postgres_cluster, monkeypatch):
    monkeypatch.setattr(pg, "db_connection", postgres_cluster.connection)
    monkeypatch.setattr(pg, "db_read_connection", postgres_cluster.connection)
    monkeypatch.setenv("REDDIT_PIPELINE_V2_ENABLED", "1")
    with postgres_cluster.connection() as conn, conn.cursor() as cur:
        cur.execute("select schemaname,tablename from pg_tables where schemaname in ('social','admin')")
        tables = [sql.Identifier(schema, table) for schema, table in cur.fetchall()]
        cur.execute(sql.SQL("truncate {} cascade").format(sql.SQL(",").join(tables)))
        cur.execute("truncate core.seasons cascade")
        season = str(uuid4())
        cur.execute("insert into core.seasons(id) values(%s)", [season])
    return SimpleNamespace(connection=postgres_cluster.connection, season=season, community=str(uuid4()))


def intent(database, **extra):
    return {
        "community_id": database.community,
        "season_id": database.season,
        "period_key": "episode-1",
        "subreddit": "test",
        "show_name": "Test Show",
        "requested_phases": ["comments", "media"],
        **extra,
    }


def launch(database, **extra):
    return capture.create_run(intent(database, **extra))


def owned(database, **extra):
    run = launch(database, **extra)
    claimed = capture.claim_run(str(run["id"]), "isolated-test")
    assert claimed is not None
    return claimed


def add_post(post_id="post1", **extra):
    rows = pg.execute_returning(
        "insert into social.reddit_posts(reddit_post_id,subreddit,title,selftext,author,raw_payload,url) "
        "values(%s,'test','Kept title','Kept text',%s,%s::jsonb,%s) returning *",
        [
            post_id,
            extra.get("author", "fixture-author"),
            json.dumps(extra.get("raw_payload", {"text": "source"})),
            extra.get("url", "https://example.invalid/shared.jpg"),
        ],
    )
    return rows[0]


def test_both_migrations_reapply_without_enrolling_legacy_rows(database):
    post = add_post()
    legacy = pg.execute_returning(
        "insert into social.reddit_refresh_runs(community_id,season_id,period_key,subreddit,status) "
        "values(%s,%s,'legacy','test','completed') returning *",
        [database.community, database.season],
    )[0]
    with database.connection() as conn, conn.cursor() as cur:
        cur.execute((MIGRATIONS / CAPTURE_MIGRATION).read_text())
        cur.execute((MIGRATIONS / WORKFLOW_MIGRATION).read_text())
    assert pg.fetch_one("select contract_version from social.reddit_refresh_runs where id=%s", [legacy["id"]]) == {
        "contract_version": 1
    }
    assert capture.manifest_for_run(str(legacy["id"])) is None
    assert pg.fetch_all("select * from social.reddit_lifecycle_enrollment") == []
    assert (
        pg.fetch_all("select * from social.reddit_capture_state where reddit_post_id=%s", [post["reddit_post_id"]])
        == []
    )


def test_concurrent_identical_create_is_one_atomic_run_manifest_and_outbox(database):
    with ThreadPoolExecutor(max_workers=8) as pool:
        runs = list(pool.map(lambda _index: launch(database), range(16)))
    assert len({str(run["id"]) for run in runs}) == 1
    assert sum(not run["reused"] for run in runs) == 1
    for table in ("reddit_refresh_runs", "reddit_run_manifests", "reddit_dispatch_outbox"):
        assert pg.fetch_one(f"select count(*) as total from social.{table}")["total"] == 1


def test_conflicting_config_claims_are_serialized_by_scope(database):
    first = launch(database, refresh_policy="missing")
    second = launch(database, refresh_policy="force")
    assert first["id"] != second["id"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda run: capture.claim_run(str(run["id"]), "concurrent-test"), [first, second]))
    assert sum(claim is not None for claim in claims) == 1
    winner = next(claim for claim in claims if claim)
    loser = next(run for run in (first, second) if run["id"] != winner["id"])
    capture.control_run(str(winner["id"]), "cancel")
    assert capture.claim_run(str(loser["id"]), "after-cancel") is not None


def test_expired_claim_cannot_commit_target_or_atomic_canonical_write(database):
    run = owned(database)
    run_id, token = str(run["id"]), run["claim_token"]
    capture.ensure_target(run_id, "comments", "post1")
    capture.start_target(run_id, "comments", "post1", token)
    pg.execute("update social.reddit_refresh_runs set lease_expires_at=now()-interval '1 second' where id=%s", [run_id])
    with pytest.raises(capture.LostClaim), pg.transaction() as conn:
        add_post()
        capture.finish_target(run_id, "comments", "post1", token, "committed", result={"saved": True}, conn=conn)
    assert pg.fetch_all("select * from social.reddit_posts") == []
    assert capture.list_targets(run_id)[0]["state"] == "running"
    renewed = capture.claim_run(run_id, "replacement")
    assert renewed is not None and renewed["claim_token"] != token
    with pytest.raises(capture.LostClaim):
        capture.finish_target(run_id, "comments", "post1", token, "committed")


def test_pause_resume_cancel_fences_owner_and_preserves_satisfied_targets(database):
    run = owned(database)
    run_id, token = str(run["id"]), run["claim_token"]
    capture.ensure_target(run_id, "comments", "done")
    capture.ensure_target(run_id, "media", "pending")
    capture.finish_target(run_id, "comments", "done", token, "committed", result={"saved": 1})
    capture.control_run(run_id, "pause")
    with pytest.raises(capture.RunPaused):
        capture.start_target(run_id, "media", "pending", token)
    with pytest.raises(ValueError, match="checkpointing"):
        capture.control_run(run_id, "resume")
    pg.execute("update social.reddit_refresh_runs set claim_token=null,lease_expires_at=null where id=%s", [run_id])
    resumed = capture.control_run(run_id, "resume")
    assert resumed["control_state"] == "active"
    next_claim = capture.claim_run(run_id, "resumed-worker")
    assert next_claim is not None
    capture.control_run(run_id, "cancel")
    assert {row["target_key"]: row["state"] for row in capture.list_targets(run_id)} == {
        "done": "committed",
        "pending": "cancelled",
    }
    with pytest.raises(capture.LostClaim):
        capture.finish_target(run_id, "media", "pending", next_claim["claim_token"], "committed")
    assert pg.fetch_one("select state from social.reddit_dispatch_outbox where run_id=%s", [run_id])["state"] == "done"


def test_manual_retry_links_new_run_and_preserves_parent_evidence_and_satisfied_work(database):
    run = owned(database)
    run_id, token = str(run["id"]), run["claim_token"]
    capture.ensure_target(run_id, "comments", "post1", post_id="post1", checkpoint={"cursor": "kept"})
    capture.ensure_target(run_id, "media", "post2", post_id="post2")
    capture.finish_target(run_id, "comments", "post1", token, "retry_wait", result={"partial_rows": 7}, error="retry")
    capture.finish_target(run_id, "media", "post2", token, "committed", result={"evidence": "kept"})
    pg.execute(
        "update social.reddit_run_manifests set coverage=%s::jsonb where run_id=%s",
        [json.dumps({"state": "partial", "cursor": "coverage-kept"}), run_id],
    )
    pg.execute(
        "update social.reddit_refresh_runs set status='partial',claim_token=null,lease_expires_at=null where id=%s",
        [run_id],
    )
    prior_targets = capture.list_targets(run_id)
    child = capture.control_run(run_id, "retry")
    assert str(child["parent_run_id"]) == run_id
    assert str(child["id"]) != run_id
    assert capture.list_targets(run_id) == prior_targets
    targets = {row["target_key"]: row for row in capture.list_targets(str(child["id"]))}
    assert targets["post1"]["state"] == "pending"
    assert targets["post1"]["checkpoint"] == {"cursor": "kept"}
    assert targets["post1"]["result"] == {"partial_rows": 7}
    assert targets["post1"]["attempt_count"] == 0
    assert targets["post2"]["state"] == "committed"
    assert targets["post2"]["result"] == {"evidence": "kept"}
    assert capture.manifest_for_run(str(child["id"]))["coverage"] == {"state": "partial", "cursor": "coverage-kept"}


def test_target_uniqueness_preserves_first_checkpoint_under_concurrency(database):
    run_id = str(launch(database)["id"])
    with ThreadPoolExecutor(max_workers=8) as pool:
        targets = list(
            pool.map(
                lambda index: capture.ensure_target(run_id, "comments", "post1", checkpoint={"first_writer": index}),
                range(16),
            )
        )
    assert len({str(row["id"]) for row in targets}) == 1
    assert len(capture.list_targets(run_id)) == 1
    assert len({json.dumps(row["checkpoint"], sort_keys=True) for row in targets}) == 1


def test_media_capture_reuses_shared_asset_and_keeps_both_associations(database, monkeypatch):
    from trr_backend.media import s3_mirror
    from trr_backend.repositories import reddit_execution

    posts = [add_post("post1"), add_post("post2")]
    run = owned(database)
    execution = capture.Execution(run)
    uploads = []

    def mirror(url, **_kwargs):
        uploads.append(url)
        return SimpleNamespace(
            status="mirrored",
            hosted_key="fixture/shared.jpg",
            hosted_url="https://fixture/shared.jpg",
            sha256="fixture-digest",
            content_type="image/jpeg",
            size_bytes=10,
        )

    monkeypatch.setattr(s3_mirror, "mirror_url_to_s3", mirror)
    for post in posts:
        capture.ensure_target(str(run["id"]), "media", post["reddit_post_id"], post_id=post["reddit_post_id"])
        reddit_execution.media_capture(post, execution)
    assets = pg.fetch_all("select * from social.reddit_assets")
    links = pg.fetch_all("select * from social.reddit_asset_links order by reddit_post_id")
    assert len(uploads) == 1
    assert len(assets) == 1 and assets[0]["state"] == "mirrored"
    assert [row["reddit_post_id"] for row in links] == ["post1", "post2"]
    assert {row["asset_id"] for row in links} == {assets[0]["id"]}
    assert all(row["state"] == "committed" for row in capture.list_targets(str(run["id"])))


@pytest.mark.parametrize("role", ["anon", "authenticated"])
def test_rls_and_grants_deny_operational_tables_to_public_roles(database, role):
    tables = pg.fetch_all(
        "select c.relname,c.relrowsecurity from pg_class c join pg_namespace n on n.oid=c.relnamespace "
        "where n.nspname='social' and c.relkind='r' and (c.relname like 'reddit_%%') "
        "and c.relname not in ('reddit_posts','reddit_comments','reddit_period_post_matches','reddit_media_mirrors')"
    )
    assert len(tables) >= 19
    assert all(row["relrowsecurity"] for row in tables)
    for table in tables:
        allowed = pg.fetch_one(
            "select has_table_privilege(%s,%s,'SELECT,INSERT,UPDATE,DELETE') as allowed",
            [role, "social." + table["relname"]],
        )
        assert allowed["allowed"] is False
        with pytest.raises(psycopg2.errors.InsufficientPrivilege), database.connection() as conn, conn.cursor() as cur:
            cur.execute(sql.SQL("set local role {}").format(sql.Identifier(role)))
            cur.execute(sql.SQL("select * from social.{}").format(sql.Identifier(table["relname"])))


def test_lifecycle_comment_tombstone_retains_ids_and_blocks_recapture(database):
    add_post()
    pg.execute(
        "insert into social.reddit_comments(reddit_post_id,reddit_comment_id,body,author,body_html,raw_payload) "
        "values('post1','comment1','private text','fixture-author','<p>private text</p>',%s::jsonb)",
        [json.dumps({"body": "private text", "author": "fixture-author"})],
    )
    run = owned(database)
    capture.ensure_target(str(run["id"]), "comments", "post1", post_id="post1", checkpoint={"body": "private text"})
    with pg.transaction() as conn:
        workflows._redact_source("post1", "comment1", "deleted", conn)
    saved = pg.fetch_one("select * from social.reddit_comments where reddit_comment_id='comment1'")
    assert saved["reddit_post_id"] == "post1"
    assert saved["body"] == "[deleted]" and saved["author"] is None and saved["body_html"] is None
    assert saved["raw_payload"] == {}
    assert pg.fetch_one("select * from social.reddit_comment_tombstones where id='comment1'")["signal"] == "deleted"
    assert workflows.filter_comment_tombstones([{"reddit_post_id": "post1", "reddit_comment_id": "comment1"}]) == []
    assert capture.list_targets(str(run["id"]))[0]["checkpoint"] == {}


def test_lifecycle_job_deletes_only_unreferenced_asset_and_keeps_shared_copy(database):
    for post_id in ("post1", "post2"):
        add_post(post_id)
        pg.execute(
            "insert into social.reddit_period_post_matches(community_id,season_id,period_key,reddit_post_id) "
            "values(%s,%s,'episode-1',%s)",
            [database.community, database.season, post_id],
        )
        pg.execute(
            "insert into social.reddit_lifecycle_enrollment(reddit_post_id,reason) values(%s,'new_capture')", [post_id]
        )
    asset = pg.execute_returning(
        "insert into social.reddit_assets(source_url,media_type,state,hosted_key,hosted_url) "
        "values('https://fixture/shared.jpg','image','mirrored','shared-key','https://fixture/hosted') returning id"
    )[0]
    for post_id in ("post1", "post2"):
        pg.execute(
            "insert into social.reddit_asset_links(asset_id,reddit_post_id,association_key) values(%s,%s,%s)",
            [asset["id"], post_id, post_id + ":post"],
        )
    deleted = []

    def reconcile(post_id):
        job = workflows.enqueue_job(
            community_id=database.community,
            season_id=database.season,
            kind="lifecycle",
            payload={"post_ids": [post_id]},
        )
        return workflows.execute_job(
            str(job["id"]),
            lifecycle_provider=lambda _ids: [
                {"reddit_post_id": post_id, "source": "reddit", "explicit": True, "signal": "deleted", "checked": True}
            ],
            object_deleter=deleted.append,
        )

    reconcile("post1")
    assert deleted == []
    assert pg.fetch_one("select state from social.reddit_assets where id=%s", [asset["id"]])["state"] == "mirrored"
    assert pg.fetch_one("select count(*) as total from social.reddit_asset_links")["total"] == 1
    reconcile("post2")
    assert deleted == ["shared-key"]
    assert (
        pg.fetch_one("select state from social.reddit_assets where id=%s", [asset["id"]])["state"]
        == "source_unavailable"
    )
    assert pg.fetch_one("select title,author from social.reddit_posts where reddit_post_id='post1'") == {
        "title": "[deleted]",
        "author": None,
    }
    assert pg.fetch_one("select count(*) as total from social.reddit_posts")["total"] == 2


def test_author_tombstone_retains_content_and_redacts_cross_post_identity(database):
    for post_id in ("post1", "post2"):
        add_post(
            post_id,
            author="SameAuthor",
            raw_payload={
                "author": "SameAuthor",
                "title": "Kept title",
                "nested": {"author_id": "identity", "body": "Kept body"},
            },
        )
        pg.execute(
            "insert into social.reddit_lifecycle_enrollment(reddit_post_id,reason) values(%s,'new_capture')",
            [post_id],
        )
    add_post("legacy", author="SameAuthor", raw_payload={"author": "SameAuthor", "title": "Legacy title"})
    pg.execute(
        "insert into social.reddit_comments(reddit_post_id,reddit_comment_id,body,author,raw_payload) "
        "values('post2','comment1','Kept comment','sameauthor',%s::jsonb)",
        [json.dumps({"author": "sameauthor", "body": "Kept comment"})],
    )
    with pg.transaction() as conn:
        affected = workflows._redact_author("post1", None, conn)
    assert affected == {"post1", "post2"}
    assert pg.fetch_one("select count(*) as total from social.reddit_author_tombstones")["total"] == 1
    for post in pg.fetch_all("select * from social.reddit_posts where reddit_post_id<>'legacy'"):
        assert post["author"] is None
        assert post["title"] == "Kept title" and post["selftext"] == "Kept text"
        assert post["raw_payload"] == {"title": "Kept title", "nested": {"body": "Kept body"}}
    assert pg.fetch_one("select author,raw_payload from social.reddit_posts where reddit_post_id='legacy'") == {
        "author": "SameAuthor",
        "raw_payload": {"author": "SameAuthor", "title": "Legacy title"},
    }
    comment = pg.fetch_one("select * from social.reddit_comments where reddit_comment_id='comment1'")
    assert comment["author"] is None and comment["body"] == "Kept comment"
    fresh = workflows.scrub_tombstoned_authors(
        [
            {
                "reddit_post_id": "post1",
                "author": "SAMEAUTHOR",
                "body": "Fresh content",
                "raw_payload": {"author": "SameAuthor", "body": "Fresh content"},
            }
        ]
    )
    assert fresh[0]["author"] is None
    assert fresh[0]["body"] == "Fresh content" and fresh[0]["raw_payload"] == {"body": "Fresh content"}


def _associate(database, post_id):
    pg.execute(
        "insert into social.reddit_period_post_matches(community_id,season_id,period_key,reddit_post_id) "
        "values(%s,%s,'episode-1',%s)",
        [database.community, database.season, post_id],
    )


def _make_retries_due(run_id):
    pg.execute("update social.reddit_refresh_runs set next_retry_at=now()-interval '1 second' where id=%s", [run_id])
    pg.execute(
        "update social.reddit_work_targets set next_retry_at=now()-interval '1 second' "
        "where run_id=%s and state='retry_wait'",
        [run_id],
    )


def _raw_page(post_id, posted_at, after=None):
    return {
        "data": {
            "after": after,
            "children": [
                {
                    "kind": "t3",
                    "data": {
                        "id": post_id,
                        "title": "Test Show discussion",
                        "url": f"https://reddit.com/comments/{post_id}",
                        "selftext": "Source body",
                        "author": "fixture-author",
                        "score": 10,
                        "num_comments": 1,
                        "created_utc": posted_at,
                    },
                }
            ],
        }
    }


def test_engine_discovery_failure_resumes_saved_page_without_refetch(database, monkeypatch):
    from datetime import UTC, datetime

    from trr_backend.repositories import reddit_execution, reddit_transport

    calls = []

    class Client:
        def get_json(self, path, *, params):
            calls.append((path, dict(params)))
            if len(calls) == 1:
                return _raw_page("post1", datetime(2026, 10, 20, tzinfo=UTC).timestamp(), "t3_next")
            if len(calls) == 2:
                raise reddit_transport.RedditTransportError("temporary rate limit", status=429, retry_after_seconds=1)
            assert params["after"] == "t3_next"
            return _raw_page("post2", datetime(2026, 10, 19, tzinfo=UTC).timestamp())

    monkeypatch.setattr(reddit_transport, "build_client", lambda **_kwargs: Client())
    run = owned(
        database,
        requested_phases=["discovery"],
        search_backfill=False,
        max_pages=3,
        is_show_focused=True,
        period_start="2026-10-19T00:00:00Z",
        period_end="2026-10-22T00:00:00Z",
    )
    run_id = str(run["id"])
    first = reddit_execution.execute(run)
    assert first["status"] == "queued"
    assert pg.fetch_one("select count(*) as total from social.reddit_posts")["total"] == 1
    query = next(row for row in capture.list_targets(run_id) if row["target_key"] == "listing:new")
    assert query["state"] == "retry_wait"
    assert query["checkpoint"]["after"] == "t3_next" and query["checkpoint"]["pages"] == 1
    assert (
        next(row for row in capture.list_targets(run_id) if row["target_key"] == "listing:new:page:1")["state"]
        == "committed"
    )
    _make_retries_due(run_id)
    resumed = capture.claim_run(run_id, "retry-discovery")
    assert resumed is not None
    final = reddit_execution.execute(resumed)
    assert final["status"] == "completed"
    assert len(calls) == 3
    assert [call[1].get("after") for call in calls] == [None, "t3_next", "t3_next"]
    assert pg.fetch_one("select count(*) as total from social.reddit_posts")["total"] == 2
    assert pg.fetch_one("select count(*) as total from social.reddit_period_post_matches")["total"] == 2
    assert capture.run_contract(run_id)["coverage"]["state"] == "complete"
    assert all(row["state"] == "committed" for row in capture.list_targets(run_id))
    assert (
        pg.fetch_one("select count(*) as total from social.reddit_run_post_observations where run_id=%s", [run_id])[
            "total"
        ]
        == 2
    )


def test_engine_incomplete_comments_retry_with_same_reported_count_and_deduplicate(database, monkeypatch):
    from trr_backend.repositories import reddit_execution, reddit_transport

    add_post(num_comments=1)
    pg.execute("update social.reddit_posts set num_comments=1 where reddit_post_id='post1'")
    _associate(database, "post1")
    calls = []
    row = {
        "reddit_post_id": "post1",
        "reddit_comment_id": "comment1",
        "parent_comment_id": None,
        "author": "fixture-author",
        "body": "Saved once",
        "depth": 0,
        "score": 3,
        "created_at_utc": "2026-10-20T00:00:00Z",
        "raw_payload": {"body": "Saved once"},
    }

    class Client:
        def get_comments(self, post_id, checkpoint=None, on_checkpoint=None):
            calls.append(copy_checkpoint := json.loads(json.dumps(checkpoint)))
            complete = len(calls) > 1
            if complete:
                assert copy_checkpoint["pending_children"] == [{"id": "comment2", "parent_id": "t3_post1", "depth": 0}]
            saved = {
                "version": 1,
                "post_id": post_id,
                "initial_fetched": True,
                "comments": [row],
                "pending_children": [] if complete else [{"id": "comment2", "parent_id": "t3_post1", "depth": 0}],
                "pending_continuations": [],
            }
            on_checkpoint(saved)
            return {
                "comments": [row],
                "unresolved_ids": [] if complete else ["comment2"],
                "complete": complete,
                "source_unavailable": False,
                "evidence": {"checkpoint": saved},
            }

    monkeypatch.setattr(reddit_transport, "build_client", lambda **_kwargs: Client())
    run = owned(database, requested_phases=["comments"])
    run_id = str(run["id"])
    first = reddit_execution.execute(run)
    assert first["status"] == "queued"
    assert capture.list_targets(run_id)[0]["state"] == "retry_wait"
    state = pg.fetch_one("select * from social.reddit_capture_state where reddit_post_id='post1' and phase='comments'")
    assert state["state"] == "retry_wait" and state["successful_at"] is None
    _make_retries_due(run_id)
    resumed = capture.claim_run(run_id, "retry-comments")
    final = reddit_execution.execute(resumed)
    assert final["status"] == "completed"
    assert len(calls) == 2 and calls[0] is None
    assert pg.fetch_one("select count(*) as total from social.reddit_comments")["total"] == 1
    assert capture.list_targets(run_id)[0]["attempt_count"] == 2
    assert capture.list_targets(run_id)[0]["state"] == "committed"
    state = pg.fetch_one("select * from social.reddit_capture_state where reddit_post_id='post1' and phase='comments'")
    assert state["state"] == "committed" and state["successful_at"] is not None
    assert pg.fetch_one(
        "select comments_state,saved_comments from social.reddit_run_post_observations "
        "where run_id=%s and reddit_post_id='post1'",
        [run_id],
    ) == {"comments_state": "committed", "saved_comments": 1}


def test_engine_media_failure_then_success_preserves_retry_targets(database, monkeypatch):
    from trr_backend.media import s3_mirror
    from trr_backend.repositories import reddit_execution, reddit_transport

    add_post()
    _associate(database, "post1")
    pg.execute(
        "insert into social.reddit_capture_state(reddit_post_id,phase,state,successful_at) "
        "values('post1','comments','committed',now())"
    )
    uploads = []

    def mirror(url, **_kwargs):
        uploads.append(url)
        if len(uploads) == 1:
            return SimpleNamespace(status="failed", hosted_key=None, error="temporary fixture upload failure")
        return SimpleNamespace(
            status="mirrored",
            hosted_key="fixture-key",
            hosted_url="https://fixture/hosted.jpg",
            sha256="fixture-digest",
            content_type="image/jpeg",
            size_bytes=10,
        )

    monkeypatch.setattr(reddit_transport, "build_client", lambda **_kwargs: object())
    monkeypatch.setattr(s3_mirror, "mirror_url_to_s3", mirror)
    run = owned(database, requested_phases=["media"])
    run_id = str(run["id"])
    first = reddit_execution.execute(run)
    assert first["status"] == "queued"
    assert {target["state"] for target in capture.list_targets(run_id)} == {"retry_wait"}
    assert pg.fetch_all("select * from social.reddit_assets") == []
    _make_retries_due(run_id)
    resumed = capture.claim_run(run_id, "retry-media")
    final = reddit_execution.execute(resumed)
    assert final["status"] == "completed"
    assert len(uploads) == 2
    assert all(target["state"] == "committed" for target in capture.list_targets(run_id))
    assert pg.fetch_one("select count(*) as total from social.reddit_assets")["total"] == 1
    assert pg.fetch_one("select count(*) as total from social.reddit_asset_links")["total"] == 1
    assert (
        pg.fetch_one("select media_state from social.reddit_run_post_observations where run_id=%s", [run_id])[
            "media_state"
        ]
        == "committed"
    )


def test_stale_claim_cannot_save_discovery_page_or_matches(database):
    from datetime import UTC, datetime

    from trr_backend.repositories import reddit_discovery, reddit_refresh

    run = owned(database, requested_phases=["discovery"])
    execution = capture.Execution(run)
    rows = reddit_refresh._parse_listing_rows(
        _raw_page("post1", datetime(2026, 10, 20, tzinfo=UTC).timestamp())["data"]["children"],
        source_sort="listing:new",
    )
    pg.execute(
        "update social.reddit_refresh_runs set lease_expires_at=now()-interval '1 second' where id=%s", [run["id"]]
    )
    with pytest.raises(capture.LostClaim), execution.transaction() as conn:
        reddit_discovery.save_page(rows, intent(database), execution, conn)
    assert pg.fetch_all("select * from social.reddit_posts") == []
    assert pg.fetch_all("select * from social.reddit_period_post_matches") == []


def test_failed_page_transaction_retries_original_cursor(database, monkeypatch):
    from datetime import UTC, datetime

    from trr_backend.repositories import reddit_discovery, reddit_execution, reddit_transport

    calls = []

    class Client:
        def get_json(self, path, *, params):
            calls.append(params.get("after"))
            return _raw_page("post1", datetime(2026, 10, 19, tzinfo=UTC).timestamp(), "t3_unsaved")

    monkeypatch.setattr(reddit_transport, "build_client", lambda **kw: Client())
    original = reddit_discovery.save_page
    failed = False

    def flaky(*args):
        nonlocal failed
        saved = original(*args)
        if not failed:
            failed = True
            raise RuntimeError("Simulated write rollback after post save")
        return saved

    monkeypatch.setattr(reddit_discovery, "save_page", flaky)
    run = owned(
        database,
        requested_phases=["discovery"],
        search_backfill=False,
        is_show_focused=True,
        period_start="2026-10-19T00:00:00Z",
        max_pages=3,
    )
    run_id = str(run["id"])
    reddit_execution.execute(run)
    assert pg.fetch_one("select count(*) as n from social.reddit_posts")["n"] == 0
    target = capture.list_targets(run_id)[0]
    assert target["checkpoint"] == {}
    _make_retries_due(run_id)
    result = reddit_execution.execute(capture.claim_run(run_id, "retry"))
    assert result["status"] == "completed"
    assert calls == [None, None]
    assert pg.fetch_one("select count(*) as n from social.reddit_posts")["n"] == 1


def test_missing_selected_post_is_explicit_unresolved_work(database, monkeypatch):
    from trr_backend.repositories import reddit_execution, reddit_transport

    monkeypatch.setattr(reddit_transport, "build_client", lambda **kw: object())
    run = owned(database, requested_phases=["comments"], selected_post_ids=["missingpost"])
    result = reddit_execution.execute(run)
    assert result["status"] == "failed"
    target = capture.list_targets(str(run["id"]))[0]
    assert target["state"] == "failed"
    assert target["reddit_post_id"] == "missingpost"
    assert capture.run_contract(str(run["id"]))["unresolved"] == 1


def test_abandoned_fifth_attempt_becomes_failed(database):
    run = owned(database)
    run_id = str(run["id"])
    add_post()
    capture.ensure_target(run_id, "comments", "post1", post_id="post1")
    capture.record_capture("post1", "comments", "retry_wait", run_id)
    pg.execute("update social.reddit_capture_state set next_retry_at=now()-interval '1 minute'")
    pg.execute("update social.reddit_work_targets set state='running',attempt_count=5 where run_id=%s", [run_id])
    assert capture.start_target(run_id, "comments", "post1", run["claim_token"]) is None
    target = capture.list_targets(run_id)[0]
    assert target["state"] == "failed"
    assert "exhausted" in target["error"]
    state = pg.fetch_one("select * from social.reddit_capture_state where reddit_post_id='post1'")
    assert state["state"] == "failed" and state["next_retry_at"] is None
    assert "exhausted" in state["evidence"]["error"]


def test_episode_retry_retains_checkpoint_and_respects_due_time(database, monkeypatch):
    from trr_backend.repositories import reddit_episode, reddit_transport
    from trr_backend.repositories import reddit_episode_jobs as jobs

    client = SimpleNamespace(request_guard=lambda lane: None)
    monkeypatch.setattr(reddit_transport, "build_client", lambda **kw: client)
    checkpoints = []

    def discover(payload, *, client, checkpoint, on_checkpoint):
        checkpoints.append(checkpoint)
        if len(checkpoints) == 1:
            on_checkpoint({"version": 1, "queries": {"listing:new": {"rows": [], "after": "t3_saved"}}})
            raise reddit_transport.RedditTransportError("temporary", status=429, retry_after_seconds=90)
        return {"candidates": [], "expected_episode_numbers": []}

    monkeypatch.setattr(reddit_episode, "discover_episode", discover)
    job = jobs.create_job(database.community, {"subreddit": "test"})
    job_id = str(job["id"])
    first = jobs.execute_job(job_id)
    assert first["state"] == "queued"
    assert first["checkpoint"]["queries"]["listing:new"]["after"] == "t3_saved"
    jobs.execute_job(job_id)
    assert len(checkpoints) == 1
    pg.execute(
        "update social.reddit_episode_discovery_jobs set next_attempt_at=now()-interval '1 second' where id=%s",
        [job_id],
    )
    final = jobs.execute_job(job_id)
    assert final["candidates"] == []
    assert checkpoints[1]["queries"]["listing:new"]["after"] == "t3_saved"


def test_episode_snapshot_blocks_tombstoned_content_and_invalidates_claim(database):
    from trr_backend.repositories import reddit_episode_jobs as jobs

    post = add_post()
    _associate(database, "post1")
    run = owned(database)
    workflows.enroll_capture(post_id=post["reddit_post_id"], run_id=str(run["id"]))
    job = jobs.create_job(database.community, {"subreddit": "test"})
    pg.execute(
        "update social.reddit_episode_discovery_jobs set state='running',claim_token=gen_random_uuid(), "
        "lease_expires_at=now()+interval '5 minutes' where id=%s",
        [job["id"]],
    )
    with pg.transaction() as conn:
        workflows.lock_content(post_ids=["post1"], conn=conn)
        workflows._redact_snapshots("post1", conn)
    invalidated = jobs.get_job(str(job["id"]))
    assert invalidated["state"] == "failed" and invalidated["claim_token"] is None
    pg.execute("update social.reddit_lifecycle_enrollment set state='deleted' where reddit_post_id='post1'")
    with pg.transaction() as conn:
        snapshot = jobs.safe_snapshot(
            {"queries": {"new": {"rows": [{"reddit_post_id": "post1", "title": "Deleted"}]}}}, conn
        )
    assert snapshot["queries"]["new"]["rows"] == []
    assert jobs.create_job(database.community, {"subreddit": "test"})["id"] != job["id"]


def test_media_upload_without_database_commit_retries_existing_object(database, monkeypatch):
    from trr_backend.media import s3_mirror
    from trr_backend.repositories import reddit_execution, reddit_transport

    add_post()
    _associate(database, "post1")
    pg.execute(
        "insert into social.reddit_capture_state(reddit_post_id,phase,state,successful_at) "
        "values('post1','comments','committed',now())"
    )
    uploads = []

    def mirror(url, **kwargs):
        assert kwargs["key_namespace"] == "reddit"
        uploads.append(url)
        return SimpleNamespace(
            status="mirrored" if len(uploads) == 1 else "skipped",
            hosted_key="reddit/fixture.jpg",
            hosted_url="https://fixture/hosted.jpg",
            sha256="digest",
            content_type="image/jpeg",
            size_bytes=10,
        )

    original = capture._one
    failed = False

    def flaky(sql, params=None, **kwargs):
        nonlocal failed
        result = original(sql, params, **kwargs)
        if "insert into social.reddit_assets" in sql and not failed:
            failed = True
            raise RuntimeError("Database rollback after upload")
        return result

    monkeypatch.setattr(capture, "_one", flaky)
    monkeypatch.setattr(s3_mirror, "mirror_url_to_s3", mirror)
    monkeypatch.setattr(reddit_transport, "build_client", lambda **kw: object())
    run = owned(database, requested_phases=["media"])
    run_id = str(run["id"])
    assert reddit_execution.execute(run)["status"] == "queued"
    assert pg.fetch_one("select count(*) as n from social.reddit_assets")["n"] == 0
    _make_retries_due(run_id)
    assert reddit_execution.execute(capture.claim_run(run_id, "retry"))["status"] == "completed"
    assert len(uploads) == 2
    assert pg.fetch_one("select count(*) as n from social.reddit_asset_links")["n"] == 1


@pytest.mark.parametrize("author", ["[deleted]", "[removed]", "", None, "RealAccount"])
@pytest.mark.parametrize("comment_id", [None, "scope-comment"])
def test_author_redaction_stays_within_locked_account_scope(database, monkeypatch, author, comment_id):
    for post_id in ("scope-a", "scope-b"):
        add_post(post_id, author=author, raw_payload={"author": author, "selftext": "preserved"})
        pg.execute(
            "insert into social.reddit_lifecycle_enrollment(reddit_post_id,reason) values(%s,'new_capture')",
            [post_id],
        )
        pg.execute(
            "insert into social.reddit_comments(reddit_post_id,reddit_comment_id,author,body,raw_payload) "
            "values(%s,%s,%s,'preserved',%s::jsonb)",
            [
                post_id,
                "scope-comment" if post_id == "scope-a" else "other-comment",
                author,
                json.dumps({"author": author, "body": "preserved"}),
            ],
        )
    locked = set()
    original_lock = workflows.lock_content

    def record_lock(*, post_ids, conn):
        locked.update(post_ids)
        original_lock(post_ids=post_ids, conn=conn)

    monkeypatch.setattr(workflows, "lock_content", record_lock)
    with pg.transaction() as conn:
        affected = workflows._redact_author("scope-a", comment_id, conn)
    expected = {"scope-a", "scope-b"} if author == "RealAccount" else {"scope-a"}
    assert affected == locked == expected
    other = pg.fetch_one("select author,raw_payload from social.reddit_posts where reddit_post_id='scope-b'")
    other_comment = pg.fetch_one(
        "select author,body,raw_payload from social.reddit_comments where reddit_comment_id='other-comment'"
    )
    assert other["author"] == (None if author == "RealAccount" else author)
    assert other_comment["author"] == (None if author == "RealAccount" else author)
    assert other_comment["body"] == "preserved"
    if author != "RealAccount":
        assert other["raw_payload"] == {"author": author, "selftext": "preserved"}
        assert other_comment["raw_payload"] == {"author": author, "body": "preserved"}
    target_table = "reddit_comments" if comment_id else "reddit_posts"
    target_column = "reddit_comment_id" if comment_id else "reddit_post_id"
    target = pg.fetch_one(
        f"select author from social.{target_table} where {target_column}=%s", [comment_id or "scope-a"]
    )
    assert target["author"] is None
    count = pg.fetch_one("select count(*) as n from social.reddit_author_tombstones")["n"]
    assert count == (1 if author == "RealAccount" else 0)


@pytest.mark.parametrize("field", ["checkpoint", "result", "error"])
def test_snapshot_redaction_does_not_rewrite_already_scrubbed_targets(database, field):
    add_post("post1")
    add_post("post2")
    run = owned(database)
    run_id = str(run["id"])
    pg.execute(
        "insert into social.reddit_period_post_matches(community_id,season_id,period_key,reddit_post_id) "
        "values(%s,%s,'episode-1','post1')",
        [database.community, database.season],
    )
    for post_id in ("post1", "post2"):
        capture.ensure_target(run_id, "comments", post_id, post_id=post_id)
    value = "private error" if field == "error" else json.dumps({"private": "source"})
    pg.execute(f"update social.reddit_work_targets set {field}=%s where run_id=%s", [value, run_id])
    with pg.transaction() as conn:
        workflows._redact_snapshots("post1", conn)
    rows = pg.fetch_all(
        "select id,xmin::text as version,result,checkpoint,error from social.reddit_work_targets where run_id=%s",
        [run_id],
    )
    assert len(rows) == 2
    assert all(row["result"] == {} and row["checkpoint"] == {} and row["error"] is None for row in rows)
    with pg.transaction() as conn:
        workflows._redact_snapshots("post1", conn)
    after = pg.fetch_all("select id,xmin::text as version from social.reddit_work_targets where run_id=%s", [run_id])
    assert {row["id"]: row["version"] for row in after} == {row["id"]: row["version"] for row in rows}


@pytest.mark.parametrize("signal", ["deleted", "removed"])
def test_parent_redaction_preserves_public_comments_and_comment_media(database, signal):
    add_post()
    _associate(database, "post1")
    pg.execute("insert into social.reddit_lifecycle_enrollment(reddit_post_id,reason) values('post1','new_capture')")
    for cid in ("public", "gone"):
        pg.execute(
            "insert into social.reddit_comments(reddit_post_id,reddit_comment_id,body,author,raw_payload) "
            "values('post1',%s,'public text','reader',%s::jsonb)",
            [cid, json.dumps({"body": "public text", "author": "reader"})],
        )
    for cid in (None, "public", "gone"):
        key = cid or "post"
        asset = pg.execute_returning(
            "insert into social.reddit_assets(source_url,media_type,state,hosted_key,hosted_url) "
            "values(%s,'image','mirrored',%s,%s) returning id",
            ["https://fixture/" + key, "reddit/" + key, "https://host/" + key],
        )[0]
        pg.execute(
            "insert into social.reddit_asset_links(asset_id,reddit_post_id,reddit_comment_id,association_key) "
            "values(%s,'post1',%s,%s)",
            [asset["id"], cid, key],
        )
        pg.execute(
            "insert into social.reddit_media_mirrors(reddit_post_id,reddit_comment_id,source_url,media_type,"
            "hosted_key,hosted_url,status) values('post1',%s,%s,'image',%s,%s,'mirrored')",
            [cid, "https://fixture/" + key, "reddit/" + key, "https://host/" + key],
        )
    with pg.transaction() as conn:
        workflows._redact_source("post1", None, signal, conn)
    public = pg.fetch_one("select * from social.reddit_comments where reddit_comment_id='public'")
    assert public["body"] == "public text" and public["author"] == "reader"
    assert public["raw_payload"] == {"body": "public text", "author": "reader"}
    assert pg.fetch_all("select * from social.reddit_comment_tombstones") == []
    assert {r["reddit_comment_id"] for r in pg.fetch_all("select * from social.reddit_asset_links")} == {
        "public",
        "gone",
    }
    deleted = []
    workflows.drain_object_deletions(object_deleter=deleted.append)
    assert deleted == ["reddit/post"]
    assert pg.fetch_one(
        "select status,hosted_key from social.reddit_media_mirrors where reddit_comment_id='public'"
    ) == {"status": "mirrored", "hosted_key": "reddit/public"}
    pg.execute("update social.reddit_lifecycle_enrollment set state=%s where reddit_post_id='post1'", [signal])
    row = {"reddit_post_id": "post1", "reddit_comment_id": "public", "body": "public text"}
    assert workflows.filter_comment_tombstones([row]) == [row]
    # A later explicit comment deletion still acts on that comment under a deleted parent.
    job = workflows.enqueue_job(
        community_id=database.community, season_id=database.season, kind="lifecycle", payload={"post_ids": ["post1"]}
    )
    result = workflows.execute_job(
        str(job["id"]),
        lifecycle_provider=lambda ids: [
            {
                "reddit_post_id": ids[0],
                "reddit_comment_id": "gone",
                "source": "reddit",
                "explicit": True,
                "signal": "deleted",
                "checked": True,
            }
        ],
        object_deleter=deleted.append,
    )
    assert result["checked"] == result["enrolled"] == 1
    assert pg.fetch_one("select body from social.reddit_comments where reddit_comment_id='gone'")["body"] == "[deleted]"
    assert workflows.filter_comment_tombstones([row]) == [row]
    assert (
        pg.fetch_one("select body from social.reddit_comments where reddit_comment_id='public'")["body"]
        == "public text"
    )
    assert set(deleted) == {"reddit/post", "reddit/gone"}
    assert pg.fetch_one("select state,next_check_at>now() as rescheduled from social.reddit_lifecycle_enrollment")[
        "rescheduled"
    ]


@pytest.mark.parametrize("parent_state", ["deleted", "removed"])
def test_media_writer_accepts_public_reply_under_deleted_parent(database, monkeypatch, parent_state):
    from trr_backend.media import s3_mirror
    from trr_backend.repositories import reddit_execution

    post = add_post()
    _associate(database, "post1")
    pg.execute(
        "insert into social.reddit_lifecycle_enrollment(reddit_post_id,reason,state) values('post1','new_capture',%s)",
        [parent_state],
    )
    for cid in ("public", "gone"):
        pg.execute(
            "insert into social.reddit_comments(reddit_post_id,reddit_comment_id,body) values('post1',%s,'text')", [cid]
        )
    pg.execute("insert into social.reddit_comment_tombstones(id,signal,evidence) values('gone','deleted','{}')")
    refs = [
        {"source_url": "https://fixture/" + (cid or "post") + ".jpg", "media_type": "image", "reddit_comment_id": cid}
        for cid in (None, "public", "gone")
    ]
    monkeypatch.setattr(reddit_execution, "media_references", lambda *a: refs)
    monkeypatch.setattr(
        s3_mirror,
        "mirror_url_to_s3",
        lambda url, **kw: SimpleNamespace(
            status="mirrored",
            hosted_key="reddit/" + url.rsplit("/", 1)[1],
            hosted_url=url,
            sha256="fixture",
            content_type="image/jpeg",
            size_bytes=5,
        ),
    )
    run = owned(database, requested_phases=["media"])
    capture.ensure_target(str(run["id"]), "media", "post1", post_id="post1")
    reddit_execution.media_capture(post, capture.Execution(run))
    links = pg.fetch_all("select reddit_comment_id from social.reddit_asset_links")
    assert links == [{"reddit_comment_id": "public"}]
    children = [r for r in capture.list_targets(str(run["id"])) if r["target_key"] != "post1"]
    assert sorted(r["state"] for r in children) == ["committed", "source_unavailable", "source_unavailable"]
