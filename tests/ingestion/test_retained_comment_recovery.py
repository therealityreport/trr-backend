"""Real SQL/replay guards on isolated PostgreSQL; no source or configured services."""

from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_comments as fixtures
from tests.ingestion import test_public_comment_transport as public
from trr_backend.ingestion.comments import COMMENT_PARSER_VERSION
from trr_backend.ingestion.domain import Conflict, Stage, fingerprint, stable_json
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.retained_comment_recovery import audit_and_replay_retained

source_database = fixtures.database
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def database(source_database):
    store, tx = source_database
    with tx() as conn, conn.cursor() as cur:
        cur.execute((ROOT / "supabase/migrations/20261016070300_instagram_ingestion_replay_receipts.sql").read_text())
    return store, tx


@pytest.fixture(autouse=True)
def deny_source_requests(monkeypatch):
    import httpx
    import requests

    def forbidden(*args, **kwargs):
        raise AssertionError("retained_recovery_source_request_forbidden")

    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)
    monkeypatch.setattr(httpx.Client, "send", forbidden)
    monkeypatch.setattr(httpx.AsyncClient, "send", forbidden)


def node(identity, *, parent=None, media=None):
    result = {
        "pk": identity,
        "text": "retained " + identity,
        "user": {"pk": "100", "username": "fixture"},
        "child_comment_count": 0,
    }
    if parent:
        result["parent_comment_id"] = parent
    if media:
        result["giphy_media_info"] = {"id": "native-gif", "images": {"fixed_height": {"url": media}}}
    return result


def retained(database, *, pages=None):
    store, tx, args = public.setup(database, bounded=True, seed_missing=True)
    store.acquire(
        args["run_id"],
        args["account"],
        generation=args["generation"],
        stage=Stage.COMMENTS,
        target_key=args["target_key"],
        owner_token=args["owner_token"],
    )
    pages = pages or [[node("recovered-" + uuid4().hex)]]
    evidence_ids, natives = [], []
    for index, nodes in enumerate(pages):
        payload = {
            "comments": nodes,
            "comment_count": len(nodes),
            "has_more_comments": False,
            "has_more_headload_comments": False,
        }
        evidence = Evidence.capture(
            source_identity=f"instagram:comments:{args['target_key']}:comments_api",
            captured_at=public.OBSERVED + timedelta(seconds=index),
            parser_version=COMMENT_PARSER_VERSION,
            payload=payload,
        )
        identity = store.retain_evidence(
            args["run_id"],
            "bravotv",
            generation=1,
            stage=Stage.COMMENTS,
            target_key=args["target_key"],
            owner_token=args["owner_token"],
            evidence=evidence,
        )
        evidence_ids.append(identity)
        natives.extend(str(value["pk"]) for value in nodes)
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into social.instagram_ingestion_comment_pages
                (page_id,run_id,target_key,owner_generation,evidence_id,observed_at,content_hash,
                 exhausted,canonical_identity,checkpoint) values(%s,%s,%s,1,%s,%s,%s,true,'{}'::jsonb,%s::jsonb)""",
                (
                    str(uuid4()),
                    args["run_id"],
                    args["target_key"],
                    identity,
                    evidence.captured_at,
                    evidence.content_hash,
                    stable_json({"not_a_stream_input": "large" * 10000}),
                ),
            )
    checkpoint = {
        "comment_pages": {
            "adapter_family": "instagram_public_relay",
            "adapter_version": 1,
            "scan_root_evidence_id": evidence_ids[0],
        }
    }
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_ingestion_targets set checkpoint=%s::jsonb,outcome='committed',
            owner_expires_at=null,observed_at=%s,committed_at=clock_timestamp()
            where run_id=%s and stage='comments' and target_key=%s""",
            (stable_json(checkpoint), public.OBSERVED, args["run_id"], args["target_key"]),
        )
    request = {key: args[key] for key in ("run_id", "account", "generation", "target_key")}
    request["expected_checkpoint_hash"] = fingerprint(checkpoint)
    return store, tx, args, request, evidence_ids, natives, checkpoint


def query(tx, sql, args=()):
    return fixtures.query(tx, sql, args)


def test_omitted_native_id_recovered_idempotently_and_old_rows_preserved(database):
    store, tx, args, request, evidence_ids, natives, checkpoint = retained(database)
    before = query(
        tx,
        "select evidence_id,content_hash,payload from social.instagram_ingestion_evidence where run_id=%s",
        (args["run_id"],),
    )
    result = audit_and_replay_retained(store, **request)
    assert result["status"] == "recovered" and result["stream_complete"]
    assert result["recovered_native_ids"] == natives and result["source_requests"] == 0
    assert result["pages"][0]["missing_native_ids"] == natives
    assert result["pages"][0]["replay_receipt_id"]
    assert result["pages"][0]["evidence_id"] == evidence_ids[0]
    second = audit_and_replay_retained(store, **request)
    assert second["status"] == "matched" and second["replayed_pages"] == 0
    assert query(
        tx, "select count(*) as n from social.instagram_ingestion_replay_receipts where run_id=%s", (args["run_id"],)
    ) == [{"n": 1}]
    saved = query(
        tx, "select comment_id,is_missing from social.instagram_comments where post_id=%s", (args["post_id"],)
    )
    assert {row["comment_id"] for row in saved} == {"old", *natives}
    assert all(not row["is_missing"] for row in saved)
    assert query(
        tx,
        "select checkpoint from social.instagram_ingestion_targets where run_id=%s and stage='comments'",
        (args["run_id"],),
    ) == [{"checkpoint": checkpoint}]
    assert (
        query(
            tx,
            "select evidence_id,content_hash,payload from social.instagram_ingestion_evidence where run_id=%s",
            (args["run_id"],),
        )
        == before
    )
    audits = query(
        tx,
        """select result from social.instagram_ingestion_commands
        where run_id=%s and idempotency_key like 'retained-comment-audit-v1:%%'""",
        (args["run_id"],),
    )
    assert audits and all("checkpoint" not in row["result"] and "payload" not in row["result"] for row in audits)


@pytest.mark.parametrize("change", ["active", "checkpoint", "generation", "authenticated"])
def test_active_owner_stale_checkpoint_and_run_are_rejected(database, change):
    store, tx, args, request, _, _, _ = retained(database)
    if change == "active":
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """update social.instagram_ingestion_targets set outcome='unresolved',
                owner_expires_at=clock_timestamp()+interval '1 hour' where run_id=%s and stage='comments'""",
                (args["run_id"],),
            )
    elif change == "checkpoint":
        request["expected_checkpoint_hash"] = "0" * 64
    elif change == "generation":
        request["generation"] = 2
    else:
        from dataclasses import replace

        from trr_backend.ingestion.domain import RunIntent, SourceProfile

        current = query(tx, "select intent from social.instagram_ingestion_runs where run_id=%s", (args["run_id"],))[0]
        intent = replace(
            RunIntent.from_payload(current["intent"]), source_profile=SourceProfile.for_mode("authenticated")
        )
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "update social.instagram_ingestion_runs set intent=%s::jsonb,intent_hash=%s where run_id=%s",
                (stable_json(intent.payload()), intent.digest, args["run_id"]),
            )
    with pytest.raises(Conflict):
        audit_and_replay_retained(store, **request)
    assert query(
        tx, "select count(*) as n from social.instagram_ingestion_replay_receipts where run_id=%s", (args["run_id"],)
    ) == [{"n": 0}]


def test_older_source_cannot_overwrite_newer_canonical_observation(database):
    identity = "newer-" + uuid4().hex
    source_url = "https://media.example/source.gif"
    store, tx, args, request, _, _, _ = retained(database, pages=[[node(identity, media=source_url)]])
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at,media_urls)
            values(%s,%s,'fixture','keep newer text',%s,'[]'::jsonb)""",
            (args["post_id"], identity, public.OBSERVED + timedelta(hours=1)),
        )
    result = audit_and_replay_retained(store, **request)
    assert result["status"] == "needs_fresh_source" and result["replayed_pages"] == 0
    assert result["unresolved_discrepancies"][0]["missing_attachments"] == [
        {"comment_id": identity, "missing_urls": [source_url]}
    ]
    assert result["unresolved_discrepancies"][0]["reason"] == "canonical_replay_guard_rejected_retained_page"
    assert query(
        tx,
        "select text,media_urls from social.instagram_comments where post_id=%s and comment_id=%s",
        (args["post_id"], identity),
    ) == [{"text": "keep newer text", "media_urls": []}]
    assert query(
        tx, "select count(*) as n from social.instagram_ingestion_replay_receipts where run_id=%s", (args["run_id"],)
    ) == [{"n": 0}]


def test_attachment_recovered_but_existing_different_attachment_is_preserved(database):
    for preserve in (False, True):
        identity = "attachment-" + uuid4().hex
        source_url = "https://media.example/native.gif"
        store, tx, args, request, _, _, _ = retained(database, pages=[[node(identity, media=source_url)]])
        existing = ["https://media.example/preserve.gif"] if preserve else []
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into social.instagram_comments
                (post_id,comment_id,username,text,scraped_at,last_seen_at,media_urls)
                values(%s,%s,'fixture','canonical text',%s,%s,%s::jsonb)""",
                (args["post_id"], identity, public.OBSERVED, public.OBSERVED, stable_json(existing)),
            )
        result = audit_and_replay_retained(store, **request)
        if preserve:
            assert result["status"] == "needs_fresh_source"
            assert result["unresolved_discrepancies"][0]["reason"] == "canonical_attachment_preservation_required"
        else:
            assert result["status"] == "recovered" and result["replayed_pages"] == 1
        assert query(
            tx,
            "select media_urls from social.instagram_comments where post_id=%s and comment_id=%s",
            (args["post_id"], identity),
        ) == [{"media_urls": existing if preserve else [source_url]}]


def test_all_roots_reparsed_without_advancing_current_scan_coverage(database):
    old_id, current_id = "old-scan-" + uuid4().hex, "current-scan-" + uuid4().hex
    store, tx, args, request, evidence_ids, _, checkpoint = retained(
        database, pages=[[node(old_id)], [node(current_id)]]
    )
    checkpoint["comment_pages"]["scan_root_evidence_id"] = evidence_ids[1]
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set checkpoint=%s::jsonb where run_id=%s and stage='comments'",
            (stable_json(checkpoint), args["run_id"]),
        )
    request["expected_checkpoint_hash"] = fingerprint(checkpoint)
    result = audit_and_replay_retained(store, **request)
    assert result["pages_audited"] == result["unique_native_ids"] == 2
    assert set(result["recovered_native_ids"]) == {old_id, current_id}
    assert result["historical_pages_audited"] == result["current_scan_pages_audited"] == 1
    assert result["current_scan_unique_native_ids"] == 1
    assert result["current_checkpoint_coverage_changed"] is False
    assert [page["observation_scope"] for page in result["pages"]] == ["historical", "current_scan"]
    assert query(
        tx,
        "select checkpoint from social.instagram_ingestion_targets where run_id=%s and stage='comments'",
        (args["run_id"],),
    ) == [{"checkpoint": checkpoint}]
    assert query(
        tx,
        "select comment_id from social.instagram_comments where post_id=%s and comment_id=%s",
        (args["post_id"], old_id),
    ) == [{"comment_id": old_id}]


def test_stream_never_selects_page_checkpoints_and_no_matching_page_roundtrips(database, monkeypatch):
    from trr_backend.ingestion import retained_comment_recovery as module

    assert "p.checkpoint" not in module._STREAM and "p.*" not in module._STREAM
    # Keep this small fixture crossing several batches with the production default at 128.
    monkeypatch.setattr(module, "STREAM_BATCH_PAGES", 4)
    identities = ["stream-" + uuid4().hex for _ in range(12)]
    store, tx, args, request, _, _, _ = retained(database, pages=[[node(identity)] for identity in identities])
    # One first pass recovers missing rows. The second pass must stream all pages
    # without replaying or issuing one canonical lookup per matched page.
    assert audit_and_replay_retained(store, **request)["status"] == "recovered"
    queries = []

    class CountingCursor(RealDictCursor):
        def execute(self, sql, args=None):  # pyright: ignore[reportIncompatibleMethodOverride]
            queries.append(str(sql))
            return super().execute(sql, args)

    from contextlib import contextmanager

    class CountingConnection:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def cursor(self, *args, **kwargs):
            kwargs["cursor_factory"] = CountingCursor
            return self.connection.cursor(*args, **kwargs)

    @contextmanager
    def counted_tx():
        with tx() as conn:
            yield CountingConnection(conn)

    from trr_backend.ingestion.persistence import Store

    counted = Store(counted_tx)
    result = audit_and_replay_retained(counted, **request)
    assert result["status"] == "matched" and result["pages_audited"] == 12 and result["replayed_pages"] == 0
    assert queries and any("select p.page_id" in sql for sql in queries)
    # One canonical load and one bounded full-row digest, independent of page count.
    assert sum("from social.instagram_comments c" in sql for sql in queries) == 2
    assert all("page.checkpoint" not in sql and "p.checkpoint" not in sql for sql in queries)


def test_savepoint_rolls_back_page_writes_and_replay_receipt_on_conflict(database, monkeypatch):
    from trr_backend.ingestion import retained_comment_recovery as module

    store, tx, args, request, _, natives, _ = retained(database)
    real_replay = module.ReplayCommands

    class ConflictingReplay(real_replay):
        def __call__(self, *args, **kwargs):
            super().__call__(*args, **kwargs)
            raise Conflict("fixture conflict after canonical write and replay journal")

    monkeypatch.setattr(module, "ReplayCommands", ConflictingReplay)
    result = audit_and_replay_retained(store, **request)
    assert result["status"] == "needs_fresh_source" and result["replayed_pages"] == 0
    assert result["unresolved_discrepancies"][0]["missing_native_ids"] == natives
    assert (
        query(
            tx,
            "select comment_id from social.instagram_comments where post_id=%s and comment_id=any(%s)",
            (args["post_id"], natives),
        )
        == []
    )
    assert query(
        tx, "select count(*) as n from social.instagram_ingestion_replay_receipts where run_id=%s", (args["run_id"],)
    ) == [{"n": 0}]
    assert query(
        tx,
        """select count(*) as n from social.instagram_ingestion_commands
        where run_id=%s and idempotency_key like 'retained-comment-audit-v1:%%'""",
        (args["run_id"],),
    ) == [{"n": 1}]


def test_unavailable_stream_reports_needs_source_without_success_receipts(database, monkeypatch):
    from trr_backend.ingestion import retained_comment_recovery as module

    store, tx, args, request, _, _, _ = retained(database)
    monkeypatch.setattr(module, "_STREAM", "select definitely_missing_retained_column where %s=%s")
    result = audit_and_replay_retained(store, **request)
    assert result["status"] == "offline_audit_blocked" and not result["stream_complete"]
    assert result["reason"] == "retained_stream_or_transaction_failed"
    assert result["replayed_pages"] == 0 and result["recovered_native_ids"] == []
    assert query(
        tx, "select count(*) as n from social.instagram_ingestion_replay_receipts where run_id=%s", (args["run_id"],)
    ) == [{"n": 0}]


def test_native_parent_links_recovered_through_existing_writer(database):
    parent_id, child_id = "parent-" + uuid4().hex, "child-" + uuid4().hex
    parent = node(parent_id)
    parent.update(child_comment_count=1, preview_child_comments=[node(child_id)])
    store, tx, args, request, _, _, _ = retained(database, pages=[[parent]])
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at,last_seen_at,
            parent_comment_external_id,parent_comment_id) select %s,%s,'fixture','existing child',%s,%s,'old',id
            from social.instagram_comments where post_id=%s and comment_id='old'""",
            (args["post_id"], child_id, public.OBSERVED, public.OBSERVED, args["post_id"]),
        )
    result = audit_and_replay_retained(store, **request)
    assert result["status"] == "recovered"
    assert result["pages"][0]["parent_mismatches"][0]["expected_parent"] == parent_id
    assert query(
        tx,
        """select c.parent_comment_external_id,p.comment_id as parent_native_id
        from social.instagram_comments c join social.instagram_comments p on p.id=c.parent_comment_id
        where c.post_id=%s and c.comment_id=%s""",
        (args["post_id"], child_id),
    ) == [{"parent_comment_external_id": parent_id, "parent_native_id": parent_id}]


def test_active_media_owner_cannot_be_invalidated(database):
    store, tx, args, request, _, _, _ = retained(database)
    owner = str(uuid4())
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "insert into social.instagram_ingestion_manifests(run_id,stage) values(%s,'media')", (args["run_id"],)
        )
        cur.execute(
            """insert into social.instagram_ingestion_targets(run_id,stage,target_key,max_attempts,
            owner_token,owner_generation,owner_expires_at)
            values(%s,'media',%s,3,%s,1,clock_timestamp()+interval '1 hour')""",
            (args["run_id"], args["target_key"], owner),
        )
    with pytest.raises(Conflict, match="active media owner"):
        audit_and_replay_retained(store, **request)
    saved = query(
        tx,
        "select owner_token from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (args["run_id"],),
    )
    assert str(saved[0]["owner_token"]) == owner


@pytest.mark.parametrize("kind", ["missing", "missing_unknown", "deleted"])
def test_earlier_observation_never_resurrects_newer_or_unknown_tombstone(database, kind):
    identity, current_id = "tombstone-" + uuid4().hex, "current-" + uuid4().hex
    store, tx, args, request, evidence_ids, _, checkpoint = retained(
        database, pages=[[node(identity)], [node(current_id)]]
    )
    checkpoint["comment_pages"]["scan_root_evidence_id"] = evidence_ids[1]
    marked = public.OBSERVED + timedelta(hours=1)
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_targets set checkpoint=%s::jsonb where run_id=%s and stage='comments'",
            (stable_json(checkpoint), args["run_id"]),
        )
        cur.execute(
            """insert into social.instagram_comments(post_id,comment_id,username,text,scraped_at,last_seen_at,
            is_missing,missing_at,deleted_at) values(%s,%s,'fixture','preserve tombstone',%s,%s,%s,%s,%s)""",
            (
                args["post_id"],
                identity,
                public.OBSERVED,
                public.OBSERVED,
                kind != "deleted",
                marked if kind == "missing" else None,
                marked if kind == "deleted" else None,
            ),
        )
    request["expected_checkpoint_hash"] = fingerprint(checkpoint)
    result = audit_and_replay_retained(store, **request)
    historical = result["pages"][0]
    assert historical["observation_scope"] == "historical"
    assert historical["reason"] == "newer_or_unknown_canonical_tombstone_preserved"
    assert historical["tombstone_conflicts"][0]["comment_id"] == identity
    row = query(
        tx,
        "select is_missing,missing_at,deleted_at,scraped_at,last_seen_at "
        "from social.instagram_comments where post_id=%s and comment_id=%s",
        (args["post_id"], identity),
    )[0]
    assert row["scraped_at"] == row["last_seen_at"] == public.OBSERVED
    assert row["is_missing"] == (kind != "deleted")
    assert row["deleted_at"] == (marked if kind == "deleted" else None)
    assert row["missing_at"] == (marked if kind == "missing" else None)
    summary = query(
        tx,
        "select result from social.instagram_ingestion_commands where account='bravotv' and idempotency_key=%s",
        (result["summary_key"],),
    )[0]["result"]
    assert summary["stream_complete"] and summary["status"] == "needs_fresh_source"
    assert summary["current_checkpoint_coverage_changed"] is False
