"""Frozen-source, private-PostgreSQL full ingestion resource characterization.

External source/storage/Modal boundaries are fixture-backed; application pools,
SQL and the selected orchestration paths remain real. Never load caller .env.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import io
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid5

import psycopg2

FROZEN_COMMIT = "17f978d101a26634c96ac62f6f352f8ae9bc7487"
NAMESPACE = UUID("6f437318-f439-48d7-917c-146c8a1d7f0e")
ACCOUNT = "resource_fixture"
CODES = tuple(f"Resource{i:03}" for i in range(12))
SHOW_ID = str(uuid5(NAMESPACE, "show"))
SEASON_ID = str(uuid5(NAMESPACE, "season"))
OBSERVED = datetime(2026, 10, 15, 12, tzinfo=UTC)
CDN = "https://scontent.cdninstagram.com"
HOSTED = "https://media.example/"


def identity(name):
    return str(uuid5(NAMESPACE, name))


def write_json(path, value):
    path.write_text(json.dumps(value, default=str, indent=2) + "\n")


def child_signature(pid):
    import psutil

    try:
        return psutil.Process(pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def register_child(process):
    from full_pipeline_sources import event

    event("child-processes", {"child_pid": process.pid, "signature": child_signature(process.pid)})


def reap_tracked_children(root):
    """Catch local stand-in workers whose coordinator exited without finally.

    Activity subprocesses intentionally own separate groups. Check both process
    creation identity and group ownership before signaling a task-recorded child.
    """
    journal = root / "child-processes.jsonl"
    records = [json.loads(line) for line in journal.read_text().splitlines()] if journal.exists() else []
    killed, skipped = [], []
    for row in records:
        pid = row["child_pid"]
        current = child_signature(pid)
        if current is None:
            continue
        if row["signature"] is None or current != row["signature"]:
            skipped.append(pid)
            continue
        try:
            if os.getpgid(pid) == pid:
                os.killpg(pid, signal.SIGKILL)
                killed.append(pid)
        except ProcessLookupError:
            pass
    write_json(
        root / "descendant-cleanup.json",
        {"registered": len(records), "killed": killed, "identity_changed_skipped": skipped},
    )
    return killed


def enforce_source(source):
    """Check the full exported file set, including unexpected regular files."""
    manifest_path = source.parent / "source-full-manifest.json"
    if not manifest_path.exists():
        manifest_path = source.parent / "source-manifest.json"
    manifest = json.loads(manifest_path.read_text())
    expected = manifest["files"]
    assert len(expected) >= 2400, "Full source manifest required"
    actual = {
        str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    assert actual == expected, "Frozen source manifest mismatch"
    return {
        "commit": manifest["commit"],
        "file_count": len(actual),
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
    }


def harness_hashes():
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (Path(__file__).resolve(), Path(__file__).with_name("full_pipeline_sources.py"))
    }


def pool_receipt():
    from trr_backend.db import pg

    pools = {
        name: pg._configured_pool_snapshot(name)
        for name in ("default", "social_control", "social_progress", "social_profile")
    }
    expected_default = 2 if os.getenv("EVAL_POOL_PROFILE") == "declared-modal" else 1
    assert pools["default"]["configured_maxconn"] == expected_default
    assert all(
        pools[name]["configured_maxconn"] == 1 for name in ("social_control", "social_progress", "social_profile")
    )
    if os.getenv("EVAL_POOL_PROFILE") == "declared-modal":
        assert pg._close_pool_connection_after_return() and pg._is_modal_container_runtime()
        assert os.getenv("TRR_LOCAL_DEV") == "1"
    return {
        "pools": pools,
        "close_after_return": pg._close_pool_connection_after_return(),
        "modal_detected": pg._is_modal_container_runtime(),
        "local_dev_enabled": os.getenv("TRR_LOCAL_DEV") == "1",
        "route": "direct-loopback-session",
        "live_pooler_verified": False,
    }


def _pinned_ingestion_username():
    username = (os.environ.get("TRR_INSTAGRAM_INGESTION_AUTH_USERNAME") or "").strip()
    if not username:
        raise SystemExit("Set TRR_INSTAGRAM_INGESTION_AUTH_USERNAME to the pinned ingestion username.")
    return username


def private_environment(dsn, source, root, profile):
    """Allowlist environment: no caller provider credentials/configuration."""
    env = {
        "PATH": os.defpath + ":/opt/homebrew/bin",
        "HOME": str(root / "home"),
        "TMPDIR": str(root / "tmp"),
        "PYTHONPATH": str(source),
        "PYTHONUNBUFFERED": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TRR_DB_URL": dsn,
        "TRR_DB_DIRECT_URL": dsn,
        "TRR_DB_SESSION_URL": dsn,
        "TRR_DB_POOL_MINCONN": "1",
        "TRR_DB_POOL_MAXCONN": "2" if profile == "declared-modal" else "1",
        "TRR_SOCIAL_CONTROL_DB_POOL_MINCONN": "1",
        "TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN": "1",
        "TRR_SOCIAL_PROGRESS_DB_POOL_MINCONN": "1",
        "TRR_SOCIAL_PROGRESS_DB_POOL_MAXCONN": "1",
        "TRR_SOCIAL_PROFILE_DB_POOL_MINCONN": "1",
        "TRR_SOCIAL_PROFILE_DB_POOL_MAXCONN": "1",
        "TRR_DB_POOL_CLOSE_AFTER_RETURN": "1" if profile == "declared-modal" else "0",
        "TRR_LOCAL_DEV": "1",
        "MODAL_TASK_ID": "local-fixture-only",
        "SOCIAL_QUEUE_ENABLED": "0",
        "TRR_INSTAGRAM_INGESTION_COORDINATOR_ENABLED": "1",
        "TRR_RUNTIME_CAPACITY_CONTEXT": "hosted_modal",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_ACCESS_KEY_ID": "fixture-local-only",
        "AWS_SECRET_ACCESS_KEY": "fixture-local-only",
        "AWS_DEFAULT_REGION": "us-east-1",
        "SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY": "1",
        "SOCIAL_INSTAGRAM_COMMENTS_LOAD_STRATEGY": "cursor_api",
        "SOCIAL_INSTAGRAM_COMMENTS_REQUEST_DELAY_SEC": "0",
        "SOCIAL_INSTAGRAM_MEDIA_MIRROR_COMMENT_AVATARS": "1",
        "SOCIAL_INSTAGRAM_DELAY_SEC": "0",
        # Native explicit-only credential snapshot; fixed synthetic values,
        # never inherited operator cookies or browser/profile resolution.
        "SOCIAL_INSTAGRAM_COOKIES_JSON": json.dumps(
            {"sessionid": "synthetic-fixture", "ds_user_id": "100", "csrftoken": "fixture"}
        ),
        "TRR_INSTAGRAM_INGESTION_AUTH_USER_ID": "100",
        # The pinned name is not in the repo; take it from the operator's environment.
        "TRR_INSTAGRAM_INGESTION_AUTH_USERNAME": _pinned_ingestion_username(),
        "EVAL_SOURCE_ROOT": str(source),
        "EVAL_RUNTIME_ROOT": str(root),
        "EVAL_DATABASE_URL": dsn,
        "EVAL_STABLE_SOURCE": "1",
        "EVAL_POOL_PROFILE": profile,
    }
    for name in ("home", "tmp"):
        (root / name).mkdir(exist_ok=True)
    return env


def deny_external_network():
    original = socket.socket.connect

    def guarded(sock, address):
        if not isinstance(address, tuple) or address[0] not in {"127.0.0.1", "localhost", "::1"}:
            raise RuntimeError("Evaluation denies non-loopback network")
        return original(sock, address)

    socket.socket.connect = guarded


@contextmanager
def admin(dsn):
    conn = psycopg2.connect(dsn, application_name="full-eval-observer")
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def prepare_schema(dsn, source):
    with admin(dsn) as conn, conn.cursor() as cur:
        cur.execute((source / "tests/fixtures/ingestion/canonical_schema.sql").read_text())
        cur.execute("CREATE ROLE anon;CREATE ROLE authenticated;CREATE ROLE service_role;")
        cur.execute("""ALTER TABLE core.shows ADD COLUMN name text,ADD COLUMN slug text;
            ALTER TABLE social.scrape_runs ALTER COLUMN id SET DEFAULT gen_random_uuid();
            ALTER TABLE social.scrape_jobs ALTER COLUMN id SET DEFAULT gen_random_uuid();
            ALTER TABLE core.seasons ADD COLUMN show_id uuid,ADD COLUMN season_number int,
                ADD COLUMN title text,ADD COLUMN premiere_date date,ADD COLUMN air_date date;
            CREATE TABLE core.episodes(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),season_id uuid,
                episode_number integer,air_date date);
            ALTER TABLE social.scrape_runs ADD COLUMN status text DEFAULT 'running',ADD COLUMN source_scope text,
                ADD COLUMN initiated_by text,ADD COLUMN config jsonb DEFAULT '{}',ADD COLUMN cancelled_at timestamptz,
                ADD COLUMN created_at timestamptz DEFAULT now(),ADD COLUMN updated_at timestamptz DEFAULT now(),
                ADD COLUMN completed_at timestamptz,ADD COLUMN metadata jsonb DEFAULT '{}',ADD COLUMN platform text,
                ADD COLUMN total_jobs int DEFAULT 0,ADD COLUMN completed_jobs int DEFAULT 0,
                ADD COLUMN failed_jobs int DEFAULT 0,ADD COLUMN active_jobs int DEFAULT 0,
                ADD COLUMN items_found_total int DEFAULT 0,ADD COLUMN stage_counts jsonb DEFAULT '{}',
                ADD COLUMN summary jsonb DEFAULT '{}',ADD COLUMN started_at timestamptz,
                ADD COLUMN show_id uuid,ADD COLUMN season_id uuid;
            ALTER TABLE social.scrape_jobs ADD COLUMN run_id uuid,ADD COLUMN status text DEFAULT 'running',
                ADD COLUMN worker_id text,ADD COLUMN config jsonb DEFAULT '{}',
                ADD COLUMN available_at timestamptz DEFAULT now(),
                ADD COLUMN heartbeat_at timestamptz,ADD COLUMN claimed_at timestamptz,
                ADD COLUMN platform text DEFAULT 'instagram',
                ADD COLUMN job_type text,ADD COLUMN metadata jsonb DEFAULT '{}',ADD COLUMN attempt_count int DEFAULT 1,
                ADD COLUMN max_attempts int DEFAULT 12,ADD COLUMN items_found int DEFAULT 0,
                ADD COLUMN error_message text,
                ADD COLUMN started_at timestamptz,ADD COLUMN completed_at timestamptz,
                ADD COLUMN created_at timestamptz DEFAULT now(),ADD COLUMN updated_at timestamptz DEFAULT now(),
                ADD COLUMN season_id uuid,ADD COLUMN source_scope text DEFAULT 'network',ADD COLUMN initiated_by text,
                ADD COLUMN last_error_code text,ADD COLUMN last_error_class text,ADD COLUMN last_error_at timestamptz,
                ADD COLUMN priority int DEFAULT 0,ADD COLUMN dedupe_key text,ADD COLUMN source_id text,
                ADD COLUMN show_id uuid;
            CREATE TABLE social.avatar_registry(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),platform text,
                account_handle text,source_url text,content_hash text,hosted_url text,status text,failure_reason text,
                last_checked_at timestamptz,created_at timestamptz DEFAULT now(),updated_at timestamptz,
                UNIQUE(platform,account_handle,source_url));""")
        detail = (source / "supabase/migrations/20260911120000_instagram_detail_run_targets.sql").read_text()
        cur.execute(detail.split("alter table social.social_post_observations")[0] + "commit;")
        cur.execute("""CREATE UNIQUE INDEX social_post_observations_detail_target_idx ON
            social.social_post_observations(detail_run_id,detail_source_id) WHERE detail_run_id IS NOT NULL""")
        for name in (
            "20260911121000_instagram_detail_request_lanes.sql",
            "20261016070000_instagram_ingestion_foundation.sql",
            "20261016070100_instagram_ingestion_workflows.sql",
            "20261016070200_instagram_ingestion_comment_pages.sql",
            "20260503073500_instagram_profile_pagination_state.sql",
        ):
            cur.execute((source / "supabase/migrations" / name).read_text())
        cur.execute("CREATE EXTENSION pg_stat_statements")
        cur.execute(
            (source / "supabase/migrations/20260505173000_instagram_comment_api_phase_metadata.sql").read_text()
        )
        cur.execute((source / "supabase/migrations/20260616143429_add_ig_comment_rate_pace_table.sql").read_text())
        cur.execute((source / "supabase/migrations/20260607185014_instagram_account_auth_cooldown.sql").read_text())
        cur.execute((source / "supabase/migrations/20260710123000_instagram_comments_shared_cooldown.sql").read_text())
        registry = (
            source / "supabase/migrations/0130_social_worker_heartbeat_and_comment_id_guardrails.sql"
        ).read_text()
        cur.execute(registry.split("-- Clean up malformed external IDs")[0] + "commit;")
        cur.execute((source / "supabase/migrations/0171_social_worker_supported_platforms.sql").read_text())
        shared = (source / "supabase/migrations/0179_shared_social_account_ingest.sql").read_text()
        cur.execute(
            shared[
                shared.index("create table if not exists social.shared_account_sources") : shared.index(
                    "create table if not exists social.shared_post_matches"
                )
            ]
        )
        cur.execute("""ALTER TABLE social.shared_account_sources
            DROP CONSTRAINT shared_account_sources_source_scope_check;
            ALTER TABLE social.shared_account_sources ALTER COLUMN source_scope SET DEFAULT 'network';
            ALTER TABLE social.shared_account_sources ADD CONSTRAINT shared_account_sources_source_scope_check_v2
            CHECK(source_scope IS NULL OR source_scope IN ('network','creator','community','news'));
            INSERT INTO social.shared_account_sources(platform,source_scope,account_handle)
            VALUES('instagram','network','resource_fixture');""")
        cur.execute("INSERT INTO core.shows(id,name,slug) VALUES(%s,'Fixture','fixture')", (SHOW_ID,))
        cur.execute(
            "INSERT INTO core.seasons(id,show_id,season_number,premiere_date) VALUES(%s,%s,3,'2026-01-01')",
            (SEASON_ID, SHOW_ID),
        )
        for code in CODES:
            cur.execute(
                """INSERT INTO social.instagram_posts(id,shortcode,username,source_account,show_id,season_id,
                   comments_count,posted_at,scraped_at) VALUES(%s,%s,%s,%s,%s,%s,2,%s,'2026-10-01')""",
                (identity(code), code, ACCOUNT, ACCOUNT, SHOW_ID, SEASON_ID, OBSERVED),
            )


def schema_preflight(dsn, root, binaries):
    # Fail on absent conditional-feature tables, not only on SQL exceptions.
    required = {
        "core.shows": "id name slug",
        "core.seasons": "id show_id season_number air_date premiere_date",
        "core.episodes": "id season_id episode_number air_date",
        "social.scrape_workers": "worker_id stage status run_id current_job_id metadata started_at "
        "last_seen_at updated_at supported_platforms",
        "social.scrape_runs": "id status config summary total_jobs completed_jobs failed_jobs active_jobs "
        "items_found_total stage_counts started_at completed_at cancelled_at",
        "social.scrape_jobs": "id run_id status config worker_id claimed_at heartbeat_at available_at attempt_count "
        "max_attempts detail_dispatch_token detail_dispatch_generation detail_completion_receipt",
        "social.shared_account_sources": "id source_scope account_handle last_scrape_status last_scrape_run_id "
        "last_scrape_job_id metadata",
        "social.instagram_posts": "id shortcode caption likes comments_count media_urls hosted_media_urls "
        "asset_manifest show_id season_id",
        "social.instagram_comments": "id post_id comment_id parent_comment_id text likes is_reply is_missing "
        "media_urls hosted_media_urls phase author_profile_pic_url",
        "social.instagram_account_catalog_posts": "id source_id caption likes comments_count",
        "social.instagram_post_payloads": "post_id raw_data child_posts_data asset_manifest payload_updated_at",
        "social.instagram_account_catalog_post_payloads": "catalog_post_id raw_data child_posts_data "
        "payload_updated_at",
        "social.instagram_account_catalog_post_collaborators": "catalog_post_id collaborator_handle",
        "social.social_posts": "id platform source_id body like_count comment_count",
        "social.social_post_media_assets": "id post_id position source_url hosted_url mirror_status media_payload",
        "social.social_post_observations": "id platform post_id detail_run_id detail_source_id",
        "social.social_post_legacy_refs": "post_id legacy_table legacy_pk",
        "social.social_post_entities": "post_id entity_type entity_key_norm",
        "social.avatar_registry": "id platform account_handle source_url content_hash hosted_url status "
        "last_checked_at",
        "social.instagram_detail_run_targets": "run_id source_id state",
        "social.instagram_detail_request_lanes": "identity_key cooldown_until request_times",
        "social.instagram_profile_pagination_state": "account_handle",
        "social.instagram_post_comments_audit": "post_id shortcode cursor_payload",
        "social.comment_capture_health": "post_id saved_parent_comments saved_child_replies",
        "social.ig_comment_rate_pace": "rate_key last_start cooldown_until",
        "social.account_auth_cooldown": "platform account_handle cooldown_until blocker_kind",
    }
    for suffix in (
        "runs",
        "commands",
        "outbox",
        "manifests",
        "targets",
        "discovery_pages",
        "evidence",
        "receipts",
        "partitions",
        "decisions",
        "dispatches",
        "comment_pages",
    ):
        required["social.instagram_ingestion_" + suffix] = ""
    with admin(dsn) as conn, conn.cursor() as cur:
        cur.execute("""SELECT table_schema||'.'||table_name,column_name,data_type,column_default,is_nullable
            FROM information_schema.columns WHERE table_schema IN ('core','social') ORDER BY 1,ordinal_position""")
        columns = cur.fetchall()
        inventory = {}
        for table, column, *_ in columns:
            inventory.setdefault(table, set()).add(column)
        for table, fields in required.items():
            assert table in inventory and set(fields.split()) <= inventory[table], (table, fields)
        cur.execute("""SELECT n.nspname||'.'||t.relname,c.conname,c.contype,pg_get_constraintdef(c.oid)
            FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname IN ('core','social') ORDER BY 1,2""")
        constraints = cur.fetchall()
        cur.execute(
            "SELECT schemaname||'.'||tablename,indexname,indexdef FROM pg_indexes "
            "WHERE schemaname IN ('core','social') ORDER BY 1,2"
        )
        indexes = cur.fetchall()
        for table, fragment in (
            ("social.instagram_comments", "UNIQUE (post_id, comment_id)"),
            ("social.scrape_workers", "PRIMARY KEY (worker_id)"),
            ("social.social_posts", "UNIQUE (platform, source_id)"),
            ("social.social_post_media_assets", "UNIQUE (platform, post_id, position)"),
            ("social.instagram_ingestion_targets", "PRIMARY KEY (run_id, stage, target_key)"),
        ):
            matches = any(row[0] == table and fragment in row[3].replace('"', "") for row in constraints)
            assert matches, (table, fragment)
        for name in (
            "scrape_workers_status_seen_idx",
            "shared_account_sources_active_idx",
            "instagram_ingestion_targets_ready_idx",
            "instagram_ingestion_partitions_pending_idx",
            "social_post_observations_detail_target_idx",
            "instagram_ingestion_comment_pages_target_idx",
        ):
            assert any(row[1] == name for row in indexes), name
    write_json(
        root / "schema-inventory.json",
        {
            "columns": columns,
            "constraints": constraints,
            "indexes": indexes,
            "conditional_feature_scope": {
                "scrape_workers": "required and registered natively",
                "instagram_profile_pagination_state": "present",
                "instagram_post_comments_audit": "present",
                "avatar_registry": "present",
                "shared_account_run_partitions/frontiers": "excluded: ordinary direct catalog branch",
            },
        },
    )
    dump = subprocess.run(
        [str(binaries / "pg_dump"), dsn, "--schema-only", "--no-owner"], check=True, capture_output=True
    )
    (root / "schema.sql").write_bytes(dump.stdout)


def create_legacy_job(dsn, stage, config=None, *, run_id=None, claim=True):
    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as legacy

    context = legacy.get_season_context(SEASON_ID)
    if run_id is None:
        run_id = legacy._create_run(
            context,
            source_scope="network",
            initiated_by="fixture",
            config={
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
                "platforms": ["instagram"],
                "accounts_override": [ACCOUNT],
            },
            status="running",
        )
    values = {
        "account": ACCOUNT,
        "stage": stage,
        "source_scope": "network",
        "season_id": SEASON_ID,
        "target_source_ids": list(CODES),
        "max_comments_per_post": 0,
        "fetch_replies": True,
        "comments_auth_validation_mode": "schema_only",
        "comments_load_strategy": "cursor_api",
        "details_refresh_skip_media_followups": True,
        **(config or {}),
    }
    job_id = legacy._insert_job_with_conflict_target(
        context,
        run_id=run_id,
        platform="instagram",
        source_scope="network",
        job_type={
            "media_mirror": "instagram_media_mirror",
            "comment_media_mirror": "instagram_comment_media_mirror",
        }.get(stage, stage),
        stage=stage,
        config=values,
        status="queued",
        priority=100,
        conflict_target_sql="",
        existing_lookup_sql="SELECT NULL AS id",
        existing_lookup_params=[],
    )
    if claim:
        # The external dispatch is local. Use the maintained native atomic claim;
        # capacity is bounded to one serial fixture worker in both arms.
        return legacy._claim_job_by_id(job_id=job_id, worker_id="fixture-worker")
    return pg.fetch_one("SELECT * FROM social.scrape_jobs WHERE id=%s", [job_id])


def claim_legacy(job, *, token=None):
    from trr_backend.socials import social_season_analytics_impl as legacy

    claimed = legacy._claim_job_by_id(job_id=str(job["id"]), worker_id="fixture-worker", dispatch_token=token)
    assert claimed, job["id"]
    return claimed


def child(args):
    setup_started = time.monotonic()
    source = Path(os.environ["EVAL_SOURCE_ROOT"])
    sys.path.insert(0, str(source))
    deny_external_network()
    # Publish the maintained legacy provider composition before stage runners.
    if args.child.startswith("legacy"):
        from trr_backend.socials import social_season_analytics_impl as legacy  # noqa: F401
    from full_pipeline_sources import install_sources

    raw = sys.stdin.buffer.read(65537) if args.child in {"candidate-worker", "legacy-worker"} else None
    reservation = json.loads(raw) if raw is not None else None
    if reservation:
        os.environ["EVAL_ACTIVITY_ID"] = (
            reservation.get("stage", "legacy")
            + ":"
            + str(reservation.get("target_key", reservation.get("job_id", "unknown")))
        )
    if args.child == "candidate-worker":
        sys.stdin = io.TextIOWrapper(io.BytesIO(raw))
    trace = (
        []
        if args.child in {"bootstrap", "candidate-dbos", "candidate-wrapper-probe"}
        else install_sources(
            legacy_mode=args.child.startswith("legacy"),
            stage=reservation["stage"] if reservation and args.child == "candidate-worker" else "all",
        )
    )
    from trr_backend.db import pg

    root = Path(os.environ["EVAL_RUNTIME_ROOT"])
    write_json(root / ("pool-" + str(os.getpid()) + ".json"), pool_receipt())

    try:
        if args.child == "legacy-comments":
            from trr_backend.socials.instagram.comments_scrapling.job_runner import run_instagram_comments_scrapling_job

            job = create_legacy_job(os.environ["EVAL_DATABASE_URL"], "comments_scrapling")
            result = run_instagram_comments_scrapling_job(job, worker_id="fixture-worker")
            print(
                json.dumps(
                    {"status": result["status"], "items_found": result["items_found"], "trace": trace}, default=str
                ),
                flush=True,
            )
        elif args.child == "legacy-listing":
            from trr_backend.socials.instagram.posts_scrapling.job_runner import run_instagram_posts_scrapling_job

            job = create_legacy_job(
                os.environ["EVAL_DATABASE_URL"],
                "posts_scrapling",
                {"instagram_scrape_mode": "authenticated", "bidirectional_walk_enabled": False},
            )
            result = run_instagram_posts_scrapling_job(job, worker_id="fixture-worker")
            print(
                json.dumps(
                    {"status": result["status"], "items_found": result["items_found"], "trace": trace}, default=str
                ),
                flush=True,
            )
        elif args.child == "legacy-media":
            from full_pipeline_sources import post
            from psycopg2.extras import Json

            from trr_backend.socials import social_season_analytics_impl as legacy

            # Standalone seam viability setup, outside all later measurements.
            with admin(os.environ["EVAL_DATABASE_URL"]) as conn, conn.cursor() as cur:
                for code in CODES:
                    cur.execute(
                        "UPDATE social.instagram_posts SET media_urls=%s,thumbnail_url=%s,raw_data=%s,"
                        "owner_profile_pic_url=%s WHERE shortcode=%s",
                        (
                            Json([CDN + "/" + code + ".png"]),
                            CDN + "/" + code + ".png",
                            Json({"username": ACCOUNT, "owner_detail": post(code)["owner"]}),
                            CDN + "/owner.png",
                            code,
                        ),
                    )
            job = create_legacy_job(os.environ["EVAL_DATABASE_URL"], "media_mirror")
            context = legacy.get_season_context(SEASON_ID)
            outputs = [
                legacy._run_platform_media_mirror_stage(
                    context=context,
                    platform="instagram",
                    job_id=job["id"],
                    config={"post_id": identity(code), "week_index": 41},
                )
                for code in CODES
            ]
            print(json.dumps({"results": outputs, "trace": trace}, default=str), flush=True)
        elif args.child == "legacy-full":
            result = legacy_full(os.environ["EVAL_DATABASE_URL"])
            print(json.dumps({"result": result, "trace": trace}, default=str), flush=True)
        elif args.child == "legacy-wrapper-probe":
            result = legacy_full(os.environ["EVAL_DATABASE_URL"], probe_only=True)
            print(json.dumps(result, default=str), flush=True)
        elif args.child == "legacy-worker":
            from trr_backend.socials.control_plane import claim_and_process_social_job

            worker_id = "modal:fixture:" + str(os.getpid())
            assert legacy._worker_heartbeat_schema_ready()
            legacy.update_worker_heartbeat(
                worker_id,
                stage=reservation["stage"],
                status="starting",
                metadata={"source": "local-resource-fixture"},
                supported_platforms=["instagram"],
            )
            try:
                result = claim_and_process_social_job(
                    job_id=reservation["job_id"], worker_id=worker_id, dispatch_token=reservation.get("dispatch_token")
                )
                assert result["claimed"] and result["job"]["status"] == "completed", result
                print(json.dumps({"outcome": "committed", "stage": reservation["stage"]}), flush=True)
            finally:
                legacy.update_worker_heartbeat(
                    worker_id,
                    stage=reservation["stage"],
                    status="stopped",
                    metadata={"source": "local-resource-fixture"},
                    supported_platforms=["instagram"],
                )
        elif args.child == "candidate-direct":
            result = candidate_direct()
            print(json.dumps({"result": result, "trace": trace}, default=str), flush=True)
        elif args.child == "candidate-dbos":
            candidate_direct(use_dbos=True)
        elif args.child == "candidate-wrapper-probe":
            candidate_direct(probe_only=True)
        elif args.child == "candidate-worker":
            from trr_backend.ingestion import worker

            activity_started = time.monotonic()
            result = worker.main()
            write_json(
                root / ("worker-timing-" + str(os.getpid()) + ".json"),
                {
                    "stage": reservation["stage"],
                    "fixture_and_import_seconds": activity_started - setup_started,
                    "activity_seconds": time.monotonic() - activity_started,
                    "returncode": result,
                },
            )
            if result:
                raise SystemExit(result)
        elif args.child == "bootstrap":
            from dbos import DBOS

            DBOS.migrate(os.environ["EVAL_DATABASE_URL"], application_role="eval_runtime")
        else:
            raise ValueError("Unknown child phase")
    finally:
        pg.close_pool()


def candidate_direct(*, use_dbos=False, probe_only=False):
    from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
    from trr_backend.ingestion.admission import Admission
    from trr_backend.ingestion.command_service import CommandService
    from trr_backend.ingestion.queries import Queries
    from trr_backend.ingestion.workflow_store import WorkflowStore
    from trr_backend.ingestion.workflows import DBOSAdapter, RuntimeConfig, Scheduler

    store = WorkflowStore()
    admission = Admission()
    api = CommandService(
        store,
        Queries(),
        engine="dbos",
        workflow_version="full-resource-v1",
        enabled=True,
        capacity=admission.validate_launch,
    )
    launched = api.launch(
        ACCOUNT,
        InstagramIngestionLaunchV2(force_catalog_rediscovery=True, detail_worker_count=1, comments_worker_count=1),
        idempotency_key="full-resource-start",
        initiated_by="fixture",
    )
    from trr_backend.ingestion import runtime

    # Keep the native bounded activity wrapper. Only its executable transport is
    # redirected so the child can install synthetic I/O before native main().
    real_popen = subprocess.Popen

    def fixture_popen(command, *args, **kwargs):
        if command == [sys.executable, "-m", "trr_backend.ingestion.worker"]:
            command = [sys.executable, str(Path(__file__).resolve()), "--child", "candidate-worker"]
            process = real_popen(command, *args, **kwargs)
            register_child(process)
            return process
        return real_popen(command, *args, **kwargs)

    runtime.subprocess.Popen = fixture_popen
    completions = []

    def dispatch(reservation):
        dispatched_at = time.monotonic()
        outcome = runtime.run_ingestion_activity(reservation)
        from full_pipeline_sources import event

        event(
            "dispatch-timings",
            {"stage": reservation["stage"], "seconds": time.monotonic() - dispatched_at, "outcome": outcome},
        )
        completions.append(outcome)
        assert outcome["outcome"] == "committed", outcome
        return "local:" + reservation["owner_token"]

    scheduler = Scheduler(store, admit=admission.admit, dispatch=dispatch, reconcile=lambda _: "unknown")
    if use_dbos:
        root = Path(os.environ["EVAL_RUNTIME_ROOT"])
        (root / "candidate-run.json").write_text(json.dumps(launched.model_dump(mode="json")))
        runtime_dsn = os.environ["EVAL_RUNTIME_URL"]
        adapter = DBOSAdapter(
            scheduler, RuntimeConfig(runtime_dsn, "full-resource-v1", runtime_role="eval_runtime", lifetime_seconds=90)
        )
        # The native runtime consumes CommandService's business outbox itself.
        adapter.run_bounded(lambda: psycopg2.connect(runtime_dsn))
        raise AssertionError("Dedicated runtime must terminate its process")
    item = store.seed(dict(launched.model_dump(mode="json"), account=ACCOUNT))
    for ordinal in range(150):
        tick = scheduler.tick(item, ordinal)
        if probe_only:
            assert len(completions) == 1 and completions[0]["outcome"] == "committed"
            return {"probe": "native-bounded-activity", "outcome": completions[0]}
        if tick.get("stop"):
            assert tick["stop"] == "completed", tick
            store.complete_partition(item, continue_run=False, stop_state="completed")
            break
        assert not tick.get("sleep"), tick
    else:
        raise AssertionError("Candidate did not complete bounded fixture")
    progress = api.progress(ACCOUNT, launched.run_id)
    assert progress.complete, progress
    return {"progress": progress.model_dump(mode="json"), "completions": completions}


def legacy_full(dsn, *, probe_only=False):
    """One explicit serial scheduling owner; real native worker per local child.

    Native automatic media followups are disabled through maintained task flags.
    SOCIAL_QUEUE_ENABLED=0 prevents a second dispatcher owning these local jobs;
    native claim/progress/finish/worker-registry SQL still runs inside the child.
    """
    from full_pipeline_sources import event
    from psycopg2.extras import Json

    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as legacy
    from trr_backend.socials.control_plane import instagram_detail_targets as targets

    listing_job = create_legacy_job(
        dsn,
        "shared_account_posts",
        {
            "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            "instagram_scrape_mode": "authenticated",
            "bidirectional_walk_enabled": False,
            "ingest_mode": "posts_only",
            "runner_strategy": "single_runner",
        },
        claim=False,
    )
    run_id = listing_job["run_id"]
    pending = []
    if not probe_only:
        detail_job = create_legacy_job(
            dsn,
            "shared_account_posts",
            {
                "pipeline_ingest_mode": legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
                "ingest_mode": "details_refresh",
                "details_refresh_force_detail_fetch": True,
            },
            run_id=run_id,
            claim=False,
        )
        pending.append(
            create_legacy_job(
                dsn, "comments_scrapling", {"comments_enable_media_followups": False}, run_id=run_id, claim=False
            )
        )
        for code in CODES:
            pending.append(
                create_legacy_job(dsn, "media_mirror", {"post_id": identity(code)}, run_id=run_id, claim=False)
            )
            pending.append(
                create_legacy_job(
                    dsn,
                    "comment_media_mirror",
                    {"post_id": identity(code), "comment_id": code + "-parent"},
                    run_id=run_id,
                    claim=False,
                )
            )

    def execute(job, token=None):
        label = str(job["config"]["stage"]) + ":" + str(job["id"])
        started = time.monotonic()
        payload = {"job_id": str(job["id"]), "stage": job["config"]["stage"], "dispatch_token": token}
        root = Path(os.environ["EVAL_RUNTIME_ROOT"])
        with (root / ("legacy-worker-" + str(job["id"]) + ".log")).open("w") as log:
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--child", "legacy-worker"],
                stdin=subprocess.PIPE,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            register_child(process)
            try:
                process.communicate(json.dumps(payload).encode(), timeout=50)
                assert process.returncode == 0, "Native legacy worker failed; inspect retained log"
            finally:
                from trr_backend.ingestion.runtime import _reap_process_group

                _reap_process_group(process)
        event(
            "dispatch-timings",
            {
                "stage": payload["stage"],
                "activity": label,
                "seconds": time.monotonic() - started,
                "outcome": "committed",
            },
        )

    execute(listing_job)
    if probe_only:
        return {"probe": "native-legacy-listing", "job_id": str(listing_job["id"])}
    with pg.transaction() as conn, pg.db_cursor(conn=conn) as cur:
        manifest = targets.freeze_manifest(
            run_id, ACCOUNT, {"selected_tasks": ["post_details"], "details_refresh_force_detail_fetch": True}, conn=conn
        )
        config = dict(
            detail_job["config"],
            detail_contract_version=1,
            detail_manifest=manifest,
            details_chunk_targets=50,
            details_refresh_write_batch_size=10,
        )
        cur.execute("UPDATE social.scrape_jobs SET config=%s WHERE id=%s", (Json(config), detail_job["id"]))
    reservation = targets.reserve_dispatch(str(detail_job["id"]))
    assert reservation and targets.bind_dispatch(
        str(detail_job["id"]),
        reservation["token"],
        {"call_id": "local-fixture", "function_name": "local-worker", "dispatched": True},
    )
    execute(detail_job, reservation["token"])
    for job in pending:
        execute(job)
    assert targets.snapshot(run_id)["committed"] == 12
    return {
        "jobs": 27,
        "scheduling_owner": "explicit-local-driver",
        "automatic_followups": False,
        "native_worker_entrypoint": "claim_and_process_social_job",
    }


def legacy_full_v2_diagnostic(dsn):
    from psycopg2.extras import Json

    from trr_backend.socials import social_season_analytics_impl as legacy
    from trr_backend.socials.control_plane import instagram_detail_targets as targets
    from trr_backend.socials.instagram import catalog_ingest
    from trr_backend.socials.instagram.comments_scrapling.job_runner import run_instagram_comments_scrapling_job

    def finish(job, **kwargs):
        @legacy._with_claimed_job_context
        def guarded(bound_job, *, worker_id=None):
            return legacy._finish_job(bound_job["id"], **kwargs)

        assert guarded(job), job["id"]

    listing_job = create_legacy_job(
        dsn, "shared_account_discovery", {"instagram_scrape_mode": "authenticated", "bidirectional_walk_enabled": False}
    )
    run_id = listing_job["run_id"]
    detail_job = create_legacy_job(dsn, "shared_account_posts", run_id=run_id, claim=False)
    comment_job = create_legacy_job(dsn, "comments_scrapling", run_id=run_id, claim=False)
    media_jobs = {
        code: create_legacy_job(dsn, "media_mirror", {"post_id": identity(code)}, run_id=run_id, claim=False)
        for code in CODES
    }
    comment_media_jobs = {
        code: create_legacy_job(
            dsn,
            "comment_media_mirror",
            {"post_id": identity(code), "comment_id": code + "-parent"},
            run_id=run_id,
            claim=False,
        )
        for code in CODES
    }
    listing_config = dict(
        listing_job["config"], pipeline_ingest_mode=legacy.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE
    )
    listed, listing_meta = catalog_ingest._scrape_shared_instagram_posts(
        run_id=listing_job["run_id"], account_handle=ACCOUNT, config=listing_config, job_id=listing_job["id"]
    )
    assert len(listed) == 12, listing_meta
    finish(listing_job, status="completed", items_found=12)
    from trr_backend.db import pg

    with pg.transaction() as conn, pg.db_cursor(conn=conn) as cur:
        manifest = targets.freeze_manifest(
            detail_job["run_id"],
            ACCOUNT,
            {"selected_tasks": ["post_details"], "details_refresh_force_detail_fetch": True},
            conn=conn,
        )
        config = dict(
            detail_job["config"],
            detail_contract_version=1,
            detail_manifest=manifest,
            details_chunk_targets=50,
            details_refresh_write_batch_size=10,
        )
        cur.execute("UPDATE social.scrape_jobs SET config=%s WHERE id=%s", (Json(config), detail_job["id"]))
    reservation = targets.reserve_dispatch(str(detail_job["id"]))
    assert reservation
    assert targets.bind_dispatch(
        str(detail_job["id"]),
        reservation["token"],
        {"call_id": "local-fixture", "function_name": "local-worker", "dispatched": True},
    )
    detail_job = claim_legacy(detail_job, token=reservation["token"])
    detailed = targets.execute_job(
        legacy, detail_job, worker_id="fixture-worker", detail_executor=catalog_ingest.run_durable_instagram_details
    )
    assert detailed["status"] == "completed", detailed
    assert targets.snapshot(run_id)["committed"] == 12
    comment_job = claim_legacy(comment_job)
    commented = run_instagram_comments_scrapling_job(comment_job, worker_id="fixture-worker")
    assert commented["status"] == "completed", commented
    context = legacy.get_season_context(SEASON_ID)
    for code in CODES:
        media_job = claim_legacy(media_jobs[code])
        mirrored = legacy._with_claimed_job_context(
            lambda job, worker_id=None: legacy._run_platform_media_mirror_stage(
                context=context, platform="instagram", job_id=job["id"], config=job["config"]
            )
        )(media_job)
        assert mirrored[2]["mirror"]["status"] == "mirrored", mirrored
        finish(media_job, status="completed", items_found=1)
        comment_media_job = claim_legacy(comment_media_jobs[code])
        comment = legacy._with_claimed_job_context(
            lambda job, worker_id=None: legacy._run_generic_comment_media_mirror_stage(
                context=context, platform="instagram", job_id=job["id"], config=job["config"]
            )
        )(comment_media_job)
        assert comment[2]["comment_media_mirror"]["status"] == "mirrored", comment
        finish(comment_media_job, status="completed", items_found=1)
    return {"listed": 12, "detailed": 12, "commented": 24, "media_posts": 12, "media_comments": 12}


def snapshot(dsn):
    with admin(dsn) as conn, conn.cursor() as cur:
        result = {}
        for name, query in {
            "posts": "SELECT shortcode,caption,likes,comments_count,media_urls,hosted_media_urls,"
            "hosted_thumbnail_url,media_mirror_status,hosted_owner_profile_pic_url,asset_manifest "
            "FROM social.instagram_posts ORDER BY shortcode",
            "catalog": "SELECT source_id,caption,likes,comments_count "
            "FROM social.instagram_account_catalog_posts ORDER BY source_id",
            "comments": "SELECT comment_id,text,is_reply,media_urls,hosted_media_urls,hosted_author_profile_pic_url "
            "FROM social.instagram_comments ORDER BY comment_id",
            "avatars": "SELECT account_handle,status,hosted_url FROM social.avatar_registry ORDER BY account_handle",
            "canonical": "SELECT source_id,body,like_count,comment_count,view_count,media_type,canonical_url "
            "FROM social.social_posts ORDER BY source_id",
            "comment_topology": "SELECT c.comment_id,p.comment_id AS parent_external,c.text,c.likes,c.is_reply,"
            "c.is_missing FROM social.instagram_comments c LEFT JOIN social.instagram_comments p "
            "ON p.id=c.parent_comment_id ORDER BY c.comment_id",
            "canonical_assets": "SELECT p.source_id,a.position,a.source_url,a.hosted_url,a.mirror_status "
            "FROM social.social_post_media_assets a JOIN social.social_posts p ON p.id=a.post_id "
            "ORDER BY p.source_id,a.position",
            "post_metadata": "SELECT shortcode,username,source_account,posted_at,width,height,"
            "show_id::text,season_id::text "
            "FROM social.instagram_posts ORDER BY shortcode",
            "payload_sidecars": "SELECT p.shortcode,b.raw_data,b.child_posts_data,b.asset_manifest "
            "FROM social.instagram_posts p JOIN social.instagram_post_payloads b ON b.post_id=p.id "
            "ORDER BY p.shortcode",
            "catalog_payload_sidecars": "SELECT p.source_id,b.raw_data,b.child_posts_data FROM "
            "social.instagram_account_catalog_posts p JOIN social.instagram_account_catalog_post_payloads b "
            "ON b.catalog_post_id=p.id ORDER BY p.source_id",
            "jobs": "SELECT coalesce(config->>'stage',job_type),status,count(*) "
            "FROM social.scrape_jobs GROUP BY 1,2 ORDER BY 1,2",
            "runs": "SELECT status,total_jobs,completed_jobs,failed_jobs,active_jobs "
            "FROM social.scrape_runs ORDER BY id",
            "new_targets": "SELECT stage,outcome,count(*),sum(attempts),sum(requests) "
            "FROM social.instagram_ingestion_targets GROUP BY stage,outcome ORDER BY 1,2",
            "new_runs": "SELECT state,generation FROM social.instagram_ingestion_runs ORDER BY run_id",
            "new_receipts": "SELECT count(*) FROM social.instagram_ingestion_receipts",
            "new_evidence": "SELECT parser_version,count(*) FROM social.instagram_ingestion_evidence "
            "GROUP BY parser_version ORDER BY parser_version",
        }.items():
            cur.execute(query)
            result[name] = [list(row) for row in cur.fetchall()]
        return result


def assert_outputs(root, states, *, candidate):
    from full_pipeline_sources import asset_identities
    from PIL import Image

    fixture = json.loads((root / "fixture-manifest.json").read_text())
    expected_assets = {row["identity"]: row for row in fixture["assets"]}
    assert set(expected_assets) == set(asset_identities())
    objects = {}
    for path in (root / "objects").glob("*"):
        row = json.loads(path.read_text())
        objects[HOSTED + row["key"]] = bytes.fromhex(row["data"])
    roles = []

    def resolve(url, identity_name, role, *, source_asset=None, metadata=None):
        assert url in objects, (identity_name, role, "Hosted URL has no stored bytes")
        body = objects[url]
        with Image.open(io.BytesIO(body)) as decoded:
            width, height = decoded.size
            format_ = decoded.format.lower()
        digest = hashlib.sha256(body).hexdigest()
        if source_asset:
            assert digest == expected_assets[source_asset]["sha256"], (identity_name, role, "Source byte mismatch")
            assert (width, height) == (120, 80)
        if metadata:
            assert (metadata["width"], metadata["height"]) == (width, height)
            assert metadata["bytes"] == len(body)
            assert metadata["format"].lower() == format_
            name = role.removeprefix("variant:")
            expected_dimensions = (
                (1440, 1920)
                if name.startswith("poster_detail")
                else ((720, 960) if name.startswith("poster_card") else (120, 80))
            )
            assert (width, height) == expected_dimensions, (identity_name, role, "Unexpected variant dimensions")
            assert format_ == ("jpeg" if name.endswith("_jpeg") else "webp")
        roles.append(
            {
                "identity": identity_name,
                "role": role,
                "url": url,
                "sha256": digest,
                "width": width,
                "height": height,
                "bytes": len(body),
            }
        )

    assert states["catalog"] == [[code, "Detail " + code, 7, 2] for code in CODES]
    assert states["canonical"] == [
        [code, "Detail " + code, 7, 2, 0, "image", "https://www.instagram.com/p/" + code + "/"] for code in CODES
    ]
    expected_topology = []
    for code in CODES:
        expected_topology += [
            [code + "-parent", None, "Parent " + code, 3, False, False],
            [code + "-reply", code + "-parent", "Reply " + code, 1, True, False],
        ]
    assert states["comment_topology"] == expected_topology
    assert [row[0] for row in states["posts"]] == list(CODES)
    assert [row[0] for row in states["payload_sidecars"]] == list(CODES)
    assert [row[0] for row in states["catalog_payload_sidecars"]] == list(CODES)
    for row in states["post_metadata"]:
        assert row[1:3] == [ACCOUNT, ACCOUNT] and row[4:] == [120, 80, SHOW_ID, SEASON_ID], row
        assert datetime.fromisoformat(str(row[3])) == OBSERVED
    variants = {
        f"{role}_{format_}"
        for role in ("card", "thumb", "detail", "poster_card", "poster_detail")
        for format_ in ("jpeg", "webp")
    }
    for code, caption, likes, comments, source, hosted, thumbnail, status, owner_avatar, manifest in states["posts"]:
        assert (caption, likes, comments, status) == ("Detail " + code, 7, 2, "mirrored")
        assert source == [CDN + "/" + code + ".png"] and len(hosted) == 1
        resolve(hosted[0], code, "post-original", source_asset=code + ".png")
        resolve(thumbnail, code, "post-thumbnail", source_asset=code + ".png")
        resolve(owner_avatar, code, "owner-avatar", source_asset="owner.png")
        assert set(manifest["display_variants"]) == variants
        for name, entry in manifest["display_variants"].items():
            resolve(entry["url"], code, "variant:" + name, metadata=entry)
        originals = manifest["originals"]
        assert len(originals["media"]) == 1
        resolve(originals["cover"]["url"], code, "manifest-cover", source_asset=code + ".png")
        resolve(originals["media"][0]["url"], code, "manifest-media", source_asset=code + ".png")
    for comment_id, _text, reply, source, hosted, avatar in states["comments"]:
        code = comment_id.split("-")[0]
        assert source == ([] if reply else [CDN + "/" + code + "-comment.png"])
        assert len(hosted) == (0 if reply else 1)
        if not reply:
            resolve(hosted[0], comment_id, "comment-original", source_asset=code + "-comment.png")
        resolve(avatar, comment_id, "author-avatar", source_asset="replier.png" if reply else "reader.png")
    assert [row[:2] for row in states["avatars"]] == [
        ["reader", "mirrored"],
        ["replier", "mirrored"],
        [ACCOUNT, "mirrored"],
    ]
    for handle, _, url in states["avatars"]:
        resolve(url, handle, "registry-avatar", source_asset="owner.png" if handle == ACCOUNT else handle + ".png")
    assert len(states["canonical_assets"]) == 12
    for code, position, source, hosted, status in states["canonical_assets"]:
        assert position == 0 and source == CDN + "/" + code + ".png"
        if candidate:
            assert status == "mirrored"
            resolve(hosted, code, "canonical-original", source_asset=code + ".png")
        else:
            assert hosted is None and status is None, "Review changed legacy canonical media coverage"
    if candidate:
        # Prove cached avatar references preserve identity as well as body bytes.
        registry_urls = {handle: url for handle, _, url in states["avatars"]}
        assert all(row[8] == registry_urls[ACCOUNT] for row in states["posts"])
        assert all(row[5] == registry_urls["replier" if row[2] else "reader"] for row in states["comments"])
        assert set(objects) == {row["url"] for row in roles}, "Unreferenced physical object remains"
    # Twelve shared cover/media originals plus120 variant keys,12 comment
    # originals and three avatar identities. Prior avatar HEAD hits did not add
    # physical objects, so cached registry reuse does not change this fixture.
    assert len(objects) == 147, "Physical object expectation failed; preserve native outputs and investigate"
    # Canonical hosted fields are explicitly additional candidate coverage.
    common_roles = sorted(
        ({k: value for k, value in row.items() if k != "url"} for row in roles if row["role"] != "canonical-original"),
        key=lambda row: (row["identity"], row["role"]),
    )
    result = {
        "common_metadata": {key: states[key] for key in ("catalog", "canonical", "comment_topology", "post_metadata")},
        "common_roles": common_roles,
        "literal_roles": roles,
        "physical_objects": len(objects),
        "shared_avatar_registry_urls_proven": candidate,
        "canonical_asset_delta": "candidate hosted/status populated; legacy null",
        "exact_output_parity_claimed": False,
    }
    write_json(root / "output-assertions.json", result)
    return result


def aggregate_events(root):
    channels = {}
    for channel in ("transport-events", "storage-events", "dispatch-timings"):
        path = root / (channel + ".jsonl")
        rows = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        if channel == "storage-events":
            counts = Counter((row["operation"], row["outcome"]) for row in rows)
            summary = {op + ":" + outcome: count for (op, outcome), count in sorted(counts.items())}
        else:
            summary = dict(Counter(row.get("kind", row.get("stage")) for row in rows))
        channels[channel] = {"events": len(rows), "counts": summary, "processes": len({row["pid"] for row in rows})}
    write_json(root / "operation-counts.json", channels)
    return channels


def compare_outputs(roots):
    seals = [json.loads((root / "sealed-inputs.json").read_text()) for root in roots]
    for key in ("fixture_sha256", "harness", "pool_profile", "packages"):
        assert seals[0][key] == seals[1][key], "Measured input mismatch: " + key
    left, right = (json.loads((root / "output-assertions.json").read_text()) for root in roots)
    for key in ("common_metadata", "common_roles", "physical_objects"):
        assert left[key] == right[key], "Common workload parity failed: " + key
    by_role = [{(row["identity"], row["role"]): row for row in arm["literal_roles"]} for arm in (left, right)]
    differences = [
        {
            "identity": key[0],
            "role": key[1],
            "legacy_url": by_role[0][key]["url"],
            "candidate_url": by_role[1][key]["url"],
        }
        for key in sorted(by_role[0])
        if by_role[0][key]["url"] != by_role[1][key]["url"]
    ]
    result = {
        "common_fields_and_asset_bytes": "passed",
        "literal_url_differences": differences,
        "candidate_additional_canonical_roles": [
            row for row in right["literal_roles"] if row["role"] == "canonical-original"
        ],
        "exact_output_parity_claimed": False,
    }
    write_json(roots[1] / "matched-output-comparison.json", result)
    print(json.dumps({"common_fields_and_asset_bytes": "passed", "literal_url_differences": len(differences)}))


def self_checks(source_root=None):
    """Focused negative checks for the new evidence guards; no services/network."""
    import tempfile
    from unittest.mock import Mock, patch

    from botocore.exceptions import ClientError
    from full_pipeline_sources import Storage, fixture_manifest, install_sources, png

    if source_root:
        sys.path.insert(0, str(source_root.resolve()))

    checks = []

    def must_raise(kind, operation):
        try:
            operation()
        except kind:
            return
        raise AssertionError("Expected failure: " + kind.__name__)

    with tempfile.TemporaryDirectory(prefix="trr-full-resource-checks-") as temp:
        root = Path(temp)
        with patch.dict(os.environ, {"EVAL_RUNTIME_ROOT": str(root)}):
            fixture = fixture_manifest(root)
            assert len({row["sha256"] for row in fixture["assets"]}) == 27
            assert png("Resource000.png") == (root / "fixture-assets/Resource000.png").read_bytes()
            must_raise(AssertionError, lambda: png("unknown.png"))
            checks.append("27 distinct deterministic asset bodies; unknown identity rejected")
            store = Storage()
            must_raise(ClientError, lambda: store.head_object(Bucket="fixture", Key="missing"))
            must_raise(ClientError, lambda: store.get_object(Bucket="fixture", Key="missing"))
            store.put_object(Bucket="fixture", Key="one", Body=b"one", IfNoneMatch="*")
            must_raise(ClientError, lambda: store.put_object(Bucket="fixture", Key="one", Body=b"two", IfNoneMatch="*"))
            assert store.get_object(Bucket="fixture", Key="one")["Body"].read() == b"one"
            store.head_object(Bucket="fixture", Key="one")
            store.upload_fileobj(io.BytesIO(b"two"), "fixture", "two")
            store.close()
            events = [json.loads(line) for line in (root / "storage-events.jsonl").read_text().splitlines()]
            assert len(events) == 8 and sum(row["outcome"] == "precondition_failed" for row in events) == 1
            assert events[-2]["operation"] == "upload_fileobj" and events[-2]["bytes"] == 3
            checks.append("SDK events retain misses, conditional conflict, transfer bytes and close")
            # No caller credentials or source services are consulted by this media-only fixture seam.
            install_sources(stage="media")
            import requests

            must_raise(AssertionError, lambda: requests.Session().get("https://unknown.example/Resource000"))
            must_raise(AssertionError, lambda: requests.Session().get(CDN + "/unknown.png"))
            must_raise(AssertionError, lambda: requests.Session().get("https://www.instagram.com/unknown/Resource000"))
            checks.append("unknown transport host and asset rejected")
            sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
            try:
                register_child(sleeper)
                assert sleeper.pid in reap_tracked_children(root)
                assert sleeper.wait(timeout=5) < 0
            finally:
                if sleeper.poll() is None:
                    os.killpg(sleeper.pid, signal.SIGKILL)
                    sleeper.wait(timeout=5)
            checks.append("task-recorded activity group reaped after coordinator loss")
        source = root / "source"
        source.mkdir()
        digest = hashlib.sha256(b"sealed").hexdigest()
        files = {}
        for index in range(2400):
            name = f"file-{index}"
            (source / name).write_bytes(b"sealed")
            files[name] = digest
        write_json(root / "source-full-manifest.json", {"commit": "synthetic-check", "files": files})
        assert enforce_source(source)["file_count"] == 2400
        (source / "file-0").write_bytes(b"changed")
        must_raise(AssertionError, lambda: enforce_source(source))
        (source / "file-0").write_bytes(b"sealed")
        (source / "unexpected").write_bytes(b"extra")
        must_raise(AssertionError, lambda: enforce_source(source))
        checks.append("full manifest rejects changed and unexpected source files")
        observer = Measurement.__new__(Measurement)
        observer.root, observer.dsn = root, "synthetic-no-connection"
        observer.stop, observer.error, observer.samples, observer.complete = threading.Event(), None, [], None
        observer.thread, observer.started = Mock(), time.monotonic()
        observer.thread.is_alive.return_value = False
        with patch.object(psycopg2, "connect", side_effect=RuntimeError("synthetic observer failure")):
            observer.observe()
            assert observer.error["class"] == "RuntimeError" and observer.stop.is_set()
            must_raise(AssertionError, observer.finish)
        report = json.loads((root / "measurement.json").read_text())
        assert report["valid"] is False and report["observer_error"]["class"] == "RuntimeError"
        checks.append("observer failure propagates and preserves invalid measurement")
    print(json.dumps({"focused_checks": checks, "passed": len(checks)}))


def execute_private(command, *, env, cwd, log, timeout):
    process = subprocess.Popen(command, env=env, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        process.wait(timeout=timeout)
        return process.returncode
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        reap_tracked_children(Path(env["EVAL_RUNTIME_ROOT"]))


class Measurement:
    """Observe through a separate DB role so every workload SQL call counts."""

    def __init__(self, dsn, root, kind):
        self.root, self.kind = root, kind
        with admin(dsn) as conn, conn.cursor() as cur:
            cur.execute("CREATE ROLE eval_observer LOGIN SUPERUSER")
        self.dsn = dsn.replace("eval@", "eval_observer@")
        self.stop = threading.Event()
        self.samples = []
        self.complete = None
        self.thread = None
        self.error = None

    @staticmethod
    def queries(cur):
        cur.execute("""SELECT r.rolname,s.query,s.calls,s.total_exec_time FROM pg_stat_statements s
            JOIN pg_roles r ON r.oid=s.userid WHERE r.rolname IN ('eval','eval_runtime')
            ORDER BY r.rolname,s.query""")
        return [
            {"role": role, "query": query, "calls": calls, "server_exec_ms": seconds}
            for role, query, calls, seconds in cur.fetchall()
        ]

    def start(self, dsn):
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_stat_statements_reset()")
        conn.close()
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self.observe, daemon=True)
        self.thread.start()

    def observe(self):
        conn = None
        try:
            conn = psycopg2.connect(self.dsn, application_name="full-eval-observer")
            conn.autocommit = True
            with conn.cursor() as cur:
                while not self.stop.is_set():
                    cur.execute("""SELECT usename,application_name,state,count(*) FROM pg_stat_activity
                        WHERE backend_type='client backend' AND usename IN ('eval','eval_runtime')
                        GROUP BY usename,application_name,state ORDER BY 1,2,3""")
                    self.samples.append(
                        {"seconds": time.monotonic() - self.started, "sessions": [list(row) for row in cur.fetchall()]}
                    )
                    if self.complete is None:
                        table = (
                            "social.instagram_ingestion_runs"
                            if self.kind.startswith("candidate")
                            else "social.scrape_runs"
                        )
                        field = "state" if self.kind.startswith("candidate") else "status"
                        cur.execute(f"SELECT count(*) FROM {table} WHERE {field}='completed'")
                        if cur.fetchone()[0]:
                            self.complete = {"seconds": time.monotonic() - self.started, "queries": self.queries(cur)}
                    self.stop.wait(0.01)
        except BaseException as exc:
            self.error = {"class": type(exc).__name__, "traceback": traceback.format_exc()}
            self.stop.set()
        finally:
            if conn:
                conn.close()

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            self.error = {"class": "ObserverDidNotStop"}
        conn = None
        queries, final_sessions = [], None
        try:
            conn = psycopg2.connect(self.dsn, application_name="full-eval-observer")
            conn.autocommit = True
            with conn.cursor() as cur:
                queries = self.queries(cur)
                cur.execute("""SELECT usename,application_name,state,count(*) FROM pg_stat_activity
                    WHERE backend_type='client backend' AND usename IN ('eval','eval_runtime')
                    GROUP BY usename,application_name,state ORDER BY 1,2,3""")
                final_sessions = [list(row) for row in cur.fetchall()]
        except BaseException as exc:
            self.error = {"class": type(exc).__name__, "traceback": traceback.format_exc()}
        finally:
            if conn:
                conn.close()
        result = {
            "full_lifecycle_seconds": time.monotonic() - self.started,
            "completion": self.complete,
            "queries": queries,
            "samples": self.samples,
            "peak_sessions": max((sum(row[3] for row in sample["sessions"]) for sample in self.samples), default=0),
            "scope": "All workload-role pg_stat_statements calls incl transaction/session setup, admission, "
            "coordinator polls and lease/SDK. Separate observer role entirely excluded. Raw includes one reset "
            "SELECT; bootstrap/schema/fixtures and post-run parity snapshot outside interval.",
            "final_sessions": final_sessions,
            "observer_error": self.error,
            "valid": self.error is None and bool(self.samples) and self.complete is not None and final_sessions == [],
        }
        (self.root / "measurement.json").write_text(json.dumps(result, indent=2) + "\n")
        assert result["valid"], "Invalid measurement: observer failure or unobserved completion"
        assert not final_sessions, "Workload process left database sessions after lifecycle completion"
        return result


def main(args):
    root = args.runtime_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    source = args.source_root.resolve()
    initial_source = enforce_source(source)
    initial_harness = harness_hashes()
    from full_pipeline_sources import fixture_manifest

    fixture = fixture_manifest(root)
    write_json(root / "fixture-manifest.json", fixture)
    initial_fixture = hashlib.sha256((root / "fixture-manifest.json").read_bytes()).hexdigest()
    binaries = Path("/opt/homebrew/opt/postgresql@17/bin")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "postgres"
    subprocess.run(
        [str(binaries / "initdb"), "-D", str(data), "-A", "trust", "-U", "eval", "--no-locale", "--encoding=UTF8"],
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
            f"-p {port} -h 127.0.0.1 -k '' -c shared_preload_libraries=pg_stat_statements",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    dsn = f"postgresql://eval@127.0.0.1:{port}/postgres"
    measurement, inputs = None, None
    try:
        prepare_schema(dsn, source)
        env = private_environment(dsn, source, root, args.profile)
        if args.probe == "candidate-dbos":
            with admin(dsn) as conn, conn.cursor() as cur:
                cur.execute("CREATE ROLE eval_runtime LOGIN")
            with (root / "bootstrap.log").open("w") as log:
                assert (
                    execute_private(
                        [sys.executable, str(Path(__file__).resolve()), "--child", "bootstrap"],
                        env=env,
                        cwd=source,
                        log=log,
                        timeout=30,
                    )
                    == 0
                )
            env["EVAL_RUNTIME_URL"] = dsn.replace("eval@", "eval_runtime@")
        schema_preflight(dsn, root, binaries)
        inputs = {
            "source": initial_source,
            "harness": initial_harness,
            "schema_sha256": hashlib.sha256((root / "schema.sql").read_bytes()).hexdigest(),
            "fixture_sha256": initial_fixture,
            "python": sys.version,
            "packages": {
                name: importlib.metadata.version(name)
                for name in ("dbos", "psycopg2-binary", "psycopg", "Pillow", "httpx", "requests", "psutil")
            },
            "pool_profile": args.profile,
            "measurement_excludes": [
                "schema/SDK migration",
                "fixture seed",
                "observer-role SQL",
                "post-run assertions",
            ],
        }
        write_json(root / "sealed-inputs.json", inputs)
        if args.preflight_only:
            print(json.dumps({"preflight": "passed", "source": initial_source}))
            return
        measurement = Measurement(dsn, root, args.probe) if args.measure else None
        if measurement:
            measurement.start(dsn)
        started = time.monotonic()
        with (root / "child.log").open("w") as log:
            result = execute_private(
                [sys.executable, str(Path(__file__).resolve()), "--child", args.probe],
                env=env,
                cwd=source,
                log=log,
                timeout=120,
            )
        if measurement:
            measurement.finish()
        states = snapshot(dsn)
        (root / "snapshot.json").write_text(json.dumps(states, default=str, indent=2) + "\n")
        objects = []
        for path in sorted((root / "objects").glob("*")):
            row = json.loads(path.read_text())
            body = bytes.fromhex(row["data"])
            objects.append({"key": row["key"], "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)})
        (root / "objects.json").write_text(json.dumps(objects, indent=2) + "\n")
        aggregate_events(root)
        if result == 0 and args.probe in {"legacy-full", "candidate-dbos"}:
            assert_outputs(root, states, candidate=args.probe == "candidate-dbos")
        summary = {
            "probe": args.probe,
            "returncode": result,
            "seconds": time.monotonic() - started,
            "source_commit": initial_source["commit"],
            "profile": args.profile,
            "harness_hashes": initial_harness,
        }
        (root / "result.json").write_text(json.dumps(summary, indent=2) + "\n")
        sql_errors = [line for line in (root / "postgres.log").read_text().splitlines() if "ERROR:" in line]
        (root / "sql-errors.json").write_text(json.dumps(sql_errors, indent=2) + "\n")
        assert not sql_errors, "Unexpected PostgreSQL errors; inspect preserved sql-errors.json"
        print(json.dumps(summary))
        if result == 0 and args.probe == "legacy-full":
            assert len(states["posts"]) == len(states["catalog"]) == 12
            assert len(states["comments"]) == 24 and len(states["avatars"]) == 3
            assert states["runs"][0][0] == "completed" and states["runs"][0][4] == 0, states["runs"]
        if result == 0 and args.probe == "candidate-dbos":
            assert states["new_runs"] == [["completed", 1]], states["new_runs"]
            assert states["new_receipts"] == [[38]], states["new_receipts"]
        assert result == 0, "Child failed; inspect preserved private child.log"
    except BaseException as exc:
        write_json(root / "failure.json", {"class": type(exc).__name__, "traceback": traceback.format_exc()})
        raise
    finally:
        try:
            if measurement and not measurement.stop.is_set():
                measurement.finish()
        finally:
            subprocess.run(
                [str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
            )
            write_json(root / "cleanup.json", {"postgres_stopped": not (data / "postmaster.pid").exists()})
            sql_errors = [line for line in (root / "postgres.log").read_text().splitlines() if "ERROR:" in line]
            write_json(root / "sql-errors.json", sql_errors)
        final_source = enforce_source(source)
        assert final_source == initial_source and harness_hashes() == initial_harness, "Input drift during probe"
        assert hashlib.sha256((root / "fixture-manifest.json").read_bytes()).hexdigest() == initial_fixture
        if inputs:
            assert hashlib.sha256((root / "schema.sql").read_bytes()).hexdigest() == inputs["schema_sha256"]
        for asset in fixture["assets"]:
            assert (
                hashlib.sha256((root / "fixture-assets" / asset["identity"]).read_bytes()).hexdigest()
                == asset["sha256"]
            )
        write_json(root / "verified-inputs-after.json", {"source": final_source, "harness": harness_hashes()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--probe", default="legacy-comments")
    parser.add_argument("--profile", choices=("declared-modal", "all-one-diagnostic"), default="declared-modal")
    parser.add_argument("--child")
    parser.add_argument("--measure", action="store_true")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--self-checks", action="store_true")
    parser.add_argument("--compare-roots", type=Path, nargs=2)
    options = parser.parse_args()
    if options.self_checks:
        self_checks(options.source_root)
    elif options.compare_roots:
        compare_outputs(options.compare_roots)
    else:
        child(options) if options.child else main(options)
