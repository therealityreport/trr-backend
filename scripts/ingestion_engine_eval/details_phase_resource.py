"""Actual legacy vs candidate details-phase coordination with fixture HTTP.

Begins at prepared catalog/manifests/outer job ownership, ends at target receipts.
Excludes listing/comments/media, Modal/engine service and outer launch/rollup.
"""

import argparse
import hashlib
import json
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2.extensions import cursor as pg_cursor

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests/fixtures/ingestion"
ACCOUNT = "fixture_account"
WORKER = "fixture-worker"
COUNTS = Counter()
MEASURING = False
ACTIVE = 0
PEAK = 0
OPENED = 0


def category():
    frame = sys._getframe(2)
    candidates = []
    while frame:
        name, path = frame.f_code.co_name, frame.f_code.co_filename
        if path.endswith("ingestion/canonical.py"):
            return "canonical"
        if path.endswith("ingestion/persistence.py"):
            if name == "retain_evidence":
                candidates.append("evidence_transaction")
            elif name == "commit_target":
                candidates.append("receipt_transaction")
            else:
                candidates.append("candidate_" + name)
        if path.endswith("instagram/detail_transport.py"):
            candidates.append("shared_request_gate")
        if path.endswith("ingestion/workflow_store.py"):
            candidates.append("scheduler_" + name)
        if path.endswith("control_plane/instagram_detail_targets.py"):
            candidates.append("legacy_target_" + name)
        if "social_season_analytics" in path and name in {
            "_raise_if_shared_account_stage_cancelled",
            "_touch_job_heartbeat",
            "_job_followup_write_scope",
        }:
            candidates.append("legacy_" + name)
        if path.endswith("instagram/catalog_ingest.py"):
            candidates.append("legacy_canonical_or_execution")
        frame = frame.f_back
    return candidates[0] if candidates else "other"


class CountedConnection(pg_connection):
    def cursor(self, *args, **kwargs):
        factory = kwargs.pop("cursor_factory", pg_cursor)

        class CountedCursor(factory):
            def execute(self, query, params=None):
                if MEASURING:
                    COUNTS[category()] += 1
                return super().execute(query, params)

        return super().cursor(*args, cursor_factory=CountedCursor, **kwargs)


def main(root, targets, repetitions):
    global MEASURING, ACTIVE, PEAK, OPENED
    root.mkdir(parents=True, exist_ok=False)
    binaries = Path("/opt/homebrew/opt/postgresql@17/bin")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "postgres"
    subprocess.run(
        [str(binaries / "initdb"), "-D", str(data), "-A", "trust", "-U", "eval", "--no-locale", "--encoding=UTF8"],
        check=True,
        stdout=(root / "initdb.log").open("w"),
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
    from trr_backend.db import pg
    from trr_backend.ingestion.domain import RunIntent, Stage
    from trr_backend.ingestion.post_transport import DetailSource
    from trr_backend.ingestion.worker import Worker
    from trr_backend.ingestion.workflow_store import WorkflowStore
    from trr_backend.ingestion.workflows import Scheduler
    from trr_backend.socials import social_season_analytics_impl as core
    from trr_backend.socials.control_plane import instagram_detail_targets as legacy_targets
    from trr_backend.socials.instagram import InstagramScraper
    from trr_backend.socials.instagram import catalog_ingest as legacy

    @contextmanager
    def transaction(**kwargs):
        global ACTIVE, PEAK, OPENED
        existing = pg._transaction_connection.get()
        if existing is not None:
            yield existing
            return
        conn = psycopg2.connect(dsn, connection_factory=CountedConnection, application_name="details-worker")
        ACTIVE += 1
        PEAK = max(PEAK, ACTIVE)
        if MEASURING:
            OPENED += 1
        try:
            with conn:
                yield conn
        finally:
            ACTIVE -= 1
            conn.close()

    def one(query, params=None, **kwargs):
        with pg.db_cursor(conn=kwargs.get("conn")) as cur:
            return pg.fetch_one_with_cursor(cur, query, params or [])

    pg.db_connection = transaction
    pg.fetch_one = one
    capture = json.loads((FIXTURES / "canonical_capture.json").read_text())["post"]
    payloads = {}
    for index in range(targets):
        code = f"Resource{index:03}"
        payloads[code] = {
            "items": [
                {
                    "code": code,
                    "pk": str(100001 + index),
                    "taken_at": capture["taken_at"],
                    "media_type": 1,
                    "caption": {"text": capture["caption"]},
                    "like_count": capture["likes"],
                    "comment_count": capture["comments"],
                    "user": {"pk": "fixture-owner", "username": ACCOUNT},
                    "image_versions2": {
                        "candidates": [{"url": capture["media_urls"][0], "width": 1080, "height": 1350}]
                    },
                }
            ]
        }
    (root / "input.json").write_text(json.dumps(payloads, indent=2) + "\n")
    requests = []

    class Scraper(InstagramScraper):
        def __init__(self, **kwargs):
            import requests as request_library

            self.session = request_library.Session()
            self.session.get = self.get

        def _request_cookies(self):
            return {"sessionid": "synthetic-fixture", "ds_user_id": "synthetic-fixture"}

        def _get_headers(self, referer=None):
            return {}

        def _shortcode_to_media_id(self, shortcode):
            return shortcode

        def get(self, url, **kwargs):
            assert ACTIVE == 0, "DB connection spans source request"
            code = url.split("/")[-3]
            requests.append(code)
            return SimpleNamespace(
                status_code=200,
                headers={"content-type": "application/json"},
                is_redirect=False,
                text="",
                json=lambda: payloads[code],
            )

    core._build_shared_instagram_scraper = lambda **kwargs: Scraper()
    legacy._build_shared_instagram_scraper = core._build_shared_instagram_scraper
    stop = threading.Event()
    samples = []

    def sample():
        conn = psycopg2.connect(dsn, application_name="details-observer")
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                while not stop.wait(0.01):
                    cur.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name='details-worker'")
                    samples.append(cur.fetchone()[0])
        finally:
            conn.close()

    def prepared(kind):
        with transaction() as conn, conn.cursor() as cur:
            cur.execute(
                "TRUNCATE social.instagram_posts,social.instagram_account_catalog_posts,social.social_posts,"
                "social.scrape_runs,social.instagram_detail_request_lanes CASCADE"
            )
            for code in payloads:
                cur.execute(
                    "INSERT INTO social.instagram_account_catalog_posts(source_id,source_account,posted_at) "
                    "VALUES(%s,%s,%s)",
                    (code, ACCOUNT, datetime.now(UTC)),
                )
        if kind == "legacy":
            run_id, job_id = str(uuid4()), str(uuid4())
            with transaction() as conn, conn.cursor() as cur:
                cur.execute("INSERT INTO social.scrape_runs(id,status,config) VALUES(%s,'running','{}')", (run_id,))
                manifest = legacy_targets.freeze_manifest(
                    run_id,
                    ACCOUNT,
                    {"selected_tasks": ["post_details"], "details_refresh_force_detail_fetch": True},
                    conn=conn,
                )
                config = {
                    "detail_contract_version": 1,
                    "detail_manifest": manifest,
                    "stage": "shared_account_posts",
                    "account": ACCOUNT,
                    "details_refresh_skip_media_followups": True,
                    "details_chunk_targets": 50,
                    "details_refresh_write_batch_size": 10,
                    "claim_token": "fixture-claim",
                }
                cur.execute(
                    "INSERT INTO social.scrape_jobs(id,run_id,status,worker_id,config,detail_dispatch_token,"
                    "detail_dispatch_generation,detail_dispatch_expires_at,claimed_at) "
                    "VALUES(%s,%s,'running',%s,%s::jsonb,'fixture-dispatch',1,now()+interval '10 minutes',now())",
                    (job_id, run_id, WORKER, json.dumps(config)),
                )
            return {
                "id": job_id,
                "run_id": run_id,
                "worker_id": WORKER,
                "config": config,
                "detail_dispatch_token": "fixture-dispatch",
            }
        store = WorkflowStore(transaction)
        run = store.start(
            RunIntent(account=ACCOUNT, selected_tasks=(Stage.DETAILS,), force=True),
            idempotency_key=str(uuid4()),
            engine="dbos",
            workflow_version="resource-v1",
            initiated_by="fixture",
        )
        store.discover(
            run["run_id"],
            ACCOUNT,
            generation=1,
            stage=Stage.DETAILS,
            target_keys=list(payloads),
            max_attempts=3,
            cursor_state={},
            expected_cursor_state={},
            page_id="fixture",
            seal=True,
        )
        with transaction() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE social.instagram_ingestion_manifests SET discovery_complete=true,sealed_at=clock_timestamp() "
                "WHERE run_id=%s AND stage='listing'",
                (run["run_id"],),
            )
        item = store.seed(dict(run, account=ACCOUNT))
        errors = []

        def forbidden(*args, **kwargs):
            raise AssertionError("Non-detail stage reached by details-only fixture")

        worker = Worker(
            store,
            listing_source=forbidden,
            detail_source=lambda context, owned: DetailSource(
                shortcode=owned["target_key"],
                cookies={"sessionid": "synthetic-fixture"},
                browser_account_id=ACCOUNT,
                scraper_factory=Scraper,
                transaction=transaction,
            ),
            comment_source=forbidden,
            media_loader=forbidden,
            media_storage=forbidden,
        )

        def dispatch(reservation):
            result = worker.execute(reservation)
            if result["outcome"] != "committed":
                errors.append(result)
                raise AssertionError(result)
            return "fixture:" + str(reservation["owner_token"])

        scheduler = Scheduler(store, admit=lambda decision: True, dispatch=dispatch, reconcile=lambda _: "unknown")
        return store, item, scheduler, errors

    observer = None
    try:
        with transaction() as conn, conn.cursor() as cur:
            cur.execute((FIXTURES / "canonical_schema.sql").read_text())
            cur.execute("CREATE ROLE anon;CREATE ROLE authenticated;CREATE ROLE service_role;")
            cur.execute(
                "ALTER TABLE social.scrape_runs ADD COLUMN status text,ADD COLUMN source_scope text,"
                "ADD COLUMN initiated_by text,ADD COLUMN config jsonb,ADD COLUMN cancelled_at timestamptz;"
            )
            cur.execute(
                "ALTER TABLE social.scrape_jobs ADD COLUMN run_id uuid,ADD COLUMN status text,"
                "ADD COLUMN worker_id text,ADD COLUMN config jsonb,ADD COLUMN available_at timestamptz DEFAULT now(),"
                "ADD COLUMN heartbeat_at timestamptz,ADD COLUMN claimed_at timestamptz;"
            )
            detail_sql = (REPO / "supabase/migrations/20260911120000_instagram_detail_run_targets.sql").read_text()
            cur.execute(detail_sql.split("alter table social.social_post_observations")[0] + "commit;")
            cur.execute(
                "CREATE UNIQUE INDEX social_post_observations_detail_target_idx ON "
                "social.social_post_observations(detail_run_id,detail_source_id) WHERE detail_run_id IS NOT NULL"
            )
            for name in [
                "20260911121000_instagram_detail_request_lanes.sql",
                "20261016070000_instagram_ingestion_foundation.sql",
                "20261016070100_instagram_ingestion_workflows.sql",
            ]:
                cur.execute((REPO / "supabase/migrations" / name).read_text())
            cur.execute("CREATE EXTENSION pg_stat_statements")
        observer = threading.Thread(target=sample)
        observer.start()
        results = []
        for repetition in range(repetitions):
            for kind in ("legacy", "candidate") if repetition % 2 == 0 else ("candidate", "legacy"):
                prepared_run = prepared(kind)
                with transaction() as conn, conn.cursor() as cur:
                    cur.execute("SELECT pg_stat_statements_reset()")
                COUNTS.clear()
                samples.clear()
                requests.clear()
                PEAK = ACTIVE = OPENED = 0
                MEASURING = True
                started = time.perf_counter()
                if kind == "legacy":
                    outcomes = legacy.run_durable_instagram_details(prepared_run, worker_id=WORKER)
                    assert outcomes["committed"] == targets and outcomes["unresolved"] == 0, outcomes
                    assert outcomes["attempted"] == outcomes["requests"] == targets, outcomes
                else:
                    store, item, scheduler, errors = prepared_run
                    for ordinal in range(targets + 1):
                        result = scheduler.tick(item, ordinal)
                        assert not errors, errors
                        if result.get("stop"):
                            assert ordinal == targets and result["stop"] == "completed", result
                            store.complete_partition(item, continue_run=False, stop_state="completed")
                            break
                        assert result.get("ready"), result
                    with transaction() as conn, conn.cursor() as cur:
                        cur.execute(
                            "SELECT count(*),sum(attempts),sum(requests) FROM social.instagram_ingestion_targets "
                            "WHERE run_id=%s AND outcome='committed'",
                            (item["run_id"],),
                        )
                        assert cur.fetchone() == (targets, targets, targets)
                        cur.execute(
                            "SELECT count(*) FROM social.instagram_ingestion_receipts WHERE run_id=%s",
                            (item["run_id"],),
                        )
                        assert cur.fetchone()[0] == targets
                elapsed = time.perf_counter() - started
                MEASURING = False
                categories = dict(COUNTS)
                peak, opened = PEAK, OPENED
                with transaction() as conn, conn.cursor() as cur:
                    cur.execute(
                        "SELECT query,calls FROM pg_stat_statements WHERE query NOT LIKE "
                        "'SELECT count(*) FROM pg_stat_activity%' ORDER BY calls DESC"
                    )
                    queries = [{"query": query, "calls": calls} for query, calls in cur.fetchall()]
                    cur.execute("SELECT count(*),sum(likes),sum(comments_count) FROM social.instagram_posts")
                    canonical = list(cur.fetchone())
                assert canonical == [targets, targets * capture["likes"], targets * capture["comments"]], canonical
                row = {
                    "path": kind,
                    "repetition": repetition,
                    "seconds": elapsed,
                    "statement_calls": sum(q["calls"] for q in queries),
                    "canonical_post_summary": canonical,
                    "committed_targets": targets,
                    "source_requests": len(requests),
                    "peak_checked_out_sessions": peak,
                    "sampled_peak_worker_sessions": max(samples, default=0),
                    "connections_opened": opened,
                    "python_execute_categories": categories,
                }
                results.append(row)
                (root / f"queries-{kind}-{repetition}.json").write_text(json.dumps(queries, indent=2) + "\n")
                (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                print(json.dumps(row), flush=True)
        paths = [
            "trr_backend/ingestion/canonical.py",
            "trr_backend/ingestion/application.py",
            "trr_backend/ingestion/worker.py",
            "trr_backend/ingestion/workflow_store.py",
            "trr_backend/ingestion/persistence.py",
            "trr_backend/ingestion/workflows.py",
            "trr_backend/socials/instagram/catalog_ingest.py",
            "trr_backend/socials/control_plane/instagram_detail_targets.py",
        ]
        (root / "sources.json").write_text(
            json.dumps({p: hashlib.sha256((REPO / p).read_bytes()).hexdigest() for p in paths}, indent=2)
        )
    finally:
        MEASURING = False
        stop.set()
        if observer:
            observer.join(timeout=5)
        subprocess.run(
            [str(binaries / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
        )
        (root / "cleanup.json").write_text(json.dumps({"postgres_stopped": not (data / "postmaster.pid").exists()}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--targets", type=int, default=12)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    main(args.runtime_dir, args.targets, args.repetitions)
