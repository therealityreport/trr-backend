"""Baseline captured NOW from unchanged legacy canonical sinks, not past timing.

Narrow matched persistence seam; not an end-to-end ingestion benchmark. No
transport/storage calls are permitted. Canonical fixtures and real isolated PG
are shared; production functions are imported without modifying their source.
"""

import argparse
import copy
import hashlib
import json
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
from psycopg2.extras import RealDictCursor

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests/fixtures/ingestion"
OBSERVED = datetime(2026, 10, 16, 12, tzinfo=UTC)
ACCOUNT = "fixture_account"


def main(root, count, repetitions, baseline_dir=None):
    root.mkdir(parents=True, exist_ok=False)
    pg_bin = Path("/opt/homebrew/opt/postgresql@17/bin")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data = root / "postgres"
    subprocess.run(
        [str(pg_bin / "initdb"), "-D", str(data), "-A", "trust", "-U", "eval", "--no-locale", "--encoding=UTF8"],
        check=True,
        stdout=(root / "initdb.log").open("w"),
    )
    subprocess.run(
        [
            str(pg_bin / "pg_ctl"),
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
    # All existing DB entrypoints must receive our explicit conn. If a legacy
    # helper unexpectedly uses configured connection resolution, fail closed.
    from trr_backend.db import pg
    from trr_backend.ingestion import canonical
    from trr_backend.socials import social_season_analytics_impl as core
    from trr_backend.socials.instagram import catalog_ingest as legacy

    def forbidden(*args, **kwargs):
        raise RuntimeError("Full-path configured DB/transport seam is outside isolated benchmark")

    pg.db_connection = forbidden
    pg.fetch_one = forbidden
    original_connect = socket.socket.connect

    def local_only(sock, address):
        if not isinstance(address, tuple) or address[0] not in {"127.0.0.1", "localhost", "::1"}:
            raise RuntimeError("Non-local socket denied by benchmark")
        return original_connect(sock, address)

    socket.socket.connect = local_only
    # Identical explicit observation clock; no business algorithm replacement.
    core._now_utc = lambda: OBSERVED
    source = json.loads((FIXTURES / "canonical_capture.json").read_text())["post"]
    fixtures = []
    for index in range(count):
        node = copy.deepcopy(source)
        node.update(shortcode=f"Bench{index:04}", pk=str(100001 + index))
        fixtures.append(node)
    (root / "input.json").write_text(json.dumps(fixtures, indent=2) + "\n")
    samples = []
    stop = threading.Event()

    @contextmanager
    def tx():
        conn = psycopg2.connect(dsn, application_name="baseline-worker")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def observe():
        conn = psycopg2.connect(dsn, application_name="baseline-observer")
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                while not stop.wait(0.01):
                    cur.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name='baseline-worker'")
                    samples.append(cur.fetchone()[0])
        finally:
            conn.close()

    def read_rows():
        with tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            result = {}
            for table, columns, order in [
                (
                    "instagram_posts",
                    "shortcode,username,caption,media_type,media_urls,likes,comments_count,views,posted_at",
                    "shortcode",
                ),
                (
                    "instagram_account_catalog_posts",
                    "source_id,source_account,caption,media_type,media_urls,likes,comments_count,views,posted_at",
                    "source_id",
                ),
                (
                    "social_posts",
                    "source_id,owner_handle,body,media_type,like_count,comment_count,share_count,view_count,posted_at",
                    "source_id",
                ),
            ]:
                cur.execute(f"SELECT {columns} FROM social.{table} ORDER BY {order}")
                result[table] = [dict(row) for row in cur.fetchall()]
            for table in [
                "social_post_observations",
                "social_post_legacy_refs",
                "social_post_entities",
                "social_post_media_assets",
            ]:
                cur.execute(f"SELECT count(*) FROM social.{table}")
                result[table + "_count"] = cur.fetchone()["count"]
            return result

    def measure(kind, repetition):
        trial_run_id = str(uuid4())
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "TRUNCATE social.instagram_posts,social.instagram_account_catalog_posts,social.social_posts CASCADE"
            )
            cur.execute("INSERT INTO social.scrape_runs(id) VALUES (%s)", (trial_run_id,))
            cur.execute("SELECT pg_stat_statements_reset()")
        samples.clear()
        start = time.perf_counter()
        for node in fixtures:
            with tx() as conn:
                if kind == "legacy":
                    post = SimpleNamespace(**node)
                    post.to_dict = lambda n=node: copy.deepcopy(n)
                    row = legacy._upsert_instagram_post(None, job_id=None, account=ACCOUNT, post=post, conn=conn)
                    catalog = legacy._upsert_shared_catalog_instagram_post(
                        run_id=trial_run_id, account_handle=ACCOUNT, post=post, conn=conn
                    )
                    synced = core._sync_instagram_canonical_catalog_post(catalog_row=catalog, conn=conn)
                    assert row and catalog and synced, "Legacy canonical sink returned incomplete rows"
                else:
                    canonical.write_post(
                        conn,
                        run_id=trial_run_id,
                        account=ACCOUNT,
                        observed_at=OBSERVED,
                        normalized=canonical.normalize_post_source(node, account=ACCOUNT),
                        source_payload=node,
                    )
        elapsed = time.perf_counter() - start
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT coalesce(sum(calls),0) FROM pg_stat_statements "
                "WHERE query NOT LIKE 'SELECT count(*) FROM pg_stat_activity%'"
            )
            calls = int(cur.fetchone()[0])
            cur.execute(
                "SELECT query,calls FROM pg_stat_statements "
                "WHERE query NOT LIKE 'SELECT count(*) FROM pg_stat_activity%' "
                "AND query NOT LIKE 'SELECT coalesce(sum(calls)%' ORDER BY calls DESC,query"
            )
            (root / f"queries-{kind}-{repetition}.json").write_text(
                json.dumps([{"query": q, "calls": c} for q, c in cur.fetchall()], indent=2) + "\n"
            )
        rows = read_rows()
        return {
            "path": kind,
            "repetition": repetition,
            "seconds": elapsed,
            "statement_calls": calls,
            "statement_calls_per_committed_target": calls / count,
            "sampled_peak_worker_sessions": max(samples, default=0),
            "committed_targets": len(rows["instagram_posts"]),
            "rows": rows,
        }

    observer = None
    try:
        with tx() as conn, conn.cursor() as cur:
            cur.execute((FIXTURES / "canonical_schema.sql").read_text())
            # The retained fixture has columns but omitted the maintained partial
            # index required by the actual legacy observation ON CONFLICT clause.
            cur.execute(
                "CREATE UNIQUE INDEX social_post_observations_detail_target_idx ON "
                "social.social_post_observations(detail_run_id,detail_source_id) WHERE detail_run_id IS NOT NULL"
            )
            cur.execute("CREATE EXTENSION pg_stat_statements")
        observer = threading.Thread(target=observe)
        observer.start()
        results = []
        for repetition in range(repetitions):
            paths = (
                ("candidate",)
                if baseline_dir
                else (("legacy", "candidate") if repetition % 2 == 0 else ("candidate", "legacy"))
            )
            for kind in paths:
                result = measure(kind, repetition)
                results.append(result)
                (root / "measurements.json").write_text(json.dumps(results, default=str, indent=2) + "\n")
        retained_baseline = None
        if baseline_dir:
            baseline_result = baseline_dir / "result.json"
            baseline_measurements = baseline_dir / "measurements.json"
            if not baseline_result.exists():
                baseline_result = baseline_dir / "legacy-canonical-baseline.json"
                baseline_measurements = baseline_dir / "legacy-canonical-baseline-measurements.json"
            retained_baseline = json.loads(baseline_result.read_text())
            prior_measurements = json.loads(baseline_measurements.read_text())
            legacy_rows = next(row["rows"] for row in prior_measurements if row["path"] == "legacy")
        else:
            legacy_rows = next(row["rows"] for row in results if row["path"] == "legacy")
        candidate_rows = next(row["rows"] for row in results if row["path"] == "candidate")
        # Retained baseline timestamps are JSON strings; compare both sides in
        # the same representation rather than treating datetime vs str as drift.
        legacy_rows = json.loads(json.dumps(legacy_rows, default=str))
        candidate_rows = json.loads(json.dumps(candidate_rows, default=str))
        parity = {key: legacy_rows[key] == candidate_rows[key] for key in legacy_rows}
        fingerprints = {
            str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in [
                REPO / "trr_backend/socials/instagram/catalog_ingest.py",
                REPO / "trr_backend/socials/social_season_analytics_impl.py",
                REPO / "trr_backend/ingestion/canonical.py",
                FIXTURES / "canonical_capture.json",
                FIXTURES / "canonical_schema.sql",
            ]
        }
        if retained_baseline:
            for name in (
                "tests/fixtures/ingestion/canonical_capture.json",
                "tests/fixtures/ingestion/canonical_schema.sql",
                "trr_backend/socials/instagram/catalog_ingest.py",
                "trr_backend/socials/social_season_analytics_impl.py",
            ):
                assert fingerprints[name] == retained_baseline["source_fixture_hashes"][name], name
        result = {
            "status": "PASS" if all(parity.values()) else "PARITY_DIFFERENCES",
            "baseline_capture": "NOW from unchanged legacy path; not before-implementation timing",
            "scope": (
                "post-normalization DTO to canonical sinks only; "
                "excludes full run/target/evidence/receipt orchestration"
            ),
            "parity": parity,
            "source_fixture_hashes": fingerprints,
            "measurements": [{key: value for key, value in row.items() if key != "rows"} for row in results],
            "full_ingestion_regression_gate": (
                "UNVERIFIED: legacy listing/detail policy, target claims and accounting not isolated by this sink seam"
            ),
            "retained_legacy_baseline": str(baseline_dir) if baseline_dir else None,
        }
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)
    finally:
        stop.set()
        if observer:
            observer.join(timeout=5)
        socket.socket.connect = original_connect
        subprocess.run(
            [str(pg_bin / "pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"], check=True, capture_output=True
        )
        (root / "cleanup.json").write_text(json.dumps({"postgres_stopped": not (data / "postmaster.pid").exists()}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-dir", required=True, type=Path)
    parser.add_argument("--targets", type=int, default=30)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument(
        "--baseline-dir", type=Path, help="Reuse unchanged legacy evidence; measure only changed candidate"
    )
    args = parser.parse_args()
    main(args.runtime_dir, args.targets, args.repetitions, args.baseline_dir)
