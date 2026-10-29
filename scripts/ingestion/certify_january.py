#!/usr/bin/env python3
"""Strict full-run date-range certification; read-only DB and complete hosted reads.

Load the worker's environment before invocation, or supply --env-file. No source
requests, controls, replay, uploads, database writes, or subset acceptance occur.
Output is a new private JSON artifact; existing evidence is never overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from urllib.parse import urlsplit
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ingestion.january_audit import comments, common, hosted, media, posts  # noqa: E402
from trr_backend.ingestion.certification import CERTIFICATION_VERSION, GATE_VERSION, certify, digest  # noqa: E402


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def code_hash():
    paths = [Path(__file__), ROOT / "trr_backend/ingestion/certification.py"]
    paths += sorted((ROOT / "scripts/ingestion/january_audit").glob("*.py"))
    paths += sorted((ROOT / "trr_backend/ingestion").glob("*.py"))
    return digest({str(path.relative_to(ROOT)): file_hash(path) for path in paths})


def storage_binding(path, expected_sha256, config):
    raw = Path(path).read_bytes()
    common.require(hashlib.sha256(raw).hexdigest() == expected_sha256, "storage_binding_hash_mismatch")
    value = json.loads(raw)
    common.require(
        value.get("version") == 1 and value.get("provider") == config.provider == "r2", "storage_provider_mismatch"
    )
    account = value.get("account_id", "")
    common.require(re.fullmatch(r"[a-f0-9]{32}", account) is not None, "storage_account_identity_invalid")
    endpoint = f"https://{account}.r2.cloudflarestorage.com"
    common.require(
        value.get("endpoint_url") == config.endpoint_url == endpoint and value.get("bucket") == config.bucket,
        "storage_account_endpoint_or_bucket_mismatch",
    )
    verified = common.stamp(value["verified_at"])
    common.require(verified.utcoffset() is not None and verified <= datetime.now(UTC), "storage_binding_clock_invalid")
    proof = value.get("verification") or {}
    common.require(
        proof.get("method") == "s3_object_and_public_byte_hash" and bool(proof.get("objects")),
        "storage_binding_verification_missing",
    )
    for item in proof["objects"]:
        common.require(
            isinstance(item.get("key"), str)
            and bool(item["key"])
            and type(item.get("size")) is int
            and item["size"] > 0
            and re.fullmatch(r"[a-f0-9]{64}", item.get("sha256", "")) is not None,
            "storage_binding_object_proof_invalid",
        )
    bases = []
    for entry in value.get("public_bases", []):
        base = entry.get("url", "")
        parsed = urlsplit(base)
        common.require(
            entry.get("verified") is True
            and parsed.scheme == "https"
            and bool(parsed.hostname)
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
            and "\\" not in parsed.path
            and all(part not in {".", ".."} for part in parsed.path.split("/")),
            "storage_public_alias_not_verified",
        )
        common.require(base == base.rstrip("/"), "storage_alias_must_be_normalized")
        bases.append(base)
    common.require(
        bool(bases) and len(bases) == len(set(bases)) and config.cdn_base_url in bases,
        "configured_public_base_not_verified",
    )
    return value, bases


def read_preflight(binding, context):
    """Bounded target inventory; active known gaps avoid deep scans."""
    from contextlib import nullcontext

    from psycopg2.extras import RealDictCursor

    from trr_backend.db import pg
    from trr_backend.ingestion.queries import Queries

    with (
        common.bound(context),
        pg.db_connection(label="instagram-january-certification-preflight", pool_name="social_progress") as connection,
    ):
        with connection.cursor(cursor_factory=RealDictCursor) as cur:
            common.configure_read_snapshot(connection, cur)
            progress = Queries(read_connection=lambda: nullcontext(connection)).progress(
                context.run_id, context.account
            )
            common.require(
                progress["intent"] == binding["successor_intent"]
                and progress["intent_hash"] == context.intent_hash
                and progress["account"] == context.account
                and progress["generation"] == context.generation,
                "preflight_run_binding_mismatch",
            )
            targets = common.rows(
                cur,
                """select t.stage,t.target_key,t.outcome,
                t.checkpoint #> '{comment_pages,settlement}' as checkpoint_settlement,
                r.canonical_identity->'settlement' as receipt_settlement,
                case when t.stage='listing' then r.canonical_identity->'post_keys' else null end as listing_keys
                from social.instagram_ingestion_targets t
                left join social.instagram_ingestion_receipts r on r.run_id=t.run_id and r.stage=t.stage
                  and r.target_key=t.target_key and r.owner_token=t.owner_token
                  and r.owner_generation=t.owner_generation
                where t.run_id=%s order by t.stage,t.target_key""",
                (context.run_id,),
            )
    return preflight_report(progress, targets, context)


def preflight_report(progress, targets, context):
    """A shallow report can block, but can never certify native membership."""
    inventories = {
        stage: sorted(row["target_key"] for row in targets if row["stage"] == stage)
        for stage in ("listing", "post_details", "comments", "media")
    }
    reported = sorted({key for row in targets if row["stage"] == "listing" for key in row.get("listing_keys") or []})
    mismatches = [
        {
            "stage": stage,
            "missing": sorted(set(reported) - set(inventories[stage])),
            "extra": sorted(set(inventories[stage]) - set(reported)),
        }
        for stage in ("post_details", "comments", "media")
        if set(inventories[stage]) != set(reported)
    ]
    gaps = []
    for row in targets:
        if row["stage"] != "comments":
            continue
        for field in ("receipt_settlement", "checkpoint_settlement"):
            settlement = row.get(field) or {}
            if type(settlement.get("count_gap")) is int and settlement["count_gap"] > 0:
                gaps.append(
                    {
                        "shortcode": row["target_key"],
                        "count_gap": settlement["count_gap"],
                        "expected_total": settlement.get("expected_total"),
                        "observed_total": settlement.get("observed_total"),
                        "settlement_source": field,
                        "target_outcome": row["outcome"],
                    }
                )
                break
    gates = {
        name: {
            "version": GATE_VERSION,
            "status": "pending",
            "diagnostics": ["deep_audit_not_attempted_known_source_gap"],
        }
        for name in (
            "january_listing",
            "posts",
            "comments_traversal_and_parentage",
            "media",
            "hosted_bytes",
            "unchanged_evidence",
        )
    }
    gates["source_count_parity"] = {
        "version": GATE_VERSION,
        "status": "blocked" if gaps else "pending",
        "diagnostics": ["known_source_comment_count_gap"] if gaps else ["deep_audit_required"],
    }
    return {
        "certification_version": CERTIFICATION_VERSION,
        "schema_version": 1,
        "status": "blocked",
        "final_acceptance": False,
        "operator_scope": "current_target_inventory_known_gap_preflight",
        "run_id": context.run_id,
        "intent_hash": context.intent_hash,
        "generation": context.generation,
        "run_state": progress["state"],
        "terminal_run": progress["state"] in {"completed", "blocked"},
        "full_history_gate_closed": True,
        "gates": gates,
        "reported_january_keys": reported,
        "independent_native_membership_verified": False,
        "target_inventories": inventories,
        "target_count": len(targets),
        "inventory_mismatches": mismatches,
        "known_count_gaps": gaps,
        "known_count_gap_total": sum(item["count_gap"] for item in gaps),
        "native_requests": 0,
        "database_mutations": 0,
        "storage_writes": 0,
        "retained_page_scans": 0,
        "storage_requests": 0,
        "database_snapshots": 1,
    }


def active_gap_preflight_stop(preflight):
    return bool(preflight["known_count_gaps"]) and not preflight["terminal_run"]


def read_snapshot(binding, context, bases):
    from psycopg2.extras import RealDictCursor

    from trr_backend.db import pg

    with common.bound(context):
        context.query_material.clear()
        with pg.db_connection(label="instagram-january-final-certification", pool_name="social_progress") as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cur:
                common.configure_read_snapshot(connection, cur)
                listing = common.inspect(cur, binding, ROOT, audit_listing=True)
                listing_hash = posts.digest({key: value for key, value in listing.items() if key != "observed_at"})
                post_report = posts.inspect(cur, binding, ROOT, listing)
                comment_report = comments.inspect(cur, binding, ROOT, listing, listing_hash)
                media_report = media.inspect(
                    cur,
                    binding,
                    ROOT,
                    listing,
                    comment_report,
                    listing_hash,
                    complete_observed_inventory=True,
                    post_proof=post_report,
                )
                plan = hosted.physical_plan(media_report, bases) if media_report["status"] == "passed" else []
        return {
            "reports": {"listing": listing, "posts": post_report, "comments": comment_report, "media": media_report},
            "public_only": binding["successor_intent"]["source_profile"]["access_mode"] == "public",
            "query_fingerprint": digest(
                {"version": context.query_fingerprint_version, "queries": context.query_material}
            ),
            "query_fingerprint_version": context.query_fingerprint_version,
            "physical_plan": plan,
            "object_inventory": [{"object_signature": posts.digest(item)} for item in plan],
        }


def run(args):
    if args.env_file:
        from dotenv import load_dotenv

        load_dotenv(args.env_file, override=True)
    from botocore.config import Config

    from scripts.ingestion.local_driver import validate_configuration
    from trr_backend.ingestion.domain import RunIntent
    from trr_backend.media.s3_mirror import _build_boto3_session, get_hosted_media_storage_config

    validate_configuration(os.environ["TRR_INSTAGRAM_DBOS_DATABASE_URL"])
    UUID(args.run_id)
    common.require(
        re.fullmatch(r"[a-f0-9]{64}", args.intent_hash) is not None and args.generation > 0, "invalid_run_identity"
    )
    binding_raw = Path(args.binding).read_bytes()
    binding = json.loads(binding_raw)
    common.require(
        binding["successor_run_id"] == args.run_id and binding["successor_intent_hash"] == args.intent_hash,
        "run_binding_mismatch",
    )
    intent = RunIntent.from_payload(binding["successor_intent"])
    start, end = common.stamp(args.date_start), common.stamp(args.date_end)
    common.require(
        intent.digest == args.intent_hash
        and intent.account == args.account
        and intent.date_start == start
        and intent.date_end == end
        and start.utcoffset() is not None
        and end.utcoffset() is not None
        and start < end,
        "explicit_date_range_intent_required",
    )
    context = common.AuditContext(args.run_id, args.intent_hash, args.account, start, end, args.generation)
    preflight = read_preflight(binding, context)
    if active_gap_preflight_stop(preflight):
        preflight["source_sha256"] = code_hash()
        return preflight
    config = get_hosted_media_storage_config()
    identity, bases = storage_binding(args.storage_binding, args.expected_storage_binding_sha256, config)
    source_before = code_hash()
    before = read_snapshot(binding, context, bases)
    plan = before["physical_plan"]
    options = {
        "region_name": config.region,
        "endpoint_url": config.endpoint_url,
        "config": Config(
            connect_timeout=10,
            read_timeout=60,
            retries={"total_max_attempts": 1},
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    }
    if config.access_key_id and config.secret_access_key:
        options.update(aws_access_key_id=config.access_key_id, aws_secret_access_key=config.secret_access_key)
        if config.session_token:
            options["aws_session_token"] = config.session_token
    client = _build_boto3_session(config).client("s3", **options)
    stop = Event()
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            objects = list(pool.map(lambda item: hosted.inspect_object(item, client, config, stop=stop), plan))
    finally:
        client.close()
    failures = [item for item in objects if item["status"] != "verified"]
    byte_report = {"status": "passed" if plan and not failures else "blocked", "failures": failures, "objects": objects}
    after = read_snapshot(binding, context, bases)
    common.require(
        hashlib.sha256(Path(args.binding).read_bytes()).hexdigest() == hashlib.sha256(binding_raw).hexdigest(),
        "run_binding_changed",
    )
    result = certify(
        before,
        after,
        byte_report,
        storage_binding_sha256=args.expected_storage_binding_sha256,
        storage_binding_after_sha256=file_hash(args.storage_binding),
        source_sha256=source_before,
        source_after_sha256=code_hash(),
    )
    result.update(
        query_fingerprint_version=context.query_fingerprint_version,
        observed_at=datetime.now(UTC).isoformat(),
        storage_identity={
            key: identity[key] for key in ("provider", "account_id", "bucket", "endpoint_url", "public_bases")
        },
        reports=before["reports"],
        after_reports=after["reports"],
        database_snapshots=3,
    )
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "run-id",
        "intent-hash",
        "date-start",
        "date-end",
        "binding",
        "storage-binding",
        "expected-storage-binding-sha256",
        "output",
    ):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--generation", type=int, default=1)
    parser.add_argument(
        "--workers",
        type=int,
        choices=range(1, 5),
        default=2,
        help="Hosted-byte readers only; comment workers remain unchanged",
    )
    parser.add_argument("--env-file")
    args = parser.parse_args(argv)
    output = Path(args.output)
    # Reserve before reads, protecting prior artifacts and rejecting symlinks.
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        try:
            result = run(args)
        except Exception as exc:
            result = {
                "status": "blocked",
                "certification_version": CERTIFICATION_VERSION,
                "gates": {
                    "preconditions": {
                        "version": GATE_VERSION,
                        "status": "blocked",
                        "diagnostics": ["precondition_or_read_failed"],
                    }
                },
                "final_acceptance": False,
                "error_code": str(exc) if isinstance(exc, common.GuardError) else type(exc).__name__,
                "native_requests": 0,
                "database_mutations": 0,
                "storage_writes": 0,
            }
        handle.write(json.dumps(result, sort_keys=True, indent=2, default=str) + "\n")
    print(
        json.dumps(
            {key: value for key, value in result.items() if key not in {"reports", "after_reports", "byte_report"}},
            sort_keys=True,
            default=str,
        )
    )
    return 0 if result.get("final_acceptance") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
