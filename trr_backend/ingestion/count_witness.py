"""Auxiliary public count observations; never canonical writes or certification."""

from __future__ import annotations

import json
import math
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.canonical import normalize_post_source
from trr_backend.ingestion.domain import Conflict, RunIntent, fingerprint
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.identity_policy import SourceRequestGate
from trr_backend.ingestion.public_transport import PublicDetailSource

RUN_ID = "2aef72ed-110c-4af9-b2b3-4a45da170a32"
INTENT_HASH = "10746e1197ea8b5f7272a90c662a2d1ebfd6e36a8a01cda4be1c73be428b9fc4"
PARSER_VERSION = "instagram-public-count-witness-v1"


@dataclass(frozen=True)
class WitnessRequest:
    shortcodes: tuple[str, ...]
    max_sends: int = 3
    max_seconds: float = 90

    def __post_init__(self):
        if not 1 <= len(self.shortcodes) <= 5 or len(set(self.shortcodes)) != len(self.shortcodes):
            raise ValueError("One to five distinct explicit targets required")
        if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", code) for code in self.shortcodes):
            raise ValueError("Invalid shortcode")
        if type(self.max_sends) is not int or not 1 <= self.max_sends <= 3:
            raise ValueError("One to three sends per target required")
        if not math.isfinite(self.max_seconds) or not 0 < self.max_seconds <= 90:
            raise ValueError("Total deadline must be at most 90 seconds")


def read_binding(transaction, shortcode):
    """One snapshot; hash large rows server-side and retain every keyed hash.

    No DISTINCT or row limit: ordering, identity and multiplicity all participate
    in the binding without transferring historical page checkpoints to Python.
    """
    with transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select to_jsonb(r) as run,to_jsonb(p) as post,
            clock_timestamp() as database_now,
            (select coalesce(jsonb_agg((to_jsonb(t)-'checkpoint') || jsonb_build_object(
                 'checkpoint_sha256',encode(sha256(convert_to(t.checkpoint::text,'UTF8')),'hex'))
                 order by stage),'[]')
             from social.instagram_ingestion_targets t
             where t.run_id=r.run_id and t.target_key=p.shortcode) as targets,
            (select coalesce(jsonb_agg(jsonb_build_object(
                 'run_id',c.run_id,'stage',c.stage,'target_key',c.target_key,'receipt_id',c.receipt_id,
                 'row_sha256',encode(sha256(convert_to(to_jsonb(c)::text,'UTF8')),'hex'))
                 order by receipt_id),'[]')
             from social.instagram_ingestion_receipts c
             where c.run_id=r.run_id and c.target_key=p.shortcode) as receipts,
            (select coalesce(jsonb_agg(jsonb_build_object(
                 'run_id',c.run_id,'stage',c.stage,'target_key',c.target_key,'page_id',c.page_id,
                 'row_sha256',encode(sha256(convert_to(to_jsonb(c)::text,'UTF8')),'hex'))
                 order by page_id),'[]')
             from social.instagram_ingestion_comment_pages c
             where c.run_id=r.run_id and c.target_key=p.shortcode) as pages
            from social.instagram_ingestion_runs r cross join social.instagram_posts p
            where r.run_id=%s and p.shortcode=%s""",
            (RUN_ID, shortcode),
        )
        row = cur.fetchone()
    if not row:
        raise Conflict("Witness binding missing")
    return dict(row)


def _timestamp(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise Conflict("Witness binding has an uncertain timestamp")
    return value


def validate_binding(binding, shortcode):
    run, post = binding["run"], binding["post"]
    intent = RunIntent.from_payload(run["intent"])
    if (
        str(run["run_id"]) != RUN_ID
        or run["generation"] != 1
        or run["intent_hash"] != INTENT_HASH
        or intent.digest != INTENT_HASH
        or run["account"] != "bravotv"
        or intent.account != "bravotv"
        or run.get("execution_placement") != "local"
        or run.get("execution_hold_reason")
        or intent.effective_source_profile.access_mode != "public"
    ):
        raise Conflict("Witness frozen public local run mismatch")
    posted = _timestamp(post.get("posted_at"))
    if (
        post.get("shortcode") != shortcode
        or not post.get("id")
        or not (post.get("source_post_id") or post.get("media_id"))
        or (post.get("source_account") or post.get("username", "")).lower() != "bravotv"
        or not datetime(2026, 1, 1, tzinfo=UTC) <= posted < datetime(2026, 2, 1, tzinfo=UTC)
    ):
        raise Conflict("Witness requires an existing January BravoTV post")
    targets = binding["targets"]
    if not targets or not any(t["stage"] == "comments" for t in targets):
        raise Conflict("Witness requires a bound comment target")
    now = _timestamp(binding["database_now"])
    for target in targets:
        if str(target["run_id"]) != RUN_ID or target["target_key"] != shortcode:
            raise Conflict("Witness target binding mismatch")
        expiry = target.get("owner_expires_at")
        # Terminal owner tokens remain as receipt fences after the lease ends.
        # Only unresolved tokens without an expiry represent uncertain owners.
        if (expiry is not None and _timestamp(expiry) > now) or (
            target.get("outcome") == "unresolved" and target.get("owner_token") and expiry is None
        ):
            raise Conflict("Witness target has an active or uncertain owner")
    for row in (*binding["receipts"], *binding["pages"]):
        if str(row["run_id"]) != RUN_ID or row["target_key"] != shortcode:
            raise Conflict("Witness receipt binding mismatch")
    return fingerprint({key: binding[key] for key in ("run", "post", "targets", "receipts", "pages")})


def native_count(payload, post):
    """Normalize maintained native envelopes without manufacturing a zero."""
    normalized = normalize_post_source(payload, account="bravotv")
    expected = str(post.get("source_post_id") or post.get("media_id"))
    if normalized.shortcode != post["shortcode"] or normalized.source_id != expected:
        raise Conflict("Witness source identity mismatch")
    node = normalized.raw_data
    if any(node.get(key) for key in ("comment_count_hidden", "comments_count_hidden")):
        raise ValueError("Witness count is hidden")
    values = []
    for key in ("comment_count", "edge_media_to_comment", "edge_media_to_parent_comment"):
        value = node.get(key)
        if isinstance(value, dict):
            value = value.get("count")
        if value is not None:
            if type(value) is not int or value < 0:
                raise ValueError("Witness requires an explicit nonnegative native integer count")
            values.append(value)
    if not values or len(set(values)) != 1:
        raise ValueError("Witness count missing or conflicting")
    return values[0]


def native_comments_disabled(payload):
    """Retain only an explicit native boolean; absence remains unknown."""
    node = normalize_post_source(payload, account="bravotv").raw_data
    value = node.get("comments_disabled")
    return value if type(value) is bool else None


def write_private_artifact(directory, artifact):
    directory = Path(directory)
    if not directory.is_dir() or directory.is_symlink() or directory.stat().st_mode & 0o077:
        raise ValueError("An existing private artifact directory (0700) is required")
    from uuid import uuid4

    path = directory / f"count-witness-{uuid4().hex}.json"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(artifact, handle, sort_keys=True, allow_nan=False)
        handle.write("\n")
    return str(path)


class _SendBoundGate:
    def __init__(self, gate, check, limit):
        self.inner, self.check, self.limit = gate, check, limit

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def before_request(self):
        self.check()
        if self.inner.request_count >= self.limit:
            raise Conflict("Witness send limit reached")
        return self.inner.before_request()


def refresh_witnesses(
    request,
    *,
    transaction,
    source_revision,
    apply=False,
    artifact_directory=None,
    source_factory=PublicDetailSource,
    gate_factory=SourceRequestGate,
    binding_reader=read_binding,
    clock=time.monotonic,
):
    deadline = clock() + request.max_seconds
    original_transaction = transaction

    @contextmanager
    def bounded_transaction():
        remaining = deadline - clock()
        if remaining <= 0:
            raise Conflict("Witness total deadline reached")
        with original_transaction() as conn:
            remaining = deadline - clock()
            if remaining <= 0:
                raise Conflict("Witness total deadline reached")
            with conn.cursor() as cur:
                timeout = f"{max(1, int(remaining * 1000))}ms"
                cur.execute("set local statement_timeout=%s", (timeout,))
                cur.execute("set local lock_timeout=%s", (timeout,))
            yield conn

    transaction = bounded_transaction
    baselines = {}
    for code in request.shortcodes:
        binding = binding_reader(transaction, code)
        baselines[code] = (binding, validate_binding(binding, code))
    if not apply:
        return {
            "status": "preview",
            "run_id": RUN_ID,
            "targets": list(request.shortcodes),
            "max_sends_per_target": request.max_sends,
            "max_seconds": request.max_seconds,
            "certification": False,
        }
    if not source_revision or artifact_directory is None:
        raise ValueError("Apply requires source revision and private artifact directory")
    # Validate destination before spending any source requests.
    directory = Path(artifact_directory)
    if not directory.is_dir() or directory.is_symlink() or directory.stat().st_mode & 0o077:
        raise ValueError("An existing private artifact directory (0700) is required")
    results = []
    for code, (binding, digest) in baselines.items():
        sends = 0

        def check(code=code, digest=digest, *, connection=None):
            if clock() >= deadline:
                raise Conflict("Witness total deadline reached")

            @contextmanager
            def existing_transaction():
                yield connection

            current = binding_reader(existing_transaction if connection is not None else transaction, code)
            if validate_binding(current, code) != digest:
                raise Conflict("Witness binding drift")

        def sent():
            nonlocal sends
            sends += 1

        inner_gate = gate_factory(
            cookies={},
            transaction=transaction,
            check_cancelled=check,
            check_cancelled_in_transaction=lambda connection, check=check: check(connection=connection),
            deadline=deadline,
            public_only=True,
            network_lane="local",
        )

        gate = _SendBoundGate(inner_gate, check, request.max_sends)
        source = source_factory(shortcode=code, gate=gate, deadline=deadline, check_cancelled=check)
        observed = []
        try:
            check()
            payload = source(
                check_cancelled=check,
                on_request=sent,
                source_observer=lambda stage, value, observed=observed: observed.append((stage, value)),
            )
            captured = datetime.now(UTC)
            if len(observed) != 1 or observed[0][0] != "details" or observed[0][1] != payload:
                raise ValueError("Witness source observation missing")
            count = native_count(payload, binding["post"])
            evidence = Evidence.capture(
                source_identity=f"instagram:public:count-witness:{code}",
                captured_at=captured,
                parser_version=PARSER_VERSION,
                payload=payload,
            )
            check()
            source.finish_success()
            check()
            artifact = {
                "kind": "auxiliary_public_count_witness",
                "certification": False,
                "run_id": RUN_ID,
                "generation": 1,
                "intent_hash": INTENT_HASH,
                "account": "bravotv",
                "placement": "local",
                "shortcode": code,
                "canonical_post_id": str(binding["post"]["id"]),
                "binding_sha256": digest,
                "native_count": count,
                "comments_disabled": native_comments_disabled(payload),
                "captured_at": captured.isoformat(),
                "sends": sends,
                "source_revision": source_revision,
                "parser_version": PARSER_VERSION,
                "source_identity": evidence.source_identity,
                "evidence_sha256": evidence.content_hash,
                "native_evidence": json.loads(evidence.payload_json),
            }
            path = write_private_artifact(directory, artifact)
            results.append({"shortcode": code, "native_count": count, "artifact": path, "sends": sends})
        except Exception as error:
            source.finish_failure(error)
            raise
    return {"status": "observed", "certification": False, "results": results}
